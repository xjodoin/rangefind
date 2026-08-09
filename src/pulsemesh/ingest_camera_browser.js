// Browser-backed image fetching, for camera feeds that only answer a
// browser session.
//
// Some public camera endpoints sit behind a bot-management layer that
// answers a plain HTTP client with 403 and a browser with the picture.
// This closes that gap the honest way: it drives a REAL browser
// (Playwright — an optional peer dependency, same arrangement as
// libp2p and jpeg-js), lets the site serve and run whatever it serves,
// and then reads each image from inside that page's own origin — the
// same request the site's own map makes on its own behalf.
//
// What this deliberately is NOT. It does not pretend to be a browser it
// isn't: no TLS-fingerprint impersonation, no forged user agents. It
// does not answer a challenge the browser itself cannot answer, does
// not rotate identities, addresses or sessions to get around a refusal,
// and never touches anything behind a login. When a site declines, the
// refusal is surfaced as an error and that camera simply goes unread —
// a block is an answer, not an obstacle.
//
// And being *able* to fetch is not the same as being *allowed* to. A
// camera feed belongs to its operator. Read their terms; keep
// `maxCameras` small and `intervalSeconds` long, because those two
// numbers are the load you are placing on somebody else's public
// service; and for anything ongoing, ask them for a feed. This module
// serialises every request and spaces them apart on purpose, so that a
// misconfiguration stays gentle rather than becoming a hammer.

async function loadChromium() {
  try {
    const module = await import("playwright");
    return module.chromium ?? module.default?.chromium;
  } catch {
    throw new Error(
      "The browser image fetcher needs the optional peer dependency playwright — npm install playwright"
    );
  }
}

const sleep = millis => new Promise(resolve => setTimeout(resolve, millis));

/**
 * A `fetchImage` implementation for `createCameraTrafficSource`, backed
 * by one long-lived browser page on `origin`.
 *
 * - `origin`: the site the images belong to, e.g.
 *   `"https://www.quebec511.info"`. Every image URL must be on it —
 *   the point of the exercise is that these are the page's own
 *   same-origin requests, not cross-site ones.
 * - `warmUpUrl`: a real page to load once so the session exists before
 *   any image is asked for. Defaults to the origin's root.
 * - `minIntervalMillis`: floor on the spacing between requests. They
 *   are serialised regardless — one camera at a time, always.
 * - `channel`: `"chrome"` uses the installed Google Chrome; if that is
 *   not present the bundled Chromium is used instead.
 * - `chromium`: a Playwright browser type to use instead of importing
 *   one — the seam for hosts that resolve Playwright from elsewhere,
 *   and for tests.
 */
export function createBrowserImageFetcher({
  origin,
  warmUpUrl = null,
  channel = "chrome",
  headless = true,
  minIntervalMillis = 1000,
  navigationTimeoutMillis = 45000,
  requestTimeoutMillis = 20000,
  chromium = null,
  onStatus = null
} = {}) {
  if (!origin) throw new Error("A browser image fetcher needs the origin it fetches from.");
  const home = warmUpUrl ?? origin;

  let browser = null;
  let page = null;
  let starting = null;
  let queue = Promise.resolve();
  let lastRequestAt = 0;
  const stats = { launches: 0, requests: 0, blocked: 0, resessions: 0, failures: 0, lastError: null };

  async function launch() {
    const browserType = chromium ?? await loadChromium();
    // The installed Chrome first — it is a real browser with the codecs
    // and the behaviour a site expects. Bundled Chromium is the fallback
    // for hosts that have no Chrome at all.
    try {
      browser = await browserType.launch({ headless, channel });
    } catch {
      browser = await browserType.launch({ headless });
    }
    stats.launches++;
    const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
    page = await context.newPage();
    page.setDefaultNavigationTimeout(navigationTimeoutMillis);
    await page.goto(home, { waitUntil: "domcontentloaded" });
    return page;
  }

  async function ensurePage() {
    if (page && !page.isClosed()) return page;
    starting = starting ?? launch().finally(() => { starting = null; });
    return starting;
  }

  /** Re-establish the session once, e.g. after it expires mid-run. */
  async function resession() {
    stats.resessions++;
    try {
      await browser?.close();
    } catch {
      // A browser that will not close cleanly is still being replaced.
    }
    browser = null;
    page = null;
    return ensurePage();
  }

  async function readImage(url) {
    const active = await ensurePage();
    return active.evaluate(async ({ target, timeout }) => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeout);
      try {
        const response = await fetch(target, { signal: controller.signal, credentials: "include" });
        if (!response.ok) return { status: response.status };
        const bytes = new Uint8Array(await response.arrayBuffer());
        let binary = "";
        for (let i = 0; i < bytes.length; i += 0x8000) {
          binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
        }
        const mediaType = (response.headers.get("content-type") || "image/jpeg").split(";")[0].trim();
        return { status: 200, mediaType, base64: btoa(binary) };
      } finally {
        clearTimeout(timer);
      }
    }, { target: url, timeout: requestTimeoutMillis });
  }

  async function fetchOne(camera) {
    const url = new URL(camera.imageUrl, origin);
    if (url.origin !== new URL(origin).origin) {
      throw new Error(`${url.origin} is not ${origin} — this fetcher only reads its own site's images.`);
    }
    const wait = minIntervalMillis - (Date.now() - lastRequestAt);
    if (wait > 0) await sleep(wait);
    lastRequestAt = Date.now();
    stats.requests++;

    let result = await readImage(url.href);
    // One re-session, for a session that simply aged out. A second
    // refusal is the site's answer and is reported as such — there is no
    // third attempt, and nothing here tries a different identity.
    if (result?.status === 403 || result?.status === 503) {
      stats.blocked++;
      if (onStatus) onStatus({ camera, status: result.status, stage: "first" });
      await resession();
      result = await readImage(url.href);
    }
    if (result?.status !== 200 || !result.base64) {
      stats.failures++;
      const status = result?.status ?? "no response";
      throw new Error(`${url.href}: refused (${status})`);
    }
    return { base64: result.base64, mediaType: result.mediaType };
  }

  /** Requests are serialised: one camera at a time, in order, always. */
  function fetchImage(camera) {
    // The camera source treats any throw as "this frame did not arrive",
    // which is right for it and useless for an operator trying to find
    // out why. Keep the reason where it can be read.
    const run = queue.then(() => fetchOne(camera)).catch(error => {
      stats.lastError = String(error?.message || error);
      throw error;
    });
    queue = run.catch(() => {});
    return run;
  }

  async function close() {
    try {
      await browser?.close();
    } finally {
      browser = null;
      page = null;
    }
  }

  return { fetchImage, close, stats, page: ensurePage };
}

/**
 * The gentler reader, and the one to prefer: instead of requesting
 * images itself, it opens the operator's own gallery page and keeps the
 * pictures that page loads on its own.
 *
 * This exists because asking for an image directly is not always the
 * same request the site serves. Québec 511's image URLs carry a token
 * the page mints; the picture is served with it and refused without it,
 * which is the operator scoping those images to their own page. Reading
 * what the page loaded honours that exactly — the URLs are theirs, the
 * token is theirs, the cadence is theirs — and it is cheaper for them
 * too: one page view yields every camera on it, which is precisely the
 * load one person looking at that page would cause.
 *
 * - `galleryUrlFor(camera)`: the page that shows this camera.
 * - `fileOf(camera)`: how to recognise this camera among the images the
 *   page loaded, matched against the image URL's path.
 * - `refreshMillis`: how long a page's pictures stay good before it is
 *   opened again. Long is polite.
 */
export function createGalleryImageFetcher({
  origin,
  galleryUrlFor,
  fileOf,
  chromium = null,
  channel = "chrome",
  // Headless is refused outright by some operators, and making a
  // headless browser look otherwise would be impersonation. Run a real
  // window (a virtual display on a server) or do not run this at all.
  headless = false,
  refreshMillis = 60_000,
  // A floor between page views. Opening galleries back to back looks
  // nothing like a person reading them, and an operator will say so by
  // serving pages with no pictures on them — measured on Québec 511,
  // where a burst of 72 views left every page after the first empty.
  minIntervalMillis = 5000,
  // How long to leave a gallery alone after it answers with no
  // pictures. Re-opening it for the next camera would turn one
  // throttled read into a burst of them, which is the opposite of what
  // an empty page is asking for.
  emptyCooldownMillis = 120_000,
  navigationTimeoutMillis = 45000,
  imagePattern = /\/Images\/Cameras\/[^/]+\/cam\/([^/?]+)/i
} = {}) {
  if (!origin) throw new Error("A gallery image fetcher needs the origin it reads from.");
  if (typeof galleryUrlFor !== "function") throw new Error("A gallery image fetcher needs galleryUrlFor(camera).");
  if (typeof fileOf !== "function") throw new Error("A gallery image fetcher needs fileOf(camera).");

  let browser = null;
  let page = null;
  let starting = null;
  let queue = Promise.resolve();
  const galleries = new Map(); // gallery url -> { readAt, images: Map(file -> {base64, mediaType}) }
  const stats = { pageViews: 0, imagesSeen: 0, emptyPages: 0, misses: 0, lastError: null };
  let lastPageViewAt = 0;

  async function ensurePage() {
    if (page && !page.isClosed()) return page;
    starting = starting ?? (async () => {
      const browserType = chromium ?? await loadChromium();
      try {
        browser = await browserType.launch({ headless, channel });
      } catch {
        browser = await browserType.launch({ headless });
      }
      const context = await browser.newContext({ viewport: { width: 1400, height: 1000 } });
      page = await context.newPage();
      page.setDefaultNavigationTimeout(navigationTimeoutMillis);
      return page;
    })().finally(() => { starting = null; });
    return starting;
  }

  /** Opens a gallery and keeps every camera picture it loads. */
  async function readGallery(url) {
    const active = await ensurePage();
    const wait = minIntervalMillis - (Date.now() - lastPageViewAt);
    if (wait > 0) await new Promise(resolve => setTimeout(resolve, wait));
    lastPageViewAt = Date.now();
    const images = new Map();
    const collect = async response => {
      const match = imagePattern.exec(new URL(response.url()).pathname);
      if (!match || !response.ok()) return;
      try {
        const body = await response.body();
        images.set(match[1], {
          base64: Buffer.from(body).toString("base64"),
          mediaType: (response.headers()["content-type"] || "image/jpeg").split(";")[0].trim()
        });
      } catch {
        // A body that cannot be read is simply a picture we did not get.
      }
    };
    active.on("response", collect);
    try {
      await active.goto(url, { waitUntil: "load" });
      // Give lazily-loaded tiles a moment to arrive.
      await active.waitForTimeout(2500);
    } finally {
      active.off("response", collect);
    }
    stats.pageViews++;
    stats.imagesSeen += images.size;
    // A page that showed nothing usually means the operator is asking
    // us to slow down. Remember that briefly — long enough that the
    // rest of this gallery's cameras fail fast instead of each opening
    // the page again, short enough that it is retried once the moment
    // passes.
    const now = Date.now();
    if (images.size === 0) {
      stats.emptyPages++;
      galleries.set(url, { readAt: now, images, retryAt: now + emptyCooldownMillis });
      return images;
    }
    galleries.set(url, { readAt: now, images, retryAt: 0 });
    return images;
  }

  async function imageFor(camera) {
    const url = new URL(galleryUrlFor(camera), origin).href;
    const cached = galleries.get(url);
    // A remembered page is reused while it is fresh; an empty one is
    // reused only until its cooldown expires, and then tried again.
    const usable = cached && (cached.retryAt
      ? Date.now() < cached.retryAt
      : Date.now() - cached.readAt < refreshMillis);
    const images = usable ? cached.images : await readGallery(url);
    const file = fileOf(camera);
    const found = images.get(file);
    if (!found) {
      stats.misses++;
      throw new Error(`${file} was not among the ${images.size} pictures ${url} loaded`);
    }
    return found;
  }

  function fetchImage(camera) {
    const run = queue.then(() => imageFor(camera)).catch(error => {
      stats.lastError = String(error?.message || error);
      throw error;
    });
    queue = run.catch(() => {});
    return run;
  }

  async function close() {
    try {
      await browser?.close();
    } finally {
      browser = null;
      page = null;
    }
  }

  return { fetchImage, close, stats };
}
