// Resolve HTML hrefs to internal document URL keys for the link graph.
//
// The static-site crawler discovers `<a href>` targets and must decide which
// point at other pages in the same site (an edge) versus off-site or
// non-navigational targets (dropped). Resolution is `baseUrl`-aware on purpose:
// a site deployed under `https://example.com/blog/` commonly emits *absolute*
// internal links (`https://example.com/blog/guide`), and treating those as
// external would silently produce an empty graph. So every href is resolved
// against the page's real URL and kept only when it lands on the same origin
// and under the base path.

const HTML_RE = /\.html?$/i;

// Normalize a URL pathname (or a crawler `urlPathFor` path) to a stable lookup
// key so a link's href and its target page collapse to the same string
// regardless of `.html` suffix, `index` file, or trailing slash.
export function urlKey(pathname) {
  let s;
  try {
    s = decodeURIComponent(String(pathname || "/"));
  } catch {
    s = String(pathname || "/");
  }
  s = s.replace(HTML_RE, "");
  s = s.replace(/(^|\/)index$/i, "$1");
  if (!s.startsWith("/")) s = "/" + s;
  s = s.replace(/\/{2,}/g, "/");
  if (s.length > 1) s = s.replace(/\/+$/, "");
  return s || "/";
}

// Split a `baseUrl` into a resolution origin and a base path prefix. An absolute
// baseUrl keeps its real origin so same-origin absolute links resolve; a
// path-only baseUrl (the common "/" case) uses a sentinel origin so any href
// that escapes to a real host is detectably external.
function baseParts(baseUrl) {
  const raw = String(baseUrl || "/");
  if (/^https?:\/\//i.test(raw)) {
    const url = new URL(raw);
    return { origin: url.origin, basePath: url.pathname.replace(/\/+$/, "") };
  }
  let basePath = raw.startsWith("/") ? raw : `/${raw}`;
  basePath = basePath.replace(/\/+$/, "");
  return { origin: "http://rf.invalid", basePath };
}

// Build a resolver `(href, fromSitePath) => urlKey | null` for a given baseUrl.
// `fromSitePath` is the site-root-relative path of the page the href was found
// on (the crawler's `urlPathFor` output, e.g. "/guide/intro"). Returns null for
// fragments, non-navigational schemes (mailto:, tel:, javascript:, …, which
// resolve to a foreign/opaque origin), external hosts, and — when the site is
// served under a base path — absolute paths outside that prefix.
export function createLinkResolver(baseUrl) {
  const { origin, basePath } = baseParts(baseUrl);
  return function resolve(href, fromSitePath) {
    const raw = String(href || "").trim();
    if (!raw || raw.startsWith("#")) return null;
    const fromPath = String(fromSitePath || "/");
    const fromUrl = origin + basePath + (fromPath.startsWith("/") ? fromPath : `/${fromPath}`);
    let resolved;
    try {
      resolved = new URL(raw, fromUrl);
    } catch {
      return null;
    }
    // Same-origin check also rejects mailto:/tel:/javascript:/data:, whose URL
    // origin is the opaque string "null".
    if (resolved.origin !== origin) return null;
    let path = resolved.pathname;
    if (basePath) {
      if (path === basePath) path = "/";
      else if (path.startsWith(`${basePath}/`)) path = path.slice(basePath.length);
      else return null; // same origin but outside the site's base path
    }
    return urlKey(path);
  };
}
