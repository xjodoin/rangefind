// Camera → traffic: infer congestion from public traffic-camera images
// with a small vision model, and feed it to the ingest publisher as
// ordinary flow observations.
//
// Many road authorities publish camera images but no speed feed (Québec
// is one — `ms:infos_cameras` is images only). A camera frame does not
// contain a speed, but it does contain what a human dispatcher reads
// off it in a glance: empty / flowing / slow / stopped. This module
// turns that glance into a `congestionRatio` observation, which the
// ingest publisher converts to km/h against the matched segment's own
// free-flow — "slow" on the A-40 and "slow" on a village street become
// different, honest numbers.
//
// The seams, kept separate on purpose:
//
// - `createCameraTrafficSource` is the pipeline: fetch frame → analyze
//   → observation, bounded concurrency, per-camera failure isolation.
//   It knows nothing about any AI vendor — `analyze` is an injected
//   async function, so an ONNX vehicle counter, a cloud vision API, or
//   a fixture in a test all fit the same seam.
// - `createClaudeCameraAnalyzer` is one analyzer: Claude Haiku (the
//   small, cheap tier — this is a classification glance, not reasoning)
//   with a structured-output schema, via the official SDK. The SDK is
//   an OPTIONAL peer dependency, like libp2p: nothing in rangefind
//   imports it unless an operator wires this analyzer up.
//
// Honesty rules encoded here rather than hoped for:
// - The model may answer "unusable" (dark, fogged lens, pointed at a
//   parking lot) and the pipeline drops the frame — a guess from a bad
//   frame published to the mesh is worse than no data.
// - Low-confidence answers are dropped below `minConfidence`.
// - A camera watching stopped traffic is evidence; a camera watching
//   free flow is published too (ratio ~0.95) so the ingest module's
//   congestion gate and recovery logic see it — the gate, not this
//   module, decides what reaches the wire.
//
// Cost, so nobody discovers it on an invoice: one frame is roughly
// 1000-1600 input tokens on Haiku ($1/MTok in) plus a few dozen output
// tokens — about $0.002 per analysis. 12 cameras at a 600 s interval is
// about 1700 analyses/day, roughly $3.50/day. Scale `maxCameras` and
// `intervalSeconds` with that in mind.

import { sha256Hex } from "./sha256.js";

/** How a glance maps onto the ingest module's congestion ratio. */
export const CONGESTION_RATIO = Object.freeze({
  empty: 1.0,
  free: 0.95,
  slow: 0.55,
  heavy: 0.3,
  stopped: 0.08
});

/**
 * A fetch that failed with a status the scheduler should react to.
 *
 * Carrying the code matters: "this host is pushing me away" (429, 403,
 * 5xx) and "that one image is gone" (404) call for opposite responses,
 * and a bare Error makes them look the same. The first should widen the
 * gap between requests to the whole host; the second should retire one
 * camera and leave the rest alone.
 */
export class CameraFetchError extends Error {
  constructor(message, status) {
    super(message);
    this.name = "CameraFetchError";
    this.status = status;
  }
}

async function defaultFetchImage(camera) {
  const response = await fetch(camera.imageUrl, {
    signal: AbortSignal.timeout(camera.timeoutMillis ?? 15000)
  });
  if (!response.ok) {
    throw new CameraFetchError(`${camera.imageUrl}: HTTP ${response.status}`, response.status);
  }
  const mediaType = (response.headers.get("content-type") || "image/jpeg").split(";")[0].trim();
  const bytes = new Uint8Array(await response.arrayBuffer());
  let binary = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return { base64: btoa(binary), mediaType };
}

/**
 * A source (for `createIngestPublisher`) that reads traffic cameras.
 *
 * - `cameras`: an array — or an async function returning one — of
 *   `{ id, lat, lon, imageUrl?, directions?, bearingDeg?,
 *   bothDirections?, description? }`. `lat`/`lon` place the observation
 *   on the road the camera watches (the camera's own mount point is
 *   usually close enough; refine per camera when it isn't).
 *
 *   **A camera must state which direction it is looking at**, because
 *   a count carries none of its own. At the snapped point of a divided
 *   highway the two carriageways and their two approaches are four
 *   candidate segments within a metre or two of each other, so an
 *   undirected observation lands on one of them by float ordering —
 *   deterministic, but meaningless. There are three ways to say it,
 *   in descending order of honesty:
 *
 *   1. `directions: [{ name, roi, bearingDeg, lanes?, visibleMeters? }]`
 *      — one entry per carriageway the camera sees. Each is analyzed as
 *      its own view of the frame (its own `roi`, its own background
 *      model) and becomes its own observation. This is the right answer
 *      for a divided road: each carriageway is counted separately and
 *      priced against its own lanes.
 *   2. `bearingDeg` — the camera watches one direction only. One
 *      observation, resolved to the matching approach.
 *   3. `bothDirections: true` — an undivided road where both directions
 *      genuinely share the roadway in view. A vehicle *count* is then
 *      SPLIT between the two approaches rather than duplicated onto
 *      each (a congestion *ratio* is intensive and applies to both
 *      unchanged).
 *
 *   A camera that states none of the three is skipped, and counted in
 *   `stats.undirected` — the same fail-closed rule the analyzer applies
 *   to a frame it cannot read.
 * - `analyze({ imageBase64, mediaType, camera })`: resolves to
 *   `{ congestion, confidence, vehicles?, reason? }` or null.
 *   `congestion` is one of empty|free|slow|heavy|stopped|unusable. An
 *   analyzer that COUNTS rather than classifies (the pixel analyzer)
 *   returns `vehicles` with no `congestion`: the observation then
 *   carries the count, and the ingest module extrapolates capacity
 *   from the matched road's own lanes and free-flow.
 * - `fetchImage(camera)`: resolves to `{ base64, mediaType }`; the
 *   default GETs `camera.imageUrl`. Override for feeds needing auth or
 *   a snapshot-from-stream step.
 */
export function createCameraTrafficSource({
  id = "cameras",
  intervalSeconds = 300,
  cameras,
  analyze,
  fetchImage = defaultFetchImage,
  maxCameras = 12,
  concurrency = 3,
  minConfidence = 0.5,
  onResult = null,
  // How long before a camera already looked at is worth another request.
  // This, not `intervalSeconds`, is what sets the load a feed sees: the
  // tick decides how often the budget is spent, this decides how widely.
  minRevisitSeconds = 900,
  // A frozen feed can never produce an observation, so it earns the
  // longest ordinary gap rather than the same one as a working camera.
  frozenRevisitMultiplier = 6,
  // Dark, fogged, pointed at a car park: not broken, just not answering
  // today.
  quietRevisitMultiplier = 3,
  // The ceiling on backoff after refusals. An hour is long enough to
  // stop looking like a scraper and short enough to recover the same day.
  maxBackoffSeconds = 3600,
  // Minimum gap between two requests to the same host. Concurrency is
  // per-pool, so without this the workers arrive as a burst.
  hostMinIntervalMillis = 1500,
  // How long a camera stays "interesting" after showing a queue.
  congestedBoostSeconds = 1800
} = {}) {
  if (typeof analyze !== "function") throw new Error("A camera source needs an analyze() function.");
  if (!cameras) throw new Error("A camera source needs cameras.");

  const stats = {
    frames: 0, fetchFailed: 0, analyzeFailed: 0,
    unusable: 0, lowConfidence: 0, frozen: 0, undirected: 0, observations: 0,
    // Scheduler visibility. `blocked` is the one to watch: it counts 429
    // and 403 specifically, so a feed that has started refusing us is
    // legible in the stats line instead of hiding inside fetchFailed.
    eligible: 0, skippedCooling: 0, refused: 0, blocked: 0
  };
  const lastFrameHash = new Map(); // camera id -> sha256 hex of last frame
  // camera id -> { nextEligibleMillis, failures, lastCongestedMillis, lastVisitedMillis }
  const schedule = new Map();

  /** True once a view says which way it is looking (see `cameras` above). */
  function stated(view) {
    return Number.isFinite(view.bearingDeg) || view.bothDirections === true;
  }

  /**
   * The analysis views of one camera: one per declared direction, or the
   * whole frame when the camera watches a single direction. Each view is
   * handed to the analyzer as if it were its own camera — a distinct id,
   * so the background model is per carriageway rather than per lens, and
   * its own `roi`, lanes and visible length.
   */
  function viewsOf(camera) {
    const declared = Array.isArray(camera.directions) ? camera.directions : null;
    if (!declared?.length) return [camera];
    return declared.map((direction, index) => ({
      ...camera,
      ...direction,
      id: `${camera.id ?? "camera"}#${direction.name ?? index}`,
      roi: direction.roi ?? camera.roi,
      // A named direction is one carriageway by construction; inheriting
      // the camera's bothDirections would put its count on both.
      bothDirections: false
    }));
  }

  // Returns `{ observations, outcome, status }` rather than a bare list.
  // The scheduler decides when to come back on the strength of WHY a poll
  // produced nothing, and "frozen", "refused" and "unusable" call for
  // three different answers — collapsing them into an empty array is what
  // made every camera look identical and get re-fetched at the same rate.
  async function pollCamera(camera, nowMillis) {
    const views = viewsOf(camera).filter(view => {
      if (stated(view)) return true;
      stats.undirected++;
      return false;
    });
    if (!views.length) return { observations: [], outcome: "undirected" };

    stats.frames++;
    let image;
    try {
      image = await fetchImage(camera);
    } catch (error) {
      stats.fetchFailed++;
      return { observations: [], outcome: "refused", status: error?.status };
    }
    if (!image?.base64) {
      stats.fetchFailed++;
      return { observations: [], outcome: "refused" };
    }
    // A byte-identical frame across polls is a FROZEN feed — an offline
    // camera serving its cached last picture (found in the wild: the
    // Montréal public endpoint serves frames from 2024). A live camera
    // never repeats exactly (sensor noise, re-encode, clock overlay).
    // Analyzing a frozen frame would eventually converge it into the
    // background and report an empty road that nobody is looking at.
    const hash = sha256Hex(image.base64);
    const key = camera?.id ?? "camera";
    if (lastFrameHash.get(key) === hash) {
      stats.frozen++;
      return { observations: [], outcome: "frozen" };
    }
    lastFrameHash.set(key, hash);

    // One fetch, one analysis per carriageway in view.
    const observations = [];
    for (const view of views) {
      const observation = await analyzeView(view, image, nowMillis);
      if (observation) observations.push(observation);
    }
    return {
      observations,
      outcome: observations.length ? "observed" : "unusable",
      // Congestion is the whole reason to look at a camera. One that just
      // reported a queue is worth returning to sooner than one that has
      // reported an empty road for hours.
      congested: observations.some(o => Number(o.congestionRatio) <= CONGESTION_RATIO.slow)
    };
  }

  async function analyzeView(view, image, nowMillis) {
    let result;
    try {
      result = await analyze({ imageBase64: image.base64, mediaType: image.mediaType, camera: view });
    } catch {
      stats.analyzeFailed++;
      return null;
    }
    if (onResult) {
      try {
        await onResult({ camera: view, result });
      } catch {
        // An observer must never take the pipeline down with it.
      }
    }
    const ratio = result ? CONGESTION_RATIO[result.congestion] : undefined;
    const counted = result && result.congestion == null && Number.isFinite(result.vehicles);
    if (!result || (ratio === undefined && !counted)) {
      stats.unusable++;
      return null;
    }
    if (!(Number(result.confidence) >= minConfidence)) {
      stats.lowConfidence++;
      return null;
    }
    stats.observations++;
    return {
      kind: "flow",
      lat: view.lat,
      lon: view.lon,
      // A classification carries the ratio; a count carries the count
      // plus the camera's calibration, and the road graph does the rest.
      ...(ratio !== undefined
        ? { congestionRatio: ratio }
        : {
            vehicleCount: result.vehicles,
            ...(Number.isFinite(view.visibleMeters) ? { visibleMeters: view.visibleMeters } : {}),
            ...(Number.isFinite(view.lanes) ? { lanes: view.lanes } : {})
          }),
      ...(Number.isFinite(view.bearingDeg) ? { bearingDeg: view.bearingDeg } : {}),
      ...(view.bothDirections ? { bothDirections: true } : {}),
      observedAtMillis: nowMillis
    };
  }

  /** Per-host pacing, so a whole feed is never asked faster than this. */
  const hostNextFreeMillis = new Map();

  function hostOf(camera) {
    try {
      return new URL(camera.imageUrl).host;
    } catch {
      return "";
    }
  }

  /**
   * Waits out this host's minimum gap and claims the next slot.
   *
   * Concurrency is per-pool, not per-host, so without this three workers
   * hitting one image server arrive together — which is what a rate
   * limiter sees as a burst regardless of how modest the average is.
   */
  async function claimHostSlot(host, nowMillis) {
    if (!host || hostMinIntervalMillis <= 0) return;
    const readyAt = Math.max(nowMillis, hostNextFreeMillis.get(host) ?? 0);
    hostNextFreeMillis.set(host, readyAt + hostMinIntervalMillis);
    const wait = readyAt - nowMillis;
    if (wait > 0) await new Promise(resolve => setTimeout(resolve, wait));
  }

  function stateOf(camera) {
    const key = camera?.id ?? "camera";
    let state = schedule.get(key);
    if (!state) {
      state = { nextEligibleMillis: 0, failures: 0, lastCongestedMillis: 0, lastVisitedMillis: 0, unusableStreak: 0 };
      schedule.set(key, state);
    }
    return state;
  }

  /** Seconds to wait before this camera is worth another request. */
  function revisitSeconds(outcome, status, state) {
    if (outcome === "refused") {
      // Back off hardest on the codes that mean "stop asking": 429 and
      // 403 are a decision about us, not an accident. Doubling per
      // consecutive failure turns a block into a retreat instead of a
      // tighter loop, which is how a polite client stays unblocked.
      const rejected = status === 429 || status === 403 || (status >= 500 && status < 600);
      const base = minRevisitSeconds * (rejected ? 4 : 1);
      return Math.min(maxBackoffSeconds, base * 2 ** Math.min(state.failures, 6));
    }
    // A frozen feed is an offline camera serving yesterday's picture. It
    // costs a request every time and can never produce an observation,
    // so it earns the longest ordinary gap.
    if (outcome === "frozen") return minRevisitSeconds * frozenRevisitMultiplier;
    // One unusable frame is not evidence of a useless camera, and
    // demoting on the first is actively wrong for a differencing
    // analyzer: its FIRST frame of any camera is unusable by
    // construction, because that frame is what establishes the
    // background to difference against. Backing off there means the
    // second frame never arrives and the camera can never start working
    // — it would look like a dead feed while being a perfectly good one.
    // Two in a row is a camera that is genuinely dark, fogged or aimed
    // at a car park.
    if (outcome === "unusable") {
      return state.unusableStreak > 1 ? minRevisitSeconds * quietRevisitMultiplier : minRevisitSeconds;
    }
    return minRevisitSeconds;
  }

  async function fetch_({ nowMillis }) {
    const list = (typeof cameras === "function" ? await cameras() : cameras) || [];

    // Lazy in the sense the TomTom backfill is lazy: the budget is spent
    // where it can still change something, rather than on the first N
    // entries of a list.
    //
    // The old selection was `list.slice(0, maxCameras)`, which asked the
    // same handful every tick for as long as the process lived. On a
    // 675-camera feed that is the worst of both: 667 cameras never seen,
    // and eight image URLs fetched on a metronome from one address —
    // which is precisely the shape a rate limiter blocks, and it had no
    // way to notice it had been blocked.
    const eligible = [];
    for (const camera of list) {
      const state = stateOf(camera);
      if (state.nextEligibleMillis > nowMillis) continue;
      eligible.push({ camera, state });
    }

    // Oldest first, but a camera that recently showed a queue jumps the
    // line: congestion is the only thing here worth spending a request
    // on, and it moves on a scale of minutes.
    eligible.sort((a, b) => {
      const recentA = nowMillis - a.state.lastCongestedMillis < congestedBoostSeconds * 1000 ? 1 : 0;
      const recentB = nowMillis - b.state.lastCongestedMillis < congestedBoostSeconds * 1000 ? 1 : 0;
      if (recentA !== recentB) return recentB - recentA;
      return a.state.lastVisitedMillis - b.state.lastVisitedMillis;
    });

    const wanted = eligible.slice(0, maxCameras);
    stats.eligible = eligible.length;
    stats.skippedCooling = list.length - eligible.length;

    const observations = [];
    // A bounded worker pool: one stuck frame must not serialize the rest,
    // and an analyzer billed per call must not be hit with a thundering herd.
    let next = 0;
    const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
      while (next < wanted.length) {
        const { camera, state } = wanted[next++];
        await claimHostSlot(hostOf(camera), Date.now());
        const result = await pollCamera(camera, nowMillis);
        state.lastVisitedMillis = nowMillis;
        state.unusableStreak = result.outcome === "unusable" ? state.unusableStreak + 1 : 0;
        if (result.outcome === "refused") {
          state.failures++;
          stats.refused++;
          if (result.status === 429 || result.status === 403) stats.blocked++;
        } else {
          state.failures = 0;
        }
        if (result.congested) state.lastCongestedMillis = nowMillis;
        state.nextEligibleMillis =
          nowMillis + revisitSeconds(result.outcome, result.status, state) * 1000;
        observations.push(...result.observations);
      }
    });
    await Promise.all(workers);
    return observations;
  }

  return { id, intervalSeconds, fetch: fetch_, stats };
}

const ANALYZER_SCHEMA = Object.freeze({
  type: "object",
  additionalProperties: false,
  required: ["congestion", "confidence"],
  properties: {
    congestion: {
      type: "string",
      enum: ["empty", "free", "slow", "heavy", "stopped", "unusable"],
      description: "Traffic state visible in the frame; 'unusable' when the image cannot support a judgement."
    },
    vehicles: { type: "integer", description: "Approximate vehicle count visible on the main roadway." },
    confidence: { type: "number", description: "0..1 confidence in the congestion judgement." },
    reason: { type: "string", description: "One short sentence of evidence." }
  }
});

function analyzerPrompt(camera) {
  return [
    "This is a single frame from a fixed road-traffic camera",
    camera.description ? ` (${camera.description})` : "",
    ".\n\nJudge the traffic state of the main roadway in view:\n",
    "- empty: no vehicles, or nearly none\n",
    "- free: vehicles moving at what looks like normal spacing and speed\n",
    "- slow: visibly dense traffic, reduced spacing, but moving\n",
    "- heavy: queued traffic, bumper to bumper, creeping\n",
    "- stopped: stationary queues on the roadway\n",
    "- unusable: answer this whenever the frame cannot support a judgement — ",
    "too dark, fogged or dirty lens, glare, heavy precipitation, the camera ",
    "is not showing a roadway, or the image is an error/placeholder card. ",
    "Never guess from an unusable frame.\n\n",
    "A single frame shows density, not motion: use spacing, queue shape, ",
    "brake lights, and lane occupancy as evidence. Parked cars beside the ",
    "road and vehicles on side roads are not the main roadway."
  ].join("");
}

/**
 * A camera analyzer backed by Claude Haiku — the small, cheap tier;
 * a congestion glance is classification, not reasoning. Requires the
 * optional peer dependency `@anthropic-ai/sdk` and an Anthropic
 * credential (ANTHROPIC_API_KEY or an `ant auth login` profile).
 */
export async function createClaudeCameraAnalyzer({
  model = "claude-haiku-4-5",
  apiKey = undefined,
  maxTokens = 300
} = {}) {
  let Anthropic;
  try {
    ({ default: Anthropic } = await import("@anthropic-ai/sdk"));
  } catch {
    throw new Error(
      "The Claude camera analyzer needs the optional peer dependency @anthropic-ai/sdk — npm install @anthropic-ai/sdk"
    );
  }
  const client = new Anthropic(apiKey ? { apiKey } : {});

  return async function analyze({ imageBase64, mediaType, camera }) {
    const response = await client.messages.create({
      model,
      max_tokens: maxTokens,
      output_config: { format: { type: "json_schema", schema: ANALYZER_SCHEMA } },
      messages: [{
        role: "user",
        content: [
          { type: "image", source: { type: "base64", media_type: mediaType, data: imageBase64 } },
          { type: "text", text: analyzerPrompt(camera) }
        ]
      }]
    });
    if (response.stop_reason === "refusal") return null;
    const text = response.content.find(block => block.type === "text")?.text;
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch {
      return null;
    }
  };
}
