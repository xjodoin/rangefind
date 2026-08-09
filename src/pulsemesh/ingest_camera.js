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

async function defaultFetchImage(camera) {
  const response = await fetch(camera.imageUrl, {
    signal: AbortSignal.timeout(camera.timeoutMillis ?? 15000)
  });
  if (!response.ok) throw new Error(`${camera.imageUrl}: HTTP ${response.status}`);
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
  onResult = null
} = {}) {
  if (typeof analyze !== "function") throw new Error("A camera source needs an analyze() function.");
  if (!cameras) throw new Error("A camera source needs cameras.");

  const stats = {
    frames: 0, fetchFailed: 0, analyzeFailed: 0,
    unusable: 0, lowConfidence: 0, frozen: 0, undirected: 0, observations: 0
  };
  const lastFrameHash = new Map(); // camera id -> sha256 hex of last frame

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

  async function pollCamera(camera, nowMillis) {
    const views = viewsOf(camera).filter(view => {
      if (stated(view)) return true;
      stats.undirected++;
      return false;
    });
    if (!views.length) return [];

    stats.frames++;
    let image;
    try {
      image = await fetchImage(camera);
    } catch {
      stats.fetchFailed++;
      return [];
    }
    if (!image?.base64) {
      stats.fetchFailed++;
      return [];
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
      return [];
    }
    lastFrameHash.set(key, hash);

    // One fetch, one analysis per carriageway in view.
    const observations = [];
    for (const view of views) {
      const observation = await analyzeView(view, image, nowMillis);
      if (observation) observations.push(observation);
    }
    return observations;
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

  async function fetch_({ nowMillis }) {
    const list = (typeof cameras === "function" ? await cameras() : cameras) || [];
    const wanted = list.slice(0, maxCameras);
    const observations = [];
    // A bounded worker pool: one stuck frame must not serialize the rest,
    // and an analyzer billed per call must not be hit with a thundering herd.
    let next = 0;
    const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
      while (next < wanted.length) {
        const camera = wanted[next++];
        observations.push(...await pollCamera(camera, nowMillis));
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
