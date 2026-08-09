// Pixel camera analyzer — count vehicles with no model at all.
//
// A fixed traffic camera is the easiest computer-vision setting there
// is: the road never moves, only the vehicles do. So the classical
// pipeline is enough — keep a per-camera background (running median),
// call the pixels that differ "foreground", count the foreground blobs
// — and the *interpretation* of the count belongs to the road graph,
// not to this file: the observation carries `vehicleCount`, and the
// ingest module extrapolates capacity from the matched segment's own
// class (lanes) and free-flow via the fundamental diagram. Twelve cars
// mean different things on the A-40 and on a village street; rangefind
// knows which road the camera watches, so it decides.
//
// What a single counted frame cannot see is motion — but consecutive
// polls can: if the foreground mask barely moves between two frames
// minutes apart, those vehicles are a standing queue, and the analyzer
// says "stopped" outright instead of a count.
//
// The honesty rules mirror the rest of the ingest path: this analyzer
// REFUSES frames it cannot judge — too dark (headlight blobs are not
// vehicles you can count), a scene change (PTZ cameras rotate between
// presets; the background is then a lie and gets rebuilt), or a
// background still warming up. A refused frame publishes nothing.
//
// Everything is dependency-free except JPEG decoding, which is the
// optional peer dependency `jpeg-js` (pure JS, same pattern as libp2p
// and @anthropic-ai/sdk) — and the `decode` seam accepts any
// replacement, which is also how tests feed synthetic frames.

function base64ToBytes(base64) {
  if (typeof Buffer !== "undefined") return new Uint8Array(Buffer.from(base64, "base64"));
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

async function defaultDecode(bytes) {
  let jpeg;
  try {
    const module = await import("jpeg-js");
    jpeg = module.default ?? module;
  } catch {
    throw new Error(
      "The pixel camera analyzer needs the optional peer dependency jpeg-js — npm install jpeg-js (or inject your own decode())."
    );
  }
  const { width, height, data } = jpeg.decode(bytes, { useTArray: true, maxMemoryUsageInMB: 128 });
  const gray = new Uint8Array(width * height);
  for (let i = 0, p = 0; i < gray.length; i++, p += 4) {
    // Integer luma; the exact weights matter less than being deterministic.
    gray[i] = (data[p] * 77 + data[p + 1] * 150 + data[p + 2] * 29) >> 8;
  }
  return { width, height, gray };
}

/** Nearest-neighbour downsample to `targetWidth`, preserving aspect. */
function downsample({ width, height, gray }, targetWidth) {
  if (width <= targetWidth) return { width, height, gray };
  const w = targetWidth;
  const h = Math.max(1, Math.round(height * (targetWidth / width)));
  const out = new Uint8Array(w * h);
  for (let y = 0; y < h; y++) {
    const sy = Math.min(height - 1, Math.floor(y * height / h));
    for (let x = 0; x < w; x++) {
      const sx = Math.min(width - 1, Math.floor(x * width / w));
      out[y * w + x] = gray[sy * width + sx];
    }
  }
  return { width: w, height: h, gray: out };
}

/** Count connected components (4-neighbour) of the mask, with areas. */
function blobAreas(mask, width, height, minArea, maxArea) {
  const labels = new Int32Array(mask.length);
  const areas = [];
  const stack = [];
  let nextLabel = 0;
  for (let start = 0; start < mask.length; start++) {
    if (!mask[start] || labels[start]) continue;
    nextLabel++;
    let area = 0;
    stack.push(start);
    labels[start] = nextLabel;
    while (stack.length) {
      const index = stack.pop();
      area++;
      const x = index % width;
      const y = (index / width) | 0;
      if (x > 0 && mask[index - 1] && !labels[index - 1]) { labels[index - 1] = nextLabel; stack.push(index - 1); }
      if (x < width - 1 && mask[index + 1] && !labels[index + 1]) { labels[index + 1] = nextLabel; stack.push(index + 1); }
      if (y > 0 && mask[index - width] && !labels[index - width]) { labels[index - width] = nextLabel; stack.push(index - width); }
      if (y < height - 1 && mask[index + width] && !labels[index + width]) { labels[index + width] = nextLabel; stack.push(index + width); }
    }
    if (area >= minArea && area <= maxArea) areas.push(area);
  }
  return areas;
}

/**
 * Creates an `analyze` function for `createCameraTrafficSource` that
 * needs no model and no network: per-camera running-median background,
 * foreground blobs → `vehicles`, standing-queue detection → "stopped".
 *
 * Per-camera knobs (on the camera object):
 * - `roi: [x0, y0, x1, y1]` — the road, as fractions of the frame.
 *   Default skips the top 35% (sky, horizon). Set this per camera; it
 *   is the single highest-leverage calibration. A camera watching two
 *   carriageways declares one region per direction and the source hands
 *   each to this analyzer as its own view — see `ingest_camera.js`.
 * - `carAreaFraction` — a vehicle's apparent size as a fraction of the
 *   whole frame (not of the region), so counts from differently-sized
 *   direction regions stay comparable.
 * - `visibleMeters`, `lanes` — forwarded to the ingest module's
 *   capacity extrapolation (defaults: 200 m, lanes from the matched
 *   road's class).
 *
 * The background needs `warmupFrames` polls before the first count; at
 * a 300 s interval that is ~15 minutes after start, per camera. A PTZ
 * preset change resets the warm-up (detected as a whole-frame change).
 */
export function createPixelCameraAnalyzer({
  decode = defaultDecode,
  targetWidth = 320,
  warmupFrames = 3,
  diffThreshold = 28,
  minLuma = 40,
  maxForeground = 0.6,
  stopOverlap = 0.6,
  stopOccupancy = 0.1,
  carAreaFraction = 0.004
} = {}) {
  const states = new Map(); // camera id -> { bg, frames, prevMask, width, height }

  return async function analyze({ imageBase64, camera }) {
    const frame = downsample(await decode(base64ToBytes(imageBase64)), targetWidth);
    const { width, height, gray } = frame;
    const key = camera?.id ?? "camera";

    const [rx0, ry0, rx1, ry1] = camera?.roi ?? [0, 0.35, 1, 1];
    const x0 = Math.max(0, Math.floor(rx0 * width));
    const y0 = Math.max(0, Math.floor(ry0 * height));
    const x1 = Math.min(width, Math.ceil(rx1 * width));
    const y1 = Math.min(height, Math.ceil(ry1 * height));
    const roiPixels = Math.max(1, (x1 - x0) * (y1 - y0));

    let lumaSum = 0;
    for (let y = y0; y < y1; y++) {
      for (let x = x0; x < x1; x++) lumaSum += gray[y * width + x];
    }
    if (lumaSum / roiPixels < minLuma) {
      return { congestion: "unusable", confidence: 0.9, reason: "too dark to count" };
    }

    let state = states.get(key);
    if (!state || state.width !== width || state.height !== height) {
      states.set(key, { bg: Int16Array.from(gray), frames: 1, prevMask: null, width, height });
      return { congestion: "unusable", confidence: 0.9, reason: "background warming up" };
    }

    // Foreground vs the background as it stood BEFORE this frame.
    const mask = new Uint8Array(gray.length);
    let foregroundPixels = 0;
    for (let y = y0; y < y1; y++) {
      for (let x = x0; x < x1; x++) {
        const index = y * width + x;
        if (Math.abs(gray[index] - state.bg[index]) > diffThreshold) {
          mask[index] = 1;
          foregroundPixels++;
        }
      }
    }
    // Sign-update running median: robust to passing vehicles, converges
    // on the road surface, costs one add per pixel.
    for (let i = 0; i < gray.length; i++) {
      const delta = gray[i] - state.bg[i];
      if (delta > 0) state.bg[i]++;
      else if (delta < 0) state.bg[i]--;
    }

    const occupancy = foregroundPixels / roiPixels;
    if (occupancy > maxForeground) {
      // The whole scene changed — a PTZ preset move, a wiper pass, a
      // whiteout. The background is a lie now; rebuild it.
      states.set(key, { bg: Int16Array.from(gray), frames: 1, prevMask: null, width, height });
      return { congestion: "unusable", confidence: 0.9, reason: "scene changed — background reset" };
    }

    state.frames++;
    if (state.frames <= warmupFrames) {
      return { congestion: "unusable", confidence: 0.9, reason: "background warming up" };
    }

    // A standing queue: the foreground is substantial AND it is the
    // same foreground as last poll, minutes ago. Moving traffic has
    // fully turned over in that time; only stopped vehicles persist.
    const prevMask = state.prevMask;
    state.prevMask = mask;
    if (prevMask && occupancy >= stopOccupancy) {
      let intersection = 0;
      let union = 0;
      for (let y = y0; y < y1; y++) {
        for (let x = x0; x < x1; x++) {
          const index = y * width + x;
          if (mask[index] && prevMask[index]) intersection++;
          if (mask[index] || prevMask[index]) union++;
        }
      }
      if (union > 0 && intersection / union >= stopOverlap) {
        return { congestion: "stopped", confidence: 0.7, reason: "same vehicles as last poll" };
      }
    }

    // Count: blobs where they separate, area where a queue merges them.
    // A vehicle's apparent size is a property of the camera's perspective,
    // so it is a fraction of the FRAME, never of the region being measured
    // — otherwise two direction regions of different sizes would count the
    // same car differently, and their densities could not be compared.
    const carArea = Math.max(4, width * height * carAreaFraction);
    const areas = blobAreas(mask, width, height, Math.max(3, carArea * 0.2), carArea * 20);
    let vehicles = areas.length;
    if (occupancy > 0.15) {
      vehicles = Math.max(vehicles, Math.round(foregroundPixels / carArea));
    }
    return {
      vehicles,
      occupancy,
      confidence: 0.7,
      reason: `${areas.length} blobs, ${(occupancy * 100).toFixed(1)}% occupancy`
    };
  };
}
