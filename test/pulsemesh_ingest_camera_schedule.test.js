import { test } from "node:test";
import assert from "node:assert/strict";
import { createCameraTrafficSource, CameraFetchError }
  from "../src/pulsemesh/ingest_camera.js";

const cameras = Array.from({ length: 20 }, (_, i) => ({
  id: `cam-${i}`,
  lat: 45.5 + i * 0.001,
  lon: -73.56,
  bearingDeg: 90,
  imageUrl: `https://feed.example/cam${i}.jpg`
}));

// A distinct frame each time, so nothing is mistaken for a frozen feed.
function liveFetch(seq = { n: 0 }) {
  return async () => ({ base64: `frame-${seq.n++}`, mediaType: "image/jpeg" });
}
const flowing = async () => ({ congestion: "free", confidence: 0.9 });

test("the budget rotates instead of pinning the first N cameras", async () => {
  const seen = [];
  const source = createCameraTrafficSource({
    cameras, analyze: flowing, maxCameras: 5, concurrency: 1,
    hostMinIntervalMillis: 0, minRevisitSeconds: 900,
    fetchImage: async camera => { seen.push(camera.id); return { base64: `f-${camera.id}-${seen.length}`, mediaType: "image/jpeg" }; }
  });
  let now = 1_000_000;
  for (let tick = 0; tick < 4; tick++) {
    await source.fetch({ nowMillis: now });
    now += 60_000; // ticks are far apart, but well inside minRevisitSeconds
  }
  assert.equal(seen.length, 20, "four ticks of five should spend twenty requests");
  assert.equal(new Set(seen).size, 20, "and reach twenty distinct cameras, not the same five");
});

test("a camera is not re-asked inside its revisit window", async () => {
  const seen = [];
  const source = createCameraTrafficSource({
    cameras: cameras.slice(0, 3), analyze: flowing, maxCameras: 3, concurrency: 1,
    hostMinIntervalMillis: 0, minRevisitSeconds: 900,
    fetchImage: async camera => { seen.push(camera.id); return { base64: `f-${camera.id}-${seen.length}`, mediaType: "image/jpeg" }; }
  });
  await source.fetch({ nowMillis: 1_000_000 });
  assert.equal(seen.length, 3);
  await source.fetch({ nowMillis: 1_000_000 + 60_000 });
  assert.equal(seen.length, 3, "still cooling: no new requests");
  await source.fetch({ nowMillis: 1_000_000 + 901_000 });
  assert.equal(seen.length, 6, "past the window: asked again");
});

test("a 429 backs off exponentially rather than retrying on the next tick", async () => {
  let attempts = 0;
  const source = createCameraTrafficSource({
    cameras: cameras.slice(0, 1), analyze: flowing, maxCameras: 1, concurrency: 1,
    hostMinIntervalMillis: 0, minRevisitSeconds: 10, maxBackoffSeconds: 3600,
    fetchImage: async () => { attempts++; throw new CameraFetchError("blocked", 429); }
  });
  let now = 1_000_000;
  await source.fetch({ nowMillis: now });
  assert.equal(attempts, 1);
  // minRevisit 10 x4 (rejected) x2^1 = 80s. At +40s it must stay away.
  await source.fetch({ nowMillis: now + 40_000 });
  assert.equal(attempts, 1, "still backing off");
  await source.fetch({ nowMillis: now + 100_000 });
  assert.equal(attempts, 2, "retried once the backoff expired");
  assert.equal(source.stats.blocked, 2, "429s are counted as blocks, not generic failures");
});

test("a frozen feed is demoted, not polled at the same rate as a live one", async () => {
  let attempts = 0;
  const source = createCameraTrafficSource({
    cameras: cameras.slice(0, 1), analyze: flowing, maxCameras: 1, concurrency: 1,
    hostMinIntervalMillis: 0, minRevisitSeconds: 100, frozenRevisitMultiplier: 6,
    fetchImage: async () => { attempts++; return { base64: "identical", mediaType: "image/jpeg" }; }
  });
  let now = 1_000_000;
  await source.fetch({ nowMillis: now });            // first frame, analyzed
  await source.fetch({ nowMillis: now + 101_000 });  // second: identical -> frozen
  assert.equal(attempts, 2);
  await source.fetch({ nowMillis: now + 300_000 });  // inside 600s frozen window
  assert.equal(attempts, 2, "frozen camera left alone for the longer window");
  await source.fetch({ nowMillis: now + 800_000 });
  assert.equal(attempts, 3);
});

test("requests to one host are spaced, not bursted by the worker pool", async () => {
  const times = [];
  const source = createCameraTrafficSource({
    cameras: cameras.slice(0, 4), analyze: flowing, maxCameras: 4, concurrency: 4,
    hostMinIntervalMillis: 60, minRevisitSeconds: 900,
    fetchImage: async camera => { times.push(Date.now()); return { base64: `f-${camera.id}`, mediaType: "image/jpeg" }; }
  });
  await source.fetch({ nowMillis: 1_000_000 });
  assert.equal(times.length, 4);
  const spread = times[times.length - 1] - times[0];
  assert.ok(spread >= 150, `four requests spread over ${spread}ms, expected >=150ms`);
});

test("a first unusable frame is not demoted — a differencing analyzer always produces one", async () => {
  // The pixel analyzer's first frame of any camera is unusable by
  // construction: that frame IS the background it will difference
  // against. Backing off there means the second frame never arrives and
  // a perfectly good camera looks dead forever.
  let attempts = 0;
  const source = createCameraTrafficSource({
    cameras: cameras.slice(0, 1), maxCameras: 1, concurrency: 1,
    hostMinIntervalMillis: 0, minRevisitSeconds: 100, quietRevisitMultiplier: 3,
    fetchImage: async () => ({ base64: `frame-${++attempts}`, mediaType: "image/jpeg" }),
    // Unusable first, then a real reading — exactly the differencing shape.
    analyze: async () => (attempts === 1
      ? { congestion: "unusable", confidence: 0.9 }
      : { congestion: "slow", confidence: 0.9 })
  });
  let now = 1_000_000;
  await source.fetch({ nowMillis: now });
  assert.equal(attempts, 1);
  // One unusable frame earns the ORDINARY gap (100s), not 300s.
  await source.fetch({ nowMillis: now + 101_000 });
  assert.equal(attempts, 2, "the second frame must arrive, or the camera can never start");
  const observed = await source.fetch({ nowMillis: now + 202_000 });
  assert.ok(source.stats.observations >= 1, "and it produces a reading once it has a background");
  assert.ok(observed !== undefined);
});

test("a focus box spends the budget where somebody is actually asking", async () => {
  // 20 cameras spread north; three sit inside the corridor being asked
  // about. A budget of two must land inside that box, not on whichever
  // happened to be stalest three regions away — and the budget still
  // caps the work, so demand raises priority without raising spend.
  const seen = [];
  const source = createCameraTrafficSource({
    cameras, analyze: async () => ({ congestion: "free", confidence: 0.9 }),
    maxCameras: 2, concurrency: 1, hostMinIntervalMillis: 0, minRevisitSeconds: 900,
    focus: () => [{ minLat: 45.515, minLon: -73.57, maxLat: 45.517, maxLon: -73.55 }],
    fetchImage: async camera => { seen.push(camera.id); return { base64: `f-${camera.id}`, mediaType: "image/jpeg" }; }
  });
  await source.fetch({ nowMillis: 1_000_000 });
  assert.equal(seen.length, 2, "the budget still caps the work");
  assert.ok(seen.every(id => ["cam-15", "cam-16", "cam-17"].includes(id)),
    `every request went inside the box, got ${seen.join(",")}`);
  assert.equal(source.stats.focusBoxes, 1);
  assert.equal(source.stats.inFocus, 3, "three were in the box; two fit the budget");
});

test("no demand signal falls back to plain rotation rather than stalling", async () => {
  const seen = [];
  const source = createCameraTrafficSource({
    cameras, analyze: async () => ({ congestion: "free", confidence: 0.9 }),
    maxCameras: 3, concurrency: 1, hostMinIntervalMillis: 0, minRevisitSeconds: 900,
    focus: () => [],   // nobody asking
    fetchImage: async camera => { seen.push(camera.id); return { base64: `f-${camera.id}`, mediaType: "image/jpeg" }; }
  });
  await source.fetch({ nowMillis: 1_000_000 });
  assert.equal(seen.length, 3, "a quiet mesh still surveys the map");
  assert.equal(source.stats.inFocus, 0);
});

test("a focus that throws does not take the survey down with it", async () => {
  const seen = [];
  const source = createCameraTrafficSource({
    cameras, analyze: async () => ({ congestion: "free", confidence: 0.9 }),
    maxCameras: 2, concurrency: 1, hostMinIntervalMillis: 0,
    focus: () => { throw new Error("demand signal unavailable"); },
    fetchImage: async camera => { seen.push(camera.id); return { base64: `f-${camera.id}`, mediaType: "image/jpeg" }; }
  });
  await source.fetch({ nowMillis: 1_000_000 });
  assert.equal(seen.length, 2);
});
