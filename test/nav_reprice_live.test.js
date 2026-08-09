// End to end: a jam forms on the road a driver is on, the mesh notices,
// and the drive is re-planned around it.
//
// Every piece here is the real one — a real static route graph, a real
// mesh session, real PMC1 records published through the ingest publisher
// and validated on the way in, the router's own live blend, and the
// shipped decision policy. Nothing is stubbed. The point is to prove the
// whole chain fires, because each part passing its own unit tests did
// not stop the first two versions of this from silently never diverting:
//
//   - the first compared the diversion against the drive's *stale*
//     belief, so the jam that prompted the re-price also made every
//     alternative look worse than the belief, and the answer was always
//     "keep";
//   - the second fixed that by pricing the current path under live data
//     — and compared it against `route.seconds`, which is the *static*
//     total of the chosen path. Two units, and every jam then looked
//     like a reason to divert.
//
// Both bugs pass any test that only looks at one side.

import assert from "node:assert/strict";
import test from "node:test";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { createMeshSession } from "../src/pulsemesh/session.js";
import { createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { createIngestPublisher } from "../src/pulsemesh/ingest.js";
import {
  livePathSeconds,
  pathOf,
  remainingPath,
  repriceDecision,
  segmentsOf,
  shouldRepriceNow
} from "../src/nav_reprice.js";
import { parseSegment } from "../src/pulsemesh/codec.js";

const GRAPH_DIR = "examples/osm-geo/public/route-graph";

async function graphFixture(t) {
  try {
    return await openRouteGraphDir(GRAPH_DIR);
  } catch {
    t.skip(`route graph fixture missing at ${GRAPH_DIR}`);
    return null;
  }
}

const centreOf = (engine, leaf) => {
  const bbox = engine.root.leaves[leaf].bbox;
  return { lat: (bbox.minLat + bbox.maxLat) / 2 / 1e7, lon: (bbox.minLon + bbox.maxLon) / 2 / 1e7 };
};

/**
 * Drive `from`→`to`, jam the given share of the way ahead, and run one
 * whole re-price cycle over it. Returns everything the assertions need.
 *
 * The clock is virtual but anchored to now, because live confidence
 * decays against wall-clock age — a clock in the past would decay every
 * jam to nothing and quietly prove the opposite of what is intended.
 */
async function driveIntoAJam(engine, { fromLeaf, toLeaf, window = [0.3, 0.6], jamKmh = 3 }) {
  let now = Date.now();
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });
  const session = await createMeshSession({
    engine, network, id: "driver", transport: "loopback", clock
  });

  const from = centreOf(engine, fromLeaf);
  const to = centreOf(engine, toLeaf);
  const driving = await engine.route({ from, to });
  await session.followRoute(driving);

  // The corridor watch the drive would have running.
  const fired = [];
  const watch = session.watchRoute(driving, {
    onChange: change => fired.push(change),
    debounceSeconds: 0
  });
  await session.tick(now);              // baseline: nothing known yet

  // Somebody else's cars, stopped, on the middle of our route.
  const edges = (driving.edges || []).filter(edge => edge?.segment);
  const jammed = edges.slice(
    Math.floor(edges.length * window[0]),
    Math.floor(edges.length * window[1])
  );
  const observations = [];
  for (const edge of jammed) {
    const { leafCell, geomRef } = parseSegment(edge.segment);
    if (!(session.freeflowKmhOf(`${leafCell}/${geomRef}`) > 0)) continue;
    const at = await engine.locate(edge.segment, 0.5).catch(() => null);
    if (at) observations.push({ kind: "flow", lat: at.lat, lon: at.lon, speedKmh: jamKmh });
  }

  const ingest = createIngestPublisher({
    engine, session, clock,
    sources: [{ id: "jam", intervalSeconds: 99_999, fetch: async () => observations }]
  });
  // The publisher paces itself under the receivers' rate bucket, so a
  // burst this size needs several beats to clear.
  for (let beat = 0; beat < 60 && (beat === 0 || ingest.queueLength); beat++) {
    await ingest.tick(now);
    now += 2000;
  }
  await session.tick(now);              // the beat that spots the change

  // One re-price, exactly as a host would run it.
  const candidate = await engine.route({ from, to, live: session.provider() });
  const states = await session.provider().fetch({
    epoch: session.epoch, areas: [], maxAgeSeconds: 120
  });
  const decision = repriceDecision({
    remainingSeconds: driving.seconds,
    candidateSeconds: candidate.seconds,
    currentLiveSeconds: livePathSeconds(remainingPath(driving, 0), states, { nowMillis: now }),
    candidateLiveSeconds: livePathSeconds(remainingPath(candidate, 0), states, { nowMillis: now }),
    currentSegments: segmentsOf(driving),
    candidateSegments: pathOf(candidate)
  });

  await session.close();
  return { driving, candidate, decision, fired, watch, states, published: ingest.stats.publishedSegments };
}

test("a jam on the corridor is seen by the mesh and re-plans the drive around it", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;

  const { driving, candidate, decision, fired, states, published } =
    await driveIntoAJam(engine, { fromLeaf: 28, toLeaf: 39 });

  // 1. The jam really was published as mesh records, not injected.
  assert.ok(published > 10, `${published} segments of jam reached the mesh`);
  assert.ok(states.length > 10, "and came back out of the provider as live states");

  // 2. The corridor watch noticed, and said it was worse.
  assert.equal(fired.length, 1, "the watch fired exactly once for this jam");
  assert.ok(fired[0].worsened.length > 0, "and reported it as a worsening");
  assert.equal(fired[0].improved.length, 0);
  assert.equal(shouldRepriceNow(fired[0]), true, "which is what triggers a re-price");

  // 3. The router found a genuinely different way.
  assert.equal(decision.samePath, false);
  assert.ok(decision.unsharedMeters > 1000, `${Math.round(decision.unsharedMeters)} m of new road`);

  // 4. And the decision took it, on an honest comparison.
  assert.equal(decision.action, "switch");
  assert.equal(decision.comparedAgainst, "live");
  assert.ok(decision.gain >= decision.threshold);

  // 5. The saving is real: the road ahead genuinely costs more than the
  //    way round, under the same observations.
  const before = livePathSeconds(remainingPath(driving, 0), states, { nowMillis: Date.now() });
  assert.ok(before > candidate.seconds, "the jammed way ahead costs more than the diversion's static total");
  assert.ok(decision.gain > 60, `${Math.round(decision.gain)} s saved`);
});

test("the same jam decides nothing without live data, which is the bug to guard", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  const { driving, candidate, decision } = await driveIntoAJam(engine, { fromLeaf: 28, toLeaf: 39 });
  assert.equal(decision.action, "switch", "with both live figures it diverts");

  // The first version of this compared the diversion against the drive's
  // own stale belief. The jam makes every alternative slower than that
  // belief, so the verdict was always "keep" — a navigator that drives
  // its user into every jam it can see.
  const naive = repriceDecision({
    remainingSeconds: driving.seconds,
    candidateSeconds: candidate.seconds,
    currentSegments: segmentsOf(driving),
    candidateSegments: pathOf(candidate)
  });
  assert.equal(naive.comparedAgainst, "belief");
  assert.equal(naive.action, "keep", "which is exactly the failure this pins");
  assert.ok(naive.gain < 0, "the diversion looks slower than a belief formed before the jam");
});

test("a jam with no way round tells the driver rather than inventing a detour", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  // A short corridor with no useful alternative: the router comes back
  // with the same road, so there is nothing to switch to — but the ETA
  // the driver is looking at is now minutes wrong.
  const { decision } = await driveIntoAJam(engine, { fromLeaf: 47, toLeaf: 58 });

  assert.equal(decision.samePath, true, "the router found no better way");
  assert.equal(decision.gain, 0, "so there is nothing to gain by moving");
  assert.ok(decision.etaShift < -60, `the drive is ${Math.round(-decision.etaShift)} s longer than shown`);
  assert.equal(decision.action, "refresh", "which is told, not hidden");
});
