// The re-pricing decision: when a live re-price changes a drive.
//
// The interesting cases are all refusals. A navigator that took every
// answer the router gave it would re-plan continuously, because the
// router's answer is *always* at least as good as the one being driven —
// that is what makes it the router's answer. So most of what is pinned
// here is the reasons not to act: a saving too small to be worth a
// driver's attention, a "different" path that came back slower than the
// stale estimate it was compared against, a drive already over, and an
// engine that returned nothing at all.

import assert from "node:assert/strict";
import test from "node:test";
import {
  DEFAULT_REPRICE_POLICY,
  carriedVoice,
  livePathSeconds,
  pathOf,
  pathOverlap,
  remainingPath,
  repriceDecision,
  segmentsOf,
  sharedShare,
  shouldRepriceNow
} from "../src/nav_reprice.js";

/** A route as far as this module is concerned: edges carrying segment ids. */
const routeOf = (...segments) => ({ edges: segments.map(segment => ({ segment })) });

/** n segment ids on one imaginary road. */
const road = (prefix, n) => Array.from({ length: n }, (_, i) => `${prefix}/${i}/0`);

test("a route's segments are its edges' ids, in order, and nothing else", () => {
  const route = { edges: [{ segment: "1/0/0" }, { meters: 20 }, { segment: "1/1/0" }, null] };
  assert.deepEqual(segmentsOf(route), ["1/0/0", "1/1/0"]);
  assert.deepEqual(segmentsOf(null), [], "no route is no segments, not a throw");
  assert.deepEqual(segmentsOf({}), []);
});

test("sameness is asked of the candidate, not of the pair", () => {
  const current = road("a", 20);
  // The driver has not reached the end of the road yet: the candidate is
  // a short prefix of what is being driven. That is the same way.
  assert.equal(sharedShare(current, current.slice(0, 4)), 1);
  // The reverse is a detour: most of the candidate is road the driver is
  // not on.
  assert.equal(sharedShare(current.slice(0, 4), current), 4 / 20);
});

test("with nothing to compare, nothing counts as a different path", () => {
  assert.equal(sharedShare([], road("a", 5)), 1);
  assert.equal(sharedShare(road("a", 5), []), 1);
  assert.equal(sharedShare([], []), 1);
  // …and that flows through to the verdict: a re-price the engine could
  // not answer must never move the driver.
  const decision = repriceDecision({
    remainingSeconds: 1800, candidateSeconds: 60,
    currentSegments: road("a", 10), candidateSegments: []
  });
  assert.equal(decision.samePath, true);
  assert.notEqual(decision.action, "switch");
});

test("a materially faster different road is taken", () => {
  const decision = repriceDecision({
    remainingSeconds: 900,
    candidateSeconds: 700,                    // 200 s saved, floor is 60 s
    currentSegments: road("a", 20),
    candidateSegments: road("b", 20)
  });
  assert.equal(decision.action, "switch");
  assert.equal(decision.gain, 200);
  assert.equal(decision.samePath, false);
  assert.equal(decision.threshold, 60, "the floor binds on a short drive");
});

test("on a long drive the share binds, not the floor", () => {
  // Two hours left. Ninety seconds is over the 60 s floor and still not
  // worth re-planning a two-hour drive for.
  const decision = repriceDecision({
    remainingSeconds: 7200,
    candidateSeconds: 7110,
    currentSegments: road("a", 40),
    candidateSegments: road("b", 40)
  });
  assert.equal(decision.threshold, 7200 * DEFAULT_REPRICE_POLICY.gainShare);
  assert.equal(decision.action, "keep");
  assert.equal(decision.gain, 90);

  const worthIt = repriceDecision({
    remainingSeconds: 7200,
    candidateSeconds: 6700,                   // 500 s, over the 432 s share
    currentSegments: road("a", 40),
    candidateSegments: road("b", 40)
  });
  assert.equal(worthIt.action, "switch");
});

test("a different path that comes back slower is never taken", () => {
  // The drive's own estimate can be stale and optimistic, so the router
  // returning something "worse" is a real state — and switching to it
  // would be moving the driver for nothing.
  const decision = repriceDecision({
    remainingSeconds: 600,
    candidateSeconds: 900,
    currentSegments: road("a", 20),
    candidateSegments: road("b", 20)
  });
  assert.equal(decision.action, "keep");
  assert.equal(decision.gain, -300);
});

test("the same road at a materially different cost is refreshed, not switched", () => {
  const current = road("a", 20);
  const slower = repriceDecision({
    remainingSeconds: 600,
    candidateSeconds: 700,                    // the drive got 100 s longer
    currentSegments: current,
    candidateSegments: current.slice(2)
  });
  assert.equal(slower.action, "refresh", "a drive quietly getting longer is the thing to say");
  assert.equal(slower.gain, -100);
  assert.equal(slower.samePath, true);

  const faster = repriceDecision({
    remainingSeconds: 600, candidateSeconds: 500,
    currentSegments: current, candidateSegments: current.slice(2)
  });
  assert.equal(faster.action, "refresh");
  assert.equal(faster.gain, 100);
});

test("the same road at about the same cost is left alone", () => {
  const current = road("a", 20);
  const decision = repriceDecision({
    remainingSeconds: 600,
    candidateSeconds: 590,                    // 10 s, under the 45 s shift
    currentSegments: current,
    candidateSegments: current.slice(1)
  });
  assert.equal(decision.action, "keep");
  assert.equal(decision.gain, 10);
});

test("the thresholds are inclusive, so a value exactly on the line acts", () => {
  const onTheSwitchLine = repriceDecision({
    remainingSeconds: 600,
    candidateSeconds: 540,                    // exactly 60 s
    currentSegments: road("a", 20),
    candidateSegments: road("b", 20)
  });
  assert.equal(onTheSwitchLine.action, "switch");

  const current = road("a", 20);
  const onTheRefreshLine = repriceDecision({
    remainingSeconds: 600,
    candidateSeconds: 555,                    // exactly 45 s
    currentSegments: current,
    candidateSegments: current.slice(1)
  });
  assert.equal(onTheRefreshLine.action, "refresh");
});

test("counting edges calls two short joining edges a detour; counting metres does not", () => {
  const current = road("a", 20);
  // Re-planning from the driver's fix re-snaps and picks up a couple of
  // short edges that were not on the original line.
  const joining = ["join/0/0", "join/1/0"];
  const ids = [...current.slice(3), ...joining];

  // By edge count this is 17 of 19 — under the 0.9 line, and wrong.
  assert.ok(sharedShare(current, ids) < DEFAULT_REPRICE_POLICY.samePathShare);

  // The same candidate with distances: 17 × 300 m of road already being
  // driven, plus 2 × 30 m of slip road.
  const withMeters = ids.map(segment => ({
    segment,
    meters: joining.includes(segment) ? 30 : 300
  }));
  const overlap = pathOverlap(current, withMeters);
  assert.equal(overlap.weighted, true);
  assert.equal(overlap.unsharedMeters, 60);
  assert.ok(overlap.share > 0.98, `${overlap.share} of the distance is road already being driven`);

  const decision = repriceDecision({
    remainingSeconds: 900, candidateSeconds: 700,
    currentSegments: current, candidateSegments: withMeters
  });
  assert.equal(decision.samePath, true);
  assert.equal(decision.action, "refresh", "a big saving on the same road is still not a turn");
});

test("the last kilometre of a drive is not a detour", () => {
  // Near the end there is barely any road left, so the joining edges are
  // a large share of it — the case the metres floor exists for.
  const current = road("a", 3);
  const candidate = [
    { segment: "join/0/0", meters: 40 },
    { segment: "a/1/0", meters: 120 },
    { segment: "a/2/0", meters: 140 }
  ];
  const overlap = pathOverlap(current, candidate);
  assert.ok(overlap.share < DEFAULT_REPRICE_POLICY.samePathShare, "the share alone would call this a detour");
  assert.equal(overlap.unsharedMeters, 40);

  const decision = repriceDecision({
    remainingSeconds: 120, candidateSeconds: 50,   // 70 s, over the 60 s floor
    currentSegments: current, candidateSegments: candidate
  });
  assert.equal(decision.samePath, true, "40 m off the line is not a different way");
  assert.notEqual(decision.action, "switch", "so no turn is announced in the last stretch");
});

test("a genuine detour is still a detour, however long the drive", () => {
  const current = road("a", 20).map(segment => ({ segment, meters: 300 }));
  const detour = [
    { segment: "a/0/0", meters: 300 },
    { segment: "b/0/0", meters: 900 },
    { segment: "b/1/0", meters: 900 },
    ...road("a", 8).slice(4).map(segment => ({ segment, meters: 300 }))
  ];
  const overlap = pathOverlap(current.map(e => e.segment), detour);
  assert.equal(overlap.unsharedMeters, 1800, "well past the 150 m floor");
  assert.ok(overlap.share < DEFAULT_REPRICE_POLICY.samePathShare);

  const decision = repriceDecision({
    remainingSeconds: 900, candidateSeconds: 700,
    currentSegments: current.map(e => e.segment), candidateSegments: detour
  });
  assert.equal(decision.samePath, false);
  assert.equal(decision.action, "switch");
});

test("a candidate whose edges do not all carry metres falls back to counting them", () => {
  const current = road("a", 10);
  const mixed = [
    { segment: "a/1/0", meters: 300 },
    { segment: "a/2/0" },                       // no metres on this one
    { segment: "b/0/0", meters: 40 }
  ];
  const overlap = pathOverlap(current, mixed);
  assert.equal(overlap.weighted, false, "half-measured distances would weigh nothing against nothing");
  assert.ok(Number.isNaN(overlap.unsharedMeters), "and there is no honest metre count to report");
  assert.equal(overlap.share, 2 / 3);
});

test("a drive that is over, or an engine that said nothing, decides nothing", () => {
  const base = { currentSegments: road("a", 10), candidateSegments: road("b", 10) };
  for (const broken of [
    { remainingSeconds: 0, candidateSeconds: 100 },
    { remainingSeconds: -30, candidateSeconds: 100 },
    { remainingSeconds: 600, candidateSeconds: NaN },
    { remainingSeconds: NaN, candidateSeconds: 100 },
    { remainingSeconds: 600, candidateSeconds: -5 },
    { remainingSeconds: 600, candidateSeconds: undefined }
  ]) {
    const decision = repriceDecision({ ...base, ...broken });
    assert.equal(decision.action, "keep", `${JSON.stringify(broken)} must not move the driver`);
  }
  assert.equal(repriceDecision().action, "keep", "and neither must no arguments at all");
});

test("a caller's own policy overrides only what it names", () => {
  const args = {
    remainingSeconds: 600,
    candidateSeconds: 570,                    // 30 s
    currentSegments: road("a", 20),
    candidateSegments: road("b", 20)
  };
  assert.equal(repriceDecision(args).action, "keep");
  const eager = repriceDecision({ ...args, policy: { minGainSeconds: 20, gainShare: 0 } });
  assert.equal(eager.action, "switch", "a host that wants to be eager may be");
  // The unnamed fields still hold.
  assert.equal(
    repriceDecision({ ...args, policy: { minGainSeconds: 20 } }).threshold,
    600 * DEFAULT_REPRICE_POLICY.gainShare
  );
});

test("the part of a route still ahead prorates the edge being driven", () => {
  const route = { edges: [
    { segment: "a/0/0", meters: 100, seconds: 10 },
    { segment: "a/1/0", meters: 200, seconds: 20 },
    { segment: "a/2/0", meters: 100, seconds: 10 }
  ] };
  assert.deepEqual(remainingPath(route, 0).map(e => e.segment), ["a/0/0", "a/1/0", "a/2/0"]);
  // 150 m in: the first edge (0–100 m) is behind; the driver is 50 m into
  // the second, so three quarters of it — 150 m, 15 s — is still ahead.
  const part = remainingPath(route, 150);
  assert.deepEqual(part, [
    { segment: "a/1/0", meters: 150, seconds: 15 },
    { segment: "a/2/0", meters: 100, seconds: 10 }
  ]);
  assert.equal(part.reduce((sum, e) => sum + e.meters, 0), 250, "400 m of route, 150 m driven");
  assert.deepEqual(remainingPath(route, 10_000), [], "past the end there is nothing ahead");
  assert.deepEqual(remainingPath(null, 0), []);
});

test("a path costs what the router would charge it, using the router's own blend", () => {
  const ahead = [
    { segment: "a/0/0", meters: 300, seconds: 10 },
    { segment: "a/1/0", meters: 300, seconds: 10 }
  ];
  const nowMillis = 1_000_000;
  // Static free-flow would be 300 m in 10 s = 30 m/s. Observed at 5 m/s,
  // full confidence, observed now: six times the time on that edge.
  const states = [{ segment: "a/0/0", speedMps: 5, meters: 300, confidence: 1, observedAt: nowMillis }];
  assert.equal(Math.round(livePathSeconds(ahead, states, { nowMillis })), 70);

  // An edge nobody has measured keeps its static cost, exactly as the
  // router leaves it alone.
  assert.equal(livePathSeconds(ahead, [], { nowMillis }), 20);

  // A closed road is not "expensive", it is impassable.
  assert.equal(livePathSeconds(ahead, [{ segment: "a/1/0", closed: true }], { nowMillis }), Infinity);

  // An incident penalty is added on top of the blended time.
  const penalised = livePathSeconds(
    ahead,
    [{ segment: "a/0/0", speedMps: 30, meters: 300, confidence: 1, observedAt: nowMillis, penaltySeconds: 120 }],
    { nowMillis }
  );
  assert.ok(penalised > 130 && penalised < 145, `${penalised} is the static 20 s plus a 120 s penalty`);
});

test("confidence and age pull a reading back toward the static cost", () => {
  const ahead = [{ segment: "a/0/0", meters: 300, seconds: 10 }];
  const nowMillis = 1_000_000;
  const fresh = livePathSeconds(ahead, [
    { segment: "a/0/0", speedMps: 5, meters: 300, confidence: 1, observedAt: nowMillis }
  ], { nowMillis });
  const halfSure = livePathSeconds(ahead, [
    { segment: "a/0/0", speedMps: 5, meters: 300, confidence: 0.5, observedAt: nowMillis }
  ], { nowMillis });
  const stale = livePathSeconds(ahead, [
    { segment: "a/0/0", speedMps: 5, meters: 300, confidence: 1, observedAt: nowMillis - 180_000 }
  ], { nowMillis });
  assert.ok(fresh > halfSure && halfSure > stale, `${fresh} > ${halfSure} > ${stale}`);
  assert.ok(stale < 15, "a three-minute-old reading barely moves the cost");
});

test("a diversion is judged against the jammed road, not against a stale belief", () => {
  // The case that matters, and the one the first cut of this got wrong.
  // The drive believes 790 s are left. A jam forms; the road actually
  // ahead now costs 1904 s and the best way round costs 976 s.
  const believed = repriceDecision({
    remainingSeconds: 790,
    candidateSeconds: 991,                    // the diversion's *static* total
    currentSegments: road("a", 30),
    candidateSegments: road("b", 30)
  });
  assert.equal(believed.action, "keep", "compared to a stale belief the diversion looks worse");
  assert.equal(believed.comparedAgainst, "belief");

  const measured = repriceDecision({
    remainingSeconds: 790,
    candidateSeconds: 991,
    currentLiveSeconds: 1904,
    candidateLiveSeconds: 976,
    currentSegments: road("a", 30),
    candidateSegments: road("b", 30)
  });
  assert.equal(measured.action, "switch", "priced honestly it saves fifteen minutes");
  assert.equal(measured.comparedAgainst, "live");
  assert.equal(measured.gain, 1904 - 976);
});

test("a closed road on the way ahead is the strongest reason there is to switch", () => {
  const decision = repriceDecision({
    remainingSeconds: 600,
    candidateSeconds: 1200,
    currentLiveSeconds: Infinity,             // livePathSeconds on a shut road
    candidateLiveSeconds: 1300,
    currentSegments: road("a", 20),
    candidateSegments: road("b", 20)
  });
  assert.equal(decision.action, "switch");
  assert.equal(decision.gain, Infinity);
  assert.equal(decision.comparedAgainst, "live");
});

test("one live figure is not enough to compare with; it takes both or neither", () => {
  const args = {
    remainingSeconds: 900, candidateSeconds: 800,
    currentSegments: road("a", 20), candidateSegments: road("b", 20)
  };
  for (const partial of [
    { currentLiveSeconds: null, candidateLiveSeconds: 700 },
    { currentLiveSeconds: 1500, candidateLiveSeconds: null },
    { currentLiveSeconds: null, candidateLiveSeconds: null }
  ]) {
    const decision = repriceDecision({ ...args, ...partial });
    assert.equal(decision.comparedAgainst, "belief", JSON.stringify(partial));
    assert.equal(decision.gain, decision.etaShift, "the fallback is the reported pair, both in one unit");
  }

  // The dangerous half specifically: knowing only that the road ahead is
  // jammed (1500 s) while the candidate is still a static 800 s would
  // read as a 700 s saving out of thin air. It must not.
  const halfKnown = repriceDecision({ ...args, currentLiveSeconds: 1500 });
  assert.notEqual(halfKnown.gain, 700);
  assert.equal(halfKnown.gain, 100);

  // …and `null` must never arrive as 0, which would make the road being
  // driven instantaneous and every alternative look catastrophic.
  const bothNull = repriceDecision({ ...args, currentLiveSeconds: null, candidateLiveSeconds: null });
  assert.ok(bothNull.gain > 0, "a null current path is not a free one");
});

test("the same road quietly getting slower is announced, not hidden", () => {
  // The router found nothing better, so both live figures agree — but
  // the driver is still looking at a number that is three minutes out.
  const current = road("a", 20);
  const decision = repriceDecision({
    remainingSeconds: 295,
    candidateSeconds: 295,
    currentLiveSeconds: 482,
    candidateLiveSeconds: 482,
    currentSegments: current,
    candidateSegments: current
  });
  assert.equal(decision.samePath, true);
  assert.equal(decision.gain, 0, "there is nothing to switch to");
  assert.equal(decision.etaShift, 295 - 482, "but the ETA on screen is wrong by that much");
  assert.equal(decision.action, "refresh");
});

test("voice carries across a re-plan on the instruction, never on the index", () => {
  // The same turn, renumbered by re-planning from the current position.
  const carried = carriedVoice({
    previousPhrase: "turn left onto Rue Notre-Dame",
    upcomingPhrase: "turn left onto Rue Notre-Dame",
    previousLevels: 3,
    boundary: 1
  });
  assert.deepEqual(carried, { boundary: 1, levels: 3, phrase: "turn left onto Rue Notre-Dame" });

  // A different turn is a different thing to say.
  assert.equal(carriedVoice({
    previousPhrase: "turn left onto Rue Notre-Dame",
    upcomingPhrase: "turn right onto Avenue Laurier",
    previousLevels: 3
  }), null);

  // Nothing announced yet, or nothing coming up, carries nothing.
  assert.equal(carriedVoice({ upcomingPhrase: "turn left" }), null);
  assert.equal(carriedVoice({ previousPhrase: "turn left" }), null);
  assert.equal(carriedVoice(), null);
});

test("only a corridor getting worse interrupts a drive", () => {
  assert.equal(shouldRepriceNow({ worsened: [{ segKey: "1/2" }], improved: [] }), true);
  assert.equal(shouldRepriceNow({ worsened: [], improved: [{ segKey: "1/2" }] }), false,
    "good news waits for the timer rather than spending the driver's attention");
  assert.equal(shouldRepriceNow({ worsened: [], improved: [] }), false);
  assert.equal(shouldRepriceNow(null), false);
  assert.equal(shouldRepriceNow(undefined), false);
});

test("the decision is reachable from the route engine's own module", async () => {
  // The browser demo loads the query engine and nothing else; the policy
  // has to arrive with it or the host inlines its own copy, which is
  // where this logic was living before.
  const engine = await import("../src/route_graph_query.js");
  assert.equal(typeof engine.repriceDecision, "function");
  assert.equal(typeof engine.segmentsOf, "function");
  assert.equal(typeof engine.carriedVoice, "function");
  assert.equal(typeof engine.shouldRepriceNow, "function");
  assert.equal(engine.DEFAULT_REPRICE_POLICY.minGainSeconds, 60);
});

test("a real pair of routes decides the way the segment lists do", () => {
  const shared = routeOf(...road("a", 10));
  const detour = routeOf(...road("b", 10));
  const same = repriceDecision({
    remainingSeconds: 900, candidateSeconds: 600,
    currentSegments: segmentsOf(shared), candidateSegments: pathOf(shared)
  });
  assert.equal(same.action, "refresh");
  const different = repriceDecision({
    remainingSeconds: 900, candidateSeconds: 600,
    currentSegments: segmentsOf(shared), candidateSegments: pathOf(detour)
  });
  assert.equal(different.action, "switch");
});

test("pathOf carries the distances the comparison wants; segmentsOf does not", () => {
  const route = { edges: [{ segment: "1/0/0", meters: 240 }, { segment: "1/1/0", meters: 60 }] };
  assert.deepEqual(pathOf(route), [
    { segment: "1/0/0", meters: 240 },
    { segment: "1/1/0", meters: 60 }
  ]);
  assert.equal(pathOverlap(["1/0/0"], pathOf(route)).unsharedMeters, 60);
  assert.ok(Number.isNaN(pathOverlap(["1/0/0"], segmentsOf(route)).unsharedMeters));
  assert.deepEqual(pathOf(null), []);
});
