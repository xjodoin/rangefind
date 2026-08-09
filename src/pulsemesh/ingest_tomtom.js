// PulseMesh backfill — a commercial traffic API, asked only when the
// mesh cannot answer, and only where the answer could change a route.
//
// The bootstrap problem in one line: a cold region has no contributors,
// so every itinerary through it is priced from the static graph alone.
// A paid flow API can fill that in, but paying for a whole region
// continuously is the opposite of what a mesh is for — and most of what
// you would buy is a free-flowing road, which is exactly what the static
// metric already says.
//
// So this module buys nothing on a schedule. It is *demand-driven*: you
// hand it an itinerary somebody actually asked for, it works out which
// of that itinerary's edges the mesh cannot currently price, discards
// the ones where a live speed could not change the routing answer, and
// probes only what is left. What comes back is published into the mesh
// as ordinary PMC1 records, so the second person to ask about that
// corridor is served by the mesh and costs nothing.
//
// The gates, in the order they cut:
//
//  1. Mesh sufficiency. An edge whose aggregate already has
//     AGG_MIN_REPORTS fresh reports is answered. Below that the protocol
//     itself calls the aggregate a confidence-capped hint (§8.3), which
//     is the honest line between "we know" and "we are guessing".
//  2. Recently asked. Every probe's answer is cached per segment for
//     CONTRIB_TTL, so a corridor is bought at most once per TTL however
//     many itineraries cross it. Failures are cached too, shorter — a
//     road the API cannot match must not be re-asked every request.
//  3. Class. Only the classes where congestion changes which way the
//     router goes. A jammed residential street is still the fastest way
//     through that block; buying it burns quota to learn nothing.
//  4. Length. Contiguous gap edges of the same class are merged into
//     runs and split into chunks, and a chunk shorter than
//     `minChunkMeters` is dropped — a live speed over 40 m of connector
//     cannot move a decision.
//  5. Budget. A per-route probe cap, a per-route published-segment cap,
//     a rolling daily cap, and a minimum interval between calls.
//
// What survives all five is ranked by length × class weight and the top
// `maxProbesPerRoute` are asked. Being wrong on a motorway costs more
// than being wrong on a secondary, so the motorway is bought first.
//
// TWO THINGS THIS MODULE TAKES SERIOUSLY:
//
// - Attribution. A probe answers for the chunk it was taken in, and
//   nothing further. It is tempting to spread one reading over the
//   whole run — it would look like far better coverage for the same
//   money — but that publishes a speed nobody measured onto road nobody
//   looked at, under this operator's bond. Chunks are therefore bounded
//   by `maxChunkMeters` and a reading never leaves its chunk.
//
// - Map-match disagreement. The API is asked for a point; it answers
//   for whatever road *it* matched there, which on a divided highway
//   beside a service road is routinely not our edge. When the returned
//   free-flow disagrees with the segment's own by more than
//   `maxFreeflowMismatch`, the reading is dropped as a mismatch rather
//   than published. A wrong record costs -500 trust at every receiver
//   (§6 rules 10-12) and, long before that, lies to a driver.
//
// LICENSING IS THE CALLER'S PROBLEM, AND IT IS A REAL ONE. Records
// published here propagate peer-to-peer and are cached by every
// receiver for CONTRIB_TTL. That is redistribution of the vendor's
// traffic data, and standard commercial traffic-API terms restrict
// exactly that. Check your agreement before pointing this at a mesh
// other people are on. The module is deliberately provider-agnostic —
// `probe` is a function — so an authorized feed can be swapped in
// without touching the gating.
//
// Usage:
//
//   const probe = createTomTomProbe({ key: process.env.TOMTOM_KEY });
//   const backfill = createRouteBackfill({ engine, session, probe });
//   const ingest = createIngestPublisher({
//     engine, session, sources: [backfill.source]
//   });
//
//   const route = await engine.route({ from, to, live: session.provider() });
//   const report = await backfill.backfill(route);
//   // report.states is usable immediately, as a LiveTrafficProvider, for
//   // a second pass over this same itinerary. The mesh copies take a
//   // publish cycle to land and are for whoever asks next.

import { DEFAULT_CONSTANTS, bucketAgeSeconds } from "./bins.js";
import { DENIED_CLASSES } from "./validate.js";
import { parseSegment } from "./codec.js";

/**
 * Classes worth buying a live speed for. Everything below secondary is
 * excluded on purpose: congestion there is real but it rarely changes
 * which way the router goes, and it is where a quota evaporates.
 */
export const DEFAULT_PROBE_CLASSES = Object.freeze([
  "motorway", "motorway_link", "trunk", "trunk_link",
  "primary", "primary_link", "secondary", "secondary_link"
]);

// Ranking weight when there are more gaps than budget. Being wrong about
// a motorway costs more than being wrong about a secondary, so the
// motorway is bought first.
const CLASS_WEIGHT = Object.freeze({
  motorway: 4, trunk: 3.5, primary: 3, secondary: 2,
  motorway_link: 2, trunk_link: 2, primary_link: 1.5, secondary_link: 1.2
});

const DAY_MILLIS = 24 * 60 * 60 * 1000;

function wrappedHeadingDelta(a, b) {
  const delta = Math.abs(Number(a) - Number(b)) % 360;
  return delta > 180 ? 360 - delta : delta;
}

// --- The TomTom probe ------------------------------------------------------

/**
 * TomTom Traffic Flow Segment Data as a probe function.
 *
 * Returns `({ lat, lon }) -> { currentSpeedKmh, freeFlowSpeedKmh,
 * confidence, roadClosure, frc }` or null when the API has nothing for
 * that point. Calls are serialized with `minIntervalMillis` between
 * them: a backfill burst is still a burst, and a paid endpoint is not
 * somewhere to discover your own rate limit.
 *
 * `zoom` is the road-detail level the point is matched at — 10 is about
 * right for motorways and arterials, higher pulls in smaller roads. It
 * is the single biggest lever on whether the API matches the road you
 * meant, which is why the caller can set it.
 */
export function createTomTomProbe({
  key,
  baseUrl = "https://api.tomtom.com",
  style = "absolute",
  zoom = 10,
  fetchImpl = null,
  timeoutMillis = 8000,
  minIntervalMillis = 120,
  onStatus = null
} = {}) {
  if (!key) throw new Error("createTomTomProbe needs a TomTom API key.");
  const doFetch = fetchImpl || globalThis.fetch;
  if (typeof doFetch !== "function") throw new Error("No fetch() on this host; pass fetchImpl.");

  const stats = { calls: 0, ok: 0, empty: 0, errors: 0, lastError: null, lastStatus: null };
  let chain = Promise.resolve();
  let lastCallMillis = 0;

  async function call({ lat, lon }) {
    const wait = Math.max(0, lastCallMillis + minIntervalMillis - Date.now());
    if (wait > 0) await new Promise(resolve => setTimeout(resolve, wait));
    lastCallMillis = Date.now();

    const url = `${baseUrl}/traffic/services/4/flowSegmentData/${style}/${zoom}/json`
      + `?key=${encodeURIComponent(key)}`
      + `&point=${lat.toFixed(6)},${lon.toFixed(6)}`
      + `&unit=kmph`;   // the documented spelling; also the API's default
    stats.calls++;
    const response = await doFetch(url, { signal: AbortSignal.timeout(timeoutMillis) });
    stats.lastStatus = response.status;
    if (onStatus) onStatus({ status: response.status, lat, lon });
    if (!response.ok) {
      stats.errors++;
      stats.lastError = `HTTP ${response.status}`;
      // A 403 here is an answer — a key that is not entitled, or a
      // quota that is spent. Report it; never retry around it.
      throw new Error(`TomTom flowSegmentData: HTTP ${response.status}`);
    }
    const body = await response.json();
    const data = body?.flowSegmentData;
    if (!data || !Number.isFinite(Number(data.currentSpeed))) {
      stats.empty++;
      return null;
    }
    stats.ok++;
    return {
      currentSpeedKmh: Number(data.currentSpeed),
      freeFlowSpeedKmh: Number(data.freeFlowSpeed),
      confidence: Number.isFinite(Number(data.confidence)) ? Number(data.confidence) : null,
      roadClosure: data.roadClosure === true,
      frc: data.frc ?? null
    };
  }

  const probe = point => {
    const result = chain.then(() => call(point));
    // Keep the chain alive past a rejection, but never swallow the
    // rejection the caller is awaiting.
    chain = result.then(() => {}, () => {});
    return result;
  };
  probe.stats = stats;
  return probe;
}

// --- The backfill ----------------------------------------------------------

/**
 * Binds a probe to a route graph and a mesh session as a lazy gap-filler.
 *
 * - `engine` / `session`: as for the ingest publisher. The session
 *   supplies both halves of the sufficiency test — `provider()` for what
 *   the mesh currently holds, `cellContext` for each edge's class and
 *   free-flow.
 * - `probe`: `({ lat, lon }) -> reading | null`. `createTomTomProbe`
 *   makes one; any authorized feed with a point-query can be swapped in.
 * - `minReports`: reports below which the mesh is judged insufficient.
 *   Default AGG_MIN_REPORTS — the protocol's own line (§8.3) between an
 *   aggregate and a confidence-capped hint.
 * - `maxFreeflowMismatch`: reject a reading whose free-flow disagrees
 *   with the segment's own by more than this fraction. This is the
 *   map-match check; see the header.
 * - `shouldProbe({ chunk, route, nowMillis })`: optional final veto, for
 *   policy this module has no business guessing — quiet hours, a
 *   region's own known-quiet corridors, a per-user budget.
 *
 * Returns `{ source, assess, backfill, stats, ... }`. Hand `source` to
 * `createIngestPublisher`; it is a drain for what `backfill()` queues,
 * not a poller, so it costs nothing when nobody is asking.
 */
export function createRouteBackfill({
  engine,
  session,
  probe,
  id = "tomtom-backfill",
  constants = DEFAULT_CONSTANTS,
  minReports = DEFAULT_CONSTANTS.AGG_MIN_REPORTS,
  maxAgeSeconds = DEFAULT_CONSTANTS.DISPLAY_MAX_AGE,
  probeClasses = DEFAULT_PROBE_CLASSES,
  minChunkMeters = 120,
  maxChunkMeters = 800,
  maxProbesPerRoute = 8,
  maxSegmentsPerRoute = 24,
  cacheSeconds = DEFAULT_CONSTANTS.CONTRIB_TTL,
  failureCacheSeconds = 300,
  dailyProbeBudget = 2500,
  minConfidence = 0.5,
  maxFreeflowMismatch = 0.5,
  maxSnapMeters = 40,
  shouldProbe = null,
  clock = Date.now
} = {}) {
  if (!engine?.route) throw new Error("Backfill needs an open route graph.");
  if (!session) throw new Error("Backfill needs a mesh session.");
  if (typeof probe !== "function") throw new Error("Backfill needs a probe({lat,lon}) function.");

  const wanted = new Set(probeClasses);
  const pending = [];                 // observations awaiting the publisher's drain
  const cache = new Map();            // segKey -> { atMillis, ttlSeconds, reading }

  const spendTimes = [];              // millis of each probe actually issued
  let lastProbeMillis = 0;

  const stats = {
    assessments: 0, backfills: 0,
    edgesConsidered: 0, meshCovered: 0, cacheCovered: 0,
    offClass: 0, noContext: 0, gapEdges: 0,
    chunks: 0, chunksTooShort: 0, vetoed: 0,
    probesIssued: 0, probesEmpty: 0, probeErrors: 0,
    lowConfidence: 0, freeflowMismatch: 0,
    budgetBlocked: 0, routeCapped: 0,
    closures: 0, observationsQueued: 0, segmentsCapped: 0,
    lastError: null
  };

  // --- Budget ------------------------------------------------------------

  function spendable(nowMillis) {
    while (spendTimes.length && nowMillis - spendTimes[0] > DAY_MILLIS) spendTimes.shift();
    return dailyProbeBudget - spendTimes.length;
  }

  // --- Cache -------------------------------------------------------------

  function cached(segKey, nowMillis) {
    const entry = cache.get(segKey);
    if (!entry) return null;
    if (nowMillis - entry.atMillis > entry.ttlSeconds * 1000) {
      cache.delete(segKey);
      return null;
    }
    return entry;
  }

  function remember(segKeys, reading, nowMillis) {
    const ttlSeconds = reading ? cacheSeconds : failureCacheSeconds;
    for (const segKey of segKeys) cache.set(segKey, { atMillis: nowMillis, ttlSeconds, reading });
  }

  // --- Assessment --------------------------------------------------------

  /**
   * What the mesh cannot answer on this itinerary, and what it would
   * cost to find out. Pure: issues no probes and spends no budget, so a
   * caller can render "12 of 40 km unpriced, 3 probes" before deciding.
   */
  async function assess(route, { nowMillis = clock() } = {}) {
    stats.assessments++;
    const routeEdges = route?.edges || [];
    const leaves = new Set();
    for (const edge of routeEdges) {
      if (edge?.segment) leaves.add(parseSegment(edge.segment).leafCell);
    }
    if (leaves.size) await session.warmLeaves([...leaves]);

    const provider = session.provider?.() ?? null;
    const aggregates = provider ? provider.aggregates(nowMillis) : new Map();

    const entries = [];
    for (const edge of routeEdges) {
      if (!edge?.segment) continue;
      const { leafCell, geomRef } = parseSegment(edge.segment);
      const segKey = `${leafCell}/${geomRef}`;
      const meters = Number(edge.meters) || 0;
      stats.edgesConsidered++;

      const context = session.cellContext(leafCell);
      const roadClass = context?.classOf(geomRef) ?? null;
      const freeflowKmh = session.freeflowKmhOf(segKey);
      const entry = {
        segment: edge.segment, segKey, leafCell, geomRef,
        roadClass, freeflowKmh, meters, gap: false, reason: null
      };
      entries.push(entry);

      if (!roadClass || !(freeflowKmh > 0)) { entry.reason = "no context"; stats.noContext++; continue; }
      if (DENIED_CLASSES.has(roadClass) || !wanted.has(roadClass)) {
        entry.reason = "off class"; stats.offClass++; continue;
      }
      const aggregate = aggregates.get(segKey);
      if (aggregate && aggregate.n >= minReports
          && bucketAgeSeconds(aggregate.newestBucket, nowMillis) <= maxAgeSeconds) {
        entry.reason = "mesh"; stats.meshCovered++; continue;
      }
      if (cached(segKey, nowMillis)) { entry.reason = "cached"; stats.cacheCovered++; continue; }
      entry.gap = true;
      stats.gapEdges++;
    }

    // Contiguous same-class gaps become runs; runs become bounded chunks.
    const runs = [];
    let run = null;
    for (const entry of entries) {
      if (!entry.gap) { run = null; continue; }
      if (!run || run.roadClass !== entry.roadClass) {
        run = { roadClass: entry.roadClass, edges: [], meters: 0 };
        runs.push(run);
      }
      run.edges.push(entry);
      run.meters += entry.meters;
    }

    const chunks = [];
    for (const each of runs) {
      let chunk = null;
      for (const entry of each.edges) {
        if (chunk && chunk.meters + entry.meters > maxChunkMeters) chunk = null;
        if (!chunk) {
          chunk = { roadClass: each.roadClass, edges: [], meters: 0 };
          chunks.push(chunk);
        }
        chunk.edges.push(entry);
        chunk.meters += entry.meters;
      }
    }

    const usable = [];
    for (const chunk of chunks) {
      stats.chunks++;
      if (chunk.meters < minChunkMeters) { stats.chunksTooShort++; continue; }
      if (shouldProbe && !shouldProbe({ chunk, route, nowMillis })) { stats.vetoed++; continue; }
      chunk.weight = chunk.meters * (CLASS_WEIGHT[chunk.roadClass] ?? 1);
      usable.push(chunk);
    }
    usable.sort((a, b) => b.weight - a.weight);
    const selected = usable.slice(0, maxProbesPerRoute);
    if (usable.length > selected.length) stats.routeCapped += usable.length - selected.length;

    const gapMeters = entries.reduce((sum, e) => sum + (e.gap ? e.meters : 0), 0);
    const totalMeters = entries.reduce((sum, e) => sum + e.meters, 0);
    return {
      entries,
      chunks: usable,
      probes: selected,
      gapMeters,
      totalMeters,
      budgetRemaining: spendable(nowMillis)
    };
  }

  // --- Probing -----------------------------------------------------------

  /** The chunk's midpoint by distance along it, as a locatable point. */
  function midpointOf(chunk) {
    let target = chunk.meters / 2;
    for (let i = 0; i < chunk.edges.length; i++) {
      const entry = chunk.edges[i];
      if (target <= entry.meters || i === chunk.edges.length - 1) {
        const ratio = entry.meters > 0 ? Math.max(0, Math.min(1, target / entry.meters)) : 0.5;
        return { entry, ratio };
      }
      target -= entry.meters;
    }
    return { entry: chunk.edges[0], ratio: 0.5 };
  }

  /**
   * The heading of the edge we mean, so the observation snaps back onto
   * the carriageway it was taken for. Without it a probe on a divided
   * highway is a coin flip between the two directions.
   */
  async function bearingOf(segment, point) {
    try {
      const snapped = await engine.snap(point, { maxSnapMeters });
      return snapped.matches.find(match => match.segment === segment)?.bearingDeg ?? null;
    } catch {
      return null;
    }
  }

  /**
   * Fill what the mesh cannot answer on this itinerary.
   *
   * Queues mesh observations for the publisher's next drain, and returns
   * `states` in LiveSegmentState shape for immediate use on this same
   * route — the mesh copies are for whoever asks next, not for the
   * request that paid for them.
   */
  async function backfill(route, { nowMillis = clock() } = {}) {
    stats.backfills++;
    const plan = await assess(route, { nowMillis });
    const states = [];
    const probed = [];
    let publishedSegments = 0;

    for (const chunk of plan.probes) {
      if (spendable(clock()) <= 0) { stats.budgetBlocked++; break; }
      if (publishedSegments >= maxSegmentsPerRoute) { stats.segmentsCapped++; break; }

      const { entry, ratio } = midpointOf(chunk);
      let point;
      try {
        point = await engine.locate(entry.segment, ratio);
      } catch {
        continue;
      }
      const bearingDeg = await bearingOf(entry.segment, point);

      const issuedAt = clock();
      spendTimes.push(issuedAt);
      lastProbeMillis = issuedAt;
      stats.probesIssued++;
      let reading;
      try {
        reading = await probe({ lat: point.lat, lon: point.lon, bearingDeg });
      } catch (error) {
        stats.probeErrors++;
        stats.lastError = String(error?.message || error);
        remember(chunk.edges.map(e => e.segKey), null, clock());
        continue;
      }
      const segKeys = chunk.edges.map(e => e.segKey);
      if (!reading) {
        stats.probesEmpty++;
        remember(segKeys, null, clock());
        continue;
      }
      if (Number.isFinite(reading.confidence) && reading.confidence < minConfidence) {
        stats.lowConfidence++;
        remember(segKeys, null, clock());
        continue;
      }
      // Map-match check: a free-flow far from the segment's own means
      // the API answered for a different road at this point.
      const ours = entry.freeflowKmh;
      const theirs = Number(reading.freeFlowSpeedKmh);
      if (ours > 0 && Number.isFinite(theirs) && theirs > 0
          && Math.abs(theirs - ours) / ours > maxFreeflowMismatch) {
        stats.freeflowMismatch++;
        remember(segKeys, null, clock());
        continue;
      }

      remember(segKeys, reading, clock());
      probed.push({ chunk, point, bearingDeg, reading });

      const observedAtMillis = clock();
      const speedKmh = reading.currentSpeedKmh;
      for (const each of chunk.edges) {
        if (publishedSegments >= maxSegmentsPerRoute) { stats.segmentsCapped++; break; }
        publishedSegments++;
        const at = await engine.locate(each.segment, 0.5).catch(() => null);
        if (!at) continue;
        const heading = await bearingOf(each.segment, at);
        pending.push({
          kind: "flow",
          lat: at.lat,
          lon: at.lon,
          speedKmh,
          bearingDeg: heading ?? bearingDeg ?? undefined,
          observedAtMillis
        });
        stats.observationsQueued++;
        if (reading.roadClosure) {
          // The vendor states the road is physically shut. That is the
          // one claim allowed to cross into the measurement channel —
          // see ingest.js on why, and what it costs to be wrong.
          pending.push({
            kind: "incident",
            lat: at.lat,
            lon: at.lon,
            type: "closure report",
            closedRoad: true,
            bearingDeg: heading ?? bearingDeg ?? undefined,
            observedAtMillis
          });
          stats.closures++;
          stats.observationsQueued++;
        }
        states.push({
          segment: each.segment,
          speedMps: speedKmh / 3.6,
          meters: each.meters || undefined,
          confidence: Number.isFinite(reading.confidence) ? reading.confidence : 0.5,
          observedAt: observedAtMillis,
          closed: false,
          penaltySeconds: 0
        });
      }
    }

    return {
      ...plan,
      probed,
      states,
      publishedSegments,
      queued: pending.length
    };
  }

  /**
   * Route, fill the gaps, and re-price on what was filled — in one call.
   *
   * This exists because the obvious way to use `backfill()` is wrong in
   * a way that is invisible. Having bought the gaps, you naturally
   * re-route against `session.provider()` — and get a *worse* answer
   * than the one you paid for, because the publisher paces itself under
   * the receivers' rate bucket (§6 rule 7) and has only drained part of
   * the burst. Measured on an 18 km itinerary: 99 bought states priced
   * it at 1011 s, while the mesh provider one beat later had 10 of them
   * and said 991 s.
   *
   * So the rule is: `states` is for the client that paid for the probes,
   * and the mesh copies are for whoever asks next. This does that, and
   * `route.live.provider` on the result says which of the two answered.
   *
   * `params` is passed through to `engine.route()` — `bucket`,
   * `departureTime`, `alternatives`, and so on.
   */
  async function priceRoute({ nowMillis = clock(), ...params } = {}) {
    const first = await engine.route({ ...params, live: session.provider?.() ?? null });
    const report = await backfill(first, { nowMillis });
    if (!report.states.length) return { route: first, report, repriced: false };
    // Merge, so a segment the mesh already priced is not silently
    // dropped in favour of the bought view of a different one.
    const merged = new Map();
    const meshProvider = session.provider?.() ?? null;
    if (meshProvider) {
      for (const state of await meshProvider.fetch({
        epoch: session.epoch,
        areas: [],
        maxAgeSeconds: maxAgeSeconds
      })) merged.set(state.segment, state);
    }
    for (const state of report.states) merged.set(state.segment, state);
    const states = [...merged.values()];
    const route = await engine.route({
      ...params,
      live: { name: "backfill+mesh", fetch: () => states }
    });
    return { route, report, repriced: true, states };
  }

  // The publisher's handle on this: a drain, not a poller. `fetch()`
  // hands over whatever backfill() has queued and returns nothing the
  // rest of the time, so an idle mesh costs an empty array every few
  // seconds and not one API call.
  const source = {
    id,
    intervalSeconds: 5,
    fetch: async () => pending.splice(0, pending.length)
  };

  return {
    source,
    assess,
    backfill,
    priceRoute,
    stats,
    /** Drop the per-segment answer cache (e.g. on an epoch change). */
    clearCache: () => cache.clear(),
    get cacheSize() { return cache.size; },
    get queueLength() { return pending.length; },
    get budgetRemaining() { return spendable(clock()); },
    get lastProbeMillis() { return lastProbeMillis; }
  };
}
