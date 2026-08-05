// PulseMesh provider adapter (protocol §9): the mesh terminates in an
// ordinary LiveTrafficProvider. Everything mesh-specific stays behind
// fetch(); the engine contributes epoch checks, confidence/age blending,
// closure semantics, corridor-exact search, and graceful degradation.

import { DEFAULT_CONSTANTS, DETAIL_ZOOM, bucketAgeSeconds, detailCellRings } from "./bins.js";
import { geoCellForE7 } from "../geo_cells.js";
import { aggregateSegment, congestionRatio } from "./aggregate.js";
import { INCIDENT_TYPES, applyContradictionDecay, incidentPenaltySeconds, scoreIncidentKey } from "./incidents.js";
import { segmentString } from "./codec.js";

// Route-graph leaves vary enormously in extent — a rural leaf can span a
// quarter degree — so an unbounded raster would turn one area into
// thousands of cell requests. Past the cap we fetch the area's centre
// neighbourhood and let gossip and anti-entropy cover the rest.
const MAX_CELLS_PER_AREA = 64;
// A long route touches many leaves, and every wanted cell becomes part of
// a padded snapshot request that `fetch()` awaits inline. Bounding only
// per-area still lets a cross-country query issue hundreds of round trips
// before the router sees a single edge weight, so the total is capped
// too; anything beyond it is covered by gossip and later anti-entropy.
const MAX_CELLS_PER_FETCH = 256;

function cellsForAreas(areas) {
  const cells = new Map();
  const add = cell => cells.set(`${cell.x}/${cell.y}`, cell);
  const centreOf = bbox => geoCellForE7(
    (bbox.minLat + bbox.maxLat) / 2 * 1e7,
    (bbox.minLon + bbox.maxLon) / 2 * 1e7,
    DETAIL_ZOOM
  );
  for (const area of areas || []) {
    const bbox = area.bbox;
    if (!bbox) continue;
    if (cells.size >= MAX_CELLS_PER_FETCH) break;
    const northWest = geoCellForE7(bbox.maxLat * 1e7, bbox.minLon * 1e7, DETAIL_ZOOM);
    const southEast = geoCellForE7(bbox.minLat * 1e7, bbox.maxLon * 1e7, DETAIL_ZOOM);
    const width = southEast.x - northWest.x + 1;
    const height = southEast.y - northWest.y + 1;
    // A bbox crossing the antimeridian yields a negative width; rastering
    // it would silently produce nothing, so take the centre neighbourhood
    // rather than dropping the area.
    if (width <= 0 || height <= 0 || width * height > MAX_CELLS_PER_AREA) {
      for (const cell of detailCellRings(centreOf(bbox), 2)) add(cell);
      continue;
    }
    for (let y = northWest.y; y <= southEast.y; y++) {
      for (let x = northWest.x; x <= southEast.x; x++) add({ x, y });
    }
  }
  return [...cells.values()].slice(0, MAX_CELLS_PER_FETCH);
}

/**
 * Builds the LiveTrafficProvider over a local store.
 *
 * - epochHex: the mesh's current epoch (64 hex chars). fetch() returns []
 *   for any other epoch — the engine degrades by contract.
 * - store / trust: the local TTL store and trust ledger.
 * - fetchCells(cells): optional async hook that refreshes the store from
 *   the mesh before answering (sync.js applies the §11.3 privacy batching
 *   to every such fetch, including provider-triggered ones).
 * - freeflowKmhOf(segKey): static free-flow speed, for the congestion
 *   ratio that gates incident penalties (§8.5 rule 4).
 */
export function createPulseMeshProvider({
  epochHex,
  store,
  trust = null,
  constants = DEFAULT_CONSTANTS,
  fetchCells = null,
  freeflowKmhOf = null,
  clock = Date.now,
  applyTrustFeedback = true
} = {}) {
  const trustOf = trust ? peer => trust.get(peer) : null;

  // §8.4 trust moves when a record is judged against a later aggregate —
  // an event tied to routing, not to looking. `feedback` is therefore off
  // for read-only accessors: a UI polling aggregates() once a second must
  // not walk every delivering peer to a trust bound, which would change
  // the weights the next route is computed from.
  function segmentAggregates(nowMillis, { feedback = false } = {}) {
    const out = new Map();
    for (const segKey of store.liveSegmentKeys()) {
      const entries = store.contributionsForSegment(segKey);
      if (!entries.length) continue;
      const aggregate = aggregateSegment(entries, { nowMillis, trustOf, constants });
      if (!aggregate) continue;
      if (feedback && applyTrustFeedback && trust) trust.applyAggregateFeedback(aggregate, entries);
      out.set(segKey, aggregate);
    }
    return out;
  }

  function scoredIncidentsForSegment(segKey, nowMillis) {
    // "Applies to: both" types are stored under the reported direction but
    // count for the sibling direction of the same physical polyline too.
    // Both directions' records merge into ONE key per type — scoring them
    // as two keys would double both the score and the penalty for an
    // incident reported from each side, which is one incident.
    const [leafCell, geomRefText] = segKey.split("/");
    const geomRef = Number(geomRefText);
    const siblingKey = `${leafCell}/${geomRef ^ 1}`;
    const byType = new Map();
    const collect = (entries, requireAppliesBoth) => {
      for (const entry of entries) {
        if (requireAppliesBoth && !INCIDENT_TYPES[entry.record.type]?.appliesBoth) continue;
        const type = entry.record.type;
        if (!byType.has(type)) byType.set(type, []);
        byType.get(type).push(entry);
      }
    };
    collect(store.incidentsForSegment(segKey), false);
    collect(store.incidentsForSegment(siblingKey), true);
    const scored = [];
    for (const entries of byType.values()) {
      const result = scoreIncidentKey(entries, { nowMillis, trustOf, constants });
      if (result) scored.push(result);
    }
    return scored;
  }

  async function fetch({ epoch, areas, maxAgeSeconds = constants.DISPLAY_MAX_AGE } = {}) {
    if (epoch !== epochHex) return [];
    const nowMillis = clock();
    // Areas drive *fetching*, never filtering. The store is already
    // corridor-scoped and TTL-bounded, and the engine deliberately expects
    // states outside the leaves it has fetched so far: leaves referenced
    // by live states join the query's context set and their overlay
    // shortcuts are suppressed. Filtering to the fetched leaves would
    // defeat exactly that mechanism and leave a jam one leaf ahead
    // invisible until the router had already committed to it.
    const cells = cellsForAreas(areas);
    if (fetchCells && cells.length) await fetchCells(cells);
    const aggregates = segmentAggregates(nowMillis, { feedback: true });
    const states = [];
    for (const [segKey, aggregate] of aggregates) {
      if (bucketAgeSeconds(aggregate.newestBucket, nowMillis) > maxAgeSeconds) continue;
      if (aggregate.n < constants.AGG_HINT_REPORTS) continue;
      const [leafCell, geomRef] = segKey.split("/").map(Number);
      const freeflowKmh = freeflowKmhOf ? freeflowKmhOf(segKey) : null;
      const scored = scoredIncidentsForSegment(segKey, nowMillis);
      for (const incident of scored) {
        applyContradictionDecay(incident, aggregate, freeflowKmh, { nowMillis, constants });
      }
      states.push({
        segment: segmentString(leafCell, geomRef),
        speedMps: aggregate.speedMps,
        meters: aggregate.meters || undefined,
        confidence: aggregate.confidenceBin / 7,
        observedAt: aggregate.observedAt,
        closed: false, // ALWAYS false in protocol v1 (§9)
        penaltySeconds: incidentPenaltySeconds(scored, aggregate, freeflowKmh, { constants })
      });
    }
    return states;
  }

  /**
   * Display accessor (§9): every scored incident, including informational
   * types, with its display tier. Never routed — nothing informational
   * reaches LiveSegmentState at all.
   */
  function displayIncidents({ nowMillis = clock() } = {}) {
    const seenSegments = new Set();
    const shown = new Map();
    for (const [key] of store.incidentsByKey) {
      const segKey = key.split("|")[0];
      if (seenSegments.has(segKey)) continue;
      seenSegments.add(segKey);
      for (const scored of scoredIncidentsForSegment(segKey, nowMillis)) {
        if (scored.score < constants.INCIDENT_HINT_SCORE) continue;
        // An appliesBoth incident is one thing on one road: both
        // directions resolve to the same scored key, so display it once
        // under the physical polyline rather than once per approach.
        const [leafCell, geomRefText] = scored.segKey.split("/");
        const geomRef = Number(geomRefText);
        const displayKey = INCIDENT_TYPES[scored.type]?.appliesBoth
          ? `${leafCell}/${geomRef & ~1}|${scored.type}|both`
          : `${scored.segKey}|${scored.type}`;
        if (!shown.has(displayKey)) shown.set(displayKey, scored);
      }
    }
    return [...shown.values()];
  }

  return {
    name: "pulsemesh",
    fetch,
    displayIncidents,
    /** Read-only view: never moves the trust ledger. */
    aggregates: nowMillis => segmentAggregates(nowMillis ?? clock()),
    congestionRatioOf: (segKey, nowMillis) => {
      const entries = store.contributionsForSegment(segKey);
      const aggregate = aggregateSegment(entries, { nowMillis: nowMillis ?? clock(), trustOf, constants });
      return congestionRatio(aggregate, freeflowKmhOf ? freeflowKmhOf(segKey) : null);
    }
  };
}
