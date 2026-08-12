// PulseMesh session — the integration boundary between a route graph and
// the mesh, in one object.
//
// Everything above this file (an app, a page, a keeper) wants the same
// six things and nothing else: route under live traffic, see where the
// jams and the incidents are, contribute if the user said so, report an
// incident, follow the corridor the driver is actually in, and know what
// the mesh is doing. Everything below it is protocol. Before this module
// existed each host wired the two together itself, and each host got the
// same two things wrong:
//
//   1. `cellContext` was faked (`polylineCount: 1 << 20`, `classOf: () =>
//      "secondary"`). Rule 11 could then never fire, and rule 10 capped
//      *every* road at 100 km/h — so an honest 120 km/h motorway
//      contribution failed validation and cost the peer that delivered it
//      trust. Faking the static index is worse than admitting you have
//      not loaded it, which §6 explicitly allows.
//   2. `metersOf` returned 0, so contributions carried no length. A
//      receiver then has a speed with no way to cost it and applies
//      nothing: the familiar "states: 15, applied: 0".
//
// Both answers live in the static index and are now read from it through
// `engine.cellFacts(leaf)`, warmed for the leaves the session is actually
// listening to and treated as absent everywhere else.

import { DEFAULT_CONSTANTS, detailCellForE7, zoneOfDetailCell } from "./bins.js";
import { MeshNode } from "./node.js";
import { createContributor } from "./contribute.js";
import { createReticentProfile } from "./reticent.js";
import { aggregateSegment, congestionRatio } from "./aggregate.js";
import { INCIDENT_TYPES } from "./incidents.js";
import { PROOF_BOND, parseSegment, segmentString } from "./codec.js";

/** Capabilities a host must provide before any of this can run. */
export function checkMeshHost() {
  const missing = [];
  if (typeof globalThis.crypto?.getRandomValues !== "function") missing.push("crypto.getRandomValues");
  if (typeof TextEncoder === "undefined") missing.push("TextEncoder");
  // Only the thread channel needs subtle; the traffic channel does not,
  // so it is reported separately rather than as a hard requirement.
  const threadsAvailable = typeof globalThis.crypto?.subtle?.importKey === "function";
  return { ok: missing.length === 0, missing, threadsAvailable };
}

// How many leaves' static facts to hold. A leaf is a few hundred edges;
// a long drive crosses tens of them, and the cap only bounds a session
// that has been running across a country.
const MAX_WARM_LEAVES = 384;
// Leaves warmed per maintenance tick from what the store has received.
// Warming is a byte-range read per leaf, so a burst of gossip from a new
// area must not turn into a fetch storm.
const WARM_PER_TICK = 8;
// Segments drawn per traffic() call. A traffic layer is a picture, and
// past a few hundred lines it is a smear that costs geometry fetches.
const MAX_TRAFFIC_SEGMENTS = 400;

/** How a congestion ratio reads on a map. Thresholds are display-only. */
export function congestionLevel(ratio) {
  if (ratio == null || !Number.isFinite(ratio)) return "unknown";
  if (ratio <= 0.25) return "stopped";
  if (ratio <= 0.5) return "heavy";
  if (ratio <= 0.75) return "slow";
  return "free";
}

function splitSegKey(segKey) {
  const slash = String(segKey).indexOf("/");
  if (slash < 0) return null;
  const leafCell = Number(segKey.slice(0, slash));
  const geomRef = Number(segKey.slice(slash + 1));
  if (!Number.isInteger(leafCell) || !Number.isInteger(geomRef)) return null;
  return { leafCell, geomRef };
}

/**
 * Binds an open route graph to a mesh.
 *
 * - `engine`: an open route graph (`openRouteGraphUrl` / `openRouteGraphDir`).
 *   Its `root.sourceHash` is the epoch, its `snap()` turns a fix into a
 *   segment id, and its `cellFacts()` answers validation rules 10–12.
 * - `network`: any MeshNetwork (libp2p, loopback). Omit to run the
 *   pipeline locally with no peers — still a working router, by contract.
 * - `contribute`: **off unless explicitly enabled**. A device that merely
 *   reads traffic must never start publishing because a library defaulted
 *   it on.
 * - `readOnly` (§11.6): consume without ever publishing — no gossip
 *   membership, no §5.4 bond, everything pulled on tick(). The right mode
 *   for a browser at home.
 * - `profile`: "reticent" (default) or "cadence". Reticent is mandatory
 *   for unpublished routes (threads §10 rule 3).
 * - `batteryLevel` / `charging`: §10.1 rule 5 — contribution pauses below
 *   20% unless charging.
 */
export async function createMeshSession({
  engine,
  network = null,
  id = "session",
  epochHex = null,
  previousEpochHex = null,
  constants = DEFAULT_CONSTANTS,
  profile = "reticent",
  contribute = false,
  readOnly = false,
  transport = "wire",
  proofType = PROOF_BOND,
  keeper = false,
  suppressedTypes = [],
  clock = Date.now,
  rng = Math.random,
  batteryLevel = null,
  charging = null,
  onEmit = null,
  onIncident = null,
  // Map-matching. Defaults to the engine's own snap; a simulation that
  // already knows which segment a vehicle is on passes its own, so the
  // records it produces still go through the whole real pipeline.
  snap = null
} = {}) {
  if (readOnly && contribute) throw new Error("readOnly and contribute are mutually exclusive.");
  if (!engine?.root?.sourceHash) throw new Error("A mesh session needs an open route graph.");
  const host = checkMeshHost();
  if (!host.ok) throw new Error(`PulseMesh needs ${host.missing.join(", ")} on this host.`);

  const epoch = epochHex || engine.root.sourceHash;

  // --- Static index facts, warmed per leaf --------------------------------
  //
  // The validator is synchronous, so this is a cache with an explicit
  // warm step rather than a lazy async lookup. A leaf we have not warmed
  // reads as "no context", which §6 says is a legal state for rules
  // 10–12: they are probabilistic across peers by design.
  const facts = new Map();       // leaf -> facts
  const warming = new Map();     // leaf -> promise
  // Live corridor watches (see watchRoute). Driven from tick() so a watch
  // works with no wiring beyond start(); stopped ones are dropped there.
  const watches = new Set();
  // Cells the route asked for that nobody has been asked for yet, because
  // there was no peer to ask or the ask failed. Retried from tick().
  let pendingCorridor = null;

  async function warmLeaves(leaves) {
    const wanted = [];
    for (const leaf of new Set(leaves)) {
      if (!Number.isInteger(leaf) || leaf < 0) continue;
      if (facts.has(leaf) || warming.has(leaf)) continue;
      wanted.push(leaf);
    }
    if (!wanted.length) return facts.size;
    await Promise.all(wanted.map(async leaf => {
      const promise = Promise.resolve(engine.cellFacts?.(leaf) ?? null);
      warming.set(leaf, promise);
      try {
        const entry = await promise;
        if (entry) facts.set(leaf, entry);
      } catch {
        // A leaf whose bytes we cannot read stays contextless. That is a
        // weaker validator for records in it, never a broken one.
      } finally {
        warming.delete(leaf);
      }
    }));
    // Insertion-ordered eviction: the oldest leaves are the ones furthest
    // behind on a drive.
    if (facts.size > MAX_WARM_LEAVES) {
      for (const leaf of [...facts.keys()].slice(0, facts.size - MAX_WARM_LEAVES)) facts.delete(leaf);
    }
    return facts.size;
  }

  /**
   * Deterministic, shared cell placement: the z15 cell of the leaf's bbox
   * centre.
   *
   * A wire record carries `leafCell/geomRef` and no coordinate at all, so
   * every peer has to derive the same cell from the static index alone,
   * without coordinating and without holding the leaf. The leaf's bbox is
   * in the route-graph root, which every peer has; the segment's own
   * geometry is not. This is coarser than §2.3's "z15 cell of the snapped
   * coordinate" — a whole leaf lands in one cell — and it is what the
   * keeper and every shipped host compute, so it is what interoperates.
   */
  const cellOf = record => {
    const bbox = engine.root.leaves[record.leafCell]?.bbox;
    if (!bbox) return null;
    return detailCellForE7((bbox.minLat + bbox.maxLat) / 2, (bbox.minLon + bbox.maxLon) / 2);
  };

  const cellContext = leafCell => facts.get(leafCell) ?? null;

  const staticMetersOf = segKey => {
    const parts = splitSegKey(segKey);
    if (!parts) return 0;
    return facts.get(parts.leafCell)?.metersOf(parts.geomRef) ?? 0;
  };

  const roadClassOf = segKey => {
    const parts = splitSegKey(segKey);
    if (!parts) return null;
    return facts.get(parts.leafCell)?.classOf(parts.geomRef) ?? null;
  };

  const freeflowKmhOf = segKey => {
    const parts = splitSegKey(segKey);
    if (!parts) return null;
    return facts.get(parts.leafCell)?.freeflowKmhOf(parts.geomRef) ?? null;
  };

  // --- The node ----------------------------------------------------------

  const node = network
    ? new MeshNode({
        id,
        epochHex: epoch,
        previousEpochHex,
        constants,
        cellOf,
        cellContext,
        freeflowKmhOf,
        network,
        clock,
        rng,
        transport,
        suppressedTypes,
        keeper,
        readOnly
      })
    : null;

  // The reticent profile needs to know what the mesh currently expects on
  // a segment and whether anyone else is reporting it — both of which the
  // node already holds, because a contributor is also a consumer.
  const reticent = profile === "reticent"
    ? createReticentProfile({
        constants,
        clock,
        rng,
        expectedBinOf: segKey => {
          if (!node) return NaN;
          const aggregate = aggregateSegment(node.store.contributionsForSegment(segKey), {
            nowMillis: clock(), constants
          });
          return aggregate && aggregate.confidence >= constants.SURPRISE_CONFIDENCE
            ? aggregate.speedBin
            : NaN;
        },
        companyCountOf: segKey => (node ? node.store.contributionsForSegment(segKey).length : 0),
        forwarderPool: () => (network?.peersOf ? network.peersOf(id).filter(peer => peer !== id) : [])
      })
    : "cadence";

  const stats = {
    fixes: 0, emitted: 0, suppressed: 0, incidents: 0,
    lastReason: null, zones: 0, cells: 0, warmedLeaves: 0
  };

  const contributor = createContributor({
    epoch32: node?.epoch32 ?? new Uint8Array(32),
    epochPrefix8: node?.epochPrefix8 ?? new Uint8Array(8),
    snap: async fix => {
      let match = null;
      if (snap) {
        match = await snap(fix);
      } else {
        const result = await engine.snap({ lat: fix.lat, lon: fix.lon });
        match = result?.matches?.[0] ?? null;
      }
      // The leaf we are driving through is the leaf we are about to
      // publish into and the leaf whose records we are about to receive,
      // so it is the one worth holding the facts for. Fire-and-forget:
      // the first record in a new leaf carries meters 0, which rule 10
      // skips, and every later one is exact.
      if (match?.segment) {
        const parsed = parseSegment(match.segment);
        if (!facts.has(parsed.leafCell)) warmLeaves([parsed.leafCell]).catch(() => {});
      }
      return match;
    },
    publish: async emission => {
      stats.emitted++;
      if (node) node.publishRecord(emission.record, { forwarder: emission.forwarder });
      if (onEmit) await onEmit(emission);
    },
    constants,
    profile: reticent,
    // §5.4: records are proofless; the transport's admission bond vouches
    // for them. A host that contributes over a real network must mint its
    // bond on the transport (`network.mintBond()`) — records from an
    // unbonded peer are dropped by every receiver's rule 5.
    proofType,
    clock,
    batteryLevel,
    charging,
    suppressedTypes,
    metersOf: staticMetersOf,
    classOf: roadClassOf
  });

  // --- Location ----------------------------------------------------------

  /**
   * One GPS fix. Contribution is off unless explicitly enabled; the fix
   * still counts, because "how many fixes did we decline to publish" is
   * the number that tells a user the gates are working.
   */
  async function onLocation(fix) {
    stats.fixes++;
    if (!contribute) {
      // Still map-match it. Reporting an incident is a separate act from
      // publishing measurements, and §10.4 only files a report for a
      // recently snapped position — so skipping this entirely would make
      // "I don't share my speed" silently mean "I can't report a crash".
      await contributor.observe(fix).catch(() => null);
      stats.suppressed++;
      stats.lastReason = "contribution disabled";
      return { emitted: false, reason: "contribution disabled" };
    }
    const result = await contributor.handleFix(fix);
    if (!result.emitted) {
      stats.suppressed++;
      stats.lastReason = result.reason;
    }
    return result;
  }

  // --- Corridor ----------------------------------------------------------

  /**
   * Adopt the routes the driver is actually on as the mesh's scope:
   * subscribe to the z9 zones they cross, fetch the z15 cells they touch,
   * and warm those leaves' static facts.
   *
   * The cells come from `cellOf` over the route's own edges, not from
   * rasterizing its geometry. Those two disagree — `cellOf` places a
   * record at its leaf's centre, which is routinely outside the corridor
   * — and the cell a publisher publishes to is by definition the `cellOf`
   * one. Subscribing to geometry cells would join zones nobody publishes
   * to and request cells no store indexes.
   */
  async function followRoute(routes, { fetch = true } = {}) {
    const list = (Array.isArray(routes) ? routes : [routes]).filter(Boolean);
    const zones = new Map();
    const cells = new Map();
    const leaves = new Set();
    for (const route of list) {
      for (const edge of route.edges || []) {
        if (!edge?.segment) continue;
        const { leafCell, geomRef } = parseSegment(edge.segment);
        leaves.add(leafCell);
        const cell = cellOf({ leafCell, geomRef });
        if (!cell) continue;
        cells.set(`${cell.x}/${cell.y}`, cell);
        const zone = zoneOfDetailCell(cell);
        zones.set(`${zone.x}/${zone.y}`, zone);
      }
    }
    await warmLeaves([...leaves]);
    stats.warmedLeaves = facts.size;
    const zoneList = [...zones.values()];
    stats.zones = zoneList.length;
    stats.cells = cells.size;
    if (!node) return zoneList;
    node.subscribeZones(zoneList);
    // Held until it has actually been asked for. The fetch needs a peer to
    // ask, and whether there is one at this instant has nothing to do with
    // the route: a phone whose peers blinked out for six seconds — which one
    // recorded drive did, on the minute — adopted its corridor into silence
    // and never asked again, because nothing retried and nothing said so.
    // The tick below picks it up as soon as anybody is there.
    pendingCorridor = fetch && cells.size ? [...cells.values()] : null;
    await fetchCorridor();
    return zoneList;
  }

  /**
   * Ask for the corridor, if there is one outstanding and anyone to ask.
   *
   * The §11.3 padding, decoys, shuffle and split all live inside fetchCells;
   * a corridor never leaves this device in order. A refusal leaves the
   * request outstanding rather than dropping it, so a keeper that was busy
   * is retried on the next beat instead of costing the drive its traffic.
   */
  async function fetchCorridor() {
    if (!node || !pendingCorridor) return false;
    if (!network?.peersOf?.(id)?.length) return false;
    const asked = pendingCorridor;
    pendingCorridor = null;
    const got = await node.fetchCells(asked).catch(() => null);
    if (got === null) {
      pendingCorridor = asked;
      return false;
    }
    return true;
  }

  // --- Incidents ---------------------------------------------------------

  /**
   * §10.4 manual report, for the reporter's own snapped position. There
   * is no "report anywhere" affordance and there is no way to file one
   * without having shown the user that the report is public and locates
   * them — `acknowledgedPublic` is the caller asserting it did.
   */
  async function reportIncident({ type, acknowledgedPublic = false, nowMillis = clock() } = {}) {
    if (readOnly) return { emitted: false, reason: "read-only session (§11.6)" };
    const result = contributor.reportIncident({ type, polarity: 1, acknowledgedPublic, nowMillis });
    if (!result.emitted) return result;
    stats.incidents++;
    if (node) node.publishRecord(result.record, { nowMillis });
    if (onIncident) await onIncident(result);
    return result;
  }

  /** Confirms (polarity 2) or refutes (3) an incident this device is passing. */
  async function answerIncident({ key, polarity, acknowledgedPublic = false, nowMillis = clock() } = {}) {
    if (readOnly) return { emitted: false, reason: "read-only session (§11.6)" };
    if (polarity !== 2 && polarity !== 3) return { emitted: false, reason: "bad-polarity" };
    const type = Number(String(key).split("|")[1]);
    const result = contributor.reportIncident({
      type, polarity, incidentKey: key, acknowledgedPublic, nowMillis
    });
    if (!result.emitted) return result;
    stats.incidents++;
    if (node) node.publishRecord(result.record, { nowMillis });
    if (onIncident) await onIncident(result);
    return result;
  }

  // --- Display -----------------------------------------------------------

  const geometryCache = new Map(); // segment id -> { points, meters }

  async function geometryFor(segment) {
    let entry = geometryCache.get(segment);
    if (!entry) {
      entry = engine.geometryOf(segment).catch(() => null);
      geometryCache.set(segment, entry);
      if (geometryCache.size > 2048) {
        for (const key of [...geometryCache.keys()].slice(0, 1024)) geometryCache.delete(key);
      }
    }
    return entry;
  }

  /**
   * What the mesh currently believes about the roads around us, ready to
   * draw: one entry per segment with its polyline, its observed speed,
   * the static free-flow speed it is being compared against, and how many
   * independent reports are behind it.
   *
   * `minReports` defaults to the protocol's own hint threshold: below it
   * there is no aggregate at all, and inventing one for the picture would
   * show the user a jam the router itself refuses to believe.
   */
  async function traffic({
    nowMillis = clock(),
    minReports = constants.AGG_HINT_REPORTS,
    maxAgeSeconds = constants.DISPLAY_MAX_AGE,
    levels = null,
    limit = MAX_TRAFFIC_SEGMENTS
  } = {}) {
    if (!node) return [];
    const wanted = [];
    for (const [segKey, aggregate] of node.provider.aggregates(nowMillis)) {
      if (aggregate.n < minReports) continue;
      const ageSeconds = Math.max(0, Math.round((nowMillis - aggregate.observedAt) / 1000));
      if (ageSeconds > maxAgeSeconds) continue;
      wanted.push({ segKey, aggregate, ageSeconds });
      if (wanted.length >= limit * 2) break;
    }
    // Slowest first: a limit that has to bite should keep the jams.
    wanted.sort((a, b) => a.aggregate.speedKmh - b.aggregate.speedKmh);
    const kept = wanted.slice(0, limit);
    await warmLeaves(kept.map(entry => splitSegKey(entry.segKey)?.leafCell).filter(Number.isInteger));
    const out = [];
    for (const { segKey, aggregate, ageSeconds } of kept) {
      const parts = splitSegKey(segKey);
      if (!parts) continue;
      const segment = segmentString(parts.leafCell, parts.geomRef);
      const freeflowKmh = freeflowKmhOf(segKey);
      const ratio = congestionRatio(aggregate, freeflowKmh);
      const level = congestionLevel(ratio);
      if (levels && !levels.includes(level)) continue;
      const geometry = await geometryFor(segment);
      if (!geometry) continue;
      out.push({
        segment,
        segKey,
        points: geometry.points,
        speedKmh: aggregate.speedKmh,
        freeflowKmh,
        ratio,
        level,
        reports: aggregate.n,
        confidence: aggregate.confidence,
        hint: aggregate.hint,
        ageSeconds
      });
    }
    return out;
  }

  /**
   * Scored incidents with a position, ready to pin. `tier` is the
   * protocol's own: "shown" incidents are corroborated enough to display
   * as fact, "hint" ones are not, and a UI that renders them identically
   * is making a claim the mesh did not.
   */
  // --- Corridor watch ----------------------------------------------------

  /** free < slow < heavy < stopped; -1 is "the mesh has nothing here". */
  const LEVEL_SEVERITY = { free: 0, slow: 1, heavy: 2, stopped: 3 };
  const severityOf = level => (level in LEVEL_SEVERITY ? LEVEL_SEVERITY[level] : -1);

  /**
   * Watch the segments of a route the user is actually on, and say when
   * the mesh changes under it in a way that could change the answer.
   *
   * A route is priced once, when it is computed — `route({ live })` takes
   * a snapshot, and nothing re-prices it afterwards. That is correct for
   * a query and wrong for a drive: the jam that matters is the one that
   * forms after you set off. This is the missing signal.
   *
   * What it deliberately does NOT do is fire on every number that moves.
   * A weighted median over a handful of reports jitters by a few km/h
   * constantly, and a route that re-planned on that would thrash. So the
   * unit of change is the *congestion level* (free / slow / heavy /
   * stopped) — a crossing worth acting on — with a hard `debounceSeconds`
   * floor underneath it.
   *
   * Two asymmetries are on purpose:
   *
   *  - A segment going from no-data to free or slow is not news. Most of
   *    a corridor is uncovered most of the time, and "the mesh now
   *    confirms this road is fine" must never trigger a re-plan. Only a
   *    first observation at `worsenLevel` or above counts.
   *  - A segment going from known to no-data is *expiry*, not recovery.
   *    Records TTL out constantly; treating that as "the jam cleared"
   *    would announce good news that nobody measured.
   *
   * `check()` is exposed so a host with a faster loop than the session's
   * own maintenance beat (ANTI_ENTROPY_SECONDS, 10 s) can drive it; it is
   * also called from `tick()` so a watch works with no extra wiring.
   */
  function watchRoute(routes, {
    onChange = null,
    debounceSeconds = 20,
    minReports = constants.AGG_HINT_REPORTS,
    maxAgeSeconds = constants.DISPLAY_MAX_AGE,
    worsenLevel = "heavy"
  } = {}) {
    const list = (Array.isArray(routes) ? routes : [routes]).filter(Boolean);
    const segKeys = [];
    for (const route of list) {
      for (const edge of route.edges || []) {
        if (!edge?.segment) continue;
        const { leafCell, geomRef } = parseSegment(edge.segment);
        segKeys.push(`${leafCell}/${geomRef}`);
      }
    }
    const watched = [...new Set(segKeys)];
    const last = new Map();          // segKey -> level, as of the previous check
    const worsenSeverity = severityOf(worsenLevel);
    let baselined = false;
    let lastFiredMillis = 0;
    let live = true;

    function levelOf(segKey, nowMillis) {
      if (!node) return "unknown";
      const entries = node.store.contributionsForSegment(segKey);
      if (!entries.length) return "unknown";
      const aggregate = aggregateSegment(entries, { nowMillis, constants });
      if (!aggregate || aggregate.n < minReports) return "unknown";
      if (Math.max(0, (nowMillis - aggregate.observedAt) / 1000) > maxAgeSeconds) return "unknown";
      return congestionLevel(congestionRatio(aggregate, freeflowKmhOf(segKey)));
    }

    /**
     * One comparison against the previous one. Returns the change set,
     * or null when nothing crossed a level or the debounce still holds.
     */
    function check(nowMillis = clock()) {
      if (!live || !node) return null;
      const worsened = [];
      const improved = [];
      for (const segKey of watched) {
        const level = levelOf(segKey, nowMillis);
        const previous = last.get(segKey) ?? "unknown";
        last.set(segKey, level);
        if (!baselined || level === previous) continue;
        const before = severityOf(previous);
        const after = severityOf(level);
        if (after < 0) continue;                          // expiry, not recovery
        if (before < 0) {
          if (after >= worsenSeverity) worsened.push({ segKey, from: previous, to: level });
          continue;                                       // first sight of a clear road is not news
        }
        (after > before ? worsened : improved).push({ segKey, from: previous, to: level });
      }
      if (!baselined) {
        // The first pass only establishes what "unchanged" means; firing
        // here would announce the whole corridor as new information.
        baselined = true;
        return null;
      }
      if (!worsened.length && !improved.length) return null;
      if (nowMillis - lastFiredMillis < debounceSeconds * 1000) return null;
      lastFiredMillis = nowMillis;
      const change = { worsened, improved, segments: watched.length, nowMillis };
      if (onChange) {
        // A handler that throws must not kill the maintenance loop that
        // called it.
        try { onChange(change); } catch { /* the caller's problem */ }
      }
      return change;
    }

    const watch = {
      check,
      stop() { live = false; },
      get watching() { return live; },
      get segments() { return watched.length; }
    };
    watches.add(watch);
    return watch;
  }

  async function incidents({ nowMillis = clock(), includeHints = true } = {}) {
    if (!node) return [];
    const scored = node.provider.displayIncidents({ nowMillis });
    const out = [];
    for (const entry of scored) {
      if (!includeHints && !entry.displayed) continue;
      const parts = splitSegKey(entry.segKey);
      if (!parts) continue;
      const segment = segmentString(parts.leafCell, parts.geomRef);
      const ratio = Math.max(0, Math.min(1, (entry.ratioQ12 || 0) / 4095));
      let point = null;
      try {
        point = await engine.locate(segment, ratio);
      } catch {
        continue; // a segment this graph does not have is not drawable
      }
      const ageSeconds = Math.max(0, Math.round(nowMillis / 1000 - entry.newestBucket * 15));
      out.push({
        key: `${entry.segKey}|${entry.type}`,
        segment,
        segKey: entry.segKey,
        type: entry.type,
        typeName: entry.typeName,
        appliesBoth: Boolean(INCIDENT_TYPES[entry.type]?.appliesBoth),
        informational: (INCIDENT_TYPES[entry.type]?.penaltySeconds || 0) === 0,
        lat: point.lat,
        lon: point.lon,
        score: entry.score,
        sources: entry.sources,
        reports: entry.reports,
        confirms: entry.confirms,
        refutes: entry.refutes,
        tier: entry.displayed ? "shown" : "hint",
        ageSeconds
      });
    }
    return out;
  }

  // --- Maintenance -------------------------------------------------------

  let timer = null;

  /**
   * One maintenance tick: the node's TTL sweep and anti-entropy round,
   * plus warming the facts for leaves we have started hearing about. The
   * warm is bounded per tick — a burst of gossip from a new area must not
   * become a fetch storm.
   */
  async function tick(nowMillis = clock()) {
    if (node) await node.tick(nowMillis);
    if (!node) return;
    // A corridor the route asked for and no peer was there to answer. Ask
    // now, before the watches read a store that would otherwise stay empty
    // for the whole drive.
    await fetchCorridor();
    // Corridor watches run before the warm: a driver wants to hear about
    // the jam ahead on this beat, not the next one.
    for (const watch of watches) {
      if (!watch.watching) { watches.delete(watch); continue; }
      watch.check(nowMillis);
    }
    const cold = [];
    for (const segKey of node.store.liveSegmentKeys()) {
      const parts = splitSegKey(segKey);
      if (!parts || facts.has(parts.leafCell) || warming.has(parts.leafCell)) continue;
      cold.push(parts.leafCell);
      if (cold.length >= WARM_PER_TICK) break;
    }
    if (cold.length) {
      await warmLeaves(cold);
      stats.warmedLeaves = facts.size;
    }
  }

  /** Starts the maintenance loop. Jittered, per §11.4. */
  function start({ intervalSeconds = constants.ANTI_ENTROPY_SECONDS } = {}) {
    if (timer || !node) return false;
    const schedule = () => {
      const jitter = (0.75 + rng() * 0.5) * intervalSeconds * 1000;
      timer = setTimeout(async () => {
        try {
          await tick();
        } catch {
          // A failed round is a missed repair, never a dead session.
        }
        if (timer) schedule();
      }, jitter);
      if (typeof timer.unref === "function") timer.unref();
    };
    schedule();
    return true;
  }

  function stopTimer() {
    if (!timer) return;
    clearTimeout(timer);
    timer = null;
  }

  async function close() {
    stopTimer();
    if (network?.close) await network.close();
  }

  /** Everything a status UI needs, in one cheap read. */
  function snapshot() {
    const peers = network?.peersOf ? network.peersOf(id).length : 0;
    return {
      available: true,
      epoch: epoch.slice(0, 16),
      contributing: contribute,
      readOnly,
      transport,
      profile: reticent === "cadence" ? "cadence" : "reticent",
      peers,
      bondedPeers: node ? node.bondedPeers.size : 0,
      zones: stats.zones,
      cells: stats.cells,
      warmedLeaves: facts.size,
      records: node ? node.store.size() : 0,
      segments: node ? node.store.liveSegmentKeys().length : 0,
      incidentRecords: node ? node.store.incidentCount() : 0,
      fixes: stats.fixes,
      emitted: stats.emitted,
      suppressed: stats.suppressed,
      incidents: stats.incidents,
      lastReason: stats.lastReason,
      gossip: node
        ? {
            accepted: node.stats.gossipAccepted,
            dropped: node.stats.gossipDropped,
            dropsByRule: { ...node.stats.dropsByRule },
            snapshotsMerged: node.stats.snapshotsMerged,
            antiEntropyRounds: node.stats.antiEntropyRounds,
            cellsRequested: node.stats.cellsRequested
          }
        : null,
      threadsAvailable: host.threadsAvailable
    };
  }

  return {
    node,
    network,
    contributor,
    host,
    stats,
    epoch,
    onLocation,
    followRoute,
    watchRoute,
    warmLeaves,
    reportIncident,
    answerIncident,
    traffic,
    incidents,
    tick,
    start,
    stop: stopTimer,
    close,
    snapshot,
    cellContext,
    freeflowKmhOf,
    /** Hand this to `engine.route({ live })`. Null when there is no mesh. */
    provider: () => node?.provider ?? null,
    /** Turns contribution on or off at runtime, e.g. from a settings toggle. */
    setContributing(value) {
      if (readOnly && value) throw new Error("A read-only PulseMesh session never publishes (§11.6).");
      contribute = Boolean(value);
      return contribute;
    },
    get contributing() { return contribute; },
    get running() { return timer != null; }
  };
}
