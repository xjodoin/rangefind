// PulseMesh local store (protocol §7): in-memory only, TTL-bounded, keyed
// three ways over one record set — by reportId (replay dedup), by z15 cell
// (digests, snapshots), and by (segment, direction) (aggregation; the
// direction bit lives inside geomRef). A departing peer removes nothing;
// an empty region forgets by TTL. Persistence to disk is prohibited.
//
// Cell placement must converge across peers for digests to compare equal,
// so every store derives a record's cell through the same injected
// `cellOf(record)` — a deterministic function of the record and the shared
// static index (canonically: the z15 cell of the segment polyline's
// midpoint). Records whose cell cannot be resolved are refused.

import { DEFAULT_CONSTANTS, bucketStartMillis, detailCellKey, zoneKey, zoneOfDetailCell, localOfDetailCell } from "./bins.js";
import { contributionWeight } from "./aggregate.js";
import { bucketAgeSeconds } from "./bins.js";
import { toHex } from "./sha256.js";

const INCIDENT_TTL_MAX_MS = 1800 * 1000;

export class PulseMeshStore {
  constructor({ constants = DEFAULT_CONSTANTS, cellOf, trustOf = null } = {}) {
    if (typeof cellOf !== "function") throw new Error("PulseMeshStore requires a deterministic cellOf(record).");
    this.constants = constants;
    this.cellOf = cellOf;
    this.trustOf = trustOf;
    this.byId = new Map();       // reportIdHex -> entry
    this.byCell = new Map();     // "x/y" -> Map(reportIdHex -> entry)
    this.bySegment = new Map();  // "leaf/geomRef" -> Map(reportIdHex -> entry)
    this.incidentsById = new Map();      // reportIdHex -> incident entry
    this.incidentsByCell = new Map();    // "x/y" -> Map(reportIdHex -> entry)
    this.incidentsByKey = new Map();     // "leaf/geomRef|type" -> Map(reportIdHex -> entry)
    this.stats = { added: 0, duplicates: 0, evicted: 0, expired: 0 };
  }

  segmentKey(record) {
    return `${record.leafCell}/${record.geomRef}`;
  }

  hasReport(reportId) {
    const hex = typeof reportId === "string" ? reportId : toHex(reportId);
    return this.byId.has(hex) || this.incidentsById.has(hex);
  }

  size() {
    return this.byId.size;
  }

  incidentCount() {
    return this.incidentsById.size;
  }

  #entryWeight(entry, nowMillis) {
    const trust = entry.deliveredBy != null && this.trustOf ? this.trustOf(entry.deliveredBy) : this.constants.TRUST_INIT;
    return contributionWeight(entry.record, bucketAgeSeconds(entry.record.timeBucket, nowMillis), trust);
  }

  /**
   * Inserts a validated PMC1. Returns { added, reason }. Expiry is
   * min(bucketStart + ttlSeconds, receipt + CONTRIB_TTL) (§7); caps evict
   * the lowest aggregation weight first, oldest first among ties.
   */
  addContribution(record, { nowMillis, deliveredBy = null, viaForward = false, cell = null } = {}) {
    const idHex = toHex(record.reportId);
    if (this.byId.has(idHex)) {
      this.stats.duplicates++;
      return { added: false, reason: "duplicate" };
    }
    const resolvedCell = cell || this.cellOf(record);
    if (!resolvedCell) return { added: false, reason: "unplaceable" };
    const cellKey = detailCellKey(resolvedCell);
    const ttlSeconds = Math.min(record.ttlSeconds, this.constants.CONTRIB_TTL);
    const expiresAt = Math.min(
      bucketStartMillis(record.timeBucket) + ttlSeconds * 1000,
      nowMillis + this.constants.CONTRIB_TTL * 1000
    );
    const entry = { record, cell: resolvedCell, cellKey, idHex, deliveredBy, viaForward, receivedAt: nowMillis, expiresAt };

    const segKey = this.segmentKey(record);
    let segMap = this.bySegment.get(segKey);
    if (!segMap) { segMap = new Map(); this.bySegment.set(segKey, segMap); }
    let cellMap = this.byCell.get(cellKey);
    if (!cellMap) { cellMap = new Map(); this.byCell.set(cellKey, cellMap); }

    this.byId.set(idHex, entry);
    segMap.set(idHex, entry);
    cellMap.set(idHex, entry);
    this.stats.added++;

    if (segMap.size > this.constants.SEG_CONTRIB_CAP) this.#evictLowest(segMap.values(), nowMillis);
    else if (cellMap.size > this.constants.CELL_CONTRIB_CAP) this.#evictLowest(cellMap.values(), nowMillis);
    else if (this.byId.size > this.constants.STORE_CONTRIB_CAP) this.#evictLowest(this.byId.values(), nowMillis);
    return { added: this.byId.has(idHex), reason: this.byId.has(idHex) ? null : "evicted" };
  }

  #evictLowest(entries, nowMillis) {
    let worst = null;
    let worstWeight = Infinity;
    for (const entry of entries) {
      const weight = this.#entryWeight(entry, nowMillis);
      if (weight < worstWeight || (weight === worstWeight && worst && entry.receivedAt < worst.receivedAt)) {
        worst = entry;
        worstWeight = weight;
      }
    }
    if (worst) {
      this.#removeContribution(worst);
      this.stats.evicted++;
    }
  }

  #removeContribution(entry) {
    this.byId.delete(entry.idHex);
    const segKey = this.segmentKey(entry.record);
    const segMap = this.bySegment.get(segKey);
    if (segMap) { segMap.delete(entry.idHex); if (!segMap.size) this.bySegment.delete(segKey); }
    const cellMap = this.byCell.get(entry.cellKey);
    if (cellMap) { cellMap.delete(entry.idHex); if (!cellMap.size) this.byCell.delete(entry.cellKey); }
  }

  /**
   * Inserts a validated PMI1. The per-cell incident cap evicts the entry
   * whose (segment, type) key currently scores lowest — scoring is the
   * caller's (incidents.js) job, so a scoreOf callback is taken here.
   */
  addIncident(record, { nowMillis, deliveredBy = null, viaForward = false, cell = null, scoreOf = null } = {}) {
    const idHex = toHex(record.reportId);
    if (this.incidentsById.has(idHex) || this.byId.has(idHex)) {
      this.stats.duplicates++;
      return { added: false, reason: "duplicate" };
    }
    const resolvedCell = cell || this.cellOf(record);
    if (!resolvedCell) return { added: false, reason: "unplaceable" };
    const cellKey = detailCellKey(resolvedCell);
    const expiresAt = Math.min(
      bucketStartMillis(record.timeBucket) + record.ttlSeconds * 1000,
      nowMillis + INCIDENT_TTL_MAX_MS
    );
    const entry = { record, cell: resolvedCell, cellKey, idHex, deliveredBy, viaForward, receivedAt: nowMillis, expiresAt, contradictedAtBucket: -1 };
    const key = `${record.leafCell}/${record.geomRef}|${record.type}`;

    let cellMap = this.incidentsByCell.get(cellKey);
    if (!cellMap) { cellMap = new Map(); this.incidentsByCell.set(cellKey, cellMap); }
    let keyMap = this.incidentsByKey.get(key);
    if (!keyMap) { keyMap = new Map(); this.incidentsByKey.set(key, keyMap); }

    this.incidentsById.set(idHex, entry);
    cellMap.set(idHex, entry);
    keyMap.set(idHex, entry);

    // §6 rule 7: distinct live incidents per z15 cell are capped; on
    // overflow evict the lowest-scoring key's records first.
    const distinctKeys = new Set();
    for (const other of cellMap.values()) distinctKeys.add(`${other.record.leafCell}/${other.record.geomRef}|${other.record.type}`);
    if (distinctKeys.size > this.constants.INCIDENT_CELL_CAP && scoreOf) {
      let worstKey = null;
      let worstScore = Infinity;
      for (const candidate of distinctKeys) {
        const score = scoreOf(candidate);
        if (score < worstScore) { worstScore = score; worstKey = candidate; }
      }
      if (worstKey) this.#removeIncidentKey(worstKey);
    }
    return { added: this.incidentsById.has(idHex), reason: null };
  }

  #removeIncidentKey(key) {
    const keyMap = this.incidentsByKey.get(key);
    if (!keyMap) return;
    for (const entry of [...keyMap.values()]) this.#removeIncident(entry);
  }

  #removeIncident(entry) {
    this.incidentsById.delete(entry.idHex);
    const cellMap = this.incidentsByCell.get(entry.cellKey);
    if (cellMap) { cellMap.delete(entry.idHex); if (!cellMap.size) this.incidentsByCell.delete(entry.cellKey); }
    const key = `${entry.record.leafCell}/${entry.record.geomRef}|${entry.record.type}`;
    const keyMap = this.incidentsByKey.get(key);
    if (keyMap) { keyMap.delete(entry.idHex); if (!keyMap.size) this.incidentsByKey.delete(key); }
  }

  /** TTL sweep (§7): call at least once per bucket. */
  sweep(nowMillis) {
    for (const entry of [...this.byId.values()]) {
      if (entry.expiresAt <= nowMillis) {
        this.#removeContribution(entry);
        this.stats.expired++;
      }
    }
    for (const entry of [...this.incidentsById.values()]) {
      if (entry.expiresAt <= nowMillis) {
        this.#removeIncident(entry);
        this.stats.expired++;
      }
    }
  }

  /** Live contribution entries for one wire segment key "leaf/geomRef". */
  contributionsForSegment(segKey) {
    const segMap = this.bySegment.get(segKey);
    return segMap ? [...segMap.values()] : [];
  }

  /** Live incident entries for one wire segment key, all types. */
  incidentsForSegment(segKey) {
    const out = [];
    for (const [key, keyMap] of this.incidentsByKey) {
      if (key.startsWith(`${segKey}|`)) out.push(...keyMap.values());
    }
    return out;
  }

  incidentsForKey(key) {
    const keyMap = this.incidentsByKey.get(key);
    return keyMap ? [...keyMap.values()] : [];
  }

  /** Wire segment keys that currently hold live contributions. */
  liveSegmentKeys() {
    return [...this.bySegment.keys()];
  }

  /**
   * A whole zone folded to 12 bytes: how many live contributions it holds
   * and the XOR of their reportId prefixes. Order-independent by
   * construction, so two peers holding the same set fold identically —
   * which is what lets a responder skip sending a digest that would only
   * confirm what the requester already knows.
   */
  zoneFold(zone) {
    const fold = new Uint8Array(8);
    let count = 0;
    for (const [cellKey, cellMap] of this.byCell) {
      const [x, y] = cellKey.split("/").map(Number);
      const cellZone = zoneOfDetailCell({ x, y });
      if (cellZone.x !== zone.x || cellZone.y !== zone.y) continue;
      for (const entry of cellMap.values()) {
        count++;
        for (let i = 0; i < 8; i++) fold[i] ^= entry.record.reportId[i];
      }
    }
    return { count, fold };
  }

  /**
   * §4.4 zone digest. Entries sorted by (localX, localY); idFold is the
   * XOR of the first 8 bytes of every stored contribution's reportId in
   * the cell, so equal sets fold equally regardless of arrival order.
   */
  digestForZone(zone, nowMillis) {
    const entries = [];
    let baseBucket = Math.floor(nowMillis / 15000);
    const perCell = new Map();
    for (const [cellKey, cellMap] of this.byCell) {
      const [x, y] = cellKey.split("/").map(Number);
      const cellZone = zoneOfDetailCell({ x, y });
      if (cellZone.x !== zone.x || cellZone.y !== zone.y) continue;
      let newest = 0;
      const idFold = new Uint8Array(8);
      for (const entry of cellMap.values()) {
        if (entry.record.timeBucket > newest) newest = entry.record.timeBucket;
        for (let i = 0; i < 8; i++) idFold[i] ^= entry.record.reportId[i];
      }
      if (newest > baseBucket) baseBucket = newest;
      perCell.set(cellKey, { cell: { x, y }, count: cellMap.size, newest, idFold });
    }
    for (const { cell, count, newest, idFold } of perCell.values()) {
      const local = localOfDetailCell(cell);
      entries.push({ localX: local.x, localY: local.y, count, ageBuckets: baseBucket - newest, idFold });
    }
    entries.sort((a, b) => (a.localX - b.localX) || (a.localY - b.localY));
    return { zoneX: zone.x, zoneY: zone.y, baseBucket, entries };
  }

  /**
   * §4.5 PMS1 payload cells, echoing the request order. Unknown or empty
   * cells return counts of 0 — a responder must not distinguish "cell I
   * don't track" from "cell with no data". Records are served verbatim.
   */
  snapshotForCells(cells) {
    return cells.map(cell => {
      const cellKey = detailCellKey(cell);
      const cellMap = this.byCell.get(cellKey);
      const incidentMap = this.incidentsByCell.get(cellKey);
      return {
        x: cell.x,
        y: cell.y,
        records: cellMap ? [...cellMap.values()].map(entry => entry.record) : [],
        incidents: incidentMap ? [...incidentMap.values()].map(entry => entry.record) : []
      };
    });
  }
}
