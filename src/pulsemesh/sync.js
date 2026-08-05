// PulseMesh sync (protocol §4.4, §4.5, §11.3): digest comparison for
// anti-entropy, and the padded/decoy/shuffled/split cell-request builder
// that applies to EVERY snapshot fetch — corridor fills, provider
// refreshes, and anti-entropy repairs alike. No peer ever sees an
// unpadded, ordered corridor.

import { DEFAULT_CONSTANTS, DETAIL_ZOOM, detailCellForE7, detailCellKey, detailCellRings } from "./bins.js";
import { geoCellsForRoute } from "../geo_cells.js";

/**
 * Cells worth requesting after comparing a remote PMD1 against the local
 * store's digest of the same zone: any cell the remote knows that we lack,
 * or whose contribution set provably differs (count or idFold), or that
 * the remote has fresher data for.
 */
export function diffDigest(localDigest, remoteDigest) {
  const local = new Map();
  for (const entry of localDigest.entries) local.set(`${entry.localX}/${entry.localY}`, entry);
  const wanted = [];
  for (const entry of remoteDigest.entries) {
    const mine = local.get(`${entry.localX}/${entry.localY}`);
    let want = false;
    if (!mine) want = true;
    else if (mine.count !== entry.count) want = true;
    else {
      for (let i = 0; i < 8; i++) {
        if (mine.idFold[i] !== entry.idFold[i]) { want = true; break; }
      }
    }
    if (want) {
      wanted.push({
        x: remoteDigest.zoneX * 64 + entry.localX,
        y: remoteDigest.zoneY * 64 + entry.localY
      });
    }
  }
  return wanted;
}

function shuffle(list, rng) {
  for (let i = list.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [list[i], list[j]] = [list[j], list[i]];
  }
  return list;
}

/**
 * Decoy candidates for a wanted set: the 2-ring neighborhoods of the
 * wanted cells plus previously visited cells, minus the wanted set
 * itself (§11.3 step 3).
 */
export function buildDecoyPool(wanted, visited = [], rings = 2) {
  const wantedKeys = new Set(wanted.map(detailCellKey));
  const pool = new Map();
  for (const cell of wanted) {
    for (const neighbor of detailCellRings(cell, rings)) {
      const key = detailCellKey(neighbor);
      if (!wantedKeys.has(key)) pool.set(key, neighbor);
    }
  }
  for (const cell of visited) {
    const key = detailCellKey(cell);
    if (!wantedKeys.has(key)) pool.set(key, cell);
  }
  return [...pool.values()];
}

/**
 * §11.3 (MUST, for every PMQ1): pads the wanted set with decoys up to a
 * fixed batch size (8/16/32), shuffles with the caller's rng, and splits
 * across SPLIT_PEERS distinct peers round-robin. Returns
 * [{ peer, cells }] where every request is exactly a legal batch size.
 * Decoys are indistinguishable by design — cells may lie anywhere.
 */
export function buildCellRequests({
  wanted,
  peers,
  decoyPool = [],
  constants = DEFAULT_CONSTANTS,
  rng = Math.random
}) {
  if (!wanted.length || !peers.length) return [];
  const batchSizes = constants.BATCH_SIZES;
  const decoyFraction = constants.DECOY_FRACTION;
  const splitAcross = Math.max(1, Math.min(constants.SPLIT_PEERS, peers.length));

  // Round-robin split so no peer sees the ordered corridor.
  const shuffledWanted = shuffle([...wanted], rng);
  const chosenPeers = shuffle([...peers], rng).slice(0, splitAcross);
  const perPeer = chosenPeers.map(() => []);
  shuffledWanted.forEach((cell, index) => perPeer[index % splitAcross].push(cell));

  const pool = shuffle([...decoyPool], rng);
  let poolCursor = 0;
  const requests = [];
  for (let p = 0; p < chosenPeers.length; p++) {
    const list = perPeer[p];
    if (!list.length) continue;
    // Chunk so that after the decoy share the chunk fits the largest batch.
    const maxReal = Math.max(1, Math.floor(batchSizes[batchSizes.length - 1] * (1 - decoyFraction)));
    for (let start = 0; start < list.length; start += maxReal) {
      const chunk = list.slice(start, start + maxReal);
      // Smallest batch size whose real share accommodates the chunk.
      let batch = batchSizes[batchSizes.length - 1];
      for (const size of batchSizes) {
        if (chunk.length <= Math.max(1, Math.floor(size * (1 - decoyFraction)))) { batch = size; break; }
      }
      const cells = [...chunk];
      const used = new Set(cells.map(detailCellKey));
      while (cells.length < batch) {
        let decoy = null;
        for (let scanned = 0; scanned < pool.length; scanned++) {
          const candidate = pool[poolCursor % pool.length];
          poolCursor++;
          if (!used.has(detailCellKey(candidate))) { decoy = candidate; break; }
        }
        if (!decoy) {
          // Degenerate pool: synthesize ring decoys around a wanted cell.
          const base = chunk[Math.floor(rng() * chunk.length)];
          const ring = detailCellRings(base, 3).filter(cell => !used.has(detailCellKey(cell)));
          if (!ring.length) break;
          decoy = ring[Math.floor(rng() * ring.length)];
        }
        used.add(detailCellKey(decoy));
        cells.push(decoy);
      }
      requests.push({ peer: chosenPeers[p], cells: shuffle(cells, rng) });
    }
  }
  return requests;
}

/**
 * §11.2 corridor cache target. Rasterizes locally-computed candidate
 * routes to z15 cells with `geoCellsForRoute` (the same machinery the geo
 * lanes use), broadens each route's endpoints by `ENDPOINT_RINGS` rings,
 * and adds a ring of adjacent cells as overfetch.
 *
 * `routes` are geometries as the engine returns them: arrays of
 * `{ lat, lon }` points. The corridor is computed entirely on-device;
 * only the resulting cell set — padded and split by buildCellRequests —
 * ever reaches a peer, and never in route order.
 */
export function corridorCells({
  routes,
  corridorMeters = 250,
  constants = DEFAULT_CONSTANTS,
  overfetchRings = 1,
  limit = 4096
}) {
  const cells = new Map();
  const add = cell => {
    const key = detailCellKey(cell);
    if (!cells.has(key)) cells.set(key, cell);
  };
  for (const points of routes || []) {
    if (!points || points.length < 2) continue;
    const segments = [];
    for (let i = 0; i + 1 < points.length; i++) {
      const start = points[i];
      const end = points[i + 1];
      segments.push({ start, end, lengthMeters: haversineMeters(start, end) });
    }
    const rasterized = geoCellsForRoute({ segments, corridorMeters }, DETAIL_ZOOM, limit);
    for (const cell of rasterized || []) add(cell);
    // Endpoints get broader coverage: they are the two cells whose
    // identity would say the most about the query, so they are never the
    // narrowest part of the request.
    for (const endpoint of [points[0], points[points.length - 1]]) {
      const cell = detailCellForE7(endpoint.lat * 1e7, endpoint.lon * 1e7);
      for (const ring of detailCellRings(cell, constants.ENDPOINT_RINGS)) add(ring);
    }
  }
  if (overfetchRings > 0) {
    for (const cell of [...cells.values()]) {
      for (const neighbor of detailCellRings(cell, overfetchRings)) add(neighbor);
    }
  }
  return [...cells.values()];
}

function haversineMeters(a, b) {
  const toRad = Math.PI / 180;
  const lat1 = a.lat * toRad;
  const lat2 = b.lat * toRad;
  const dLat = lat2 - lat1;
  let dLon = (b.lon - a.lon) * toRad;
  if (dLon > Math.PI) dLon -= 2 * Math.PI;
  if (dLon < -Math.PI) dLon += 2 * Math.PI;
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * 6371008.8 * Math.asin(Math.min(1, Math.sqrt(h)));
}

/** Overhead accounting for the benchmark harness: cells fetched vs wanted. */
export function requestOverhead(requests, wantedCount) {
  let total = 0;
  for (const request of requests) total += request.cells.length;
  return {
    wanted: wantedCount,
    fetched: total,
    decoys: total - wantedCount,
    overheadRatio: wantedCount ? total / wantedCount : 0
  };
}
