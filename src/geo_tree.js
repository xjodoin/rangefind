import {
  GEO_BRANCH_PAGE_MAGIC,
  GEO_LEAF_PAGE_MAGIC,
  GEO_TREE_ROOT_MAGIC,
  fixedWidth,
  pushVarint,
  readFixedInt,
  readVarint,
  writeFixedInt
} from "./binary.js";
import { assertMagic, pushUtf8, readBlockFilterSummary, readUtf8, writeBlockFilterSummary } from "./codec.js";
import { decodeDocPageColumns, encodeDocPageColumns } from "./doc_pages.js";

export const GEO_TREE_ROOT_FORMAT = "rfgeotreeroot-v1";
export const GEO_BRANCH_PAGE_FORMAT = "rfgeobranchpage-v1";
export const GEO_LEAF_PAGE_FORMAT = "rfgeoleafpage-v1";
export const GEO_LEAF_CAPSULE_PAGE_FORMAT = "rfgeoleafpage-v2";

const FORMAT_VERSION = 1;
const GEO_LEAF_CAPSULE_VERSION = 2;

export const LAT_E7_MIN = -900000000;
export const LAT_E7_MAX = 900000000;
export const LON_E7_MIN = -1800000000;
export const LON_E7_MAX = 1800000000;

const EARTH_RADIUS_METERS = 6371008.7714;
const E7 = 1e7;
const RADIANS_PER_E7 = Math.PI / (180 * E7);

export function latToE7(lat) {
  if (lat == null || lat === "") return null;
  const value = Math.round(Number(lat) * E7);
  if (!Number.isFinite(value) || value < LAT_E7_MIN || value > LAT_E7_MAX) return null;
  return value;
}

export function lonToE7(lon) {
  if (lon == null || lon === "") return null;
  const value = Math.round(Number(lon) * E7);
  if (!Number.isFinite(value) || value < LON_E7_MIN || value > LON_E7_MAX) return null;
  return value === LON_E7_MAX ? LON_E7_MIN : value;
}

export function e7ToDegrees(value) {
  return value / E7;
}

function pushZigzag(out, value) {
  pushVarint(out, value < 0 ? -2 * value - 1 : 2 * value);
}

function readZigzag(bytes, state) {
  const raw = readVarint(bytes, state);
  return raw % 2 === 0 ? raw / 2 : -(raw + 1) / 2;
}

export function haversineMeters(lat1, lon1, lat2, lon2) {
  const phi1 = lat1 * (Math.PI / 180);
  const phi2 = lat2 * (Math.PI / 180);
  const dPhi = phi2 - phi1;
  const dLambda = (lon2 - lon1) * (Math.PI / 180);
  const sinPhi = Math.sin(dPhi / 2);
  const sinLambda = Math.sin(dLambda / 2);
  const a = sinPhi * sinPhi + Math.cos(phi1) * Math.cos(phi2) * sinLambda * sinLambda;
  return 2 * EARTH_RADIUS_METERS * Math.asin(Math.min(1, Math.sqrt(a)));
}

export function haversineMetersE7(latE7A, lonE7A, latE7B, lonE7B) {
  const phi1 = latE7A * RADIANS_PER_E7;
  const phi2 = latE7B * RADIANS_PER_E7;
  const dPhi = (latE7B - latE7A) * RADIANS_PER_E7;
  const dLambda = (lonE7B - lonE7A) * RADIANS_PER_E7;
  const sinPhi = Math.sin(dPhi / 2);
  const sinLambda = Math.sin(dLambda / 2);
  const a = sinPhi * sinPhi + Math.cos(phi1) * Math.cos(phi2) * sinLambda * sinLambda;
  return 2 * EARTH_RADIUS_METERS * Math.asin(Math.min(1, Math.sqrt(a)));
}

function lonDeltaE7(lonE7A, lonE7B) {
  let delta = Math.abs(lonE7A - lonE7B);
  if (delta > LON_E7_MAX) delta = 2 * LON_E7_MAX - delta;
  return delta;
}

// Exact minimum great-circle distance from a point to a lat/lon-aligned box.
// Outside the box's longitude range the nearest boundary point lies on the
// closer constant-longitude edge: at the cross-track latitude
// atan(tan(phi) / cos(dLambda)) when the meridian faces the point
// (dLambda <= 90 degrees), otherwise at one of the edge endpoints.
export function pointToBoxDistanceMetersE7(latE7, lonE7, box) {
  const inLon = box.minLonE7 <= box.maxLonE7
    ? lonE7 >= box.minLonE7 && lonE7 <= box.maxLonE7
    : lonE7 >= box.minLonE7 || lonE7 <= box.maxLonE7;
  if (inLon && latE7 >= box.minLatE7 && latE7 <= box.maxLatE7) return 0;
  if (inLon) {
    // Directly north or south of the box: nearest point shares our longitude.
    const edgeLat = latE7 < box.minLatE7 ? box.minLatE7 : box.maxLatE7;
    return haversineMetersE7(latE7, lonE7, edgeLat, lonE7);
  }
  const nearLon = lonDeltaE7(lonE7, box.minLonE7) <= lonDeltaE7(lonE7, box.maxLonE7)
    ? box.minLonE7
    : box.maxLonE7;
  let best = Math.min(
    haversineMetersE7(latE7, lonE7, box.minLatE7, nearLon),
    haversineMetersE7(latE7, lonE7, box.maxLatE7, nearLon)
  );
  const dLambda = lonDeltaE7(lonE7, nearLon) * RADIANS_PER_E7;
  if (Math.cos(dLambda) > 0) {
    const phi = latE7 * RADIANS_PER_E7;
    const meridianLat = Math.atan(Math.tan(phi) / Math.cos(dLambda)) / RADIANS_PER_E7;
    const clampedLat = Math.max(box.minLatE7, Math.min(box.maxLatE7, meridianLat));
    best = Math.min(best, haversineMetersE7(latE7, lonE7, clampedLat, nearLon));
  }
  return best;
}

// Expands a radius query into one or two E7 boxes (two when the circle
// crosses the antimeridian). Latitude overflow at the poles widens the box to
// the full longitude range, which stays correct as a coarse prune.
export function boxesForRadiusE7(latE7, lonE7, radiusMeters) {
  const radius = Math.max(0, Number(radiusMeters) || 0);
  const latDeltaE7 = Math.ceil((radius / EARTH_RADIUS_METERS) / RADIANS_PER_E7);
  const minLatE7 = Math.max(LAT_E7_MIN, latE7 - latDeltaE7);
  const maxLatE7 = Math.min(LAT_E7_MAX, latE7 + latDeltaE7);
  const sinRatio = Math.sin(Math.min(Math.PI / 2, radius / EARTH_RADIUS_METERS));
  const cosLat = Math.cos(latE7 * RADIANS_PER_E7);
  if (latE7 - latDeltaE7 < LAT_E7_MIN || latE7 + latDeltaE7 > LAT_E7_MAX || sinRatio >= cosLat) {
    return [{ minLatE7, maxLatE7, minLonE7: LON_E7_MIN, maxLonE7: LON_E7_MAX }];
  }
  const lonDelta = Math.ceil(Math.asin(sinRatio / cosLat) / RADIANS_PER_E7);
  let minLonE7 = lonE7 - lonDelta;
  let maxLonE7 = lonE7 + lonDelta;
  if (minLonE7 < LON_E7_MIN && maxLonE7 > LON_E7_MAX) {
    return [{ minLatE7, maxLatE7, minLonE7: LON_E7_MIN, maxLonE7: LON_E7_MAX }];
  }
  if (minLonE7 < LON_E7_MIN) {
    return [
      { minLatE7, maxLatE7, minLonE7: LON_E7_MIN, maxLonE7 },
      { minLatE7, maxLatE7, minLonE7: minLonE7 + 2 * LON_E7_MAX, maxLonE7: LON_E7_MAX }
    ];
  }
  if (maxLonE7 > LON_E7_MAX) {
    return [
      { minLatE7, maxLatE7, minLonE7, maxLonE7: LON_E7_MAX },
      { minLatE7, maxLatE7, minLonE7: LON_E7_MIN, maxLonE7: maxLonE7 - 2 * LON_E7_MAX }
    ];
  }
  return [{ minLatE7, maxLatE7, minLonE7, maxLonE7 }];
}

export function boxIntersectsE7(a, b) {
  if (a.maxLatE7 < b.minLatE7 || a.minLatE7 > b.maxLatE7) return false;
  return a.maxLonE7 >= b.minLonE7 && a.minLonE7 <= b.maxLonE7;
}

export function boxContainsBoxE7(outer, inner) {
  return inner.minLatE7 >= outer.minLatE7 && inner.maxLatE7 <= outer.maxLatE7
    && inner.minLonE7 >= outer.minLonE7 && inner.maxLonE7 <= outer.maxLonE7;
}

export function boxContainsPointE7(box, latE7, lonE7) {
  return latE7 >= box.minLatE7 && latE7 <= box.maxLatE7
    && lonE7 >= box.minLonE7 && lonE7 <= box.maxLonE7;
}

// Farthest distance from a point to anywhere inside a box; used to prove a
// whole leaf lies inside a radius so per-point verification can be skipped.
// The farthest point sits at a corner, at the antipodal longitude when the
// box spans it, or at the interior critical latitude of a facing-away
// longitude (cos(dLambda) < 0), so all three candidate sets are checked.
export function pointToBoxMaxDistanceMetersE7(latE7, lonE7, box) {
  const lonCandidates = [box.minLonE7, box.maxLonE7];
  let antipodeLon = lonE7 + 1800000000;
  if (antipodeLon >= LON_E7_MAX) antipodeLon -= 2 * LON_E7_MAX;
  const antipodeInRange = box.minLonE7 <= box.maxLonE7
    ? antipodeLon >= box.minLonE7 && antipodeLon <= box.maxLonE7
    : antipodeLon >= box.minLonE7 || antipodeLon <= box.maxLonE7;
  if (antipodeInRange) lonCandidates.push(antipodeLon);
  let max = 0;
  for (const candidateLon of lonCandidates) {
    const dLambda = lonDeltaE7(lonE7, candidateLon) * RADIANS_PER_E7;
    const latCandidates = [box.minLatE7, box.maxLatE7];
    if (Math.cos(dLambda) < 0) {
      const critical = Math.atan(Math.tan(latE7 * RADIANS_PER_E7) / Math.cos(dLambda)) / RADIANS_PER_E7;
      if (critical >= box.minLatE7 && critical <= box.maxLatE7) latCandidates.push(critical);
    }
    for (const candidateLat of latCandidates) {
      const distance = haversineMetersE7(latE7, lonE7, candidateLat, candidateLon);
      if (distance > max) max = distance;
    }
  }
  return max;
}

// Bulk-loads a static KD tree over parallel typed arrays, splitting on the
// physically wider dimension (longitude widths shrink with cos(latitude)).
// Reorders the arrays in place and returns depth-first leaf descriptors.
export function buildGeoTreeLeaves(latsE7, lonsE7, docs, leafSize) {
  const count = docs.length;
  const target = Math.max(16, Math.floor(leafSize) || 512);
  const leaves = [];
  if (!count) return leaves;

  function swap(i, j) {
    const lat = latsE7[i];
    latsE7[i] = latsE7[j];
    latsE7[j] = lat;
    const lon = lonsE7[i];
    lonsE7[i] = lonsE7[j];
    lonsE7[j] = lon;
    const doc = docs[i];
    docs[i] = docs[j];
    docs[j] = doc;
  }

  // Hoare-style quickselect so values[start..mid] <= values[mid..end).
  function partitionAt(values, start, end, mid) {
    let lo = start;
    let hi = end - 1;
    while (lo < hi) {
      const pivot = values[(lo + hi) >> 1];
      let i = lo;
      let j = hi;
      while (i <= j) {
        while (values[i] < pivot) i++;
        while (values[j] > pivot) j--;
        if (i <= j) {
          swap(i, j);
          i++;
          j--;
        }
      }
      if (mid <= j) hi = j;
      else if (mid >= i) lo = i;
      else break;
    }
  }

  function bboxOf(start, end) {
    let minLat = latsE7[start];
    let maxLat = minLat;
    let minLon = lonsE7[start];
    let maxLon = minLon;
    for (let i = start + 1; i < end; i++) {
      const lat = latsE7[i];
      const lon = lonsE7[i];
      if (lat < minLat) minLat = lat;
      else if (lat > maxLat) maxLat = lat;
      if (lon < minLon) minLon = lon;
      else if (lon > maxLon) maxLon = lon;
    }
    return { minLatE7: minLat, maxLatE7: maxLat, minLonE7: minLon, maxLonE7: maxLon };
  }

  const stack = [[0, count]];
  const ordered = [];
  while (stack.length) {
    const [start, end] = stack.pop();
    if (end - start <= target) {
      ordered.push([start, end]);
      continue;
    }
    const box = bboxOf(start, end);
    const latSpan = box.maxLatE7 - box.minLatE7;
    const midLat = (box.minLatE7 + box.maxLatE7) / 2;
    const lonSpan = (box.maxLonE7 - box.minLonE7) * Math.max(0.05, Math.cos(midLat * RADIANS_PER_E7));
    const mid = (start + end) >> 1;
    partitionAt(latSpan >= lonSpan ? latsE7 : lonsE7, start, end, mid);
    // Push the right half first so the depth-first order goes left to right.
    stack.push([mid, end]);
    stack.push([start, mid]);
  }
  ordered.sort((a, b) => a[0] - b[0]);
  for (const [start, end] of ordered) {
    leaves.push({ start, end, count: end - start, ...bboxOf(start, end) });
  }
  return leaves;
}

export function encodeGeoLeafPage(fieldName, latsE7, lonsE7, docs, leaf, options = {}) {
  const capsuleFields = Array.isArray(options.capsuleFields)
    ? options.capsuleFields.map(String).filter(Boolean)
    : [];
  const capsules = Array.isArray(options.capsules) ? options.capsules : null;
  if ((capsules && !capsuleFields.length) || (!capsules && capsuleFields.length)) {
    throw new Error("Rangefind geo leaf capsules need both capsules and capsuleFields.");
  }
  const count = leaf.count;
  if (capsules && capsules.length !== count) {
    throw new Error(`Rangefind geo leaf capsule row count ${capsules.length} does not match point count ${count}.`);
  }
  const version = capsules ? GEO_LEAF_CAPSULE_VERSION : FORMAT_VERSION;
  const header = [...GEO_LEAF_PAGE_MAGIC];
  pushVarint(header, version);
  pushUtf8(header, fieldName);
  pushVarint(header, count);
  pushZigzag(header, leaf.minLatE7);
  pushVarint(header, leaf.maxLatE7 - leaf.minLatE7);
  pushZigzag(header, leaf.minLonE7);
  pushVarint(header, leaf.maxLonE7 - leaf.minLonE7);
  let maxLatDelta = 0;
  let maxLonDelta = 0;
  let maxDoc = 0;
  for (let i = leaf.start; i < leaf.end; i++) {
    const latDelta = latsE7[i] - leaf.minLatE7;
    const lonDelta = lonsE7[i] - leaf.minLonE7;
    if (latDelta > maxLatDelta) maxLatDelta = latDelta;
    if (lonDelta > maxLonDelta) maxLonDelta = lonDelta;
    if (docs[i] > maxDoc) maxDoc = docs[i];
  }
  const latWidth = fixedWidth([maxLatDelta]);
  const lonWidth = fixedWidth([maxLonDelta]);
  const docWidth = fixedWidth([maxDoc]);
  header.push(latWidth, lonWidth, docWidth);
  if (capsules) {
    pushVarint(header, capsuleFields.length);
    for (const field of capsuleFields) pushUtf8(header, field);
  }
  const body = Buffer.alloc(count * (latWidth + lonWidth + docWidth));
  let pos = 0;
  for (let i = leaf.start; i < leaf.end; i++, pos += latWidth) {
    writeFixedInt(body, pos, latWidth, latsE7[i] - leaf.minLatE7);
  }
  for (let i = leaf.start; i < leaf.end; i++, pos += lonWidth) {
    writeFixedInt(body, pos, lonWidth, lonsE7[i] - leaf.minLonE7);
  }
  for (let i = leaf.start; i < leaf.end; i++, pos += docWidth) {
    writeFixedInt(body, pos, docWidth, docs[i]);
  }
  const capsuleBody = capsules
    ? Buffer.from(encodeDocPageColumns(capsules, capsuleFields))
    : null;
  return capsuleBody
    ? Buffer.concat([Buffer.from(Uint8Array.from(header)), body, capsuleBody])
    : Buffer.concat([Buffer.from(Uint8Array.from(header)), body]);
}

export function decodeGeoLeafPage(buffer, expected = {}) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, GEO_LEAF_PAGE_MAGIC, "Unsupported Rangefind geo leaf page");
  const state = { pos: GEO_LEAF_PAGE_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== FORMAT_VERSION && version !== GEO_LEAF_CAPSULE_VERSION) {
    throw new Error(`Unsupported Rangefind geo leaf page version ${version}`);
  }
  const field = readUtf8(bytes, state);
  if (expected.name && expected.name !== field) throw new Error(`Rangefind geo leaf page field mismatch: ${field}`);
  const count = readVarint(bytes, state);
  const minLatE7 = readZigzag(bytes, state);
  const maxLatE7 = minLatE7 + readVarint(bytes, state);
  const minLonE7 = readZigzag(bytes, state);
  const maxLonE7 = minLonE7 + readVarint(bytes, state);
  const latWidth = bytes[state.pos++];
  const lonWidth = bytes[state.pos++];
  const docWidth = bytes[state.pos++];
  const capsuleFields = version >= GEO_LEAF_CAPSULE_VERSION
    ? Array.from({ length: readVarint(bytes, state) }, () => readUtf8(bytes, state))
    : [];
  const latsE7 = new Int32Array(count);
  const lonsE7 = new Int32Array(count);
  const docs = new Uint32Array(count);
  let pos = state.pos;
  for (let i = 0; i < count; i++, pos += latWidth) latsE7[i] = minLatE7 + readFixedInt(bytes, pos, latWidth);
  for (let i = 0; i < count; i++, pos += lonWidth) lonsE7[i] = minLonE7 + readFixedInt(bytes, pos, lonWidth);
  for (let i = 0; i < count; i++, pos += docWidth) docs[i] = readFixedInt(bytes, pos, docWidth);
  let capsules = null;
  if (version >= GEO_LEAF_CAPSULE_VERSION) {
    capsules = decodeDocPageColumns(bytes.subarray(pos), capsuleFields, 0);
    if (capsules.length !== count) {
      throw new Error(`Rangefind geo leaf capsule row count ${capsules.length} does not match point count ${count}.`);
    }
    for (const capsule of capsules) delete capsule.index;
    pos = bytes.length;
  }
  if (pos !== bytes.length) throw new Error("Rangefind geo leaf page has trailing bytes.");
  return {
    field,
    count,
    minLatE7,
    maxLatE7,
    minLonE7,
    maxLonE7,
    latsE7,
    lonsE7,
    docs,
    capsuleFields,
    capsules
  };
}

function pushObjectPointer(out, entry, packIndexes) {
  pushVarint(out, packIndexes.get(entry.pack) ?? 0);
  pushVarint(out, entry.offset);
  pushVarint(out, entry.length);
  pushVarint(out, entry.physicalLength || entry.length);
  pushVarint(out, entry.logicalLength || 0);
  pushUtf8(out, entry.checksum?.algorithm || "");
  pushUtf8(out, entry.checksum?.value || "");
}

function readObjectPointer(bytes, state, packTable, target) {
  const packIndex = readVarint(bytes, state);
  target.pack = packTable[packIndex];
  target.offset = readVarint(bytes, state);
  target.length = readVarint(bytes, state);
  target.physicalLength = readVarint(bytes, state);
  target.logicalLength = readVarint(bytes, state) || null;
  const algorithm = readUtf8(bytes, state);
  const value = readUtf8(bytes, state);
  target.checksum = value ? { algorithm: algorithm || "sha256", value } : null;
  return target;
}

// Filter summaries mirror posting-block summaries so the runtime can prune
// leaves and branches with the existing blockMayPass logic. A complete
// default is written for missing entries; partial summaries would desync the
// fixed field-order layout on read.
function completeFilterSummary(blockFilters, summary) {
  const out = {};
  for (const filter of blockFilters) {
    const item = summary?.[filter.name];
    if (filter.kind === "facet") {
      out[filter.name] = { words: item?.words?.length === filter.words ? item.words : new Array(filter.words).fill(0) };
    } else {
      out[filter.name] = { min: item?.min ?? null, max: item?.max ?? null };
    }
  }
  return out;
}

export function mergeBlockFilterSummaries(blockFilters, summaries) {
  const merged = completeFilterSummary(blockFilters, null);
  for (const summary of summaries) {
    for (const filter of blockFilters) {
      const item = summary?.[filter.name];
      const target = merged[filter.name];
      if (filter.kind === "facet") {
        for (let w = 0; w < filter.words; w++) {
          target.words[w] = ((target.words[w] || 0) | (item?.words?.[w] || 0)) >>> 0;
        }
      } else {
        if (Number.isFinite(item?.min)) target.min = target.min == null ? item.min : Math.min(target.min, item.min);
        if (Number.isFinite(item?.max)) target.max = target.max == null ? item.max : Math.max(target.max, item.max);
      }
    }
  }
  return merged;
}

function pushFilterSummary(out, blockFilters, summary) {
  if (!blockFilters?.length) return;
  writeBlockFilterSummary(out, blockFilters, completeFilterSummary(blockFilters, summary));
}

function readFilterSummary(bytes, state, blockFilters) {
  if (!blockFilters?.length) return null;
  return readBlockFilterSummary(bytes, state, { block_filters: blockFilters });
}

function pushLeafEntry(out, leaf, base, packIndexes, blockFilters) {
  pushVarint(out, leaf.minLatE7 - base.minLatE7);
  pushVarint(out, leaf.maxLatE7 - leaf.minLatE7);
  pushVarint(out, leaf.minLonE7 - base.minLonE7);
  pushVarint(out, leaf.maxLonE7 - leaf.minLonE7);
  pushVarint(out, leaf.count);
  pushObjectPointer(out, leaf.entry, packIndexes);
  pushFilterSummary(out, blockFilters, leaf.summary);
}

function readLeafEntry(bytes, state, base, packTable, index, blockFilters) {
  const minLatE7 = base.minLatE7 + readVarint(bytes, state);
  const maxLatE7 = minLatE7 + readVarint(bytes, state);
  const minLonE7 = base.minLonE7 + readVarint(bytes, state);
  const maxLonE7 = minLonE7 + readVarint(bytes, state);
  const count = readVarint(bytes, state);
  const leaf = readObjectPointer(bytes, state, packTable, {
    index,
    minLatE7,
    maxLatE7,
    minLonE7,
    maxLonE7,
    count
  });
  leaf.filters = readFilterSummary(bytes, state, blockFilters);
  return leaf;
}

// The tree root is single-level (leaf entries inline) for small point sets
// and two-level for large ones: the root lists branch entries whose leaf
// tables live in lazily fetched, range-addressed branch pages.
export function encodeGeoTreeRoot({ field, total, leafSize, leafCount, bbox, leaves, branches, packTable, packIndexes, blockFilters }) {
  const out = [...GEO_TREE_ROOT_MAGIC];
  pushVarint(out, FORMAT_VERSION);
  pushUtf8(out, field);
  pushVarint(out, total);
  pushVarint(out, leafSize);
  pushVarint(out, leafCount ?? (leaves ? leaves.length : 0));
  pushZigzag(out, bbox.minLatE7);
  pushVarint(out, bbox.maxLatE7 - bbox.minLatE7);
  pushZigzag(out, bbox.minLonE7);
  pushVarint(out, bbox.maxLonE7 - bbox.minLonE7);
  pushVarint(out, packTable.length);
  for (const pack of packTable) pushUtf8(out, pack);
  const levels = branches ? 2 : 1;
  out.push(levels);
  const summaryFilters = blockFilters?.length ? blockFilters : null;
  out.push(summaryFilters ? 1 : 0);
  if (levels === 1) {
    pushVarint(out, leaves.length);
    for (const leaf of leaves) pushLeafEntry(out, leaf, bbox, packIndexes, summaryFilters);
  } else {
    pushVarint(out, branches.length);
    for (const branch of branches) {
      pushVarint(out, branch.minLatE7 - bbox.minLatE7);
      pushVarint(out, branch.maxLatE7 - branch.minLatE7);
      pushVarint(out, branch.minLonE7 - bbox.minLonE7);
      pushVarint(out, branch.maxLonE7 - branch.minLonE7);
      pushVarint(out, branch.count);
      pushVarint(out, branch.firstLeafIndex);
      pushVarint(out, branch.leafCount);
      pushObjectPointer(out, branch.entry, packIndexes);
      pushFilterSummary(out, summaryFilters, branch.summary);
    }
  }
  return {
    buffer: Buffer.from(Uint8Array.from(out)),
    meta: {
      format: GEO_TREE_ROOT_FORMAT,
      page_format: GEO_LEAF_PAGE_FORMAT,
      field,
      total,
      leaf_size: leafSize,
      leaves: leafCount ?? (leaves ? leaves.length : 0),
      levels,
      branches: branches ? branches.length : 0
    }
  };
}

export function parseGeoTreeRoot(buffer, blockFilters = []) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, GEO_TREE_ROOT_MAGIC, "Unsupported Rangefind geo tree root");
  const state = { pos: GEO_TREE_ROOT_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== FORMAT_VERSION) throw new Error(`Unsupported Rangefind geo tree root version ${version}`);
  const field = readUtf8(bytes, state);
  const total = readVarint(bytes, state);
  const leafSize = readVarint(bytes, state);
  const leafCount = readVarint(bytes, state);
  const minLatE7 = readZigzag(bytes, state);
  const maxLatE7 = minLatE7 + readVarint(bytes, state);
  const minLonE7 = readZigzag(bytes, state);
  const maxLonE7 = minLonE7 + readVarint(bytes, state);
  const bbox = { minLatE7, maxLatE7, minLonE7, maxLonE7 };
  const packCount = readVarint(bytes, state);
  const packTable = new Array(packCount);
  for (let i = 0; i < packCount; i++) packTable[i] = readUtf8(bytes, state);
  const levels = bytes[state.pos++];
  const hasSummaries = bytes[state.pos++] === 1;
  if (hasSummaries && !blockFilters?.length) {
    throw new Error("Rangefind geo tree root has filter summaries but no block_filters manifest was provided.");
  }
  const summaryFilters = hasSummaries ? blockFilters : null;
  const root = {
    format: GEO_TREE_ROOT_FORMAT,
    pageFormat: GEO_LEAF_PAGE_FORMAT,
    field,
    total,
    leafSize,
    leafCount,
    levels,
    hasSummaries,
    bbox,
    packTable,
    leaves: null,
    branches: null
  };
  if (levels === 1) {
    const count = readVarint(bytes, state);
    const leaves = new Array(count);
    for (let i = 0; i < count; i++) leaves[i] = readLeafEntry(bytes, state, bbox, packTable, i, summaryFilters);
    root.leaves = leaves;
  } else if (levels === 2) {
    const count = readVarint(bytes, state);
    const branches = new Array(count);
    for (let i = 0; i < count; i++) {
      const branchMinLat = minLatE7 + readVarint(bytes, state);
      const branchMaxLat = branchMinLat + readVarint(bytes, state);
      const branchMinLon = minLonE7 + readVarint(bytes, state);
      const branchMaxLon = branchMinLon + readVarint(bytes, state);
      const pointCount = readVarint(bytes, state);
      const firstLeafIndex = readVarint(bytes, state);
      const branchLeafCount = readVarint(bytes, state);
      const branch = readObjectPointer(bytes, state, packTable, {
        index: i,
        minLatE7: branchMinLat,
        maxLatE7: branchMaxLat,
        minLonE7: branchMinLon,
        maxLonE7: branchMaxLon,
        count: pointCount,
        firstLeafIndex,
        leafCount: branchLeafCount
      });
      branch.filters = readFilterSummary(bytes, state, summaryFilters);
      branches[i] = branch;
    }
    root.branches = branches;
  } else {
    throw new Error(`Unsupported Rangefind geo tree root levels ${levels}`);
  }
  if (state.pos !== bytes.length) throw new Error("Rangefind geo tree root has trailing bytes.");
  return root;
}

export function encodeGeoBranchPage({ field, branchIndex, firstLeafIndex, bbox, leaves, packIndexes, blockFilters }) {
  const out = [...GEO_BRANCH_PAGE_MAGIC];
  pushVarint(out, FORMAT_VERSION);
  pushUtf8(out, field);
  pushVarint(out, branchIndex);
  pushVarint(out, firstLeafIndex);
  pushZigzag(out, bbox.minLatE7);
  pushVarint(out, bbox.maxLatE7 - bbox.minLatE7);
  pushZigzag(out, bbox.minLonE7);
  pushVarint(out, bbox.maxLonE7 - bbox.minLonE7);
  const summaryFilters = blockFilters?.length ? blockFilters : null;
  out.push(summaryFilters ? 1 : 0);
  pushVarint(out, leaves.length);
  for (const leaf of leaves) pushLeafEntry(out, leaf, bbox, packIndexes, summaryFilters);
  return Buffer.from(Uint8Array.from(out));
}

export function decodeGeoBranchPage(buffer, packTable, blockFilters = [], expected = {}) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, GEO_BRANCH_PAGE_MAGIC, "Unsupported Rangefind geo branch page");
  const state = { pos: GEO_BRANCH_PAGE_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== FORMAT_VERSION) throw new Error(`Unsupported Rangefind geo branch page version ${version}`);
  const field = readUtf8(bytes, state);
  if (expected.name && expected.name !== field) throw new Error(`Rangefind geo branch page field mismatch: ${field}`);
  const branchIndex = readVarint(bytes, state);
  const firstLeafIndex = readVarint(bytes, state);
  const minLatE7 = readZigzag(bytes, state);
  const maxLatE7 = minLatE7 + readVarint(bytes, state);
  const minLonE7 = readZigzag(bytes, state);
  const maxLonE7 = minLonE7 + readVarint(bytes, state);
  const bbox = { minLatE7, maxLatE7, minLonE7, maxLonE7 };
  const hasSummaries = bytes[state.pos++] === 1;
  if (hasSummaries && !blockFilters?.length) {
    throw new Error("Rangefind geo branch page has filter summaries but no block_filters manifest was provided.");
  }
  const summaryFilters = hasSummaries ? blockFilters : null;
  const count = readVarint(bytes, state);
  const leaves = new Array(count);
  for (let i = 0; i < count; i++) {
    leaves[i] = readLeafEntry(bytes, state, bbox, packTable, firstLeafIndex + i, summaryFilters);
  }
  if (state.pos !== bytes.length) throw new Error("Rangefind geo branch page has trailing bytes.");
  return { field, branchIndex, firstLeafIndex, bbox, leaves };
}
