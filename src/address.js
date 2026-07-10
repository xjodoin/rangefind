import { foldMulti } from "./analysis_fold.js";

const DIRECTION_PHRASES = [
  [/\bnorth[\s-]*east\b/gu, "ne"],
  [/\bnorth[\s-]*west\b/gu, "nw"],
  [/\bsouth[\s-]*east\b/gu, "se"],
  [/\bsouth[\s-]*west\b/gu, "sw"]
];

const TOKEN_ALIASES = new Map(Object.entries({
  north: "n",
  south: "s",
  east: "e",
  west: "w",
  northeast: "ne",
  northwest: "nw",
  southeast: "se",
  southwest: "sw",
  street: "st",
  str: "st",
  avenue: "ave",
  av: "ave",
  boulevard: "blvd",
  road: "rd",
  drive: "dr",
  lane: "ln",
  court: "ct",
  circle: "cir",
  highway: "hwy",
  parkway: "pkwy",
  place: "pl",
  plaza: "plz",
  square: "sq",
  terrace: "ter",
  trail: "trl",
  route: "rte",
  apartment: "apt",
  suite: "ste",
  unit: "unit"
}));

const SIMPLE_ORDINALS = new Map(Object.entries({
  first: 1,
  second: 2,
  third: 3,
  fourth: 4,
  fifth: 5,
  sixth: 6,
  seventh: 7,
  eighth: 8,
  ninth: 9,
  tenth: 10,
  eleventh: 11,
  twelfth: 12,
  thirteenth: 13,
  fourteenth: 14,
  fifteenth: 15,
  sixteenth: 16,
  seventeenth: 17,
  eighteenth: 18,
  nineteenth: 19
}));

const ORDINAL_TENS = new Map(Object.entries({
  twentieth: 20,
  thirtieth: 30,
  fortieth: 40,
  fiftieth: 50,
  sixtieth: 60,
  seventieth: 70,
  eightieth: 80,
  ninetieth: 90
}));

const CARDINAL_TENS = new Map(Object.entries({
  twenty: 20,
  thirty: 30,
  forty: 40,
  fifty: 50,
  sixty: 60,
  seventy: 70,
  eighty: 80,
  ninety: 90
}));

function ordinal(value) {
  const mod100 = value % 100;
  const suffix = mod100 >= 11 && mod100 <= 13
    ? "th"
    : value % 10 === 1
      ? "st"
      : value % 10 === 2
        ? "nd"
        : value % 10 === 3
          ? "rd"
          : "th";
  return `${value}${suffix}`;
}

function normalizeOrdinalTokens(tokens) {
  const out = [];
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index];
    const simple = SIMPLE_ORDINALS.get(token);
    if (simple != null) {
      out.push(ordinal(simple));
      continue;
    }
    const tensOrdinal = ORDINAL_TENS.get(token);
    if (tensOrdinal != null) {
      out.push(ordinal(tensOrdinal));
      continue;
    }
    const tens = CARDINAL_TENS.get(token);
    const nextOrdinal = SIMPLE_ORDINALS.get(tokens[index + 1]);
    if (tens != null && nextOrdinal != null && nextOrdinal < 10) {
      out.push(ordinal(tens + nextOrdinal));
      index++;
      continue;
    }
    out.push(token);
  }
  return out;
}

// A compact, deterministic address key shared by builders and runtimes.
// It intentionally normalizes only unambiguous address syntax: punctuation,
// compass directions, common street suffixes, unit markers, and ordinal
// street numbers. Locality/state names remain untouched to avoid false keys.
export function normalizeAddressKey(value) {
  let normalized = foldMulti(String(value || "")).toLowerCase();
  for (const [pattern, replacement] of DIRECTION_PHRASES) normalized = normalized.replace(pattern, replacement);
  const tokens = normalizeOrdinalTokens(normalized.match(/[a-z0-9]+/gu) || []);
  return tokens.map(token => TOKEN_ALIASES.get(token) || token).join(" ");
}

// Address component order varies by locale and by user input (street first vs
// locality first). Authority lookup needs set-like equality, so its single key
// sorts the already normalized tokens while retaining duplicates.
export function normalizeAddressAuthorityKey(value) {
  const key = normalizeAddressKey(value);
  return key ? key.split(" ").sort().join(" ") : "";
}

const CANADIAN_POSTAL_CODE = /\b([abceghj-nprstvxy]\d[abceghj-nprstvwxyz])\s*([0-9][abceghj-nprstvwxyz][0-9])\b/giu;
const CANADIAN_POSTAL_CODE_QUERY = /^\s*[abceghj-nprstvxy]\d[abceghj-nprstvwxyz]\s*[0-9][abceghj-nprstvwxyz][0-9]\s*$/iu;
const CANADIAN_POSTAL_CODE_PREFIX = /\b([abceghj-nprstvxy]\d[abceghj-nprstvwxyz])\s*([0-9](?:[abceghj-nprstvwxyz](?:[0-9])?)?)$/iu;

export function looksLikeAddressQuery(value) {
  if (CANADIAN_POSTAL_CODE_QUERY.test(String(value || ""))) return true;
  const key = normalizeAddressKey(value);
  if (!key) return false;
  const tokens = key.split(" ");
  if (tokens.length < 2) return false;
  return tokens.some(token => /^\d+[a-z]?(?:-\d+[a-z]?)?$/u.test(token) || /^\d+(?:st|nd|rd|th)$/u.test(token));
}

// The analyzer intentionally tokenizes letters and digits together, so a
// compact Canadian postal code is one term while the customary spaced form is
// two. Canonicalize queries to the indexed two-part surface without changing
// case or other address text.
export function normalizePostalCodeSpacing(value) {
  return String(value || "").replace(CANADIAN_POSTAL_CODE, "$1 $2");
}

// Autocomplete needs the same canonical boundary before all six characters
// have been entered; search itself remains strict to complete postal codes.
export function normalizePostalCodePrefixSpacing(value) {
  return String(value || "").replace(CANADIAN_POSTAL_CODE_PREFIX, "$1 $2");
}

export const ADDRESS_RANGE_BUCKET_SIZE = 16;

function sortedAddressTail(value) {
  const normalized = normalizeAddressKey(value);
  return normalized ? normalized.split(" ").sort().join(" ") : "";
}

function rangeLookupValue(bucket, parity, tail) {
  // Put the naturally diverse street/locality first so prefix sharding splits
  // national corpora before the low-entropy bucket/parity suffix.
  return `${tail} ${bucket.toString(36)} ${parity}`;
}

// A numeric interpolation range is indexed once per small number bucket, not
// once per inferred house. This keeps authority-index growth proportional to
// range span / bucket size while a lookup normally hydrates only one range doc.
export function addressRangeLookupValues(start, end, step, addressTails, bucketSize = ADDRESS_RANGE_BUCKET_SIZE) {
  const first = Number(start);
  const last = Number(end);
  const increment = Math.max(1, Math.floor(Number(step)));
  if (!Number.isInteger(first) || !Number.isInteger(last) || first === last) return [];
  const low = Math.min(first, last);
  const high = Math.max(first, last);
  const firstBucket = Math.floor(low / bucketSize);
  const lastBucket = Math.floor(high / bucketSize);
  const parities = increment % 2 === 0 ? [Math.abs(first % 2)] : [0, 1];
  const tails = [...new Set((addressTails || []).map(sortedAddressTail).filter(Boolean))];
  const values = [];
  for (let bucket = firstBucket; bucket <= lastBucket; bucket++) {
    for (const parity of parities) {
      for (const tail of tails) values.push(rangeLookupValue(bucket, parity, tail));
    }
  }
  return values;
}

// Return every plausible numeric house token. Considering all numeric tokens
// preserves reordered address queries and gracefully handles postal codes.
export function addressRangeQueryCandidates(value, bucketSize = ADDRESS_RANGE_BUCKET_SIZE) {
  const tokens = normalizeAddressKey(value).split(" ").filter(Boolean);
  const candidates = [];
  const seen = new Set();
  for (let index = 0; index < tokens.length; index++) {
    if (!/^\d+$/u.test(tokens[index])) continue;
    const houseNumber = Number(tokens[index]);
    if (!Number.isSafeInteger(houseNumber) || houseNumber < 0) continue;
    const tail = tokens.filter((_, tokenIndex) => tokenIndex !== index).sort().join(" ");
    if (!tail) continue;
    const parity = Math.abs(houseNumber % 2);
    // Probe both parity suffixes. Valid ranges match their natural parity;
    // probing the companion key also lets a complete opposite-parity range
    // prove an exact zero without adding duplicate keys to the index.
    for (const candidateParity of [parity, 1 - parity]) {
      const lookupValue = rangeLookupValue(
        Math.floor(houseNumber / bucketSize),
        candidateParity,
        tail
      );
      if (seen.has(lookupValue)) continue;
      seen.add(lookupValue);
      candidates.push({ houseNumber, lookupValue });
    }
  }
  return candidates;
}

export function addressRangeContains(start, end, step, houseNumber) {
  const first = Number(start);
  const last = Number(end);
  const increment = Math.max(1, Math.floor(Number(step)));
  const target = Number(houseNumber);
  if (![first, last, target].every(Number.isInteger) || first === last) return false;
  if (target < Math.min(first, last) || target > Math.max(first, last)) return false;
  return Math.abs(target - first) % increment === 0;
}

function encodeSigned(value) {
  let encoded = value < 0 ? ~(value << 1) : value << 1;
  let out = "";
  while (encoded >= 0x20) {
    out += String.fromCharCode((0x20 | (encoded & 0x1f)) + 63);
    encoded >>>= 5;
  }
  return out + String.fromCharCode(encoded + 63);
}

// Google-style delta polyline at 1e-6 degree precision (~11 cm). Keeping the
// geometry in the ordinary doc payload avoids a second range-request sidecar.
export function encodeAddressRangeGeometry(points) {
  let previousLat = 0;
  let previousLon = 0;
  let encoded = "";
  for (const point of points || []) {
    const lat = Math.round(Number(point.lat) * 1e6);
    const lon = Math.round(Number(point.lon) * 1e6);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    encoded += encodeSigned(lat - previousLat) + encodeSigned(lon - previousLon);
    previousLat = lat;
    previousLon = lon;
  }
  return encoded;
}

export function decodeAddressRangeGeometry(encoded) {
  const source = String(encoded || "");
  const points = [];
  let index = 0;
  let lat = 0;
  let lon = 0;
  function nextDelta() {
    let result = 0;
    let shift = 0;
    for (;;) {
      if (index >= source.length) throw new Error("Truncated address interpolation geometry.");
      const value = source.charCodeAt(index++) - 63;
      result |= (value & 0x1f) << shift;
      if (value < 0x20) break;
      shift += 5;
    }
    return result & 1 ? ~(result >>> 1) : result >>> 1;
  }
  while (index < source.length) {
    lat += nextDelta();
    lon += nextDelta();
    points.push({ lat: lat / 1e6, lon: lon / 1e6 });
  }
  return points;
}

function segmentLength(left, right) {
  const meanLat = (left.lat + right.lat) * Math.PI / 360;
  const dx = (right.lon - left.lon) * Math.cos(meanLat);
  const dy = right.lat - left.lat;
  return Math.hypot(dx, dy);
}

export function interpolateAddressRangePoint(encodedGeometry, start, end, houseNumber) {
  const points = decodeAddressRangeGeometry(encodedGeometry);
  if (!points.length) return null;
  if (points.length === 1) return points[0];
  const denominator = Number(end) - Number(start);
  const fraction = denominator ? Math.max(0, Math.min(1, (Number(houseNumber) - Number(start)) / denominator)) : 0;
  const lengths = new Array(points.length - 1);
  let total = 0;
  for (let index = 0; index < lengths.length; index++) {
    lengths[index] = segmentLength(points[index], points[index + 1]);
    total += lengths[index];
  }
  if (!total) return points[0];
  let remaining = total * fraction;
  for (let index = 0; index < lengths.length; index++) {
    if (remaining > lengths[index] && index + 1 < lengths.length) {
      remaining -= lengths[index];
      continue;
    }
    const local = lengths[index] ? remaining / lengths[index] : 0;
    return {
      lat: points[index].lat + (points[index + 1].lat - points[index].lat) * local,
      lon: points[index].lon + (points[index + 1].lon - points[index].lon) * local
    };
  }
  return points.at(-1);
}
