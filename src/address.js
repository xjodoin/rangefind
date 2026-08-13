import { foldMulti } from "./analysis_fold.js";
import {
  ADDRESS_TOKEN_READINGS,
  CONCATENATED_STREET_SUFFIXES,
  CONCATENATED_SUFFIX_READINGS,
  STREET_TYPE_TOKENS
} from "./address_abbreviations.js";

const DIRECTION_PHRASES = [
  [/\bnorth[\s-]*east\b/gu, "ne"],
  [/\bnorth[\s-]*west\b/gu, "nw"],
  [/\bsouth[\s-]*east\b/gu, "se"],
  [/\bsouth[\s-]*west\b/gu, "sw"]
];

export const TOKEN_ALIASES = new Map(Object.entries({
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
  // French-Canadian civic addresses abbreviate "boulevard" as "boul" or "bd"
  // ("311 Bd Cartier O"). OSM/RQA data spells the word out, so both spellings
  // must land on the same canonical token as "boulevard" itself.
  boul: "blvd",
  bd: "blvd",
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

// Ambiguous abbreviations expanded only on the query side: "St" means Saint
// or Street, a bare "O" in "311 Bd Cartier O" means Ouest but is also an
// initial ("Rue O'Brien" tokenizes to "o brien"), so these readings cannot be
// part of the deterministic key shared with the builder. Queries probe every
// reading instead. The table is generated from libpostal's en/fr address
// dictionaries plus curated saints, units, and state/province names.

const CANADIAN_POSTAL_FSA = /^[abceghj-nprstvxy]\d[abceghj-nprstvwxyz]$/u;
const CANADIAN_POSTAL_LDU = /^\d[abceghj-nprstvwxyz]\d$/u;
// Canonical street-suffix and directional tokens that legitimately end an
// address ("100 Main St"); a trailing region abbreviation ("QC", "NY") is
// droppable, these are not.
const TRAILING_KEEP_TOKENS = new Set([...new Set(TOKEN_ALIASES.values()), "o"]);

function isNumberToken(token) {
  return /^\d+$/u.test(token);
}

function withoutIndex(tokens, index) {
  return tokens.filter((_, tokenIndex) => tokenIndex !== index);
}

// Detached or attached unit letters: "311 A Bd Cartier" and "311A Bd Cartier"
// both describe house 311 (unit A) or housenumber "311A" depending on the
// data, so the query probes the merged, split, and dropped readings.
function unitLetterVariants(tokens) {
  const variants = [{ tokens, cost: 0 }];
  const numberIndex = tokens.findIndex(isNumberToken);
  if (numberIndex >= 0 && /^[a-z]$/u.test(tokens[numberIndex + 1] || "")) {
    variants.push({ tokens: withoutIndex(tokens, numberIndex + 1), cost: 1 });
    variants.push({
      tokens: tokens.map((token, index) => index === numberIndex ? `${token}${tokens[numberIndex + 1]}` : token)
        .filter((_, index) => index !== numberIndex + 1),
      cost: 1
    });
  }
  const compactIndex = tokens.findIndex(token => /^\d+[a-z]$/u.test(token));
  if (compactIndex >= 0) {
    variants.push({
      tokens: tokens.map((token, index) => index === compactIndex ? token.slice(0, -1) : token),
      cost: 1
    });
  }
  return variants;
}

// Alternative readings of ambiguous abbreviations, breadth-first by how many
// tokens are substituted: most queries abbreviate one thing, so all single
// substitutions come before any stacked pair. Multi-token readings ("no" ->
// "nord ouest") splice in place; substitution positions are bounded so a
// pathological query cannot explode the frontier.
// Readings for one token: the shared table, plus concatenated Germanic
// abbreviations rewritten in place ("hauptstr" -> "hauptstrasse",
// "kerkstr" -> "kerkstraat") — the stem stays, only the suffix expands.
function tokenReadings(token) {
  const readings = ADDRESS_TOKEN_READINGS.get(token);
  for (const [abbrev, expansions] of CONCATENATED_SUFFIX_READINGS) {
    if (token.length <= abbrev.length + 2 || !token.endsWith(abbrev)) continue;
    const stem = token.slice(0, -abbrev.length);
    return [
      ...(readings || []),
      ...expansions.map(expansion => [`${stem}${expansion}`])
    ];
  }
  return readings;
}

function readingsVariants(baseTokens, maxVariants = 12) {
  const positions = [];
  for (let index = 0; index < baseTokens.length && positions.length < 5; index++) {
    const readings = tokenReadings(baseTokens[index]);
    if (readings?.length) positions.push({ index, readings });
  }
  const variants = [{ tokens: baseTokens, cost: 0 }];
  if (!positions.length) return variants;
  const substitute = (tokens, entries) => {
    let out = tokens;
    // Apply from the highest index down so earlier positions stay valid when
    // a reading changes the token count.
    for (const { index, reading } of [...entries].sort((a, b) => b.index - a.index)) {
      out = [...out.slice(0, index), ...reading, ...out.slice(index + 1)];
    }
    return out;
  };
  for (const { index, readings } of positions) {
    // A lone letter carries almost no meaning of its own, so reading it as
    // its directional word ("O" -> Ouest, "E" -> Est) is the likeliest
    // interpretation and ranks ahead of rewriting a full abbreviation.
    const cost = baseTokens[index].length === 1 ? 0.5 : 1;
    for (const reading of readings) {
      variants.push({ tokens: substitute(baseTokens, [{ index, reading }]), cost });
      if (variants.length >= maxVariants) return variants;
    }
  }
  for (let left = 0; left < positions.length; left++) {
    for (let right = left + 1; right < positions.length; right++) {
      for (const leftReading of positions[left].readings) {
        for (const rightReading of positions[right].readings) {
          // Two independent abbreviations in one query are genuinely rarer
          // than one, so stacked substitutions rank behind every single
          // substitution combined with cheap tail shedding.
          variants.push({
            tokens: substitute(baseTokens, [
              { index: positions[left].index, reading: leftReading },
              { index: positions[right].index, reading: rightReading }
            ]),
            cost: 3
          });
          if (variants.length >= maxVariants) return variants;
        }
      }
    }
  }
  return variants;
}

const UNIT_MARKER_TOKENS = new Set(["unit", "apt", "ste", "app"]);

// The street types people actually omit, most common first. Canonical forms
// (post TOKEN_ALIASES), one insertion probe each. German/Dutch types are
// absent on purpose: they concatenate onto the name, so an omitted type
// changes the name token itself and insertion cannot reconstruct it.
const STREET_TYPE_INSERTIONS = [
  "st", "rue", "ave", "rd", "dr", "blvd", "ln", "chemin", "ct", "calle", "via", "rua"
];

// "214 libersan ste-thérèse": a house number with a bare street name. Every
// indexed key carries the street type, so a query that has a number but no
// type token probes the common types inserted after the number. A missing
// word is a bigger leap than reading an abbreviation, so insertions cost 2
// and rank behind every single substitution.
function streetTypeInsertionVariants(tokens) {
  const variants = [{ tokens, cost: 0 }];
  const numberIndex = tokens.findIndex(isNumberToken);
  if (numberIndex < 0 || tokens.length < 3) return variants;
  if (tokens.some(token => STREET_TYPE_TOKENS.has(token))) return variants;
  for (const type of STREET_TYPE_INSERTIONS) {
    variants.push({
      tokens: [...tokens.slice(0, numberIndex + 1), type, ...tokens.slice(numberIndex + 1)],
      cost: 2
    });
  }
  return variants;
}

// Germanic and Scandinavian street names concatenate their type suffix, so a
// spaced query ("Markt Straße 5") also probes the merged form the index
// derived from "Marktstraße". Runs after readings so "Markt Str" reaches
// "marktstrasse" through the strasse reading.
function concatenationVariants(tokens) {
  const variants = [{ tokens, cost: 0 }];
  for (let index = 1; index < tokens.length && variants.length <= 2; index++) {
    if (!CONCATENATED_STREET_SUFFIXES.has(tokens[index])) continue;
    const stem = tokens[index - 1];
    if (!/^[a-z]{3,}$/u.test(stem)) continue;
    variants.push({
      tokens: [
        ...tokens.slice(0, index - 1),
        `${stem}${tokens[index]}`,
        ...tokens.slice(index + 1)
      ],
      cost: 1
    });
  }
  return variants;
}

// Unit designations ("Apt 5", "#2F") are never part of an indexed component
// key, so they shed independently of the locality tail. A marker + number
// pair sheds together; a bare lettered number sheds only when another pure
// number remains to serve as the house number.
function unitShedVariants(tokens) {
  const markerIndex = tokens.findIndex((token, index) => (
    UNIT_MARKER_TOKENS.has(token) && /^\d+[a-z]?$/u.test(tokens[index + 1] || "")
  ));
  if (markerIndex >= 0) {
    return [tokens, tokens.filter((_, index) => index !== markerIndex && index !== markerIndex + 1)];
  }
  const letteredIndex = tokens.findIndex(token => /^\d+[a-z]$/u.test(token));
  if (
    letteredIndex >= 0
    && tokens.length > 2
    && tokens.some(isNumberToken)
  ) {
    return [tokens, withoutIndex(tokens, letteredIndex)];
  }
  return [tokens];
}

// A pasted address usually trails "…, City, Province Postal". The index keys
// component variants (base, base+locality, base+postcode, full formatted), so
// with and without its unit the query cumulatively sheds the postal code and
// then a short trailing region abbreviation to meet those variants; street
// suffixes and directionals are never shed.
function tailVariants(tokens) {
  const variants = [];
  const branches = unitShedVariants(tokens);
  for (let branch = 0; branch < branches.length; branch++) {
    let trimmed = branches[branch];
    // Pasted envelope forms routinely carry a postal/region tail the index
    // never keys, so shedding is half the cost of a substitution.
    let cost = branch;
    variants.push({ tokens: trimmed, cost });
    const push = next => {
      if (next.length && next.length !== trimmed.length) {
        trimmed = next;
        cost += 0.5;
        variants.push({ tokens: next, cost });
      }
    };
    const postalIndex = trimmed.findIndex((token, index) => (
      CANADIAN_POSTAL_FSA.test(token) && CANADIAN_POSTAL_LDU.test(trimmed[index + 1] || "")
    ));
    if (postalIndex >= 0) {
      push(trimmed.filter((_, index) => index !== postalIndex && index !== postalIndex + 1));
    } else if (
      trimmed.length > 1
      && /^\d{5}$/u.test(trimmed.at(-1))
      && trimmed.slice(0, -1).some(isNumberToken)
    ) {
      push(trimmed.slice(0, -1));
    }
    const last = trimmed.at(-1) || "";
    if (trimmed.length > 2 && /^[a-z]{2,3}$/u.test(last) && !TRAILING_KEEP_TOKENS.has(last)) {
      push(trimmed.slice(0, -1));
    }
  }
  return variants;
}

// Every plausible reading of a typed address, as ordered token lists,
// cheapest interpretation first: candidates are ranked by how many rewrites
// (readings substituted, suffixes merged, unit letters handled, tail tokens
// shed) separate them from the typed form, so a low-value stacked rewrite
// can never crowd a likely one out of the probe budget. The first entry is
// always the canonical normalizeAddressKey reading, so callers that only
// need one key keep their existing behavior.
export function addressQueryTokenVariants(value, maxVariants = 40) {
  const base = normalizeAddressKey(value);
  if (!base) return [];
  const candidates = [];
  for (const reading of readingsVariants(base.split(" "))) {
    for (const insertion of streetTypeInsertionVariants(reading.tokens)) {
      for (const concatenation of concatenationVariants(insertion.tokens)) {
        for (const unitVariant of unitLetterVariants(concatenation.tokens)) {
          for (const tail of tailVariants(unitVariant.tokens)) {
            candidates.push({
              tokens: tail.tokens,
              cost: reading.cost + insertion.cost + concatenation.cost + unitVariant.cost + tail.cost,
              order: candidates.length
            });
          }
        }
      }
    }
  }
  candidates.sort((left, right) => left.cost - right.cost || left.order - right.order);
  const seen = new Set();
  const out = [];
  for (const candidate of candidates) {
    const key = candidate.tokens.join(" ");
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(candidate.tokens);
    if (out.length >= maxVariants) break;
  }
  return out;
}

// Sorted set-like authority keys for every reading of a typed address. The
// builder still derives exactly one key per indexed value; probing extra keys
// at query time keeps abbreviated French forms ("311 A Bd Cartier O, Laval,
// QC H7N 2J3") resolvable against already-published indexes.
export function addressAuthorityQueryKeys(value) {
  const keys = [];
  const seen = new Set();
  for (const tokens of addressQueryTokenVariants(value)) {
    const key = tokens.slice().sort().join(" ");
    if (seen.has(key)) continue;
    seen.add(key);
    keys.push(key);
  }
  return keys;
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
// preserves reordered address queries and gracefully handles postal codes;
// query token variants keep abbreviated French forms resolvable.
export function addressRangeQueryCandidates(value, bucketSize = ADDRESS_RANGE_BUCKET_SIZE) {
  const candidates = [];
  const seen = new Set();
  // Interpolation probes pay two parity keys per candidate, so the variant
  // budget stays tighter than the flat authority lane's — but wide enough
  // that a street-type insertion stacked on one substitution ("214 libersan
  // ste-thérèse" -> "214 rue libersan sainte-thérèse") still makes the cut.
  // This lane only runs after the flat authority lane came up empty.
  for (const tokens of addressQueryTokenVariants(value, 24)) {
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
