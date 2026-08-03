import {
  addressRangeLookupValues,
  encodeAddressRangeGeometry,
  interpolateAddressRangePoint,
  normalizeAddressKey,
  normalizePostalCodeSpacing
} from "../../address.js";
import { foldMulti } from "../../analysis_fold.js";
import { encodePolyline } from "../../geo_route.js";

// Primary OSM keys that make a feature a searchable place, in ranking order.
const CATEGORY_KEYS = [
  "place", "amenity", "shop", "tourism", "leisure", "historic",
  "healthcare", "office", "craft", "man_made", "natural", "railway",
  "aeroway", "emergency", "sport"
];

// Values that describe map plumbing rather than searchable places.
const NOISE_TYPES = new Set([
  "yes", "no", "tree", "rock", "stone", "survey_point", "switch",
  "signal", "milestone", "level_crossing", "crossing", "buffer_stop",
  "fire_hydrant", "street_lamp", "waste_basket", "grit_bin",
  "parking_space", "tree_row"
]);

// Searchable identities in selection order. OSM supports language-qualified
// variants of the name keys (for example `official_name:fr`) and occasionally
// stores several alternatives in one semicolon-separated value. Keep the
// priorities explicit so a mapper/PBF tag-order change cannot decide which
// names survive the bounded alias budget.
const ALIAS_KEY_PRIORITY = new Map([
  ["short_name", 10], ["alt_name", 20], ["loc_name", 30],
  ["nickname", 35], ["int_name", 40], ["nat_name", 45],
  ["reg_name", 50], ["official_name", 55], ["full_name", 60],
  ["brand", 65], ["ref_name", 70], ["bridge:name", 75],
  ["tunnel:name", 75], ["name:left", 80], ["name:right", 80],
  ["old_name", 90], ["operator", 100], ["ref", 110],
  ["sorting_name", 120]
]);
const ALIAS_KEYS = [...ALIAS_KEY_PRIORITY.keys()];
const FALLBACK_NAME_PRIORITY = new Map([
  ["official_name", 10], ["loc_name", 20], ["int_name", 30],
  ["nat_name", 35], ["reg_name", 40], ["full_name", 45],
  ["brand", 50], ["short_name", 55], ["alt_name", 60],
  ["nickname", 65], ["ref_name", 70], ["bridge:name", 75],
  ["tunnel:name", 75], ["name:left", 80], ["name:right", 80]
]);
const NON_ALIAS_NAME_SUFFIXES = new Set([
  "etymology", "language", "pronunciation", "signed", "source",
  "prefix", "suffix", "conditional"
]);
const LANGUAGE_SUFFIX_RE = /^[a-z]{2,3}(?:-[a-z0-9]{2,8})*$/iu;
const HISTORICAL_NAME_SUFFIX_RE = /^\d{4}(?:(?:-|--)\d{0,4})?$/u;
const MAX_SEARCH_ALIASES = 24;

const ADDRESS_TAG_KEYS = [
  "addr:street", "addr:housenumber", "addr:city", "addr:town",
  "addr:village", "addr:hamlet", "addr:place", "addr:full", "addr:unit",
  "addr:flats", "addr:suburb", "addr:district", "addr:county",
  "addr:state", "addr:province", "addr:postcode", "addr:country"
];

const WAY_DOC_KEYS = new Set([
  "name", ...CATEGORY_KEYS, ...ALIAS_KEYS, ...ADDRESS_TAG_KEYS,
  "addr:interpolation", "addr:inclusion", "cuisine", "population",
  "highway",
  "capital", "admin_level", "importance", "wikidata", "wikipedia",
  "opening_hours", "opening_hours:kitchen", "phone", "contact:phone",
  "website", "contact:website", "email", "contact:email", "wheelchair",
  "toilets:wheelchair", "internet_access", "outdoor_seating", "takeaway",
  "delivery", "drive_through", "reservation", "payment:cash",
  "payment:credit_cards", "payment:contactless", "capacity", "stars",
  "fee", "access", "smoking", "entrance", "level", "building", "area"
]);

const DETAIL_TAGS = Object.freeze({
  brand: ["brand"],
  operator: ["operator"],
  cuisine: ["cuisine"],
  opening_hours: ["opening_hours"],
  kitchen_hours: ["opening_hours:kitchen"],
  phone: ["contact:phone", "phone"],
  website: ["contact:website", "website"],
  email: ["contact:email", "email"],
  wheelchair: ["wheelchair"],
  toilets_wheelchair: ["toilets:wheelchair"],
  internet_access: ["internet_access"],
  outdoor_seating: ["outdoor_seating"],
  takeaway: ["takeaway"],
  delivery: ["delivery"],
  drive_through: ["drive_through"],
  reservation: ["reservation"],
  payment_cash: ["payment:cash"],
  payment_cards: ["payment:credit_cards"],
  payment_contactless: ["payment:contactless"],
  capacity: ["capacity"],
  stars: ["stars"],
  fee: ["fee"],
  access: ["access"],
  smoking: ["smoking"],
  wikidata: ["wikidata"],
  wikipedia: ["wikipedia"],
  entrance: ["entrance"],
  level: ["level"]
});

const PLACE_PROMINENCE = new Map([
  ["country", 0.98], ["state", 0.9], ["province", 0.88],
  ["region", 0.82], ["city", 0.72], ["town", 0.55],
  ["municipality", 0.5], ["village", 0.38], ["suburb", 0.28],
  ["quarter", 0.23], ["neighbourhood", 0.18], ["hamlet", 0.14],
  ["locality", 0.1]
]);

// OSM tag values can contain raw newlines and U+2028/U+2029 line separators,
// which JSON.stringify leaves unescaped and line-based JSONL readers split on.
function cleanText(value) {
  return String(value || "").replaceAll(/[\u0000-\u001f\u0085\u2028\u2029]+/gu, " ").trim();
}

function firstCategory(tags) {
  for (const key of CATEGORY_KEYS) {
    const value = tags.get(key);
    if (value && !NOISE_TYPES.has(value)) return { category: key, type: value };
  }
  return null;
}

function normalizedNameIdentity(value) {
  return foldMulti(value)
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/gu, " ");
}

function languageQualifiedAliasKey(key) {
  for (const base of ALIAS_KEYS) {
    if (!key.startsWith(`${base}:`)) continue;
    const suffix = key.slice(base.length + 1);
    if (LANGUAGE_SUFFIX_RE.test(suffix)) return base;
  }
  return null;
}

function aliasKeyPriority(key) {
  if (ALIAS_KEY_PRIORITY.has(key)) return ALIAS_KEY_PRIORITY.get(key);
  const qualifiedBase = languageQualifiedAliasKey(key);
  if (qualifiedBase) return ALIAS_KEY_PRIORITY.get(qualifiedBase) + 1;
  if (!key.startsWith("name:")) return null;
  const suffix = key.slice("name:".length);
  const namespace = suffix.split(":", 1)[0].toLowerCase();
  if (!suffix || NON_ALIAS_NAME_SUFFIXES.has(namespace)) return null;
  if (LANGUAGE_SUFFIX_RE.test(suffix)) return 25;
  if (HISTORICAL_NAME_SUFFIX_RE.test(suffix)) return 91;
  // Preserve useful established extensions such as name:botanical while the
  // explicit deny-list above prevents metadata values from polluting search.
  return 85;
}

function splitAliasValues(rawValue) {
  return String(rawValue ?? "")
    .split(";")
    .map(cleanText)
    .filter(Boolean);
}

function searchNameCandidates(tags) {
  const candidates = [];
  for (const [key, rawValue] of tags) {
    const priority = aliasKeyPriority(key);
    if (priority == null) continue;
    splitAliasValues(rawValue).forEach((value, valueIndex) => {
      candidates.push({ key, priority, value, valueIndex });
    });
  }
  return candidates.sort((left, right) => (
    left.priority - right.priority
    || (left.key < right.key ? -1 : left.key > right.key ? 1 : 0)
    || left.valueIndex - right.valueIndex
    || (left.value < right.value ? -1 : left.value > right.value ? 1 : 0)
  ));
}

function fallbackSearchName(candidates) {
  let best = null;
  for (const candidate of candidates) {
    const qualifiedBase = languageQualifiedAliasKey(candidate.key);
    const localized = candidate.key.startsWith("name:")
      && LANGUAGE_SUFFIX_RE.test(candidate.key.slice("name:".length));
    const priority = FALLBACK_NAME_PRIORITY.get(candidate.key)
      ?? (qualifiedBase ? (FALLBACK_NAME_PRIORITY.get(qualifiedBase) ?? 100) + 1 : null)
      ?? (localized ? 25 : null);
    if (priority == null) continue;
    if (!best || priority < best.priority
      || (priority === best.priority && candidate.key < best.candidate.key)) {
      best = { candidate, priority };
    }
  }
  return best?.candidate.value || "";
}

function collectAliases(candidates, name) {
  const aliases = [];
  const seen = new Set([normalizedNameIdentity(name)].filter(Boolean));
  for (const { value } of candidates) {
    const identity = normalizedNameIdentity(value);
    if (!identity || seen.has(identity)) continue;
    seen.add(identity);
    aliases.push(value);
    if (aliases.length >= MAX_SEARCH_ALIASES) break;
  }
  return aliases;
}

function typeLabel(type) {
  return String(type || "").replaceAll(/[_;]+/gu, " ").trim();
}

function uniqueText(parts) {
  const seen = new Set();
  const out = [];
  for (const part of parts) {
    const value = cleanText(part);
    const key = value.toLocaleLowerCase("en-US");
    if (!value || seen.has(key)) continue;
    seen.add(key);
    out.push(value);
  }
  return out;
}

function firstCleanTag(tags, keys) {
  for (const key of keys) {
    const value = cleanText(tags.get(key));
    if (value) return value;
  }
  return "";
}

export function placeDetails(tags) {
  const details = {};
  for (const [field, keys] of Object.entries(DETAIL_TAGS)) {
    const value = firstCleanTag(tags, keys);
    if (value) details[field] = value;
  }
  return Object.keys(details).length ? details : null;
}

// A compact, deterministic popularity prior derived entirely from OSM. It is
// deliberately conservative: population and place hierarchy carry most of the
// signal, while capital/reference/contact tags only break otherwise-close
// textual ties. The normalized result can be reused by Rangefind's generic
// rankPrior post-pass without introducing an OSM-only index format.
export function osmProminence(tags, category = firstCategory(tags)) {
  const population = Number(tags.get("population"));
  const populationScore = Number.isFinite(population) && population > 0
    ? Math.min(0.96, Math.log10(population + 1) / 7.4)
    : 0;
  const explicitImportance = Number(tags.get("importance"));
  const importanceScore = Number.isFinite(explicitImportance)
    ? Math.max(0, Math.min(1, explicitImportance))
    : 0;
  let score = Math.max(
    populationScore,
    importanceScore,
    category?.category === "place" ? (PLACE_PROMINENCE.get(category.type) || 0.08) : 0.05
  );
  const capital = cleanText(tags.get("capital")).toLowerCase();
  if (capital && capital !== "no") score += capital === "yes" ? 0.12 : 0.08;
  if (tags.get("wikipedia")) score += 0.08;
  if (tags.get("wikidata")) score += 0.05;
  if (tags.get("website") || tags.get("contact:website")) score += 0.025;
  if (tags.get("brand") || tags.get("operator")) score += 0.025;
  return Math.round(Math.max(0, Math.min(1, score)) * 1e6) / 1e6;
}

export function wayDocTagEntries(tags) {
  const entries = [];
  for (const [key, value] of tags) {
    if (WAY_DOC_KEYS.has(key) || aliasKeyPriority(key) != null) entries.push([key, value]);
  }
  return entries;
}

// Keep shapes where geometry materially improves a search result (parks,
// campuses, venues, shops, named buildings). Roads and enormous boundaries
// stay out: they already have specialized representations and would dominate
// capsule size. The cap protects pathological OSM ways on phone clients.
export function shouldRetainPlaceGeometry(tags, refs = []) {
  if (!Array.isArray(refs) || refs.length < 4 || refs.length > 2048 || refs[0] !== refs.at(-1)) return false;
  const category = firstCategory(tags);
  return Boolean(category || tags.get("building") || tags.get("area") === "yes")
    && Boolean(tags.get("name") || category);
}

function squaredSegmentDistance(point, start, end) {
  const dx = end.lon - start.lon;
  const dy = end.lat - start.lat;
  if (!dx && !dy) return (point.lon - start.lon) ** 2 + (point.lat - start.lat) ** 2;
  const ratio = Math.max(0, Math.min(1,
    ((point.lon - start.lon) * dx + (point.lat - start.lat) * dy) / (dx * dx + dy * dy)
  ));
  return (point.lon - (start.lon + dx * ratio)) ** 2 + (point.lat - (start.lat + dy * ratio)) ** 2;
}

function simplifyRing(points, tolerance = 0.00001, maxPoints = 256) {
  const closed = points.length > 2
    && points[0].lat === points.at(-1).lat
    && points[0].lon === points.at(-1).lon;
  const source = closed ? points.slice(0, -1) : points.slice();
  if (source.length <= 4) return points;
  const keep = new Uint8Array(source.length);
  keep[0] = 1;
  keep[source.length - 1] = 1;
  const stack = [[0, source.length - 1]];
  const threshold = tolerance ** 2;
  while (stack.length) {
    const [start, end] = stack.pop();
    let best = threshold;
    let bestIndex = -1;
    for (let index = start + 1; index < end; index++) {
      const distance = squaredSegmentDistance(source[index], source[start], source[end]);
      if (distance > best) {
        best = distance;
        bestIndex = index;
      }
    }
    if (bestIndex >= 0) {
      keep[bestIndex] = 1;
      stack.push([start, bestIndex], [bestIndex, end]);
    }
  }
  let simplified = source.filter((_, index) => keep[index]);
  if (simplified.length > maxPoints) {
    const step = (simplified.length - 1) / (maxPoints - 1);
    simplified = Array.from({ length: maxPoints }, (_, index) => simplified[Math.round(index * step)]);
  }
  if (closed && simplified.length < 3) simplified = source.slice(0, Math.min(source.length, 4));
  if (closed) simplified.push({ ...simplified[0] });
  return simplified;
}

export function geometryForWay(points) {
  const sourceRing = (points || []).filter(Boolean).map(point => ({ lat: Number(point.lat), lon: Number(point.lon) }));
  const ring = simplifyRing(sourceRing);
  if (ring.length < 4) return null;
  const closed = ring[0].lat === ring.at(-1).lat && ring[0].lon === ring.at(-1).lon;
  let twiceArea = 0;
  let centroidLon = 0;
  let centroidLat = 0;
  for (let index = 1; index < ring.length; index++) {
    const left = ring[index - 1];
    const right = ring[index];
    const cross = left.lon * right.lat - right.lon * left.lat;
    twiceArea += cross;
    centroidLon += (left.lon + right.lon) * cross;
    centroidLat += (left.lat + right.lat) * cross;
  }
  const unique = closed ? ring.slice(0, -1) : ring;
  const center = closed && Math.abs(twiceArea) > 1e-12
    ? { lat: centroidLat / (3 * twiceArea), lon: centroidLon / (3 * twiceArea) }
    : {
        lat: unique.reduce((sum, point) => sum + point.lat, 0) / unique.length,
        lon: unique.reduce((sum, point) => sum + point.lon, 0) / unique.length
      };
  const lats = ring.map(point => point.lat);
  const lons = ring.map(point => point.lon);
  return {
    center,
    geometry: {
      type: closed ? "Polygon" : "LineString",
      encoding: "polyline",
      precision: 5,
      encoded: encodePolyline(ring, 5),
      bbox: [Math.min(...lons), Math.min(...lats), Math.max(...lons), Math.max(...lats)]
    }
  };
}

export function retainedAddressTagEntries(tags) {
  if (!tags) return [];
  return ADDRESS_TAG_KEYS
    .filter(key => tags.has(key))
    .map(key => [key, cleanText(tags.get(key))])
    .filter(([, value]) => value);
}

export function searchablePlaceTags(tags) {
  return Boolean(
    tags.get("name")
    || firstCategory(tags)
    || tags.get("addr:full")
    || (tags.get("addr:housenumber") && (tags.get("addr:street") || tags.get("addr:place")))
    || tags.get("addr:interpolation")
  );
}

export function addressFromTags(tags) {
  if (!ADDRESS_TAG_KEYS.some(key => tags.has(key))) return null;
  const full = cleanText(tags.get("addr:full"));
  const houseNumber = cleanText(tags.get("addr:housenumber"));
  const street = cleanText(tags.get("addr:street"));
  const place = cleanText(tags.get("addr:place"));
  const unit = cleanText(tags.get("addr:unit") || tags.get("addr:flats"));
  const suburb = cleanText(tags.get("addr:suburb"));
  const city = cleanText(tags.get("addr:city") || tags.get("addr:town") || tags.get("addr:village") || tags.get("addr:hamlet"));
  const district = cleanText(tags.get("addr:district") || tags.get("addr:county"));
  const state = cleanText(tags.get("addr:state") || tags.get("addr:province"));
  const postcode = normalizePostalCodeSpacing(cleanText(tags.get("addr:postcode")));
  const country = cleanText(tags.get("addr:country"));
  const thoroughfare = street || place;
  const base = uniqueText([houseNumber, thoroughfare]).join(" ");
  const locality = uniqueText([suburb, city, district, state, postcode, country]);
  const complete = Boolean(full || (houseNumber && thoroughfare));
  if (!full && !base && !locality.length) return null;

  const premises = unit && base ? `${base}, Unit ${unit}` : base;
  const formatted = full || uniqueText([premises, ...locality]).join(", ");
  if (!formatted) return null;
  return {
    formatted,
    search: formatted,
    complete,
    houseNumber,
    street: thoroughfare,
    unit,
    suburb,
    city,
    district,
    state,
    postcode,
    country
  };
}

export function placeDoc(osmType, osmId, lat, lon, tags, options = {}) {
  const primaryName = cleanText(tags.get("name"));
  const searchNames = searchNameCandidates(tags);
  // Brand-only shops and language-only names are common enough in real OSM
  // data that rendering them as a generic "restaurant"/"shop" label loses
  // the identity users actually type. Historical names, operators and refs
  // remain searchable aliases but never masquerade as the current display.
  const name = primaryName || fallbackSearchName(searchNames);
  const primaryCategory = firstCategory(tags);
  // Named road ways were already retained as searchable documents. Preserve
  // their OSM road class explicitly so street/locality authority can serve
  // forward geocoding and intersection fallbacks without broad postings.
  const category = primaryCategory || (name && tags.get("highway")
    ? { category: "highway", type: cleanText(tags.get("highway")) }
    : null);
  const address = addressFromTags(tags);
  if (!name && !category && !address?.complete) return null;
  const label = typeLabel(category?.type || (address ? "address" : ""));
  const displayName = name || address?.formatted || label;
  const aliases = collectAliases(searchNames, name);
  const cuisine = typeLabel(tags.get("cuisine"));
  const details = placeDetails(tags);
  const bodyParts = [
    label,
    category ? category.category : address ? "address" : "",
    cuisine,
    details?.brand,
    details?.operator
  ];
  const doc = {
    id: `${osmType}/${osmId}`,
    url: `https://www.openstreetmap.org/${osmType}/${osmId}`,
    name: displayName,
    search_name: displayName,
    body: bodyParts.filter(Boolean).join(" "),
    lat: Number(lat.toFixed(7)),
    lon: Number(lon.toFixed(7)),
    geo_lat: Number(lat.toFixed(7)),
    geo_lon: Number(lon.toFixed(7)),
    prominence: osmProminence(tags, category)
  };
  if (details) doc.details = details;
  if (options.geometry) doc.geometry = options.geometry;
  if (aliases.length) doc.aliases = aliases;
  if (address) {
    doc.address = address.formatted;
    doc.address_search = address.search;
    if (address.houseNumber) doc.house_number = address.houseNumber;
    if (address.street) doc.street = address.street;
    if (address.unit) doc.unit = address.unit;
    if (address.suburb) doc.suburb = address.suburb;
    if (address.city) doc.city = address.city;
    if (address.district) doc.district = address.district;
    if (address.state) doc.state = address.state;
    if (address.postcode) doc.postcode = address.postcode;
    if (address.country) doc.country = address.country;
  }
  if (category) {
    doc.category = category.category;
    doc.type = category.type;
  } else if (address) {
    doc.category = "address";
    doc.type = "address";
  }
  if (category?.category === "highway" && name) {
    doc.street = doc.street || name;
    if (doc.city) doc.street_authority = `${name}, ${doc.city}`;
  }
  if (tags.get("place")) {
    const population = Number(tags.get("population"));
    if (Number.isFinite(population) && population > 0) doc.population = population;
  }
  return doc;
}

// Stamps a spatially derived locality onto a document that has no
// mapper-provided city. The name lands in `city` (display, street/civic
// matching) and in `address_search` (indexed text), so "jean coutu rosemère"
// matches the Rosemère pharmacy even though the OSM node carries only
// name+amenity. The formatted `address` and the authority address lane stay
// untouched — the derived locality is a search hint, not an address claim.
export function enrichDocLocality(doc, city) {
  if (!doc || doc.city) return false;
  // Places name localities; stamping a city onto a city (or its own suburbs
  // onto itself) would only distort ranking.
  if (doc.category === "place") return false;
  const cityText = cleanText(city);
  if (!cityText) return false;
  doc.city = cityText;
  const search = cleanText(doc.address_search);
  const key = cityText.toLocaleLowerCase("en-US");
  if (!search) {
    doc.address_search = cityText;
  } else if (!search.toLocaleLowerCase("en-US").includes(key)) {
    doc.address_search = `${search}, ${cityText}`;
  }
  if (doc.category === "highway" && doc.name) {
    doc.street = doc.street || doc.name;
    doc.street_authority = `${doc.name}, ${cityText}`;
  }
  return true;
}

function interpolationStep(value) {
  const normalized = cleanText(value).toLowerCase();
  if (normalized === "all") return 1;
  if (normalized === "odd" || normalized === "even") return 2;
  const numeric = Number(normalized);
  return Number.isInteger(numeric) && numeric > 0 ? numeric : 0;
}

function numericHouseNumber(value) {
  const normalized = cleanText(value);
  return /^\d+$/u.test(normalized) ? Number(normalized) : null;
}

function compatibleAddressTags(left, right, wayTags) {
  const merged = new Map();
  for (const key of ADDRESS_TAG_KEYS) {
    if (key === "addr:housenumber" || key === "addr:full" || key === "addr:unit" || key === "addr:flats") continue;
    const wayValue = cleanText(wayTags.get(key));
    const leftValue = cleanText(left.get(key));
    const rightValue = cleanText(right.get(key));
    if (leftValue && rightValue && normalizeAddressKey(leftValue) !== normalizeAddressKey(rightValue)) return null;
    const value = wayValue || leftValue || rightValue;
    if (value) merged.set(key, value);
  }
  if (!merged.get("addr:street") && !merged.get("addr:place")) return null;
  return merged;
}

function addressRangeTails(address) {
  const locality = uniqueText([address.suburb, address.city, address.district, address.state, address.country]);
  const tails = [address.street];
  for (let length = 1; length <= locality.length; length++) {
    tails.push(uniqueText([address.street, ...locality.slice(0, length)]).join(" "));
  }
  if (address.postcode) {
    tails.push(uniqueText([address.street, address.postcode]).join(" "));
    tails.push(uniqueText([address.street, ...locality, address.postcode]).join(" "));
  }
  return uniqueText(tails);
}

function applyAddressFields(doc, address) {
  if (address.street) doc.street = address.street;
  if (address.suburb) doc.suburb = address.suburb;
  if (address.city) doc.city = address.city;
  if (address.district) doc.district = address.district;
  if (address.state) doc.state = address.state;
  if (address.postcode) doc.postcode = address.postcode;
  if (address.country) doc.country = address.country;
}

// Materialize one compact range document per pair of numeric address anchors.
// Individual inferred houses remain virtual and are resolved by the runtime's
// bucketed authority lane, avoiding millions of full posting/doc payloads.
export function interpolationRangeDocs(osmId, refs, points, wayTags) {
  const kind = cleanText(wayTags.get("addr:interpolation")).toLowerCase();
  const step = interpolationStep(kind);
  if (!step || refs.length < 2 || points.length !== refs.length) return [];
  const anchors = [];
  for (let index = 0; index < points.length; index++) {
    const point = points[index];
    if (!point) continue;
    const tags = point.tags || new Map();
    const number = numericHouseNumber(tags.get("addr:housenumber"));
    if (number != null) anchors.push({ index, number, tags });
  }
  if (anchors.length < 2 || anchors[0].index !== 0 || anchors.at(-1).index !== points.length - 1) return [];

  const docs = [];
  for (let anchorIndex = 0; anchorIndex + 1 < anchors.length; anchorIndex++) {
    const left = anchors[anchorIndex];
    const right = anchors[anchorIndex + 1];
    const difference = right.number - left.number;
    if (!difference || Math.abs(difference) <= step || Math.abs(difference) % step) continue;
    if (kind === "even" && (left.number % 2 || right.number % 2)) continue;
    if (kind === "odd" && (!(left.number % 2) || !(right.number % 2))) continue;
    const addressTags = compatibleAddressTags(left.tags, right.tags, wayTags);
    if (!addressTags) continue;
    addressTags.set("addr:housenumber", String(left.number));
    const startAddress = addressFromTags(addressTags);
    if (!startAddress?.complete) continue;
    const geometryPoints = points.slice(left.index, right.index + 1);
    if (geometryPoints.some(point => !point)) continue;
    const geometry = encodeAddressRangeGeometry(geometryPoints);
    const midpoint = interpolateAddressRangePoint(geometry, left.number, right.number, (left.number + right.number) / 2);
    if (!midpoint) continue;
    const rangeLabel = `${left.number}\u2013${right.number} ${startAddress.street}`;
    const locality = uniqueText([
      startAddress.suburb, startAddress.city, startAddress.district,
      startAddress.state, startAddress.postcode, startAddress.country
    ]);
    const formatted = uniqueText([rangeLabel, ...locality]).join(", ");
    const doc = {
      id: `way/${osmId}/address-range/${docs.length}`,
      url: `https://www.openstreetmap.org/way/${osmId}`,
      name: formatted,
      search_name: formatted,
      address_search: formatted,
      body: `address interpolation ${kind}`,
      category: "address",
      type: "interpolated_address_range",
      lat: Number(midpoint.lat.toFixed(7)),
      lon: Number(midpoint.lon.toFixed(7)),
      geo_lat: Number(midpoint.lat.toFixed(7)),
      geo_lon: Number(midpoint.lon.toFixed(7)),
      interpolation_keys: addressRangeLookupValues(left.number, right.number, step, addressRangeTails(startAddress)),
      _address_range_start: left.number,
      _address_range_end: right.number,
      _address_range_step: step,
      _address_range_geometry: geometry,
      _address_range_kind: kind,
      _address_range_inclusion: cleanText(wayTags.get("addr:inclusion")) || "actual"
    };
    applyAddressFields(doc, startAddress);
    docs.push(doc);
  }
  return docs;
}
