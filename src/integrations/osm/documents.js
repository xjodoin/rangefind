import {
  addressRangeLookupValues,
  encodeAddressRangeGeometry,
  interpolateAddressRangePoint,
  normalizeAddressKey,
  normalizePostalCodeSpacing
} from "../../address.js";

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

const ALIAS_KEYS = [
  "alt_name", "old_name", "short_name", "official_name", "loc_name",
  "brand", "operator", "ref"
];

const ADDRESS_TAG_KEYS = [
  "addr:street", "addr:housenumber", "addr:city", "addr:town",
  "addr:village", "addr:hamlet", "addr:place", "addr:full", "addr:unit",
  "addr:flats", "addr:suburb", "addr:district", "addr:county",
  "addr:state", "addr:province", "addr:postcode", "addr:country"
];

const WAY_DOC_KEYS = new Set([
  "name", ...CATEGORY_KEYS, ...ALIAS_KEYS, ...ADDRESS_TAG_KEYS,
  "addr:interpolation", "addr:inclusion", "cuisine", "population"
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

function collectAliases(tags, name) {
  const aliases = [];
  for (const [key, rawValue] of tags) {
    const value = cleanText(rawValue);
    if (!value || value === name) continue;
    if (ALIAS_KEYS.includes(key) || key.startsWith("name:")) {
      if (!aliases.includes(value) && aliases.length < 8) aliases.push(value);
    }
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

export function wayDocTagEntries(tags) {
  const entries = [];
  for (const [key, value] of tags) {
    if (WAY_DOC_KEYS.has(key) || key.startsWith("name:")) entries.push([key, value]);
  }
  return entries;
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

export function placeDoc(osmType, osmId, lat, lon, tags) {
  const name = cleanText(tags.get("name"));
  const category = firstCategory(tags);
  const address = addressFromTags(tags);
  if (!name && !category && !address?.complete) return null;
  const label = typeLabel(category?.type || (address ? "address" : ""));
  const displayName = name || address?.formatted || label;
  const aliases = name ? collectAliases(tags, name) : [];
  const cuisine = typeLabel(tags.get("cuisine"));
  const bodyParts = [label, category ? category.category : address ? "address" : "", cuisine];
  const doc = {
    id: `${osmType}/${osmId}`,
    url: `https://www.openstreetmap.org/${osmType}/${osmId}`,
    name: displayName,
    search_name: displayName,
    body: bodyParts.filter(Boolean).join(" "),
    lat: Number(lat.toFixed(7)),
    lon: Number(lon.toFixed(7)),
    geo_lat: Number(lat.toFixed(7)),
    geo_lon: Number(lon.toFixed(7))
  };
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
  if (tags.get("place")) {
    const population = Number(tags.get("population"));
    if (Number.isFinite(population) && population > 0) doc.population = population;
  }
  return doc;
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
