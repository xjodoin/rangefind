import {
  buildCategoryLexicon,
  defaultCategoryLexicon,
  fold,
  lookupCategory,
  lookupCategoryFuzzy,
  withinOneEdit
} from "./category_lexicon.js";

const LOCALITY_CONNECTORS = new Set(["a", "around", "dans", "de", "du", "in", "near", "pres"]);
const LOCALITY_CACHE = new WeakMap();
const AUTHORITY_EXACT_CACHE = new WeakMap();
const AUTHORITY_LOOKUP_CACHE = new WeakMap();
const LOCALITY_SEARCH_STATS = Symbol("rangefind.localitySearchStats");
const CANADIAN_POSTAL_CODE = /^\s*([abceghj-nprstvxy]\d[abceghj-nprstvwxyz])\s*([0-9][abceghj-nprstvwxyz][0-9])\s*$/iu;
const LOCALITY_TYPES = new Set(["city", "town", "municipality", "village", "hamlet"]);
const STREET_DESIGNATORS = new Set([
  "allee", "avenue", "boulevard", "chemin", "cote", "cour", "impasse",
  "montee", "parkway", "place", "rang", "route", "rue", "terrasse",
  "court", "drive", "highway", "lane", "road", "street"
]);
const COORDINATE_NUMBER = "[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)";
const DECIMAL_COORDINATES = new RegExp(
  `^\\s*(${COORDINATE_NUMBER})(?:\\s*,\\s*|\\s+)(${COORDINATE_NUMBER})\\s*$`,
  "u"
);
const INTERSECTION_CONNECTOR = /\s+(?:&|and|at|et|x)\s+|\s*\/\s*/iu;
const DEFAULT_REVERSE_GEOCODE_RADIUS_METERS = 5000;
const DEFAULT_REVERSE_LOCALITY_RADIUS_METERS = 30000;
const MAX_REVERSE_GEOCODE_RESULTS = 25;

export function parseCoordinateIntent(value) {
  const match = String(value || "").match(DECIMAL_COORDINATES);
  if (!match) return null;
  const lat = Number(match[1]);
  const lon = Number(match[2]);
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) {
    return null;
  }
  return { lat, lon };
}

function coordinateResponse(coordinate, params, stats = {}, lane = "osmCoordinates") {
  const label = `${coordinate.lat.toFixed(6)}, ${coordinate.lon.toFixed(6)}`;
  return {
    total: 1,
    page: 1,
    size: Number(params.size || 10),
    approximate: false,
    results: [{
      id: `coordinate:${coordinate.lat},${coordinate.lon}`,
      name: label,
      title: label,
      address: label,
      category: "place",
      type: "coordinate",
      lat: coordinate.lat,
      lon: coordinate.lon,
      distanceMeters: 0
    }],
    resolvedQuery: label,
    stats: {
      ...stats,
      plannerLane: lane,
      shardsQueried: Number(stats.shardsQueried || 0)
    }
  };
}

function reverseCoordinate(params) {
  const value = params?.coordinate || params;
  const lat = Number(value?.lat);
  const lon = Number(value?.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) {
    throw new RangeError("reverse geocoding requires latitude in [-90, 90] and longitude in [-180, 180]");
  }
  return { lat, lon };
}

function reverseAddressComponents(result) {
  return [
    ["house_number", "street_number"],
    ["street", "route"],
    ["unit", "subpremise"],
    ["suburb", "sublocality"],
    ["city", "locality"],
    ["district", "administrative_area_level_2"],
    ["state", "administrative_area_level_1"],
    ["postcode", "postal_code"],
    ["country", "country"]
  ].flatMap(([field, type]) => {
    const value = String(result?.[field] || "").trim();
    return value ? [{ longText: value, shortText: value, types: [type] }] : [];
  });
}

function reverseAddressResult(result) {
  const address = String(result?.address || result?.name || result?.title || "").trim();
  const ranged = result?.type === "interpolated_address_range";
  const precise = Boolean(result?.house_number);
  return {
    ...result,
    ...(address ? { address } : {}),
    ...(address ? { formattedAddress: address } : {}),
    addressComponents: reverseAddressComponents(result),
    reverseGeocodeAccuracy: ranged ? "range" : precise ? "address-point" : "address",
    locationType: ranged ? "RANGE_INTERPOLATED" : precise ? "ROOFTOP" : "GEOMETRIC_CENTER",
    types: [ranged || precise ? "street_address" : "route"]
  };
}

function reverseLocalityResult(result) {
  const address = String(result?.name || result?.title || "").trim();
  const localityType = LOCALITY_TYPES.has(result?.type) ? result.type : "locality";
  return {
    ...result,
    ...(address ? { address, formattedAddress: address } : {}),
    addressComponents: reverseAddressComponents({ ...result, city: result.city || address }),
    reverseGeocodeAccuracy: "locality",
    locationType: "APPROXIMATE",
    types: [localityType]
  };
}

function requestedReverseTypes(rawParams) {
  const values = rawParams.resultTypes || rawParams.resultType;
  return new Set((Array.isArray(values) ? values : values ? [values] : []).map(String));
}

function requestedLocationTypes(rawParams) {
  const values = rawParams.locationTypes || rawParams.locationType;
  return new Set((Array.isArray(values) ? values : values ? [values] : []).map(value => String(value).toUpperCase()));
}

function matchesReverseFilters(result, resultTypes, locationTypes) {
  return (!resultTypes.size || result.types?.some(type => resultTypes.has(type)))
    && (!locationTypes.size || locationTypes.has(result.locationType));
}

function uniqueReverseAddressResults(results) {
  const seen = new Set();
  return results.filter(result => {
    // OSM can represent one civic address as a building, an entrance, and a
    // named POI. Reverse geocoding returns addresses rather than POI details,
    // so keep the nearest row for each complete display address.
    const key = fold(result.address);
    if (!key || !seen.has(key)) {
      if (key) seen.add(key);
      return true;
    }
    return false;
  });
}

/**
 * Resolve a coordinate to nearby indexed OSM addresses without a server-side
 * geocoder. The hard radius prevents coordinates in oceans or sparse regions
 * from silently returning an address hundreds of kilometres away.
 */
export async function reverseGeocodeOsm(engine, rawParams = {}) {
  if (typeof engine?.search !== "function") throw new TypeError("reverse geocoding requires a Rangefind search engine");
  const coordinate = reverseCoordinate(rawParams);
  const requestedRadius = rawParams.radiusMeters == null
    ? DEFAULT_REVERSE_GEOCODE_RADIUS_METERS
    : Number(rawParams.radiusMeters);
  if (!Number.isFinite(requestedRadius) || requestedRadius <= 0) {
    throw new RangeError("reverse geocoding radiusMeters must be a positive number");
  }
  const requestedSize = Number(rawParams.size ?? 10);
  const size = Number.isFinite(requestedSize) && requestedSize > 0
    ? Math.min(MAX_REVERSE_GEOCODE_RESULTS, Math.max(1, Math.floor(requestedSize)))
    : 10;
  const explicitScope = explicitShardScope(rawParams);
  const coverage = explicitScope.length
    ? explicitScope
    : anchorRadiusCoverageShards(engine, coordinate, requestedRadius);
  const sharded = Array.isArray(engine.manifest?.shards) && engine.manifest.shards.length > 0;
  const baseStats = {
    plannerLane: "osmReverseGeocode",
    osmIntentCoordinate: coordinate,
    osmIntentRadiusMeters: requestedRadius,
    ...(coverage.length ? { osmIntentCoverageShards: coverage } : {})
  };
  const resultTypes = requestedReverseTypes(rawParams);
  const locationTypes = requestedLocationTypes(rawParams);
  if (rawParams.localityRadiusMeters != null) {
    const value = Number(rawParams.localityRadiusMeters);
    if (!Number.isFinite(value) || value <= 0) {
      throw new RangeError("reverse geocoding localityRadiusMeters must be a positive number");
    }
  }

  // A sharded root can prove that the radius touches no published region.
  // Return ZERO_RESULTS without opening all children.
  if (sharded && !coverage.length) {
    return {
      total: 0,
      page: 1,
      size,
      approximate: false,
      results: [],
      resolvedQuery: `${coordinate.lat.toFixed(6)}, ${coordinate.lon.toFixed(6)}`,
      stats: { ...baseStats, shardsQueried: 0 }
    };
  }

  const {
    coordinate: _coordinate,
    lat: _lat,
    lon: _lon,
    radiusMeters: _radiusMeters,
    reverseGeocode: _reverseGeocode,
    resultTypes: _resultTypes,
    resultType: _resultType,
    locationTypes: _locationTypes,
    locationType: _locationType,
    localityRadiusMeters: _localityRadiusMeters,
    q: _q,
    near: _near,
    geo: _geo,
    filters,
    shards: _shards,
    page: _page,
    size: _size,
    ...engineOptions
  } = rawParams;
  const response = await engine.search({
    ...engineOptions,
    q: "",
    ...(coverage.length ? { shards: coverage } : {}),
    filters: {
      ...(filters || {}),
      facets: {
        ...(filters?.facets || {}),
        category: ["address"]
      }
    },
    geo: {
      near: { ...coordinate, radiusMeters: requestedRadius },
      sort: "distance"
    },
    page: 1,
    size
  });
  const results = uniqueReverseAddressResults((response.results || [])
    .map(reverseAddressResult)
    .filter(result => matchesReverseFilters(result, resultTypes, locationTypes)));
  const localityRequested = (!resultTypes.size
    || [...resultTypes].some(type => LOCALITY_TYPES.has(type) || type === "locality"))
    && (!locationTypes.size || locationTypes.has("APPROXIMATE"));
  if (!results.length && localityRequested) {
    const localityRadius = Math.max(
      requestedRadius,
      Number(rawParams.localityRadiusMeters || DEFAULT_REVERSE_LOCALITY_RADIUS_METERS)
    );
    const localityCoverage = explicitScope.length
      ? explicitScope
      : anchorRadiusCoverageShards(engine, coordinate, localityRadius);
    // The closest OSM `place` points are often suburbs, neighbourhoods,
    // squares, islands, and named localities. Read a bounded wider candidate
    // page before applying the requested administrative types so a city centre
    // a few kilometres away is not hidden behind those denser micro-features.
    const localityCandidateSize = Math.min(64, Math.max(32, size * 4));
    const localityResponse = await engine.search({
      ...engineOptions,
      q: "",
      ...(localityCoverage.length ? { shards: localityCoverage } : {}),
      filters: { facets: { category: ["place"] } },
      geo: {
        near: { ...coordinate, radiusMeters: localityRadius },
        sort: "distance"
      },
      page: 1,
      size: localityCandidateSize
    });
    const localityCandidates = uniqueReverseAddressResults((localityResponse.results || [])
      .filter(result => LOCALITY_TYPES.has(result.type))
      .map(reverseLocalityResult)
      .filter(result => matchesReverseFilters(result, resultTypes, locationTypes)));
    const localities = localityCandidates.slice(0, size);
    return {
      ...localityResponse,
      total: localityCandidates.length,
      page: 1,
      size,
      results: localities,
      resolvedQuery: localities[0]?.address || `${coordinate.lat.toFixed(6)}, ${coordinate.lon.toFixed(6)}`,
      stats: {
        ...(localityResponse.stats || {}),
        ...baseStats,
        plannerLane: "osmReverseGeocodeLocality",
        osmIntentLocalityRadiusMeters: localityRadius,
        ...(localityCoverage.length ? { osmIntentCoverageShards: localityCoverage } : {})
      }
    };
  }
  return {
    ...response,
    total: results.length,
    page: 1,
    size,
    results,
    resolvedQuery: results[0]?.address || `${coordinate.lat.toFixed(6)}, ${coordinate.lon.toFixed(6)}`,
    stats: {
      ...(response.stats || {}),
      ...baseStats
    }
  };
}

function mergeRuntimeTraces(...values) {
  const traces = [...new Set(values.filter(trace => trace?.spans?.length))];
  if (!traces.length) return null;
  if (traces.length === 1) return traces[0];
  const byName = new Map();
  for (const trace of traces) {
    for (const span of trace.spans) {
      const current = byName.get(span.name) || {
        name: span.name,
        count: 0,
        totalMs: 0,
        maxMs: 0,
        bytes: 0
      };
      current.count += Number(span.count || 0);
      current.totalMs += Number(span.totalMs || 0);
      current.maxMs = Math.max(current.maxMs, Number(span.maxMs || 0));
      current.bytes += Number(span.bytes || 0);
      byName.set(span.name, current);
    }
  }
  const spans = [...byName.values()]
    .map(({ bytes, ...span }) => bytes > 0 ? { ...span, bytes } : span)
    .sort((left, right) => right.totalMs - left.totalMs || left.name.localeCompare(right.name));
  return {
    totalMs: traces.reduce((sum, trace) => sum + Number(trace.totalMs || 0), 0),
    totalBytes: spans.reduce((sum, span) => sum + Number(span.bytes || 0), 0),
    spans
  };
}

export function parseOsmQueryIntent(value, lexicon = defaultCategoryLexicon()) {
  const surface = String(value || "").trim();
  const tokens = surface.split(/\s+/u).filter(Boolean);
  if (tokens.length < 2) return null;
  const first = lookupCategory(lexicon, tokens[0]) || lookupCategoryFuzzy(lexicon, tokens[0]);
  if (first) {
    const localityTokens = tokens.slice(1);
    let connector = false;
    while (LOCALITY_CONNECTORS.has(fold(localityTokens[0]))) {
      localityTokens.shift();
      connector = true;
    }
    if (!localityTokens.length) return null;
    return {
      category: first,
      locality: localityTokens.join(" "),
      order: "category-locality",
      connector
    };
  }
  const last = lookupCategory(lexicon, tokens.at(-1)) || lookupCategoryFuzzy(lexicon, tokens.at(-1));
  if (last) {
    return {
      category: last,
      locality: tokens.slice(0, -1).join(" "),
      order: "locality-category"
    };
  }
  return null;
}

// "pharmacy near me", "cafés nearby", "restaurants autour de moi", or just
// "pharmacie": a category with no locality. Without an anchor these fall to
// the global text lane; with `params.near` they become a nearest-sorted
// search around the caller's location — the difference between an
// unroutable single-common-term fan-out and a one-shard geo query.
const NEARBY_SUFFIXES = [
  "near me", "nearby", "around me", "close by", "close to me",
  "pres de moi", "autour de moi", "a proximite", "proche", "proches"
];

export function parseNearbyCategoryIntent(value, lexicon = defaultCategoryLexicon()) {
  let normalized = fold(value);
  if (!normalized) return null;
  for (const suffix of NEARBY_SUFFIXES) {
    if (normalized === suffix) return null;
    if (normalized.endsWith(` ${suffix}`)) {
      normalized = normalized.slice(0, -suffix.length - 1).trim();
      break;
    }
  }
  return lookupCategory(lexicon, normalized);
}

// Category vocabulary for an engine, resolved once and cached. A sharded
// root built with the lexicon artifact carries it in the manifest (its own
// corpus vocabulary joined with the alias table at build time); a single
// index reads its lazy `type` facet dictionary — one small cached fetch.
// Everything else (older roots, test doubles) gets the bundled canonical
// vocabulary, so category gating never depends on a rebuild.
const CATEGORY_LEXICON_CACHE = new WeakMap();

function engineCategoryLexicon(engine) {
  if (!engine || typeof engine !== "object") return defaultCategoryLexicon();
  if (!CATEGORY_LEXICON_CACHE.has(engine)) {
    CATEGORY_LEXICON_CACHE.set(engine, (async () => {
      const embedded = engine.manifest?.category_lexicon;
      if (Array.isArray(embedded?.types) && embedded.types.length) {
        return buildCategoryLexicon(embedded);
      }
      // A sharded root without the artifact would pay a full per-shard
      // fan-out to merge dictionaries — never worth it for a gate.
      const sharded = Boolean(engine.manifest?.features?.shards);
      if (!sharded && typeof engine.loadFacetValues === "function") {
        try {
          const values = await engine.loadFacetValues("type");
          if (Array.isArray(values) && values.length) return buildCategoryLexicon(values);
        } catch {
          // The dictionary is an accelerator, never a gate.
        }
      }
      return defaultCategoryLexicon();
    })());
  }
  return CATEGORY_LEXICON_CACHE.get(engine);
}

function nearAnchor(params) {
  const near = params?.near;
  const lat = Number(near?.lat);
  const lon = Number(near?.lon);
  return Number.isFinite(lat) && Number.isFinite(lon) ? { lat, lon } : null;
}

function withinOneFoldedEdit(leftValue, rightValue) {
  const left = fold(leftValue);
  const right = fold(rightValue);
  return Boolean(left && right && withinOneEdit(left, right));
}

async function exactAuthorityScope(engine, surface) {
  if (typeof engine?.authorityLookup !== "function") return null;
  if (!AUTHORITY_EXACT_CACHE.has(engine)) AUTHORITY_EXACT_CACHE.set(engine, new Map());
  const cache = AUTHORITY_EXACT_CACHE.get(engine);
  const key = fold(surface);
  if (!cache.has(key)) {
    const promise = (async () => {
      try {
        const lookup = await authorityLookupCached(engine, surface);
        return exactAuthorityFromLookup(lookup, key);
      } catch {
        return null;
      }
    })();
    promise.catch(() => cache.delete(key));
    cache.set(key, promise);
  }
  return cache.get(key);
}

async function authorityLookupCached(engine, surface) {
  if (typeof engine?.authorityLookup !== "function") return null;
  if (!AUTHORITY_LOOKUP_CACHE.has(engine)) AUTHORITY_LOOKUP_CACHE.set(engine, new Map());
  const cache = AUTHORITY_LOOKUP_CACHE.get(engine);
  const key = fold(surface);
  if (!cache.has(key)) {
    const promise = engine.authorityLookup(surface, { size: 32 });
    promise.catch(() => cache.delete(key));
    cache.set(key, promise);
  }
  return cache.get(key);
}

function exactAuthorityFromLookup(lookup, surface) {
  const key = fold(surface);
  const matches = (lookup?.matches || []).filter(match => fold(match.text) === key);
  if (!matches.length) return null;
  const bestWeight = Math.max(...matches.map(match => Number(match.weight || 0)));
  const best = matches.filter(match => Number(match.weight || 0) === bestWeight);
  const shards = [...new Set(best.flatMap(match => Array.isArray(match.shards) ? match.shards : []))]
    .map(String)
    .sort()
    .slice(0, 8);
  return {
    shards,
    text: best[0].text,
    count: Math.max(...best.map(match => Number(match.count || 0)))
  };
}

function bboxContainsPoint(bbox, lat, lon) {
  if (!Array.isArray(bbox) || bbox.length !== 4) return false;
  const [minLat, minLon, maxLat, maxLon] = bbox.map(Number);
  if (![minLat, minLon, maxLat, maxLon].every(Number.isFinite)) return false;
  if (lat < minLat || lat > maxLat) return false;
  return minLon <= maxLon
    ? lon >= minLon && lon <= maxLon
    : lon >= minLon || lon <= maxLon;
}

function anchorCoverageShards(engine, anchor) {
  return (engine.manifest?.shards || [])
    .filter(shard => bboxContainsPoint(shard.bbox, anchor.lat, anchor.lon))
    .map(shard => String(shard.id))
    .sort();
}

function explicitShardScope(params) {
  if (params?.shards == null) return [];
  return (Array.isArray(params.shards) ? params.shards : [params.shards]).map(String);
}

// Root-wide feature bits deliberately stay false during rolling format
// upgrades. A query routed to a known subset can still use the capability as
// soon as every selected child advertises it. Group scopes are expanded to
// their member descriptors so hierarchy labels remain safe too.
function shardScopeSupportsFeature(engine, shards, feature) {
  if (engine.manifest?.features?.[feature] === true) return true;
  if (!shards?.length) return false;
  const descriptors = engine.manifest?.shards || [];
  const selected = new Set();
  let resolvedScopes = 0;
  for (const scope of shards.map(String)) {
    const direct = descriptors.find(shard => String(shard.id) === scope);
    if (direct) {
      selected.add(direct);
      resolvedScopes++;
      continue;
    }
    let matchedGroup = false;
    for (const shard of descriptors) {
      if ((shard.groups || []).map(String).includes(scope)) {
        selected.add(shard);
        matchedGroup = true;
      }
    }
    if (matchedGroup) resolvedScopes++;
  }
  return resolvedScopes === shards.length
    && selected.size > 0
    && [...selected].every(shard => shard.features?.[feature] === true);
}

function resultMatchesLocality(result, surface) {
  const expected = fold(surface);
  if (!expected) return false;
  return [
    result?.city,
    result?.district,
    result?.suburb,
    result?.county,
    result?.state
  ].some(value => fold(value) === expected);
}

function compactFold(value) {
  return fold(value).replace(/[^\p{L}\p{N}]+/gu, "");
}

// "Parc Larochelle Repentigny", "Hotel Bonaventure Montreal", and similar
// map queries contain both a category and a venue name before the locality.
// The ordinary category-locality parser initially treats the whole tail as a
// locality; when that misses, an unrestricted text fallback can reopen every
// shard containing the terms. Prove a short trailing locality through root
// authority, search only its shard, and verify the structured locality field
// before accepting the shortcut. Exact full-tail authority wins first so
// genuine multi-token localities such as "New York" keep the normal lane.
async function resolveNamedCategoryLocality(engine, intent, params) {
  if (intent?.connector || intent?.order !== "category-locality") return null;
  const tokens = String(intent.locality || "").split(/\s+/u).filter(Boolean);
  if (tokens.length < 2) return null;
  const maxLocalityTokens = Math.min(4, tokens.length - 1);
  const candidates = [];
  // Prefer the longest authority suffix. For "Hotel Plaza New York", New York
  // is more specific than the independently valid authority York.
  for (let localityLength = maxLocalityTokens; localityLength >= 1; localityLength--) {
    const split = tokens.length - localityLength;
    const nameTokens = tokens.slice(0, split);
    const distinctiveNameTokens = nameTokens.map(fold).filter(token => token.length >= 3);
    if (!distinctiveNameTokens.length) continue;
    candidates.push({
      localitySurface: tokens.slice(split).join(" "),
      nameSurface: nameTokens.join(" "),
      query: nameTokens.join(" "),
      distinctiveNameTokens
    });
  }
  if (!candidates.length) return null;

  // The whole tail is the overwhelmingly common category + locality form.
  // Prove it first so "restaurant saint jean sur richelieu" does not also
  // download authority blocks for Richelieu, Sur Richelieu, and Jean Sur
  // Richelieu. A miss then unlocks the bounded named-venue suffix probes.
  const fullLocality = await exactAuthorityScope(engine, intent.locality);
  if (fullLocality?.shards?.length) return null;
  const authorities = await Promise.all(
    candidates.map(candidate => exactAuthorityScope(engine, candidate.localitySurface))
  );

  const requestedSize = Number(params.size || 10);
  const explicitScope = explicitShardScope(params);
  let provenMiss = null;
  for (let index = 0; index < candidates.length; index++) {
    const authority = authorities[index];
    if (!authority?.shards?.length) continue;
    const candidate = candidates[index];
    const scope = explicitScope.length ? explicitScope : authority.shards;
    const categoryFacetSafe = shardScopeSupportsFeature(engine, scope, "facetSummaryUint32");
    const response = await engine.search({
      ...params,
      q: candidate.query,
      typo: false,
      geo: undefined,
      shards: scope,
      size: Math.min(100, Math.max(32, requestedSize * 2)),
      ...(categoryFacetSafe ? {
        filters: {
          ...(params.filters || {}),
          facets: {
            ...(params.filters?.facets || {}),
            type: [intent.category.type]
          }
        }
      } : {})
    });
    const results = (response.results || []).filter(result => {
      const name = fold(result.name || result.title);
      const compactName = compactFold(result.name || result.title);
      const categoryMatches = categoryFacetSafe
        || fold(result.type || result.category) === fold(intent.category.type);
      return categoryMatches
        && candidate.distinctiveNameTokens.every(token => (
          name.includes(token) || compactName.includes(compactFold(token))
        ))
        && resultMatchesLocality(result, candidate.localitySurface);
    });
    const resolved = {
      ...response,
      total: results.length,
      size: requestedSize,
      results: results.slice(0, requestedSize),
      resolvedQuery: `${intent.category.label} ${candidate.nameSurface} ${authority.text || candidate.localitySurface}`,
      stats: {
        ...(response.stats || {}),
        plannerLane: "osmNamedCategoryLocality",
        osmIntentCategory: intent.category.query,
        osmIntentCategoryType: intent.category.type,
        ...(categoryFacetSafe ? { osmIntentCategoryFacet: true } : {}),
        osmIntentLocality: authority.text || candidate.localitySurface,
        osmIntentLocalityType: "authority",
        osmIntentLocalityAuthority: true,
        osmIntentNameTokens: candidate.distinctiveNameTokens,
        osmIntentCoverageShards: scope
      }
    };
    if (results.length) return resolved;
    // Once an explicit trailing locality is authority-proven, a scoped miss is
    // trustworthy. Keep trying other (more compact) authority suffixes, but do
    // not reopen an unbounded global text search if all of them miss.
    if (!provenMiss) provenMiss = resolved;
  }
  return provenMiss;
}

async function resolveAmbiguousCategoryLocality(engine, wholeSurface, localitySurface, params) {
  // When the root has authority routing, prove both interpretations before
  // starting either place search. Running the searches concurrently is unsafe:
  // "Park City" resolves quickly in Utah, but its split tail "City" can still
  // launch a planet-wide search that cannot be cancelled after the winner is
  // known. Authority reads are small, cacheable, and safe to parallelize.
  if (params.shards == null && typeof engine?.authorityLookup === "function") {
    let wholeLookup;
    let localityLookup;
    try {
      [wholeLookup, localityLookup] = await Promise.all([
        authorityLookupCached(engine, wholeSurface),
        authorityLookupCached(engine, localitySurface)
      ]);
    } catch {
      wholeLookup = null;
      localityLookup = null;
    }
    // A null artifact (as opposed to an empty match list) is not authoritative;
    // preserve the legacy fail-open probes when routing data is unavailable.
    if (wholeLookup == null || localityLookup == null) {
      const [wholePlace, locality] = await Promise.all([
        resolveLocality(engine, wholeSurface, params, { authorityMissIsFinal: true }),
        resolveLocality(engine, localitySurface, params)
      ]);
      return { wholePlace, locality };
    }
    const wholeAuthority = exactAuthorityFromLookup(wholeLookup, wholeSurface);
    if (wholeAuthority && !wholeAuthority.shards.length) {
      const [wholePlace, locality] = await Promise.all([
        resolveLocality(engine, wholeSurface, params, { authorityMissIsFinal: true, authorityLookup: wholeLookup }),
        resolveLocality(engine, localitySurface, params, { authorityLookup: localityLookup })
      ]);
      return { wholePlace, locality };
    }
    if (wholeAuthority?.shards?.length) {
      const wholePlace = await resolveLocality(
        engine,
        wholeSurface,
        params,
        { authorityMissIsFinal: true, authorityLookup: wholeLookup }
      );
      if (wholePlace) return { wholePlace, locality: null };
    }
    return {
      wholePlace: null,
      locality: await resolveLocality(engine, localitySurface, params, { authorityLookup: localityLookup })
    };
  }

  // Explicit shard scopes bound both probes, and legacy engines without an
  // authority artifact retain their previous fail-open behavior.
  const [wholePlace, locality] = await Promise.all([
    resolveLocality(engine, wholeSurface, params, { authorityMissIsFinal: true }),
    resolveLocality(engine, localitySurface, params)
  ]);
  return { wholePlace, locality };
}

function resultNameMatchesSurface(result, surface, allowTypo = false) {
  const expected = fold(surface).split(/[^\p{L}\p{N}]+/u).filter(token => token.length >= 3);
  if (!expected.length) return false;
  const actual = fold(result?.name || result?.title)
    .split(/[^\p{L}\p{N}]+/u)
    .filter(Boolean);
  return expected.every(token => actual.some(word => (
    word.includes(token)
    || token.includes(word)
    || (allowTypo && withinOneEdit(word, token))
  )));
}

// Unanchored Google-style text queries commonly include either an exact
// landmark name ("Calgary Tower") or a landmark plus a leading/trailing city
// ("Berlin Hauptbahnhof", "hauptbanhof berlin"). Root authority can bound
// both forms before the generic text federation opens unrelated shards.
async function resolveGlobalNamedText(engine, surface, params, { exactOnly = false } = {}) {
  if (params.shards != null || params.geo != null) return null;
  const tokens = String(surface || "").trim().split(/\s+/u).filter(Boolean);
  if (tokens.length < 2 || tokens.length > 9 || tokens.some(token => /\d/u.test(token))) return null;

  const requestedSize = Math.max(1, Number(params.size || 10));
  const wholeAuthority = await exactAuthorityScope(engine, surface);
  if (wholeAuthority?.shards?.length) {
    const response = await engine.search({
      ...params,
      q: surface,
      typo: false,
      shards: wholeAuthority.shards,
      size: Math.min(64, Math.max(24, requestedSize * 2))
    });
    const exactResults = (response.results || []).filter(
      result => fold(result.name || result.title) === fold(surface)
    );
    const exactLocality = exactResults.find(result => LOCALITY_TYPES.has(result.type));
    if (exactLocality) {
      return {
        ...localityExactResponse(exactLocality, params, surface),
        stats: {
          ...(response.stats || {}),
          plannerLane: "osmLocalityExact",
          osmIntentLocality: exactLocality.name || surface,
          osmIntentLocalityType: exactLocality.type || "",
          osmIntentCoverageShards: wholeAuthority.shards
        }
      };
    }
    const results = exactResults.filter(result => !LOCALITY_TYPES.has(result.type));
    if (results.length) {
      return {
        ...response,
        total: results.length,
        size: requestedSize,
        results: results.slice(0, requestedSize),
        stats: {
          ...(response.stats || {}),
          plannerLane: "osmGlobalExactText",
          osmIntentCoverageShards: wholeAuthority.shards,
          osmIntentAuthorityName: wholeAuthority.text || surface
        }
      };
    }
  }
  if (exactOnly) return null;

  const candidates = [];
  const seen = new Set();
  const maxLocalityTokens = Math.min(4, tokens.length - 1);
  for (let localityLength = maxLocalityTokens; localityLength >= 1; localityLength--) {
    for (const side of ["trailing", "leading"]) {
      const localityTokens = side === "trailing"
        ? tokens.slice(tokens.length - localityLength)
        : tokens.slice(0, localityLength);
      const nameTokens = side === "trailing"
        ? tokens.slice(0, tokens.length - localityLength)
        : tokens.slice(localityLength);
      const localitySurface = localityTokens.join(" ");
      const nameSurface = nameTokens.join(" ");
      const key = `${fold(localitySurface)}\0${fold(nameSurface)}`;
      if (!nameSurface || seen.has(key)) continue;
      seen.add(key);
      candidates.push({ localitySurface, nameSurface });
    }
  }

  const authorities = await Promise.all(
    candidates.map(candidate => exactAuthorityScope(engine, candidate.localitySurface))
  );
  let provenMiss = null;
  for (let index = 0; index < candidates.length; index++) {
    const authority = authorities[index];
    if (!authority?.shards?.length) continue;
    const candidate = candidates[index];
    const response = await engine.search({
      ...params,
      q: candidate.nameSurface,
      shards: authority.shards,
      size: Math.min(64, Math.max(24, requestedSize * 2))
    });
    const localitySurface = authority.text || candidate.localitySurface;
    const results = (response.results || []).filter(result => (
      resultNameMatchesSurface(result, candidate.nameSurface, true)
      && resultMatchesLocality(result, localitySurface)
    ));
    const resolved = {
      ...response,
      total: results.length,
      size: requestedSize,
      results: results.slice(0, requestedSize),
      resolvedQuery: `${candidate.nameSurface}, ${localitySurface}`,
      stats: {
        ...(response.stats || {}),
        plannerLane: "osmNamedTextLocality",
        osmIntentLocality: localitySurface,
        osmIntentLocalityAuthority: true,
        osmIntentName: candidate.nameSurface,
        osmIntentCoverageShards: authority.shards
      }
    };
    if (results.length) return resolved;
    if (!provenMiss) provenMiss = resolved;
  }
  return provenMiss;
}

function viewportCoverageShards(engine, box) {
  if (!box) return [];
  const minLat = Number(box.minLat);
  const maxLat = Number(box.maxLat);
  const minLon = Number(box.minLon);
  const maxLon = Number(box.maxLon);
  if (![minLat, maxLat, minLon, maxLon].every(Number.isFinite) || minLat > maxLat) return [];
  const lonIntervals = (left, right) => left <= right
    ? [[left, right]]
    : [[left, 180], [-180, right]];
  const requestedLon = lonIntervals(minLon, maxLon);
  return (engine.manifest?.shards || [])
    .filter(shard => {
      if (!Array.isArray(shard.bbox) || shard.bbox.length !== 4) return false;
      const [shardMinLat, shardMinLon, shardMaxLat, shardMaxLon] = shard.bbox.map(Number);
      if (![shardMinLat, shardMinLon, shardMaxLat, shardMaxLon].every(Number.isFinite)) return false;
      if (shardMaxLat < minLat || shardMinLat > maxLat) return false;
      return requestedLon.some(([left, right]) => (
        lonIntervals(shardMinLon, shardMaxLon).some(([shardLeft, shardRight]) => (
          shardRight >= left && shardLeft <= right
        ))
      ));
    })
    .map(shard => String(shard.id))
    .sort();
}

function anchorRadiusCoverageShards(engine, anchor, radiusMeters) {
  const angularDistance = (left, right) => {
    const distance = Math.abs(left - right) % 360;
    return Math.min(distance, 360 - distance);
  };
  return (engine.manifest?.shards || [])
    .filter(shard => {
      if (!Array.isArray(shard.bbox) || shard.bbox.length !== 4) return false;
      const [minLat, minLon, maxLat, maxLon] = shard.bbox.map(Number);
      if (![minLat, minLon, maxLat, maxLon].every(Number.isFinite)) return false;
      const lat = Math.max(minLat, Math.min(maxLat, anchor.lat));
      let lon = anchor.lon;
      if (!bboxContainsPoint(shard.bbox, lat, anchor.lon)) {
        lon = angularDistance(anchor.lon, minLon) <= angularDistance(anchor.lon, maxLon)
          ? minLon
          : maxLon;
      }
      return haversineMeters(anchor.lat, anchor.lon, lat, lon) <= radiusMeters;
    })
    .map(shard => String(shard.id))
    .sort();
}

function viewportAnchor(box) {
  if (!box) return null;
  const minLat = Number(box.minLat);
  const maxLat = Number(box.maxLat);
  const minLon = Number(box.minLon);
  const maxLon = Number(box.maxLon);
  if (![minLat, maxLat, minLon, maxLon].every(Number.isFinite) || minLat > maxLat) return null;
  let lon = minLon <= maxLon
    ? (minLon + maxLon) / 2
    : (minLon + maxLon + 360) / 2;
  if (lon > 180) lon -= 360;
  return { lat: (minLat + maxLat) / 2, lon };
}

function exactGeoResponse(response, authority, scope, radiusMeters, lane) {
  return collapseCivicDuplicates({
    ...response,
    stats: {
      ...(response.stats || {}),
      plannerLane: lane,
      ...(radiusMeters ? { osmIntentRadiusMeters: radiusMeters } : {}),
      ...(scope.length ? { osmIntentCoverageShards: scope } : {}),
      ...(authority?.shards?.length ? { osmIntentAuthorityShards: authority.shards } : {})
    }
  });
}

async function anchoredExactText(engine, q, params, anchor, {
  allowUngatedFuzzy = false,
  intent = null
} = {}) {
  const coverage = anchorCoverageShards(engine, anchor);
  const explicitScope = explicitShardScope(params);
  const directScope = explicitScope.length ? explicitScope : coverage;
  const compoundName = fold(q).split(/[^\p{L}\p{N}]+/u).filter(Boolean).length >= 2;

  // A geo-sorted local text result can prove its own whole-name match. For
  // compound brands and landmarks, do that useful search first instead of
  // serially downloading the root authority lexicon merely to authorize the
  // exact same search. A one-edit whole-name match covers benign singulars
  // and typos ("Tim Horton" -> "Tim Hortons"). Category-locality queries do
  // not pass this gate because their result names do not equal the full
  // surface, so they continue through the established intent resolver.
  if (!intent && compoundName && directScope.length) {
    try {
      const viewport = params.geo?.box;
      const direct = await engine.search({
        ...params,
        shards: directScope,
        geo: viewport ? {
          box: viewport,
          near: { lat: anchor.lat, lon: anchor.lon },
          sort: "distance"
        } : {
          near: { lat: anchor.lat, lon: anchor.lon, radiusMeters: NEAR_TEXT_RADIUS_METERS },
          sort: "distance"
        }
      });
      const canonical = (direct.results || []).filter(result => {
        const name = result.name || result.title;
        return fold(name) === fold(q) || withinOneFoldedEdit(name, q);
      });
      if (canonical.length) {
        const response = canonical.length === direct.results.length ? direct : {
          ...direct,
          total: canonical.length,
          approximate: true,
          results: canonical
        };
        const resolved = exactGeoResponse(
          response,
          null,
          directScope,
          viewport ? 0 : NEAR_TEXT_RADIUS_METERS,
          viewport ? "osmViewportExactGeo" : "osmNearExactGeo"
        );
        return {
          ...resolved,
          stats: {
            ...(resolved.stats || {}),
            osmIntentLocalNameProof: true
          }
        };
      }
    } catch (error) {
      if (!isGeoTextSortBudgetError(error)) throw error;
      // Common text can exceed the exact distance-sort budget. Authority and
      // the bounded embedded-coordinate lane below remain the fallback.
    }
  }

  let intentAuthority;
  if (intent && allowUngatedFuzzy) intentAuthority = await exactAuthorityScope(engine, q);

  // A category-like suffix can make a named destination look like a
  // category/locality request ("Montréal Trudeau Airport", "McGil
  // University"). If exact distance sorting is too broad, or its nearest
  // page is crowded by addresses, keep the full name and run one relevance
  // page inside the already selected shard. Embedded result coordinates are
  // enough to enforce the local radius without opening province-wide
  // doc-value chunks. The parsed category is used only as a ranking hint.
  if (
    intent
    && allowUngatedFuzzy
    && !intentAuthority
    && directScope.length
    && !shardScopeSupportsFeature(engine, directScope, "facetSummaryUint32")
  ) {
    const requestedSize = Math.max(1, Number(params.size || 10));
    const localText = await engine.search({
      ...params,
      q,
      shards: directScope,
      geo: undefined,
      size: Math.min(64, Math.max(32, requestedSize * 2))
    });
    const intentTokens = fold(intent.locality)
      .split(/[^\p{L}\p{N}]+/u)
      .filter(token => token.length >= 3);
    const viewport = params.geo?.box;
    const candidateRows = (localText.results || []).map((result, rank) => {
      const lat = Number(result.lat);
      const lon = Number(result.lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
      if (viewport && !bboxContainsPoint([
        Number(viewport.minLat), Number(viewport.minLon),
        Number(viewport.maxLat), Number(viewport.maxLon)
      ], lat, lon)) return null;
      const distanceMeters = haversineMeters(anchor.lat, anchor.lon, lat, lon);
      if (!viewport && distanceMeters > NEAR_TEXT_RADIUS_METERS) return null;
      const words = fold(result.name || result.title).split(/[^\p{L}\p{N}]+/u).filter(Boolean);
      const matchesIntent = intentTokens.every(token => words.some(word => (
        word.includes(token) || token.includes(word) || withinOneEdit(word, token)
      )));
      if (!matchesIntent) return null;
      return {
        result: { ...result, distanceMeters },
        rank,
        categoryMatch: String(result.type || "") === String(intent.category.type || "")
      };
    }).filter(Boolean);
    candidateRows.sort((left, right) => (
      Number(right.categoryMatch) - Number(left.categoryMatch)
      || left.rank - right.rank
      || left.result.distanceMeters - right.result.distanceMeters
    ));
    if (candidateRows.length) {
      const results = candidateRows.slice(0, requestedSize).map(row => row.result);
      return collapseCivicDuplicates({
        ...localText,
        total: candidateRows.length,
        size: requestedSize,
        approximate: localText.total > localText.results.length,
        results,
        stats: {
          ...(localText.stats || {}),
          plannerLane: viewport ? "osmViewportIntentText" : "osmNearIntentText",
          osmIntentRadiusMeters: viewport ? 0 : NEAR_TEXT_RADIUS_METERS,
          osmIntentCategoryHint: intent.category.type,
          osmIntentCoverageShards: directScope
        }
      });
    }
  }

  const authority = intentAuthority === undefined ? await exactAuthorityScope(engine, q) : intentAuthority;
  if (!authority && !allowUngatedFuzzy) return null;
  const authorityCoverage = authority?.shards?.length
    ? directScope.filter(shard => authority.shards.includes(shard))
    : [];
  const scope = authorityCoverage.length ? authorityCoverage : directScope;
  const requestedSize = Math.max(1, Number(params.size || 10));
  const intentNameTokens = !authority && allowUngatedFuzzy && intent && !intent.connector
    ? fold(intent.locality).split(" ").filter(token => token.length >= 3)
    : [];
  const distinctiveIntentToken = intentNameTokens.at(-1);
  const categoryFacetSafe = shardScopeSupportsFeature(engine, scope, "facetSummaryUint32");
  const probeParams = distinctiveIntentToken ? {
    ...params,
    q: distinctiveIntentToken,
    ...(categoryFacetSafe ? {
      filters: {
        ...(params.filters || {}),
        facets: {
          ...(params.filters?.facets || {}),
          type: [intent.category.type]
        }
      }
    } : {})
  } : params;
  if (scope.length && params.geo?.box && authority) {
    const response = await engine.search({
      ...params,
      shards: params.shards == null ? scope : params.shards,
      geo: {
        box: params.geo.box,
        near: { lat: anchor.lat, lon: anchor.lon },
        sort: "distance"
      }
    });
    return exactGeoResponse(response, authority, scope, 0, "osmViewportExactGeo");
  }
  const nearestExact = async lane => {
    try {
      const response = await engine.search({
        ...probeParams,
        shards: params.shards == null ? scope : params.shards,
        geo: {
          near: { lat: anchor.lat, lon: anchor.lon, radiusMeters: NEAR_TEXT_RADIUS_METERS },
          sort: "distance"
        }
      });
      return exactGeoResponse(response, authority, scope, NEAR_TEXT_RADIUS_METERS, lane);
    } catch (error) {
      if (!isGeoTextSortBudgetError(error)) throw error;
      return null;
    }
  };
  // A high root-authority count already proves this is a repeated name, so
  // skip the text hydration probe and go straight to exact nearest traversal.
  if (scope.length && Number(authority?.count || 0) >= Math.max(32, requestedSize * 2)) {
    const nearest = await nearestExact("osmNearExactGeo");
    if (nearest) return nearest;
  }
  // A bounded text window is considerably cheaper than attaching lat/lon
  // doc-value chunks to every exact brand hit in a province. OSM documents
  // already carry their display coordinates, so the integration can verify
  // the 50 km orbit and rank this small page locally after root coverage has
  // selected the one geographic shard.
  const scopedText = scope.length > 0;
  const response = await engine.search(scopedText ? {
    ...probeParams,
    shards: params.shards == null ? scope : params.shards,
    size: Math.min(100, Math.max(32, requestedSize * 3)),
    geo: undefined
  } : {
    ...probeParams,
    geo: {
      near: { lat: anchor.lat, lon: anchor.lon, radiusMeters: NEAR_TEXT_RADIUS_METERS },
      boost: NEAR_TEXT_BOOST
    }
  });
  const corrected = response.correctedQuery
    && withinOneFoldedEdit(response.correctedQuery, q)
    ? response.correctedQuery
    : null;
  if (!authority && !corrected && allowUngatedFuzzy) {
    const namedFuzzy = (response.results || []).some(result => {
      const name = fold(result.name || result.title);
      return intentNameTokens.length
        ? intentNameTokens.every(token => name.includes(token))
        : withinOneFoldedEdit(name, q);
    });
    if (!namedFuzzy) return null;
  }
  // Dense repeated names (brands and station entrances) need a real nearest
  // traversal. The bounded text probe above prices the match set first: a
  // sparse landmark page stays on the cheap embedded-coordinate lane, while
  // a result set larger than the probe window is handed to the geo tree for
  // an exact distance-ordered page.
  if (scopedText && response.total > response.results.length) {
    const nearest = await nearestExact(authority ? "osmNearExactGeo" : "osmNearFuzzyGeo");
    if (nearest) return nearest;
    // The bounded embedded-coordinate page below remains correct and local;
    // it is only not guaranteed to contain every nearer repeated name.
  }
  const exact = [];
  const fuzzy = [];
  const other = [];
  for (const result of response.results || []) {
    const name = result.name || result.title;
    const distanceMeters = Number.isFinite(result.lat) && Number.isFinite(result.lon)
      ? haversineMeters(anchor.lat, anchor.lon, result.lat, result.lon)
      : result.distanceMeters;
    if (scopedText && (!Number.isFinite(distanceMeters) || distanceMeters > NEAR_TEXT_RADIUS_METERS)) continue;
    const ranked = Number.isFinite(distanceMeters) ? { ...result, distanceMeters } : result;
    const foldedName = fold(name);
    const foldedQuery = fold(corrected || q);
    if (foldedName === foldedQuery) exact.push(ranked);
    else if (foldedName.includes(foldedQuery) || foldedQuery.split(" ").some(token => (
      token.length >= 4 && foldedName.includes(token)
    ))) fuzzy.push(ranked);
    else other.push(ranked);
  }
  if (!authority && !exact.length && !fuzzy.length) return null;
  const byDistance = (left, right) => Number(left.distanceMeters ?? Infinity) - Number(right.distanceMeters ?? Infinity);
  exact.sort(byDistance);
  fuzzy.sort(byDistance);
  other.sort(byDistance);
  const localCandidates = [...exact, ...fuzzy, ...other];
  const localResults = localCandidates.slice(0, requestedSize);
  if (!localResults.length) return null;
  return collapseCivicDuplicates({
    ...response,
    total: scopedText ? localCandidates.length : response.total,
    size: requestedSize,
    approximate: scopedText ? response.total > response.results.length : response.approximate,
    results: localResults,
    stats: {
      ...(response.stats || {}),
      plannerLane: authority ? "osmNearExactText" : "osmNearFuzzyText",
      osmIntentRadiusMeters: NEAR_TEXT_RADIUS_METERS,
      ...(distinctiveIntentToken ? {
        osmIntentCategoryFacet: categoryFacetSafe,
        osmIntentNameToken: distinctiveIntentToken
      } : {}),
      ...(scope.length ? { osmIntentCoverageShards: scope } : {}),
      ...(authority?.shards?.length ? { osmIntentAuthorityShards: authority.shards } : {})
    }
  });
}

// Callers pass `near` as advisory context for the OSM cascade; the engine
// itself only understands `geo`, so the hint never travels further down.
function engineParams(params) {
  const { near, ...rest } = params;
  return rest;
}

const NEARBY_CATEGORY_RADII_METERS = [10000, 50000];
const NEAR_TEXT_RADIUS_METERS = 50000;
const NEAR_TEXT_BOOST = { weight: 2, pivotMeters: 2000 };

// A text + distance sort resolves the exact match set before the geo tree
// orders it, and the engine refuses when the terms' posting volume exceeds
// its geoTextSortMaxDf budget — category labels like "car repair" blow
// through it in dense shards ("garage brunet" resolving to a Provence
// village). The intent is still sound, so a refusal degrades to relevance
// ranking with a proximity boost inside the same radius — presented
// nearest-first — instead of failing the whole query.
function isGeoTextSortBudgetError(error) {
  return error?.code === "RANGEFIND_GEO_TEXT_SORT_BUDGET"
    || String(error?.message || "").includes("geoTextSortMaxDf");
}

async function searchNearestWithBudgetFallback(engine, params) {
  try {
    return await engine.search(params);
  } catch (error) {
    if (params.geo?.sort !== "distance" || !params.geo?.near || !isGeoTextSortBudgetError(error)) {
      throw error;
    }
    const { sort, ...geo } = params.geo;
    const response = await engine.search({ ...params, geo: { ...geo, boost: NEAR_TEXT_BOOST } });
    const meters = value => Number.isFinite(value) ? value : Infinity;
    const results = [...(response.results || [])]
      .sort((left, right) => meters(left.distanceMeters) - meters(right.distanceMeters));
    return {
      ...response,
      results,
      stats: { ...(response.stats || {}), osmDistanceSortFallback: "geo-boost" }
    };
  }
}

function haversineMeters(lat1, lon1, lat2, lon2) {
  const rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad;
  const dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(dLon / 2) ** 2;
  return 6371000 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function localityRadiusMeters(type) {
  if (type === "postal_code") return 5000;
  if (type === "city") return 30000;
  if (type === "town" || type === "municipality") return 10000;
  if (type === "village") return 7000;
  return 5000;
}

// Shared with resolveLocality's best-match sort: higher ranks are places a
// traveler plausibly names from afar; village and below (priority <= 2) are
// coincidence candidates when they sit far from the caller's anchor.
const LOCALITY_TYPE_PRIORITY = new Map([["city", 5], ["town", 4], ["municipality", 3], ["village", 2], ["hamlet", 1]]);
const WEAK_LOCALITY_ANCHOR_METERS = 100000;

function typePriority(type) {
  return LOCALITY_TYPE_PRIORITY.get(type) || 0;
}

function possibleLocalityQuery(value, lexicon) {
  const surface = String(value || "").trim();
  const normalized = fold(surface);
  const tokens = normalized.split(" ").filter(Boolean);
  return tokens.length >= 1
    && tokens.length <= 4
    && !tokens.some(token => /\d/u.test(token))
    && !lookupCategory(lexicon || defaultCategoryLexicon(), normalized);
}

function hasHouseNumber(value) {
  return String(value || "")
    .trim()
    .split(/[^\p{L}\p{N}-]+/u)
    .some(token => /^\d+[\p{L}]?(?:-\d+[\p{L}]?)?$/u.test(token));
}

function streetLocalitySurface(value) {
  const withoutNumber = String(value || "").replace(
    /^\s*\d+[\p{L}]?(?:\s*[-–—]\s*\d+[\p{L}]?)?\s+/u,
    ""
  );
  const comma = withoutNumber.indexOf(",");
  if (comma <= 0 || comma === withoutNumber.length - 1) return null;
  const street = withoutNumber.slice(0, comma).trim();
  const locality = withoutNumber.slice(comma + 1).trim();
  const streetTokens = fold(street).split(" ").filter(Boolean);
  if (!streetTokens.some(token => STREET_DESIGNATORS.has(token))) return null;
  return { text: `${street}, ${locality}`, key: fold(`${street}, ${locality}`) };
}

export function collapseStreetSuggestions(response, params = {}) {
  const size = Math.max(1, Math.min(50, Math.floor(Number(params.size || 8))));
  if (hasHouseNumber(params.q)) {
    return { ...response, suggestions: (response.suggestions || []).slice(0, size) };
  }
  const grouped = new Map();
  const passthrough = [];
  for (const item of response.suggestions || []) {
    const surface = streetLocalitySurface(item.text);
    if (!surface) {
      passthrough.push(item);
      continue;
    }
    const current = grouped.get(surface.key) || {
      text: surface.text,
      weight: 0,
      count: 0,
      matches: 0,
      type: "street",
      shards: new Set()
    };
    current.weight += Number(item.weight || 0);
    current.count += Number(item.count || 1);
    current.matches++;
    for (const shard of item.shards || []) current.shards.add(shard);
    grouped.set(surface.key, current);
  }
  const streets = [...grouped.values()]
    .filter(item => item.matches >= 2)
    .map(({ matches, shards, ...item }) => ({
      ...item,
      ...(shards.size ? { shards: [...shards].sort() } : {})
    }));
  const collapsedKeys = new Set(streets.map(item => fold(item.text)));
  const remaining = passthrough.concat((response.suggestions || []).filter(item => {
    const surface = streetLocalitySurface(item.text);
    return surface && !collapsedKeys.has(surface.key);
  }));
  const suggestions = streets.concat(remaining)
    .sort((left, right) => (
      (right.type === "street") - (left.type === "street")
      || Number(right.weight || 0) - Number(left.weight || 0)
      || String(left.text).localeCompare(String(right.text))
    ))
    .slice(0, size);
  return {
    ...response,
    suggestions,
    stats: {
      ...(response.stats || {}),
      osmStreetSuggestionsCollapsed: streets.length
    }
  };
}

function foldedTextWithOffsets(value) {
  let text = "";
  const starts = [];
  const ends = [];
  let offset = 0;
  for (const character of String(value || "")) {
    const folded = fold(character);
    for (const unit of folded) {
      text += unit;
      starts.push(offset);
      ends.push(offset + character.length);
    }
    offset += character.length;
  }
  return { text, starts, ends };
}

function suggestionMatchRanges(text, input) {
  const haystack = foldedTextWithOffsets(text);
  const tokens = fold(input).split(/\s+/u).filter(token => token.length >= 1);
  const ranges = [];
  for (const token of tokens) {
    const start = haystack.text.indexOf(token);
    if (start < 0) continue;
    ranges.push({ start: haystack.starts[start], end: haystack.ends[start + token.length - 1] });
  }
  return ranges
    .filter((range, index, all) => !all.some((other, otherIndex) => (
      otherIndex < index && other.start === range.start && other.end === range.end
    )))
    .sort((left, right) => left.start - right.start || left.end - right.end);
}

function structuredSuggestion(item, input) {
  const text = String(item.text || "").trim();
  const comma = text.indexOf(",");
  const mainText = item.type === "category-locality"
    ? String(item.category || text).replaceAll("_", " ")
    : comma > 0 ? text.slice(0, comma).trim() : text;
  const secondaryText = item.type === "category-locality"
    ? String(item.locality || "")
    : comma > 0 ? text.slice(comma + 1).trim() : "";
  const kind = item.type || "place";
  return {
    ...item,
    description: text,
    mainText,
    secondaryText,
    matchedRanges: suggestionMatchRanges(text, input),
    kind,
    types: item.category ? [item.category] : kind === "street" ? ["route"] : [kind],
    selection: {
      query: text,
      ...(item.shards?.length ? { shards: item.shards } : {})
    }
  };
}

function structuredSuggestions(response, input) {
  return {
    ...response,
    suggestions: (response.suggestions || []).map(item => structuredSuggestion(item, input))
  };
}

export async function suggestOsmQuery(engine, rawParams = {}) {
  const { inputOffset: rawInputOffset, ...rawEngineParams } = rawParams;
  const originalQ = String(rawParams.q || "");
  const parsedOffset = Number(rawInputOffset);
  const inputOffset = Number.isFinite(parsedOffset)
    ? Math.max(0, Math.min(originalQ.length, Math.floor(parsedOffset)))
    : originalQ.length;
  const input = originalQ.slice(0, inputOffset);
  const params = { ...engineParams(rawEngineParams), q: input };
  const requestedSize = Math.max(1, Math.min(50, Math.floor(Number(params.size || 8))));
  const anchor = nearAnchor(rawParams) || viewportAnchor(rawParams.geo?.box);
  const coverage = anchor && params.shards == null ? anchorCoverageShards(engine, anchor) : [];
  const q = String(params.q || "").trim();
  const lexicon = await engineCategoryLexicon(engine);
  const intent = parseOsmQueryIntent(q, lexicon);
  if (intent?.locality && !intent.category.correctedFrom) {
    const localityResponse = await engine.suggest({
      ...params,
      q: intent.locality,
      ...(coverage.length ? { shards: coverage } : {}),
      size: Math.max(32, requestedSize)
    });
    const suggestions = (localityResponse.suggestions || [])
      .map(item => ({
        ...item,
        text: intent.order === "locality-category"
          ? `${item.text} ${intent.category.label}`
          : `${intent.category.label} ${item.text}`,
        type: "category-locality",
        category: intent.category.type,
        locality: item.text
      }))
      .slice(0, requestedSize);
    return structuredSuggestions({
      ...localityResponse,
      q: originalQ,
      inputOffset,
      suggestions,
      stats: {
        ...(localityResponse.stats || {}),
        plannerLane: "osmSuggestCategoryLocality",
        osmIntentCategory: intent.category.query,
        ...(coverage.length ? { osmSuggestCoverageShards: coverage } : {})
      }
    }, input);
  }
  const response = await engine.suggest({
    ...params,
    ...(coverage.length ? { shards: coverage } : {}),
    size: hasHouseNumber(params.q) ? requestedSize : Math.max(32, requestedSize)
  });
  const collapsed = collapseStreetSuggestions(response, { ...params, size: requestedSize });
  return structuredSuggestions(coverage.length ? {
    ...collapsed,
    q: originalQ,
    inputOffset,
    stats: {
      ...(collapsed.stats || {}),
      osmSuggestCoverageShards: coverage
    }
  } : { ...collapsed, q: originalQ, inputOffset }, input);
}

function collapseCivicDuplicates(response) {
  const results = response.results || [];
  const named = results.filter(result => result.type !== "civic_address");
  if (!named.length || !results.some(result => result.type === "civic_address")) return response;
  const duplicate = civic => named.some(result => {
    if (!civic.house_number || !civic.street || !result.house_number || !result.street) return false;
    if (fold(`${civic.house_number} ${civic.street}`) !== fold(`${result.house_number} ${result.street}`)) return false;
    const samePostcode = civic.postcode && result.postcode
      && fold(civic.postcode) === fold(result.postcode);
    const nearby = Number.isFinite(civic.lat) && Number.isFinite(civic.lon)
      && Number.isFinite(result.lat) && Number.isFinite(result.lon)
      && Math.abs(civic.lat - result.lat) <= 0.001
      && Math.abs(civic.lon - result.lon) <= 0.0015;
    return samePostcode || nearby;
  });
  const collapsed = results.filter(result => result.type !== "civic_address" || !duplicate(result));
  const removed = results.length - collapsed.length;
  if (!removed) return response;
  return {
    ...response,
    total: Math.max(collapsed.length, Number(response.total || results.length) - removed),
    results: collapsed,
    stats: { ...(response.stats || {}), osmCivicDuplicatesCollapsed: removed }
  };
}

async function resolveLocality(engine, surface, params = {}, options = {}) {
  const normalizedLocality = fold(surface);
  const shardScope = params.shards == null
    ? []
    : (Array.isArray(params.shards) ? params.shards : [params.shards]).map(String).sort();
  const localityKey = `${normalizedLocality}\0${shardScope.join("\0")}`;
  if (!LOCALITY_CACHE.has(engine)) LOCALITY_CACHE.set(engine, new Map());
  const cache = LOCALITY_CACHE.get(engine);
  // Cached locality coordinates are reusable, but transport traces are not:
  // replaying the cold query's byte receipt on a warm cache hit would make
  // Query X-Ray claim reads that did not happen.
  if (cache.has(localityKey)) {
    const cached = cache.get(localityKey);
    if (!cached) return null;
    const resolved = { ...cached };
    if (params.trace) {
      Object.defineProperty(resolved, LOCALITY_SEARCH_STATS, {
        value: { trace: { totalMs: 0, totalBytes: 0, spans: [] } },
        enumerable: false
      });
    }
    return resolved;
  }
  const postalMatch = String(surface || "").match(CANADIAN_POSTAL_CODE);
  const bestMatch = results => {
    const matches = (results || []).filter(result => {
      if (!Number.isFinite(result.lat) || !Number.isFinite(result.lon)) return false;
      if (postalMatch) {
        const expected = `${postalMatch[1]} ${postalMatch[2]}`.toUpperCase();
        return result.type === "postal_code" && String(result.postcode || "").toUpperCase() === expected;
      }
      return LOCALITY_TYPES.has(result.type) && fold(result.name || result.title) === normalizedLocality;
    });
    matches.sort((left, right) => (
      typePriority(right.type) - typePriority(left.type)
      || Number(right.population || 0) - Number(left.population || 0)
    ));
    return matches[0] || null;
  };
  // Sharded roots with a suggest-routing artifact resolve the name's home
  // shard(s) from the root authority lexicon in a couple of small reads. The
  // locality search below then runs scoped to those shards instead of
  // fanning out to the whole federation — on a planet index that is the
  // difference between ~100KB and tens of megabytes per cold locality. The
  // lookup is advisory: a scoped miss retries unscoped.
  let authorityShards = null;
  let authoritativeMiss = false;
  if (!postalMatch && !shardScope.length && typeof engine.authorityLookup === "function") {
    try {
      const lookup = Object.hasOwn(options, "authorityLookup")
        ? options.authorityLookup
        : await authorityLookupCached(engine, surface);
      const scoped = [];
      let exactMatchWithoutShard = false;
      const exactMatches = (lookup?.matches || [])
        .filter(match => fold(match.text) === normalizedLocality);
      const bestWeight = exactMatches.length
        ? Math.max(...exactMatches.map(match => Number(match.weight || 0)))
        : null;
      for (const match of exactMatches) {
        if (Number(match.weight || 0) !== bestWeight) continue;
        if (!Array.isArray(match.shards) || !match.shards.length) {
          exactMatchWithoutShard = true;
          continue;
        }
        for (const id of match.shards) {
          if (!scoped.includes(id)) scoped.push(id);
        }
        if (scoped.length >= 4) break;
      }
      if (scoped.length) authorityShards = scoped.slice(0, 4).sort();
      // A non-null response proves the root authority lexicon was read
      // successfully. For the connectorless whole-query ambiguity probe,
      // absence there is decisive: do not turn "cinema laval" into a
      // federation-wide place search merely to prove it is not a city.
      // An exact legacy row without shard provenance remains fail-open.
      authoritativeMiss = lookup != null && !authorityShards && !exactMatchWithoutShard;
    } catch {
      // The lexicon is an accelerator, never a gate.
    }
  }
  if (options.authorityMissIsFinal && authoritativeMiss) return null;

  // Locality names are common worldwide ("Montréal" is also a Mauritius
  // suburb and a Brazilian allotment) and BM25 scores tie tightly across
  // shards, so the real city can sit well below the first page. Fetch a
  // wider page and let the type-priority/population sort above pick the
  // actual locality; a too-small page here silently degrades the whole
  // query into an unscoped global fan-out.
  const attemptResolve = async scope => {
    const localityResponse = await engine.search(postalMatch ? {
      q: surface,
      size: 8,
      ...(params.trace ? { trace: params.trace } : {}),
      ...(scope ? { shards: scope } : {})
    } : {
      q: surface,
      filters: { facets: { category: ["place"] } },
      size: 32,
      ...(params.trace ? { trace: params.trace } : {}),
      ...(scope ? { shards: scope } : {})
    });
    let resolved = bestMatch(localityResponse.results);
    let resolvedStats = localityResponse.stats || {};
    // Popular locality names drown their major bearer under a wall of
    // same-named hamlets ("Laval": 260 place docs; the 438k-person city ties
    // every BM25 score and never reaches the page). When the first page
    // resolves to nothing better than a village, ask again restricted to
    // populated places — population is a doc-value number filter, so it works
    // even where the type facet cannot be used for filtering.
    // A zero-hit first page proves the retry hopeless: the populous query
    // selects a strict subset of the same place-filtered docs.
    if (!postalMatch && localityResponse.total > 0 && typePriority(resolved?.type) <= 2) {
      const populousResponse = await engine.search({
        q: surface,
        filters: { facets: { category: ["place"] }, numbers: { population: { min: 25000 } } },
        size: 8,
        ...(params.trace ? { trace: params.trace } : {}),
        ...(scope ? { shards: scope } : {})
      });
      const populous = bestMatch(populousResponse.results);
      if (populous) {
        resolved = populous;
        const trace = mergeRuntimeTraces(localityResponse.stats?.trace, populousResponse.stats?.trace);
        resolvedStats = { ...(populousResponse.stats || {}), ...(trace ? { trace } : {}) };
      }
    }
    return { resolved, resolvedStats };
  };

  let outcome = await attemptResolve(authorityShards || (shardScope.length ? shardScope : null));
  if (!outcome.resolved && authorityShards) {
    outcome = await attemptResolve(shardScope.length ? shardScope : null);
  }
  const resolved = outcome.resolved;
  const resolvedStats = outcome.resolvedStats;
  if (resolved) {
    cache.set(localityKey, { ...resolved });
    Object.defineProperty(resolved, LOCALITY_SEARCH_STATS, {
      value: resolvedStats,
      enumerable: false
    });
  } else {
    cache.set(localityKey, null);
  }
  return resolved;
}

function parsedStreetSurface(value) {
  const tokens = String(value || "").trim().split(/\s+/u).filter(Boolean);
  if (tokens.length < 2 || !tokens.some(token => STREET_DESIGNATORS.has(fold(token)))) return null;
  const core = tokens.filter(token => !STREET_DESIGNATORS.has(fold(token)));
  return core.length ? { text: tokens.join(" "), key: fold(tokens.join(" ")), core: core.join(" ") } : null;
}

function parseIntersectionSurface(value) {
  const surface = String(value || "").trim().replaceAll(/\s*,\s*/gu, " ");
  const connector = INTERSECTION_CONNECTOR.exec(surface);
  if (!connector) return null;
  const left = parsedStreetSurface(surface.slice(0, connector.index));
  const right = surface.slice(connector.index + connector[0].length).trim();
  return left && right ? { left, right } : null;
}

async function resolveIntersectionLocality(engine, surface, params, anchor = null) {
  const parsed = parseIntersectionSurface(surface);
  if (!parsed) return null;
  const rightTokens = parsed.right.split(/\s+/u).filter(Boolean);
  const coverage = anchor && params.shards == null ? anchorCoverageShards(engine, anchor) : [];
  const routedParams = coverage.length ? { ...params, shards: coverage } : params;
  const maxLocalityTokens = Math.min(4, rightTokens.length - 2);
  let locality = null;
  let rightStreet = null;
  for (let localityLength = 1; localityLength <= maxLocalityTokens; localityLength++) {
    const split = rightTokens.length - localityLength;
    const candidateStreet = parsedStreetSurface(rightTokens.slice(0, split).join(" "));
    if (!candidateStreet) continue;
    const localitySurface = rightTokens.slice(split).join(" ");
    const authority = await exactAuthorityScope(engine, localitySurface);
    const candidateLocality = authority?.shards?.length ? {
      name: authority.text || localitySurface,
      type: "authority",
      shard: authority.shards[0]
    } : await resolveLocality(
      engine,
      localitySurface,
      routedParams,
      { authorityMissIsFinal: true }
    );
    if (!candidateLocality) continue;
    locality = candidateLocality;
    rightStreet = candidateStreet;
    break;
  }
  if (!locality || !rightStreet) {
    return {
      total: 0,
      page: 1,
      size: Number(params.size || 10),
      approximate: false,
      results: [],
      stats: {
        plannerLane: "osmIntersectionMiss",
        ...(coverage.length ? { osmIntentCoverageShards: coverage } : {})
      }
    };
  }

  const localityShard = String(locality.shard || "").split("/")[0];
  const localityName = locality.name || rightTokens.at(-1) || "";
  const intersectionScope = localityShard ? [localityShard] : coverage;
  const directIntersection = await engine.search({
    ...params,
    q: `${parsed.left.core} ${rightStreet.core} ${localityName}`.trim(),
    typo: false,
    size: Math.max(20, Number(params.size || 10)),
    geo: undefined,
    ...(intersectionScope.length ? { shards: intersectionScope } : {})
  });
  const streetTokensMatch = (name, street) => {
    const haystack = fold(name).split(/[^\p{L}\p{N}]+/u).filter(Boolean);
    const needles = fold(street.core).split(/[^\p{L}\p{N}]+/u).filter(Boolean);
    return needles.length > 0 && needles.every(token => haystack.includes(token));
  };
  const explicitIntersection = (directIntersection.results || []).find(result => (
    Number.isFinite(result.lat)
    && Number.isFinite(result.lon)
    && streetTokensMatch(result.name || result.title, parsed.left)
    && streetTokensMatch(result.name || result.title, rightStreet)
    && (
      fold(result.city) === fold(localityName)
      || haversineMeters(locality.lat, locality.lon, result.lat, result.lon) <= localityRadiusMeters(locality.type)
    )
  ));
  if (explicitIntersection) {
    const name = explicitIntersection.name || explicitIntersection.title;
    const trace = mergeRuntimeTraces(
      locality[LOCALITY_SEARCH_STATS]?.trace,
      directIntersection.stats?.trace
    );
    return {
      ...directIntersection,
      total: 1,
      size: Number(params.size || 10),
      approximate: false,
      results: [{
        ...explicitIntersection,
        name,
        title: name,
        type: explicitIntersection.type || "intersection",
        category: explicitIntersection.category || "highway",
        ...(anchor ? {
          distanceMeters: haversineMeters(anchor.lat, anchor.lon, explicitIntersection.lat, explicitIntersection.lon)
        } : {})
      }],
      resolvedQuery: `${name}, ${localityName}`,
      stats: {
        ...(directIntersection.stats || {}),
        ...(trace ? { trace } : {}),
        plannerLane: "osmIntersectionDocument",
        osmIntentLocality: localityName,
        osmIntentStreets: [parsed.left.text, rightStreet.text],
        ...(intersectionScope.length ? { osmIntentCoverageShards: intersectionScope } : {})
      }
    };
  }

  // The authority-only locality is enough to validate a real intersection
  // document by its city. Synthetic fallback needs an actual center for its
  // street-pair distance proof, so hydrate the place only on this miss path.
  if (!Number.isFinite(locality.lat) || !Number.isFinite(locality.lon)) {
    const resolvedLocality = await resolveLocality(
      engine,
      localityName,
      routedParams,
      { authorityMissIsFinal: true }
    );
    if (resolvedLocality) locality = resolvedLocality;
  }

  const searchStreet = async street => {
    const scoped = {
      ...params,
      size: 100,
      typo: false,
      geo: undefined,
      ...(localityShard ? { shards: [localityShard] } : coverage.length ? { shards: coverage } : {})
    };
    const local = await engine.search({ ...scoped, q: `${street.core} ${localityName}`.trim() });
    if (local.total > 0) return local;
    const primary = await engine.search({ ...scoped, q: street.core });
    if (primary.total > 0) return primary;
    // Hyphenated street names are sometimes indexed as one compound token
    // while query analysis splits the typed surface. Retrying the most
    // distinctive component gives the post-filter below candidates to
    // verify without weakening the intersection's exact street-name check.
    const fallbackTerm = street.core
      .split(/[^\p{L}\p{N}]+/u)
      .filter(token => Array.from(token).length >= 3)
      .sort((left, right) => Array.from(right).length - Array.from(left).length)[0];
    if (!fallbackTerm || fold(fallbackTerm) === fold(street.core)) return primary;
    const fallback = await engine.search({ ...scoped, q: fallbackTerm });
    const trace = mergeRuntimeTraces(primary.stats?.trace, fallback.stats?.trace);
    return {
      ...fallback,
      stats: {
        ...(fallback.stats || {}),
        ...(trace ? { trace } : {}),
        osmIntersectionStreetFallback: fallbackTerm
      }
    };
  };
  const [leftResponse, rightResponse] = await Promise.all([
    searchStreet(parsed.left),
    searchStreet(rightStreet)
  ]);
  const radiusMeters = localityRadiusMeters(locality.type);
  const streetPoints = (response, street) => (response.results || []).filter(result => (
    Number.isFinite(result.lat)
    && Number.isFinite(result.lon)
    && (
      fold(result.city) === fold(localityName)
      || haversineMeters(locality.lat, locality.lon, result.lat, result.lon) <= radiusMeters
    )
    && (
      fold(result.name || result.title) === street.key
      || fold(result.street) === street.key
      || fold(result.street).startsWith(`${street.key} `)
    )
  ));
  const leftPoints = streetPoints(leftResponse, parsed.left);
  const rightPoints = streetPoints(rightResponse, rightStreet);
  let nearest = null;
  for (const left of leftPoints) {
    for (const right of rightPoints) {
      const distance = haversineMeters(left.lat, left.lon, right.lat, right.lon);
      if (!nearest || distance < nearest.distance) nearest = { left, right, distance };
    }
  }
  const trace = mergeRuntimeTraces(
    locality[LOCALITY_SEARCH_STATS]?.trace,
    leftResponse.stats?.trace,
    rightResponse.stats?.trace
  );
  if (!nearest || nearest.distance > 2000) {
    return {
      total: 0,
      page: 1,
      size: Number(params.size || 10),
      approximate: false,
      results: [],
      stats: {
        ...(trace ? { trace } : {}),
        plannerLane: "osmIntersectionMiss",
        osmIntentLocality: locality.name,
        osmIntentIntersectionCandidateDistanceMeters: nearest?.distance ?? null,
        ...(localityShard ? { osmIntentCoverageShards: [localityShard] } : {})
      }
    };
  }

  const lat = (nearest.left.lat + nearest.right.lat) / 2;
  const lon = (nearest.left.lon + nearest.right.lon) / 2;
  const name = `${parsed.left.text} & ${rightStreet.text}`;
  const city = locality.name || "";
  return {
    total: 1,
    page: 1,
    size: Number(params.size || 10),
    approximate: nearest.distance > 100,
    results: [{
      id: `intersection:${localityShard}:${fold(name)}`,
      name,
      title: name,
      address: city ? `${name}, ${city}` : name,
      city,
      category: "highway",
      type: "intersection",
      shard: localityShard,
      lat,
      lon,
      ...(anchor ? { distanceMeters: haversineMeters(anchor.lat, anchor.lon, lat, lon) } : {})
    }],
    resolvedQuery: city ? `${name}, ${city}` : name,
    stats: {
      ...(leftResponse.stats || {}),
      ...(trace ? { trace } : {}),
      plannerLane: "osmIntersectionLocality",
      osmIntentLocality: city,
      osmIntentStreets: [parsed.left.text, rightStreet.text],
      osmIntentIntersectionCandidateDistanceMeters: nearest.distance,
      ...(localityShard ? { osmIntentCoverageShards: [localityShard] } : {})
    }
  };
}

async function resolveStreetLocality(engine, surface, params) {
  const tokens = String(surface || "")
    .trim()
    .replaceAll(/\s*,\s*/gu, " ")
    .split(/\s+/u)
    .filter(Boolean);
  if (tokens.length < 3) return null;
  const designatorIndexes = tokens
    .map((token, index) => STREET_DESIGNATORS.has(fold(token)) ? index : -1)
    .filter(index => index >= 0);
  if (!designatorIndexes.length) return null;

  // Try the shortest locality suffix first. Most Québec municipalities are one
  // token, and misses are cached; longer names remain bounded to four tokens.
  const maxLocalityTokens = Math.min(4, tokens.length - 2);
  for (let localityLength = 1; localityLength <= maxLocalityTokens; localityLength++) {
    const split = tokens.length - localityLength;
    const streetTokens = tokens.slice(0, split);
    if (!streetTokens.some(token => STREET_DESIGNATORS.has(fold(token)))) continue;
    const coreTokens = streetTokens.filter(token => !STREET_DESIGNATORS.has(fold(token)));
    if (!coreTokens.length) continue;
    const localitySurface = tokens.slice(split).join(" ");
    const streetSurface = streetTokens.join(" ");
    const streetKey = fold(streetSurface);
    const civicLookup = hasHouseNumber(streetSurface);
    const distinctiveStreetToken = coreTokens
      .flatMap(token => String(token).split(/[^\p{L}\p{N}]+/u))
      .filter(Boolean)
      .at(-1);
    const streetLookupQuery = civicLookup
      ? streetSurface
      : distinctiveStreetToken || coreTokens.join(" ");
    const searchStreet = shards => engine.search({
      ...params,
      q: streetLookupQuery,
      typo: false,
      size: Math.max(30, Number(params.size || 10)),
      geo: undefined,
      ...(shards?.length ? { shards } : {})
    });
    const exactStreetResult = result => (
      fold(result.name || result.title) === streetKey
      || (
        result.house_number != null
        && result.street
        && fold(`${result.house_number} ${result.street}`) === streetKey
      )
      || (result.street && fold(result.street) === streetKey)
    );

    let locality = null;
    let response = null;
    let authorityLocality = false;

    // The root authority already proves which shard owns an exact locality.
    // Search that shard immediately and validate the candidate's structured
    // city. This removes a serial place search from the normal cold path.
    // Documents without usable locality metadata retain the coordinate-based
    // resolver below as the correctness fallback.
    if (params.shards == null) {
      const authority = await exactAuthorityScope(engine, localitySurface);
      if (authority?.shards?.length) {
        try {
          const candidateResponse = await searchStreet(authority.shards);
          const localityKeys = new Set([
            fold(localitySurface),
            fold(authority.text)
          ].filter(Boolean));
          const validated = candidateResponse.results.some(result => (
            exactStreetResult(result)
            && localityKeys.has(fold(result.city))
          ));
          if (validated) {
            locality = {
              name: authority.text || localitySurface,
              type: "authority",
              shard: authority.shards[0]
            };
            response = candidateResponse;
            authorityLocality = true;
          }
        } catch {
          // Root routing is an accelerator; the normal resolver is fail-open.
        }
      }
    }

    if (!locality) {
      locality = await resolveLocality(engine, localitySurface, params);
      if (!locality) continue;
    }
    // Rank by plain text relevance, scoped to the locality's root shard,
    // and enforce the locality radius on the returned page here instead of
    // through the engine's geo machinery. A distance sort would decode
    // every posting in the radius (street tokens like "saint" blow through
    // the geoTextSortMaxDf budget in dense shards and fail the query), and
    // a radius filter verifies every text candidate's lat/lon through
    // doc-value chunks — both cost far more than checking thirty results.
    // A miss falls back to the surrounding cascade like before.
    const localityShard = String(locality.shard || "").split("/")[0];
    if (!response) {
      try {
        response = await searchStreet(localityShard ? [localityShard] : null);
      } catch {
        // A shard-level posting budget or transport failure must not kill the
        // whole query — fall back to the surrounding cascade.
        return null;
      }
    }
    const radiusMeters = authorityLocality ? null : localityRadiusMeters(locality.type);
    const authorityLocalityKeys = new Set([
      fold(localitySurface),
      fold(locality.name)
    ].filter(Boolean));
    const nearLocality = authorityLocality
      ? result => authorityLocalityKeys.has(fold(result.city))
      : result => haversineMeters(locality.lat, locality.lon, result.lat, result.lon) <= radiusMeters;
    const street = response.results.find(result => (
      Number.isFinite(result.lat)
      && Number.isFinite(result.lon)
      && nearLocality(result)
      && fold(result.name || result.title) === streetKey
    ));
    // Civic addresses are usually named after the occupant ("Tower Centre"),
    // not the address, so a name match misses them. Their structured
    // house_number + street fields are the address — matching those returns
    // the exact point directly instead of abandoning the resolved locality
    // for a full-index text fan-out.
    const civic = street ? null : response.results.find(result => (
      Number.isFinite(result.lat)
      && Number.isFinite(result.lon)
      && nearLocality(result)
      && result.house_number != null
      && result.street
      && fold(`${result.house_number} ${result.street}`) === streetKey
    ));
    // Streets themselves often rank below the wall of civic addresses that
    // sit on them. A civic address whose street FIELD matches confirms the
    // street exists and anchors its location — good enough for the street
    // result the demo renders.
    const streetAnchor = street || civic ? null : response.results.find(result => (
      Number.isFinite(result.lat)
      && Number.isFinite(result.lon)
      && nearLocality(result)
      && result.street
      && fold(result.street) === streetKey
    ));
    if (civic) {
      const civicCity = civic.city || locality.name || localitySurface;
      const civicTrace = mergeRuntimeTraces(locality[LOCALITY_SEARCH_STATS]?.trace, response.stats?.trace);
      return {
        total: 1,
        page: 1,
        size: Number(params.size || 10),
        approximate: false,
        results: [{
          ...civic,
          address: `${civic.house_number} ${civic.street}, ${civicCity}`,
          city: civicCity,
          distanceMeters: undefined
        }],
        resolvedQuery: `${civic.house_number} ${civic.street}, ${civicCity}`,
        stats: {
          ...(response.stats || {}),
          ...(civicTrace ? { trace: civicTrace } : {}),
          plannerLane: "osmStreetLocality",
          osmIntentStreet: streetSurface,
          osmIntentStreetLookup: streetLookupQuery,
          osmIntentLocality: locality.name || localitySurface,
          osmIntentLocalityType: locality.type || "",
          ...(radiusMeters == null ? {} : { osmIntentRadiusMeters: radiusMeters }),
          ...(authorityLocality ? { osmIntentLocalityAuthority: true } : {}),
          osmIntentCivicAddress: true
        }
      };
    }
    const anchor = street || streetAnchor;
    // A short suffix can itself be a valid but wrong locality ("York" in
    // "350 5th Avenue New York"). Keep trying the bounded longer suffixes
    // before allowing the surrounding global fallback.
    if (!anchor) continue;
    const trace = mergeRuntimeTraces(locality[LOCALITY_SEARCH_STATS]?.trace, response.stats?.trace);
    return {
      total: 1,
      page: 1,
      size: Number(params.size || 10),
      approximate: false,
      results: [{
        ...anchor,
        name: streetSurface,
        address: `${streetSurface}, ${locality.name || localitySurface}`,
        city: locality.name || localitySurface,
        category: "highway",
        type: "street",
        distanceMeters: undefined
      }],
      resolvedQuery: `${streetSurface}, ${locality.name || localitySurface}`,
      stats: {
        ...(response.stats || {}),
        ...(trace ? { trace } : {}),
        plannerLane: "osmStreetLocality",
        osmIntentStreet: streetSurface,
        osmIntentStreetLookup: streetLookupQuery,
        osmIntentLocality: locality.name || localitySurface,
        osmIntentLocalityType: locality.type || "",
        ...(radiusMeters == null ? {} : { osmIntentRadiusMeters: radiusMeters }),
        ...(authorityLocality ? { osmIntentLocalityAuthority: true } : {})
      }
    };
  }
  return null;
}

function localityExactResponse(locality, params, surface) {
  return {
    total: 1,
    page: 1,
    size: Number(params.size || 10),
    approximate: false,
    results: [locality],
    resolvedQuery: locality.name || surface,
    stats: {
      ...(locality[LOCALITY_SEARCH_STATS] || {}),
      plannerLane: "osmLocalityExact",
      osmIntentLocality: locality.name || surface,
      osmIntentLocalityType: locality.type || ""
    }
  };
}

export async function searchOsmQuery(engine, rawParams = {}) {
  // `near` is an advisory anchor (user location or map viewport center) for
  // the cascade only; the engine never sees it. Explicit place intents in
  // the query text always outrank proximity.
  const anchor = rawParams.geo == null ? nearAnchor(rawParams) : null;
  const params = engineParams(rawParams);
  const q = String(params.q || "").trim();
  const coordinate = parseCoordinateIntent(q);
  if (coordinate) {
    if (rawParams.reverseGeocode === false) return coordinateResponse(coordinate, params);
    const reversed = await reverseGeocodeOsm(engine, {
      ...params,
      ...(rawParams.reverseGeocode && typeof rawParams.reverseGeocode === "object"
        ? rawParams.reverseGeocode
        : {}),
      ...coordinate
    });
    return reversed.results.length
      ? reversed
      : coordinateResponse(coordinate, params, reversed.stats, "osmReverseGeocodeFallback");
  }
  const lexicon = await engineCategoryLexicon(engine);
  // "pharmacy near me" parses before category-locality — otherwise "me"
  // would be treated as a locality name and resolved against the planet.
  const nearbyCategory = parseNearbyCategoryIntent(q, lexicon);
  const intent = nearbyCategory ? null : parseOsmQueryIntent(q, lexicon);
  // A whole-name POI can begin or end with a category word ("McGill
  // University", "Garage Brunet"), and ordinary brand/landmark queries used
  // to pay every locality/street probe before reaching the anchored text
  // lane. Root authority is a cheap exact-name gate; only confirmed names
  // open the local geo shard. A strict one-edit local check covers a category
  // at the end ("McGil University") without making broad typo queries fan
  // out.
  const mapAnchor = anchor || viewportAnchor(params.geo?.box);
  const viewportWholeName = params.geo?.box
    && (q.split(/\s+/u).filter(Boolean).length >= 2 || /[^\x00-\x7f]/u.test(q));
  if (mapAnchor && q && !intent?.connector && !nearbyCategory && (anchor || viewportWholeName)) {
    const exactText = await anchoredExactText(engine, q, params, mapAnchor, {
      allowUngatedFuzzy: intent?.order === "locality-category",
      intent
    });
    if (exactText) return exactText;
  }
  if (!intent) {
    // Bare category (or explicit near-me phrasing) with an anchor: a
    // nearest-sorted category search around the caller instead of an
    // unroutable global single-term fan-out. The wider retry covers rural
    // anchors where 10 km holds nothing.
    if (nearbyCategory && anchor) {
      let response = null;
      let radiusMeters = 0;
      let categoryFacetSafe = false;
      let categoryScope = [];
      const explicitScope = explicitShardScope(params);
      for (const radius of NEARBY_CATEGORY_RADII_METERS) {
        radiusMeters = radius;
        categoryScope = explicitScope.length
          ? explicitScope
          : anchorRadiusCoverageShards(engine, anchor, radius);
        categoryFacetSafe = shardScopeSupportsFeature(engine, categoryScope, "facetSummaryUint32");
        response = await searchNearestWithBudgetFallback(engine, {
          ...params,
          ...(params.shards == null && categoryScope.length ? { shards: categoryScope } : {}),
          q: nearbyCategory.query,
          // The category lexicon already resolved the user's wording to the
          // exact OSM type. Safe unsigned facet summaries let the geo tree
          // reject irrelevant cells before fetching their point pages.
          ...(categoryFacetSafe ? {
            filters: {
              ...(params.filters || {}),
              facets: {
                ...(params.filters?.facets || {}),
                type: [nearbyCategory.type]
              }
            }
          } : {}),
          geo: {
            near: { lat: anchor.lat, lon: anchor.lon, radiusMeters: radius },
            sort: "distance"
          }
        });
        if (response.total > 0) break;
      }
      return {
        ...response,
        resolvedQuery: `${nearbyCategory.label} nearby`,
        stats: {
          ...(response.stats || {}),
          plannerLane: "osmCategoryNearby",
          osmIntentCategory: nearbyCategory.query,
          osmIntentRadiusMeters: radiusMeters,
          ...(categoryScope.length ? { osmIntentCoverageShards: categoryScope } : {}),
          ...(categoryFacetSafe ? { osmIntentCategoryFacet: true } : {})
        }
      };
    }
    // Near-me phrasing without a usable anchor: the words themselves are
    // not a place, so skip the street/locality resolvers and search the
    // category text directly.
    if (nearbyCategory) {
      const explicitScope = explicitShardScope(params);
      const viewportScope = explicitScope.length
        ? explicitScope
        : viewportCoverageShards(engine, params.geo?.box);
      const categoryFacetSafe = shardScopeSupportsFeature(engine, viewportScope, "facetSummaryUint32");
      const response = await searchNearestWithBudgetFallback(engine, {
        ...params,
        ...(params.shards == null && viewportScope.length ? { shards: viewportScope } : {}),
        // Inside an explicit viewport the exact type facet is the predicate;
        // repeating the category as text forces the engine to materialize the
        // whole spatial doc set before ranking identical category labels.
        // Facet-only geo browse prunes cells by their summaries and stops as
        // soon as the requested page is full.
        q: categoryFacetSafe && params.geo ? "" : nearbyCategory.query,
        ...(categoryFacetSafe ? {
          filters: {
            ...(params.filters || {}),
            facets: {
              ...(params.filters?.facets || {}),
              type: [nearbyCategory.type]
            }
          }
        } : {})
      });
      return collapseCivicDuplicates({
        ...response,
        ...(params.geo ? { resolvedQuery: nearbyCategory.label } : {}),
        stats: {
          ...(response.stats || {}),
          ...(params.geo ? {
            plannerLane: "osmCategoryGeo",
            osmIntentCategory: nearbyCategory.query,
            ...(categoryFacetSafe ? { osmIntentCategoryFacet: true } : {})
          } : {})
        }
      });
    }
    const intersection = await resolveIntersectionLocality(engine, q, params, mapAnchor);
    if (intersection) return intersection;
    const street = await resolveStreetLocality(engine, q, params);
    if (street) return street;
    const namedText = !mapAnchor ? await resolveGlobalNamedText(engine, q, params) : null;
    if (namedText) return collapseCivicDuplicates(namedText);
    // Local-first text: scoped to the anchor's radius, geo routing opens the
    // one or two shards whose coverage contains the caller instead of every
    // shard holding the terms, and proximity boosts break BM25 ties toward
    // the nearby bearer of a common name. Run this before the locality probe:
    // a named destination ending in a place-like word must not open every
    // shard merely to prove the full POI name is not a city.
    let localProbeStats = null;
    if (anchor && q) {
      const localScope = params.shards == null
        ? anchorRadiusCoverageShards(engine, anchor, NEAR_TEXT_RADIUS_METERS)
        : [];
      const local = await engine.search({
        ...params,
        ...(localScope.length ? { shards: localScope } : {}),
        geo: {
          near: { lat: anchor.lat, lon: anchor.lon, radiusMeters: NEAR_TEXT_RADIUS_METERS },
          boost: NEAR_TEXT_BOOST
        }
      });
      if (local.total > 0 && local.results.length) {
        return collapseCivicDuplicates({
          ...local,
          stats: {
            ...(local.stats || {}),
            plannerLane: "osmNearText",
            osmIntentRadiusMeters: NEAR_TEXT_RADIUS_METERS,
            ...(localScope.length ? { osmIntentCoverageShards: localScope } : {})
          }
        });
      }
      localProbeStats = local.stats;
    }
    if (possibleLocalityQuery(q, lexicon)) {
      const locality = await resolveLocality(engine, q, params);
      if (locality) {
        const response = localityExactResponse(locality, params, q);
        const trace = mergeRuntimeTraces(localProbeStats?.trace, response.stats?.trace);
        return {
          ...response,
          stats: {
            ...(response.stats || {}),
            ...(trace ? { trace } : {})
          }
        };
      }
    }
    if (anchor && q) {
      const global = await engine.search(params);
      const trace = mergeRuntimeTraces(localProbeStats?.trace, global.stats?.trace);
      return collapseCivicDuplicates({
        ...global,
        stats: {
          ...(global.stats || {}),
          ...(trace ? { trace } : {}),
          osmNearFallback: true
        }
      });
    }
    return collapseCivicDuplicates(await searchNearestWithBudgetFallback(engine, params));
  }
  const exactIntentCandidate = intent.order === "locality-category"
    || String(intent.locality || "").trim().split(/\s+/u).filter(Boolean).length < 2;
  const exactIntentText = !mapAnchor && !intent.connector && exactIntentCandidate
    ? await resolveGlobalNamedText(engine, q, params, { exactOnly: true })
    : null;
  if (exactIntentText) return collapseCivicDuplicates(exactIntentText);
  const namedCategoryLocality = await resolveNamedCategoryLocality(engine, intent, params);
  if (namedCategoryLocality) return collapseCivicDuplicates(namedCategoryLocality);
  // "Bar Harbor", "Park City", "Miami Beach", "Market Harborough": place
  // names that open or close with a category word. Before splitting a
  // connectorless query, the whole surface gets one shot at resolving as a
  // locality — the resolver caches misses, and "cinema in Nice" (connector)
  // states its intent explicitly and skips straight to the split. Both
  // resolutions are independent reads, so they run concurrently: the probe
  // usually misses, and paying its latency serially would double a cold
  // "cinema laval" for nothing.
  let locality;
  let localityProbeStats = null;
  const anchorScope = mapAnchor && params.shards == null
    ? anchorCoverageShards(engine, mapAnchor)
    : [];
  const routedParams = anchorScope.length ? { ...params, shards: anchorScope } : params;
  if (!intent.connector) {
    const { wholePlace, locality: split } = await resolveAmbiguousCategoryLocality(
      engine,
      q,
      intent.locality,
      routedParams
    );
    if (wholePlace) return localityExactResponse(wholePlace, params, q);
    locality = split;
  } else {
    locality = await resolveLocality(engine, intent.locality, routedParams);
  }
  if (!locality && mapAnchor && q) {
    const explicitScope = explicitShardScope(params);
    const localScope = explicitScope.length
      ? explicitScope
      : anchorRadiusCoverageShards(engine, mapAnchor, NEAR_TEXT_RADIUS_METERS);
    const intentNameTokens = !intent.connector
      ? fold(intent.locality).split(" ").filter(token => token.length >= 3)
      : [];
    const distinctiveIntentToken = intentNameTokens.at(-1);
    const categoryFacetSafe = shardScopeSupportsFeature(engine, localScope, "facetSummaryUint32");
    const local = await engine.search({
      ...params,
      ...(localScope.length ? { shards: localScope } : {}),
      ...(distinctiveIntentToken ? { q: distinctiveIntentToken } : {}),
      ...(distinctiveIntentToken && categoryFacetSafe ? {
        filters: {
          ...(params.filters || {}),
          facets: {
            ...(params.filters?.facets || {}),
            type: [intent.category.type]
          }
        }
      } : {}),
      size: Math.min(100, Math.max(32, Number(params.size || 10) * 3)),
      geo: {
        near: {
          lat: mapAnchor.lat,
          lon: mapAnchor.lon,
          radiusMeters: NEAR_TEXT_RADIUS_METERS
        },
        boost: NEAR_TEXT_BOOST
      }
    });
    localityProbeStats = local.stats;
    const localResults = distinctiveIntentToken
      ? (local.results || []).filter(result => {
          const name = fold(result.name || result.title);
          return intentNameTokens.every(token => name.includes(token));
        })
      : local.results || [];
    if (localResults.length) {
      return collapseCivicDuplicates({
        ...local,
        total: distinctiveIntentToken ? localResults.length : local.total,
        size: Number(params.size || 10),
        results: localResults.slice(0, Number(params.size || 10)),
        stats: {
          ...(local.stats || {}),
          plannerLane: "osmNearText",
          osmIntentRadiusMeters: NEAR_TEXT_RADIUS_METERS,
          osmAmbiguousIntentTextFirst: true,
          ...(distinctiveIntentToken ? {
            osmIntentCategoryFacet: categoryFacetSafe,
            osmIntentNameToken: distinctiveIntentToken
          } : {}),
          ...(localScope.length ? { osmIntentCoverageShards: localScope } : {})
        }
      });
    }
    // The map is context, not a restriction. If the bounded local probe
    // misses, resolve the named place globally so an explicit query such as
    // "restaurants New York" still travels away from the current viewport.
    if (!intent.connector) {
      const { wholePlace, locality: split } = await resolveAmbiguousCategoryLocality(
        engine,
        q,
        intent.locality,
        params
      );
      if (wholePlace) return localityExactResponse(wholePlace, params, q);
      locality = split;
    } else {
      locality = await resolveLocality(engine, intent.locality, params);
    }
  }
  if (!locality) {
    const response = await searchNearestWithBudgetFallback(engine, params);
    const trace = mergeRuntimeTraces(localityProbeStats?.trace, response.stats?.trace);
    return collapseCivicDuplicates({
      ...response,
      stats: {
        ...(response.stats || {}),
        ...(trace ? { trace } : {}),
        ...(localityProbeStats ? { osmNearFallback: true } : {})
      }
    });
  }

  // A category word plus a proper name often false-parses as category +
  // locality when some distant hamlet happens to bear the name: "garage
  // brunet" means the Garage Marcel Brunet next door, not car repair shops
  // around the village of Brunet in Provence. A weak locality (below town
  // rank, no recorded population) an ocean away from the caller's anchor is
  // far more likely a coincidence than a travel plan, so the anchored text
  // lane gets the first shot; an empty local page falls through to the
  // category interpretation unchanged.
  const weakDistantLocality = anchor
    && typePriority(locality.type) <= 2
    && !Number(locality.population || 0)
    && haversineMeters(anchor.lat, anchor.lon, locality.lat, locality.lon) > WEAK_LOCALITY_ANCHOR_METERS;
  let weakLocalProbe = null;
  if (weakDistantLocality) {
    weakLocalProbe = await engine.search({
      ...params,
      geo: {
        near: { lat: anchor.lat, lon: anchor.lon, radiusMeters: NEAR_TEXT_RADIUS_METERS },
        boost: NEAR_TEXT_BOOST
      }
    });
    if (weakLocalProbe.total > 0 && weakLocalProbe.results.length) {
      return collapseCivicDuplicates({
        ...weakLocalProbe,
        stats: {
          ...(weakLocalProbe.stats || {}),
          plannerLane: "osmNearText",
          osmIntentRadiusMeters: NEAR_TEXT_RADIUS_METERS,
          osmWeakLocalityTextFirst: true
        }
      });
    }
  }

  // Federation results carry their owning shard. Once the locality has
  // resolved, that provenance is a stronger route than the category's
  // global text directory: searching "cinema" around Laval needs Quebec,
  // not every shard that happens to contain the word and lacks a usable
  // bbox. Preserve an explicit caller scope when one was supplied.
  const localityShard = String(locality.shard || "").split("/")[0];
  const explicitScope = explicitShardScope(params);
  const localityScope = explicitScope.length ? explicitScope : (localityShard ? [localityShard] : []);
  const categoryFacetSafe = shardScopeSupportsFeature(engine, localityScope, "facetSummaryUint32");
  const response = await searchNearestWithBudgetFallback(engine, {
    ...params,
    ...(params.shards == null && localityShard ? { shards: [localityShard] } : {}),
    q: intent.category.query,
    // The category lexicon resolves the user's alias to the corpus's exact
    // OSM `type` value. Supplying it as a facet lets the geo tree reject
    // cells that cannot contain the category before opening their range
    // pages; the text term remains for compatibility and scoring.
    ...(categoryFacetSafe ? {
      filters: {
        ...(params.filters || {}),
        facets: {
          ...(params.filters?.facets || {}),
          type: [intent.category.type]
        }
      }
    } : {}),
    geo: {
      near: {
        lat: locality.lat,
        lon: locality.lon,
        radiusMeters: localityRadiusMeters(locality.type)
      },
      sort: "distance"
    }
  });
  const trace = mergeRuntimeTraces(
    locality[LOCALITY_SEARCH_STATS]?.trace,
    weakLocalProbe?.stats?.trace,
    response.stats?.trace
  );
  return {
    ...response,
    resolvedQuery: `${intent.category.label} ${locality.name || intent.locality}`,
    stats: {
      ...(response.stats || {}),
      ...(trace ? { trace } : {}),
      plannerLane: "osmCategoryLocality",
      osmIntentCategory: intent.category.query,
      osmIntentCategoryType: intent.category.type,
      ...(categoryFacetSafe ? { osmIntentCategoryFacet: true } : {}),
      osmIntentLocality: locality.name || intent.locality,
      osmIntentLocalityType: locality.type || "",
      osmIntentRadiusMeters: localityRadiusMeters(locality.type),
      ...(localityShard ? { osmIntentShard: localityShard } : {}),
      ...(weakLocalProbe ? { osmWeakLocalityTextProbe: true } : {})
    }
  };
}
