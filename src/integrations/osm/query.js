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
const LOCALITY_SEARCH_STATS = Symbol("rangefind.localitySearchStats");
const CANADIAN_POSTAL_CODE = /^\s*([abceghj-nprstvxy]\d[abceghj-nprstvwxyz])\s*([0-9][abceghj-nprstvwxyz][0-9])\s*$/iu;
const LOCALITY_TYPES = new Set(["city", "town", "municipality", "village", "hamlet"]);
const STREET_DESIGNATORS = new Set([
  "allee", "avenue", "boulevard", "chemin", "cote", "cour", "impasse",
  "montee", "place", "rang", "route", "rue", "terrasse",
  "court", "drive", "highway", "lane", "road", "street"
]);
const COORDINATE_NUMBER = "[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)";
const DECIMAL_COORDINATES = new RegExp(
  `^\\s*(${COORDINATE_NUMBER})(?:\\s*,\\s*|\\s+)(${COORDINATE_NUMBER})\\s*$`,
  "u"
);
const INTERSECTION_CONNECTOR = /\s+(?:&|and|at|et|x)\s+|\s*\/\s*/iu;

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

function coordinateResponse(coordinate, params) {
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
      plannerLane: "osmCoordinates",
      shardsQueried: 0
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
        const lookup = await engine.authorityLookup(surface, { size: 32 });
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
      } catch {
        return null;
      }
    })();
    promise.catch(() => cache.delete(key));
    cache.set(key, promise);
  }
  return cache.get(key);
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
  const authority = await exactAuthorityScope(engine, q);
  if (!authority && !allowUngatedFuzzy) return null;
  const coverage = anchorCoverageShards(engine, anchor);
  const authorityCoverage = authority?.shards?.length
    ? coverage.filter(shard => authority.shards.includes(shard))
    : [];
  const scope = authorityCoverage.length ? authorityCoverage : coverage;
  const requestedSize = Math.max(1, Number(params.size || 10));
  const intentNameTokens = !authority && allowUngatedFuzzy && intent && !intent.connector
    ? fold(intent.locality).split(" ").filter(token => token.length >= 3)
    : [];
  const distinctiveIntentToken = intentNameTokens.at(-1);
  const categoryFacetSafe = engine.manifest?.features?.facetSummaryUint32 === true;
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

export async function suggestOsmQuery(engine, rawParams = {}) {
  const params = engineParams(rawParams);
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
    return {
      ...localityResponse,
      q,
      suggestions,
      stats: {
        ...(localityResponse.stats || {}),
        plannerLane: "osmSuggestCategoryLocality",
        osmIntentCategory: intent.category.query,
        ...(coverage.length ? { osmSuggestCoverageShards: coverage } : {})
      }
    };
  }
  const response = await engine.suggest({
    ...params,
    ...(coverage.length ? { shards: coverage } : {}),
    size: hasHouseNumber(params.q) ? requestedSize : Math.max(32, requestedSize)
  });
  const collapsed = collapseStreetSuggestions(response, { ...params, size: requestedSize });
  return coverage.length ? {
    ...collapsed,
    stats: {
      ...(collapsed.stats || {}),
      osmSuggestCoverageShards: coverage
    }
  } : collapsed;
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
      const lookup = await engine.authorityLookup(surface, { size: 8 });
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
    const candidateLocality = await resolveLocality(
      engine,
      rightTokens.slice(split).join(" "),
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
  const searchStreet = async street => {
    const scoped = {
      ...params,
      size: 100,
      typo: false,
      geo: undefined,
      ...(localityShard ? { shards: [localityShard] } : coverage.length ? { shards: coverage } : {})
    };
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
    && haversineMeters(locality.lat, locality.lon, result.lat, result.lon) <= radiusMeters
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
    const locality = await resolveLocality(engine, localitySurface, params);
    if (!locality) continue;

    const streetSurface = streetTokens.join(" ");
    const streetKey = fold(streetSurface);
    // Rank by plain text relevance, scoped to the locality's root shard,
    // and enforce the locality radius on the returned page here instead of
    // through the engine's geo machinery. A distance sort would decode
    // every posting in the radius (street tokens like "saint" blow through
    // the geoTextSortMaxDf budget in dense shards and fail the query), and
    // a radius filter verifies every text candidate's lat/lon through
    // doc-value chunks — both cost far more than checking thirty results.
    // A miss falls back to the surrounding cascade like before.
    const localityShard = String(locality.shard || "").split("/")[0];
    let response;
    try {
      response = await engine.search({
        ...params,
        q: coreTokens.join(" "),
        size: Math.max(30, Number(params.size || 10)),
        geo: undefined,
        ...(localityShard ? { shards: [localityShard] } : {})
      });
    } catch {
      // A shard-level posting budget or transport failure must not kill the
      // whole query — fall back to the surrounding cascade.
      return null;
    }
    const radiusMeters = localityRadiusMeters(locality.type);
    const nearLocality = result => haversineMeters(locality.lat, locality.lon, result.lat, result.lon) <= radiusMeters;
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
          osmIntentLocality: locality.name || localitySurface,
          osmIntentLocalityType: locality.type || "",
          osmIntentRadiusMeters: localityRadiusMeters(locality.type),
          osmIntentCivicAddress: true
        }
      };
    }
    const anchor = street || streetAnchor;
    if (!anchor) return null;
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
        osmIntentLocality: locality.name || localitySurface,
        osmIntentLocalityType: locality.type || "",
        osmIntentRadiusMeters: localityRadiusMeters(locality.type)
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
  if (coordinate) return coordinateResponse(coordinate, params);
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
      const categoryFacetSafe = engine.manifest?.features?.facetSummaryUint32 === true;
      for (const radius of NEARBY_CATEGORY_RADII_METERS) {
        radiusMeters = radius;
        response = await searchNearestWithBudgetFallback(engine, {
          ...params,
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
          ...(categoryFacetSafe ? { osmIntentCategoryFacet: true } : {})
        }
      };
    }
    // Near-me phrasing without a usable anchor: the words themselves are
    // not a place, so skip the street/locality resolvers and search the
    // category text directly.
    if (nearbyCategory) {
      const categoryFacetSafe = engine.manifest?.features?.facetSummaryUint32 === true;
      const response = await searchNearestWithBudgetFallback(engine, {
        ...params,
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
    const [wholePlace, split] = await Promise.all([
      resolveLocality(engine, q, routedParams, { authorityMissIsFinal: true }),
      resolveLocality(engine, intent.locality, routedParams)
    ]);
    if (wholePlace) return localityExactResponse(wholePlace, params, q);
    locality = split;
  } else {
    locality = await resolveLocality(engine, intent.locality, routedParams);
  }
  if (!locality && mapAnchor && q) {
    const localScope = params.shards == null
      ? anchorRadiusCoverageShards(engine, mapAnchor, NEAR_TEXT_RADIUS_METERS)
      : [];
    const intentNameTokens = !intent.connector
      ? fold(intent.locality).split(" ").filter(token => token.length >= 3)
      : [];
    const distinctiveIntentToken = intentNameTokens.at(-1);
    const categoryFacetSafe = engine.manifest?.features?.facetSummaryUint32 === true;
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
      const [wholePlace, split] = await Promise.all([
        resolveLocality(engine, q, params, { authorityMissIsFinal: true }),
        resolveLocality(engine, intent.locality, params)
      ]);
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
  const localityShardDescriptor = localityShard
    ? engine.manifest?.shards?.find(shard => String(shard.id) === localityShard)
    : null;
  const categoryFacetSafe = engine.manifest?.features?.facetSummaryUint32 === true
    || localityShardDescriptor?.features?.facetSummaryUint32 === true;
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
