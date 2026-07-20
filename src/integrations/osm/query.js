const CATEGORY_INTENTS = new Map([
  ["pharmacie", { query: "pharmacy", label: "Pharmacie" }],
  ["pharmacy", { query: "pharmacy", label: "Pharmacy" }],
  ["pharmacies", { query: "pharmacy", label: "Pharmacies" }],
  ["cafe", { query: "cafe", label: "Cafe" }],
  ["cafes", { query: "cafe", label: "Cafes" }],
  ["coffee", { query: "cafe", label: "Coffee" }],
  ["restaurant", { query: "restaurant", label: "Restaurant" }],
  ["restaurants", { query: "restaurant", label: "Restaurants" }],
  ["hotel", { query: "hotel", label: "Hotel" }],
  ["hotels", { query: "hotel", label: "Hotels" }],
  ["hospital", { query: "hospital", label: "Hospital" }],
  ["hospitals", { query: "hospital", label: "Hospitals" }],
  ["hopital", { query: "hospital", label: "Hopital" }],
  ["hopitaux", { query: "hospital", label: "Hopitaux" }],
  ["park", { query: "park", label: "Park" }],
  ["parks", { query: "park", label: "Parks" }],
  ["parc", { query: "park", label: "Parc" }],
  ["parcs", { query: "park", label: "Parcs" }],
  ["supermarket", { query: "supermarket", label: "Supermarket" }],
  ["supermarkets", { query: "supermarket", label: "Supermarkets" }],
  ["epicerie", { query: "supermarket", label: "Epicerie" }],
  ["epiceries", { query: "supermarket", label: "Epiceries" }]
]);
const LOCALITY_CONNECTORS = new Set(["a", "around", "dans", "de", "du", "in", "near", "pres"]);
const LOCALITY_CACHE = new WeakMap();
const LOCALITY_SEARCH_STATS = Symbol("rangefind.localitySearchStats");
const CANADIAN_POSTAL_CODE = /^\s*([abceghj-nprstvxy]\d[abceghj-nprstvwxyz])\s*([0-9][abceghj-nprstvwxyz][0-9])\s*$/iu;
const LOCALITY_TYPES = new Set(["city", "town", "municipality", "village", "hamlet"]);
const STREET_DESIGNATORS = new Set([
  "allee", "avenue", "boulevard", "chemin", "cote", "cour", "impasse",
  "montee", "place", "rang", "route", "rue", "terrasse",
  "court", "drive", "highway", "lane", "road", "street"
]);

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

function fold(value) {
  return String(value || "")
    .normalize("NFKD")
    .replaceAll(/\p{M}+/gu, "")
    .toLocaleLowerCase("en-US")
    .replaceAll(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

export function parseOsmQueryIntent(value) {
  const surface = String(value || "").trim();
  const tokens = surface.split(/\s+/u).filter(Boolean);
  if (tokens.length < 2) return null;
  const first = CATEGORY_INTENTS.get(fold(tokens[0]));
  if (first) {
    const localityTokens = tokens.slice(1);
    while (LOCALITY_CONNECTORS.has(fold(localityTokens[0]))) localityTokens.shift();
    if (!localityTokens.length) return null;
    return {
      category: first,
      locality: localityTokens.join(" "),
      order: "category-locality"
    };
  }
  const last = CATEGORY_INTENTS.get(fold(tokens.at(-1)));
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

export function parseNearbyCategoryIntent(value) {
  let normalized = fold(value);
  if (!normalized) return null;
  for (const suffix of NEARBY_SUFFIXES) {
    if (normalized === suffix) return null;
    if (normalized.endsWith(` ${suffix}`)) {
      normalized = normalized.slice(0, -suffix.length - 1).trim();
      break;
    }
  }
  return CATEGORY_INTENTS.get(normalized) || null;
}

function nearAnchor(params) {
  const near = params?.near;
  const lat = Number(near?.lat);
  const lon = Number(near?.lon);
  return Number.isFinite(lat) && Number.isFinite(lon) ? { lat, lon } : null;
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

function possibleLocalityQuery(value) {
  const surface = String(value || "").trim();
  const normalized = fold(surface);
  const tokens = normalized.split(" ").filter(Boolean);
  return tokens.length >= 1
    && tokens.length <= 4
    && !tokens.some(token => /\d/u.test(token))
    && !CATEGORY_INTENTS.has(normalized);
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
  const response = await engine.suggest({
    ...params,
    size: hasHouseNumber(params.q) ? requestedSize : Math.max(32, requestedSize)
  });
  return collapseStreetSuggestions(response, { ...params, size: requestedSize });
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

async function resolveLocality(engine, surface, params = {}) {
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
  const typePriority = new Map([["city", 5], ["town", 4], ["municipality", 3], ["village", 2], ["hamlet", 1]]);
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
      (typePriority.get(right.type) || 0) - (typePriority.get(left.type) || 0)
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
  if (!postalMatch && !shardScope.length && typeof engine.authorityLookup === "function") {
    try {
      const lookup = await engine.authorityLookup(surface, { size: 8 });
      const scoped = [];
      for (const match of lookup?.matches || []) {
        if (!Array.isArray(match.shards) || !match.shards.length) continue;
        if (fold(match.text) !== normalizedLocality) continue;
        for (const id of match.shards) {
          if (!scoped.includes(id)) scoped.push(id);
        }
        if (scoped.length >= 4) break;
      }
      if (scoped.length) authorityShards = scoped.slice(0, 4).sort();
    } catch {
      // The lexicon is an accelerator, never a gate.
    }
  }

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
    if (!postalMatch && localityResponse.total > 0 && (typePriority.get(resolved?.type) || 0) <= 2) {
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

export async function searchOsmQuery(engine, rawParams = {}) {
  // `near` is an advisory anchor (user location or map viewport center) for
  // the cascade only; the engine never sees it. Explicit place intents in
  // the query text always outrank proximity.
  const anchor = rawParams.geo == null ? nearAnchor(rawParams) : null;
  const params = engineParams(rawParams);
  const q = String(params.q || "").trim();
  // "pharmacy near me" parses before category-locality — otherwise "me"
  // would be treated as a locality name and resolved against the planet.
  const nearbyCategory = parseNearbyCategoryIntent(q);
  const intent = nearbyCategory ? null : parseOsmQueryIntent(q);
  if (!intent) {
    // Bare category (or explicit near-me phrasing) with an anchor: a
    // nearest-sorted category search around the caller instead of an
    // unroutable global single-term fan-out. The wider retry covers rural
    // anchors where 10 km holds nothing.
    if (nearbyCategory && anchor) {
      let response = null;
      let radiusMeters = 0;
      for (const radius of NEARBY_CATEGORY_RADII_METERS) {
        radiusMeters = radius;
        response = await engine.search({
          ...params,
          q: nearbyCategory.query,
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
          osmIntentRadiusMeters: radiusMeters
        }
      };
    }
    // Near-me phrasing without a usable anchor: the words themselves are
    // not a place, so skip the street/locality resolvers and search the
    // category text directly.
    if (nearbyCategory) return collapseCivicDuplicates(await engine.search(params));
    const street = await resolveStreetLocality(engine, q, params);
    if (street) return street;
    if (possibleLocalityQuery(q)) {
      const locality = await resolveLocality(engine, q, params);
      if (locality) {
        return {
          total: 1,
          page: 1,
          size: Number(params.size || 10),
          approximate: false,
          results: [locality],
          resolvedQuery: locality.name || q,
          stats: {
            ...(locality[LOCALITY_SEARCH_STATS] || {}),
            plannerLane: "osmLocalityExact",
            osmIntentLocality: locality.name || q,
            osmIntentLocalityType: locality.type || ""
          }
        };
      }
    }
    // Local-first text: scoped to the anchor's radius, geo routing opens the
    // one or two shards whose coverage contains the caller instead of every
    // shard holding the terms, and proximity boosts break BM25 ties toward
    // the nearby bearer of a common name. An empty local page (totals are
    // real counts, never floors) falls back to the global cascade.
    if (anchor && q) {
      const local = await engine.search({
        ...params,
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
            osmIntentRadiusMeters: NEAR_TEXT_RADIUS_METERS
          }
        });
      }
      const global = await engine.search(params);
      const trace = mergeRuntimeTraces(local.stats?.trace, global.stats?.trace);
      return collapseCivicDuplicates({
        ...global,
        stats: {
          ...(global.stats || {}),
          ...(trace ? { trace } : {}),
          osmNearFallback: true
        }
      });
    }
    return collapseCivicDuplicates(await engine.search(params));
  }
  const locality = await resolveLocality(engine, intent.locality, params);
  if (!locality) return collapseCivicDuplicates(await engine.search(params));

  const response = await engine.search({
    ...params,
    q: intent.category.query,
    geo: {
      near: {
        lat: locality.lat,
        lon: locality.lon,
        radiusMeters: localityRadiusMeters(locality.type)
      },
      sort: "distance"
    }
  });
  const trace = mergeRuntimeTraces(locality[LOCALITY_SEARCH_STATS]?.trace, response.stats?.trace);
  return {
    ...response,
    resolvedQuery: `${intent.category.label} ${locality.name || intent.locality}`,
    stats: {
      ...(response.stats || {}),
      ...(trace ? { trace } : {}),
      plannerLane: "osmCategoryLocality",
      osmIntentCategory: intent.category.query,
      osmIntentLocality: locality.name || intent.locality,
      osmIntentLocalityType: locality.type || "",
      osmIntentRadiusMeters: localityRadiusMeters(locality.type)
    }
  };
}
