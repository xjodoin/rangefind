const CATEGORY_INTENTS = new Map([
  ["pharmacie", { query: "pharmacy", label: "Pharmacie" }],
  ["pharmacy", { query: "pharmacy", label: "Pharmacy" }],
  ["pharmacies", { query: "pharmacy", label: "Pharmacies" }]
]);
const LOCALITY_CACHE = new WeakMap();
const CANADIAN_POSTAL_CODE = /^\s*([abceghj-nprstvxy]\d[abceghj-nprstvwxyz])\s*([0-9][abceghj-nprstvwxyz][0-9])\s*$/iu;
const LOCALITY_TYPES = new Set(["city", "town", "municipality", "village", "hamlet"]);
const STREET_DESIGNATORS = new Set([
  "allee", "avenue", "boulevard", "chemin", "cote", "cour", "impasse",
  "montee", "place", "rang", "route", "rue", "terrasse",
  "court", "drive", "highway", "lane", "road", "street"
]);

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
    return {
      category: first,
      locality: tokens.slice(1).join(" "),
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

async function resolveLocality(engine, surface) {
  const localityKey = fold(surface);
  if (!LOCALITY_CACHE.has(engine)) LOCALITY_CACHE.set(engine, new Map());
  const cache = LOCALITY_CACHE.get(engine);
  if (cache.has(localityKey)) return cache.get(localityKey);
  const postalMatch = String(surface || "").match(CANADIAN_POSTAL_CODE);
  const localityResponse = await engine.search(postalMatch ? {
    q: surface,
    size: 8
  } : {
    q: surface,
    filters: { facets: { category: ["place"] } },
    size: 8
  });
  const matches = localityResponse.results.filter(result => {
    if (!Number.isFinite(result.lat) || !Number.isFinite(result.lon)) return false;
    if (postalMatch) {
      const expected = `${postalMatch[1]} ${postalMatch[2]}`.toUpperCase();
      return result.type === "postal_code" && String(result.postcode || "").toUpperCase() === expected;
    }
    return LOCALITY_TYPES.has(result.type) && fold(result.name || result.title) === localityKey;
  });
  const typePriority = new Map([["city", 5], ["town", 4], ["municipality", 3], ["village", 2], ["hamlet", 1]]);
  matches.sort((left, right) => (
    (typePriority.get(right.type) || 0) - (typePriority.get(left.type) || 0)
    || Number(right.population || 0) - Number(left.population || 0)
  ));
  const resolved = matches[0] || null;
  cache.set(localityKey, resolved);
  return resolved;
}

async function resolveStreetLocality(engine, surface, params) {
  const tokens = String(surface || "").trim().split(/\s+/u).filter(Boolean);
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
    const locality = await resolveLocality(engine, localitySurface);
    if (!locality) continue;

    const streetSurface = streetTokens.join(" ");
    const streetKey = fold(streetSurface);
    const response = await engine.search({
      ...params,
      q: coreTokens.join(" "),
      size: Math.max(30, Number(params.size || 10)),
      geo: {
        near: {
          lat: locality.lat,
          lon: locality.lon,
          radiusMeters: localityRadiusMeters(locality.type)
        },
        sort: "distance"
      }
    });
    const street = response.results.find(result => (
      Number.isFinite(result.lat)
      && Number.isFinite(result.lon)
      && fold(result.name || result.title) === streetKey
    ));
    if (!street) return null;
    return {
      total: 1,
      page: 1,
      size: Number(params.size || 10),
      approximate: false,
      results: [{
        ...street,
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

export async function searchOsmQuery(engine, params = {}) {
  const q = String(params.q || "").trim();
  const intent = parseOsmQueryIntent(q);
  if (!intent) {
    const street = await resolveStreetLocality(engine, q, params);
    if (street) return street;
    if (possibleLocalityQuery(q)) {
      const locality = await resolveLocality(engine, q);
      if (locality) {
        return {
          total: 1,
          page: 1,
          size: Number(params.size || 10),
          approximate: false,
          results: [locality],
          resolvedQuery: locality.name || q,
          stats: {
            plannerLane: "osmLocalityExact",
            osmIntentLocality: locality.name || q,
            osmIntentLocalityType: locality.type || ""
          }
        };
      }
    }
    return collapseCivicDuplicates(await engine.search(params));
  }
  const locality = await resolveLocality(engine, intent.locality);
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
  return {
    ...response,
    resolvedQuery: `${intent.category.label} ${locality.name || intent.locality}`,
    stats: {
      ...(response.stats || {}),
      plannerLane: "osmCategoryLocality",
      osmIntentCategory: intent.category.query,
      osmIntentLocality: locality.name || intent.locality,
      osmIntentLocalityType: locality.type || "",
      osmIntentRadiusMeters: localityRadiusMeters(locality.type)
    }
  };
}
