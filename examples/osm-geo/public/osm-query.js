const CATEGORY_INTENTS = new Map([
  ["pharmacie", { query: "pharmacy", label: "Pharmacie" }],
  ["pharmacy", { query: "pharmacy", label: "Pharmacy" }],
  ["pharmacies", { query: "pharmacy", label: "Pharmacies" }]
]);
const LOCALITY_CACHE = new WeakMap();

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
  if (type === "city") return 30000;
  if (type === "town" || type === "municipality") return 10000;
  if (type === "village") return 7000;
  return 5000;
}

export async function searchOsmQuery(engine, params = {}) {
  const q = String(params.q || "").trim();
  const intent = parseOsmQueryIntent(q);
  if (!intent) return engine.search(params);
  const localityKey = fold(intent.locality);
  if (!LOCALITY_CACHE.has(engine)) LOCALITY_CACHE.set(engine, new Map());
  const cache = LOCALITY_CACHE.get(engine);
  let locality = cache.get(localityKey);
  if (locality === undefined) {
    const localityResponse = await engine.search({
      q: intent.locality,
      filters: { facets: { category: ["place"] } },
      size: 8
    });
    locality = localityResponse.results.find(result => (
      fold(result.name || result.title) === localityKey
      && Number.isFinite(result.lat)
      && Number.isFinite(result.lon)
    )) || null;
    cache.set(localityKey, locality);
  }
  if (!locality) return engine.search(params);

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
