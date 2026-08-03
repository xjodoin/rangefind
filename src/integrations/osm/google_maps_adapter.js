import {
  reverseGeocodeOsm,
  searchAlongRouteOsm,
  searchOsmQuery,
  suggestOsmQuery
} from "./query.js";

const TYPE_ALIASES = Object.freeze({
  amusement_park: "theme park",
  coffee_shop: "cafe",
  gas_station: "fuel",
  grocery_store: "supermarket",
  movie_theater: "cinema",
  park_and_ride: "park and ride",
  public_bath: "public bath",
  shopping_mall: "mall",
  tourist_attraction: "attraction"
});

function clampSize(value, fallback = 10, max = 50) {
  const parsed = Math.floor(Number(value ?? fallback));
  return Number.isFinite(parsed) ? Math.max(1, Math.min(max, parsed)) : fallback;
}

function normalizeLocation(value, label = "location") {
  const lat = Number(value?.latitude ?? value?.lat);
  const lon = Number(value?.longitude ?? value?.lng ?? value?.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    throw new TypeError(`${label} needs latitude/longitude or lat/lng.`);
  }
  return { lat, lon };
}

function centerOfRectangle(rectangle) {
  if (!rectangle) return null;
  if (rectangle.low && rectangle.high) {
    const low = normalizeLocation(rectangle.low, "rectangle.low");
    const high = normalizeLocation(rectangle.high, "rectangle.high");
    return { lat: (low.lat + high.lat) / 2, lon: (low.lon + high.lon) / 2 };
  }
  const north = Number(rectangle.north ?? rectangle.maxLat);
  const south = Number(rectangle.south ?? rectangle.minLat);
  const east = Number(rectangle.east ?? rectangle.maxLon);
  const west = Number(rectangle.west ?? rectangle.minLon);
  if (![north, south, east, west].every(Number.isFinite)) return null;
  return { lat: (north + south) / 2, lon: (east + west) / 2 };
}

function rectangleGeo(rectangle) {
  if (!rectangle) return null;
  if (rectangle.low && rectangle.high) {
    const low = normalizeLocation(rectangle.low, "rectangle.low");
    const high = normalizeLocation(rectangle.high, "rectangle.high");
    return {
      box: {
        minLat: Math.min(low.lat, high.lat),
        minLon: Math.min(low.lon, high.lon),
        maxLat: Math.max(low.lat, high.lat),
        maxLon: Math.max(low.lon, high.lon)
      }
    };
  }
  const north = Number(rectangle.north ?? rectangle.maxLat);
  const south = Number(rectangle.south ?? rectangle.minLat);
  const east = Number(rectangle.east ?? rectangle.maxLon);
  const west = Number(rectangle.west ?? rectangle.minLon);
  if (![north, south, east, west].every(Number.isFinite)) return null;
  return { box: { minLat: south, minLon: west, maxLat: north, maxLon: east } };
}

function circleFrom(value) {
  const circle = value?.circle || (value?.center ? value : null);
  if (!circle) return null;
  const center = normalizeLocation(circle.center, "circle.center");
  const radiusMeters = Number(circle.radius ?? circle.radiusMeters);
  return {
    center,
    ...(Number.isFinite(radiusMeters) && radiusMeters > 0 ? { radiusMeters } : {})
  };
}

function requestBias(request) {
  const circle = circleFrom(request.locationBias);
  if (circle) return circle.center;
  return centerOfRectangle(request.locationBias?.rectangle || request.locationBias);
}

function requestRestriction(request, { distanceSort = false } = {}) {
  const circle = circleFrom(request.locationRestriction);
  if (circle) {
    if (!circle.radiusMeters) throw new RangeError("A circular locationRestriction needs a positive radius.");
    return {
      near: { ...circle.center, radiusMeters: circle.radiusMeters },
      ...(distanceSort ? { sort: "distance" } : {})
    };
  }
  return rectangleGeo(request.locationRestriction?.rectangle || request.locationRestriction);
}

function canonicalType(value) {
  const type = String(value || "").trim();
  return TYPE_ALIASES[type] || type.replaceAll("_", " ");
}

function resultTypes(result) {
  return [...new Set([
    ...(Array.isArray(result.types) ? result.types : []),
    ...(Array.isArray(result.category) ? result.category : []),
    result.type,
    typeof result.category === "string" ? result.category : null
  ].filter(Boolean).map(String))];
}

function formattedAddress(result) {
  if (result.formattedAddress) return String(result.formattedAddress);
  if (result.address) return String(result.address);
  return [
    [result.house_number, result.street].filter(Boolean).join(" "),
    result.city,
    result.state,
    result.postcode,
    result.country
  ].filter(Boolean).join(", ");
}

function sourceId(result) {
  if (result.id != null) return String(result.id);
  if (result.osm_type && result.osm_id != null) return `${result.osm_type}/${result.osm_id}`;
  return null;
}

/**
 * Convert a Rangefind OSM result into a migration-friendly Place object.
 * This intentionally is not advertised as a byte-for-byte Google Place.
 */
export function toMigrationPlace(result) {
  const id = sourceId(result);
  const lat = Number(result.lat);
  const lon = Number(result.lon ?? result.lng);
  const types = resultTypes(result);
  const details = result.details || {};
  return {
    ...(id ? { id } : {}),
    displayName: { text: String(result.name || result.title || formattedAddress(result) || id || "") },
    formattedAddress: formattedAddress(result),
    ...(Number.isFinite(lat) && Number.isFinite(lon)
      ? { location: { latitude: lat, longitude: lon } }
      : {}),
    ...(types[0] ? { primaryType: types[0] } : {}),
    types,
    ...(details.opening_hours || result.openNow != null ? {
      openingHours: {
        osmExpression: details.opening_hours || null,
        openNow: result.openNow ?? null,
        state: result.openingHoursState || "unknown"
      }
    } : {}),
    ...(details.wheelchair ? {
      accessibilityOptions: { wheelchair: details.wheelchair }
    } : {}),
    ...(details.phone ? { nationalPhoneNumber: details.phone } : {}),
    ...(details.website ? { websiteUri: details.website } : {}),
    ...(result.geometry ? { geometry: result.geometry } : {}),
    details,
    source: {
      dataset: "OpenStreetMap",
      osmType: result.osm_type || null,
      osmId: result.osm_id ?? null
    },
    rangefind: {
      score: result.score ?? null,
      shard: result.shard ?? null,
      distanceMeters: result.distanceMeters ?? null,
      routeDistanceMeters: result.routeDistanceMeters ?? null,
      routeProgressMeters: result.routeProgressMeters ?? null,
      routeProgressRatio: result.routeProgressRatio ?? null,
      routeBearingDegrees: result.routeBearingDegrees ?? null,
      routeRank: result.routeRank ?? null,
      rejoinPoint: result.rejoinPoint ?? null,
      constraintMatches: result.constraintMatches ?? null,
      locationType: result.locationType ?? null,
      reverseGeocodeAccuracy: result.reverseGeocodeAccuracy ?? null
    }
  };
}

function responseMeta(response) {
  return {
    total: response.total ?? response.results?.length ?? 0,
    approximate: response.approximate === true,
    correctedQuery: response.correctedQuery || null,
    stats: response.stats || null
  };
}

function statusFor(items) {
  return items.length ? "OK" : "ZERO_RESULTS";
}

/**
 * A small migration facade for applications moving common Google Maps Places
 * and Geocoding calls to a Rangefind OSM index. Methods return Promises and use
 * Google-like request names while keeping Rangefind/OSM metadata visible.
 */
export function createRangefindMapsAdapter(engine, options = {}) {
  if (!engine) throw new TypeError("createRangefindMapsAdapter needs a Rangefind engine.");
  const runSearch = options.searchOsmQuery || searchOsmQuery;
  const runSuggest = options.suggestOsmQuery || suggestOsmQuery;
  const runReverse = options.reverseGeocodeOsm || reverseGeocodeOsm;
  const runRoute = options.searchAlongRouteOsm || searchAlongRouteOsm;
  const defaults = options.defaults || {};
  const placeCache = new Map();

  function mapPlaces(response) {
    const places = (response.results || []).map(toMigrationPlace);
    for (const place of places) if (place.id) placeCache.set(place.id, place);
    return places;
  }

  async function autocomplete(request = {}) {
    const input = String(request.input ?? request.query ?? "");
    const size = clampSize(request.maxResultCount ?? request.size, 8, 50);
    const near = requestBias(request) || defaults.near;
    const response = await runSuggest(engine, {
      query: input,
      q: input,
      size,
      limit: size,
      inputOffset: request.inputOffset,
      ...(near ? { near } : {}),
      ...(request.shards ? { shards: request.shards } : {}),
      trace: request.trace ?? defaults.trace
    });
    const includedTypes = new Set((request.includedPrimaryTypes || []).map(canonicalType));
    const suggestions = (response.suggestions || [])
      .filter(item => !includedTypes.size
        || (item.types || []).map(canonicalType).some(type => includedTypes.has(type)))
      .map(item => ({
      placePrediction: {
        text: { text: item.description || item.text },
        structuredFormat: {
          mainText: { text: item.mainText || item.text },
          secondaryText: { text: item.secondaryText || "" }
        },
        types: item.types || [],
        matchedRanges: item.matchedRanges || [],
        rangefindSelection: item.selection || { query: item.text }
      }
    }));
    return {
      suggestions,
      status: statusFor(suggestions),
      rangefind: { normalizedQuery: response.normalizedQuery || null, stats: response.stats || null }
    };
  }

  async function textSearch(request = {}) {
    let query = String(request.textQuery ?? request.query ?? "").trim();
    if (request.includedType) query = `${canonicalType(request.includedType)} ${query}`.trim();
    const size = clampSize(request.maxResultCount ?? request.pageSize, 10, 50);
    const geo = requestRestriction(request);
    const near = geo ? null : requestBias(request) || defaults.near;
    const response = await runSearch(engine, {
      query,
      q: query,
      size,
      limit: size,
      constraints: {
        ...(request.constraints || {}),
        ...(request.openNow === true ? { openNow: true } : {})
      },
      ...(geo ? { geo } : {}),
      ...(near ? { near } : {}),
      ...(request.timeZone || defaults.timeZone ? { timeZone: request.timeZone || defaults.timeZone } : {}),
      ...(request.at ? { at: request.at } : {}),
      ...(request.shards ? { shards: request.shards } : {}),
      trace: request.trace ?? defaults.trace
    });
    const places = mapPlaces(response);
    return { places, status: statusFor(places), rangefind: responseMeta(response) };
  }

  async function nearbySearch(request = {}) {
    const geo = requestRestriction(request, { distanceSort: request.rankPreference !== "POPULARITY" });
    if (!geo?.near) throw new TypeError("nearbySearch needs a circular locationRestriction.");
    const size = clampSize(request.maxResultCount, 10, 50);
    const included = (request.includedTypes || []).map(canonicalType).filter(Boolean);
    const queries = included.length ? included : [""];
    const responses = await Promise.all(queries.map(query => runSearch(engine, {
      query,
      q: query,
      geo,
      size: Math.min(50, size * 2),
      limit: Math.min(50, size * 2),
      constraints: {
        ...(request.constraints || {}),
        ...(request.openNow === true ? { openNow: true } : {})
      },
      ...(request.timeZone || defaults.timeZone ? { timeZone: request.timeZone || defaults.timeZone } : {}),
      trace: request.trace ?? defaults.trace
    })));
    const excluded = new Set((request.excludedTypes || []).map(canonicalType));
    const includedSet = new Set(included);
    const merged = new Map();
    for (const response of responses) {
      for (const result of response.results || []) {
        const types = new Set(resultTypes(result).map(canonicalType));
        if (includedSet.size && ![...includedSet].some(type => types.has(type))) continue;
        if ([...excluded].some(type => types.has(type))) continue;
        const key = sourceId(result) || `${result.lat}:${result.lon}:${result.name || result.title}`;
        const prior = merged.get(key);
        if (!prior || Number(result.score || 0) > Number(prior.score || 0)) merged.set(key, result);
      }
    }
    const rows = [...merged.values()];
    if (request.rankPreference !== "POPULARITY") {
      rows.sort((a, b) => Number(a.distanceMeters ?? Infinity) - Number(b.distanceMeters ?? Infinity));
    } else {
      rows.sort((a, b) => Number(b.score || 0) - Number(a.score || 0));
    }
    const synthetic = { results: rows.slice(0, size), total: rows.length, stats: { queries: responses.map(item => item.stats) } };
    const places = mapPlaces(synthetic);
    return { places, status: statusFor(places), rangefind: responseMeta(synthetic) };
  }

  async function geocode(request = {}) {
    const address = String(request.address ?? request.query ?? "").trim();
    if (!address) throw new TypeError("geocode needs an address.");
    const size = clampSize(request.maxResultCount, 5, 25);
    const response = await runSearch(engine, {
      query: address,
      q: address,
      size,
      limit: size,
      ...(requestBias(request) || defaults.near ? { near: requestBias(request) || defaults.near } : {}),
      trace: request.trace ?? defaults.trace
    });
    const results = mapPlaces(response);
    return { results, status: statusFor(results), rangefind: responseMeta(response) };
  }

  async function reverseGeocode(request = {}) {
    const location = normalizeLocation(request.location || request, "reverseGeocode.location");
    const response = await runReverse(engine, {
      ...location,
      radiusMeters: request.radiusMeters,
      size: clampSize(request.maxResultCount ?? request.size, 8, 25),
      resultTypes: request.resultTypes,
      locationTypes: request.locationTypes,
      localityRadiusMeters: request.localityRadiusMeters,
      shards: request.shards,
      trace: request.trace ?? defaults.trace
    });
    const results = mapPlaces(response);
    return { results, status: statusFor(results), rangefind: responseMeta(response) };
  }

  async function searchAlongRoute(request = {}) {
    if (!request.route) throw new TypeError("searchAlongRoute needs route geometry.");
    const response = await runRoute(engine, {
      route: request.route,
      query: String(request.query ?? request.textQuery ?? ""),
      corridorMeters: request.corridorMeters,
      polylinePrecision: request.polylinePrecision,
      routePositionMeters: request.routePositionMeters,
      routeDirection: request.routeDirection,
      viewport: request.viewport,
      constraints: {
        ...(request.constraints || {}),
        ...(request.openNow === true ? { openNow: true } : {})
      },
      timeZone: request.timeZone || defaults.timeZone,
      at: request.at,
      limit: clampSize(request.maxResultCount ?? request.limit, 20, 50),
      shards: request.shards,
      trace: request.trace ?? defaults.trace
    });
    const places = mapPlaces(response);
    return { places, status: statusFor(places), rangefind: responseMeta(response) };
  }

  async function placeDetails(request = {}) {
    const placeId = String(request.placeId ?? request.id ?? "");
    if (!placeId) throw new TypeError("placeDetails needs placeId.");
    const place = placeCache.get(placeId);
    if (!place) {
      throw new Error(
        `Place ${placeId} is not in this adapter's result cache. `
        + "Rangefind returns indexed details inline; retain the selected Place instead of issuing a second request."
      );
    }
    return { place, status: "OK" };
  }

  return {
    autocomplete,
    textSearch,
    nearbySearch,
    geocode,
    reverseGeocode,
    searchAlongRoute,
    placeDetails,
    clearPlaceCache: () => placeCache.clear()
  };
}

export default createRangefindMapsAdapter;
