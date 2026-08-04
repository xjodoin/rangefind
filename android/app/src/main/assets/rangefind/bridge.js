// Bridge between the Android app and the rangefind browser runtime.
//
// Contract: Kotlin evaluates `__rfCall(id, method, argsJson)`; every call
// answers exactly once through `AndroidBridge.onResult(id, json)` with
// `{ ok: true, payload }` or `{ ok: false, error }`. Binary never crosses
// this boundary — the runtime fetches byte ranges itself over https, and
// only small JSON results come back.

import { createSearch } from "./runtime.browser.js";
import {
  reverseGeocodeOsm,
  searchOsmQuery,
  suggestOsmQuery
} from "./osm.browser.js";
import { openRouteGraphUrl } from "./route.browser.js";

let searchEngine = null;
let routeEngine = null;
let routeUnavailable = null;

function post(id, ok, body) {
  const message = ok ? { ok: true, payload: body } : { ok: false, error: String(body?.message || body) };
  try {
    AndroidBridge.onResult(id, JSON.stringify(message));
  } catch (err) {
    // The activity went away mid-flight; nothing to deliver to.
  }
}

function near(anchor) {
  return anchor && Number.isFinite(anchor.lat) && Number.isFinite(anchor.lon)
    ? { near: { lat: anchor.lat, lon: anchor.lon } }
    : {};
}

// OSM documents carry display fields directly on the result. Normalize the
// handful the app renders so the Kotlin models stay small.
function toPlace(result) {
  const locality = [result.city, result.state, result.country].filter(Boolean).join(", ");
  const street = [result.house_number, result.street].filter(Boolean).join(" ");
  return {
    id: String(result.id ?? ""),
    name: result.name || result.title || street || "Unnamed place",
    address: result.address || [street, locality].filter(Boolean).join(", "),
    locality,
    category: result.category || "",
    type: result.type || "",
    lat: result.lat,
    lon: result.lon,
    distanceMeters: Number.isFinite(result.distanceMeters) ? result.distanceMeters : null
  };
}

function placesOf(response) {
  return (response.results || [])
    .filter(item => Number.isFinite(item.lat) && Number.isFinite(item.lon))
    .map(toPlace);
}

function trimRoute(route) {
  if (!route) return null;
  return {
    seconds: route.seconds,
    adjustedSeconds: route.adjustedSeconds ?? null,
    distanceMeters: route.distanceMeters ?? null,
    bucket: route.bucket || "",
    geometry: route.geometry || [],
    steps: (route.steps || []).map(step => ({
      name: step.name || "",
      meters: step.meters,
      seconds: step.seconds,
      at: step.at ?? 0
    })),
    junctions: (route.junctions || []).map(j => ({
      kind: j.kind,
      lat: j.lat,
      lon: j.lon,
      atMeters: j.atMeters
    })),
    from: route.from || null,
    to: route.to || null,
    stats: {
      httpRequests: route.stats?.httpRequests ?? 0,
      bytesFetched: route.stats?.bytesFetched ?? 0
    }
  };
}

const handlers = {
  async init({ searchBase, routeBase }) {
    searchEngine = await createSearch({ baseUrl: searchBase });
    const meta = searchEngine.manifest?.meta || {};

    // Directions are optional: with no reachable route index the app still
    // does search, exactly like the web demo degrades.
    if (routeBase) {
      try {
        const probe = await fetch(new URL("manifest.json", routeBase).toString());
        if (!probe.ok) throw new Error(`manifest ${probe.status}`);
        routeEngine = await openRouteGraphUrl(routeBase);
      } catch (err) {
        routeUnavailable = String(err?.message || err);
      }
    } else {
      routeUnavailable = "No route index configured";
    }

    return {
      attribution: meta.attribution || "© OpenStreetMap contributors",
      license: meta.license || "ODbL-1.0",
      total: searchEngine.manifest?.total ?? 0,
      routing: Boolean(routeEngine),
      routingError: routeUnavailable,
      profile: routeEngine?.root?.profile || ""
    };
  },

  async search({ q, anchor, size, shards }) {
    const response = await searchOsmQuery(searchEngine, {
      q,
      size: size || 20,
      // A picked suggestion carries its home shard, which turns a fuzzy text
      // search into a direct lookup of the place the user actually chose.
      ...(Array.isArray(shards) && shards.length ? { shards } : {}),
      ...near(anchor)
    });
    return { places: placesOf(response), total: response.total ?? 0 };
  },

  async suggest({ q, anchor, inputOffset }) {
    const response = await suggestOsmQuery(searchEngine, {
      q,
      size: 8,
      inputOffset: inputOffset ?? q.length,
      ...near(anchor)
    });
    return {
      suggestions: (response.suggestions || []).map(s => ({
        text: s.text || "",
        mainText: s.mainText || s.text || "",
        secondaryText: s.secondaryText || "",
        kind: s.kind || "",
        // The prediction's own resolution hints: its canonical query text and
        // the shard that owns it.
        selectionQuery: String(s.selection?.query || s.text || ""),
        selectionShards: s.selection?.shards || s.shards || []
      }))
    };
  },

  async reverse({ lat, lon }) {
    const response = await reverseGeocodeOsm(searchEngine, { lat, lon, size: 1 });
    const places = placesOf(response);
    return { place: places[0] || null };
  },

  async route({ from, to, alternatives, departureTime }) {
    if (!routeEngine) throw new Error(routeUnavailable || "Routing unavailable");
    const result = await routeEngine.route({
      from,
      to,
      alternatives: alternatives || 0,
      ...(departureTime ? { departureTime } : {})
    });
    return {
      primary: trimRoute(result),
      alternatives: (result.alternatives || []).map(trimRoute)
    };
  },

  async snap({ lat, lon }) {
    if (!routeEngine) throw new Error(routeUnavailable || "Routing unavailable");
    const result = await routeEngine.snap({ lat, lon });
    const best = (result.matches || [])[0];
    return best
      ? {
        lat: best.snappedLatE7 / 1e7,
        lon: best.snappedLonE7 / 1e7,
        distMeters: best.distMeters,
        segment: best.segment || ""
      }
      : null;
  }
};

window.__rfCall = async function (id, method, argsJson) {
  const handler = handlers[method];
  if (!handler) {
    post(id, false, `Unknown method ${method}`);
    return;
  }
  try {
    post(id, true, await handler(argsJson ? JSON.parse(argsJson) : {}));
  } catch (err) {
    post(id, false, err);
  }
};

// Tell the host the module graph loaded; init() is a separate call so the
// app controls when network work starts.
try {
  AndroidBridge.onReady();
} catch (err) {
  // Standalone browser preview — nothing to notify.
}
