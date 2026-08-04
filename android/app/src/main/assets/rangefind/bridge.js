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

const ROUTE_PROBE_TIMEOUT_MS = 6000;
const MANIFEST_CACHE_PREFIX = "rangefind.manifest:";

let searchEngine = null;
let searchUnavailable = null;
let routeEngine = null;
let routeUnavailable = null;

function readCachedManifest(baseUrl) {
  try {
    const raw = localStorage.getItem(MANIFEST_CACHE_PREFIX + baseUrl);
    return raw ? JSON.parse(raw) : null;
  } catch (err) {
    return null;
  }
}

function writeCachedManifest(baseUrl, manifest) {
  if (!manifest) return;
  try {
    localStorage.setItem(MANIFEST_CACHE_PREFIX + baseUrl, JSON.stringify(manifest));
  } catch (err) {
    // Quota, or storage turned off. The cache is an optimisation for the
    // offline case, never a requirement for the online one.
  }
}

/**
 * Opens the search index, falling back to the manifest cached from a previous
 * run when the network is gone.
 *
 * Search reads still need the network, so this does not make search work
 * offline. What it does is keep the failure local: a device holding a
 * downloaded region can navigate with no connectivity at all, and letting an
 * unreachable search host throw out of init would take that away over
 * something routing never needed.
 */
async function openSearch(baseUrl) {
  try {
    searchEngine = await createSearch({ baseUrl });
    writeCachedManifest(baseUrl, searchEngine.manifest);
    return;
  } catch (err) {
    searchUnavailable = String(err?.message || err);
  }
  const manifest = readCachedManifest(baseUrl);
  if (!manifest) return;
  try {
    searchEngine = await createSearch({ baseUrl, manifest });
  } catch (err) {
    // Keep the original failure: it describes why the network is unusable.
  }
}

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
      at: step.at ?? 0,
      // Posted limit in km/h; 0 when the way carries no maxspeed tag.
      speedLimitKmh: step.speedLimitKmh ?? 0
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

// The route graph covers whatever region it was built for, while search and
// the basemap are worldwide. Publishing its extent lets the app say "outside
// the routable area" instead of failing a snap 5 km from a road. A single
// rectangle around the whole graph is too generous — it spans neighbouring
// countries the extract never included. The leaf boxes tile the actual node
// distribution, so shipping them tells "3 km past the border" from "in a
// covered town".
function routingInfo() {
  let routeBounds = null;
  const leaves = routeEngine?.root?.leaves;
  if (leaves?.length) {
    let minLat = Infinity, maxLat = -Infinity, minLon = Infinity, maxLon = -Infinity;
    const cells = [];
    for (const leaf of leaves) {
      const box = leaf.bbox;
      if (!box) continue;
      if (box.minLat < minLat) minLat = box.minLat;
      if (box.maxLat > maxLat) maxLat = box.maxLat;
      if (box.minLon < minLon) minLon = box.minLon;
      if (box.maxLon > maxLon) maxLon = box.maxLon;
      cells.push(box.minLat / 1e7, box.maxLat / 1e7, box.minLon / 1e7, box.maxLon / 1e7);
    }
    if (Number.isFinite(minLat)) {
      routeBounds = {
        minLat: minLat / 1e7,
        maxLat: maxLat / 1e7,
        minLon: minLon / 1e7,
        maxLon: maxLon / 1e7,
        cells
      };
    }
  }
  return {
    routing: Boolean(routeEngine),
    routingError: routeUnavailable,
    profile: routeEngine?.root?.profile || "",
    routeBounds
  };
}

const handlers = {
  async init({ searchBase, routeBase }) {
    searchUnavailable = null;
    await openSearch(searchBase);
    const meta = searchEngine?.manifest?.meta || {};

    // Directions are optional: with no reachable route index the app still
    // does search, exactly like the web demo degrades.
    if (routeBase) {
      try {
        // Bounded for the same reason as useRouteBase, and more urgently: this
        // is the startup path, so an index that hangs holds the whole app on
        // its loading screen rather than degrading to "no routing".
        const probe = await fetch(new URL("manifest.json", routeBase).toString(), {
          signal: AbortSignal.timeout(ROUTE_PROBE_TIMEOUT_MS)
        });
        if (!probe.ok) throw new Error(`manifest ${probe.status}`);
        routeEngine = await openRouteGraphUrl(routeBase);
      } catch (err) {
        const timedOut = err?.name === "TimeoutError" || err?.name === "AbortError";
        routeUnavailable = timedOut
          ? `No answer from ${routeBase}`
          : String(err?.message || err);
      }
    } else {
      routeUnavailable = "No route index configured";
    }

    return {
      attribution: meta.attribution || "© OpenStreetMap contributors",
      license: meta.license || "ODbL-1.0",
      total: searchEngine?.manifest?.total ?? 0,
      searchUnavailable,
      ...routingInfo()
    };
  },

  /**
   * Point routing at a different index — a freshly preloaded region served
   * from local storage, or back at the network base. Only the route engine is
   * rebuilt; search is unrelated and stays warm.
   */
  async useRouteBase({ routeBase }) {
    routeEngine = null;
    routeUnavailable = null;
    if (!routeBase) {
      routeUnavailable = "No route index configured";
      return routingInfo();
    }
    try {
      // A base that has gone away answers by hanging, not by refusing, and
      // the UI cannot show anything until this returns. Bound the wait so an
      // unreachable index degrades to "no routing" instead of a dead screen.
      const probe = await fetch(new URL("manifest.json", routeBase).toString(), {
        signal: AbortSignal.timeout(ROUTE_PROBE_TIMEOUT_MS)
      });
      if (!probe.ok) throw new Error(`manifest ${probe.status}`);
      routeEngine = await openRouteGraphUrl(routeBase);
    } catch (err) {
      const timedOut = err?.name === "TimeoutError" || err?.name === "AbortError";
      routeUnavailable = timedOut
        ? `No answer from ${routeBase}`
        : String(err?.message || err);
    }
    return routingInfo();
  },

  async search({ q, anchor, size, shards }) {
    if (!searchEngine) throw new Error(searchUnavailable || "Search index unavailable");
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
    if (!searchEngine) throw new Error(searchUnavailable || "Search index unavailable");
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
    if (!searchEngine) throw new Error(searchUnavailable || "Search index unavailable");
    const response = await reverseGeocodeOsm(searchEngine, { lat, lon, size: 1 });
    const places = placesOf(response);
    return { place: places[0] || null };
  },

  async route({ from, to, alternatives, departureTime, fromHeading }) {
    if (!routeEngine) throw new Error(routeUnavailable || "Routing unavailable");
    const result = await routeEngine.route({
      from,
      to,
      alternatives: alternatives || 0,
      ...(departureTime ? { departureTime } : {}),
      // Only sent while actually moving: a heading from a standing vehicle is
      // noise, and biasing the snap with it would invent a U-turn penalty for
      // a driver who is free to pull away in either direction.
      ...(Number.isFinite(fromHeading) ? { fromHeading } : {})
    });
    return {
      primary: trimRoute(result),
      alternatives: (result.alternatives || []).map(trimRoute)
    };
  },

  /**
   * Every file that makes up a route index, for offline preloading. The root
   * enumerates its own shards and names file, so the download list is exact
   * rather than guessed from a directory listing HTTP does not provide.
   */
  async regionFiles({ baseUrl }) {
    const manifestUrl = new URL("manifest.json", baseUrl).toString();
    const response = await fetch(manifestUrl);
    if (!response.ok) throw new Error(`manifest ${response.status}`);
    const manifest = await response.json();

    const probe = await openRouteGraphUrl(baseUrl);
    const root = probe.root;
    const files = ["manifest.json", manifest.root];
    if (root.namesFile) files.push(root.namesFile);
    for (const shard of root.shards || []) {
      for (const pack of shard.packs || []) {
        files.push(shard.dir ? `${shard.dir}/${pack}` : pack);
      }
    }
    return {
      files,
      profile: root.profile || "",
      nodes: manifest.nodes ?? 0,
      leaves: manifest.leaves ?? 0
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
