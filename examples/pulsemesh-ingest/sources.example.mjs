// Example source config for scripts/pulsemesh_ingest.mjs.
//
// Copy this file, fill in the feeds your region actually has, and run:
//
//   node scripts/pulsemesh_ingest.mjs \
//     --epoch=<sourceHash> --graph=<route-graph dir> \
//     --config=./my-sources.mjs --bootstrap=<a keeper's multiaddr>
//
// A source either declares { url, map } — the script fetches JSON and
// calls map(body) — or brings its own async fetch({ nowMillis }) for
// anything else (XML, protobuf, an SDK, a camera-analytics endpoint).
// Both resolve to an array of observations:
//
//   { kind: "flow", lat, lon, speedKmh, bearingDeg?, bothDirections?,
//     observedAtMillis? }
//   { kind: "incident", lat, lon, type, observedAtMillis? }
//
// `type` is a PulseMesh §2.6 code or exact name: "crash",
// "hazard on road", "closure report", "standstill", "police",
// "road works", "stopped vehicle", "object on road",
// "slippery surface", "poor visibility", "animal on road",
// "signal outage", "hazard on shoulder".
//
// Honesty rules worth keeping in mind while mapping a feed:
// - Map a feed's event vocabulary onto the closest §2.6 type and drop
//   what does not fit, rather than shoehorning everything into "hazard".
// - Pass the feed's own timestamp as observedAtMillis when it has one;
//   the ingest node drops stale entries instead of republishing them
//   as current.
// - Only set bothDirections when the feed genuinely means both
//   carriageways (e.g. a full closure of an undivided road).

export default {
  sources: [
    // --- Declarative shape: a detector/flow feed serving JSON ----------
    //
    // Example: a city open-data endpoint returning
    //   [{ "lat": 45.51, "lon": -73.56, "speed_kmh": 22,
    //      "heading": 87, "measured_at": "2026-08-08T14:03:00Z" }, ...]
    //
    // {
    //   id: "city-detectors",
    //   intervalSeconds: 60,
    //   url: "https://example.city/api/traffic/detectors.json",
    //   map: body => body.map(row => ({
    //     kind: "flow",
    //     lat: row.lat,
    //     lon: row.lon,
    //     speedKmh: row.speed_kmh,
    //     bearingDeg: row.heading,
    //     observedAtMillis: Date.parse(row.measured_at)
    //   }))
    // },

    // --- Declarative shape: a 511-style event feed ---------------------
    //
    // Many North American 511 deployments share one API family:
    //   GET https://511on.ca/api/v2/get/event  (Ontario)
    //   GET https://www.quebec511.info/...     (Québec exposes similar data)
    // returning events with LanesAffected / EventType / Latitude /
    // Longitude. Map the vocabulary, don't force it.
    //
    // {
    //   id: "on511-events",
    //   intervalSeconds: 120,
    //   url: "https://511on.ca/api/v2/get/event?format=json&lang=en",
    //   map: events => events
    //     .map(event => {
    //       const type =
    //         event.EventType === "accidentsAndIncidents" ? "crash" :
    //         event.EventType === "roadwork" ? "road works" :
    //         event.EventType === "closures" ? "closure report" :
    //         null;
    //       if (!type) return null;
    //       return {
    //         kind: "incident",
    //         lat: event.Latitude,
    //         lon: event.Longitude,
    //         type,
    //         observedAtMillis: event.LastUpdated ? event.LastUpdated * 1000 : undefined
    //       };
    //     })
    //     .filter(Boolean)
    // },

    // --- Imperative shape: bring your own fetch ------------------------
    //
    // For camera analytics, XML feeds (DATEX II), SDKs, or anything that
    // is not one JSON GET. Whatever it returns goes through the same
    // map-matching, gating and pacing as everything else.
    //
    // {
    //   id: "camera-analytics",
    //   intervalSeconds: 30,
    //   fetch: async ({ nowMillis }) => {
    //     const speeds = await myCameraApi.currentSpeeds();
    //     return speeds.map(camera => ({
    //       kind: "flow",
    //       lat: camera.lat,
    //       lon: camera.lon,
    //       speedKmh: camera.medianSpeedKmh,
    //       bearingDeg: camera.approachBearing,
    //       observedAtMillis: camera.timestampMillis
    //     }));
    //   }
    // }
  ]
};
