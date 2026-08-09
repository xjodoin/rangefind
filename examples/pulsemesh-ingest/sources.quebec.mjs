// Québec sources for scripts/pulsemesh_ingest.mjs — the initial test
// region (graph: bench/route/quebec-index, which covers the province).
//
//   node scripts/pulsemesh_ingest.mjs \
//     --epoch=<quebec sourceHash> --graph=bench/route/quebec-index \
//     --config=examples/pulsemesh-ingest/sources.quebec.mjs \
//     --bootstrap=<a keeper's multiaddr>
//
// Everything here reads the Ministère des Transports et de la Mobilité
// durable's public WFS (the service behind Québec 511's map), GeoJSON
// reprojected to EPSG:4326 by the server:
//
//   https://ws.mapserver.transports.gouv.qc.ca/swtq
//     ms:evenements          live road events (accidents, floods, closures)
//     ms:chantiers_mtmdet    roadwork registry with active windows
//     ms:conditions_routieres winter road-state segments (empty in summer)
//     ms:infos_cameras       cameras — images only, no speeds; a CV
//                            pipeline could feed flow observations here
//
// Québec publishes no live speed feed, so by default this region
// bootstraps with incidents only — hint-tier pins by protocol
// construction (§8.5 scores
// min(raw, distinct deliverers); one ingest peer is one deliverer).
// That is also why the caps below are small and honest: a single peer's
// incident budget is INCIDENT_PEER_RATE−1 (5) per 10 minutes, and a
// hint stops scoring INCIDENT_WINDOW (600 s) after its newest record —
// so at any moment roughly five pins can be alive. Five *right* pins
// (a closed bridge, a crash on the 40) beat a rotation through 150
// construction sites, so live events outrank the works registry and
// the registry is filtered to closures and major hindrances.

const WFS = "https://ws.mapserver.transports.gouv.qc.ca/swtq";
export const QUEBEC511 = "https://www.quebec511.info";

// A camera's still image, derived from the open data alone. The first
// letter of `NumeroCamera` names the diffusion server that holds the
// picture; the rest is the file. Verified against one live camera per
// server (M/Q/T/G — 675 cameras in total).
const CAMERA_SERVER = Object.freeze({ M: "Montreal", Q: "Quebec", T: "TroisRivieres", G: "Gatineau" });

export function cameraStillUrl(numeroCamera) {
  const numero = String(numeroCamera || "");
  const server = CAMERA_SERVER[numero[0]];
  if (!server) return null;
  return `${QUEBEC511}/Images/Cameras/${server}/cam/${numero.slice(1)}.jpg`;
}

/** How a camera's picture is named among the ones a gallery loads. */
export function cameraImageFile(numeroCamera) {
  const numero = String(numeroCamera || "");
  return CAMERA_SERVER[numero[0]] ? `${numero.slice(1)}.jpg` : null;
}

/** The route gallery a camera appears on. 72 pages cover all 675. */
export function cameraGalleryUrl(numeroRoute) {
  const route = String(numeroRoute || "").replace(/^0+/, "");
  if (!/^\d+$/.test(route)) return null;
  return `${QUEBEC511}/en/Diffusion/EtatReseau/Camera.aspx?Type=2&Id=${route}`;
}

function wfsUrl(typename) {
  const params = new URLSearchParams({
    service: "wfs",
    version: "2.0.0",
    request: "GetFeature",
    typename,
    outputformat: "geojson",
    srsname: "EPSG:4326"
  });
  return `${WFS}?${params}`;
}

// --- Geometry helpers ------------------------------------------------------

/** A point ON the feature: the middle vertex of its (multi)linestring. */
function midpointOf(geometry) {
  let line = null;
  if (geometry?.type === "LineString") line = geometry.coordinates;
  else if (geometry?.type === "MultiLineString") line = geometry.coordinates[0];
  else if (geometry?.type === "Point") return { lon: geometry.coordinates[0], lat: geometry.coordinates[1] };
  if (!line?.length) return null;
  const [lon, lat] = line[Math.floor(line.length / 2)];
  return { lon, lat };
}

/** Forward bearing of the linestring around its middle vertex. */
function bearingOf(geometry) {
  const line = geometry?.type === "LineString" ? geometry.coordinates
    : geometry?.type === "MultiLineString" ? geometry.coordinates[0]
    : null;
  if (!line || line.length < 2) return null;
  const mid = Math.floor(line.length / 2);
  const [lon1, lat1] = line[Math.max(0, mid - 1)];
  const [lon2, lat2] = line[Math.min(line.length - 1, mid + 1)];
  const dLat = lat2 - lat1;
  const dLon = (lon2 - lon1) * Math.cos(((lat1 + lat2) / 2) * Math.PI / 180);
  if (!dLat && !dLon) return null;
  return (Math.atan2(dLon, dLat) * 180 / Math.PI + 360) % 360;
}

const COMPASS_AZIMUTH = { nord: 0, est: 90, sud: 180, ouest: 270 };

/**
 * The feed's `direction` field, resolved against the feature's own
 * geometry. "SUD et NORD" means both carriageways; a single compass
 * word picks whichever orientation of the road points that way, which
 * snap() then uses to choose the approach.
 */
function directionOf(direction, geometry) {
  const words = String(direction || "").toLowerCase().match(/nord|sud|est|ouest/g) || [];
  if (words.length !== 1) return { bothDirections: true };
  const road = bearingOf(geometry);
  if (road == null) return { bothDirections: true };
  const wanted = COMPASS_AZIMUTH[words[0]];
  const delta = Math.abs(road - wanted) % 360;
  const wrapped = delta > 180 ? 360 - delta : delta;
  return { bearingDeg: wrapped <= 90 ? road : (road + 180) % 360 };
}

// --- §2.6 type mapping -----------------------------------------------------

/** Événements: cause decides when it can; the obstruction text otherwise. */
function eventTypeOf({ cause, entrave }) {
  const causeText = String(cause || "");
  if (/accident/i.test(causeText)) return "crash";
  if (/travaux/i.test(causeText)) return "road works";
  const closed = /fermeture(?!\s+de\s+\d)|fermé/i.test(String(entrave || ""));
  if (/inondation/i.test(causeText)) return closed ? "closure report" : "hazard on road";
  if (/bris|érosion|affaissement|glissement/i.test(causeText)) {
    return closed ? "closure report" : "hazard on road";
  }
  if (closed) return "closure report";
  if (/fermeture de \d|alternance|contresens|voie/i.test(String(entrave || ""))) {
    return "hazard on road";
  }
  // Free-text advisories (ferry schedules and the like) map to nothing,
  // deliberately: shoehorning them into "hazard" would pin phantom
  // obstacles on the map.
  return null;
}

/** "2026/02/09 06:30:00" — local Québec wall time. Run the node in
 * America/Montreal; a works window is months long, so an hour of
 * timezone skew cannot flip its active state in a way that matters. */
function parseQuebecTime(text) {
  const parts = /^(\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2})/.exec(String(text || ""));
  if (!parts) return null;
  return new Date(+parts[1], +parts[2] - 1, +parts[3], +parts[4], +parts[5]).getTime();
}

// --- Cameras → vehicle counts → the road prices itself --------------------
//
// The one speed-shaped signal Québec has is its cameras, and the open
// data is enough to reach every one of them: `ms:infos_cameras` gives
// each camera's position, route and `NumeroCamera`, the number gives
// the image filename (`cameraImageFile`), and the route gives the
// gallery page it appears on (`cameraGalleryUrl`). All 675 cameras
// carry both, across 72 route galleries.
//
// The images are served only to a browser session and only under the
// token the site's own page mints for them, so this reads them the way
// the site serves them: `createGalleryImageFetcher` opens the operator's
// route gallery in a real browser and keeps the pictures that page
// loads by itself. One page view yields every camera on that route —
// the same request one person looking at that page would make — and
// nothing here forges a URL or disguises the browser.
//
// `maxCameras` and `intervalSeconds` are load on a public road-safety
// service. Keep them modest, and for anything ongoing ask the MTMD for
// a feed.
//
// The analyzer is the PIXEL one — no model, no API, no cost. It counts
// vehicles against a per-camera background (`npm install jpeg-js`), and
// the ingest module turns a count into a speed using the road graph's
// own lanes and free-flow.
//
// DIRECTION IS DECLARED, NOT GUESSED. A count says nothing about which
// way the vehicles are going, and at a snapped point on a divided road
// the two carriageways and their approaches are four segments within a
// metre of each other. A camera therefore appears here with either a
// per-carriageway calibration from CAMERA_CALIBRATION:
//
//   "3345": { directions: [
//     { name: "ne", roi: [0, 0.20, 1, 0.50], bearingDeg: 53,  lanes: 3 },
//     { name: "sw", roi: [0, 0.50, 1, 1.00], bearingDeg: 233, lanes: 3 }
//   ] }
//
// ...or, absent one, the coarse default below: `bothDirections`, which
// reads the whole frame as one roadway carrying both ways and splits
// the count between the two approaches. That default is deliberate and
// stated — it is a true statement about an uncalibrated camera, where
// picking one approach would not be — but a calibrated camera is
// strictly better, so calibrate the ones you care about. `roi` is
// [x0, y0, x1, y1] as fractions of the frame and is the
// highest-leverage setting there is.

import { createCameraTrafficSource } from "../../src/pulsemesh/ingest_camera.js";
import { createPixelCameraAnalyzer } from "../../src/pulsemesh/ingest_camera_pixels.js";
import { createGalleryImageFetcher } from "../../src/pulsemesh/ingest_camera_browser.js";

/** Per-camera direction calibration. Everything absent falls back to
 *  the coarse `bothDirections` default described above. */
export const CAMERA_CALIBRATION = Object.freeze({
  // Aut. 15 at pont Gédéon-Ouimet — the two carriageways, verified live.
  "3345": {
    visibleMeters: 250,
    directions: [
      { name: "ne", roi: [0, 0.20, 1, 0.50], bearingDeg: 53, lanes: 3 },
      { name: "sw", roi: [0, 0.50, 1, 1.00], bearingDeg: 233, lanes: 3 }
    ]
  }
});

/** The camera list, straight from the open data. */
export async function quebecCameras({ calibration = CAMERA_CALIBRATION } = {}) {
  const response = await fetch(wfsUrl("ms:infos_cameras"), { signal: AbortSignal.timeout(30000) });
  if (!response.ok) throw new Error(`cameras: HTTP ${response.status}`);
  const collection = await response.json();
  return (collection.features || [])
    .map(feature => {
      const properties = feature.properties || {};
      const id = String(properties.IDEcamera ?? "");
      return {
        id,
        numeroCamera: properties.NumeroCamera,
        numeroRoute: properties.NumeroRoute,
        lat: feature.geometry?.coordinates?.[1],
        lon: feature.geometry?.coordinates?.[0],
        description: properties.DescriptionLocalisationEn || properties.DescriptionLocalisationFr,
        imageUrl: cameraStillUrl(properties.NumeroCamera),
        galleryUrl: cameraGalleryUrl(properties.NumeroRoute),
        // Calibrated where we have it; an honest coarse reading otherwise.
        ...(calibration[id] ?? { bothDirections: true, visibleMeters: 200 })
      };
    })
    .filter(camera => Number.isFinite(camera.lat) && camera.imageUrl && camera.galleryUrl);
}

export function createQuebecCameraSource({
  maxCameras = 8,
  intervalSeconds = 300,
  calibration = CAMERA_CALIBRATION,
  headless = false,
  chromium = null,
  decode = undefined
} = {}) {
  const gallery = createGalleryImageFetcher({
    origin: QUEBEC511,
    chromium,
    headless,
    galleryUrlFor: camera => camera.galleryUrl,
    fileOf: camera => cameraImageFile(camera.numeroCamera),
    // A route gallery's pictures stay good for a poll; the site
    // refreshes them on its own cadence.
    refreshMillis: Math.max(30_000, intervalSeconds * 500)
  });
  const source = createCameraTrafficSource({
    id: "quebec511-cameras",
    intervalSeconds,
    maxCameras,
    cameras: () => quebecCameras({ calibration }),
    fetchImage: gallery.fetchImage,
    analyze: createPixelCameraAnalyzer(decode ? { decode } : {})
  });
  return { ...source, gallery, close: () => gallery.close() };
}

// Cameras are opt-in, because they need a real browser window and two
// optional packages (`playwright`, `jpeg-js`) that the incident feeds
// do not. Set RANGEFIND_QUEBEC_CAMERAS to the number of cameras to
// watch — start small:
//
//   RANGEFIND_QUEBEC_CAMERAS=8 node scripts/pulsemesh_ingest.mjs …
const cameraBudget = Number(process.env.RANGEFIND_QUEBEC_CAMERAS || 0);
const cameraSources = cameraBudget > 0
  ? [createQuebecCameraSource({ maxCameras: cameraBudget, intervalSeconds: 300 })]
  : [];

export default {
  // One operator's pins rotate through a 5-per-10-minutes budget, so a
  // published pin should not compete with unpublished ones for half an
  // hour. (INCIDENT_WINDOW still bounds visibility; this trades a pin
  // going dark sooner for broader coverage of distinct sites.)
  options: { incidentReEmitSeconds: 1800 },

  sources: [
    ...cameraSources,
    // --- Live events: the pins worth the budget -----------------------
    {
      id: "quebec511-evenements",
      intervalSeconds: 120,
      url: wfsUrl("ms:evenements"),
      map: collection => (collection.features || [])
        .map(feature => {
          const type = eventTypeOf(feature.properties || {});
          const point = midpointOf(feature.geometry);
          if (!type || !point) return null;
          return {
            kind: "incident",
            lat: point.lat,
            lon: point.lon,
            type,
            // A full closure stated by the road authority also carries
            // the near-zero speed state that makes the router avoid the
            // segment; "closure report" here only comes from a literal
            // "Fermeture"/"fermé" obstruction, never a lane closure.
            closedRoad: type === "closure report",
            ...directionOf(feature.properties?.direction, feature.geometry)
          };
        })
        .filter(Boolean)
    },

    // --- Works registry: closures and major hindrances only -----------
    {
      id: "quebec511-chantiers",
      intervalSeconds: 600,
      url: wfsUrl("ms:chantiers_mtmdet"),
      map: collection => {
        const now = Date.now();
        return (collection.features || [])
          .filter(feature => {
            const props = feature.properties || {};
            const from = parseQuebecTime(props.debut);
            const until = parseQuebecTime(props.fin);
            if (from == null || until == null || now < from || now > until) return false;
            return /fermeture complète|fermé/i.test(props.entrave || "")
              || /^majeure/i.test(props.entraveType || "");
          })
          .map(feature => {
            const props = feature.properties || {};
            const point = midpointOf(feature.geometry);
            if (!point) return null;
            const closed = /fermeture complète|fermé/i.test(props.entrave || "");
            return {
              kind: "incident",
              lat: point.lat,
              lon: point.lon,
              type: closed ? "closure report" : "road works",
              closed,
              closedRoad: closed,
              ...directionOf(props.direction, feature.geometry)
            };
          })
          .filter(Boolean)
          // Closures ahead of hindrances; the per-fetch cap keeps the
          // queue at what the budget can plausibly rotate through.
          .sort((a, b) => Number(b.closed) - Number(a.closed))
          .slice(0, 24)
          .map(({ closed, ...observation }) => observation)
      }
    },

    // --- Winter road state: empty in summer, the point in January -----
    {
      id: "quebec511-conditions",
      intervalSeconds: 300,
      url: wfsUrl("ms:conditions_routieres"),
      map: collection => (collection.features || [])
        .flatMap(feature => {
          const props = feature.properties || {};
          const point = midpointOf(feature.geometry);
          if (!point) return [];
          const out = [];
          // Matching the French description, not the code table: the
          // descriptions are stable prose ("Glacée", "Enneigée", …) and
          // a new code would otherwise map silently to nothing.
          const surface = String(props.DescriptionEtatChausseeFR || "");
          if (/glac|enneig|durci/i.test(surface)) {
            out.push({ kind: "incident", lat: point.lat, lon: point.lon, type: "slippery surface", severity: /glac/i.test(surface) ? 2 : 1 });
          }
          const visibility = String(props.DescriptionVisibiliteFR || "");
          if (/nulle|réduite/i.test(visibility)) {
            out.push({ kind: "incident", lat: point.lat, lon: point.lon, type: "poor visibility", severity: /nulle/i.test(visibility) ? 2 : 1 });
          }
          return out;
        })
        // Worst first, then cap: in a storm half the province matches,
        // and sixteen well-placed pins beat a shed queue.
        .sort((a, b) => b.severity - a.severity)
        .slice(0, 16)
        .map(({ severity, ...observation }) => observation)
    }
  ]
};
