import { createSearch } from "./runtime.browser.js";
import {
  decodePolyline,
  hydrateOsmSuggestions,
  matchPointToRoute,
  prepareRoute,
  resolveOsmSuggestion,
  reverseGeocodeOsm,
  searchOsmQuery,
  suggestOsmQuery
} from "./osm.browser.js";
import {
  carriedVoice,
  livePathSeconds,
  openRouteGraphUrl,
  remainingPath,
  repriceDecision,
  resolveIndexBase,
  segmentsOf,
  shouldRepriceNow
} from "./route.browser.js";
import { createPulseMeshDemo } from "./pulsemesh-demo.js";
import { encodeQr, qrSvg } from "./qr.browser.js";
import { geometryFeatureBounds, resultGeometryFeature } from "./result_geometry.js";

const queryInput = document.querySelector("#queryInput");
const searchButton = document.querySelector("#searchButton");
const suggestList = document.querySelector("#suggestList");
const resultList = document.querySelector("#resultList");
const statusLine = document.querySelector("#statusLine");
const indexMeta = document.querySelector("#indexMeta");
const areaToggle = document.querySelector("#areaToggle");
const clearButton = document.querySelector("#clearButton");
const emptyState = document.querySelector("#emptyState");
const emptyTitle = document.querySelector("#emptyTitle");
const emptyCopy = document.querySelector("#emptyCopy");
const placeMetric = document.querySelector("#placeMetric");
const regionMetric = document.querySelector("#regionMetric");
const coverageMetric = document.querySelector("#coverageMetric");
const livePill = document.querySelector("#livePill");
const liveState = document.querySelector("#liveState");
const mapHudText = document.querySelector("#mapHudText");
const searchPanel = document.querySelector("#searchPanel");
const panelToggle = document.querySelector("#panelToggle");
const collapsedSelection = document.querySelector("#collapsedSelection");
const queryReceipt = document.querySelector("#queryReceipt");
const queryReceiptSummary = document.querySelector("#queryReceiptSummary");
const queryReceiptRoute = document.querySelector("#queryReceiptRoute");
const queryReceiptBars = document.querySelector("#queryReceiptBars");
const queryTraceCopy = document.querySelector("#queryTraceCopy");
const queryTraceDownload = document.querySelector("#queryTraceDownload");
const queryTraceFeedback = document.querySelector("#queryTraceFeedback");
const placeLens = document.querySelector("#placeLens");
const placeLensEyebrow = document.querySelector("#placeLensEyebrow");
const placeLensTitle = document.querySelector("#placeLensTitle");
const placeLensCopy = document.querySelector("#placeLensCopy");
const placeLensBadges = document.querySelector("#placeLensBadges");
const placeLensFacts = document.querySelector("#placeLensFacts");
const placeLensLinks = document.querySelector("#placeLensLinks");
const placeLensClose = document.querySelector("#placeLensClose");
const mapPickButton = document.querySelector("#mapPickButton");

// The client ships with Rangefind; ../osm-rangefind-index independently
// publishes the rolling regional shards. Every root-manifest update is picked
// up by the demo without copying index artifacts into the Pages deployment.
const OSM_INDEX_BASE_URL = "https://osm.rangefind.dev/";
const SUGGEST_MIN_CHARACTERS = 3;
const SUGGEST_DEBOUNCE_MS = 180;
const SUGGEST_CACHE_LIMIT = 32;

let engine;
let markers = [];
let moveTimer = null;
let suggestTimer = null;
let suggestToken = 0;
let searchToken = 0;
let activeSuggestion = -1;
let suggestionsSuppressed = false;
let suppressedQuery = "";
let selectedSuggestionHint = null;
let visibleSuggestions = [];
let suggestInFlight = false;
let suggestQueued = false;
let selectedPlace = null;
let activeQueryOverride = null;
// True while the last search was anchored to the map view (a plain category
// or text query, not a resolved locality/address). Such results change with
// the viewport, so panning the map re-runs them.
let anchoredQueryActive = false;
let mapPickActive = false;
const suggestionCache = new Map();

const RESOLVED_LOCATION_LANES = new Set([
  "osmCategoryLocality",
  "osmGlobalExactText",
  "osmIntersectionDocument",
  "osmIntersectionLocality",
  "osmLocalityExact",
  "osmNamedCategoryLocality",
  "osmNamedTextLocality",
  "osmReverseGeocode",
  "osmReverseGeocodeFallback",
  "osmReverseGeocodeLocality",
  "osmStreetLocality",
  "osmCoordinates",
  "osmSuggestEntity"
]);

const LOCATION_TYPE_LABELS = new Map([
  ["ROOFTOP", "Exact address"],
  ["RANGE_INTERPOLATED", "Interpolated address"],
  ["GEOMETRIC_CENTER", "Address area"],
  ["APPROXIMATE", "Nearby locality"]
]);

const PLANNER_LABELS = new Map([
  ["osmCategoryLocality", "category + locality"],
  ["osmGlobalExactText", "exact place"],
  ["osmIntersectionDocument", "exact intersection"],
  ["osmIntersectionLocality", "intersection + locality"],
  ["osmLocalityExact", "exact locality"],
  ["osmNamedCategoryLocality", "named place + locality"],
  ["osmNamedTextLocality", "place + locality"],
  ["osmReverseGeocode", "reverse geocode"],
  ["osmReverseGeocodeFallback", "coordinate"],
  ["osmReverseGeocodeLocality", "nearby locality"],
  ["osmStreetLocality", "street authority"],
  ["osmCoordinates", "coordinate"],
  ["osmSuggestEntity", "direct place"]
]);

const TRACE_LABELS = new Map([
  ["manifest", "Route manifests"],
  ["textRouting", "Region routing"],
  ["directory", "Term directory"],
  ["terms", "Posting lists"],
  ["postingBlocks", "Posting blocks"],
  ["docs", "Result documents"],
  ["docPages", "Result pages"],
  ["docPointers", "Document pointers"],
  ["geo", "Geo index"],
  ["filterBitmaps", "Facet filters"],
  ["authority", "Address authority"]
]);

const CANADIAN_POSTAL_CODE = /\b[abceghj-nprstvxy]\d[abceghj-nprstvwxyz]\s*[0-9][abceghj-nprstvwxyz][0-9]\b/iu;

const map = new maplibregl.Map({
  container: "map",
  style: {
    version: 8,
    // Free glyphs so vector text (route street names) can render over the
    // raster basemap; raster labels rotate with the nav camera, these stay
    // upright.
    glyphs: "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf",
    sources: {
      osm: {
        type: "raster",
        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
        tileSize: 256,
        attribution: "© OpenStreetMap contributors"
      },
      resultGeometries: {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] }
      }
    },
    layers: [
      {
        id: "osm",
        type: "raster",
        source: "osm",
        paint: {
          "raster-saturation": -0.62,
          "raster-contrast": 0.12,
          "raster-brightness-min": 0.22,
          "raster-brightness-max": 0.9
        }
      },
      {
        id: "result-postal-areas-fill",
        type: "fill",
        source: "resultGeometries",
        filter: ["==", ["get", "kind"], "postal-area"],
        paint: { "fill-color": "#4285f4", "fill-opacity": 0.16 }
      },
      {
        id: "result-geometries-fill",
        type: "fill",
        source: "resultGeometries",
        filter: ["all", ["==", ["geometry-type"], "Polygon"], ["!=", ["get", "kind"], "postal-area"]],
        paint: { "fill-color": "#16a085", "fill-opacity": 0.2 }
      },
      {
        id: "result-geometries-line",
        type: "line",
        source: "resultGeometries",
        filter: ["!=", ["get", "kind"], "postal-area"],
        paint: { "line-color": "#0b7a69", "line-width": 3, "line-opacity": 0.85 }
      },
      {
        id: "result-postal-areas-line",
        type: "line",
        source: "resultGeometries",
        filter: ["==", ["get", "kind"], "postal-area"],
        paint: {
          "line-color": "#1a73e8",
          "line-width": 3,
          "line-opacity": 0.95,
          "line-dasharray": [2, 1]
        }
      }
    ]
  },
  // Birmingham is already covered by the rolling public index and gives the
  // first visit a useful, searchable map instead of the midpoint of a global
  // bounding box.
  center: [-86.8025, 33.5207],
  zoom: 11
});

map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), "top-right");
const geolocateControl = new maplibregl.GeolocateControl({
  positionOptions: { enableHighAccuracy: true },
  trackUserLocation: false,
  showUserHeading: true
});
map.addControl(geolocateControl, "top-right");

// The user's location anchors searches: bare categories ("pharmacy") become
// nearest-first, and plain text tries the shard under the user before fanning
// out. Never prompt on load — adopt the position silently when permission was
// already granted, otherwise the anchor is the map center (panning the map is
// itself an expression of where the user cares about).
let userLocation = null;
geolocateControl.on("geolocate", event => {
  const { latitude, longitude } = event.coords || {};
  if (Number.isFinite(latitude) && Number.isFinite(longitude)) {
    userLocation = { lat: latitude, lon: longitude };
    mapHudText.textContent = "Searches now prioritize places near you";
  }
});
if (navigator.permissions?.query) {
  navigator.permissions.query({ name: "geolocation" })
    .then(status => {
      if (status.state !== "granted") return;
      if (map.loaded()) geolocateControl.trigger();
      else map.once("load", () => geolocateControl.trigger());
    })
    .catch(() => {});
}

// The anchor is always where the map is looking: geolocation centers the
// map on the user at startup, and panning to another city moves the anchor
// with the view — "pharmacy" always means pharmacies around the visible
// area. The label says "near you" only while the view actually sits on the
// user's position.
function searchAnchor() {
  const center = map.getCenter();
  if (!Number.isFinite(center?.lat) || !Number.isFinite(center?.lng)) {
    return userLocation ? { ...userLocation, source: "you" } : null;
  }
  const onUser = userLocation
    && Math.abs(center.lat - userLocation.lat) < 0.03
    && Math.abs(center.lng - userLocation.lon) < 0.04;
  return { lat: center.lat, lon: center.lng, source: onUser ? "you" : "map view" };
}

function formatNumber(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function formatCompact(value) {
  return new Intl.NumberFormat(undefined, {
    notation: "compact",
    maximumFractionDigits: 1
  }).format(Number(value || 0));
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (bytes < 1024) return `${formatNumber(bytes)} B`;
  if (bytes < 1024 ** 2) return `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(bytes / 1024)} KB`;
  return `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(bytes / 1024 ** 2)} MB`;
}

function formatDistance(meters) {
  const value = Number(meters);
  if (!Number.isFinite(value)) return "";
  if (value >= 1000) return `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(value / 1000)} km`;
  return `${formatNumber(Math.round(value))} m`;
}

function formatDate(value) {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return "";
  return new Intl.DateTimeFormat(undefined, { day: "numeric", month: "short", year: "numeric" }).format(date);
}

function humanize(value) {
  return String(value || "")
    .replaceAll(/[-_]+/gu, " ")
    .replaceAll(/\b\p{L}/gu, letter => letter.toLocaleUpperCase());
}

function detailText(value) {
  return String(value || "")
    .split(";")
    .map(part => humanize(part.trim()))
    .filter(Boolean)
    .join(" · ");
}

function semanticLabel(item) {
  return LOCATION_TYPE_LABELS.get(String(item?.locationType || "").toUpperCase())
    || humanize(item?.type || item?.category || item?.types?.[0] || "place");
}

function safeWebUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  try {
    const url = new URL(/^https?:\/\//iu.test(raw) ? raw : `https://${raw}`);
    return url.protocol === "http:" || url.protocol === "https:" ? url.href : "";
  } catch {
    return "";
  }
}

function wikipediaUrl(value) {
  const raw = String(value || "").trim();
  const separator = raw.indexOf(":");
  if (separator < 1) return "";
  const language = raw.slice(0, separator).toLowerCase();
  const title = raw.slice(separator + 1).trim();
  if (!/^[a-z][a-z0-9-]{0,11}$/u.test(language) || !title) return "";
  return `https://${language}.wikipedia.org/wiki/${encodeURIComponent(title.replaceAll(" ", "_"))}`;
}

function wikidataUrl(value) {
  const id = String(value || "").trim().toUpperCase();
  return /^Q\d+$/u.test(id) ? `https://www.wikidata.org/wiki/${id}` : "";
}

function appendBadge(container, label, tone = "") {
  if (!label) return;
  const badge = document.createElement("span");
  badge.className = `place-badge${tone ? ` place-badge--${tone}` : ""}`;
  badge.textContent = label;
  container.append(badge);
}

function appendFact(label, value) {
  if (!value) return;
  const term = document.createElement("dt");
  term.textContent = label;
  const description = document.createElement("dd");
  description.textContent = value;
  placeLensFacts.append(term, description);
}

function appendPlaceLink(label, href, className = "") {
  if (!href) return;
  const link = document.createElement("a");
  link.href = href;
  link.className = className;
  link.textContent = label;
  if (/^https?:/u.test(href)) {
    link.target = "_blank";
    link.rel = "noreferrer";
  }
  placeLensLinks.append(link);
}

function yesNoDetail(value, yesLabel, noLabel = "") {
  const normalized = String(value || "").toLowerCase();
  if (["yes", "designated", "permissive", "customers"].includes(normalized)) return yesLabel;
  if (normalized === "no") return noLabel;
  return value ? detailText(value) : "";
}

function resultLocation(item) {
  const name = item.name || item.title || item.id;
  if (item.formattedAddress && item.formattedAddress !== name) return item.formattedAddress;
  if (item.address && item.address !== name) return item.address;
  const parts = [item.suburb, item.city, item.district, item.state, item.postcode, item.country]
    .map(value => String(value || "").trim())
    .filter((value, index, values) => value && values.indexOf(value) === index);
  return parts.join(", ") || humanize(item.shard);
}

function resultLimit() {
  return window.matchMedia("(max-width: 720px)").matches ? 12 : 18;
}

function resultFitPadding() {
  const compact = window.matchMedia("(max-width: 720px)").matches;
  const panelBox = searchPanel.getBoundingClientRect();
  if (compact) {
    return {
      top: 56,
      right: 32,
      bottom: Math.min(panelBox.height + 24, window.innerHeight - 150),
      left: 32
    };
  }
  return { top: 70, right: 70, bottom: 70, left: panelBox.width + 44 };
}

function viewportBox() {
  const bounds = map.getBounds();
  const box = {
    minLat: bounds.getSouth(),
    maxLat: bounds.getNorth(),
    minLon: bounds.getWest(),
    maxLon: bounds.getEast()
  };
  return Object.values(box).every(Number.isFinite) ? box : null;
}

function looksLikeAddress(value) {
  const text = String(value || "");
  if (CANADIAN_POSTAL_CODE.test(text)) return true;
  const tokens = text.trim().split(/[^\p{L}\p{N}-]+/u).filter(Boolean);
  return tokens.length >= 2 && tokens.some(token => /^\d+[\p{L}]?(?:-\d+[\p{L}]?)?$/u.test(token));
}

function setStatus(text, state = "ready") {
  statusLine.textContent = text;
  statusLine.dataset.state = state;
  searchButton.dataset.state = state;
}

// The exportable trace of the query currently shown in the X-Ray. Captured on
// render so the export buttons never have to re-run anything.
let lastQueryTrace = null;
let queryTraceStartedAt = 0;

function beginQueryReceipt() {
  queryReceipt.hidden = false;
  queryReceipt.open = false;
  queryReceipt.dataset.state = "loading";
  queryReceiptSummary.textContent = "Tracing byte ranges…";
  queryReceiptRoute.textContent = "Following the request from this browser into the static index.";
  queryReceiptBars.replaceChildren();
  // Resource timings are sliced from here, so the export's waterfall covers
  // this query rather than the whole session.
  queryTraceStartedAt = performance.now();
  setQueryTrace(null);
}

function setQueryTrace(payload) {
  lastQueryTrace = payload;
  const disabled = !payload;
  queryTraceCopy.disabled = disabled;
  queryTraceDownload.disabled = disabled;
  queryTraceFeedback.textContent = "";
  queryTraceFeedback.removeAttribute("data-state");
}

// Per-request waterfall for the export. Cross-origin entries only expose
// sizes and status when the host sends Timing-Allow-Origin, so the fields are
// reported as-is and a note explains zeros rather than inventing numbers.
function queryTraceNetwork(indexBase) {
  if (typeof performance?.getEntriesByType !== "function") return null;
  const limit = 600;
  const entries = performance.getEntriesByType("resource")
    .filter(entry => entry.startTime >= queryTraceStartedAt && String(entry.name).startsWith(indexBase))
    .map(entry => ({
      path: String(entry.name).slice(indexBase.length),
      startMs: Math.round(entry.startTime - queryTraceStartedAt),
      durationMs: Math.round(entry.duration),
      transferBytes: entry.transferSize ?? null,
      encodedBytes: entry.encodedBodySize ?? null,
      status: entry.responseStatus ?? null,
      protocol: entry.nextHopProtocol || null
    }));
  const timingVisible = entries.some(entry => entry.transferBytes > 0 || entry.status != null);
  return {
    requests: entries.length,
    truncated: entries.length > limit,
    ...(timingVisible ? {} : {
      note: "Sizes/status are 0 or null because the index host does not send Timing-Allow-Origin to this page; stats.trace carries the runtime's own byte accounting."
    }),
    entries: entries.slice(0, limit)
  };
}

// A self-contained diagnostic bundle: everything needed to reproduce and
// explain the query without the reporter's screen.
function buildQueryTracePayload(response, shown, context = {}) {
  const anchor = context.anchor || null;
  const center = map.getCenter();
  return {
    tool: "rangefind-osm-demo",
    exportedAt: new Date().toISOString(),
    query: {
      text: context.query ?? queryInput.value.trim(),
      kind: context.kind || "search",
      near: anchor ? { lat: anchor.lat, lon: anchor.lon, source: anchor.source } : null,
      limitToMapArea: Boolean(context.areaBox),
      geoBox: context.areaBox || null,
      scopedShards: context.shards || null,
      params: context.params || null
    },
    mapView: {
      center: { lat: Number(center.lat.toFixed(5)), lon: Number(center.lng.toFixed(5)) },
      zoom: Number(map.getZoom().toFixed(2))
    },
    index: {
      baseUrl: OSM_INDEX_BASE_URL,
      total: engine?.manifest?.total ?? null,
      shards: engine?.shards?.length ?? engine?.manifest?.shards?.length ?? null,
      builtAt: engine?.manifest?.built_at ?? null,
      engineVersion: engine?.manifest?.version ?? null
    },
    response: {
      total: response.total ?? null,
      shown,
      approximate: response.approximate ?? null,
      resolvedQuery: response.resolvedQuery ?? null,
      correctedQuery: response.correctedQuery ?? null,
      elapsedMs: context.elapsedMs ?? null,
      firstResult: response.results?.[0]
        ? { name: response.results[0].name || response.results[0].title || null, type: response.results[0].type || null }
        : null
    },
    // The runtime's own accounting: planner/geo lanes, shard routing, and the
    // per-bucket fetch spans the X-Ray bars are drawn from.
    stats: response.stats || null,
    network: queryTraceNetwork(OSM_INDEX_BASE_URL),
    client: {
      userAgent: navigator.userAgent,
      hardwareConcurrency: navigator.hardwareConcurrency ?? null,
      viewport: { width: window.innerWidth, height: window.innerHeight, dpr: window.devicePixelRatio || 1 }
    }
  };
}

function queryTraceFilename(payload) {
  const slug = String(payload.query.text || "viewport")
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "-")
    .replace(/^-|-$/gu, "")
    .slice(0, 40) || "query";
  return `rangefind-trace-${slug}-${payload.exportedAt.replace(/[:.]/gu, "-")}.json`;
}

function flashQueryTraceFeedback(message, state = "") {
  queryTraceFeedback.textContent = message;
  if (state) queryTraceFeedback.dataset.state = state;
  else queryTraceFeedback.removeAttribute("data-state");
  clearTimeout(flashQueryTraceFeedback.timer);
  flashQueryTraceFeedback.timer = setTimeout(() => {
    queryTraceFeedback.textContent = "";
    queryTraceFeedback.removeAttribute("data-state");
  }, 2600);
}

queryTraceCopy.addEventListener("click", async () => {
  if (!lastQueryTrace) return;
  const json = JSON.stringify(lastQueryTrace, null, 2);
  try {
    await navigator.clipboard.writeText(json);
    flashQueryTraceFeedback("Copied");
  } catch {
    // Clipboard access needs a secure context and permission; fall back to a
    // selection-based copy before telling the user it failed.
    const area = document.createElement("textarea");
    area.value = json;
    area.setAttribute("readonly", "");
    area.style.cssText = "position:fixed;top:0;left:0;opacity:0;";
    document.body.append(area);
    area.select();
    const copied = document.execCommand?.("copy");
    area.remove();
    if (copied) flashQueryTraceFeedback("Copied");
    else flashQueryTraceFeedback("Copy blocked — use Download", "error");
  }
});

queryTraceDownload.addEventListener("click", () => {
  if (!lastQueryTrace) return;
  const blob = new Blob([JSON.stringify(lastQueryTrace, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = queryTraceFilename(lastQueryTrace);
  document.body.append(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 0);
  flashQueryTraceFeedback("Downloaded");
});

function renderQueryReceipt(response, shown, context = {}) {
  const trace = response.stats?.trace;
  if (!trace) {
    queryReceipt.hidden = true;
    setQueryTrace(null);
    return;
  }
  setQueryTrace(buildQueryTracePayload(response, shown, context));
  const fetchSpans = (trace?.spans || []).filter(span => span.name.endsWith(".fetch") && span.count > 0);
  const queried = Number(response.stats?.shardsQueried || 1);
  const available = Number(response.stats?.shards || engine.shards?.length || 1);
  queryReceipt.hidden = false;
  queryReceipt.dataset.state = "ready";
  if (!fetchSpans.length) {
    queryReceiptSummary.textContent = "0 network reads · memory hit";
    queryReceiptRoute.textContent = `This browser → memory cache → ${formatNumber(shown)} hydrated ${shown === 1 ? "result" : "results"}`;
    const warm = document.createElement("p");
    warm.className = "query-receipt__warm";
    warm.textContent = "Everything required for this query was already resident in this tab.";
    queryReceiptBars.replaceChildren(warm);
    return;
  }
  const reads = fetchSpans.reduce((sum, span) => sum + Number(span.count || 0), 0);
  const bytes = Number(trace.totalBytes || fetchSpans.reduce((sum, span) => sum + Number(span.bytes || 0), 0));
  queryReceiptSummary.textContent = `${formatNumber(reads)} static ${reads === 1 ? "read" : "reads"} · ${formatBytes(bytes)}`;
  queryReceiptRoute.textContent = `This browser → ${formatNumber(queried)} of ${formatNumber(available)} regions → ${formatNumber(shown)} hydrated ${shown === 1 ? "result" : "results"}`;

  const ranked = fetchSpans
    .map(span => ({ ...span, cost: Number(span.bytes || 0) || Number(span.count || 0) }))
    .sort((left, right) => right.cost - left.cost)
    .slice(0, 5);
  const maxCost = Math.max(1, ...ranked.map(span => span.cost));
  queryReceiptBars.replaceChildren(...ranked.map(span => {
    const bucket = span.name.slice(0, -".fetch".length);
    const row = document.createElement("div");
    row.className = "query-receipt__bar";
    const label = document.createElement("span");
    label.textContent = TRACE_LABELS.get(bucket) || humanize(bucket);
    const rail = document.createElement("i");
    const fill = document.createElement("b");
    fill.style.width = `${Math.max(5, Math.round((span.cost / maxCost) * 100))}%`;
    rail.append(fill);
    const value = document.createElement("strong");
    value.textContent = span.bytes
      ? formatBytes(span.bytes)
      : `${formatNumber(span.count)} ${span.count === 1 ? "read" : "reads"}`;
    row.append(label, rail, value);
    return row;
  }));
}

function interruptQueryReceipt() {
  if (queryReceipt.hidden) return;
  queryReceipt.dataset.state = "error";
  queryReceiptSummary.textContent = "Trace interrupted";
  setQueryTrace(null);
}

function showEmpty(title, copy) {
  emptyTitle.textContent = title;
  emptyCopy.textContent = copy;
  emptyState.hidden = false;
  resultList.hidden = true;
}

function setPanelCollapsed(collapsed, selection = "") {
  searchPanel.classList.toggle("is-collapsed", collapsed);
  if (typeof syncSheetMode === "function") syncSheetMode();
  panelToggle.setAttribute("aria-expanded", String(!collapsed));
  panelToggle.setAttribute("aria-label", collapsed ? "Expand search panel" : "Collapse search panel");
  panelToggle.title = collapsed ? "Expand search panel" : "Collapse search panel";
  if (selection) collapsedSelection.textContent = `${selection} selected`;
  if (collapsed) {
    hideSuggestions();
    queryInput.blur();
  }
  requestAnimationFrame(() => map.resize());
}

function hidePlaceLens() {
  placeLens.hidden = true;
  selectedPlace = null;
}

// Dim (never clear) the previous answer while a new one is in flight.
function setResultsLoading(loading) {
  resultList.classList.toggle("is-loading", Boolean(loading));
  placeLens.classList.toggle("is-loading", Boolean(loading));
}

function setMapPickActive(active) {
  mapPickActive = Boolean(active);
  mapPickButton.setAttribute("aria-pressed", String(mapPickActive));
  mapPickButton.classList.toggle("active", mapPickActive);
  map.getCanvas().classList.toggle("is-picking", mapPickActive);
  if (mapPickActive) {
    hideSuggestions();
    setPanelCollapsed(false);
    setStatus("Choose any point on the map…", "ready");
    mapHudText.textContent = "Tap the map to reverse geocode";
  }
}

function openPlaceLens(item) {
  if (!Number.isFinite(item?.lat) || !Number.isFinite(item?.lon)) return;
  selectedPlace = item;
  const title = item.name || item.title || item.id;
  const location = resultLocation(item);
  const kind = semanticLabel(item);
  const details = item.details && typeof item.details === "object" ? item.details : {};
  const postalArea = String(item.type || "").toLowerCase() === "postal_code";
  const services = [
    yesNoDetail(details.delivery, "Delivery"),
    yesNoDetail(details.takeaway, "Takeaway"),
    yesNoDetail(details.drive_through, "Drive-through"),
    yesNoDetail(details.reservation, "Reservations")
  ].filter(Boolean).join(" · ");
  const payments = [
    yesNoDetail(details.payment_cash, "Cash"),
    yesNoDetail(details.payment_cards, "Cards"),
    yesNoDetail(details.payment_contactless, "Contactless")
  ].filter(Boolean).join(" · ");
  placeLensTitle.textContent = title;
  placeLensEyebrow.textContent = postalArea
    ? "Postal code area"
    : item.locationType ? "Reverse geocode" : "OpenStreetMap place";
  placeLensCopy.textContent = location || `${item.lat.toFixed(5)}, ${item.lon.toFixed(5)}`;
  placeLensBadges.replaceChildren();
  appendBadge(placeLensBadges, kind, item.locationType ? "accuracy" : "");
  if (postalArea) appendBadge(placeLensBadges, "Approximate coverage", "accuracy");
  appendBadge(placeLensBadges, formatDistance(item.distanceMeters));
  appendBadge(placeLensBadges, details.brand);
  appendBadge(placeLensBadges, details.cuisine ? detailText(details.cuisine) : "");
  appendBadge(placeLensBadges, yesNoDetail(details.wheelchair, "Wheelchair accessible"), "accessible");

  placeLensFacts.replaceChildren();
  if (postalArea) {
    appendFact("Postal code", item.postcode);
    appendFact("Indexed addresses", Number(item.address_count) > 0 ? formatNumber(item.address_count) : "");
    appendFact("Coverage samples", Number(item.sample_count) > 0 ? formatNumber(item.sample_count) : "");
    appendFact("Area", "Approximate extent of indexed source points");
  }
  appendFact("Hours", details.opening_hours);
  appendFact("Kitchen", details.kitchen_hours);
  appendFact("Brand", details.brand);
  appendFact("Operator", details.operator);
  appendFact("Cuisine", detailText(details.cuisine));
  appendFact("Wheelchair", yesNoDetail(details.wheelchair, "Accessible", "Not accessible"));
  appendFact("Accessible toilets", yesNoDetail(details.toilets_wheelchair, "Available", "Not available"));
  appendFact("Internet", yesNoDetail(details.internet_access, "Available", "Not available"));
  appendFact("Outdoor seating", yesNoDetail(details.outdoor_seating, "Available", "Not available"));
  appendFact("Services", services);
  appendFact("Payments", payments);
  appendFact("Capacity", details.capacity);
  appendFact("Stars", details.stars);
  appendFact("Access", detailText(details.access));
  appendFact("Fee", yesNoDetail(details.fee, "Required", "Free"));
  appendFact("Smoking", detailText(details.smoking));
  appendFact("Entrance", detailText(details.entrance));
  appendFact("Level", details.level);

  placeLensLinks.replaceChildren();
  appendPlaceLink("Website", safeWebUrl(details.website), "place-link--primary");
  const phone = String(details.phone || "").trim();
  if (phone && /^[+\d*#(),;.\s-]+$/u.test(phone)) appendPlaceLink("Call", `tel:${phone}`);
  const email = String(details.email || "").trim();
  if (email && /^[^\s@]+@[^\s@]+$/u.test(email)) appendPlaceLink("Email", `mailto:${email}`);
  appendPlaceLink("Wikipedia", wikipediaUrl(details.wikipedia));
  appendPlaceLink("Wikidata", wikidataUrl(details.wikidata));
  appendPlaceLink("View on OSM", safeWebUrl(item.url));
  placeLens.hidden = false;
  placeLens.focus({ preventScroll: true });
}

function runDiscoveryOrbit(query, label) {
  if (!selectedPlace) return;
  const place = selectedPlace;
  const title = place.name || place.title || place.id;
  const displayQuery = `${label} around ${title}`;
  const rootShard = String(place.shard || "").split("/")[0];
  activeQueryOverride = {
    displayQuery,
    mode: "discovery orbit",
    params: {
      q: query,
      geo: {
        near: { lat: place.lat, lon: place.lon, radiusMeters: 2500 },
        sort: "distance"
      },
      ...(rootShard ? { shards: [rootShard] } : {})
    }
  };
  queryInput.value = displayQuery;
  selectedSuggestionHint = null;
  clearButton.hidden = false;
  areaToggle.checked = false;
  hidePlaceLens();
  setPanelCollapsed(false);
  runSearch({ fit: true });
}

function clearMarkers() {
  for (const { marker } of markers) marker.remove();
  markers = [];
  map.getSource("resultGeometries")?.setData({ type: "FeatureCollection", features: [] });
}

function markerFor(item, index) {
  const title = item.name || item.title || item.id;
  const location = resultLocation(item);
  const accessibleLabel = location ? `${title}, ${location}` : title;
  const element = document.createElement("button");
  element.className = "result-marker";
  element.classList.toggle("is-postal-area", String(item.type || "").toLowerCase() === "postal_code");
  element.type = "button";
  element.setAttribute("aria-label", accessibleLabel);
  const label = document.createElement("span");
  label.textContent = index + 1;
  element.append(label);
  const popup = document.createElement("div");
  popup.className = "map-popup";
  const popupTitle = document.createElement("strong");
  popupTitle.textContent = title;
  popup.append(popupTitle);
  if (location) {
    const popupLocation = document.createElement("span");
    popupLocation.textContent = location;
    popup.append(popupLocation);
  }
  const popupKind = document.createElement("small");
  popupKind.textContent = semanticLabel(item);
  popup.append(popupKind);
  const marker = new maplibregl.Marker({ element, anchor: "bottom" })
    .setLngLat([item.lon, item.lat])
    .setPopup(new maplibregl.Popup({ closeButton: false, offset: 18 }).setDOMContent(popup))
    .addTo(map);
  element.addEventListener("click", () => {
    mapHudText.textContent = `Selected ${title}`;
    openPlaceLens(item);
    setPanelCollapsed(true, title);
  });
  // MapLibre supplies a generic marker label during construction; restore the
  // real place name once the marker is mounted.
  element.setAttribute("aria-label", accessibleLabel);
  return { marker, element };
}

function renderResults(results, { fit = false, query = "" } = {}) {
  clearMarkers();
  resultList.replaceChildren();
  const bounds = new maplibregl.LngLatBounds();
  const visibleResults = results.slice(0, resultLimit());
  const geometryFeatures = visibleResults.map(item => resultGeometryFeature(item, decodePolyline)).filter(Boolean);
  const geometryById = new Map(geometryFeatures.map(feature => [String(feature.id), feature]));
  map.getSource("resultGeometries")?.setData({
    type: "FeatureCollection",
    features: geometryFeatures
  });

  for (const [index, item] of visibleResults.entries()) {
    const hasPoint = Number.isFinite(item.lat) && Number.isFinite(item.lon);
    const markerEntry = hasPoint ? markerFor(item, index) : null;
    if (markerEntry) {
      markers.push(markerEntry);
    }
    const featureBounds = geometryFeatureBounds(geometryById.get(String(item.id)));
    if (featureBounds) {
      bounds.extend(featureBounds[0]);
      bounds.extend(featureBounds[1]);
    } else if (hasPoint) bounds.extend([item.lon, item.lat]);

    const listItem = document.createElement("li");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "result-card";
    button.classList.toggle("is-postal-area", String(item.type || "").toLowerCase() === "postal_code");
    const title = item.name || item.title || item.id;
    const location = resultLocation(item);
    button.setAttribute("aria-label", `${title}${location ? `, ${location}` : ""}`);

    const number = document.createElement("span");
    number.className = "result-card__number";
    number.textContent = index + 1;

    const body = document.createElement("span");
    body.className = "result-card__body";
    const name = document.createElement("span");
    name.className = "name";
    name.textContent = title;
    body.append(name);

    if (location) {
      const address = document.createElement("span");
      address.className = "address";
      address.textContent = location;
      body.append(address);
    }

    const meta = document.createElement("span");
    meta.className = "meta";
    const addressCount = Number(item.address_count);
    const sampleCount = Number(item.sample_count);
    const parts = [
      semanticLabel(item),
      Number.isFinite(addressCount) && addressCount > 0
        ? `${formatCompact(addressCount)} civic ${addressCount === 1 ? "address" : "addresses"}`
        : "",
      item.type === "postal_code" && Number.isFinite(sampleCount) && sampleCount > 0
        ? `${formatCompact(sampleCount)} coverage ${sampleCount === 1 ? "sample" : "samples"}`
        : "",
      formatDistance(item.distanceMeters),
      item.shard ? String(item.shard).replaceAll("-", " ") : ""
    ].filter(Boolean);
    for (const part of parts) {
      const span = document.createElement("span");
      span.textContent = part;
      meta.append(span);
    }
    body.append(meta);

    const details = item.details && typeof item.details === "object" ? item.details : {};
    const chipValues = [
      item.openNow === true ? "Open now" : item.openNow === false ? "Closed" : details.opening_hours ? "Hours" : "",
      details.brand,
      details.cuisine ? detailText(details.cuisine) : "",
      yesNoDetail(details.wheelchair, "Accessible"),
      yesNoDetail(details.delivery, "Delivery")
    ].filter(Boolean).slice(0, 3);
    if (chipValues.length) {
      const chips = document.createElement("span");
      chips.className = "result-card__chips";
      for (const value of chipValues) {
        const chip = document.createElement("span");
        chip.textContent = value;
        chips.append(chip);
      }
      body.append(chips);
    }

    const arrow = document.createElement("span");
    arrow.className = "result-card__arrow";
    arrow.textContent = "→";
    arrow.setAttribute("aria-hidden", "true");
    button.append(number, body, arrow);

    const flyToResult = () => {
      if (featureBounds) {
        map.fitBounds(featureBounds, { padding: resultFitPadding(), maxZoom: 14, duration: 700 });
      } else if (hasPoint) {
        map.flyTo({ center: [item.lon, item.lat], zoom: Math.max(map.getZoom(), 15), essential: true });
      }
      markerEntry?.marker.togglePopup();
      mapHudText.textContent = `Selected ${title}`;
      openPlaceLens(item);
      setPanelCollapsed(true, title);
    };
    button.addEventListener("click", flyToResult);
    button.addEventListener("mouseenter", () => markerEntry?.element.classList.add("active"));
    button.addEventListener("mouseleave", () => markerEntry?.element.classList.remove("active"));
    button.addEventListener("focus", () => markerEntry?.element.classList.add("active"));
    button.addEventListener("blur", () => markerEntry?.element.classList.remove("active"));
    listItem.append(button);
    resultList.append(listItem);
  }

  if (!visibleResults.length) {
    showEmpty(
      query ? "No signal found" : "This view is quiet",
      query
        ? `No indexed place matched “${query}”. Try another spelling or a published region.`
        : "Move or zoom the map to scan another published region."
    );
    return;
  }

  emptyState.hidden = true;
  resultList.hidden = false;
  if (fit && !bounds.isEmpty()) {
    const hasAreaGeometry = geometryFeatures.some(feature => feature.geometry?.type === "Polygon");
    map.fitBounds(bounds, { padding: resultFitPadding(), maxZoom: hasAreaGeometry ? 14 : 15, duration: 700 });
  }
}

async function runSearch({ fit = false } = {}) {
  if (!engine) return;
  if (mapPickActive) setMapPickActive(false);
  const token = ++searchToken;
  const q = queryInput.value.trim();
  const queryOverride = activeQueryOverride?.displayQuery === q ? activeQueryOverride : null;
  const addressLookup = !queryOverride && looksLikeAddress(q);
  const areaWasChecked = areaToggle.checked;
  const useArea = !queryOverride && areaToggle.checked && !addressLookup;
  if (addressLookup && areaWasChecked) areaToggle.checked = false;
  clearButton.hidden = !q;
  searchPanel.classList.toggle("has-query", Boolean(q));

  const params = {
    ...(queryOverride?.params || { q }),
    size: resultLimit(),
    trace: true
  };
  const hintedShards = !queryOverride && selectedSuggestionHint?.query === q
    ? selectedSuggestionHint.shards
    : [];
  if (hintedShards?.length) params.shards = hintedShards;
  const areaBox = useArea ? viewportBox() : null;
  if (useArea && !areaBox) {
    setStatus("Waiting for the map to settle…", "ready");
    mapHudText.textContent = "Move or zoom the map, then search this area";
    return;
  }
  if (areaBox) params.geo = { box: areaBox };
  // Anchor plain queries to the user (or the viewport they chose): bare
  // categories become nearest-first and text search tries the local shard
  // before the world. Explicit modes — a suggestion's home shard, the area
  // toggle, a discovery orbit — already carry stronger intent.
  const anchor = !queryOverride && !areaBox && !hintedShards?.length ? searchAnchor() : null;
  if (anchor) params.near = { lat: anchor.lat, lon: anchor.lon };
  if (!q && !useArea) {
    anchoredQueryActive = false;
    hidePlaceLens();
    queryReceipt.hidden = true;
    resultList.replaceChildren();
    clearMarkers();
    setStatus("Ready to search", "ready");
    mapHudText.textContent = "Search anywhere in the published index";
    showEmpty("Find a place", "Search by name or address, try “pharmacy near me”, or a category with a place such as “pharmacy in Birmingham”.");
    return;
  }

  beginQueryReceipt();
  // Previous results and the place card stay visible (dimmed) while the new
  // answer streams in — a slow query should read as "working", not "wiped".
  setResultsLoading(true);
  setStatus("Scanning static byte ranges…", "loading");
  mapHudText.textContent = q ? `Searching “${q}”` : "Scanning this viewport";
  const started = performance.now();
  try {
    const response = await searchOsmQuery(engine, params);
    if (token !== searchToken) return;
    const plannerLane = response.stats?.plannerLane;
    const resolvedLocation = RESOLVED_LOCATION_LANES.has(plannerLane);
    if (resolvedLocation) areaToggle.checked = false;
    // A view-anchored result (not a named place): panning re-runs it.
    anchoredQueryActive = Boolean(anchor) && !resolvedLocation;
    setResultsLoading(false);
    hidePlaceLens();
    renderResults(response.results, {
      fit: fit || resolvedLocation || (addressLookup && areaWasChecked),
      query: q
    });
    const ms = Math.round(performance.now() - started);
    const queried = response.stats?.shardsQueried;
    const available = response.stats?.shards || engine.shards?.length;
    const shardText = queried != null
      ? ` · ${formatNumber(queried)} of ${formatNumber(available)} shards`
      : "";
    const directText = hintedShards?.length ? " · direct suggestion route" : "";
    const nearLane = response.stats?.plannerLane === "osmCategoryNearby"
      || response.stats?.plannerLane === "osmNearText"
      || response.stats?.plannerLane === "osmNearExactText"
      || response.stats?.plannerLane === "osmNearFuzzyText"
      || response.stats?.plannerLane === "osmNearExactGeo"
      || response.stats?.plannerLane === "osmNearFuzzyGeo";
    const nearText = nearLane && anchor ? ` · near ${anchor.source}` : "";
    const modeText = queryOverride?.mode ? ` · ${queryOverride.mode}` : "";
    const plannerText = PLANNER_LABELS.has(plannerLane) ? ` · ${PLANNER_LABELS.get(plannerLane)}` : "";
    const shown = Math.min(response.results.length, resultLimit());
    const timing = `${(ms / 1000).toFixed(1)}s${shardText}`;
    setStatus(response.total
      ? `Showing ${formatNumber(shown)} of ${formatNumber(response.total)}${response.approximate ? "+" : ""} matches · ${timing}${directText}${nearText}${modeText}${plannerText}`
      : `No matches · ${timing}${directText}${nearText}${modeText}${plannerText}`);
    renderQueryReceipt(response, shown, {
      query: q,
      kind: "search",
      anchor,
      areaBox,
      shards: hintedShards?.length ? hintedShards : null,
      params,
      elapsedMs: ms
    });
    const scopeText = useArea
      ? "this map area"
      : anchoredQueryActive
        ? "drag to refresh"
        : "everywhere";
    const postalResult = shown === 1 && response.results[0]?.type === "postal_code"
      ? response.results[0]
      : null;
    mapHudText.textContent = postalResult
      ? `${postalResult.postcode || postalResult.name} · approximate postal coverage`
      : `${formatNumber(shown)} ${shown === 1 ? "result" : "results"} · ${scopeText}`;
  } catch (error) {
    if (token !== searchToken) return;
    anchoredQueryActive = false;
    setResultsLoading(false);
    hidePlaceLens();
    const message = error?.message || "Search failed";
    interruptQueryReceipt();
    setStatus(message, "error");
    mapHudText.textContent = "Search interrupted";
    showEmpty("The atlas lost the trail", "The public index could not answer this query. Try again in a moment.");
  }
}

function hideSuggestions() {
  suggestList.hidden = true;
  suggestList.replaceChildren();
  visibleSuggestions = [];
  activeSuggestion = -1;
  clearSuggestionPreviews();
  queryInput.setAttribute("aria-expanded", "false");
  queryInput.removeAttribute("aria-activedescendant");
}

function setActiveSuggestion(index) {
  const options = [...suggestList.querySelectorAll('[role="option"]')];
  if (!options.length) return;
  activeSuggestion = (index + options.length) % options.length;
  for (const [optionIndex, option] of options.entries()) {
    const active = optionIndex === activeSuggestion;
    option.classList.toggle("active", active);
    option.setAttribute("aria-selected", String(active));
    if (active) {
      queryInput.setAttribute("aria-activedescendant", option.id);
      option.scrollIntoView({ block: "nearest" });
    }
  }
  setActiveSuggestionPreview(activeSuggestion);
}

let suggestionPreviewMarkers = new Map();
let suggestionPreviewToken = 0;

function clearSuggestionPreviews() {
  suggestionPreviewToken++;
  for (const marker of suggestionPreviewMarkers.values()) marker.remove();
  suggestionPreviewMarkers.clear();
}

function setActiveSuggestionPreview(index) {
  for (const [markerIndex, marker] of suggestionPreviewMarkers) {
    marker.getElement().classList.toggle("active", markerIndex === index);
  }
  previewSuggestion(index);
}

// Hydrating a suggestion's document is two small range reads, so the dropdown
// previews the row the user is actually considering — a pin where it is, plus
// the distance from the search anchor — instead of paying for every row's
// document on every keystroke. Pins accumulate as the user arrows through and
// clear with the dropdown.
async function previewSuggestion(index) {
  const item = visibleSuggestions[index];
  if (!item || item.doc == null || suggestionPreviewMarkers.has(index)) return;
  const token = suggestionPreviewToken;
  if (!item.result) {
    try {
      await hydrateOsmSuggestions(engine, [item]);
    } catch {
      return; // Advisory: a preview that cannot load simply does not appear.
    }
  }
  if (token !== suggestionPreviewToken) return;
  const place = item.result;
  if (!place || !Number.isFinite(place.lat) || !Number.isFinite(place.lon)) return;
  const element = document.createElement("span");
  element.className = "suggestion-preview-marker active";
  element.dataset.kind = item.kind || "place";
  element.setAttribute("aria-hidden", "true");
  suggestionPreviewMarkers.set(index, new maplibregl.Marker({ element, anchor: "center" })
    .setLngLat([place.lon, place.lat])
    .addTo(map));
  setActiveSuggestionPreview(index);
  annotateSuggestionDistance(index, item);
}

// A hydrated preview knows where it is: annotate its row with the distance
// from the search anchor once the document arrives.
function annotateSuggestionDistance(index, item) {
  const option = suggestList.querySelector(`#suggestion-${index}`);
  const count = option?.querySelector(".count");
  const anchor = searchAnchor();
  const place = item.result;
  if (!count || !anchor || !place || !Number.isFinite(place.lat) || !Number.isFinite(place.lon)) return;
  const distance = formatDistance(haversineMeters(anchor, { lat: place.lat, lon: place.lon }));
  if (!distance) return;
  const base = count.dataset.baseLabel || count.textContent;
  count.dataset.baseLabel = base;
  count.textContent = base === "place" ? distance : `${base} · ${distance}`;
  option.setAttribute("aria-label", `${item.description || item.text}, ${count.textContent}`);
}

function haversineMeters(a, b) {
  const rad = Math.PI / 180;
  const dLat = (b.lat - a.lat) * rad;
  const dLon = (b.lon - a.lon) * rad;
  const h = Math.sin(dLat / 2) ** 2
    + Math.cos(a.lat * rad) * Math.cos(b.lat * rad) * Math.sin(dLon / 2) ** 2;
  return 2 * 6371000 * Math.asin(Math.sqrt(Math.min(1, h)));
}

function chooseSuggestion(suggestion) {
  const item = typeof suggestion === "string" ? { text: suggestion } : suggestion;
  const query = String(item.selection?.query || item.text || "").trim();
  const shards = item.selection?.shards || item.shards || [];
  if (!query) return;
  queryInput.value = query;
  activeQueryOverride = null;
  selectedSuggestionHint = shards.length
    ? { query, shards: [...shards] }
    : null;
  clearButton.hidden = false;
  hideSuggestions();
  areaToggle.checked = false;
  // An entity suggestion names exactly one document: hydrate it directly
  // (a couple of small range reads) instead of re-running the query as a
  // search. Anything else — categories, streets, ambiguous names — still
  // needs the planner.
  if (item.selection?.doc) {
    runEntitySelection(item, query);
    return;
  }
  runSearch({ fit: true });
}

async function runEntitySelection(item, query) {
  if (!engine) return;
  if (mapPickActive) setMapPickActive(false);
  const token = ++searchToken;
  searchPanel.classList.add("has-query");
  beginQueryReceipt();
  setResultsLoading(true);
  setStatus("Resolving the place from its suggestion…", "loading");
  mapHudText.textContent = `Locating “${query}”`;
  const started = performance.now();
  try {
    const response = await resolveOsmSuggestion(engine, item);
    if (token !== searchToken) return;
    if (!response.results.length) throw new Error("entity suggestion resolved to no document");
    const place = response.results[0];
    anchoredQueryActive = false;
    setResultsLoading(false);
    hidePlaceLens();
    renderResults(response.results, { fit: true, query });
    const ms = Math.round(performance.now() - started);
    setStatus(`Showing 1 of 1 matches · ${(ms / 1000).toFixed(1)}s · ${PLANNER_LABELS.get("osmSuggestEntity")}`);
    renderQueryReceipt(response, 1, {
      query,
      kind: "suggestion-entity",
      anchor: searchAnchor(),
      params: { selection: item.selection || null },
      elapsedMs: ms
    });
    openPlaceLens(place);
    mapHudText.textContent = `${place.name || query} · direct from the suggestion`;
  } catch {
    if (token !== searchToken) return;
    // Fail open: the classic suggestion-to-search path still answers.
    setResultsLoading(false);
    runSearch({ fit: true });
  }
}

function suggestionRegionLabel(shards) {
  const labels = (shards || []).map(shard => String(shard)
    .replaceAll("-", " ")
    .replace(/\b\p{L}/gu, letter => letter.toLocaleUpperCase()));
  if (labels.length <= 2) return labels.join(" + ");
  return `${labels.length} regions`;
}

function cancelSuggestions() {
  clearTimeout(suggestTimer);
  suggestToken++;
  suggestQueued = false;
  suggestionsSuppressed = true;
  suppressedQuery = queryInput.value;
  hideSuggestions();
}

async function showSuggestions() {
  const rawQuery = queryInput.value;
  const q = rawQuery.trim();
  const leadingWhitespace = rawQuery.length - rawQuery.trimStart().length;
  const cursor = Number.isFinite(queryInput.selectionStart)
    ? Math.max(0, Math.min(q.length, queryInput.selectionStart - leadingWhitespace))
    : q.length;
  const token = ++suggestToken;
  if (!engine || Array.from(q).length < SUGGEST_MIN_CHARACTERS || suggestionsSuppressed) {
    hideSuggestions();
    return;
  }
  const anchor = searchAnchor();
  const cacheKey = `${q.toLocaleLowerCase()}\0${cursor}\0${anchor
    ? `${anchor.lat.toFixed(1)},${anchor.lon.toFixed(1)}`
    : ""}`;
  const cached = suggestionCache.get(cacheKey);
  if (cached) {
    suggestionCache.delete(cacheKey);
    suggestionCache.set(cacheKey, cached);
    renderSuggestions(cached);
    return;
  }
  if (suggestInFlight) {
    suggestQueued = true;
    return;
  }
  suggestInFlight = true;
  try {
    const response = await suggestOsmQuery(engine, {
      q,
      inputOffset: cursor,
      size: 8,
      // Previews hydrate per focused row (see previewSuggestion), not in bulk:
      // on a planet-scale index with rich payloads, eight scattered documents
      // live on eight ~1 MB doc pages, while one document is two ~0.4 KB
      // reads. Bulk `hydrate: true` is the right call on smaller indexes.
      ...(anchor ? { near: { lat: anchor.lat, lon: anchor.lon } } : {})
    });
    if (token !== suggestToken || suggestionsSuppressed) return;
    suggestionCache.set(cacheKey, response);
    if (suggestionCache.size > SUGGEST_CACHE_LIMIT) {
      suggestionCache.delete(suggestionCache.keys().next().value);
    }
    renderSuggestions(response);
  } catch {
    if (token === suggestToken) hideSuggestions();
  } finally {
    suggestInFlight = false;
    if (suggestQueued && !suggestionsSuppressed) {
      suggestQueued = false;
      queueMicrotask(showSuggestions);
    }
  }
}

function appendMatchedText(node, text, ranges, sourceOffset = 0) {
  const value = String(text || "");
  const endOffset = sourceOffset + value.length;
  const relevant = (Array.isArray(ranges) ? ranges : [])
    .map(range => ({
      start: Math.max(sourceOffset, Number(range?.start)),
      end: Math.min(endOffset, Number(range?.end))
    }))
    .filter(range => Number.isFinite(range.start) && Number.isFinite(range.end) && range.end > range.start)
    .sort((left, right) => left.start - right.start || left.end - right.end);
  let cursor = 0;
  for (const range of relevant) {
    const start = Math.max(cursor, range.start - sourceOffset);
    const end = Math.max(start, range.end - sourceOffset);
    if (start > cursor) node.append(document.createTextNode(value.slice(cursor, start)));
    if (end > cursor) {
      const mark = document.createElement("mark");
      mark.textContent = value.slice(start, end);
      node.append(mark);
    }
    cursor = Math.max(cursor, end);
  }
  if (cursor < value.length) node.append(document.createTextNode(value.slice(cursor)));
}

function renderSuggestions(response) {
  const suggestions = response.suggestions || [];
  if (!suggestions.length) {
    hideSuggestions();
    return;
  }
  // A peek-height sheet would clip the list; give it room to be tapped.
  if (searchPanel.dataset.snap === "peek") snapSheet("half");
  visibleSuggestions = suggestions;
  clearSuggestionPreviews();
  suggestList.replaceChildren(...suggestions.map((item, index) => {
    const option = document.createElement("li");
    option.id = `suggestion-${index}`;
    option.setAttribute("role", "option");
    option.setAttribute("aria-selected", "false");
    const description = String(item.description || item.text || "");
    const mainText = String(item.mainText || description);
    const secondaryText = String(item.secondaryText || "");
    const mainIndex = description.indexOf(mainText);
    const mainOffset = mainIndex >= 0 ? mainIndex : description.length + 1;
    const secondaryIndex = secondaryText ? description.indexOf(secondaryText, Math.max(0, mainIndex) + mainText.length) : -1;
    const secondaryOffset = secondaryIndex >= 0 ? secondaryIndex : description.length + 1;
    const icon = document.createElement("span");
    icon.className = "suggestion-icon";
    icon.dataset.kind = item.kind || item.type || "place";
    icon.setAttribute("aria-hidden", "true");
    const body = document.createElement("span");
    body.className = "suggestion-body";
    const primary = document.createElement("span");
    primary.className = "suggestion-primary";
    appendMatchedText(primary, mainText, item.matchedRanges, mainOffset);
    body.append(primary);
    if (secondaryText) {
      const secondary = document.createElement("span");
      secondary.className = "suggestion-secondary";
      appendMatchedText(secondary, secondaryText, item.matchedRanges, secondaryOffset);
      body.append(secondary);
    }
    const count = document.createElement("span");
    count.className = "count";
    const shards = item.selection?.shards || item.shards;
    count.textContent = item.kind === "category-locality"
      ? "nearby"
      : item.type === "street" || item.kind === "street"
      ? "street"
      : item.interpolated
        ? "interpolated"
        : shards?.length
          ? suggestionRegionLabel(shards)
          : item.count > 1 ? `×${formatNumber(item.count)}` : "place";
    option.setAttribute("aria-label", `${description}${count.textContent ? `, ${count.textContent}` : ""}`);
    option.append(icon, body, count);
    option.addEventListener("pointerdown", event => {
      event.preventDefault();
      chooseSuggestion(item);
    });
    option.addEventListener("pointerenter", () => setActiveSuggestionPreview(index));
    return option;
  }));
  suggestList.hidden = false;
  queryInput.setAttribute("aria-expanded", "true");
  // Preview the top row immediately; the rest hydrate as the user moves.
  previewSuggestion(0);
}

async function loadIndexStatus() {
  try {
    const response = await fetch(new URL("status.json", OSM_INDEX_BASE_URL));
    if (!response.ok) throw new Error(`Status ${response.status}`);
    const status = await response.json();
    const index = status.index || {};
    const running = status.run?.state === "running";
    const expanding = Number(index.publishedShards || 0) < Number(index.totalRegions || 0);
    livePill.dataset.state = running ? "running" : "ready";
    liveState.textContent = running ? "Indexing" : expanding ? "Expanding" : "Live";
    livePill.title = `${formatNumber(index.publishedShards)} of ${formatNumber(index.totalRegions)} regional shards published`;
    coverageMetric.textContent = Number.isFinite(Number(index.publicationPercent))
      ? `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(Number(index.publicationPercent))}%`
      : "Live";
  } catch {
    livePill.dataset.state = "ready";
    liveState.textContent = "Live";
    coverageMetric.textContent = "Live";
  }
}

queryInput.addEventListener("input", () => {
  if (mapPickActive) setMapPickActive(false);
  activeQueryOverride = null;
  clearButton.hidden = !queryInput.value;
  if (selectedSuggestionHint?.query !== queryInput.value.trim()) selectedSuggestionHint = null;
  if (queryInput.value !== suppressedQuery) {
    suggestionsSuppressed = false;
    suppressedQuery = "";
  }
  clearTimeout(suggestTimer);
  if (suggestionsSuppressed) return;
  suggestTimer = setTimeout(showSuggestions, SUGGEST_DEBOUNCE_MS);
});

queryInput.addEventListener("keydown", event => {
  const options = suggestList.querySelectorAll('[role="option"]');
  if (event.key === "ArrowDown" && !suggestList.hidden && options.length) {
    event.preventDefault();
    setActiveSuggestion(activeSuggestion + 1);
    return;
  }
  if (event.key === "ArrowUp" && !suggestList.hidden && options.length) {
    event.preventDefault();
    setActiveSuggestion(activeSuggestion - 1);
    return;
  }
  if (event.key === "Enter") {
    event.preventDefault();
    if (activeSuggestion >= 0 && options[activeSuggestion]) {
      chooseSuggestion(visibleSuggestions[activeSuggestion] || options[activeSuggestion].querySelector("span")?.textContent || "");
    } else {
      cancelSuggestions();
      runSearch({ fit: !areaToggle.checked });
    }
  }
  if (event.key === "Escape") cancelSuggestions();
});

queryInput.addEventListener("blur", () => setTimeout(hideSuggestions, 150));

clearButton.addEventListener("click", () => {
  queryInput.value = "";
  activeQueryOverride = null;
  selectedSuggestionHint = null;
  clearButton.hidden = true;
  areaToggle.checked = false;
  cancelSuggestions();
  queryInput.focus();
  runSearch();
});

searchButton.addEventListener("click", () => {
  cancelSuggestions();
  runSearch({ fit: !areaToggle.checked });
});

panelToggle.addEventListener("click", () => {
  setPanelCollapsed(!searchPanel.classList.contains("is-collapsed"));
});

placeLensClose.addEventListener("click", () => {
  hidePlaceLens();
  setPanelCollapsed(false);
});

mapPickButton.addEventListener("click", () => {
  const active = !mapPickActive;
  setMapPickActive(active);
  if (!active) {
    setStatus("Map selection cancelled", "ready");
    mapHudText.textContent = "Search anywhere in the published index";
  }
});

for (const action of document.querySelectorAll("[data-orbit-query]")) {
  action.addEventListener("click", () => runDiscoveryOrbit(
    action.dataset.orbitQuery,
    action.dataset.orbitLabel
  ));
}

for (const example of document.querySelectorAll("[data-query]")) {
  example.addEventListener("click", () => {
    queryInput.value = example.dataset.query;
    activeQueryOverride = null;
    clearButton.hidden = false;
    areaToggle.checked = false;
    cancelSuggestions();
    runSearch({ fit: true });
  });
}

areaToggle.addEventListener("change", () => {
  activeQueryOverride = null;
  runSearch();
});

document.addEventListener("keydown", event => {
  if (event.key === "Escape" && mapPickActive) {
    setMapPickActive(false);
    setStatus("Map selection cancelled", "ready");
    return;
  }
  if (event.key === "Escape" && !placeLens.hidden && document.activeElement !== queryInput) {
    hidePlaceLens();
    setPanelCollapsed(false);
    return;
  }
  const shortcut = event.key === "/" && document.activeElement !== queryInput;
  const command = (event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k";
  if (!shortcut && !command) return;
  event.preventDefault();
  setPanelCollapsed(false);
  queryInput.focus();
  queryInput.select();
});

map.on("click", event => {
  if (!mapPickActive) return;
  const lat = Number(event.lngLat?.lat);
  const lon = Number(event.lngLat?.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
  const coordinateQuery = `${lat.toFixed(6)}, ${lon.toFixed(6)}`;
  setMapPickActive(false);
  activeQueryOverride = {
    displayQuery: coordinateQuery,
    mode: "map point",
    params: { q: coordinateQuery, reverseGeocode: true }
  };
  queryInput.value = coordinateQuery;
  selectedSuggestionHint = null;
  clearButton.hidden = false;
  areaToggle.checked = false;
  runSearch({ fit: true });
});

map.on("movestart", event => {
  if (!engine) return;
  if (areaToggle.checked) mapHudText.textContent = "New viewport detected";
  else if (anchoredQueryActive && event.originalEvent) mapHudText.textContent = "Release to search this area";
});

map.on("moveend", event => {
  if (!engine) return;
  // Area toggle: the box IS the query, so any settle re-runs it.
  if (areaToggle.checked) {
    clearTimeout(moveTimer);
    moveTimer = setTimeout(() => runSearch(), 250);
    return;
  }
  // View-anchored query: re-run only when the USER moved the map (drag,
  // scroll-zoom, pinch — all carry originalEvent), never on the programmatic
  // recentring the app does after a search, which would loop. A refresh
  // must not yank the map back to the result bounds, so it never fits.
  if (anchoredQueryActive && event.originalEvent) {
    clearTimeout(moveTimer);
    moveTimer = setTimeout(() => runSearch({ fit: false }), 320);
  }
});

// --- Directions (rfroutegraph-v1 static routing) ---------------------------
//
// The route graph is the same substrate as search: immutable content-addressed
// objects behind HTTP range reads. The engine snaps, routes, and unpacks
// geometry entirely in this browser; its output polyline feeds the existing
// route-corridor search lane, closing the "coffee along my trip" loop.

const modeSearchTab = document.querySelector("#modeSearchTab");
const modeDirectionsTab = document.querySelector("#modeDirectionsTab");
const searchControls = document.querySelector("#searchControls");
const directionsControls = document.querySelector("#directionsControls");
const directionsForm = document.querySelector("#directionsForm");
const routeSetupHint = document.querySelector("#routeSetupHint");
const stopListEl = document.querySelector("#stopList");
const addStopButton = document.querySelector("#addStopButton");
const swapStopsButton = document.querySelector("#swapStopsButton");
const clearStopsButton = document.querySelector("#clearStopsButton");
const departureRow = document.querySelector("#departureRow");
const tripEndRow = document.querySelector("#tripEndRow");
const routeReceipt = document.querySelector("#routeReceipt");
const routeReceiptSummary = document.querySelector("#routeReceiptSummary");
const routeReceiptRoute = document.querySelector("#routeReceiptRoute");
const routeReceiptBars = document.querySelector("#routeReceiptBars");
const routeCard = document.querySelector("#routeCard");
const routeEta = document.querySelector("#routeEta");
const routeDistanceEl = document.querySelector("#routeDistance");
const routeBucketEl = document.querySelector("#routeBucket");
const routeOrderEl = document.querySelector("#routeOrder");
const routeAlternativesEl = document.querySelector("#routeAlternatives");
const routeStepsWrap = document.querySelector("#routeStepsWrap");
const routeStepsEl = document.querySelector("#routeSteps");
const toastEl = document.querySelector("#toast");

const MAX_STOPS = 8;
const ROUTE_COLORS = { active: "#ffc940", legAlt: "#35c2ac", casing: "#14161d", alt: "#8a92a0" };
const SNAP_NOTE_MIN_METERS = 20;
const COVERAGE_PAD_DEG = 0.12;

// The published planet catalog: one immutable route graph per region, plus
// the portal packs that join them. It is the discovery layer only — a single
// regional graph is what snaps, routes, and unpacks geometry here.
const OSM_ROUTE_CATALOG_URL = new URL("routes/catalog.json", OSM_INDEX_BASE_URL).href;

let routeEngine = null;
let routeAvailable = null; // null = probing, false = missing, true = ready
let routeCoverage = null;
let routeBucketNames = [];
let routeCatalogIndexes = null; // null while local, else the car-profile regions
let routeRegionLabel = "";
let directionsMode = false;
let stops = [];
let stopPickIndex = -1;
let departureChoice = "now";
// Where a multi-stop run is allowed to finish. "last" pins the address the
// dispatcher typed last, "open" lets the optimizer choose the terminus, and
// "loop" brings the vehicle home. Only ever offered from three stops up,
// because with two there is nothing to order.
let tripEndChoice = "last";
let routePlan = null; // { kind: "pair", candidates, active } | { kind: "trip", trip }
let routeToken = 0;
let stopMarkers = [];
let toastTimer = null;

function showToast(message, tone = "info", duration = 4200) {
  toastEl.textContent = message;
  toastEl.dataset.tone = tone;
  toastEl.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { toastEl.hidden = true; }, duration);
}

function formatDuration(seconds) {
  const minutes = Math.round(Math.max(0, Number(seconds) || 0) / 60);
  if (minutes < 1) return "< 1 min";
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest ? `${hours} h ${String(rest).padStart(2, "0")}` : `${hours} h`;
}

function stopLetter(index) {
  return String.fromCharCode(65 + (index % 26));
}

function bucketLabel(name) {
  if (name === "peak") return "rush-hour metric";
  if (name === "base") return routeBucketNames.length > 1 ? "free-flow metric" : "static metric";
  return `${name} metric`;
}

function coverageCenter() {
  if (!routeCoverage) return null;
  return {
    lat: (routeCoverage.minLat + routeCoverage.maxLat) / 2,
    lon: (routeCoverage.minLon + routeCoverage.maxLon) / 2
  };
}

// fitBounds silently no-ops (with a console warning) when the padding cannot
// fit the canvas — e.g. a briefly mis-measured panel. Fall back to centering.
function safeFitBounds(bounds, options) {
  const camera = map.cameraForBounds(bounds, { padding: options.padding, maxZoom: options.maxZoom });
  if (camera) map.fitBounds(bounds, options);
  else {
    const center = bounds instanceof maplibregl.LngLatBounds ? bounds.getCenter() : {
      lng: (bounds[0][0] + bounds[1][0]) / 2,
      lat: (bounds[0][1] + bounds[1][1]) / 2
    };
    map.flyTo({ center, zoom: Math.min(options.maxZoom ?? 11, 11), duration: options.duration });
  }
}

function insideCoverage(point, pad = COVERAGE_PAD_DEG) {
  if (!routeCoverage) return true;
  return point.lat >= routeCoverage.minLat - pad && point.lat <= routeCoverage.maxLat + pad
    && point.lon >= routeCoverage.minLon - pad && point.lon <= routeCoverage.maxLon + pad;
}

function activateRouteGraph(engine, label) {
  routeEngine = engine;
  routeRegionLabel = label;
  let minLat = Infinity;
  let maxLat = -Infinity;
  let minLon = Infinity;
  let maxLon = -Infinity;
  for (const leaf of engine.root.leaves) {
    if (leaf.bbox.minLat < minLat) minLat = leaf.bbox.minLat;
    if (leaf.bbox.maxLat > maxLat) maxLat = leaf.bbox.maxLat;
    if (leaf.bbox.minLon < minLon) minLon = leaf.bbox.minLon;
    if (leaf.bbox.maxLon > maxLon) maxLon = leaf.bbox.maxLon;
  }
  routeCoverage = { minLat: minLat / 1e7, maxLat: maxLat / 1e7, minLon: minLon / 1e7, maxLon: maxLon / 1e7 };
  routeBucketNames = engine.root.buckets.map(bucket => bucket.name);
  routeAvailable = true;
  departureRow.hidden = routeBucketNames.length < 2;
  modeDirectionsTab.title = label
    ? `Turn-by-turn routing computed in this browser — ${label}`
    : "Turn-by-turn routing computed in this browser";
  // Live traffic is bound to this exact index build (its sourceHash is
  // the mesh epoch), so the card only exists once the graph is open.
  wireMeshControls();
}

// Catalog bboxes are [minLat, minLon, maxLat, maxLon] in degrees.
function regionCovers(index, point, pad = COVERAGE_PAD_DEG) {
  const [minLat, minLon, maxLat, maxLon] = index.bbox;
  return point.lat >= minLat - pad && point.lat <= maxLat + pad
    && point.lon >= minLon - pad && point.lon <= maxLon + pad;
}

function regionArea(index) {
  const [minLat, minLon, maxLat, maxLon] = index.bbox;
  return Math.max(0, maxLat - minLat) * Math.max(0, maxLon - minLon);
}

// Open the published graph covering `point` and make it the active one.
// Regions are tried tightest-first: a small graph is the cheaper open, and
// where coverage nests the local build is the more detailed one. A region
// published by an older builder than this client simply does not open, so
// the search continues with the next candidate instead of failing outright.
async function openRegionFor(point) {
  if (!routeCatalogIndexes) return false;
  const candidates = routeCatalogIndexes
    .filter(index => regionCovers(index, point))
    .sort((left, right) => regionArea(left) - regionArea(right));
  for (const index of candidates) {
    try {
      const engine = await openRouteGraphUrl(resolveIndexBase(OSM_ROUTE_CATALOG_URL, index.base));
      activateRouteGraph(engine, humanize(index.region));
      return true;
    } catch {
      // Try the next region that covers this point.
    }
  }
  return false;
}

async function loadRouteCatalog() {
  const response = await fetch(OSM_ROUTE_CATALOG_URL, { cache: "no-store" });
  if (!response.ok) throw new Error(`route catalog ${response.status}`);
  const catalog = await response.json();
  const indexes = (catalog.indexes || [])
    .filter(index => index.profile === "car" && index.base && Array.isArray(index.bbox) && index.bbox.length === 4);
  if (!indexes.length) throw new Error("route catalog publishes no car regions");
  return indexes;
}

// A locally published graph wins when present — it is how the demo is
// developed offline — and otherwise the demo routes on the published planet
// catalog, opening the one region the current view needs.
async function initRouteGraph() {
  try {
    const probe = await fetch("route-graph/manifest.json").catch(() => null);
    if (probe?.ok) {
      activateRouteGraph(await openRouteGraphUrl("route-graph/"), "");
      return;
    }
    routeCatalogIndexes = await loadRouteCatalog();
    const center = map.getCenter();
    if (!await openRegionFor({ lat: center.lat, lon: center.lng })) {
      // No region opened for the current view: directions stay available,
      // and the first stop the user picks chooses the region instead.
      routeAvailable = true;
      routeCoverage = null;
      modeDirectionsTab.title = "Turn-by-turn routing computed in this browser";
    }
  } catch {
    routeAvailable = false;
    routeCatalogIndexes = null;
    modeDirectionsTab.setAttribute("aria-disabled", "true");
    modeDirectionsTab.title = "No route graph published";
    if (directionsMode) enterDirectionsUnavailable();
  }
}

function newStop() {
  return { text: "", place: null, snap: null, error: "", suggestToken: 0, resolveToken: 0 };
}

function resolvedStops() {
  return stops.filter(stop => stop.place && !stop.error);
}

function directionsReady() {
  return stops.length >= 2 && stops.every(stop => stop.place && !stop.error);
}

/** The end mode the itinerary planner is asked for. */
function tripEndParams() {
  if (tripEndChoice === "open") return { openEnd: true };
  if (tripEndChoice === "loop") return { roundTrip: true };
  return { roundTrip: false };
}

function updateTripEndRow() {
  if (!tripEndRow) return;
  tripEndRow.hidden = resolvedStops().length < 3;
}

function updateDirectionsHud() {
  if (!directionsMode || !routeAvailable) return;
  if (routePlan) return;
  const unresolved = stops.findIndex(stop => !stop.place);
  if (unresolved === 0) mapHudText.textContent = "Click the map to set the start point";
  else if (unresolved > 0) mapHudText.textContent = "Click the map to set the destination";
}

function directionsEmptyState() {
  queryReceipt.hidden = true;
  routeReceipt.hidden = true;
  routeCard.hidden = true;
  resultList.hidden = true;
  resultList.replaceChildren();
  showEmpty(
    "Chart a route",
    "Type a place in each field, or click the map — the first click sets the start, the next sets the destination. Routing runs in this browser from static byte ranges."
  );
}

function enterDirectionsUnavailable() {
  routeSetupHint.hidden = false;
  directionsForm.hidden = true;
  setStatus("Route graph unreachable", "error");
  showEmpty("Directions needs a route index", "Publish a route graph at route-graph/ using the commands above, or make the published route catalog reachable, then reload.");
}

function setDirectionsMode(on) {
  if (on === directionsMode) return;
  directionsMode = on;
  modeSearchTab.classList.toggle("active", !on);
  modeSearchTab.setAttribute("aria-selected", String(!on));
  modeDirectionsTab.classList.toggle("active", on);
  modeDirectionsTab.setAttribute("aria-selected", String(on));
  searchControls.hidden = on;
  directionsControls.hidden = !on;
  if (on) {
    setMapPickActive(false);
    cancelSuggestions();
    suggestionsSuppressed = false;
    suppressedQuery = "";
    hidePlaceLens();
    setPanelCollapsed(false);
    if (typeof snapSheet === "function") {
      // Let the directions controls lay out before measuring peek.
      requestAnimationFrame(() => snapSheet("peek"));
    }
    searchPanel.classList.add("has-query");
    clearMarkers();
    if (routeAvailable === false) {
      enterDirectionsUnavailable();
      return;
    }
    routeSetupHint.hidden = true;
    directionsForm.hidden = false;
    while (stops.length < 2) stops.push(newStop());
    renderStops();
    if (routePlan) {
      updateStopMarkers();
      drawRoutePlan();
      renderRouteCard();
      fitRoutePlan();
      const seconds = routePlan.kind === "pair"
        ? routePlan.candidates[routePlan.active].seconds
        : routePlan.trip.totalSeconds;
      const meters = routePlan.kind === "pair"
        ? routePlan.candidates[routePlan.active].distanceMeters
        : routePlan.trip.totalMeters;
      setStatus(`Route ready · ${formatDuration(seconds)} · ${formatDistance(meters)}`);
      mapHudText.textContent = `${formatDuration(seconds)} drive · ${formatDistance(meters)}`;
    } else {
      directionsEmptyState();
      setStatus("Plot a route", "ready");
      const center = map.getCenter();
      if (routeCoverage && !insideCoverage({ lat: center.lat, lon: center.lng }, 0.02)) {
        safeFitBounds(
          [[routeCoverage.minLon, routeCoverage.minLat], [routeCoverage.maxLon, routeCoverage.maxLat]],
          { padding: resultFitPadding(), maxZoom: 12, duration: 900 }
        );
        mapHudText.textContent = "Directions cover the published route graph";
      } else {
        updateDirectionsHud();
      }
      stops[0]?.input?.focus();
    }
  } else {
    cancelStopPick();
    clearRouteLayers();
    clearStopMarkers();
    routeToken++;
    routeCard.hidden = true;
    routeReceipt.hidden = true;
    clearMarkers();
    resultList.hidden = true;
    resultList.replaceChildren();
    activeQueryOverride = null;
    queryInput.value = "";
    clearButton.hidden = true;
    searchPanel.classList.remove("has-query");
    setStatus("Ready to search", "ready");
    mapHudText.textContent = "Search anywhere in the published index";
    showEmpty("Find a place", "Search by name or address, try “pharmacy near me”, or a category with a place such as “pharmacy in Birmingham”.");
  }
}

function cancelStopPick() {
  stopPickIndex = -1;
  map.getCanvas().classList.remove("is-picking");
  for (const stop of stops) stop.pickButton?.classList.remove("active");
}

function setStopPick(index) {
  const wasActive = stopPickIndex === index;
  cancelStopPick();
  if (wasActive) return;
  stopPickIndex = index;
  stops[index].pickButton?.classList.add("active");
  map.getCanvas().classList.add("is-picking");
  mapHudText.textContent = `Click the map to set point ${stopLetter(index)}`;
}

function hideStopSuggest(stop) {
  if (!stop?.suggestEl) return;
  stop.suggestEl.hidden = true;
  stop.suggestEl.replaceChildren();
  stop.suggestActive = -1;
  stop.suggestItems = [];
}

function renderStopSuggest(stop, index, suggestions) {
  stop.suggestItems = suggestions;
  stop.suggestActive = -1;
  stop.suggestEl.replaceChildren(...suggestions.map(item => {
    const option = document.createElement("li");
    option.setAttribute("role", "option");
    const main = document.createElement("span");
    main.textContent = String(item.mainText || item.description || item.text || "");
    option.append(main);
    const secondary = String(item.secondaryText || "");
    if (secondary) {
      const small = document.createElement("small");
      small.textContent = secondary;
      option.append(small);
    }
    option.addEventListener("pointerdown", event => {
      event.preventDefault();
      chooseStopSuggestion(index, item);
    });
    return option;
  }));
  stop.suggestEl.hidden = !suggestions.length;
  // A peek-height sheet clips the list; give it room to be tapped.
  if (suggestions.length && searchPanel.dataset.snap === "peek") snapSheet("half");
}

function chooseStopSuggestion(index, item) {
  const stop = stops[index];
  const query = String(item.selection?.query || item.text || item.description || "").trim();
  if (!stop || !query) return;
  stop.input.value = query;
  hideStopSuggest(stop);
  resolveStopText(index, query, item.selection?.shards || item.shards || []);
}

async function requestStopSuggest(index) {
  const stop = stops[index];
  if (!engine || !stop) return;
  const q = stop.input.value.trim();
  const token = ++stop.suggestToken;
  if (Array.from(q).length < SUGGEST_MIN_CHARACTERS) {
    hideStopSuggest(stop);
    return;
  }
  try {
    const anchor = coverageCenter() || searchAnchor();
    const response = await suggestOsmQuery(engine, {
      q,
      size: 6,
      ...(anchor ? { near: { lat: anchor.lat, lon: anchor.lon } } : {})
    });
    if (token !== stop.suggestToken || stop.input.value.trim() !== q) return;
    renderStopSuggest(stop, index, response.suggestions || []);
  } catch {
    if (token === stop.suggestToken) hideStopSuggest(stop);
  }
}

function setStopNote(stop, text, tone = "") {
  stop.noteText = text || "";
  stop.noteTone = tone;
  stop.note.hidden = !text;
  stop.note.textContent = text || "";
  if (tone) stop.note.dataset.tone = tone;
  else delete stop.note.dataset.tone;
  stop.row.dataset.state = tone === "error" ? "invalid" : "";
}

async function setStopPlace(index, place) {
  const stop = stops[index];
  if (!stop) return;
  const token = ++stop.resolveToken;
  stop.place = place;
  stop.snap = null;
  stop.error = "";
  if (!routeEngine || (routeCoverage && !insideCoverage(place))) {
    // One regional graph routes within itself, so a stop outside the open
    // region can only move the whole route: allowed while this is the sole
    // anchor, refused once another stop is committed to the current region.
    const anchored = stops.some(other => other !== stop && other.place && !other.error);
    const switched = !anchored && await openRegionFor(place);
    if (token !== stop.resolveToken) return;
    if (!switched) {
      stop.error = "coverage";
      setStopNote(stop, "Outside the routable area", "error");
      showToast(
        anchored && routeRegionLabel
          ? `This route is inside ${routeRegionLabel} — clear the stops to route somewhere else.`
          : "No published route graph covers that point yet.",
        "error"
      );
      updateStopMarkers();
      maybeRoute();
      return;
    }
  }
  if (!routeEngine) {
    setStopNote(stop, "Route graph still loading — try again in a moment", "error");
    stop.error = "loading";
    return;
  }
  setStopNote(stop, "Snapping to the road network…");
  updateStopMarkers();
  try {
    const snapped = await routeEngine.snap({ lat: place.lat, lon: place.lon });
    if (token !== stop.resolveToken) return;
    const match = snapped.matches[0];
    stop.snap = {
      lat: match.snappedLatE7 / 1e7,
      lon: match.snappedLonE7 / 1e7,
      meters: match.distMeters
    };
    setStopNote(stop, stop.snap.meters >= SNAP_NOTE_MIN_METERS
      ? `Snapped ${formatDistance(stop.snap.meters)} to the nearest road`
      : "");
  } catch (error) {
    if (token !== stop.resolveToken) return;
    if (error?.code === "RANGEFIND_ROUTE_SNAP_TOO_FAR") {
      stop.error = "snap";
      setStopNote(stop, "No road within 250 m of this point", "error");
      showToast("No road near that point — try closer to a street.", "error");
    } else {
      stop.error = "snap";
      setStopNote(stop, error?.message || "Snap failed", "error");
    }
  }
  updateStopMarkers();
  maybeRoute();
}

async function resolveStopText(index, text, shards = []) {
  const stop = stops[index];
  if (!stop || !engine) return;
  const q = String(text || "").trim();
  stop.text = q;
  stop.place = null;
  stop.snap = null;
  stop.error = "";
  const token = ++stop.resolveToken;
  if (!q) {
    setStopNote(stop, "");
    updateStopMarkers();
    maybeRoute();
    return;
  }
  setStopNote(stop, "Locating…");
  try {
    const anchor = coverageCenter() || searchAnchor();
    const params = { q, size: 1 };
    if (shards?.length) params.shards = [...shards];
    else if (anchor) params.near = { lat: anchor.lat, lon: anchor.lon };
    const response = await searchOsmQuery(engine, params);
    if (token !== stop.resolveToken) return;
    const hit = (response.results || []).find(item => Number.isFinite(item.lat) && Number.isFinite(item.lon));
    if (!hit) {
      stop.error = "match";
      setStopNote(stop, `No indexed place matched “${q}”`, "error");
      maybeRoute();
      return;
    }
    const label = hit.name || hit.title || hit.formattedAddress || q;
    stop.text = label;
    stop.input.value = label;
    setStopPlace(index, { lat: hit.lat, lon: hit.lon, label });
  } catch (error) {
    if (token !== stop.resolveToken) return;
    stop.error = "match";
    setStopNote(stop, error?.message || "Lookup failed", "error");
    maybeRoute();
  }
}

function setStopFromMap(index, lat, lon) {
  const stop = stops[index];
  if (!stop) return;
  const coordinateLabel = `${lat.toFixed(5)}, ${lon.toFixed(5)}`;
  stop.text = coordinateLabel;
  stop.input.value = coordinateLabel;
  setStopNote(stop, "");
  setStopPlace(index, { lat, lon, label: coordinateLabel });
  // Best-effort pretty label via the search index's reverse geocoder.
  if (!engine) return;
  reverseGeocodeOsm(engine, { lat, lon, size: 1 })
    .then(response => {
      const hit = (response.results || [])[0];
      const label = hit?.formattedAddress || hit?.name;
      if (!label || stops[index] !== stop) return;
      if (stop.place && stop.place.label === coordinateLabel) {
        stop.place.label = label;
        stop.text = label;
        stop.input.value = label;
        updateStopMarkers();
        if (routePlan) renderRouteCard();
      }
    })
    .catch(() => {});
}

function renderStops() {
  stopListEl.replaceChildren(...stops.map((stop, index) => {
    const row = document.createElement("li");
    row.className = "stop-row";
    row.dataset.role = index === 0 ? "origin" : index === stops.length - 1 ? "end" : "via";
    const badge = document.createElement("span");
    badge.className = "stop-row__badge";
    badge.textContent = stopLetter(index);
    const field = document.createElement("span");
    field.className = "stop-row__field";
    const input = document.createElement("input");
    input.type = "text";
    input.autocomplete = "off";
    input.spellcheck = false;
    input.placeholder = index === 0
      ? "Start — place, address, or map click"
      : index === stops.length - 1 ? "Destination" : `Stop ${stopLetter(index)}`;
    input.setAttribute("aria-label", `Route point ${stopLetter(index)}`);
    input.value = stop.text;
    const tools = document.createElement("span");
    tools.className = "stop-row__tools";
    const pick = document.createElement("button");
    pick.type = "button";
    pick.className = "stop-pick";
    pick.title = `Pick point ${stopLetter(index)} on the map`;
    pick.append(document.createElement("i"));
    tools.append(pick);
    if (stops.length > 2) {
      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "stop-remove";
      remove.title = "Remove this stop";
      remove.textContent = "×";
      remove.addEventListener("click", () => {
        stops.splice(index, 1);
        renderStops();
        updateStopMarkers();
        maybeRoute();
      });
      tools.append(remove);
    }
    const suggest = document.createElement("ul");
    suggest.className = "stop-suggest";
    suggest.setAttribute("role", "listbox");
    suggest.hidden = true;
    field.append(input, tools, suggest);
    const note = document.createElement("small");
    note.className = "stop-row__note";
    note.hidden = true;
    row.append(badge, field, note);

    stop.row = row;
    stop.input = input;
    stop.note = note;
    stop.pickButton = pick;
    stop.suggestEl = suggest;
    stop.suggestActive = -1;
    stop.suggestItems = [];
    if (stop.noteText) setStopNote(stop, stop.noteText, stop.noteTone || "");

    let debounce = null;
    input.addEventListener("input", () => {
      clearTimeout(debounce);
      debounce = setTimeout(() => requestStopSuggest(index), SUGGEST_DEBOUNCE_MS);
    });
    input.addEventListener("keydown", event => {
      const items = stop.suggestItems || [];
      if (event.key === "ArrowDown" && items.length) {
        event.preventDefault();
        stop.suggestActive = (stop.suggestActive + 1) % items.length;
        highlightStopSuggest(stop);
        return;
      }
      if (event.key === "ArrowUp" && items.length) {
        event.preventDefault();
        stop.suggestActive = (stop.suggestActive - 1 + items.length) % items.length;
        highlightStopSuggest(stop);
        return;
      }
      if (event.key === "Enter") {
        event.preventDefault();
        if (stop.suggestActive >= 0 && items[stop.suggestActive]) {
          chooseStopSuggestion(index, items[stop.suggestActive]);
        } else {
          hideStopSuggest(stop);
          resolveStopText(index, input.value);
        }
        return;
      }
      if (event.key === "Escape") {
        event.stopPropagation();
        if (!stop.suggestEl.hidden) hideStopSuggest(stop);
        else input.blur();
      }
    });
    input.addEventListener("blur", () => setTimeout(() => hideStopSuggest(stop), 150));
    pick.addEventListener("click", () => setStopPick(index));
    return row;
  }));
  addStopButton.disabled = stops.length >= MAX_STOPS;
  updateTripEndRow();
  updateDirectionsHud();
}

function highlightStopSuggest(stop) {
  const options = [...stop.suggestEl.children];
  for (const [optionIndex, option] of options.entries()) {
    option.classList.toggle("active", optionIndex === stop.suggestActive);
    if (optionIndex === stop.suggestActive) option.scrollIntoView({ block: "nearest" });
  }
}

// --- Route rendering -------------------------------------------------------

// The map style may still be loading when the first draw happens (slow
// raster tiles); addSource/addLayer throw until it settles. All route
// drawing funnels through setRouteFeatures, which queues the latest feature
// set until the map is ready and never lets a style race break data flow.
let pendingRouteFeatures = null;
let routeLayersWaiting = false;

// Recognizable road-sign icons drawn on canvas (no image assets): a
// three-lamp traffic light, a STOP octagon, and a level-crossing diamond.
function addJunctionIcons() {
  const scale = 2;
  const size = 24 * scale;
  const draw = (paint) => {
    const canvas = document.createElement("canvas");
    canvas.width = size;
    canvas.height = size;
    const ctx = canvas.getContext("2d");
    paint(ctx);
    return ctx.getImageData(0, 0, size, size);
  };
  const register = (name, image) => {
    if (!map.hasImage(name)) map.addImage(name, image, { pixelRatio: scale });
  };
  register("nav-junction-1", draw(ctx => {
    const w = 11 * scale;
    const h = 20 * scale;
    const x = (size - w) / 2;
    const y = (size - h) / 2;
    ctx.fillStyle = "#262a31";
    ctx.strokeStyle = "#faf9f2";
    ctx.lineWidth = 1.6 * scale;
    ctx.beginPath();
    ctx.roundRect(x, y, w, h, 3 * scale);
    ctx.fill();
    ctx.stroke();
    const lampR = 2.6 * scale;
    const colors = ["#e5484d", "#f2a60d", "#46a758"];
    colors.forEach((color, index) => {
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.arc(size / 2, y + (4.2 + index * 5.8) * scale, lampR, 0, Math.PI * 2);
      ctx.fill();
    });
  }));
  register("nav-junction-2", draw(ctx => {
    const r = 10 * scale;
    const cx = size / 2;
    const cy = size / 2;
    ctx.fillStyle = "#d23b2f";
    ctx.strokeStyle = "#faf9f2";
    ctx.lineWidth = 1.8 * scale;
    ctx.beginPath();
    for (let i = 0; i < 8; i++) {
      const angle = (Math.PI / 8) + (i * Math.PI) / 4;
      const px = cx + r * Math.cos(angle);
      const py = cy + r * Math.sin(angle);
      if (i === 0) ctx.moveTo(px, py);
      else ctx.lineTo(px, py);
    }
    ctx.closePath();
    ctx.fill();
    ctx.stroke();
    ctx.fillStyle = "#ffffff";
    ctx.font = `700 ${5.6 * scale}px system-ui, sans-serif`;
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText("STOP", cx, cy + 0.5 * scale);
  }));
  register("nav-junction-4", draw(ctx => {
    const r = 10 * scale;
    const cx = size / 2;
    const cy = size / 2;
    ctx.fillStyle = "#f5c518";
    ctx.strokeStyle = "#262a31";
    ctx.lineWidth = 1.6 * scale;
    ctx.beginPath();
    ctx.moveTo(cx, cy - r);
    ctx.lineTo(cx + r, cy);
    ctx.lineTo(cx, cy + r);
    ctx.lineTo(cx - r, cy);
    ctx.closePath();
    ctx.fill();
    ctx.stroke();
    ctx.strokeStyle = "#262a31";
    ctx.lineWidth = 2 * scale;
    ctx.beginPath();
    ctx.moveTo(cx - 4 * scale, cy - 4 * scale);
    ctx.lineTo(cx + 4 * scale, cy + 4 * scale);
    ctx.moveTo(cx + 4 * scale, cy - 4 * scale);
    ctx.lineTo(cx - 4 * scale, cy + 4 * scale);
    ctx.stroke();
  }));
}

function createRouteLayers() {
  map.addSource("routeLines", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
  map.addLayer({
    id: "route-alt",
    type: "line",
    source: "routeLines",
    filter: ["==", ["get", "kind"], "alt"],
    layout: { "line-join": "round" },
    paint: {
      "line-color": ROUTE_COLORS.alt,
      "line-width": 4,
      "line-opacity": 0.8,
      "line-dasharray": [1.8, 1.6]
    }
  });
  map.addLayer({
    id: "route-casing",
    type: "line",
    source: "routeLines",
    filter: ["match", ["get", "kind"], ["active", "leg"], true, false],
    layout: { "line-cap": "round", "line-join": "round" },
    paint: { "line-color": ROUTE_COLORS.casing, "line-width": 8, "line-opacity": 0.85 }
  });
  map.addLayer({
    id: "route-line",
    type: "line",
    source: "routeLines",
    filter: ["match", ["get", "kind"], ["active", "leg"], true, false],
    layout: { "line-cap": "round", "line-join": "round" },
    paint: { "line-color": ["get", "color"], "line-width": 4.5, "line-opacity": 0.96 }
  });
  map.addLayer({
    id: "route-traveled",
    type: "line",
    source: "routeLines",
    filter: ["==", ["get", "kind"], "traveled"],
    layout: { "line-cap": "round", "line-join": "round" },
    paint: { "line-color": "#8d939c", "line-width": 4.5, "line-opacity": 0.9 }
  });
  addJunctionIcons();
  map.addLayer({
    id: "route-junctions",
    type: "symbol",
    source: "routeLines",
    filter: ["==", ["get", "kind"], "junction"],
    minzoom: 12,
    layout: {
      "icon-image": ["concat", "nav-junction-", ["to-string", ["get", "jk"]]],
      "icon-size": ["interpolate", ["linear"], ["zoom"], 12, 0.55, 15, 0.8, 17.5, 1],
      "icon-allow-overlap": true,
      "icon-ignore-placement": true
    }
  });
  map.addLayer({
    id: "route-names",
    type: "symbol",
    source: "routeLines",
    filter: ["==", ["get", "kind"], "roadname"],
    minzoom: 12.5,
    layout: {
      "symbol-placement": "line",
      "text-field": ["get", "name"],
      "text-font": ["Open Sans Semibold"],
      "text-size": 11.5,
      "text-max-angle": 30,
      "symbol-spacing": 320
    },
    paint: {
      "text-color": "#243040",
      "text-halo-color": "rgba(250, 249, 242, 0.92)",
      "text-halo-width": 1.6
    }
  });
  map.addLayer({
    id: "route-coarse",
    type: "line",
    source: "routeLines",
    filter: ["==", ["get", "kind"], "coarse"],
    layout: { "line-join": "round" },
    paint: {
      "line-color": ROUTE_COLORS.active,
      "line-width": 3.5,
      "line-opacity": 0.65,
      "line-dasharray": [0.8, 1.6]
    }
  });
  map.addLayer({
    id: "route-snap-line",
    type: "line",
    source: "routeLines",
    filter: ["==", ["get", "kind"], "snapline"],
    paint: { "line-color": "#3f444d", "line-width": 2, "line-dasharray": [1, 1.6] }
  });
  map.addLayer({
    id: "route-snap-dot",
    type: "circle",
    source: "routeLines",
    filter: ["==", ["get", "kind"], "snapdot"],
    paint: {
      "circle-radius": 4,
      "circle-color": "#faf9f2",
      "circle-stroke-width": 2,
      "circle-stroke-color": ROUTE_COLORS.casing
    }
  });
  map.on("click", "route-alt", event => {
    const candidate = Number(event.features?.[0]?.properties?.candidate);
    if (Number.isFinite(candidate)) promoteCandidate(candidate);
  });
  map.on("mouseenter", "route-alt", () => { map.getCanvas().style.cursor = "pointer"; });
  map.on("mouseleave", "route-alt", () => { map.getCanvas().style.cursor = ""; });
}

function setRouteFeatures(features) {
  pendingRouteFeatures = features;
  if (!map.getSource("routeLines")) {
    let created = false;
    if (map.isStyleLoaded()) {
      try {
        createRouteLayers();
        created = true;
      } catch {
        // fall through to the retry below
      }
    }
    if (!created) {
      // Style still settling (slow raster tiles): retry until it accepts
      // the source, drawing whatever the latest pending feature set is.
      if (!routeLayersWaiting) {
        routeLayersWaiting = true;
        setTimeout(() => {
          routeLayersWaiting = false;
          if (pendingRouteFeatures) setRouteFeatures(pendingRouteFeatures);
        }, 400);
      }
      return;
    }
  }
  map.getSource("routeLines")?.setData({ type: "FeatureCollection", features });
}

function lineFeature(geometry, properties) {
  return {
    type: "Feature",
    properties,
    geometry: { type: "LineString", coordinates: geometry.map(([lat, lon]) => [lon, lat]) }
  };
}

function snapFeatures() {
  const features = [];
  for (const stop of stops) {
    if (!stop.place || !stop.snap) continue;
    features.push({
      type: "Feature",
      properties: { kind: "snapdot" },
      geometry: { type: "Point", coordinates: [stop.snap.lon, stop.snap.lat] }
    });
    if (stop.snap.meters >= SNAP_NOTE_MIN_METERS) {
      features.push({
        type: "Feature",
        properties: { kind: "snapline" },
        geometry: {
          type: "LineString",
          coordinates: [[stop.place.lon, stop.place.lat], [stop.snap.lon, stop.snap.lat]]
        }
      });
    }
  }
  return features;
}

function drawRoutePlan() {
  const features = [];
  if (routePlan?.kind === "pair") {
    for (const [index, candidate] of routePlan.candidates.entries()) {
      if (index === routePlan.active || !candidate.geometry) continue;
      features.push(lineFeature(candidate.geometry, { kind: "alt", candidate: index }));
    }
    const active = routePlan.candidates[routePlan.active];
    if (active?.geometry) {
      features.push(lineFeature(active.geometry, { kind: "active", color: ROUTE_COLORS.active }));
      features.push(...roadNameFeatures(active));
      features.push(...junctionFeatures(active.junctions));
    }
  } else if (routePlan?.kind === "trip") {
    for (const [index, leg] of routePlan.trip.legs.entries()) {
      if (!leg.geometry) continue;
      features.push(lineFeature(leg.geometry, {
        kind: "leg",
        color: index % 2 ? ROUTE_COLORS.legAlt : ROUTE_COLORS.active
      }));
      features.push(...roadNameFeatures(leg));
      features.push(...junctionFeatures(leg.junctions));
    }
  }
  features.push(...snapFeatures());
  setRouteFeatures(features);
}

function clearRouteLayers() {
  setRouteFeatures([]);
}

function clearStopMarkers() {
  for (const marker of stopMarkers) marker.remove();
  stopMarkers = [];
}

function updateStopMarkers() {
  clearStopMarkers();
  const visitOrder = routePlan?.kind === "trip" ? routePlan.trip.order : null;
  for (const [index, stop] of stops.entries()) {
    if (!stop.place) continue;
    const element = document.createElement("div");
    element.className = "stop-marker";
    element.dataset.role = index === 0 ? "origin" : index === stops.length - 1 ? "end" : "via";
    const visit = visitOrder ? visitOrder.indexOf(index) : -1;
    element.textContent = visit >= 0 ? String(visit + 1) : stopLetter(index);
    element.title = stop.place.label || stopLetter(index);
    stopMarkers.push(new maplibregl.Marker({ element, anchor: "center" })
      .setLngLat([stop.place.lon, stop.place.lat])
      .addTo(map));
  }
  if (directionsMode && routeAvailable) drawRoutePlanIfAny();
}

function drawRoutePlanIfAny() {
  if (routePlan) drawRoutePlan();
  else setRouteFeatures(snapFeatures());
}

function activeRouteGeometry() {
  if (routePlan?.kind === "pair") return routePlan.candidates[routePlan.active]?.geometry || null;
  if (routePlan?.kind === "trip") {
    const merged = [];
    for (const leg of routePlan.trip.legs) {
      for (const point of leg.geometry || []) {
        const last = merged[merged.length - 1];
        if (!last || last[0] !== point[0] || last[1] !== point[1]) merged.push(point);
      }
    }
    return merged.length ? merged : null;
  }
  return null;
}

function fitRoutePlan() {
  const bounds = new maplibregl.LngLatBounds();
  const extendGeometry = geometry => {
    for (const [lat, lon] of geometry || []) bounds.extend([lon, lat]);
  };
  if (routePlan?.kind === "pair") for (const candidate of routePlan.candidates) extendGeometry(candidate.geometry);
  else if (routePlan?.kind === "trip") for (const leg of routePlan.trip.legs) extendGeometry(leg.geometry);
  for (const stop of stops) if (stop.place) bounds.extend([stop.place.lon, stop.place.lat]);
  if (!bounds.isEmpty()) safeFitBounds(bounds, { padding: resultFitPadding(), maxZoom: 15, duration: 700 });
}

// --- Route computation -----------------------------------------------------

function departureParams() {
  const live = pulseMesh?.provider() ?? null;
  // Every route call in this file spreads departureParams(), so attaching
  // the live provider here reaches point-to-point routes, alternatives,
  // itineraries and navigation reroutes alike. When the mesh is off, or
  // has no data, this is simply absent and the router runs static — which
  // is the whole degradation contract.
  const liveParams = live ? { live } : {};
  if (routeBucketNames.length < 2) return liveParams;
  if (departureChoice === "peak") return { ...liveParams, bucket: routeBucketNames[1] };
  if (departureChoice === "base") return { ...liveParams, bucket: routeBucketNames[0] };
  return { ...liveParams, departureTime: new Date() };
}

// --- PulseMesh -------------------------------------------------------------
//
// Live traffic with no traffic server: peers gossip 42-byte contributions,
// every subscriber recomputes the same weighted-median aggregate, and the
// router consumes it through the ordinary LiveTrafficProvider contract.
// See docs/pulsemesh.md.
//
// Three things are drawn from it, and they are deliberately different
// claims. The traffic layer is a measurement — an aggregate of independent
// observations the router itself acts on. An incident is a *claim*, shown
// as a hint until enough distinct peers corroborate it. A thread is one
// vehicle that authorized you specifically to watch it. The UI never
// blurs the three.

const meshCard = document.querySelector("#meshCard");
const meshToggle = document.querySelector("#meshToggle");
const meshModePill = document.querySelector("#meshModePill");
const meshBody = document.querySelector("#meshBody");
const meshStats = document.querySelector("#meshStats");
const meshNote = document.querySelector("#meshNote");
const meshContribute = document.querySelector("#meshContribute");
const meshSimulate = document.querySelector("#meshSimulate");
const meshReportRow = document.querySelector("#meshReportRow");
const meshIncidentList = document.querySelector("#meshIncidentList");
const meshShareButton = document.querySelector("#meshShareButton");
const meshShareOut = document.querySelector("#meshShareOut");
const meshModeFine = document.querySelector("#meshModeFine");
const meshModeCoarse = document.querySelector("#meshModeCoarse");
const meshFollowInput = document.querySelector("#meshFollowInput");
const meshFollowButton = document.querySelector("#meshFollowButton");
const meshFollowOut = document.querySelector("#meshFollowOut");
const meshFollowCard = document.querySelector("#meshFollowCard");
const meshFileButton = document.querySelector("#meshFileButton");
const meshFileInput = document.querySelector("#meshFileInput");
const meshDriveRow = document.querySelector("#meshDriveRow");
const meshTravelCar = document.querySelector("#meshTravelCar");
const meshTravelBike = document.querySelector("#meshTravelBike");
const meshTravelFoot = document.querySelector("#meshTravelFoot");
const meshJobButton = document.querySelector("#meshJobButton");
const meshOfferButton = document.querySelector("#meshOfferButton");
const meshOfferOut = document.querySelector("#meshOfferOut");
const meshOfferPay = document.querySelector("#meshOfferPay");
const meshOfferLabel = document.querySelector("#meshOfferLabel");
const meshOfferCurrency = document.querySelector("#meshOfferCurrency");
const meshJobStops = document.querySelector("#meshJobStops");
const meshJobOut = document.querySelector("#meshJobOut");
const meshJobGate = document.querySelector("#meshJobGate");
const meshHandoverButton = document.querySelector("#meshHandoverButton");
const meshDeviceName = document.querySelector("#meshDeviceName");
const meshDeviceOut = document.querySelector("#meshDeviceOut");
const meshDeviceList = document.querySelector("#meshDeviceList");
const meshEnrolInput = document.querySelector("#meshEnrolInput");
const meshEnrolButton = document.querySelector("#meshEnrolButton");
const meshEnrolSelfButton = document.querySelector("#meshEnrolSelfButton");
const meshSeedInput = document.querySelector("#meshSeedInput");
const meshSeedButton = document.querySelector("#meshSeedButton");
const meshSeedOut = document.querySelector("#meshSeedOut");
const meshRecipientRow = document.querySelector("#meshRecipientRow");
const meshAcceptCard = document.querySelector("#meshAcceptCard");
const routeLiveNote = document.querySelector("#routeLiveNote");

const MESH_REFRESH_MS = 2000;
// Reporting is public and locates you. §10.4 will not mint a report
// without the caller asserting it said so, and this is where it is said.
const MESH_DISCLOSURE = "Reports are public and carry the spot you are at right now. Continue?";
const MESH_LEVEL_COLORS = {
  stopped: "#c0392b",
  heavy: "#e07b39",
  slow: "#e5b93c",
  free: "#3f9d6a",
  unknown: "#8d939c"
};
const MESH_REPORTABLE = [
  { type: 1, label: "Crash" },
  { type: 3, label: "Closure" },
  { type: 6, label: "Road works" },
  { type: 5, label: "Police" },
  { type: 8, label: "Object on road" }
];

let pulseMesh = null;
let meshTimer = null;
let meshFollowTimer = null;
let meshFollow = null;
let meshRun = null;
// §11: what a shared link is worth if it leaks. Fine is a live locator;
// coarse withholds position entirely and publishes stop events only.
let meshShareFine = true;
// The plan a followed run is measured against — for a private drive that
// is the one stop the publisher put in the link's plan.
let meshFollowPlan = null;
let meshLastTraffic = [];
let meshLastIncidents = [];
let meshRouteAt = 0;
// §20: the ticket this tab issued or accepted, and the one it has been
// offered but not taken. They are different states and the card shows
// different things for each.
let meshJob = null;
let meshPendingTicket = null;
// A ticket's mode byte, from §20.3: 1 coarse, 2 fine.
const THREAD_MODE_FINE = 2;
// The plan's label field is capped at 48 UTF-8 bytes (§20.2); stay under it.
const JOB_LABEL_BYTES = 40;

// §5.2 travelMode: how the vehicle moves, which is a different question
// from §11's `mode` — one is what the link reveals, the other is what
// graph the follower's ETA should be computed on.
const TRAVEL_MODE = { CAR: 1, BIKE: 2, FOOT: 3 };
const TRAVEL_MODE_LABELS = { 0: "unspecified", 1: "by car", 2: "by bike", 3: "on foot" };
let meshTravelMode = TRAVEL_MODE.CAR;

// §5.2 outcomes. Pending is the absence of an assertion, never a claim
// that the vehicle has not arrived.
const STOP_OUTCOME = { PENDING: 0, DELIVERED: 1, SKIPPED: 2, FAILED: 3 };
const STOP_OUTCOME_LABELS = { 1: "delivered", 2: "skipped", 3: "failed" };
// Two labels per reason on purpose: the driver picks from an instruction
// ("Other — say why") and the follower reads a statement ("no reason
// given"). Reusing one string makes one of the two read as nonsense.
const STOP_REASONS = [
  { code: 1, label: "Customer absent", said: "customer absent" },
  { code: 2, label: "Refused", said: "refused" },
  { code: 3, label: "Could not get in", said: "could not get in" },
  { code: 4, label: "Parcel missing or damaged", said: "parcel missing or damaged" },
  { code: 5, label: "Other — say why", said: "no reason given" }
];
const STOP_REASON_LABELS = Object.fromEntries(STOP_REASONS.map(r => [r.code, r.said]));
const STOP_REASON_OTHER = 5;

/**
 * The reason clause a follower reads, or "" for none.
 *
 * Code 5 with a note attached says nothing on its own — the note *is* the
 * reason — so it is dropped rather than printed as "no reason given"
 * immediately before the reason.
 */
function stopReasonClause(reasonCode, hasNote) {
  if (!reasonCode) return "";
  if (reasonCode === STOP_REASON_OTHER && hasNote) return "";
  return ` — ${STOP_REASON_LABELS[reasonCode] || "no reason given"}`;
}
// Which stop the driver has the skip sheet open for, if any.
let meshSkipFor = null;
// This browser's device identity (§20.9), once the controller has minted
// it: `{ publicKeyHex, fingerprint, name, cardUrl }`. Held so the card
// tile is not rebuilt — and the QR re-encoded — on every two-second tick.
let meshDeviceIdentity = null;
// The enrolled device the next job will be sealed to, by public key hex.
// There is no default: choosing who a job is addressed to is the decision
// §20.9 exists to make explicit.
let meshRecipient = null;
// A device card someone pasted or opened, waiting to be enrolled.
let meshPendingCard = null;
// Per-stop delivery metadata the dispatcher typed, keyed by rounded
// coordinate rather than by list position: details belong to the *stop*,
// so reordering the round or optimizing the itinerary must carry them
// along rather than leave a phone number attached to a slot.
const meshStopDetails = new Map();
// The stop set the details editor is currently showing. Re-rendering it
// on every mesh tick would collapse an open panel and wipe half-typed
// text out from under the dispatcher, so it is rebuilt only when the
// round itself changes.
let meshStopDetailsKey = "";
// The delivery photo currently on the follower card: its commitment and
// the object URL rendering it. Revoked on replace — a blob URL that is
// never released keeps the decoded image alive for the life of the tab.
let meshPhotoShown = { hash: null, url: null };

/**
 * The longest edge a delivery photo is downscaled to, and the JPEG
 * quality it is re-encoded at (threads §20.7).
 *
 * The re-encode is not only about size. A canvas `toBlob` writes a fresh
 * JPEG from pixels, so **every EXIF tag is dropped** — including the GPS
 * fix a phone camera stamps into the file, which would otherwise publish
 * the customer's front door to anyone who can open the photo, out of
 * band and past every §11 control on the run. `markStop` cannot do this
 * for us: it has no image decoder, and stripping metadata is stated in
 * the spec as the host's obligation.
 */
const PHOTO_MAX_EDGE = 1024;
const PHOTO_QUALITY = 0.6;
// §20.7's cap, minus the seal's own 28 bytes.
const PHOTO_MAX_SEALED = 131072;

/**
 * A camera file to bytes the mesh will carry: downscaled, re-encoded,
 * metadata gone. Rejects rather than silently sending something too
 * large — a driver is owed the sentence, not a failed mark.
 */
async function compressPhoto(file) {
  const bitmap = await createImageBitmap(file);
  const scale = Math.min(1, PHOTO_MAX_EDGE / Math.max(bitmap.width, bitmap.height));
  const canvas = document.createElement("canvas");
  canvas.width = Math.max(1, Math.round(bitmap.width * scale));
  canvas.height = Math.max(1, Math.round(bitmap.height * scale));
  canvas.getContext("2d").drawImage(bitmap, 0, 0, canvas.width, canvas.height);
  bitmap.close?.();
  const blob = await new Promise(resolve => canvas.toBlob(resolve, "image/jpeg", PHOTO_QUALITY));
  if (!blob) throw new Error("this browser could not re-encode that image");
  const bytes = new Uint8Array(await blob.arrayBuffer());
  if (bytes.length + 28 > PHOTO_MAX_SEALED) {
    throw new Error(`that photo is ${Math.round(bytes.length / 1024)} KB even after downscaling`);
  }
  return bytes;
}

function meshRequested() {
  return new URLSearchParams(location.search).get("keeper");
}

async function togglePulseMesh(on) {
  if (!routeEngine) return null;
  if (!on) {
    if (meshTimer) { clearInterval(meshTimer); meshTimer = null; }
    if (pulseMesh) await pulseMesh.stop();
    pulseMesh = null;
    meshFollow = null;
    meshRun = null;
    meshJob = null;
    meshDeviceIdentity = null;
    meshDeviceCardKey = "";
    meshRecipient = null;
    meshLastTraffic = [];
    meshLastIncidents = [];
    if (meshJobOut) { meshJobOut.hidden = true; meshJobOut.replaceChildren(); }
    if (meshOfferOut) { meshOfferOut.hidden = true; meshOfferOut.replaceChildren(); }
    dismissJobOffer();
    setMeshFeatures([]);
    renderMeshCard();
    return null;
  }
  if (!pulseMesh) {
    pulseMesh = createPulseMeshDemo({
      engine: routeEngine,
      // A keeper multiaddr in the URL joins the real mesh; without one the
      // demo runs peers inside this tab so it works on any static host.
      mode: meshRequested() ? "mesh" : "local",
      keeperAddress: meshRequested(),
      onChange: () => { refreshMeshDisplay().catch(() => {}); }
    });
    await pulseMesh.start();
    // Minted before anything is rendered: this browser is a device, and
    // the card it shows has to exist before a dispatcher can enrol it.
    await refreshDeviceIdentity();
    await meshFollowActiveRoute();
    if (pulseMesh.mode === "local") pulseMesh.startSimulation();
    meshTimer = setInterval(() => {
      refreshMeshDisplay().catch(() => {});
      pulseMesh?.tickThreads();
    }, MESH_REFRESH_MS);
  }
  renderMeshCard();
  return pulseMesh;
}

/** Hands the mesh whatever routes are on screen, so it scopes to them. */
async function meshFollowActiveRoute() {
  if (!pulseMesh) return;
  const routes = [];
  if (routePlan?.kind === "pair") routes.push(...routePlan.candidates);
  else if (routePlan?.kind === "trip") routes.push(...routePlan.trip.legs);
  if (!routes.length) return;
  await pulseMesh.followRoutes(routes);
}

// --- Map layers -------------------------------------------------------------

let meshLayersWaiting = false;
let pendingMeshFeatures = null;
let meshLayerError = null;

// The international no-entry disc, canvas-drawn like the junction signs:
// a closure is a statement about the road itself, and a generic incident
// dot undersells the one incident type that means "you cannot pass".
function addMeshClosureIcon() {
  if (map.hasImage("mesh-closure")) return;
  const scale = 2;
  const size = 24 * scale;
  const canvas = document.createElement("canvas");
  canvas.width = size;
  canvas.height = size;
  const ctx = canvas.getContext("2d");
  const cx = size / 2;
  const cy = size / 2;
  const r = 10 * scale;
  ctx.fillStyle = "#d23b2f";
  ctx.strokeStyle = "#faf9f2";
  ctx.lineWidth = 1.8 * scale;
  ctx.beginPath();
  ctx.arc(cx, cy, r, 0, Math.PI * 2);
  ctx.fill();
  ctx.stroke();
  ctx.fillStyle = "#ffffff";
  const barWidth = 12 * scale;
  const barHeight = 3.4 * scale;
  ctx.beginPath();
  ctx.roundRect(cx - barWidth / 2, cy - barHeight / 2, barWidth, barHeight, 1.2 * scale);
  ctx.fill();
  map.addImage("mesh-closure", ctx.getImageData(0, 0, size, size), { pixelRatio: scale });
}

function createMeshLayers() {
  addMeshClosureIcon();
  map.addSource("pulsemeshLive", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
  // Under the route line, over the basemap: the road is being described,
  // not replaced.
  const before = map.getLayer("route-alt") ? "route-alt" : undefined;
  map.addLayer({
    id: "mesh-traffic-casing",
    type: "line",
    source: "pulsemeshLive",
    filter: ["==", ["get", "kind"], "traffic"],
    layout: { "line-cap": "round", "line-join": "round" },
    paint: { "line-color": "#1b1e24", "line-width": 9, "line-opacity": 0.35 }
  }, before);
  map.addLayer({
    id: "mesh-traffic",
    type: "line",
    source: "pulsemeshLive",
    filter: ["==", ["get", "kind"], "traffic"],
    layout: { "line-cap": "round", "line-join": "round" },
    paint: {
      "line-color": ["get", "color"],
      "line-width": 6,
      // Confidence is drawn, not hidden: a two-report hint is visibly
      // fainter than a corroborated jam, because it is a weaker claim.
      "line-opacity": ["max", 0.35, ["to-number", ["get", "confidence"], 0]]
    }
  }, before);
  map.addLayer({
    id: "mesh-incident-dot",
    type: "circle",
    source: "pulsemeshLive",
    filter: ["all", ["==", ["get", "kind"], "incident"], ["!=", ["get", "closure"], true]],
    paint: {
      "circle-radius": ["case", ["==", ["get", "tier"], "shown"], 8, 6],
      "circle-color": ["case", ["==", ["get", "tier"], "shown"], "#c0392b", "#e5b93c"],
      "circle-stroke-width": 2,
      "circle-stroke-color": "#faf9f2",
      "circle-opacity": ["case", ["==", ["get", "tier"], "shown"], 0.95, 0.7]
    }
  });
  map.addLayer({
    id: "mesh-closure-icon",
    type: "symbol",
    source: "pulsemeshLive",
    filter: ["all", ["==", ["get", "kind"], "incident"], ["==", ["get", "closure"], true]],
    layout: {
      "icon-image": "mesh-closure",
      "icon-size": ["case", ["==", ["get", "tier"], "shown"], 0.95, 0.8],
      // A closure sign that collides with a label must still win: it is
      // the strongest statement the layer makes.
      "icon-allow-overlap": true
    },
    paint: {
      // Same confidence honesty as everything else: an uncorroborated
      // closure is visibly weaker than a shown one.
      "icon-opacity": ["case", ["==", ["get", "tier"], "shown"], 1, 0.8]
    }
  });
  map.addLayer({
    id: "mesh-incident-label",
    type: "symbol",
    source: "pulsemeshLive",
    filter: ["==", ["get", "kind"], "incident"],
    minzoom: 11,
    layout: {
      "text-field": ["get", "label"],
      "text-font": ["Open Sans Semibold"],
      "text-size": 11,
      "text-offset": [0, 1.2],
      "text-anchor": "top",
      "text-allow-overlap": false
    },
    paint: {
      "text-color": "#2a2118",
      "text-halo-color": "rgba(250, 249, 242, 0.94)",
      "text-halo-width": 1.5
    }
  });
  map.addLayer({
    id: "mesh-thread",
    type: "circle",
    source: "pulsemeshLive",
    filter: ["==", ["get", "kind"], "thread"],
    paint: {
      "circle-radius": 7,
      "circle-color": "#2f6df6",
      "circle-stroke-width": 3,
      "circle-stroke-color": "#faf9f2"
    }
  });
}

function setMeshFeatures(features) {
  pendingMeshFeatures = features;
  if (!map.getSource("pulsemeshLive")) {
    let created = false;
    // Attempted rather than gated on isStyleLoaded(): that flag goes false
    // again whenever sources are loading, so a periodic refresh that only
    // tries while it is true can miss every window it gets and leave the
    // overlay permanently invisible. addSource throws when the style is
    // not ready, which is a perfectly good gate on its own.
    try {
      createMeshLayers();
      created = true;
      meshLayerError = null;
    } catch (error) {
      // Kept rather than swallowed: a layer that never gets added is a
      // traffic overlay that never appears, and "nothing happened" is the
      // hardest bug there is to see.
      meshLayerError = String(error?.message || error);
    }
    if (!created) {
      if (!meshLayersWaiting) {
        meshLayersWaiting = true;
        setTimeout(() => {
          meshLayersWaiting = false;
          if (pendingMeshFeatures) setMeshFeatures(pendingMeshFeatures);
        }, 400);
      }
      return;
    }
  }
  map.getSource("pulsemeshLive")?.setData({ type: "FeatureCollection", features });
}

function meshFeatures(traffic, incidents, threadPoint) {
  const features = [];
  for (const entry of traffic) {
    features.push({
      type: "Feature",
      properties: {
        kind: "traffic",
        color: MESH_LEVEL_COLORS[entry.level] || MESH_LEVEL_COLORS.unknown,
        confidence: Number(entry.confidence?.toFixed(2) || 0),
        level: entry.level
      },
      geometry: { type: "LineString", coordinates: entry.points.map(([lat, lon]) => [lon, lat]) }
    });
  }
  for (const incident of incidents) {
    // §2.6 type 3: closures get the no-entry sign and plain words — the
    // map should say "road closed", not "closure report".
    const closure = incident.type === 3;
    const name = closure ? "Road closed" : incident.typeName;
    features.push({
      type: "Feature",
      properties: {
        kind: "incident",
        tier: incident.tier,
        closure,
        label: incident.tier === "shown" ? name : `${name} (unconfirmed)`
      },
      geometry: { type: "Point", coordinates: [incident.lon, incident.lat] }
    });
  }
  if (threadPoint) {
    features.push({
      type: "Feature",
      properties: { kind: "thread" },
      geometry: { type: "Point", coordinates: [threadPoint.lon, threadPoint.lat] }
    });
  }
  return features;
}

async function refreshMeshDisplay() {
  if (!pulseMesh) return;
  const [traffic, incidents] = await Promise.all([pulseMesh.traffic(), pulseMesh.incidents()]);
  meshLastTraffic = traffic;
  meshLastIncidents = incidents;
  const threadPoint = meshFollow ? await meshFollow.position().catch(() => null) : null;
  setMeshFeatures(meshFeatures(traffic, incidents, threadPoint));
  renderMeshCard();
  await maybeRerouteForTraffic();
}

/**
 * Re-runs the route when the live picture has moved, at most every
 * 20 seconds. The point of the demo is that a jam changes the answer, and
 * an answer computed before the jam existed cannot show that; recomputing
 * on every aggregation bucket would just burn byte ranges.
 */
async function maybeRerouteForTraffic() {
  if (!pulseMesh || !routePlan || nav) return;
  if (!meshLastTraffic.some(entry => entry.level === "stopped" || entry.level === "heavy")) return;
  if (Date.now() - meshRouteAt < 20000) return;
  meshRouteAt = Date.now();
  await maybeRoute();
}

// --- The panel --------------------------------------------------------------

function meshStatChip(label, value) {
  const chip = document.createElement("div");
  chip.className = "mesh-stat";
  const strong = document.createElement("strong");
  strong.textContent = String(value);
  const small = document.createElement("small");
  small.textContent = label;
  chip.append(strong, small);
  return chip;
}

function renderMeshCard() {
  if (!meshCard) return;
  meshCard.hidden = !routeEngine;
  const running = Boolean(pulseMesh);
  meshToggle.textContent = running ? "Turn off" : "Turn on live traffic";
  meshToggle.classList.toggle("active", running);
  meshBody.hidden = !running;
  if (!running) {
    meshModePill.textContent = "Off";
    meshModePill.dataset.mode = "off";
    return;
  }
  const snapshot = pulseMesh.snapshot();
  meshModePill.textContent = snapshot.mode === "mesh" ? "Live mesh" : "In-tab peers";
  meshModePill.dataset.mode = snapshot.mode || "off";

  meshStats.replaceChildren(
    meshStatChip("peers", snapshot.peers),
    meshStatChip("records", snapshot.records),
    meshStatChip("segments", snapshot.segments),
    meshStatChip("zones", snapshot.zones),
    meshStatChip("drawn", meshLastTraffic.length),
    meshStatChip("incidents", meshLastIncidents.length)
  );

  const notes = [];
  if (snapshot.error) notes.push(snapshot.error);
  if (snapshot.mode === "mesh") {
    notes.push("Read-only on the wire: this tab mints no admission bond, joins no gossip topic, and pulls what it shows over the padded sync path (§11.6).");
  } else {
    notes.push(`${snapshot.vehicles} simulated vehicles are driving your corridor inside this tab — real records, real validation, no server.`);
  }
  if (snapshot.contributing) {
    notes.push(`This tab is contributing under the reticent profile: ${snapshot.emitted} of ${snapshot.fixes} fixes published, the rest suppressed by the gates (${snapshot.lastReason || "—"}).`);
  }
  const dispatched = pulseMesh.hasActiveTicket;
  if (snapshot.sharing && dispatched) {
    notes.push("This run came from a dispatch ticket: the followers were given their link before this device took the job, and handing the ticket on keeps that link alive (§20).");
  }
  meshNote.textContent = notes.join(" ");

  if (meshShareButton) {
    meshShareButton.textContent = snapshot.sharing
      ? (dispatched ? "End this job" : "Stop sharing this drive")
      : "Create a tracking link";
  }
  if (meshJobButton) {
    const routed = Boolean(activeRouteGeometry());
    // While this device is publishing someone's job it has one; issuing a
    // second from the same tab would only be confusing. An empty roster
    // does *not* disable it: the button's job in that state is to explain
    // what enrolment is and where to do it (§20.9).
    meshJobButton.disabled = !routed || dispatched;
    meshJobButton.title = routed
      ? "Seal a dispatch ticket for the route on screen to an enrolled device"
      : "Plot a route first — a job ticket names where the vehicle is going";
  }
  if (meshOfferButton) {
    // An offer needs a round and nothing else — no roster, no recipient.
    // That is the point: it goes to couriers this dispatcher has not met.
    const routed = Boolean(activeRouteGeometry());
    meshOfferButton.disabled = !routed || dispatched;
    meshOfferButton.title = routed
      ? "Publish a signed advertisement of this round — no address in it, safe to post anywhere"
      : "Plot a route first — an offer describes a round";
  }
  if (meshJobGate) {
    const gate = dispatched ? null : jobGateReason();
    meshJobGate.hidden = !gate;
    meshJobGate.textContent = gate || "";
  }
  if (meshHandoverButton) meshHandoverButton.hidden = !dispatched;
  meshContribute.checked = snapshot.contributing;
  meshContribute.disabled = snapshot.readOnly;
  meshSimulate.hidden = snapshot.mode !== "local";
  meshSimulate.textContent = snapshot.simulating ? "Pause simulated traffic" : "Resume simulated traffic";

  renderMeshIncidents();
  renderDeviceCard();
  renderFleetSeed();
  renderDeviceRoster();
  renderRecipientRow();
  renderJobStopDetails();
  renderDriverStops();
  updateRouteLiveNote();
}

function renderMeshIncidents() {
  if (!meshIncidentList) return;
  meshIncidentList.replaceChildren();
  meshIncidentList.hidden = meshLastIncidents.length === 0;
  for (const incident of meshLastIncidents.slice(0, 6)) {
    const item = document.createElement("li");
    item.className = "mesh-incident";
    item.dataset.tier = incident.tier;
    const title = document.createElement("b");
    title.textContent = incident.typeName;
    const meta = document.createElement("small");
    meta.textContent = incident.tier === "shown"
      ? `${incident.sources} peers · ${incident.ageSeconds}s ago`
      : `unconfirmed · ${incident.sources} peer${incident.sources === 1 ? "" : "s"}`;
    item.append(title, meta);
    const actions = document.createElement("span");
    actions.className = "mesh-incident__actions";
    for (const [label, polarity] of [["Still there", 2], ["Gone", 3]]) {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = label;
      button.addEventListener("click", () => answerMeshIncident(incident, polarity));
      actions.append(button);
    }
    item.append(actions);
    item.addEventListener("click", event => {
      if (event.target.tagName === "BUTTON") return;
      map.easeTo({ center: [incident.lon, incident.lat], zoom: Math.max(map.getZoom(), 14) });
    });
    meshIncidentList.append(item);
  }
}

function updateRouteLiveNote() {
  if (!routeLiveNote) return;
  const active = routePlan?.kind === "pair" ? routePlan.candidates[routePlan.active] : null;
  const live = active?.live;
  if (!live || !Number.isFinite(active.adjustedSeconds)) {
    routeLiveNote.hidden = true;
    return;
  }
  const delta = Math.round((active.adjustedSeconds - active.seconds) / 60);
  routeLiveNote.hidden = false;
  routeLiveNote.dataset.tone = delta > 0 ? "slow" : "clear";
  // `applied` counts edges re-weighted, not states consumed: one observed
  // segment fans out to every edge sharing the leaf's canonical polyline,
  // so it routinely exceeds `states` and the two must not be phrased as a
  // fraction of each other.
  routeLiveNote.textContent = live.applied === 0
    ? `Live traffic on; nothing observed on this route yet (${live.states} live segments held).`
    : delta > 0
      ? `+${delta} min from live traffic · ${live.applied} edges re-weighted from ${live.states} observed segments`
      : `Live traffic agrees with the static metric here · ${live.applied} edges re-weighted`;
}

// --- Reporting --------------------------------------------------------------

function renderMeshReportRow() {
  if (!meshReportRow) return;
  meshReportRow.replaceChildren();
  for (const entry of MESH_REPORTABLE) {
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = entry.label;
    button.addEventListener("click", () => reportMeshIncident(entry));
    meshReportRow.append(button);
  }
}

async function reportMeshIncident(entry) {
  if (!pulseMesh) return;
  if (!window.confirm(MESH_DISCLOSURE)) return;
  const result = await pulseMesh.report({ type: entry.type, acknowledgedPublic: true });
  if (result.emitted) {
    showToast(`${entry.label} reported to the mesh`, "info");
    await refreshMeshDisplay();
    return;
  }
  showToast(
    result.reason === "no-position"
      ? "Reports come from where you are: start the demo drive, or share your location, first."
      : `Report declined by the protocol (${result.reason})`,
    "error"
  );
}

async function answerMeshIncident(incident, polarity) {
  if (!pulseMesh) return;
  if (!window.confirm(MESH_DISCLOSURE)) return;
  const result = await pulseMesh.answer({ key: incident.key, polarity, acknowledgedPublic: true });
  showToast(
    result.emitted
      ? "Answer published — §8.5 scores it against the other peers'"
      : `Answer declined (${result.reason})`,
    result.emitted ? "info" : "error"
  );
  await refreshMeshDisplay();
}

// --- Threads ----------------------------------------------------------------

async function shareThisDrive() {
  if (!pulseMesh) return;
  if (meshRun) {
    const dispatched = pulseMesh.hasActiveTicket;
    await pulseMesh.endDrive();
    meshRun = null;
    meshJob = null;
    meshShareOut.hidden = true;
    if (meshJobOut) { meshJobOut.hidden = true; meshJobOut.replaceChildren(); }
    renderMeshCard();
    showToast(dispatched ? "Job closed — the run is finished" : "Sharing stopped — the run is closed", "info");
    return;
  }
  const geometry = activeRouteGeometry();
  if (!geometry?.length) {
    showToast("Plot a route first — a thread follows a drive", "error");
    return;
  }
  const end = geometry[geometry.length - 1];
  const run = await pulseMesh.shareDrive({
    fine: meshShareFine,
    travelMode: meshTravelMode,
    plan: {
      stops: [{
        index: 1, lat: end[0], lon: end[1],
        label: planLabel(stops[stops.length - 1]?.place?.label || "Destination")
      }],
      dwellSeconds: 0,
      travelMode: meshTravelMode
    }
  });
  if (!run?.url) {
    showToast("Threads need WebCrypto's Ed25519 on this browser", "error");
    return;
  }
  meshRun = run;
  meshShareOut.hidden = false;
  meshShareOut.replaceChildren();
  const input = document.createElement("input");
  input.readOnly = true;
  input.value = run.url;
  input.addEventListener("focus", () => input.select());
  const note = document.createElement("small");
  note.textContent = meshShareFine
    ? "45 bytes in the fragment — the page host never sees it. Expires in 3 hours. Start the demo drive to publish."
    : "Stops only: this link carries no position at all. Expires in 3 hours.";
  meshShareOut.append(input, note);
  input.select();
  renderMeshCard();
}

/**
 * The driver's row: the stop in front of them, and the two things only
 * they can say about it.
 *
 * Everything else on this channel is inferred from movement, and rightly
 * so — where the van is, whether it is dwelling, how far it has got. What
 * happened at the door is not inferable: a van parked outside a house for
 * four minutes could have delivered, been refused, or found nobody in,
 * and those are three different sentences to send a customer. So this is
 * a button, it publishes immediately rather than on the next heartbeat,
 * and an unmarked stop stays blank on the wire rather than being guessed.
 */
function renderDriverStops() {
  if (!meshDriveRow) return;
  const driving = pulseMesh?.drivingSnapshot?.() ?? null;
  if (!driving) {
    meshDriveRow.replaceChildren();
    meshDriveRow.hidden = true;
    meshSkipFor = null;
    return;
  }

  // The stop in front of the driver: the first one nobody has spoken for.
  const pending = driving.stops.find(stop => !driving.outcomes[stop.index - 1]);
  const done = driving.outcomes.filter(Boolean).length;

  // The card re-renders on the mesh tick, and a driver halfway through
  // typing why a delivery failed must not have the sheet pulled out from
  // under them. While it is open for the stop it was opened for, it is
  // left exactly as it is — chosen reason, typed text, and cursor.
  if (meshSkipFor != null && meshSkipFor === pending?.index
    && meshDriveRow.querySelector(".mesh-drive__reason")) {
    return;
  }
  meshDriveRow.replaceChildren();
  meshDriveRow.hidden = false;

  const summary = document.createElement("small");
  summary.className = "mesh-drive__summary";
  summary.textContent = `${done} of ${driving.stops.length} stops marked · ${TRAVEL_MODE_LABELS[driving.travelMode] || "unspecified"}`;
  meshDriveRow.append(summary);

  if (!pending) {
    const finished = document.createElement("small");
    finished.textContent = "Every stop on this run has an outcome.";
    meshDriveRow.append(finished);
    return;
  }

  const current = document.createElement("b");
  current.className = "mesh-drive__stop";
  current.textContent = pending.label || `Stop ${pending.index}`;
  meshDriveRow.append(current);
  renderStopDetails(pending);

  if (meshSkipFor !== pending.index) {
    const actions = document.createElement("div");
    actions.className = "mesh-drive__actions";
    const delivered = document.createElement("button");
    delivered.type = "button";
    delivered.textContent = "Delivered";
    delivered.addEventListener("click", () => {
      markCurrentStop(pending.index, STOP_OUTCOME.DELIVERED).catch(() => {});
    });
    const skip = document.createElement("button");
    skip.type = "button";
    skip.textContent = "Skip…";
    skip.addEventListener("click", () => {
      meshSkipFor = pending.index;
      renderDriverStops();
    });

    // Proof of delivery (§20.7). Choosing a photo *is* marking delivered
    // — one action, because a driver holding a parcel at a door is not
    // going to press two buttons, and a photo without the mark says
    // nothing on the wire. The image never rides gossip: it is sealed
    // under a key only the dispatcher and this device can derive, and
    // the record carries a 32-byte commitment to it.
    const photoLabel = document.createElement("label");
    photoLabel.className = "mesh-drive__photo";
    photoLabel.textContent = "Add photo";
    const photoInput = document.createElement("input");
    photoInput.type = "file";
    photoInput.accept = "image/*";
    // Opens the camera directly on a phone rather than the gallery.
    photoInput.setAttribute("capture", "environment");
    photoInput.addEventListener("change", () => {
      const file = photoInput.files?.[0];
      photoInput.value = "";
      if (file) deliverWithPhoto(pending.index, file).catch(() => {});
    });
    photoLabel.append(photoInput);

    actions.append(delivered, photoLabel, skip);
    meshDriveRow.append(actions);
    return;
  }

  // The skip sheet. A reason code is five bits of structure the customer's
  // app can render in their own language; the free text is the escape
  // hatch and it costs 64 bytes on the wire, so it is capped there.
  const reason = document.createElement("select");
  reason.className = "mesh-drive__reason";
  reason.setAttribute("aria-label", "Why this stop was not made");
  for (const entry of STOP_REASONS) {
    const option = document.createElement("option");
    option.value = String(entry.code);
    option.textContent = entry.label;
    reason.append(option);
  }
  const detail = document.createElement("input");
  detail.type = "text";
  detail.className = "mesh-drive__detail";
  detail.placeholder = "Up to 64 characters, sent to the follower";
  detail.maxLength = 64;
  detail.hidden = true;
  reason.addEventListener("change", () => {
    detail.hidden = Number(reason.value) !== 5;
    if (!detail.hidden) detail.focus();
  });

  const actions = document.createElement("div");
  actions.className = "mesh-drive__actions";
  // Skipped and failed are different claims — "we did not go" against "we
  // went and it did not work" — and the customer is owed the right one.
  for (const [outcome, label] of [[STOP_OUTCOME.SKIPPED, "Mark skipped"], [STOP_OUTCOME.FAILED, "Mark failed"]]) {
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = label;
    button.addEventListener("click", () => {
      markCurrentStop(pending.index, outcome, {
        reason: Number(reason.value),
        note: detail.hidden ? null : detail.value.trim() || null
      }).catch(() => {});
    });
    actions.append(button);
  }
  const cancel = document.createElement("button");
  cancel.type = "button";
  cancel.className = "mesh-drive__cancel";
  cancel.textContent = "Cancel";
  cancel.addEventListener("click", () => { meshSkipFor = null; renderDriverStops(); });
  actions.append(cancel);

  meshDriveRow.append(reason, detail, actions);
}

/**
 * What the driver reads at the door (§20.8).
 *
 * The order reference and the parcel count go first and go large,
 * because they are what gets checked against the label on the box while
 * standing on a doorstep; the instruction is the sentence that decides
 * what to do when nobody answers; the contact is a `tel:` link so it is
 * one tap rather than a number to memorise and retype in a car.
 *
 * `parcels` is `null` when the dispatcher never said and a number when
 * they did — including **0**, which is a real instruction ("nothing to
 * carry": a collection, a survey, a signature). Rendering unstated as
 * "0 parcels" would invent a claim, so unstated renders as nothing.
 */
function renderStopDetails(stop) {
  if (!stop) return;
  const hasParcels = Number.isFinite(stop.parcels);
  if (stop.orderRef || hasParcels) {
    const line = document.createElement("div");
    line.className = "mesh-drive__ids";
    if (stop.orderRef) {
      const ref = document.createElement("b");
      ref.textContent = stop.orderRef;
      ref.className = "mesh-drive__ref";
      line.append(ref);
    }
    if (hasParcels) {
      const parcels = document.createElement("b");
      parcels.className = "mesh-drive__parcels";
      parcels.textContent = stop.parcels === 1 ? "1 parcel" : `${stop.parcels} parcels`;
      line.append(parcels);
    }
    meshDriveRow.append(line);
  }
  if (stop.instructions) {
    const instructions = document.createElement("p");
    instructions.className = "mesh-drive__instructions";
    instructions.textContent = stop.instructions;
    meshDriveRow.append(instructions);
  }
  if (stop.contact) {
    const contact = document.createElement("a");
    contact.className = "mesh-drive__contact";
    // The dispatcher typed this and the ticket's signature covers it, so
    // it is not attacker-controlled the way a search result is — but it
    // is still user text in a URL, and `tel:` takes no spaces.
    contact.href = `tel:${stop.contact.replace(/[^\d+]/gu, "")}`;
    contact.textContent = `Call ${stop.contact}`;
    contact.rel = "noreferrer";
    meshDriveRow.append(contact);
  }
}

/**
 * Delivered, with the photo. The downscale-and-re-encode happens here,
 * in the page, because stripping the camera's EXIF GPS is the host's
 * obligation and the library states it as a MUST (§20.7).
 */
async function deliverWithPhoto(index, file) {
  let photo;
  try {
    photo = await compressPhoto(file);
  } catch (error) {
    showToast(`That photo could not be attached: ${error?.message || error}`, "error");
    return;
  }
  await markCurrentStop(index, STOP_OUTCOME.DELIVERED, { photo });
}

async function markCurrentStop(index, outcome, options = {}) {
  if (!pulseMesh) return;
  try {
    await pulseMesh.markStop(index, outcome, options);
  } catch (error) {
    showToast(`That stop could not be marked: ${error?.message || error}`, "error");
    return;
  }
  meshSkipFor = null;
  showToast(`Stop ${index} marked ${STOP_OUTCOME_LABELS[outcome]} — published to the followers now`, "info");
  renderDriverStops();
}

/**
 * §11 is a harm decision, not a bandwidth one, so it is asked rather
 * than defaulted: coarse publishes stop events and a heartbeat with no
 * position at all, and a leaked coarse link is worth roughly what a
 * printed timetable is.
 */
function setShareMode(fine) {
  meshShareFine = fine;
  meshModeFine?.classList.toggle("active", fine);
  meshModeCoarse?.classList.toggle("active", !fine);
  meshModeFine?.setAttribute("aria-checked", String(fine));
  meshModeCoarse?.setAttribute("aria-checked", String(!fine));
}

/**
 * How the round is made. It goes into the *plan*, which means it is
 * hashed into `planRef` and therefore part of the job's identity — the
 * dispatcher who routed and priced a bike round decided this, not the
 * driver's phone, and not the follower's guess.
 */
function setTravelMode(value) {
  meshTravelMode = value;
  for (const [mode, button] of [
    [TRAVEL_MODE.CAR, meshTravelCar], [TRAVEL_MODE.BIKE, meshTravelBike], [TRAVEL_MODE.FOOT, meshTravelFoot]
  ]) {
    button?.classList.toggle("active", mode === value);
    button?.setAttribute("aria-checked", String(mode === value));
  }
}

// --- Dispatch tickets (§20) -------------------------------------------------
//
// A tracking link and a dispatch ticket both live in a URL fragment and
// both look like noise, and there the resemblance ends: one lets you
// watch a vehicle, the other lets you *be* it. Everything below exists to
// keep that difference visible — the driver link is labelled as a publish
// capability, the customer link as read-only, and an incoming ticket gets
// an accept card rather than being acted on the way a follow link is.

function planLabel(text) {
  const encoder = new TextEncoder();
  let value = String(text || "").trim();
  while (encoder.encode(value).length > JOB_LABEL_BYTES) value = value.slice(0, -1);
  return value;
}

function formatExpiry(unixSeconds) {
  return new Intl.DateTimeFormat(undefined, {
    day: "numeric", month: "short", hour: "numeric", minute: "2-digit"
  }).format(new Date(unixSeconds * 1000));
}

function jobModeLabel(mode) {
  return mode === THREAD_MODE_FINE ? "live position" : "stops only";
}

// A phone camera, not the encoder, is the real ceiling. Past about
// version 25 the modules are finer than a hand-held scan across a
// counter can resolve, so the policy is to refuse rather than to print a
// technically-valid symbol nobody can read — a full delivery day simply
// travels as a file instead.
const QR_MAX_VERSION = 25;

function meshQrTile(text, label = "Driver link") {
  const tile = document.createElement("div");
  tile.className = "mesh-qr";
  const qr = encodeQr(text, { ecLevel: "M", maxVersion: QR_MAX_VERSION });
  // The markup is generated by src/qr.js from module coordinates — the
  // payload reaches it as geometry, never as text — so there is nothing
  // here for a link to inject.
  tile.innerHTML = qrSvg(qr, { margin: 4 });
  tile.querySelector("svg")?.setAttribute("aria-label", `${label} as a QR code, version ${qr.version}`);
  return tile;
}

/**
 * The same signed bytes, as a file.
 *
 * One line, the driver URL — `wayfind://ticket#<base64url>`, which the
 * driver app registers — because text survives mail clients, messengers
 * and copy-paste, and because any human who opens it sees a tappable
 * link rather than a binary blob. Nothing about the trust model changes
 * with the carrier: a ticket file is a publish capability, and it
 * travels under the same rule the ticket does.
 */
function ticketFileName(jobIdHex) {
  const id = String(jobIdHex || "").slice(0, 12) || "unassigned";
  return `job-${id}.wayfindjob`;
}

function ticketFileUrl(ticketBase64) {
  return `wayfind://ticket#${ticketBase64}`;
}

function downloadTicketFile(ticketBase64, jobIdHex) {
  const blob = new Blob([`${ticketFileUrl(ticketBase64)}\n`], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = ticketFileName(jobIdHex);
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  // Revoked on the next turn: Safari reads the blob after the click
  // returns, so revoking synchronously hands it a dead URL.
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

function meshLinkField(caption, value, tone = "") {
  const wrap = document.createElement("label");
  wrap.className = "mesh-link";
  if (tone) wrap.dataset.tone = tone;
  const label = document.createElement("small");
  label.textContent = caption;
  const input = document.createElement("input");
  input.type = "text";
  input.readOnly = true;
  input.spellcheck = false;
  input.value = value;
  input.addEventListener("focus", () => input.select());
  wrap.append(label, input);
  return wrap;
}

// --- Devices (§20.9) --------------------------------------------------------
//
// A job is ciphertext addressed to a device, which means this browser is
// a device: it has a keypair, a name, a fingerprint and a card, and it
// shows all four. The same card is what a dispatcher scans to be able to
// send this browser a job, and what this browser shows a driver.
//
// One page plays both roles, and that is exactly where a demo can lie.
// It does not: there is no hidden "driver device". If you want to send
// this browser a job from this browser, you enrol its own card through
// the same paste-and-confirm path a second phone would use, and the
// roster says "this device" next to the row so the two keys acting are
// never confused for two devices.

/** The card tile, rebuilt only when the card itself changes. */
let meshDeviceCardKey = "";

async function refreshDeviceIdentity() {
  if (!pulseMesh) return null;
  meshDeviceIdentity = await pulseMesh.deviceIdentity().catch(() => null);
  return meshDeviceIdentity;
}

function renderDeviceCard() {
  if (!meshDeviceOut) return;
  const me = meshDeviceIdentity;
  if (!me) {
    meshDeviceOut.replaceChildren();
    meshDeviceCardKey = "";
    return;
  }
  // Never while it is being typed into: the mesh card re-renders every
  // two seconds and would otherwise reset the cursor mid-name.
  if (meshDeviceName && document.activeElement !== meshDeviceName) meshDeviceName.value = me.name;
  if (me.cardUrl === meshDeviceCardKey) return;
  meshDeviceCardKey = me.cardUrl;
  meshDeviceOut.replaceChildren();

  const identity = document.createElement("small");
  identity.className = "mesh-job__summary";
  identity.textContent = `${me.name} · fingerprint ${me.fingerprint}`;
  meshDeviceOut.append(identity);

  try {
    meshDeviceOut.append(meshQrTile(me.cardUrl, "This device's card"));
  } catch {
    // A card is 40-odd bytes; there is no plan in it and no version of
    // this that does not fit. Nothing to say if the encoder ever refuses.
  }
  const caption = document.createElement("small");
  caption.className = "mesh-job__caption";
  // The fingerprint's whole purpose, said where the fingerprint is.
  caption.textContent = "Point a dispatcher's camera here, then read the fingerprint aloud and check it "
    + "matches what their screen shows. The card is a public key: it says where a job can be sent, "
    + "and it opens nothing.";
  meshDeviceOut.append(caption);

  meshDeviceOut.append(meshLinkField("Device card link — send it however you like; it is not a secret.", me.cardUrl));

  const warning = document.createElement("p");
  warning.className = "mesh-job__warning";
  // The irreversible one. A browser has no Keystore, and a job sealed to
  // a key that no longer exists cannot be opened by anybody at all.
  warning.textContent = "This device's private key lives in this browser's localStorage — clearing site "
    + "data or using another browser destroys it. Jobs already sealed to it can then be opened by "
    + "nobody, including the dispatcher unless it sealed a copy to itself. Enrol again after a reset.";
  meshDeviceOut.append(warning);
}

// --- The fleet's seed (§20.10) ----------------------------------------------
//
// The one thing in this card that is a **location** rather than a
// capability. Everything else here grants something: a follow link
// grants watching, a job grants publishing, a device card grants being
// sent one. A seed address grants nothing at all — it says where a peer
// is, and every thread on the far side still needs its own key.
//
// Which is why it needs no fingerprint ceremony and no enrolment gate,
// and why the demo can accept one from a pasted link without asking
// anybody to compare digits: an address naming the wrong machine does
// not impersonate a seed, it fails to connect.

function renderFleetSeed() {
  if (!meshSeedOut || !pulseMesh) return;
  const seeds = pulseMesh.fleetSeeds();
  const remembered = pulseMesh.rememberedPeers();
  meshSeedOut.replaceChildren();

  const summary = document.createElement("small");
  summary.className = "mesh-job__summary";
  summary.textContent = seeds.length
    ? `${seeds.length} seed address${seeds.length === 1 ? "" : "es"} · every job this browser issues carries ${seeds.length === 1 ? "it" : "them"}, sealed`
    : "No seed — a job issued now carries no way to reach the mesh";
  meshSeedOut.append(summary);

  if (seeds.length) {
    const list = document.createElement("ol");
    list.className = "mesh-job__stops";
    for (const address of seeds) {
      const item = document.createElement("li");
      item.textContent = address;
      list.append(item);
    }
    meshSeedOut.append(list);
    const clear = document.createElement("button");
    clear.type = "button";
    clear.className = "mesh-secondary";
    clear.textContent = "Forget this seed";
    clear.addEventListener("click", () => {
      pulseMesh.setFleetSeeds("").then(() => {
        showToast("Seed forgotten — new jobs will carry no bootstrap address", "info");
        renderMeshCard();
      }).catch(() => {});
    });
    meshSeedOut.append(clear);
  }

  const peers = document.createElement("small");
  peers.className = "mesh-job__caption";
  // The point of remembering: the seed is a first-contact problem, and
  // once it has been solved once it should stay solved.
  peers.textContent = remembered.length
    ? `${remembered.length} peer${remembered.length === 1 ? "" : "s"} remembered from earlier sessions — `
      + "this browser dials those too on start, so the seed only has to be up the first time."
    : "No peers remembered yet. Once this browser has connected to the mesh once, it dials those "
      + "peers on the next start and the seed stops being a single point of failure for joining.";
  meshSeedOut.append(peers);
}

/**
 * Takes a seed from whatever arrived: a `wayfind://seed#…` card, a bare
 * fragment, or a multiaddr typed straight in. The card is the path a
 * dispatcher uses (scan the keeper's own terminal); the typed multiaddr
 * is the path an operator who already has the address uses.
 */
async function enrolFleetSeed(text) {
  if (!pulseMesh) return null;
  const value = String(text || "").trim();
  if (!value) return null;
  try {
    // A card is base64url, in a fragment or bare; anything else is
    // treated as typed addresses so the sentence a user gets for
    // "seed.depot.example:4001" is the codec's own — "a seed address is
    // a multiaddr and starts with /" — rather than base64's complaint
    // about a string that was never meant to be base64.
    const card = value.includes("#") || /^[A-Za-z0-9_-]+$/u.test(value);
    if (!card) {
      const seeds = await pulseMesh.setFleetSeeds(value);
      showToast(`Seed set — ${seeds.length} address${seeds.length === 1 ? "" : "es"}`, "info");
      renderMeshCard();
      return seeds;
    }
    const enrolled = await pulseMesh.enrolSeed(value);
    showToast(
      `${enrolled.label || "Seed"} enrolled — ${enrolled.addresses.length} address`
      + `${enrolled.addresses.length === 1 ? "" : "es"}. Jobs issued from here carry it, sealed.`,
      "info"
    );
    renderMeshCard();
    return enrolled.seeds;
  } catch (error) {
    showToast(`That is not a seed: ${error?.message || error}`, "error");
    return null;
  }
}

function renderDeviceRoster() {
  if (!meshDeviceList || !pulseMesh) return;
  const list = pulseMesh.roster();
  meshDeviceList.replaceChildren();
  if (!list.length) {
    const empty = document.createElement("li");
    empty.className = "mesh-device";
    const title = document.createElement("b");
    title.textContent = "No devices enrolled yet";
    const note = document.createElement("code");
    note.textContent = "no card, no job";
    empty.append(title, note);
    meshDeviceList.append(empty);
    return;
  }
  for (const entry of list) {
    const self = entry.publicKey === meshDeviceIdentity?.publicKeyHex;
    const item = document.createElement("li");
    item.className = "mesh-device";
    item.dataset.self = String(self);
    const name = document.createElement("b");
    name.textContent = entry.name || "Unnamed device";
    const fingerprint = document.createElement("code");
    fingerprint.textContent = entry.fingerprint;
    item.append(name, fingerprint);
    if (self) {
      const badge = document.createElement("small");
      badge.className = "mesh-device__self";
      badge.textContent = "This device — a job sealed here can be accepted in this tab";
      item.append(badge);
    }
    const remove = document.createElement("button");
    remove.type = "button";
    remove.textContent = "Remove";
    remove.addEventListener("click", () => {
      pulseMesh.removeDevice(entry.publicKey);
      if (meshRecipient === entry.publicKey) meshRecipient = null;
      showToast(`${entry.name} removed — no new job can be sealed to it`, "info");
      renderMeshCard();
    });
    item.append(remove);
    meshDeviceList.append(item);
  }
}

function renderRecipientRow() {
  if (!meshRecipientRow || !pulseMesh) return;
  const list = pulseMesh.roster();
  // A device that was removed cannot stay selected, or the next job would
  // be sealed to a key the dispatcher no longer claims to know.
  if (meshRecipient && !list.some(entry => entry.publicKey === meshRecipient)) meshRecipient = null;
  meshRecipientRow.replaceChildren();
  meshRecipientRow.hidden = !list.length;
  for (const entry of list) {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.role = "radio";
    chip.ariaChecked = String(meshRecipient === entry.publicKey);
    chip.className = meshRecipient === entry.publicKey ? "active" : "";
    chip.textContent = `${entry.name} · ${entry.fingerprint}`;
    chip.addEventListener("click", () => {
      meshRecipient = entry.publicKey;
      renderMeshCard();
    });
    meshRecipientRow.append(chip);
  }
}

/**
 * Enrols a card, from wherever it arrived: the enrol field, the follow
 * field, or a `wayfind://device#…` opened in the address bar.
 */
async function enrolDeviceCard(text) {
  if (!pulseMesh) return null;
  try {
    const entry = await pulseMesh.enrolDevice(text);
    await refreshDeviceIdentity();
    // Newly enrolled is almost always the device you are about to send
    // to, but selecting it is still a click the user sees happen.
    meshRecipient = entry.publicKey;
    showToast(
      entry.alreadyEnrolled
        ? `${entry.name} was already enrolled — fingerprint ${entry.fingerprint}`
        : `${entry.name} enrolled — check the fingerprint ${entry.fingerprint} against their screen`,
      "info"
    );
    renderMeshCard();
    return entry;
  } catch (error) {
    showToast(`That is not a device card: ${error?.message || error}`, "error");
    return null;
  }
}

/** A card that arrived on its own, shown before it is trusted. */
function showDeviceCardOffer(card, source) {
  if (!meshAcceptCard) return;
  meshPendingCard = source;
  meshFollowOut.hidden = true;
  meshAcceptCard.hidden = false;
  meshAcceptCard.replaceChildren();
  const title = document.createElement("b");
  title.textContent = "A device card was opened";
  const meta = document.createElement("small");
  meta.textContent = `${card.name || "Unnamed device"} · fingerprint ${card.fingerprint}`;
  const note = document.createElement("small");
  // Enrolling is not neutral: it is what makes this device a place jobs —
  // and the customer data in them — can be sent.
  note.textContent = "Enrolling adds it to this dispatcher's list, so jobs can be sealed to it. "
    + "Read the fingerprint aloud first and check it matches the other phone's screen.";
  const enrol = document.createElement("button");
  enrol.type = "button";
  enrol.textContent = "Enrol this device";
  enrol.addEventListener("click", () => {
    const pending = meshPendingCard;
    dismissJobOffer();
    enrolDeviceCard(pending).catch(() => {});
  });
  meshAcceptCard.append(title, meta, note, enrol);
}

/**
 * The issued ticket, or the same ticket again when it is being handed on.
 * Handover shows no customer link because the customer already has one —
 * that it survives the change of driver is the point of §20.
 */
function renderJobTicket({ handover = false } = {}) {
  if (!meshJobOut || !meshJob) return;
  meshJobOut.hidden = false;
  meshJobOut.replaceChildren();

  const ticketStopList = meshJob.stops || [];
  if (ticketStopList.length) {
    const summary = document.createElement("small");
    summary.className = "mesh-job__summary";
    summary.textContent = ticketStopList.length === 1
      ? "1 stop"
      : `${ticketStopList.length} stops · optimized order`;
    const list = document.createElement("ol");
    list.className = "mesh-job__stops";
    for (const [index, stop] of ticketStopList.entries()) {
      const item = document.createElement("li");
      item.textContent = stop.label || `Stop ${index + 1}`;
      list.append(item);
    }
    meshJobOut.append(summary, list);
  }

  // A full delivery day does not fit a QR at a scannable version, and a
  // squeezed symbol nobody's camera can read is worse than no symbol:
  // the driver would try, fail, and have no idea why. When the plan is
  // too big the tile is simply replaced by the sentence that says so —
  // the file below carries the identical bytes either way. Sealing costs
  // 130 bytes for one recipient and 194 for two, so the ceiling is lower
  // than it was: about ten metadata-free stops rather than fifteen.
  const carriesContact = ticketStopList.some(stop => stop.contact);
  const sealedTo = meshJob.recipient
    ? `${meshJob.recipient.name} (${meshJob.recipient.fingerprint})`
    : "the chosen device";

  const caption = document.createElement("small");
  caption.className = "mesh-job__caption";
  try {
    meshJobOut.append(meshQrTile(meshJob.driverUrl, "Sealed job"));
    // The claim that changed with §20.9, said plainly: this symbol is
    // ciphertext. Not "handle it carefully" — unreadable.
    caption.textContent = handover
      ? `Point the next driver's camera here. It is sealed to ${sealedTo}: no other phone in the `
        + "room gets anything from photographing it."
      : `Point the driver's camera here. The job is sealed to ${sealedTo}, so a photograph of this `
        + "code is ciphertext to everyone else — and nothing about the job reaches the mesh either, "
        + "which only ever carries an 8-byte hash of the plan.";
  } catch {
    caption.textContent = "This plan is bigger than a phone camera can scan — "
      + "hand the driver the file or the link instead. The file is the same sealed bytes.";
  }
  meshJobOut.append(caption);

  const privacy = document.createElement("p");
  privacy.className = "mesh-job__warning";
  // The honest residue. Sealing removes the bystander with a camera; it
  // does not remove the recipient, who holds a publish capability and,
  // since §20.8, a short customer list.
  privacy.textContent = (carriesContact
    ? "Only the devices this was sealed to can read it — including the order numbers and customer "
      + "phone numbers inside. "
    : "Only the devices this was sealed to can read it. ")
    + (meshJob.recipientCount === 1
      // The dispatcher sealed to itself and to nobody else, which in this
      // demo means it enrolled its own card as the driver. Saying "and
      // this dispatcher" as if that were a second party would be a lie.
      ? `That is one device — ${sealedTo}, which is this browser acting as both dispatcher and driver. `
      : `That is two devices: ${sealedTo}, and this dispatcher, so this browser can still open the `
        + "job it sent. ")
    + "Whoever can open it can publish this vehicle — the protocol cannot tell two holders of one "
    + "run seed apart, so send it to one device and no other.";
  meshJobOut.append(privacy);

  if (handover) {
    const warning = document.createElement("p");
    warning.className = "mesh-job__warning";
    // §20.5: two holders of one seed are indistinguishable on the wire,
    // so the protocol cannot enforce this and the page has to say it.
    warning.textContent = "Handing this over means the other device publishes this vehicle, and reads "
      + "every stop's order number, parcel count and customer phone number. "
      + "Stop sharing here once they have it — the protocol cannot tell two holders of one run apart.";
    meshJobOut.append(warning);
  }

  meshJobOut.append(meshLinkField(
    meshJob.recipientCount === 1
      ? `Sealed job — readable only by ${sealedTo}. Whoever can open it publishes the vehicle.`
      : `Sealed job — readable only by ${sealedTo} and this dispatcher. Whoever can open it publishes the vehicle.`,
    meshJob.driverUrl,
    "publish"
  ));

  if (meshJob.ticketBase64) {
    const download = document.createElement("button");
    download.type = "button";
    download.className = "mesh-secondary";
    download.textContent = "Download sealed job file";
    download.addEventListener("click", () => {
      downloadTicketFile(meshJob.ticketBase64, meshJob.jobIdHex);
    });
    const fileNote = document.createElement("small");
    fileNote.className = "mesh-job__caption";
    // Same sealed bytes as the QR, different carrier — and the file is
    // the only carrier once a plan outgrows a camera.
    fileNote.textContent = `${ticketFileName(meshJob.jobIdHex)} — the same sealed job as a one-line file, `
      + "for a plan too big to scan. It is ciphertext, so the channel it travels over cannot read it; "
      + "the device it is addressed to still gets a publish capability.";
    meshJobOut.append(download, fileNote);
  }

  if (meshJob.bootstrap?.length) {
    const seed = document.createElement("small");
    seed.className = "mesh-job__caption";
    // The claim §20.10 makes, said where it is true: the address is in
    // the sealed bytes above, so it reached the driver's device and no
    // channel it travelled over saw it.
    seed.textContent = `This job carries the fleet's seed (${meshJob.bootstrap.join(", ")}) inside the `
      + "sealed ticket, so the driver's phone can reach the mesh on a cold start — and only that "
      + "phone learns the address. An offer never carries one: it is broadcast to strangers, and a "
      + "small operator's own machine is not something to publish to everyone who reads an ad.";
    meshJobOut.append(seed);
  }

  if (meshJob.customerUrl) {
    meshJobOut.append(meshLinkField(
      "Customer link — read-only. Send it with the order confirmation.",
      meshJob.customerUrl
    ));
    if (meshJob.customerHintUrl) {
      meshJobOut.append(meshLinkField(
        "Customer link with the seed hinted — same capability, plus an address to dial.",
        meshJob.customerHintUrl
      ));
      const placement = document.createElement("small");
      placement.className = "mesh-job__caption";
      // One line, because the difference is one line: what is in front of
      // the `#` is sent to the page host, and what is behind it never is.
      placement.textContent = "The capability is behind the #, which browsers never transmit, so no "
        + "server ever sees it; the seed is in front of it, as a public address the customer's device "
        + "may dial or ignore. The two links have byte-identical fragments.";
      meshJobOut.append(placement);
    }
    if (ticketStopList.length > 1) {
      const caveat = document.createElement("small");
      caveat.className = "mesh-job__caption";
      // The plan is the driver's and the dispatcher's. Sending a customer
      // the whole run to explain their own ETA sends them every other
      // customer's address, which is not a trade the customer was offered.
      caveat.textContent = "Send each customer this link plus their own stop — never the run. "
        + "Handing over the whole plan hands over every other customer's address, order number "
        + "and phone number.";
      meshJobOut.append(caveat);
    }
  }

  const meta = document.createElement("small");
  meta.className = "mesh-job__meta";
  meta.textContent = `Job ${meshJob.jobIdHex.slice(0, 12)} · ${jobModeLabel(meshJob.mode)}`
    + ` · ${TRAVEL_MODE_LABELS[meshJob.travelMode] || "unspecified"}`
    + ` · expires ${formatExpiry(meshJob.notAfter)}`;
  meshJobOut.append(meta);
}

/**
 * The deliveries a ticket carries, in the order the driver should make
 * them.
 *
 * A trip's stop 0 is where the dispatcher is sending the vehicle *from* —
 * the yard, the restaurant, the driver's current position — not a
 * customer, so it never becomes a stop on the ticket. A round trip names
 * it a second time at the tail; that repeat is not a delivery either.
 */
function ticketStops() {
  if (routePlan?.kind === "trip") {
    const seen = new Set([0]);
    const ordered = [];
    for (const index of routePlan.trip.order) {
      if (seen.has(index)) continue;
      seen.add(index);
      const place = stops[index]?.place;
      if (!place) continue;
      ordered.push({
        lat: place.lat,
        lon: place.lon,
        label: planLabel(place.label || `Stop ${ordered.length + 1}`)
      });
    }
    return ordered;
  }
  // A two-point route: the ticket names the end of the line the router
  // actually traced, which is the snapped road position rather than the
  // rooftop the search matched.
  const geometry = activeRouteGeometry();
  if (!geometry?.length) return [];
  const end = geometry[geometry.length - 1];
  return [{ lat: end[0], lon: end[1], label: planLabel(stops[stops.length - 1]?.place?.label || "Drop-off") }];
}

/**
 * Delivery metadata is per **stop**, not per slot, so it is keyed by the
 * stop's own position: re-optimizing a round or dropping a stop out of
 * the middle must not leave a customer's phone number attached to
 * whoever ends up third in the list.
 */
function stopDetailKey(stop) {
  return `${stop.lat.toFixed(5)},${stop.lon.toFixed(5)}`;
}

// The caps are the protocol's (§20.8), restated here the way the note
// field's 64 already is. They are UTF-8 **byte** caps and these are
// character limits, so an accented instruction can still be refused —
// the encoder is the authority and its message reaches the toast.
const STOP_DETAIL_FIELDS = [
  { key: "orderRef", label: "Order reference", max: 24, placeholder: "e.g. 4471" },
  { key: "parcels", label: "Parcels", max: 5, placeholder: "e.g. 2", numeric: true },
  { key: "instructions", label: "Instructions", max: 64, placeholder: "e.g. side gate, leave with neighbour" },
  { key: "contact", label: "Contact", max: 24, placeholder: "e.g. +1 514 555 0134", contact: true }
];

/** A one-line summary of what a stop already carries, for the collapsed row. */
function stopDetailSummary(details) {
  if (!details) return "Add details";
  const parts = [];
  if (details.orderRef) parts.push(`#${details.orderRef}`);
  // `0` is a statement — "nothing to carry" — and must not vanish into a
  // falsy check the way an unfilled box does.
  if (details.parcels !== "" && details.parcels != null) {
    parts.push(details.parcels === "1" ? "1 parcel" : `${details.parcels} parcels`);
  }
  if (details.instructions) parts.push("instructions");
  if (details.contact) parts.push("contact");
  return parts.length ? parts.join(" · ") : "Add details";
}

/**
 * The dispatcher's per-stop detail editor.
 *
 * Collapsed by default: most stops on most days are an address and
 * nothing else, and five boxes per stop on a twelve-drop round is a form
 * nobody fills in. A `<details>` element does the disclosure natively,
 * so it keeps the keyboard and screen-reader behaviour for free.
 */
function renderJobStopDetails() {
  if (!meshJobStops) return;
  const planned = pulseMesh && !pulseMesh.hasActiveTicket ? ticketStops() : [];
  const signature = planned.map(stopDetailKey).join("|");
  if (!planned.length) {
    meshJobStops.hidden = true;
    meshJobStops.replaceChildren();
    meshStopDetailsKey = "";
    return;
  }
  // Same round as last render: leave the DOM alone. The mesh tick runs
  // every two seconds and a dispatcher typing an address into a box does
  // not want it replaced underneath the cursor.
  if (signature === meshStopDetailsKey && meshJobStops.childElementCount) return;
  meshStopDetailsKey = signature;
  meshJobStops.hidden = false;
  meshJobStops.replaceChildren();

  for (const [index, stop] of planned.entries()) {
    const key = stopDetailKey(stop);
    const details = meshStopDetails.get(key) || {};
    const panel = document.createElement("details");
    panel.className = "mesh-job-details__stop";
    const summary = document.createElement("summary");
    const name = document.createElement("b");
    name.textContent = `${index + 1}. ${stop.label || `Stop ${index + 1}`}`;
    const state = document.createElement("small");
    state.textContent = stopDetailSummary(details);
    summary.append(name, state);
    panel.append(summary);

    for (const field of STOP_DETAIL_FIELDS) {
      const row = document.createElement("label");
      row.className = "mesh-job-details__field";
      const caption = document.createElement("small");
      caption.textContent = field.label;
      const input = document.createElement("input");
      input.type = field.numeric ? "number" : (field.contact ? "tel" : "text");
      if (field.numeric) { input.min = "0"; input.step = "1"; }
      input.maxLength = field.max;
      input.placeholder = field.placeholder;
      input.value = details[field.key] ?? "";
      input.addEventListener("input", () => {
        const current = meshStopDetails.get(key) || {};
        const value = input.value.trim();
        if (value === "") delete current[field.key];
        else current[field.key] = value;
        meshStopDetails.set(key, current);
        state.textContent = stopDetailSummary(current);
      });
      row.append(caption, input);
      panel.append(row);
    }
    meshJobStops.append(panel);
  }

  const note = document.createElement("small");
  note.className = "mesh-job-details__note";
  // The privacy claim, stated where the phone number is typed rather
  // than only in the docs: this is why it is safe to put here at all.
  note.textContent = "These reach the driver inside the ticket and stop there — the wire carries an "
    + "8-byte hash of the plan, so no follower ever sees an order number, a parcel count or a phone "
    + "number. They do live in the ticket, so a leaked one now reads as a customer list.";
  meshJobStops.append(note);
}

/** The typed details for a stop, as the plan codec wants them. */
function withStopDetails(stop) {
  const details = meshStopDetails.get(stopDetailKey(stop));
  if (!details) return stop;
  const parcels = details.parcels === "" || details.parcels == null ? null : Number(details.parcels);
  return {
    ...stop,
    orderRef: details.orderRef || null,
    // `0` survives: "nothing to carry, and the dispatcher said so" is a
    // different claim from "nobody said", and the plan can hold both.
    parcels: Number.isInteger(parcels) ? parcels : null,
    instructions: details.instructions || null,
    contact: details.contact || null
  };
}

/**
 * What is missing before a job can be issued at all.
 *
 * Enrolment is the gate (§20.9), so the button cannot simply fail: an
 * empty roster is not an error the dispatcher made, it is a step nobody
 * has taken yet, and the page has to name the step.
 */
function jobGateReason() {
  if (!pulseMesh) return null;
  if (!pulseMesh.roster().length) {
    return "No device is enrolled, and a job can only be sealed to a device this dispatcher already "
      + "holds the key for. Ask the driver for their device card — the wayfind://device link or QR on "
      + "their phone — paste it under “Enrolled devices”, and check the fingerprint aloud. To try both "
      + "roles in this one browser, press “Use this device's card” and enrol it.";
  }
  if (!meshRecipient) {
    return "Pick the device this job is for. It is sealed to that device and to this dispatcher, and "
      + "to nobody else — there is no “seal to whoever holds it”.";
  }
  return null;
}

async function createJobTicket() {
  if (!pulseMesh) return;
  const gate = jobGateReason();
  if (gate) {
    // Said in place rather than in a toast that disappears: the fix is
    // three steps long and lives in a section further up the card.
    if (meshJobGate) { meshJobGate.hidden = false; meshJobGate.textContent = gate; }
    if (!pulseMesh.roster().length) meshEnrolInput?.focus();
    return;
  }
  const ticket = ticketStops().map(withStopDetails);
  if (!ticket.length) {
    showToast("Plot a route first — a job ticket names where the vehicle is going", "error");
    return;
  }
  try {
    const job = await pulseMesh.issueJob({
      stops: ticket,
      fine: meshShareFine,
      travelMode: meshTravelMode,
      recipient: meshRecipient
    });
    meshJob = { ...job, stops: ticket };
    renderJobTicket();
    showToast(
      `Job sealed to ${job.recipient.name} — the customer's link works before a driver exists`,
      "info"
    );
  } catch (error) {
    showToast(`The job could not be issued: ${error?.message || error}`, "error");
    return;
  }
  renderMeshCard();
}

// --- Offers (§20.4) ---------------------------------------------------------
//
// The one artifact on this channel that is *meant* to be public. A
// ticket is sealed because it goes to one device; an offer is unsealed
// because it goes to strangers, and it is safe in public because it
// contains a commitment to the plan and coarse descriptors rather than
// anything worth stealing. Both halves of that are said on the card,
// because a page that shows a QR without saying which of the two it is
// has taught the user nothing.

/** The routed distance of what is on screen — the size of the work. */
function routeTotalMeters() {
  if (!routePlan) return 0;
  return routePlan.kind === "pair"
    ? routePlan.candidates[routePlan.active].distanceMeters
    : routePlan.trip.totalMeters;
}

function formatOfferPay(payMinor, currency) {
  if (payMinor == null || !currency) return null;
  try {
    return new Intl.NumberFormat(undefined, { style: "currency", currency }).format(payMinor / 100);
  } catch {
    return `${(payMinor / 100).toFixed(2)} ${currency}`;
  }
}

/** "around 45.51, −73.58" — best-effort a place name for the same cell. */
async function coarsePlaceName(centroid) {
  const coordinates = `${centroid.lat.toFixed(2)}, ${centroid.lon.toFixed(2)}`;
  if (!engine) return coordinates;
  try {
    const response = await reverseGeocodeOsm(engine, { lat: centroid.lat, lon: centroid.lon, size: 1 });
    // The hit's **city and nothing else**. The nearest match to a grid
    // centre is a street address, and printing it beside the cell would
    // hand back exactly the precision the grid was there to remove — the
    // municipality is coarser than the cell, so it adds a name without
    // adding a location.
    const place = (response.results || [])[0]?.city;
    return place ? `${place} — ${coordinates}` : coordinates;
  } catch {
    return coordinates;
  }
}

/** The coarse summary both the dispatcher and the bidder see. */
function offerSummaryLines(offer) {
  const pay = formatOfferPay(offer.payMinor, offer.currency);
  const lines = [
    `${offer.stopCount === 1 ? "1 stop" : `${offer.stopCount} stops`} · ${formatDistance(offer.totalMeters)}`
      + ` · ${TRAVEL_MODE_LABELS[offer.travelMode] || "unspecified"} · ${jobModeLabel(offer.mode)}`,
    `Ranges ${offer.spreadLabel}`,
    `Expires ${formatExpiry(offer.notAfter)} · issuer ${offer.issuerHex.slice(0, 12)}`
  ];
  if (pay) lines.push(`Pay ${pay}`);
  if (offer.label) lines.push(`“${offer.label}”`);
  return lines;
}

async function broadcastJobOffer() {
  if (!pulseMesh) return;
  const stops = ticketStops().map(withStopDetails);
  if (!stops.length) {
    showToast("Plot a route first — an offer describes a round", "error");
    return;
  }
  let offer;
  try {
    offer = await pulseMesh.broadcastOffer({
      stops,
      fine: meshShareFine,
      travelMode: meshTravelMode,
      totalMeters: Math.round(routeTotalMeters()),
      payMinor: meshOfferPay?.value ? Number(meshOfferPay.value) : null,
      // No default. A currency the page guessed is a price the dispatcher
      // did not quote, and the codec refuses anything that is not ISO 4217.
      currency: meshOfferPay?.value ? (meshOfferCurrency?.value.trim() || "EUR") : null,
      label: meshOfferLabel?.value.trim() || null
    });
  } catch (error) {
    showToast(`The offer could not be broadcast: ${error?.message || error}`, "error");
    return;
  }
  await renderJobOffer(offer);
  showToast(`Offer broadcast — ${offer.bytes} bytes, and not one address in them`, "info");
}

async function renderJobOffer(offer) {
  if (!meshOfferOut) return;
  meshOfferOut.hidden = false;
  meshOfferOut.replaceChildren();

  const title = document.createElement("b");
  title.textContent = "A public offer for this round";
  meshOfferOut.append(title);

  const summary = document.createElement("small");
  summary.className = "mesh-job__summary";
  summary.textContent = offerSummaryLines(offer).join(" · ");
  meshOfferOut.append(summary);

  const where = document.createElement("small");
  where.className = "mesh-job__summary";
  where.textContent = `Around ${await coarsePlaceName(offer.centroid)}`
    + ` (the centroid of the stops, rounded to a ${offer.centroid.gridDegrees}° grid — about 1 km)`;
  meshOfferOut.append(where);

  try {
    meshOfferOut.append(meshQrTile(offer.offerUrl, "Job offer"));
  } catch {
    // 148–177 bytes. There is no round that makes this fail.
  }

  const caption = document.createElement("small");
  caption.className = "mesh-job__caption";
  // The claim that makes this button different from the one above it.
  caption.textContent = "This one is not sealed, and does not need to be: post it in a group chat, a "
    + "channel, a noticeboard. Anybody who reads it learns how many stops, how far, which travel mode, "
    + "roughly where the round sits and until when — and nothing that names a door.";
  meshOfferOut.append(caption);

  const learns = document.createElement("p");
  learns.className = "mesh-job__caption";
  learns.textContent = "What a bidder does not learn: any stop's position, any address label, any order "
    + "number, parcel count, instruction or customer phone number. Those are in the plan, and the offer "
    + `carries only an 8-byte hash of it (${offer.bytes} bytes total). They reach one device — the one `
    + "that is finally awarded the sealed ticket.";
  meshOfferOut.append(learns);

  meshOfferOut.append(meshLinkField(
    "Offer link — public. Safe to post anywhere; it opens nothing and moves no vehicle.",
    offer.offerUrl
  ));

  const meta = document.createElement("small");
  meta.className = "mesh-job__meta";
  meta.textContent = `Job ${offer.jobIdHex.slice(0, 12)} · the courier's device checks the ticket it is `
    + "finally handed against this — a swapped plan is refused by name.";
  meshOfferOut.append(meta);
}

/**
 * An offer that arrived, shown to a courier deciding whether to bid.
 *
 * The honest line is the whole card: the addresses are *not* in this,
 * and they arrive only if the job is awarded to this device. A page that
 * showed a coarse summary without saying so would read as a job with
 * missing details rather than a job with withheld ones.
 */
async function showOfferCard(text) {
  if (!pulseMesh || !meshAcceptCard) return;
  let offer;
  try {
    offer = await pulseMesh.describeOffer(text);
  } catch (error) {
    meshFollowOut.hidden = false;
    meshFollowOut.textContent = `That offer did not decode: ${error?.message || error}`;
    return;
  }
  meshFollowOut.hidden = true;
  meshAcceptCard.hidden = false;
  meshAcceptCard.replaceChildren();

  const title = document.createElement("b");
  title.textContent = "A job offer";
  const meta = document.createElement("small");
  meta.textContent = offerSummaryLines(offer).join(" · ");
  meshAcceptCard.append(title, meta);

  const where = document.createElement("small");
  where.textContent = `Around ${await coarsePlaceName(offer.centroid)} — a ${offer.centroid.gridDegrees}° `
    + "grid cell, about a kilometre across.";
  meshAcceptCard.append(where);

  const honest = document.createElement("small");
  // The sentence this card exists for.
  honest.textContent = offer.ok
    ? "The addresses are not in this offer. Nobody who reads it — including you — can tell which doors "
      + "the round visits. They arrive only if the dispatcher awards the job to this device, sealed to "
      + "its key. What is in here is a hash of the plan, so the ticket you are eventually sent can be "
      + "checked against this exact round."
    : `This offer cannot be trusted: ${offer.reason}.`;
  meshAcceptCard.append(honest);

  const reply = document.createElement("button");
  reply.type = "button";
  reply.textContent = "Send this device's card to be considered";
  reply.disabled = !offer.ok;
  reply.addEventListener("click", () => { bidOnOffer(text).catch(() => {}); });
  meshAcceptCard.append(reply);

  const transport = document.createElement("small");
  // Said plainly rather than implied: there is no market here.
  transport.textContent = "There is no bidding channel in this protocol. Replying means sending the "
    + "dispatcher your device card — the wayfind://device link below — by whatever channel you already "
    + "use. Enrolment is what makes a sealed job possible at all (§20.9).";
  meshAcceptCard.append(transport);
}

/** Records the bid and hands the courier their card to send. */
async function bidOnOffer(text) {
  try {
    await pulseMesh.rememberOffer(text, { bid: true });
  } catch (error) {
    showToast(`That offer could not be kept: ${error?.message || error}`, "error");
    return;
  }
  const me = await refreshDeviceIdentity();
  if (!me) return;
  meshAcceptCard.replaceChildren();
  const title = document.createElement("b");
  title.textContent = "Offer kept — send this card to the dispatcher";
  const note = document.createElement("small");
  note.textContent = "This device now remembers the round that was advertised. When a sealed job arrives, "
    + "it is checked against it, and a job that is not the one offered is refused by name rather than "
    + "quietly accepted.";
  meshAcceptCard.append(title, note);
  try {
    meshAcceptCard.append(meshQrTile(me.cardUrl, "This device's card"));
  } catch {
    // A card is 40-odd bytes.
  }
  meshAcceptCard.append(meshLinkField(
    "Device card — a public key. It says where a job can be sent, and opens nothing.",
    me.cardUrl
  ));
  showToast("Offer kept — the ticket you are sent will be checked against it", "info");
}

/**
 * Handing the job to the next driver.
 *
 * Since §20.9 this is not "show the same QR again": the ticket is
 * re-sealed to a device chosen from the roster, which means the next
 * driver has to have been enrolled here *before* the bike broke. There
 * is no path around that, and when the roster is empty the panel says so
 * instead of offering a button that cannot work.
 */
function handOverJob() {
  if (!pulseMesh?.hasActiveTicket || !meshJobOut) return;
  const devices = pulseMesh.roster();
  meshJobOut.hidden = false;
  meshJobOut.replaceChildren();

  const title = document.createElement("b");
  title.textContent = "Hand this job to another device";
  meshJobOut.append(title);

  if (!devices.length) {
    const gate = document.createElement("p");
    gate.className = "mesh-job__warning";
    gate.textContent = "No device is enrolled here, so this job cannot be handed to anyone. A job is "
      + "encrypted to the device it is for: get the next driver's device card (the wayfind://device "
      + "link or QR on their phone), enrol it under “Enrolled devices”, then come back. No card, no "
      + "transfer — that is the rule, not a limitation of this page.";
    meshJobOut.append(gate);
    return;
  }

  const note = document.createElement("small");
  note.className = "mesh-job__caption";
  note.textContent = "The same run, re-sealed to the device you pick. The customer's link keeps "
    + "working — that it survives the change of driver is the point of a dispatch ticket.";
  meshJobOut.append(note);

  const row = document.createElement("div");
  row.className = "mesh-chips";
  for (const entry of devices) {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.textContent = `${entry.name} · ${entry.fingerprint}`;
    chip.addEventListener("click", () => { resealToDevice(entry).catch(() => {}); });
    row.append(chip);
  }
  meshJobOut.append(row);
}

async function resealToDevice(entry) {
  try {
    const job = await pulseMesh.handOverJob(entry.publicKey);
    meshJob = { ...job, stops: meshJob?.stops || pulseMesh.drivingSnapshot()?.stops || [] };
    renderJobTicket({ handover: true });
    showToast(`Re-sealed to ${entry.name} — only that device can open it`, "info");
  } catch (error) {
    showToast(`The job could not be handed over: ${error?.message || error}`, "error");
  }
}

/** An offered ticket, shown before it is taken. */
async function showJobOffer(text) {
  if (!pulseMesh || !meshAcceptCard) return;
  let job;
  try {
    job = await pulseMesh.describeJob(text);
  } catch (error) {
    meshFollowOut.hidden = false;
    // `describeJob` already says "ticket" when the artifact is one, so
    // this renders its sentence rather than wrapping it in a second.
    meshFollowOut.textContent = String(error?.message || error);
    return;
  }
  meshPendingTicket = text;
  meshFollowOut.hidden = true;
  meshAcceptCard.hidden = false;
  meshAcceptCard.replaceChildren();

  const title = document.createElement("b");
  // It opened, so it was sealed to this device: that is the only reason
  // its stops are on screen, and saying so makes the gate visible at the
  // moment it let something through.
  title.textContent = "A job sealed to this device";
  const meta = document.createElement("small");
  meta.textContent = `${jobModeLabel(job.mode)} · ${TRAVEL_MODE_LABELS[job.travelMode] || "unspecified"}`
    + ` · expires ${formatExpiry(job.notAfter)}`
    + ` · issuer ${job.issuerHex.slice(0, 12)} · job ${job.jobIdHex.slice(0, 12)}`;
  meshAcceptCard.append(title, meta);

  const list = document.createElement("ol");
  list.className = "mesh-accept__stops";
  for (const stop of job.stops) {
    const item = document.createElement("li");
    item.textContent = stop.label || `Stop ${stop.index}`;
    const coordinates = document.createElement("span");
    coordinates.textContent = `${stop.lat.toFixed(5)}, ${stop.lon.toFixed(5)}`;
    item.append(coordinates);
    list.append(item);
  }
  meshAcceptCard.append(list);

  // §20.4: if this device bid on an offer, say out loud whether the job
  // it was handed is the job that was advertised. A verdict shown only on
  // failure is a verdict nobody trusts on success.
  if (job.award) {
    const award = document.createElement("small");
    award.className = "mesh-job__meta";
    award.textContent = job.award.ok
      ? "Matches the offer you kept: same plan (planRef), same job, same issuer, same graph epoch, and "
        + "it does not run later than advertised."
      : `This is NOT the job that was offered — ${job.award.reason}. `
        + "Refusing it: the round you agreed to is not the round in this ticket.";
    meshAcceptCard.append(award);
  }

  const note = document.createElement("small");
  note.textContent = job.ok
    ? "This job was sealed to this device's key — that is why it opened here at all. "
      + "Accepting publishes this vehicle to everyone holding the customer's link."
    : `This job cannot be published: ${job.reason}.`;
  meshAcceptCard.append(note);

  const accept = document.createElement("button");
  accept.type = "button";
  accept.textContent = "Accept job";
  accept.disabled = !job.ok;
  accept.addEventListener("click", () => { acceptOfferedJob().catch(() => {}); });
  meshAcceptCard.append(accept);
}

function dismissJobOffer() {
  meshPendingTicket = null;
  meshPendingCard = null;
  if (meshAcceptCard) {
    meshAcceptCard.hidden = true;
    meshAcceptCard.replaceChildren();
  }
}

async function acceptOfferedJob() {
  const ticket = meshPendingTicket;
  if (!ticket) return;
  if (!pulseMesh) await togglePulseMesh(true);
  if (!pulseMesh) return;
  try {
    const { job } = await pulseMesh.acceptJob(ticket);
    meshRun = pulseMesh.run;
    // No ticket bytes are kept here. The opened artifact stays inside the
    // controller, and handing the job on goes back through it to be
    // re-sealed — there is no plaintext ticket for this page to hold.
    meshJob = {
      ticketBase64: null,
      driverUrl: null,
      customerUrl: null,
      stops: job.stops,
      jobIdHex: job.jobIdHex,
      mode: job.mode,
      travelMode: job.travelMode,
      notAfter: job.notAfter
    };
    dismissJobOffer();
    showToast("Job accepted — you are now publishing this run", "info");
    await routeToJobStop(job);
  } catch (error) {
    showToast(`The job could not be accepted: ${error?.message || error}`, "error");
  }
  renderMeshCard();
}

/**
 * Puts the job's last stop where the router already looks for a
 * destination, so the driver gets the ordinary directions flow rather
 * than a second, parallel one.
 */
async function routeToJobStop(job) {
  const last = job.stops?.[job.stops.length - 1];
  if (!Number.isFinite(last?.lat) || !Number.isFinite(last?.lon)) return;
  map.flyTo({ center: [last.lon, last.lat], zoom: Math.max(map.getZoom(), 13), essential: true });
  if (routeAvailable !== true) return;
  if (!directionsMode) setDirectionsMode(true);
  if (stops.length < 2) {
    while (stops.length < 2) stops.push(newStop());
    renderStops();
  }
  const index = stops.length - 1;
  const stop = stops[index];
  if (!stop?.input) return;
  const label = last.label || `${last.lat.toFixed(5)}, ${last.lon.toFixed(5)}`;
  stop.text = label;
  stop.input.value = label;
  setStopNote(stop, "");
  await setStopPlace(index, { lat: last.lat, lon: last.lon, label });
  if (!stops[0]?.place) {
    showToast("Destination set from the ticket — set your start point to route there.", "info");
  }
}

async function followSharedDrive() {
  if (!pulseMesh) return;
  if (meshFollow) {
    pulseMesh.stopFollowing();
    meshFollow = null;
    meshFollowPlan = null;
    // A blob URL nobody revokes keeps the decoded image alive for the
    // life of the tab, which is exactly the wrong thing to do with a
    // photo of somebody's doorstep.
    if (meshPhotoShown.url) URL.revokeObjectURL(meshPhotoShown.url);
    meshPhotoShown = { hash: null, url: null };
    clearInterval(meshFollowTimer);
    meshFollowTimer = null;
    meshFollowCard.hidden = true;
    meshFollowOut.hidden = true;
    meshFollowButton.textContent = "Follow";
    await refreshMeshDisplay();
    return;
  }
  const value = meshFollowInput.value.trim();
  if (!value) return;
  // A ticket pasted here is not a link to follow: it is a job to take,
  // and acting on it the way this function acts on a link would put the
  // person who pasted it on the road without being asked.
  const artifact = await pulseMesh.classifyFragment(value).catch(() => null);
  if (artifact?.kind === "ticket") {
    await showJobOffer(value);
    return;
  }
  // An offer is neither a run to watch nor a job to take: it is an
  // advertisement, and the only decision it asks for is whether to bid.
  if (artifact?.kind === "offer") {
    if (artifact.reason) {
      meshFollowOut.hidden = false;
      meshFollowOut.textContent = artifact.reason;
      return;
    }
    await showOfferCard(value);
    return;
  }
  // A device card is neither a run to watch nor a job to take: it is an
  // address to seal jobs *to*, and enrolling it is a decision about who
  // this dispatcher will send customer data to.
  if (artifact?.kind === "device") {
    showDeviceCardOffer(artifact.card, value);
    return;
  }
  // A seed card is a location, not a capability: there is nothing to
  // watch and nothing to accept, so it goes straight to the seed list.
  // Nothing is granted by holding one, which is why this needs no
  // confirmation step where a device card needs two.
  if (artifact?.kind === "seed") {
    if (artifact.reason) {
      meshFollowOut.hidden = false;
      meshFollowOut.textContent = artifact.reason;
      return;
    }
    await enrolFleetSeed(value);
    meshFollowInput.value = "";
    return;
  }
  // A link this build cannot read is named for what it is. Falling
  // through would hand the user the link decoder's byte-count complaint.
  if (artifact?.kind === "link" && artifact.reason) {
    meshFollowOut.hidden = false;
    meshFollowOut.textContent = artifact.reason;
    return;
  }
  dismissJobOffer();
  try {
    meshFollow = await pulseMesh.followDrive(value, {
      onUpdate: () => { refreshMeshDisplay().catch(() => {}); }
    });
    // A follow that starts mid-run has a hole by definition, and the
    // channel fills it from other followers on join — so the card may
    // have a position before a single new update arrives.
    meshFollowOut.hidden = false;
    meshFollowOut.textContent = "Following. Nothing but the link left this browser.";
    meshFollowButton.textContent = "Stop";
    clearInterval(meshFollowTimer);
    meshFollowTimer = setInterval(() => { renderMeshFollow().catch(() => {}); }, 2000);
    await renderMeshFollow();
  } catch (error) {
    meshFollowOut.hidden = false;
    meshFollowOut.textContent = `That link did not decode: ${error?.message || error}`;
  }
}

/**
 * A ticket or link handed over as a file.
 *
 * A day's worth of stops outgrows a QR long before it outgrows the
 * protocol, so the artifact also travels as a one-line text file. It is
 * the same signed bytes over a different carrier, which is why this does
 * nothing of its own: it reads the line into the follow field and calls
 * the paste handler, so classify, accept and describe all behave exactly
 * as they do for a link someone typed.
 */
async function openTicketFile() {
  const file = meshFileInput?.files?.[0];
  if (!file) return;
  let text;
  try {
    text = await new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.addEventListener("load", () => resolve(String(reader.result || "")));
      reader.addEventListener("error", () => reject(reader.error || new Error("unreadable")));
      reader.readAsText(file);
    });
  } catch (error) {
    showToast(`That file could not be read: ${error?.message || error}`, "error");
    return;
  } finally {
    // Cleared so that choosing the same file twice fires `change` again.
    meshFileInput.value = "";
  }
  if (!text.trim()) {
    showToast("That file is empty — a ticket file is one line, the job's link", "error");
    return;
  }
  meshFollowInput.value = text.trim();
  // The follow button is a toggle. A file arriving while a follow is
  // open means "watch this one instead", not "stop watching".
  if (meshFollow) await followSharedDrive();
  await followSharedDrive();
}

const THREAD_STATE_LABELS = {
  1: "scheduled", 2: "en route", 3: "at a stop", 4: "arrived", 5: "cancelled", 6: "off plan"
};

/**
 * What the driver has said about the stops, on the followed run's card.
 *
 * The map is cumulative in every record precisely so this can be drawn
 * from the newest update alone: the gossip channel is lossy and catch-up
 * reaches back two minutes, so a card that had to diff a history would
 * show a follower who joined late a day with holes in it.
 *
 * A pending stop draws nothing. "Nobody has said what happened here" and
 * "the van has not arrived" are different statements, and a badge for the
 * first would be read as the second.
 */
function renderFollowedOutcomes(status, update) {
  const outcomes = status.outcomes || [];
  const marked = outcomes
    .map((outcome, i) => ({ index: i + 1, outcome }))
    .filter(entry => entry.outcome !== STOP_OUTCOME.PENDING);
  if (!marked.length) return;

  const badges = document.createElement("div");
  badges.className = "mesh-outcomes";
  for (const entry of marked) {
    const badge = document.createElement("span");
    badge.className = "mesh-outcome";
    badge.dataset.outcome = STOP_OUTCOME_LABELS[entry.outcome];
    badge.textContent = entry.outcome === STOP_OUTCOME.DELIVERED
      ? `Stop ${entry.index} ✓`
      : `Stop ${entry.index} ${STOP_OUTCOME_LABELS[entry.outcome]}`;
    badges.append(badge);
  }
  meshFollowCard.append(badges);

  // The latest mark in words, with its reason — which the bitmap alone
  // cannot carry, and which is the whole difference between "skipped"
  // and something a customer can act on.
  const last = status.lastOutcome;
  if (!last) return;
  const sentence = document.createElement("small");
  sentence.className = "mesh-follow-card__mark";
  const hasNote = Boolean(update?.note?.length);
  const note = hasNote ? ` (“${new TextDecoder().decode(update.note)}”)` : "";
  const reason = stopReasonClause(last.reasonCode, hasNote);
  sentence.textContent = `Stop ${last.stopIndex} ${STOP_OUTCOME_LABELS[last.outcome]}${reason}${note}`;
  meshFollowCard.append(sentence);
}

/**
 * The proof-of-delivery photo behind a mark, on the follower card
 * (§20.7).
 *
 * Two outcomes, and the difference between them is the design rather
 * than a limitation. A **dispatcher** — this browser, if it issued the
 * job and still holds the ticket — can derive the key from the run seed,
 * so it fetches the sealed blob by content hash, verifies it against the
 * commitment, opens it and shows it. A **customer**, holding only the
 * 45-byte link, cannot: the seed is not in the link and is not derivable
 * from it. They get a chip saying a photo exists, which is honest and
 * costs nothing — the commitment is in the record they can already read
 * — and no fetch is attempted at all, because there would be nothing to
 * do with the answer.
 */
async function renderFollowedPhoto(last, update) {
  if (!last?.photoHash || !meshFollowCard) return;
  if (meshPhotoShown.hash !== last.photoHash) {
    if (meshPhotoShown.url) URL.revokeObjectURL(meshPhotoShown.url);
    // The hash is claimed before the await, so the next tick's render
    // does not fire a second fetch for the same photo.
    meshPhotoShown = { hash: last.photoHash, url: null, holdsSeed: false };
    let result = null;
    try {
      result = await pulseMesh?.followedPhoto({
        photoHash: last.photoHash, stopIndex: last.stopIndex, planRef: update?.planRef
      });
    } catch (error) {
      showToast(`That delivery photo would not open: ${error?.message || error}`, "error");
    }
    meshPhotoShown.holdsSeed = Boolean(result?.holdsSeed);
    if (result?.bytes) {
      meshPhotoShown.url = URL.createObjectURL(new Blob([result.bytes], { type: "image/jpeg" }));
    }
  }

  if (meshPhotoShown.url) {
    const image = document.createElement("img");
    image.className = "mesh-photo";
    image.src = meshPhotoShown.url;
    image.alt = `Proof of delivery for stop ${last.stopIndex}`;
    meshFollowCard.append(image);
    return;
  }

  const chip = document.createElement("span");
  chip.className = "mesh-photo-chip";
  chip.dataset.state = meshPhotoShown.holdsSeed ? "unreachable" : "sealed";
  // The two sentences are different facts and must not be blurred: one
  // says the bytes are not for you, the other says they are for you and
  // nobody reachable is holding them. Nothing relays a photo, so a
  // driver who has gone home takes them with him.
  chip.textContent = meshPhotoShown.holdsSeed
    ? "Photo attached — the driver is not reachable right now"
    : "Photo attached — dispatcher only";
  meshFollowCard.append(chip);
}

/**
 * The followed run's card.
 *
 * Every claim on it comes from the subscriber's own `status()`, not from
 * this code: "bus expected 07:44" and "bus is here, arriving 07:44" are
 * different claims, and §12 is explicit that a stale position must never
 * be presented as the second.
 */
async function renderMeshFollow() {
  if (!meshFollow || !meshFollowCard) return;
  // §12's four rows are (live position?) × (live traffic?), and the
  // subscriber can only answer the first half — the second is this
  // device's own traffic channel. Without it the card said "live
  // position, static-metric ETA" directly above a live-traffic ETA,
  // which is the self-contradiction §12 exists to prevent.
  const status = meshFollow.status({ hasTraffic: meshLastTraffic.length > 0 });
  const update = meshFollow.latest();
  meshFollowCard.hidden = false;
  meshFollowCard.replaceChildren();

  const claim = document.createElement("b");
  claim.textContent = status.claim;
  const meta = document.createElement("small");
  meta.textContent = update
    ? `${THREAD_STATE_LABELS[update.state] || "unknown"} · seq ${update.seq}` +
      (status.travelMode ? ` · ${TRAVEL_MODE_LABELS[status.travelMode]}` : "") +
      (status.ageSeconds != null ? ` · ${status.ageSeconds}s ago` : "")
    : "waiting for the first update";
  meshFollowCard.dataset.live = String(status.live);
  meshFollowCard.append(claim, meta);

  renderFollowedOutcomes(status, update);
  await renderFollowedPhoto(status.lastOutcome, update);

  // §9, the reason this channel lives in a maps engine: arrival is a
  // local route query from the reported position to the stop *this*
  // device cares about, under this device's live-traffic metric. The
  // publisher broadcasts one position and never learns which stop
  // anyone asked about.
  const point = await meshFollow.position().catch(() => null);
  if (point) {
    if (!meshFollowPlan) {
      meshFollowPlan = { stops: [{ index: 1, lat: point.lat, lon: point.lon }], dwellSeconds: 0 };
    }
    const destination = activeRouteGeometry()?.at(-1);
    if (destination) {
      meshFollowPlan = {
        stops: [{ index: 1, lat: destination[0], lon: destination[1] }],
        dwellSeconds: 0
      };
    }
    const eta = await pulseMesh.followedEta(meshFollowPlan, 1).catch(() => null);
    if (eta?.basis === "marked") {
      // The driver said this stop is not being made. There is no arrival
      // to show, so the card shows none — a number here would be a lie
      // with a decimal point on it.
      const line = document.createElement("small");
      line.className = "mesh-follow-card__eta";
      line.dataset.tone = "marked";
      line.textContent = `The driver marked this stop ${eta.outcomeName}`
        + stopReasonClause(eta.reasonCode, Boolean(update?.note?.length))
        + ". No arrival is expected.";
      meshFollowCard.append(line);
    } else if (eta?.secondsFromNow != null) {
      const line = document.createElement("small");
      line.className = "mesh-follow-card__eta";
      line.textContent = `${formatDuration(eta.secondsFromNow)} away · ${eta.basis} · from its ${eta.positionBasis === "reported-position" ? "reported position" : "last stop"}`;
      meshFollowCard.append(line);
      // §9 honesty: this page holds one graph and it is a driving graph.
      // Routing a bike courier on it understates the time badly, and the
      // follower is owed that sentence rather than a confident number.
      if (eta.travelModeMismatch) {
        const caveat = document.createElement("small");
        caveat.className = "mesh-follow-card__caveat";
        caveat.textContent = `This run is ${TRAVEL_MODE_LABELS[status.travelMode]}, and this page only has a `
          + `${eta.profile} graph — the estimate is a ${eta.profile}'s, so treat it as a floor.`;
        meshFollowCard.append(caveat);
      }
    }
    const jump = document.createElement("button");
    jump.type = "button";
    jump.textContent = "Show on map";
    jump.addEventListener("click", () => {
      map.easeTo({ center: [point.lon, point.lat], zoom: Math.max(map.getZoom(), 14) });
    });
    meshFollowCard.append(jump);
  } else if (update) {
    // Coarse mode withholds position entirely — that is the setting
    // working, not a failure, and the card says which.
    const note = document.createElement("small");
    note.textContent = "This link carries stop events only; no position is published.";
    meshFollowCard.append(note);
  }
}

/**
 * One fix, to both channels, in the right order.
 *
 * The thread goes first because it is the one that can veto: threads §10
 * rule 4 says a vehicle publishing a run must stop contributing *traffic*
 * near its planned stops. A dwelling vehicle reports 0 km/h on a road
 * that is flowing, and several of them corroborate each other into a
 * convincing standstill that never happened.
 */
async function offerFixToMesh(lat, lon, speedMps, courseDeg) {
  if (!pulseMesh) return;
  let mayContribute = true;
  if (meshRun) {
    const result = await pulseMesh.threadFix({ lat, lon, speedMps }).catch(() => null);
    if (result) mayContribute = result.contributeTraffic;
  }
  if (!mayContribute) return;
  await pulseMesh.onLocation({ lat, lon, speedMps, courseDeg }).catch(() => {});
}

function wireMeshControls() {
  if (!meshCard) return;
  renderMeshReportRow();
  meshToggle?.addEventListener("click", async () => {
    meshToggle.disabled = true;
    try {
      await togglePulseMesh(!pulseMesh);
      if (pulseMesh) await refreshMeshDisplay();
    } catch (error) {
      showToast(`Live traffic could not start: ${error?.message || error}`, "error");
    } finally {
      meshToggle.disabled = false;
    }
  });
  meshContribute?.addEventListener("change", () => {
    if (!pulseMesh) return;
    if (meshContribute.checked && !window.confirm(
      "Contributing publishes anonymous speed observations for the roads you drive. " +
      "The reticent profile suppresses most of them and adds no identity to any record. Continue?"
    )) {
      meshContribute.checked = false;
      return;
    }
    pulseMesh.setContributing(meshContribute.checked);
    renderMeshCard();
  });
  meshSimulate?.addEventListener("click", () => {
    if (!pulseMesh) return;
    if (pulseMesh.snapshot().simulating) pulseMesh.stopSimulation();
    else pulseMesh.startSimulation();
    renderMeshCard();
  });
  meshShareButton?.addEventListener("click", () => { shareThisDrive().catch(() => {}); });
  meshFollowButton?.addEventListener("click", () => { followSharedDrive().catch(() => {}); });
  meshFileButton?.addEventListener("click", () => meshFileInput?.click());
  meshFileInput?.addEventListener("change", () => { openTicketFile().catch(() => {}); });
  meshJobButton?.addEventListener("click", () => { createJobTicket().catch(() => {}); });
  meshOfferButton?.addEventListener("click", () => { broadcastJobOffer().catch(() => {}); });
  meshHandoverButton?.addEventListener("click", () => handOverJob());
  meshEnrolButton?.addEventListener("click", () => {
    const value = meshEnrolInput?.value.trim();
    if (!value) return;
    enrolDeviceCard(value).then(entry => { if (entry && meshEnrolInput) meshEnrolInput.value = ""; });
  });
  meshEnrolInput?.addEventListener("keydown", event => {
    if (event.key === "Enter") meshEnrolButton?.click();
  });
  meshSeedButton?.addEventListener("click", () => {
    const value = meshSeedInput?.value.trim();
    if (!value) return;
    enrolFleetSeed(value).then(seeds => { if (seeds && meshSeedInput) meshSeedInput.value = ""; });
  });
  meshSeedInput?.addEventListener("keydown", event => {
    if (event.key === "Enter") meshSeedButton?.click();
  });
  // One browser, both roles. This fills the enrol field with this
  // device's own card and stops — the user still enrols it, so the
  // mechanism they are shown is the mechanism a second phone would use.
  meshEnrolSelfButton?.addEventListener("click", () => {
    if (!meshDeviceIdentity || !meshEnrolInput) return;
    meshEnrolInput.value = meshDeviceIdentity.cardUrl;
    meshEnrolInput.focus();
    showToast("This device's card is in the field — press Enrol to add it to the list", "info");
  });
  meshDeviceName?.addEventListener("change", () => {
    if (!pulseMesh) return;
    pulseMesh.setDeviceName(meshDeviceName.value)
      .then(identity => {
        meshDeviceIdentity = identity;
        // A rename changes the card bytes, so anybody already holding it
        // holds an out-of-date name for the same key — worth saying,
        // because the fingerprint is what did not change.
        showToast("Device renamed — the card changed, the key and fingerprint did not", "info");
        renderMeshCard();
      })
      .catch(error => {
        showToast(`That name will not fit: ${error?.message || error}`, "error");
        meshDeviceName.value = meshDeviceIdentity?.name || "";
      });
  });
  meshModeFine?.addEventListener("click", () => setShareMode(true));
  meshModeCoarse?.addEventListener("click", () => setShareMode(false));
  meshTravelCar?.addEventListener("click", () => setTravelMode(TRAVEL_MODE.CAR));
  meshTravelBike?.addEventListener("click", () => setTravelMode(TRAVEL_MODE.BIKE));
  meshTravelFoot?.addEventListener("click", () => setTravelMode(TRAVEL_MODE.FOOT));
  // A link opened in the address bar is the intended way in: the
  // capability lives in the fragment, which browsers never transmit, so
  // the page can act on it without any server ever having seen it. Once
  // the mesh is up, following starts on its own — being handed a link
  // and then having to press a button is a step that exists only
  // because someone forgot to take it out.
  if (location.hash.length > 40) {
    meshFollowInput.value = location.href;
    followLinkFromFragment().catch(() => {});
  }
  renderMeshCard();
}

/**
 * Follows the link this page was opened with, bringing the mesh up if it
 * is not already. Threads need no admission bond and work in read-only
 * mode — they are authenticated end to end by the thread key — so a
 * viewer at home can follow a drive without contributing anything.
 */
async function followLinkFromFragment() {
  if (!routeEngine) return;
  if (!pulseMesh) await togglePulseMesh(true);
  if (!pulseMesh) return;
  // Same fragment, two very different artifacts. A follow link starts
  // watching on its own — being handed a link and then having to press a
  // button is a step nobody meant to leave in. A ticket does not: it is a
  // publish capability, and taking a job is a decision.
  const artifact = await pulseMesh.classifyFragment(location.href).catch(() => null);
  if (artifact?.kind === "ticket") {
    await showJobOffer(location.href);
    return;
  }
  // An offer opened from a group chat: a coarse summary and a decision
  // about whether to bid, never anything that starts on its own.
  if (artifact?.kind === "offer" && !artifact.reason) {
    await showOfferCard(location.href);
    return;
  }
  // A `wayfind://device#…` opened here enrols nothing on its own: being
  // handed a card is not the same as trusting it, and the fingerprint is
  // meant to be compared before anybody presses anything.
  if (artifact?.kind === "device") {
    showDeviceCardOffer(artifact.card, location.href);
    return;
  }
  // A `wayfind://seed#…` opened here — the dispatcher scanning the
  // keeper's terminal — adds an address and nothing else. It grants
  // nothing, so unlike a device card it needs no confirmation.
  if (artifact?.kind === "seed" && !artifact.reason) {
    await enrolFleetSeed(location.href);
    return;
  }
  await followSharedDrive();
}

if (typeof window !== "undefined") {
  // Still exposed for the console, but the page now drives the mesh
  // through its own card rather than through this object.
  window.rangefindPulseMesh = {
    enable: () => togglePulseMesh(true),
    disable: () => togglePulseMesh(false),
    snapshot: () => pulseMesh?.snapshot() ?? null,
    traffic: () => meshLastTraffic,
    incidents: () => meshLastIncidents,
    threads: () => pulseMesh?.threadStats ?? null,
    follow: () => pulseMesh?.follow ?? null,
    /** What actually reached the map, for checking the layer wiring. */
    drawn: () => ({
      layers: ["mesh-traffic", "mesh-traffic-casing", "mesh-incident-dot", "mesh-closure-icon", "mesh-thread"]
        .filter(id => map.getLayer(id)),
      features: map.getSource("pulsemeshLive")?.serialize?.().data?.features?.length ?? 0,
      styleLoaded: map.isStyleLoaded(),
      routeSource: Boolean(map.getSource("routeLines")),
      error: meshLayerError
    }),
    get session() { return pulseMesh?.session ?? null; },
    get mode() { return pulseMesh?.mode ?? null; }
  };
}

function clearCorridorResults() {
  clearMarkers();
  resultList.hidden = true;
  resultList.replaceChildren();
  queryReceipt.hidden = true;
  activeQueryOverride = null;
  for (const chip of routeCard.querySelectorAll("[data-corridor]")) chip.classList.remove("active");
}

function maybeRoute() {
  updateTripEndRow();
  if (!directionsMode && !routePlan) return;
  if (!directionsReady()) {
    routeToken++;
    routePlan = null;
    clearRouteLayers();
    updateStopMarkers();
    if (directionsMode) {
      directionsEmptyState();
      setStatus("Plot a route", "ready");
      updateDirectionsHud();
    }
    return;
  }
  computeRoute();
}

async function computeRoute() {
  if (!routeEngine) return;
  const token = ++routeToken;
  const points = stops.map(stop => ({ lat: stop.place.lat, lon: stop.place.lon }));
  routeCard.dataset.state = "loading";
  routeReceipt.hidden = false;
  routeReceipt.dataset.state = "loading";
  routeReceiptSummary.textContent = "Tracing byte ranges…";
  routeReceiptRoute.textContent = "Fetching the bounded object set for this route.";
  routeReceiptBars.replaceChildren();
  setStatus("Routing over static byte ranges…", "loading");
  mapHudText.textContent = "Computing route in this browser";
  routeEngine.resetStats();
  const started = performance.now();
  try {
    if (points.length === 2) {
      const result = await routeEngine.route({
        from: points[0],
        to: points[1],
        alternatives: 2,
        ...departureParams()
      });
      if (token !== routeToken) return;
      routePlan = { kind: "pair", candidates: [result, ...(result.alternatives || [])], active: 0 };
    } else {
      const trip = await routeEngine.itinerary({ stops: points, ...tripEndParams(), ...departureParams() });
      if (token !== routeToken) return;
      routePlan = { kind: "trip", trip };
    }
    const ms = Math.round(performance.now() - started);
    clearCorridorResults();
    updateStopMarkers();
    drawRoutePlan();
    renderRouteCard();
    renderRouteReceipt(ms);
    fitRoutePlan();
    const seconds = routePlan.kind === "pair"
      ? routePlan.candidates[routePlan.active].seconds
      : routePlan.trip.totalSeconds;
    const meters = routePlan.kind === "pair"
      ? routePlan.candidates[routePlan.active].distanceMeters
      : routePlan.trip.totalMeters;
    setStatus(`Route ready · ${formatDuration(seconds)} · ${formatDistance(meters)} · ${formatNumber(ms)} ms`);
    mapHudText.textContent = `${formatDuration(seconds)} drive · ${formatDistance(meters)}`;
  } catch (error) {
    if (token !== routeToken) return;
    routePlan = null;
    clearRouteLayers();
    routeCard.hidden = true;
    routeReceipt.dataset.state = "error";
    routeReceiptSummary.textContent = "Trace interrupted";
    const friendly = error?.code === "RANGEFIND_ROUTE_SNAP_TOO_FAR"
      ? "No road near one of your points."
      : error?.code === "RANGEFIND_ROUTE_NO_PATH"
        ? "No drivable path connects those points."
        : error?.code === "RANGEFIND_ROUTE_BAD_POINT"
          ? "Those coordinates are outside the valid range."
          : error?.message || "Routing failed";
    showToast(friendly, "error");
    setStatus(friendly, "error");
    mapHudText.textContent = "Routing interrupted";
    showEmpty("No route", friendly);
  } finally {
    if (token === routeToken) routeCard.dataset.state = "";
  }
}

function promoteCandidate(index) {
  if (routePlan?.kind !== "pair" || index === routePlan.active || !routePlan.candidates[index]) return;
  routePlan.active = index;
  clearCorridorResults();
  drawRoutePlan();
  renderRouteCard();
  fitRoutePlan();
  const active = routePlan.candidates[index];
  setStatus(`Alternative selected · ${formatDuration(active.seconds)} · ${formatDistance(active.distanceMeters)}`);
  mapHudText.textContent = `${formatDuration(active.seconds)} drive · ${formatDistance(active.distanceMeters)}`;
}

// --- Route summary card ----------------------------------------------------

function appendStep(name, meters, cumulativeSeconds, first) {
  const step = document.createElement("li");
  step.className = "route-step";
  const dot = document.createElement("i");
  dot.className = "route-step__dot";
  const body = document.createElement("span");
  body.className = "route-step__body";
  const stepName = document.createElement("span");
  stepName.className = "route-step__name";
  if (!name) {
    stepName.textContent = "Unnamed road";
    stepName.classList.add("is-unnamed");
  } else stepName.textContent = name;
  const at = document.createElement("span");
  at.className = "route-step__at";
  at.textContent = first ? "start" : `at ${formatDuration(cumulativeSeconds)}`;
  body.append(stepName, at);
  const length = document.createElement("span");
  length.className = "route-step__meters";
  length.textContent = formatDistance(meters);
  step.append(dot, body, length);
  routeStepsEl.append(step);
}

function appendLegHeader(text, shade) {
  const step = document.createElement("li");
  step.className = "route-step route-step--leg";
  if (shade) step.dataset.shade = "alt";
  const dot = document.createElement("i");
  dot.className = "route-step__dot";
  const body = document.createElement("span");
  body.className = "route-step__body";
  const name = document.createElement("span");
  name.className = "route-step__name";
  name.textContent = text;
  body.append(name);
  step.append(dot, body, document.createElement("span"));
  routeStepsEl.append(step);
}

function renderSteps() {
  routeStepsEl.replaceChildren();
  let cumulative = 0;
  const renderLegSteps = steps => {
    let pendingName = null;
    let pendingMeters = 0;
    let pendingStart = cumulative;
    let first = cumulative === 0;
    const flush = () => {
      if (pendingName === null) return;
      appendStep(pendingName, pendingMeters, pendingStart, first && pendingStart === 0);
      first = false;
      pendingName = null;
    };
    for (const step of steps || []) {
      const name = step.name || "";
      if (pendingName !== null && pendingName === name) {
        pendingMeters += step.meters;
      } else {
        flush();
        pendingName = name;
        pendingMeters = step.meters;
        pendingStart = cumulative;
      }
      cumulative += step.seconds;
    }
    flush();
  };
  if (routePlan.kind === "pair") {
    renderLegSteps(routePlan.candidates[routePlan.active].steps);
  } else {
    for (const [index, leg] of routePlan.trip.legs.entries()) {
      appendLegHeader(
        `Leg ${index + 1} · ${stopLetter(leg.fromStop)} → ${stopLetter(leg.toStop)} · ${formatDuration(leg.seconds)} · ${formatDistance(leg.distanceMeters)}`,
        index % 2 === 1
      );
      renderLegSteps(leg.steps);
    }
  }
}

function renderRouteCard() {
  if (!routePlan) return;
  emptyState.hidden = true;
  routeCard.hidden = false;
  // A finished route deserves the summary on screen; leave taller sheets
  // alone so a deliberate drag is never undone.
  if (searchPanel.dataset.snap === "peek") snapSheet("half");
  if (routePlan.kind === "pair") {
    const active = routePlan.candidates[routePlan.active];
    routeEta.textContent = formatDuration(active.seconds);
    routeDistanceEl.textContent = formatDistance(active.distanceMeters);
    routeBucketEl.textContent = bucketLabel(active.bucket);
    routeOrderEl.hidden = true;
    const chips = routePlan.candidates.map((candidate, index) => {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.classList.toggle("active", index === routePlan.active);
      chip.append(document.createTextNode(index === 0 ? "Fastest" : `Alt ${index}`));
      const detail = document.createElement("small");
      const delta = Math.round((candidate.seconds - routePlan.candidates[0].seconds) / 60);
      detail.textContent = index === 0 ? formatDuration(candidate.seconds) : delta > 0 ? `+${delta} min` : "similar";
      chip.append(detail);
      chip.addEventListener("click", () => promoteCandidate(index));
      return chip;
    });
    routeAlternativesEl.replaceChildren(...chips);
    routeAlternativesEl.hidden = routePlan.candidates.length < 2;
  } else {
    const trip = routePlan.trip;
    routeEta.textContent = formatDuration(trip.totalSeconds);
    routeDistanceEl.textContent = formatDistance(trip.totalMeters);
    routeBucketEl.textContent = bucketLabel(trip.legs[0]?.bucket || routeBucketNames[0] || "base");
    routeOrderEl.hidden = false;
    routeOrderEl.textContent = `Optimized order · ${trip.order.map(stopLetter).join(" → ")} · ${trip.legs.length} legs`;
    routeAlternativesEl.hidden = true;
    routeAlternativesEl.replaceChildren();
  }
  renderSteps();
  routeStepsWrap.open = true;
  routeNavActions.hidden = !routeEngine;
  // A new route is a new corridor: the mesh re-scopes to it, and the card
  // says what the live metric did to the answer.
  updateRouteLiveNote();
  meshFollowActiveRoute().catch(() => {});
}

function renderRouteReceipt(ms) {
  if (!routeEngine) return;
  const stats = routeEngine.stats();
  routeReceipt.hidden = false;
  routeReceipt.dataset.state = "ready";
  const what = routePlan?.kind === "trip"
    ? `${routePlan.trip.legs.length} routed legs`
    : `${formatNumber(routePlan?.candidates.length || 1)} candidate ${routePlan?.candidates.length === 1 ? "route" : "routes"}`;
  if (!stats.objectFetches) {
    routeReceiptSummary.textContent = "0 network reads · memory hit";
    routeReceiptRoute.textContent = `This browser → object cache → ${what} in ${formatNumber(ms)} ms`;
    const warm = document.createElement("p");
    warm.className = "query-receipt__warm";
    warm.textContent = "Every graph object this route needed was already resident in this tab.";
    routeReceiptBars.replaceChildren(warm);
    return;
  }
  routeReceiptSummary.textContent = `${formatNumber(stats.objectFetches)} range ${stats.objectFetches === 1 ? "read" : "reads"} · ${formatBytes(stats.bytesFetched)}`;
  const shardText = stats.shardsTouched.length
    ? `${formatNumber(stats.shardsTouched.length)} route ${stats.shardsTouched.length === 1 ? "shard" : "shards"}`
    : "route graph";
  routeReceiptRoute.textContent = `This browser → ${shardText} → ${what} in ${formatNumber(ms)} ms`;
  const rows = [
    ["Snap + query cells", stats.cellFetches],
    ["Overlay cliques", stats.overlayFetches],
    ["Path unpack cells", stats.unpackCellFetches]
  ].filter(([, count]) => count > 0);
  const maxCount = Math.max(1, ...rows.map(([, count]) => count));
  routeReceiptBars.replaceChildren(...rows.map(([label, count]) => {
    const row = document.createElement("div");
    row.className = "query-receipt__bar";
    const name = document.createElement("span");
    name.textContent = label;
    const rail = document.createElement("i");
    const fill = document.createElement("b");
    fill.style.width = `${Math.max(5, Math.round((count / maxCount) * 100))}%`;
    rail.append(fill);
    const value = document.createElement("strong");
    value.textContent = `${formatNumber(count)} ${count === 1 ? "read" : "reads"}`;
    row.append(name, rail, value);
    return row;
  }));
}

// --- Corridor search -------------------------------------------------------

function runCorridorSearch(query, label, chip) {
  const geometry = activeRouteGeometry();
  if (!geometry || !engine) return;
  for (const other of routeCard.querySelectorAll("[data-corridor]")) other.classList.toggle("active", other === chip);
  const displayQuery = `${label} along the route`;
  activeQueryOverride = {
    displayQuery,
    mode: "route corridor",
    params: {
      q: query,
      route: geometry.map(([lat, lon]) => ({ lat, lon })),
      corridorMeters: 1500
    }
  };
  queryInput.value = displayQuery;
  selectedSuggestionHint = null;
  clearButton.hidden = false;
  areaToggle.checked = false;
  runSearch({ fit: false });
  routeStepsWrap.open = false;
}

// --- Live navigation -------------------------------------------------------
//
// Turn-by-turn over the same static substrate: the position stream (real
// geolocation or a simulated demo drive replaying the route at the road
// speeds the metric itself modeled) is matched against the active route with
// rangefind's own corridor math (prepareRoute / matchPointToRoute), and going
// off route triggers a client-side reroute whose coarse polyline renders
// before the exact geometry finishes unpacking.

const atlasEl = document.querySelector(".atlas");
const navHud = document.querySelector("#navHud");
const navBanner = document.querySelector("#navBanner");
const navGlyph = document.querySelector("#navGlyph");
const navDistanceEl = document.querySelector("#navDistance");
const navInstructionEl = document.querySelector("#navInstruction");
const navThen = document.querySelector("#navThen");
const navThenGlyph = document.querySelector("#navThenGlyph");
const navThenName = document.querySelector("#navThenName");
const navFooter = document.querySelector("#navFooter");
const navProgressFill = document.querySelector("#navProgressFill");
const navEtaEl = document.querySelector("#navEta");
const navRemainingEl = document.querySelector("#navRemaining");
const navSourceTag = document.querySelector("#navSourceTag");
const navRoadChip = document.querySelector("#navRoadChip");
const navSpeedChip = document.querySelector("#navSpeedChip");
const navSpeedGroup = document.querySelector("#navSpeedGroup");
const navOffRouteButton = document.querySelector("#navOffRouteButton");
const navMuteButton = document.querySelector("#navMuteButton");
const navRecenterButton = document.querySelector("#navRecenterButton");
const navEndButton = document.querySelector("#navEndButton");
const navStepsWrap = document.querySelector("#navStepsWrap");
const navStepsEl = document.querySelector("#navSteps");
const routeNavActions = document.querySelector("#routeNavActions");
const navDemoButton = document.querySelector("#navDemoButton");
const navGpsButton = document.querySelector("#navGpsButton");

const NAV_SIM_TICK_MS = 350;
const NAV_OFF_ROUTE_METERS = 40;
const NAV_ARRIVE_METERS = 30;
const NAV_EARTH_M_PER_DEG = 111320;

let nav = null;

// Inline SVG maneuver glyphs (24x24, stroked with currentColor). Left-side
// variants mirror the right-side path with a scaleX(-1) transform.
const NAV_GLYPHS = {
  depart: '<path d="M12 21V8"/><path d="M7 12l5-6 5 6"/>',
  straight: '<path d="M12 21V8"/><path d="M7 12l5-6 5 6"/>',
  slight: '<path d="M10 21v-7l6-6"/><path d="M11 7h6v6"/>',
  turn: '<path d="M8 21v-8a3 3 0 0 1 3-3h7"/><path d="M14 5l5 5-5 5"/>',
  sharp: '<path d="M9 21V9l9 8"/><path d="M18 11v6h-6"/>',
  uturn: '<path d="M16 21v-9a4 4 0 0 0-8 0v4"/><path d="M4.5 13.5L8 18l3.5-4.5"/>',
  arrive: '<circle cx="12" cy="12" r="7.5"/><circle cx="12" cy="12" r="2.6" fill="currentColor" stroke="none"/>'
};

function maneuverGlyphSvg(maneuver) {
  const body = NAV_GLYPHS[maneuver?.type] || NAV_GLYPHS.straight;
  const mirror = maneuver?.side === "left" ? ' transform="scale(-1,1) translate(-24,0)"' : "";
  return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><g${mirror}>${body}</g></svg>`;
}

function maneuverPhrase(maneuver, { spoken = false } = {}) {
  const road = maneuver.name || (spoken ? "the unnamed road" : "unnamed road");
  switch (maneuver.type) {
    case "arrive": return spoken ? "arrive at your destination" : "Arrive at destination";
    case "uturn": return spoken ? "make a U-turn" : "Make a U-turn";
    case "straight": return `${spoken ? "continue straight onto" : "Continue onto"} ${road}`;
    case "slight": return `${spoken ? `bear ${maneuver.side} onto` : `Bear ${maneuver.side} onto`} ${road}`;
    case "sharp": return `${spoken ? `turn sharply ${maneuver.side} onto` : `Sharp ${maneuver.side} onto`} ${road}`;
    default: return `${spoken ? `turn ${maneuver.side} onto` : `Turn ${maneuver.side} onto`} ${road}`;
  }
}

function navFormatCountdown(meters) {
  if (meters <= 20) return "Now";
  if (meters >= 1000) return `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(meters / 1000)} km`;
  return `${Math.round(meters / 10) * 10} m`;
}

function offsetPointMeters(lat, lon, bearingDegrees, meters) {
  const bearing = bearingDegrees * Math.PI / 180;
  const dLat = (Math.cos(bearing) * meters) / NAV_EARTH_M_PER_DEG;
  const dLon = (Math.sin(bearing) * meters) / (NAV_EARTH_M_PER_DEG * Math.cos(lat * Math.PI / 180));
  return { lat: lat + dLat, lon: lon + dLon };
}

function pointAtProgress(prepared, meters) {
  const segments = prepared.segments;
  const target = Math.max(0, Math.min(prepared.totalMeters, meters));
  let low = 0;
  let high = segments.length - 1;
  while (low < high) {
    const mid = (low + high + 1) >> 1;
    if (segments[mid].startMeters <= target) low = mid;
    else high = mid - 1;
  }
  const segment = segments[low];
  const ratio = segment.lengthMeters ? Math.max(0, Math.min(1, (target - segment.startMeters) / segment.lengthMeters)) : 0;
  return {
    lat: segment.start.lat + (segment.end.lat - segment.start.lat) * ratio,
    lon: segment.start.lon + (segment.end.lon - segment.start.lon) * ratio,
    bearing: segment.bearingDegrees
  };
}

function classifyTurn(bearingIn, bearingOut) {
  const delta = ((bearingOut - bearingIn + 540) % 360) - 180;
  const magnitude = Math.abs(delta);
  const side = delta >= 0 ? "right" : "left";
  if (magnitude <= 28) return { type: "straight", side };
  if (magnitude <= 62) return { type: "slight", side };
  if (magnitude <= 140) return { type: "turn", side };
  if (magnitude <= 166) return { type: "sharp", side };
  return { type: "uturn", side };
}

// Precomputed per-route navigation model: the corridor-matched polyline, the
// step boundaries mapped onto it, and a classified maneuver per boundary.
function buildNavModel(route, destinationLabel, liveSeconds = null) {
  // prepareRoute reads coordinate ARRAYS in GeoJSON [lon, lat] order; route
  // geometry is [lat, lon] pairs, so hand it unambiguous {lat, lon} objects.
  const prepared = prepareRoute(route.geometry.map(([lat, lon]) => ({ lat, lon })), { corridorMeters: 1000 });
  const steps = route.steps || [];
  const stepsTotal = steps.reduce((sum, step) => sum + step.meters, 0) || 1;
  const scale = prepared.totalMeters / stepsTotal;
  const starts = [];
  const cumSeconds = [];
  let meters = 0;
  let seconds = 0;
  for (const step of steps) {
    starts.push(meters * scale);
    cumSeconds.push(seconds);
    meters += step.meters;
    seconds += step.seconds;
  }
  const totalSeconds = seconds;
  const maneuvers = new Array(steps.length + 1);
  for (let i = 1; i < steps.length; i++) {
    const boundary = starts[i];
    const inBearing = pointAtProgress(prepared, Math.max(0, boundary - 10)).bearing;
    const outBearing = pointAtProgress(prepared, Math.min(prepared.totalMeters, boundary + 10)).bearing;
    maneuvers[i] = { ...classifyTurn(inBearing, outBearing), name: steps[i].name || "" };
  }
  maneuvers[0] = { type: "depart", side: "right", name: steps[0]?.name || "" };
  maneuvers[steps.length] = { type: "arrive", side: "right", name: destinationLabel || "" };
  // `route.seconds` — and so these step timings — is the *static* cost of
  // the path the router chose: live weights steer the search but never
  // appear in the reported duration. When the caller has priced the path
  // under live states, carry that as a scale so the ETA and the countdown
  // are the honest ones rather than a free-flow fiction.
  const timeScale = Number.isFinite(liveSeconds) && liveSeconds > 0 && totalSeconds > 0
    ? liveSeconds / totalSeconds
    : 1;
  return { prepared, steps, starts, cumSeconds, totalSeconds, scale, timeScale, maneuvers, total: prepared.totalMeters, junctions: route.junctions || [], geometry: route.geometry || null };
}

// Boundary the vehicle is approaching: index i means the maneuver entering
// step i (i === steps.length is arrival). The current step is i - 1.
function navNextBoundary(model, progress) {
  for (let i = 1; i < model.starts.length; i++) {
    if (model.starts[i] > progress + 1) return i;
  }
  return model.steps.length;
}

function navRemainingSeconds(model, progress) {
  const boundary = navNextBoundary(model, progress);
  const stepIndex = boundary - 1;
  const step = model.steps[stepIndex];
  const stepStart = model.starts[stepIndex];
  const stepMeters = (step?.meters || 0) * model.scale || 1;
  const within = Math.max(0, Math.min(1, (progress - stepStart) / stepMeters));
  const elapsed = model.cumSeconds[stepIndex] + (step?.seconds || 0) * within;
  // Scaling the remainder scales both halves alike, so a live-priced
  // model counts down in live seconds without rewriting every step.
  return Math.max(0, model.totalSeconds - elapsed) * (model.timeScale || 1);
}

function navSpeak(text) {
  if (!nav || nav.muted) return;
  if (typeof speechSynthesis === "undefined" || typeof SpeechSynthesisUtterance !== "function") return;
  try {
    speechSynthesis.cancel();
    const utterance = new SpeechSynthesisUtterance(text);
    utterance.lang = "en-US";
    utterance.rate = 1.05;
    speechSynthesis.speak(utterance);
  } catch {
    // Voice guidance is strictly best-effort.
  }
}

// Traffic signals (1), stop signs (2), and level crossings (4) along the
// active route, from the engine's per-edge junction annotations. Give-way
// and pedestrian crossings exist in the data but are too dense to draw.
// Street-name labels along the active route, sliced from the route
// geometry at each named step's start index.
function roadNameFeatures(route) {
  const steps = route?.steps;
  const geometry = route?.geometry;
  if (!steps || !geometry) return [];
  const features = [];
  for (let i = 0; i < steps.length; i++) {
    if (!steps[i].name || steps[i].at == null) continue;
    const end = steps[i + 1]?.at != null ? steps[i + 1].at + 1 : geometry.length;
    const slice = geometry.slice(steps[i].at, Math.min(end, geometry.length));
    if (slice.length < 2) continue;
    features.push(lineFeature(slice, { kind: "roadname", name: steps[i].name }));
  }
  return features;
}

function activeRouteForNames() {
  if (routePlan?.kind === "pair") return routePlan.candidates[routePlan.active];
  if (routePlan?.kind === "trip") {
    return null; // per-leg steps already carry names in the leg list
  }
  return null;
}

function junctionFeatures(junctions) {
  return (junctions || [])
    .filter(junction => junction.kind === 1 || junction.kind === 2 || junction.kind === 4)
    .map(junction => ({
      type: "Feature",
      properties: { kind: "junction", jk: junction.kind },
      geometry: { type: "Point", coordinates: [junction.lon, junction.lat] }
    }));
}

function activeJunctions() {
  if (routePlan?.kind === "pair") return routePlan.candidates[routePlan.active]?.junctions || [];
  if (routePlan?.kind === "trip") return routePlan.trip.legs.flatMap(leg => leg.junctions || []);
  return [];
}

function navRouteSource() {
  if (routePlan?.kind === "pair") {
    const active = routePlan.candidates[routePlan.active];
    return active?.geometry ? active : null;
  }
  if (routePlan?.kind === "trip") {
    const geometry = activeRouteGeometry();
    if (!geometry) return null;
    return {
      geometry,
      steps: routePlan.trip.legs.flatMap(leg => leg.steps || []),
      junctions: routePlan.trip.legs.flatMap(leg => leg.junctions || []),
      seconds: routePlan.trip.totalSeconds,
      distanceMeters: routePlan.trip.totalMeters,
      bucket: routePlan.trip.legs[0]?.bucket || routeBucketNames[0] || "base"
    };
  }
  return null;
}

function drawNavRoute(geometry, { coarse = false } = {}) {
  const features = geometry
    ? [lineFeature(geometry, coarse ? { kind: "coarse" } : { kind: "active", color: ROUTE_COLORS.active })]
    : [];
  if (geometry && !coarse && nav?.model?.junctions) features.push(...junctionFeatures(nav.model.junctions));
  if (geometry && !coarse && nav?.model) features.push(...roadNameFeatures({ steps: nav.model.steps, geometry }));
  setRouteFeatures(features);
}

// Google-style covered-route dimming: split the line at the current
// progress and grey out what's behind the puck.
function drawNavProgress(progress) {
  const prepared = nav?.model?.prepared;
  if (!prepared) return;
  const points = prepared.points;
  const behind = [];
  const ahead = [];
  let placed = false;
  for (let i = 0; i < prepared.segments.length; i++) {
    const segment = prepared.segments[i];
    const segmentEnd = segment.startMeters + segment.lengthMeters;
    const from = points[segment.index];
    if (!placed) behind.push([from.lat, from.lon]);
    else ahead.push([from.lat, from.lon]);
    if (!placed && progress <= segmentEnd) {
      const ratio = segment.lengthMeters > 0 ? Math.max(0, (progress - segment.startMeters) / segment.lengthMeters) : 0;
      const to = points[segment.index + 1];
      const split = [from.lat + (to.lat - from.lat) * ratio, from.lon + (to.lon - from.lon) * ratio];
      behind.push(split);
      ahead.push(split);
      placed = true;
    }
  }
  const last = points[points.length - 1];
  ahead.push([last.lat, last.lon]);
  if (!placed) behind.push([last.lat, last.lon]);
  const features = [];
  if (behind.length > 1) features.push(lineFeature(behind, { kind: "traveled" }));
  if (ahead.length > 1) features.push(lineFeature(ahead, { kind: "active", color: ROUTE_COLORS.active }));
  features.push(...junctionFeatures(nav.model.junctions));
  const routeGeometry = nav.model.geometry;
  if (routeGeometry) features.push(...roadNameFeatures({ steps: nav.model.steps, geometry: routeGeometry }));
  setRouteFeatures(features);
}

function renderNavSteps() {
  navStepsEl.replaceChildren(...nav.model.steps.map((step, index) => {
    const item = document.createElement("li");
    item.className = "nav-step";
    const glyph = document.createElement("span");
    glyph.className = "nav-step__glyph";
    glyph.innerHTML = maneuverGlyphSvg(nav.model.maneuvers[index]);
    const name = document.createElement("span");
    name.className = "nav-step__name";
    if (step.name) name.textContent = step.name;
    else {
      name.textContent = "Unnamed road";
      name.classList.add("is-unnamed");
    }
    const meters = document.createElement("span");
    meters.className = "nav-step__meters";
    meters.textContent = formatDistance(step.meters);
    item.append(glyph, name, meters);
    return item;
  }));
}

function updateNavStepsHighlight(currentIndex) {
  const items = navStepsEl.children;
  for (let i = 0; i < items.length; i++) {
    items[i].classList.toggle("is-current", i === currentIndex);
    items[i].classList.toggle("is-done", i < currentIndex);
  }
  if (navStepsWrap.open && items[currentIndex]) {
    items[currentIndex].scrollIntoView({ block: "nearest" });
  }
}

function updateNavHud() {
  const model = nav.model;
  const progress = nav.progress;
  const boundary = navNextBoundary(model, progress);
  const maneuver = model.maneuvers[boundary];
  const distToManeuver = Math.max(0, (boundary >= model.starts.length ? model.total : model.starts[boundary]) - progress);
  navGlyph.innerHTML = maneuverGlyphSvg(maneuver);
  navDistanceEl.textContent = navFormatCountdown(distToManeuver);
  navInstructionEl.textContent = maneuverPhrase(maneuver);
  const followup = model.maneuvers[boundary + 1];
  if (followup && distToManeuver < 250) {
    navThen.hidden = false;
    navThenGlyph.innerHTML = maneuverGlyphSvg(followup);
    navThenName.textContent = followup.type === "arrive"
      ? "arrive at destination"
      : followup.name || "unnamed road";
  } else {
    navThen.hidden = true;
  }
  const remainingSeconds = navRemainingSeconds(model, progress);
  const remainingMeters = Math.max(0, model.total - progress);
  navEtaEl.textContent = formatDuration(remainingSeconds);
  const arrival = new Date(Date.now() + remainingSeconds * 1000)
    .toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  navRemainingEl.textContent = `${formatDistance(remainingMeters)} \u00b7 ${arrival}`;
  if (navRoadChip) {
    const roadName = model.steps[Math.max(0, boundary - 1)]?.name || "";
    navRoadChip.textContent = roadName;
    navRoadChip.hidden = !roadName;
  }
  if (navSpeedChip) {
    // In simulation the fixes advance at the playback multiple; show the
    // modeled vehicle speed, not the fast-forward rate.
    const divisor = nav.source === "sim" ? Math.max(1, nav.speed || 1) : 1;
    const kmh = Math.round(((nav.speedMps || 0) / divisor) * 3.6);
    navSpeedChip.textContent = `${kmh} km/h`;
    navSpeedChip.hidden = kmh <= 0;
  }
  navProgressFill.style.width = `${Math.min(100, (progress / model.total) * 100).toFixed(1)}%`;
  updateNavStepsHighlight(boundary - 1);

  // Voice guidance: a long-range cue near 300 m and a short-range cue near
  // 60 m, each spoken once per boundary.
  if (boundary !== nav.voicedBoundary) {
    nav.voicedBoundary = boundary;
    nav.voicedLevels = 0;
  }
  if (maneuver.type !== "depart") {
    // The phrase, not the boundary index, is what a live re-price can
    // carry over: re-planning from the current position renumbers every
    // boundary, but "turn left onto Rue Notre-Dame" is the same
    // instruction and must not be spoken twice.
    if (!(nav.voicedLevels & 2) && distToManeuver <= 65) {
      nav.voicedLevels |= 3;
      nav.voicedPhrase = maneuverPhrase(maneuver, { spoken: true });
      navSpeak(nav.voicedPhrase);
    } else if (!(nav.voicedLevels & 1) && distToManeuver <= 320 && distToManeuver > 65) {
      nav.voicedLevels |= 1;
      nav.voicedPhrase = maneuverPhrase(maneuver, { spoken: true });
      navSpeak(`In ${Math.round(distToManeuver / 50) * 50} meters, ${nav.voicedPhrase}`);
    }
  }
}

// The footer's height changes as its controls wrap (narrow screens) and as
// the step list opens. Publishing it as a CSS variable keeps the road pill
// and the map attribution clear of it without hardcoded offsets.
function syncNavFooterHeight() {
  const root = document.documentElement;
  if (!navFooter || navFooter.hidden) {
    root.style.removeProperty("--nav-footer-height");
    root.style.removeProperty("--nav-chrome-height");
    return;
  }
  // Space the footer occupies measured from the viewport bottom, so its own
  // bottom offset (which differs per breakpoint) is included.
  const height = Math.max(0, Math.round(window.innerHeight - navFooter.getBoundingClientRect().top));
  root.style.setProperty("--nav-footer-height", `${height}px`);
  // Attribution is lifted above the footer during navigation; measure from
  // its actual top edge so the pill also clears MapLibre's control margins.
  const attrib = document.querySelector(".maplibregl-ctrl-attrib");
  const attribTop = attrib?.getBoundingClientRect().top;
  const chrome = Number.isFinite(attribTop) && attribTop > 0
    ? Math.round(window.innerHeight - attribTop)
    : height;
  root.style.setProperty("--nav-chrome-height", `${Math.max(height, chrome)}px`);
}

if (typeof ResizeObserver === "function" && navFooter) {
  new ResizeObserver(syncNavFooterHeight).observe(navFooter);
}
window.addEventListener("resize", syncNavFooterHeight);

// Animated camera ops depend on requestAnimationFrame, which hidden tabs
// never receive — a backgrounded easeTo would freeze mid-flight. Jump there.
function navCameraTo(options, duration) {
  if (document.visibilityState === "hidden") {
    map.jumpTo(options);
    return;
  }
  map.easeTo({ ...options, duration, easing: t => t, essential: true });
}

// Google-style follow camera: high pitch, the puck anchored in the lower
// third (top padding pushes the look-ahead onto the screen), and zoom that
// backs out as speed rises. Zoom changes only at band boundaries so the
// camera never pumps.
function navFollowZoom() {
  // Google-drive framing: street-level detail in town, backing out just
  // enough on fast roads to keep the next maneuver in view. Simulation
  // playback multiplies apparent speed, so normalize by the sim rate.
  const divisor = nav.source === "sim" ? Math.max(1, nav.speed || 1) : 1;
  const speed = (nav.speedMps || 0) / divisor;
  if (speed > 19) return 16.6;
  if (speed > 8) return 17.4;
  return 18.2;
}

function updateNavCamera(lat, lon, bearing, duration) {
  if (!nav.follow) return;
  const zoom = navFollowZoom();
  navCameraTo({
    center: [lon, lat],
    bearing,
    pitch: 58,
    zoom,
    padding: { top: Math.round(map.getContainer().clientHeight * 0.42), bottom: 0, left: 0, right: 0 }
  }, duration);
}

function navHandleFix(lat, lon, headingHint) {
  if (!nav || nav.arrived) return;
  const match = matchPointToRoute(nav.model.prepared, lat, lon);
  const bearing = Number.isFinite(headingHint) ? headingHint : match.bearingDegrees;
  const nowMs = performance.now();
  if (nav.speedSample && nowMs - nav.speedSample.t > 200) {
    const metersPerSecond = Math.max(0, match.progressMeters - nav.speedSample.p) / ((nowMs - nav.speedSample.t) / 1000);
    nav.speedMps = nav.speedMps == null ? metersPerSecond : nav.speedMps * 0.6 + metersPerSecond * 0.4;
    nav.speedSample = { t: nowMs, p: match.progressMeters };
  } else if (!nav.speedSample) {
    nav.speedSample = { t: nowMs, p: match.progressMeters };
  }
  nav.lastFix = { lat, lon, bearing };
  offerFixToMesh(lat, lon, nav.speedMps ?? 0, bearing);
  nav.puck?.setLngLat([lon, lat]);
  nav.puck?.setRotation(bearing);
  updateNavCamera(lat, lon, bearing, nav.source === "sim" ? NAV_SIM_TICK_MS + 60 : 900);
  if (nav.rerouting) return;
  if (match.distanceMeters > NAV_OFF_ROUTE_METERS) {
    nav.offRouteCount++;
    if (nav.offRouteCount >= 2) startNavReroute(lat, lon);
    return;
  }
  nav.offRouteCount = 0;
  nav.progress = match.progressMeters;
  drawNavProgress(nav.progress);
  if (nav.progress >= nav.model.total - NAV_ARRIVE_METERS) {
    navArrive();
    return;
  }
  updateNavHud();
}

function navArrive() {
  nav.arrived = true;
  clearInterval(nav.simTimer);
  nav.simTimer = null;
  navBanner.dataset.state = "arrived";
  navGlyph.innerHTML = maneuverGlyphSvg({ type: "arrive" });
  navDistanceEl.textContent = "Arrived";
  navInstructionEl.textContent = nav.destinationLabel
    ? `You have arrived at ${nav.destinationLabel}`
    : "You have arrived";
  navThen.hidden = true;
  navProgressFill.style.width = "100%";
  navEtaEl.textContent = "0 min";
  navRemainingEl.textContent = "0 m";
  navSpeedGroup.hidden = true;
  navOffRouteButton.hidden = true;
  updateNavStepsHighlight(nav.model.steps.length);
  navSpeak("You have arrived at your destination");
  showToast("You have arrived — nicely driven.", "info");
}

function installNavRoute(route, { keepVoice = false, liveSeconds = null } = {}) {
  const carried = keepVoice ? { phrase: nav.voicedPhrase, levels: nav.voicedLevels } : null;
  nav.route = route;
  nav.model = buildNavModel(route, nav.destinationLabel, liveSeconds);
  nav.progress = 0;
  nav.offRouteCount = 0;
  nav.voicedBoundary = -1;
  nav.voicedLevels = 0;
  // A live re-price that lands on the same road ahead renumbers the
  // boundaries but does not change the instruction. Match on the phrase
  // and the driver is not told to turn left a second time.
  if (carried?.phrase) {
    const boundary = navNextBoundary(nav.model, 0);
    const upcoming = nav.model.maneuvers[boundary];
    const kept = carriedVoice({
      previousPhrase: carried.phrase,
      upcomingPhrase: upcoming ? maneuverPhrase(upcoming, { spoken: true }) : null,
      previousLevels: carried.levels,
      boundary
    });
    if (kept) {
      nav.voicedBoundary = kept.boundary;
      nav.voicedLevels = kept.levels;
      nav.voicedPhrase = kept.phrase;
    }
  }
  if (nav.sim) {
    nav.sim.progress = 0;
    nav.sim.veer = null;
  }
  drawNavRoute(route.geometry);
  renderNavSteps();
  updateNavHud();
  navWatchCorridor(route);
}

// --- Live re-pricing -------------------------------------------------------
//
// A route is priced once, when it is computed: `route({ live })` takes a
// snapshot and nothing re-prices it afterwards. Correct for a query,
// wrong for a drive — the jam that matters is the one that forms after
// you set off. Two things drive a re-price here: the mesh reporting that
// a segment on this corridor crossed a congestion level
// (`session.watchRoute`), and a slow timer underneath it for everything
// the mesh does not cover.
//
// *What to do with the answer* is not decided here. That is
// `repriceDecision` in src/nav_reprice.js — reluctance thresholds, what
// counts as a different road, and the edge cases (a "faster" route that
// came back slower, a re-price on a finished drive) that no amount of
// clicking around a demo would reach. This function is the part that
// talks to the driver.
const NAV_REPRICE_SECONDS = 45;

/** Watch the corridor this route runs on, replacing any previous watch. */
function navWatchCorridor(route) {
  nav.watch?.stop();
  nav.watch = null;
  nav.watch = pulseMesh?.watchRoute?.(route, {
    debounceSeconds: 20,
    onChange: change => {
      if (nav && shouldRepriceNow(change)) startNavReprice().catch(() => {});
    }
  }) ?? null;
}

async function startNavReprice() {
  if (!nav || !routeEngine || nav.rerouting || nav.repricing || nav.arrived) return;
  const fix = nav.lastFix;
  if (!fix) return;
  nav.repricing = true;
  const generation = nav.generation;
  try {
    const remainingSeconds = navRemainingSeconds(nav.model, nav.progress);
    const candidate = await routeEngine.route({
      from: { lat: fix.lat, lon: fix.lon },
      to: nav.destination,
      ...departureParams()
    });
    if (!nav || nav.generation !== generation || nav.rerouting || nav.arrived) return;

    // Both paths have to be priced the same way or the comparison is
    // meaningless — `route.seconds` is the *static* cost of whatever the
    // live weights steered the search onto, so it cannot be weighed
    // against a live-priced road. Price both through the router's own
    // blend, over the states the candidate was routed with.
    const provider = pulseMesh?.provider?.() ?? null;
    const states = provider
      ? await provider.fetch({ epoch: pulseMesh.epoch, areas: [], maxAgeSeconds: 120 }).catch(() => [])
      : [];
    if (!nav || nav.generation !== generation) return;
    const nowMillis = Date.now();
    const ahead = remainingPath(nav.route, nav.progress / (nav.model.scale || 1));
    const candidatePath = remainingPath(candidate, 0);
    const candidateLiveSeconds = states.length
      ? livePathSeconds(candidatePath, states, { nowMillis })
      : null;

    const { action, gain, etaShift } = repriceDecision({
      remainingSeconds,
      candidateSeconds: candidate.seconds,
      currentLiveSeconds: states.length ? livePathSeconds(ahead, states, { nowMillis }) : null,
      candidateLiveSeconds,
      currentSegments: segmentsOf(nav.route),
      candidateSegments: candidatePath
    });

    if (action === "refresh") {
      // The way ahead is the same; only what it costs has moved. Refresh
      // the ETA the driver is watching, and say nothing about turns.
      installNavRoute(candidate, { keepVoice: true, liveSeconds: candidateLiveSeconds });
      showToast(etaShift < 0
        ? `Traffic ahead · ${formatDuration(-etaShift)} added`
        : `Clearing ahead · ${formatDuration(etaShift)} saved`, "info");
      return;
    }
    if (action !== "switch") return;

    routePlan = { kind: "pair", candidates: [candidate], active: 0 };
    installNavRoute(candidate, { liveSeconds: candidateLiveSeconds });
    showToast(`Faster route · ${formatDuration(gain)} saved`, "info");
    navSpeak(`Faster route ahead, saving ${Math.max(1, Math.round(gain / 60))} minutes`);
  } catch {
    // A failed re-price is a missed opportunity, never a dead drive.
  } finally {
    if (nav && nav.generation === generation) nav.repricing = false;
  }
}

async function startNavReroute(lat, lon) {
  if (!routeEngine || nav.rerouting) return;
  nav.rerouting = true;
  nav.offRouteCount = 0;
  navBanner.dataset.state = "rerouting";
  navGlyph.innerHTML = maneuverGlyphSvg({ type: "straight" });
  navDistanceEl.textContent = "Rerouting…";
  navInstructionEl.textContent = "Computing a new route from your position";
  navThen.hidden = true;
  navSpeak("Rerouting");
  const generation = nav.generation;
  try {
    const route = await routeEngine.route({
      from: { lat, lon },
      to: nav.destination,
      // Live traffic, like every other route call in this file. Leaving
      // it off here priced the one moment it matters most — the driver
      // is moving and has just left the plan — off the static graph
      // alone, while departureParams() claimed otherwise.
      ...departureParams(),
      onCoarseRoute: coarse => {
        if (nav && nav.generation === generation) drawNavRoute(coarse.geometry, { coarse: true });
      }
    });
    if (!nav || nav.generation !== generation) return;
    // The rerouted path replaces the whole plan so exiting navigation shows it.
    routePlan = { kind: "pair", candidates: [route], active: 0 };
    installNavRoute(route);
    navBanner.dataset.state = "";
    showToast(`Rerouted — ${formatDuration(route.seconds)} to destination`, "info");
    setTimeout(() => { if (nav && !nav.muted) navSpeak(maneuverPhrase(nav.model.maneuvers[navNextBoundary(nav.model, 0)] || nav.model.maneuvers[0], { spoken: true })); }, 250);
  } catch (error) {
    if (!nav || nav.generation !== generation) return;
    navBanner.dataset.state = "";
    showToast(error?.code === "RANGEFIND_ROUTE_SNAP_TOO_FAR"
      ? "Can't reroute yet — no road near your position."
      : "Rerouting failed — trying again shortly.", "error");
    // Back off a little before the next attempt.
    nav.offRouteCount = -4;
  } finally {
    if (nav && nav.generation === generation) nav.rerouting = false;
  }
}

function navSimTick() {
  if (!nav?.sim || nav.arrived) return;
  const sim = nav.sim;
  // Wall-clock dt: hidden tabs throttle timers, so a fixed per-tick step
  // would slow the drive down. Clamp to keep resumes from teleporting.
  const now = performance.now();
  const elapsed = Math.min(2, (now - (sim.lastTick || now)) / 1000) || NAV_SIM_TICK_MS / 1000;
  sim.lastTick = now;
  const dt = elapsed * nav.speed;
  if (sim.veer) {
    sim.veer.dist += 11 * dt;
    const pos = offsetPointMeters(sim.veer.lat, sim.veer.lon, sim.veer.bearing, sim.veer.dist);
    navHandleFix(pos.lat, pos.lon, sim.veer.bearing);
    return;
  }
  const model = nav.model;
  const boundary = navNextBoundary(model, sim.progress);
  const step = model.steps[boundary - 1];
  const speed = step && step.seconds > 0 ? Math.max(3.5, (step.meters * model.scale) / step.seconds) : 12;
  sim.progress = Math.min(model.total, sim.progress + speed * dt);
  const point = pointAtProgress(model.prepared, sim.progress);
  // GPS-like jitter: a small smoothed random walk, in meters.
  sim.jitterX = sim.jitterX * 0.72 + (Math.random() - 0.5) * 2.4;
  sim.jitterY = sim.jitterY * 0.72 + (Math.random() - 0.5) * 2.4;
  const lat = point.lat + sim.jitterY / NAV_EARTH_M_PER_DEG;
  const lon = point.lon + sim.jitterX / (NAV_EARTH_M_PER_DEG * Math.cos(point.lat * Math.PI / 180));
  navHandleFix(lat, lon, point.bearing);
}

function navSetSpeed(multiplier) {
  if (!nav) return;
  nav.speed = multiplier;
  for (const option of navSpeedGroup.querySelectorAll("[data-nav-speed]")) {
    option.classList.toggle("active", Number(option.dataset.navSpeed) === multiplier);
  }
}

function startNavigation(source) {
  const route = navRouteSource();
  if (!route || !routeEngine || nav) return;
  const destinationStop = stops[stops.length - 1];
  const destination = destinationStop?.place
    ? { lat: destinationStop.place.lat, lon: destinationStop.place.lon }
    : { lat: route.geometry[route.geometry.length - 1][0], lon: route.geometry[route.geometry.length - 1][1] };
  nav = {
    source,
    generation: Date.now(),
    route,
    destination,
    destinationLabel: destinationStop?.place?.label || "",
    muted: false,
    follow: true,
    speed: 4,
    progress: 0,
    offRouteCount: 0,
    rerouting: false,
    repricing: false,
    arrived: false,
    voicedBoundary: -1,
    voicedLevels: 0,
    voicedPhrase: null,
    sim: null,
    simTimer: null,
    watchId: null,
    // The mesh corridor watch, and the slow timer under it for road the
    // mesh does not cover.
    watch: null,
    repriceTimer: null,
    puck: null
  };
  nav.model = buildNavModel(route, nav.destinationLabel);
  // Watch the corridor from the first metre, and keep a slow beat under
  // it: the mesh only speaks for road somebody is driving, and the rest
  // of the route still has to be re-priced from time to time.
  navWatchCorridor(route);
  nav.repriceTimer = setInterval(() => { startNavReprice().catch(() => {}); }, NAV_REPRICE_SECONDS * 1000);
  cancelStopPick();
  hidePlaceLens();
  clearCorridorResults();
  hideSuggestionsForNav();
  atlasEl.classList.add("nav-active");
  navHud.hidden = false;
  navFooter.hidden = false;
  syncNavFooterHeight();
  navBanner.dataset.state = "";
  navStepsWrap.open = false;
  navSpeedGroup.hidden = source !== "sim";
  navOffRouteButton.hidden = source !== "sim";
  navRecenterButton.hidden = true;
  navMuteButton.setAttribute("aria-pressed", "false");
  navMuteButton.textContent = "Voice on";
  navSourceTag.textContent = source === "sim" ? "demo drive" : "live gps";
  navSetSpeed(4);
  drawNavRoute(route.geometry);
  renderNavSteps();

  const startPoint = pointAtProgress(nav.model.prepared, 0);
  const element = document.createElement("div");
  element.className = "nav-puck";
  const cone = document.createElement("i");
  cone.className = "nav-puck__cone";
  const dot = document.createElement("i");
  dot.className = "nav-puck__dot";
  element.append(cone, dot);
  nav.puck = new maplibregl.Marker({ element, rotationAlignment: "map", pitchAlignment: "map" })
    .setLngLat([startPoint.lon, startPoint.lat])
    .setRotation(startPoint.bearing)
    .addTo(map);
  navCameraTo({
    center: [startPoint.lon, startPoint.lat],
    zoom: 18.2,
    pitch: 58,
    bearing: startPoint.bearing,
    padding: { top: Math.round(map.getContainer().clientHeight * 0.42), bottom: 0, left: 0, right: 0 }
  }, 1100);
  updateNavHud();

  if (source === "sim") {
    nav.sim = { progress: 0, veer: null, jitterX: 0, jitterY: 0 };
    nav.simTimer = setInterval(navSimTick, NAV_SIM_TICK_MS);
    navSpeak("Starting demo drive");
  } else {
    navDistanceEl.textContent = "—";
    navInstructionEl.textContent = "Waiting for a GPS fix…";
    nav.watchId = navigator.geolocation.watchPosition(
      position => {
        const { latitude, longitude, heading } = position.coords;
        if (!nav || nav.source !== "gps") return;
        if (routeCoverage && !insideCoverage({ lat: latitude, lon: longitude }, 0.05)) {
          showToast(
            `Your GPS fix is outside the ${routeRegionLabel || "open"} route graph — switching to the demo drive.`,
            "error",
            5200
          );
          switchNavToSim();
          return;
        }
        navHandleFix(latitude, longitude, Number.isFinite(heading) ? heading : undefined);
      },
      error => {
        if (!nav || nav.source !== "gps") return;
        showToast(error?.code === 1
          ? "Location permission denied — try the demo drive instead."
          : "No GPS fix available — try the demo drive instead.", "error", 5200);
        endNavigation();
      },
      { enableHighAccuracy: true, maximumAge: 1000, timeout: 15000 }
    );
  }
}

function switchNavToSim() {
  if (!nav) return;
  if (nav.watchId != null) {
    navigator.geolocation.clearWatch(nav.watchId);
    nav.watchId = null;
  }
  nav.source = "sim";
  nav.sim = { progress: nav.progress, veer: null, jitterX: 0, jitterY: 0 };
  clearInterval(nav.simTimer);
  nav.simTimer = setInterval(navSimTick, NAV_SIM_TICK_MS);
  navSpeedGroup.hidden = false;
  navOffRouteButton.hidden = false;
  navSourceTag.textContent = "demo drive";
}

// Suggest dropdowns must not linger under the hidden panel.
function hideSuggestionsForNav() {
  hideSuggestions();
  for (const stop of stops) hideStopSuggest(stop);
}

function endNavigation() {
  if (!nav) return;
  clearInterval(nav.simTimer);
  clearInterval(nav.repriceTimer);
  nav.watch?.stop();
  if (nav.watchId != null) navigator.geolocation.clearWatch(nav.watchId);
  if (typeof speechSynthesis !== "undefined") {
    try { speechSynthesis.cancel(); } catch { /* best effort */ }
  }
  nav.puck?.remove();
  nav = null;
  atlasEl.classList.remove("nav-active");
  navHud.hidden = true;
  navFooter.hidden = true;
  if (navRoadChip) navRoadChip.hidden = true;
  syncNavFooterHeight();
  navBanner.dataset.state = "";
  navCameraTo({ pitch: 0, bearing: 0, padding: { top: 0, bottom: 0, left: 0, right: 0 } }, 700);
  updateStopMarkers();
  drawRoutePlan();
  renderRouteCard();
  fitRoutePlan();
  const seconds = routePlan?.kind === "pair"
    ? routePlan.candidates[routePlan.active].seconds
    : routePlan?.trip.totalSeconds;
  if (Number.isFinite(seconds)) setStatus(`Route ready · ${formatDuration(seconds)}`);
  mapHudText.textContent = "Navigation ended";
}

navDemoButton.addEventListener("click", () => startNavigation("sim"));
navGpsButton.addEventListener("click", () => {
  if (!navigator.geolocation) {
    showToast("Geolocation is unavailable in this browser — try the demo drive.", "error");
    return;
  }
  startNavigation("gps");
});
navEndButton.addEventListener("click", endNavigation);
navMuteButton.addEventListener("click", () => {
  if (!nav) return;
  nav.muted = !nav.muted;
  navMuteButton.setAttribute("aria-pressed", String(nav.muted));
  navMuteButton.textContent = nav.muted ? "Voice off" : "Voice on";
  if (nav.muted && typeof speechSynthesis !== "undefined") {
    try { speechSynthesis.cancel(); } catch { /* best effort */ }
  }
});
navRecenterButton.addEventListener("click", () => {
  if (!nav) return;
  nav.follow = true;
  navRecenterButton.hidden = true;
  if (nav.lastFix) updateNavCamera(nav.lastFix.lat, nav.lastFix.lon, nav.lastFix.bearing, 600);
});
navOffRouteButton.addEventListener("click", () => {
  if (!nav?.sim || nav.arrived || nav.sim.veer) return;
  const here = pointAtProgress(nav.model.prepared, nav.sim.progress);
  nav.sim.veer = {
    lat: here.lat,
    lon: here.lon,
    bearing: (here.bearing + (Math.random() < 0.5 ? 55 : -55) + 360) % 360,
    dist: 0
  };
  showToast("Veering off the route — watch the reroute.", "info");
});
for (const option of navSpeedGroup.querySelectorAll("[data-nav-speed]")) {
  option.addEventListener("click", () => navSetSpeed(Number(option.dataset.navSpeed)));
}
map.on("dragstart", () => {
  if (!nav || !nav.follow) return;
  nav.follow = false;
  navRecenterButton.hidden = false;
});

// --- Directions events -----------------------------------------------------

modeSearchTab.addEventListener("click", () => setDirectionsMode(false));
modeDirectionsTab.addEventListener("click", () => setDirectionsMode(true));

addStopButton.addEventListener("click", () => {
  if (stops.length >= MAX_STOPS) return;
  stops.push(newStop());
  renderStops();
  updateStopMarkers();
  stops[stops.length - 1].input.focus();
});

swapStopsButton.addEventListener("click", () => {
  cancelStopPick();
  stops.reverse();
  for (const stop of stops) stop.resolveToken++;
  renderStops();
  updateStopMarkers();
  maybeRoute();
});

clearStopsButton.addEventListener("click", () => {
  cancelStopPick();
  routeToken++;
  stops = [newStop(), newStop()];
  routePlan = null;
  clearRouteLayers();
  clearStopMarkers();
  renderStops();
  clearCorridorResults();
  directionsEmptyState();
  setStatus("Plot a route", "ready");
  updateDirectionsHud();
  stops[0].input.focus();
});

for (const option of departureRow.querySelectorAll("[data-departure]")) {
  option.addEventListener("click", () => {
    if (departureChoice === option.dataset.departure) return;
    departureChoice = option.dataset.departure;
    for (const other of departureRow.querySelectorAll("[data-departure]")) {
      const active = other === option;
      other.classList.toggle("active", active);
      other.setAttribute("aria-pressed", String(active));
    }
    if (directionsReady()) computeRoute();
  });
}

for (const option of tripEndRow?.querySelectorAll("[data-trip-end]") || []) {
  option.addEventListener("click", () => {
    if (tripEndChoice === option.dataset.tripEnd) return;
    tripEndChoice = option.dataset.tripEnd;
    for (const other of tripEndRow.querySelectorAll("[data-trip-end]")) {
      const active = other === option;
      other.classList.toggle("active", active);
      other.setAttribute("aria-pressed", String(active));
    }
    maybeRoute();
  });
}

for (const chip of routeCard.querySelectorAll("[data-corridor]")) {
  chip.addEventListener("click", () => runCorridorSearch(
    chip.dataset.corridor,
    chip.dataset.corridorLabel,
    chip
  ));
}

map.on("click", event => {
  if (!directionsMode || !routeAvailable) return;
  // Alternative polylines own their own click (promotion).
  if (map.getLayer("route-alt")) {
    if (map.queryRenderedFeatures(event.point, { layers: ["route-alt"] }).length) return;
  }
  const lat = Number(event.lngLat?.lat);
  const lon = Number(event.lngLat?.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
  const target = stopPickIndex >= 0 ? stopPickIndex : stops.findIndex(stop => !stop.place);
  if (target < 0) return;
  cancelStopPick();
  setStopFromMap(target, lat, lon);
});

document.addEventListener("keydown", event => {
  if (event.key !== "Escape" || !directionsMode) return;
  if (nav) {
    endNavigation();
    return;
  }
  if (!placeLens.hidden) return; // the base handler closes the lens first
  if (document.activeElement?.closest?.(".stop-row")) return; // input handles it
  if (stopPickIndex >= 0) {
    cancelStopPick();
    updateDirectionsHud();
    return;
  }
  setDirectionsMode(false);
});

// --- Bottom sheet (phones) -------------------------------------------
//
// On narrow screens the panel becomes a draggable sheet with three snap
// points, so the map keeps most of the screen while directions are open.
// Dragging tracks the finger 1:1; releasing snaps to whichever point the
// gesture is heading for (position, biased by flick velocity).

const SHEET_BREAKPOINT = 720;
const SHEET_SNAPS = { peek: 0.26, half: 0.55, full: 0.88 };
const sheetHandle = document.querySelector("#sheetHandle");
let sheetSnap = "half";
let sheetDrag = null;

function sheetEnabled() {
  return window.innerWidth <= SHEET_BREAKPOINT;
}

// Peek must clear the panel header plus whichever controls are active —
// a fixed fraction clips the destination field in directions mode.
function peekHeight() {
  const header = searchPanel.querySelector(".panel-header");
  const controls = directionsMode ? directionsControls : searchControls;
  const chrome = (sheetHandle?.offsetHeight || 0)
    + (header?.offsetHeight || 0)
    + (controls && !controls.hidden ? controls.offsetHeight : 0);
  const min = SHEET_SNAPS.peek * window.innerHeight;
  const max = SHEET_SNAPS.half * window.innerHeight;
  return Math.min(max, Math.max(min, chrome + 16));
}

function applySheetSnap(snap, { persist = true } = {}) {
  if (persist) sheetSnap = snap;
  searchPanel.dataset.snap = snap;
  const height = snap === "peek" ? peekHeight() : SHEET_SNAPS[snap] * window.innerHeight;
  searchPanel.style.setProperty("--sheet-height", `${Math.round(height)}px`);
}

// Programmatic snap used by flow transitions (entering directions, a route
// becoming ready). No-op on desktop.
function snapSheet(snap) {
  if (!searchPanel.classList.contains("is-sheet")) return;
  applySheetSnap(snap);
}

function syncSheetMode() {
  const on = sheetEnabled() && !searchPanel.classList.contains("is-collapsed");
  searchPanel.classList.toggle("is-sheet", on);
  if (on) applySheetSnap(sheetSnap);
  else {
    searchPanel.style.removeProperty("--sheet-height");
    delete searchPanel.dataset.snap;
  }
}

function nearestSnap(heightPx, velocity, travelPx = 0) {
  const ratio = heightPx / window.innerHeight;
  // A short quick flick steps one stop, the way native sheets behave. A
  // long drag is deliberate, so its end position wins instead.
  const shortFlick = travelPx < window.innerHeight * 0.16;
  if (shortFlick && Math.abs(velocity) > 0.55) {
    const order = ["peek", "half", "full"];
    const index = order.indexOf(sheetSnap);
    const next = velocity < 0 ? index + 1 : index - 1;
    return order[Math.min(order.length - 1, Math.max(0, next))];
  }
  let best = "half";
  let bestDelta = Infinity;
  for (const [name, target] of Object.entries(SHEET_SNAPS)) {
    const delta = Math.abs(ratio - target);
    if (delta < bestDelta) {
      bestDelta = delta;
      best = name;
    }
  }
  return best;
}

if (sheetHandle) {
  sheetHandle.addEventListener("pointerdown", event => {
    if (!searchPanel.classList.contains("is-sheet")) return;
    sheetHandle.setPointerCapture(event.pointerId);
    sheetDrag = {
      startY: event.clientY,
      startHeight: searchPanel.getBoundingClientRect().height,
      lastY: event.clientY,
      lastT: event.timeStamp,
      velocity: 0,
      moved: false
    };
    searchPanel.classList.add("is-dragging");
  });

  sheetHandle.addEventListener("pointermove", event => {
    if (!sheetDrag) return;
    const dy = event.clientY - sheetDrag.startY;
    if (Math.abs(dy) > 3) sheetDrag.moved = true;
    const dt = event.timeStamp - sheetDrag.lastT;
    if (dt > 0) sheetDrag.velocity = (event.clientY - sheetDrag.lastY) / dt;
    sheetDrag.lastY = event.clientY;
    sheetDrag.lastT = event.timeStamp;
    // Dragging up grows the sheet; clamp within the snap range.
    const min = SHEET_SNAPS.peek * window.innerHeight * 0.7;
    const max = SHEET_SNAPS.full * window.innerHeight;
    const height = Math.min(max, Math.max(min, sheetDrag.startHeight - dy));
    searchPanel.style.setProperty("--sheet-height", `${Math.round(height)}px`);
  });

  const endSheetDrag = (event) => {
    if (!sheetDrag) return;
    const drag = sheetDrag;
    sheetDrag = null;
    searchPanel.classList.remove("is-dragging");
    if (sheetHandle.hasPointerCapture?.(event.pointerId)) sheetHandle.releasePointerCapture(event.pointerId);
    if (!drag.moved) {
      // Tap cycles the snap points for keyboard and non-drag users.
      const order = ["peek", "half", "full"];
      applySheetSnap(order[(order.indexOf(sheetSnap) + 1) % order.length]);
      return;
    }
    const travel = Math.abs(drag.lastY - drag.startY);
    applySheetSnap(nearestSnap(searchPanel.getBoundingClientRect().height, drag.velocity, travel));
  };
  sheetHandle.addEventListener("pointerup", endSheetDrag);
  sheetHandle.addEventListener("pointercancel", endSheetDrag);
}

window.addEventListener("resize", syncSheetMode);
syncSheetMode();

async function boot() {
  loadIndexStatus();
  initRouteGraph();
  // Cloudflare (this index's CDN) rejects multipart byte ranges with a 400,
  // so probing multi-range would waste one guaranteed round trip on the very
  // first grouped read; the runtime's per-origin downgrade only kicks in
  // after that first failure.
  engine = await createSearch({ baseUrl: OSM_INDEX_BASE_URL, multiRangeRequests: false });
  const total = engine.manifest.total || 0;
  const shardCount = engine.shards?.length || 1;
  const builtAt = formatDate(engine.manifest.built_at);
  placeMetric.textContent = formatCompact(total);
  placeMetric.title = `${formatNumber(total)} indexed places`;
  regionMetric.textContent = formatNumber(shardCount);
  indexMeta.textContent = `${builtAt ? `Manifest ${builtAt}. ` : ""}Queries run entirely in this browser over immutable HTTP byte ranges.`;

  await runSearch();
}

boot().catch(error => {
  const message = error?.message || "Index failed to load";
  indexMeta.textContent = message;
  livePill.dataset.state = "error";
  liveState.textContent = "Offline";
  setStatus(message, "error");
  mapHudText.textContent = "Index unavailable";
  showEmpty("The atlas is offline", "The public manifest could not be loaded. Try refreshing in a moment.");
});
