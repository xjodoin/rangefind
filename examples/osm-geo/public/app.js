import { createSearch } from "./runtime.browser.js";
import {
  decodePolyline,
  matchPointToRoute,
  prepareRoute,
  reverseGeocodeOsm,
  searchOsmQuery,
  suggestOsmQuery
} from "./osm.browser.js";
import { openRouteGraphUrl } from "./route.browser.js";
import { createPulseMeshDemo } from "./pulsemesh-demo.js";
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
  "osmCoordinates"
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
  ["osmCoordinates", "coordinate"]
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

function beginQueryReceipt() {
  queryReceipt.hidden = false;
  queryReceipt.open = false;
  queryReceipt.dataset.state = "loading";
  queryReceiptSummary.textContent = "Tracing byte ranges…";
  queryReceiptRoute.textContent = "Following the request from this browser into the static index.";
  queryReceiptBars.replaceChildren();
}

function renderQueryReceipt(response, shown) {
  const trace = response.stats?.trace;
  if (!trace) {
    queryReceipt.hidden = true;
    return;
  }
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
  hidePlaceLens();

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
    queryReceipt.hidden = true;
    resultList.replaceChildren();
    clearMarkers();
    setStatus("Ready to search", "ready");
    mapHudText.textContent = "Search anywhere in the published index";
    showEmpty("Find a place", "Search by name or address, try “pharmacy near me”, or a category with a place such as “pharmacy in Birmingham”.");
    return;
  }

  beginQueryReceipt();
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
    renderQueryReceipt(response, shown);
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
  runSearch({ fit: true });
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
    return option;
  }));
  suggestList.hidden = false;
  queryInput.setAttribute("aria-expanded", "true");
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

let routeEngine = null;
let routeAvailable = null; // null = probing, false = missing, true = ready
let routeCoverage = null;
let routeBucketNames = [];
let directionsMode = false;
let stops = [];
let stopPickIndex = -1;
let departureChoice = "now";
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

async function initRouteGraph() {
  try {
    const probe = await fetch("route-graph/manifest.json");
    if (!probe.ok) throw new Error(`route-graph manifest ${probe.status}`);
    routeEngine = await openRouteGraphUrl("route-graph/");
    let minLat = Infinity;
    let maxLat = -Infinity;
    let minLon = Infinity;
    let maxLon = -Infinity;
    for (const leaf of routeEngine.root.leaves) {
      if (leaf.bbox.minLat < minLat) minLat = leaf.bbox.minLat;
      if (leaf.bbox.maxLat > maxLat) maxLat = leaf.bbox.maxLat;
      if (leaf.bbox.minLon < minLon) minLon = leaf.bbox.minLon;
      if (leaf.bbox.maxLon > maxLon) maxLon = leaf.bbox.maxLon;
    }
    routeCoverage = { minLat: minLat / 1e7, maxLat: maxLat / 1e7, minLon: minLon / 1e7, maxLon: maxLon / 1e7 };
    routeBucketNames = routeEngine.root.buckets.map(bucket => bucket.name);
    routeAvailable = true;
    departureRow.hidden = routeBucketNames.length < 2;
    modeDirectionsTab.title = "Turn-by-turn routing computed in this browser";
  } catch {
    routeAvailable = false;
    modeDirectionsTab.setAttribute("aria-disabled", "true");
    modeDirectionsTab.title = "No route graph published at route-graph/";
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
  setStatus("Route graph not published", "error");
  showEmpty("Directions needs a route index", "Publish a route graph at route-graph/ using the commands above, then reload.");
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
  if (routeCoverage && !insideCoverage(place)) {
    stop.error = "coverage";
    setStopNote(stop, "Outside the routable area", "error");
    showToast("The route graph covers Luxembourg only — pick a point inside it.", "error");
    updateStopMarkers();
    maybeRoute();
    return;
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

let pulseMesh = null;

async function togglePulseMesh(on) {
  if (!routeEngine) return null;
  if (!on) {
    if (pulseMesh) await pulseMesh.stop();
    pulseMesh = null;
    return null;
  }
  if (!pulseMesh) {
    pulseMesh = createPulseMeshDemo({
      engine: routeEngine,
      // A keeper multiaddr in the URL joins the real mesh; without one the
      // demo runs peers inside this tab so it works on any static host.
      mode: new URLSearchParams(location.search).get("keeper") ? "keeper" : "local",
      keeperAddress: new URLSearchParams(location.search).get("keeper")
    });
    await pulseMesh.start();
  }
  return pulseMesh;
}

/**
 * Reports the jam the active route is driving into, then re-routes. The
 * contributions are made by simulated vehicles in local mode; on a real
 * mesh this is what other drivers' phones would already have said.
 */
async function reportPulseMeshJam() {
  const active = routePlan?.candidates?.[routePlan.active];
  if (!pulseMesh || !active?.edges?.length) return null;
  pulseMesh.followRoute(active);
  const segments = active.edges
    .slice(Math.floor(active.edges.length * 0.3), Math.floor(active.edges.length * 0.3) + 15)
    .map(edge => edge.segment);
  const published = await pulseMesh.simulateJam(segments);
  return { published, ...pulseMesh.refreshState() };
}

if (typeof window !== "undefined") {
  // Exposed so the page (and anyone poking at the console) can drive the
  // mesh without this module owning any UI.
  window.rangefindPulseMesh = {
    enable: () => togglePulseMesh(true),
    disable: () => togglePulseMesh(false),
    report: reportPulseMeshJam,
    state: () => pulseMesh?.refreshState() ?? null,
    jams: () => pulseMesh?.jammedSegments() ?? [],
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
      const trip = await routeEngine.itinerary({ stops: points, roundTrip: false, ...departureParams() });
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
function buildNavModel(route, destinationLabel) {
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
  return { prepared, steps, starts, cumSeconds, totalSeconds, scale, maneuvers, total: prepared.totalMeters, junctions: route.junctions || [], geometry: route.geometry || null };
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
  return Math.max(0, model.totalSeconds - elapsed);
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
    if (!(nav.voicedLevels & 2) && distToManeuver <= 65) {
      nav.voicedLevels |= 3;
      navSpeak(maneuverPhrase(maneuver, { spoken: true }));
    } else if (!(nav.voicedLevels & 1) && distToManeuver <= 320 && distToManeuver > 65) {
      nav.voicedLevels |= 1;
      navSpeak(`In ${Math.round(distToManeuver / 50) * 50} meters, ${maneuverPhrase(maneuver, { spoken: true })}`);
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

function installNavRoute(route) {
  nav.route = route;
  nav.model = buildNavModel(route, nav.destinationLabel);
  nav.progress = 0;
  nav.offRouteCount = 0;
  nav.voicedBoundary = -1;
  nav.voicedLevels = 0;
  if (nav.sim) {
    nav.sim.progress = 0;
    nav.sim.veer = null;
  }
  drawNavRoute(route.geometry);
  renderNavSteps();
  updateNavHud();
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
      bucket: nav.route.bucket,
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
    arrived: false,
    voicedBoundary: -1,
    voicedLevels: 0,
    sim: null,
    simTimer: null,
    watchId: null,
    puck: null
  };
  nav.model = buildNavModel(route, nav.destinationLabel);
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
          showToast("Your GPS fix is outside the Luxembourg route graph — switching to the demo drive.", "error", 5200);
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
  engine = await createSearch({ baseUrl: OSM_INDEX_BASE_URL });
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
