import { createSearch } from "./runtime.browser.js";
import { searchOsmQuery, suggestOsmQuery } from "./osm.browser.js";

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
const placeLensTitle = document.querySelector("#placeLensTitle");
const placeLensCopy = document.querySelector("#placeLensCopy");
const placeLensClose = document.querySelector("#placeLensClose");

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
const suggestionCache = new Map();

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
    sources: {
      osm: {
        type: "raster",
        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
        tileSize: 256,
        attribution: "© OpenStreetMap contributors"
      }
    },
    layers: [{
      id: "osm",
      type: "raster",
      source: "osm",
      paint: {
        "raster-saturation": -0.62,
        "raster-contrast": 0.12,
        "raster-brightness-min": 0.22,
        "raster-brightness-max": 0.9
      }
    }]
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

function searchAnchor() {
  if (userLocation) return { ...userLocation, source: "you" };
  const center = map.getCenter();
  return Number.isFinite(center?.lat) && Number.isFinite(center?.lng)
    ? { lat: center.lat, lon: center.lng, source: "map view" }
    : null;
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

function resultLocation(item) {
  const name = item.name || item.title || item.id;
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

function openPlaceLens(item) {
  if (!Number.isFinite(item?.lat) || !Number.isFinite(item?.lon)) return;
  selectedPlace = item;
  const title = item.name || item.title || item.id;
  const location = resultLocation(item);
  const kind = humanize(item.type || item.category || "place");
  placeLensTitle.textContent = title;
  placeLensCopy.textContent = `${kind}${location ? ` · ${location}` : ""}. Search within a 2.5 km orbit.`;
  placeLens.hidden = false;
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
}

function markerFor(item, index) {
  const title = item.name || item.title || item.id;
  const location = resultLocation(item);
  const accessibleLabel = location ? `${title}, ${location}` : title;
  const element = document.createElement("button");
  element.className = "result-marker";
  element.type = "button";
  element.setAttribute("aria-label", accessibleLabel);
  const label = document.createElement("span");
  label.textContent = index + 1;
  element.append(label);
  const marker = new maplibregl.Marker({ element, anchor: "bottom" })
    .setLngLat([item.lon, item.lat])
    .setPopup(new maplibregl.Popup({ closeButton: false, offset: 18 }).setText(accessibleLabel))
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

  for (const [index, item] of visibleResults.entries()) {
    const hasPoint = Number.isFinite(item.lat) && Number.isFinite(item.lon);
    const markerEntry = hasPoint ? markerFor(item, index) : null;
    if (markerEntry) {
      markers.push(markerEntry);
      bounds.extend([item.lon, item.lat]);
    }

    const listItem = document.createElement("li");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "result-card";
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
    const parts = [
      item.type ? String(item.type).replaceAll("_", " ") : item.category,
      Number.isFinite(addressCount) && addressCount > 0
        ? `${formatCompact(addressCount)} civic ${addressCount === 1 ? "address" : "addresses"}`
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

    const arrow = document.createElement("span");
    arrow.className = "result-card__arrow";
    arrow.textContent = "→";
    arrow.setAttribute("aria-hidden", "true");
    button.append(number, body, arrow);

    const flyToResult = () => {
      if (hasPoint) {
        map.flyTo({ center: [item.lon, item.lat], zoom: Math.max(map.getZoom(), 15), essential: true });
        markerEntry.marker.togglePopup();
      }
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
    map.fitBounds(bounds, { padding: resultFitPadding(), maxZoom: 15, duration: 700 });
  }
}

async function runSearch({ fit = false } = {}) {
  if (!engine) return;
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
    const resolvedLocation = response.stats?.plannerLane === "osmCategoryLocality"
      || response.stats?.plannerLane === "osmLocalityExact"
      || response.stats?.plannerLane === "osmStreetLocality";
    if (resolvedLocation) areaToggle.checked = false;
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
      || response.stats?.plannerLane === "osmNearText";
    const nearText = nearLane && anchor ? ` · near ${anchor.source}` : "";
    const modeText = queryOverride?.mode ? ` · ${queryOverride.mode}` : "";
    const shown = Math.min(response.results.length, resultLimit());
    const timing = `${(ms / 1000).toFixed(1)}s${shardText}`;
    setStatus(response.total
      ? `Showing ${formatNumber(shown)} of ${formatNumber(response.total)}${response.approximate ? "+" : ""} matches · ${timing}${directText}${nearText}${modeText}`
      : `No matches · ${timing}${directText}${nearText}${modeText}`);
    renderQueryReceipt(response, shown);
    mapHudText.textContent = `${formatNumber(shown)} ${shown === 1 ? "result" : "results"} · ${useArea ? "this map area" : "everywhere"}`;
  } catch (error) {
    if (token !== searchToken) return;
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
  queryInput.value = item.text;
  activeQueryOverride = null;
  selectedSuggestionHint = item.shards?.length
    ? { query: item.text.trim(), shards: [...item.shards] }
    : null;
  clearButton.hidden = false;
  hideSuggestions();
  areaToggle.checked = false;
  runSearch({ fit: true });
}

function adoptExactSuggestionHint() {
  const query = queryInput.value.trim();
  if (!query || selectedSuggestionHint?.query === query) return;
  const exact = visibleSuggestions.find(item => (
    item.shards?.length
    && item.text.localeCompare(query, undefined, { sensitivity: "base" }) === 0
  ));
  if (exact) selectedSuggestionHint = { query, shards: [...exact.shards] };
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
  const q = queryInput.value.trim();
  const token = ++suggestToken;
  if (!engine || Array.from(q).length < SUGGEST_MIN_CHARACTERS || suggestionsSuppressed) {
    hideSuggestions();
    return;
  }
  const cacheKey = q.toLocaleLowerCase();
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
    const response = await suggestOsmQuery(engine, { q, size: 8 });
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

function renderSuggestions(response) {
  if (!response.suggestions.length) {
    hideSuggestions();
    return;
  }
  visibleSuggestions = response.suggestions;
  suggestList.replaceChildren(...response.suggestions.map((item, index) => {
    const option = document.createElement("li");
    option.id = `suggestion-${index}`;
    option.setAttribute("role", "option");
    option.setAttribute("aria-selected", "false");
    const text = document.createElement("span");
    text.textContent = item.text;
    const count = document.createElement("span");
    count.className = "count";
    count.textContent = item.type === "street"
      ? "street"
      : item.interpolated
        ? "interpolated"
        : item.shards?.length
          ? suggestionRegionLabel(item.shards)
          : item.count > 1 ? `×${formatNumber(item.count)}` : "place";
    option.append(text, count);
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
      adoptExactSuggestionHint();
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
  adoptExactSuggestionHint();
  cancelSuggestions();
  runSearch({ fit: !areaToggle.checked });
});

panelToggle.addEventListener("click", () => {
  setPanelCollapsed(!searchPanel.classList.contains("is-collapsed"));
});

placeLensClose.addEventListener("click", hidePlaceLens);

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
  const shortcut = event.key === "/" && document.activeElement !== queryInput;
  const command = (event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k";
  if (!shortcut && !command) return;
  event.preventDefault();
  setPanelCollapsed(false);
  queryInput.focus();
  queryInput.select();
});

map.on("movestart", () => {
  if (areaToggle.checked && engine) mapHudText.textContent = "New viewport detected";
});

map.on("moveend", () => {
  if (!areaToggle.checked || !engine) return;
  clearTimeout(moveTimer);
  moveTimer = setTimeout(() => runSearch(), 250);
});

async function boot() {
  loadIndexStatus();
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
