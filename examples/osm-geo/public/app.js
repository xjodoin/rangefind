import { createSearch } from "./runtime.browser.js";
import { searchOsmQuery, suggestOsmQuery } from "./osm.browser.js";

const queryInput = document.querySelector("#queryInput");
const suggestList = document.querySelector("#suggestList");
const resultList = document.querySelector("#resultList");
const statusLine = document.querySelector("#statusLine");
const indexMeta = document.querySelector("#indexMeta");
const areaToggle = document.querySelector("#areaToggle");
const rqaAttribution = document.querySelector("#rqaAttribution");

let engine;
let markers = [];
let moveTimer = null;
let suggestTimer = null;
let suggestToken = 0;
let searchToken = 0;
let suggestionsSuppressed = false;
let suppressedQuery = "";

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
    layers: [{ id: "osm", type: "raster", source: "osm" }]
  },
  center: [-73.6, 45.53],
  zoom: 12
});
map.addControl(new maplibregl.NavigationControl(), "top-right");

function formatNumber(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function viewportBox() {
  const bounds = map.getBounds();
  return {
    minLat: bounds.getSouth(),
    maxLat: bounds.getNorth(),
    minLon: bounds.getWest(),
    maxLon: bounds.getEast()
  };
}

function looksLikeAddress(value) {
  const text = String(value || "");
  // Postal-only queries are global identities too. They cannot be inferred
  // from the house-number heuristic because a suffix such as `1Z5` contains
  // a second digit after its letter.
  if (CANADIAN_POSTAL_CODE.test(text)) return true;
  const tokens = text.trim().split(/[^\p{L}\p{N}-]+/u).filter(Boolean);
  return tokens.length >= 2 && tokens.some(token => /^\d+[\p{L}]?(?:-\d+[\p{L}]?)?$/u.test(token));
}

function clearMarkers() {
  for (const marker of markers) marker.remove();
  markers = [];
}

function renderResults(results, { fit = false } = {}) {
  clearMarkers();
  resultList.replaceChildren();
  const bounds = new maplibregl.LngLatBounds();
  for (const item of results) {
    const hasPoint = Number.isFinite(item.lat) && Number.isFinite(item.lon);
    if (hasPoint) {
      const marker = new maplibregl.Marker({ color: "#2a7a4b" })
        .setLngLat([item.lon, item.lat])
        .setPopup(new maplibregl.Popup({ closeButton: false }).setText(item.name || item.title || item.id));
      marker.addTo(map);
      markers.push(marker);
      bounds.extend([item.lon, item.lat]);
    }
    const li = document.createElement("li");
    const name = document.createElement("div");
    name.className = "name";
    name.textContent = item.name || item.title || item.id;
    const meta = document.createElement("div");
    meta.className = "meta";
    const addressCount = Number(item.address_count);
    meta.textContent = [
      item.type ? String(item.type).replaceAll("_", " ") : item.category,
      Number.isFinite(addressCount) && addressCount > 0
        ? `${formatNumber(addressCount)} civic ${addressCount === 1 ? "address" : "addresses"}`
        : "",
      item.distanceMeters != null ? `${formatNumber(Math.round(item.distanceMeters))} m` : ""
    ].filter(Boolean).join(" · ");
    li.append(name);
    if (item.address && item.address !== (item.name || item.title)) {
      const address = document.createElement("div");
      address.className = "address";
      address.textContent = item.address;
      li.append(address);
    }
    li.append(meta);
    li.addEventListener("click", () => {
      if (hasPoint) map.flyTo({ center: [item.lon, item.lat], zoom: Math.max(map.getZoom(), 15) });
    });
    resultList.append(li);
  }
  if (fit && !bounds.isEmpty()) map.fitBounds(bounds, { padding: 80, maxZoom: 15 });
}

async function runSearch({ fit = false } = {}) {
  if (!engine) return;
  const token = ++searchToken;
  const q = queryInput.value.trim();
  // Exact addresses are global identities. Keeping a stale viewport filter on
  // them disables the zero-posting authority lanes and can hide the requested
  // result; ordinary place/browse queries remain map-bounded.
  const addressLookup = looksLikeAddress(q);
  const areaWasChecked = areaToggle.checked;
  const useArea = areaToggle.checked && !addressLookup;
  if (addressLookup && areaWasChecked) areaToggle.checked = false;
  const params = { q, size: 30 };
  if (useArea) params.geo = { box: viewportBox() };
  if (!q && !useArea) {
    resultList.replaceChildren();
    clearMarkers();
    statusLine.textContent = "";
    return;
  }
  const started = performance.now();
  try {
    const response = await searchOsmQuery(engine, params);
    if (token !== searchToken) return;
    const resolvedLocation = response.stats?.plannerLane === "osmCategoryLocality"
      || response.stats?.plannerLane === "osmLocalityExact"
      || response.stats?.plannerLane === "osmStreetLocality";
    if (resolvedLocation) areaToggle.checked = false;
    renderResults(response.results, { fit: fit || resolvedLocation || (addressLookup && areaWasChecked) });
    const ms = Math.round(performance.now() - started);
    statusLine.textContent = `${formatNumber(response.total)}${response.approximate ? "+" : ""} matches · ${ms} ms`;
  } catch (error) {
    if (token === searchToken) statusLine.textContent = error?.message || "Search failed";
  }
}

function hideSuggestions() {
  suggestList.hidden = true;
  suggestList.replaceChildren();
}

function cancelSuggestions() {
  clearTimeout(suggestTimer);
  suggestToken++;
  suggestionsSuppressed = true;
  suppressedQuery = queryInput.value;
  hideSuggestions();
}

async function showSuggestions() {
  const q = queryInput.value.trim();
  const token = ++suggestToken;
  if (!q || suggestionsSuppressed) {
    hideSuggestions();
    return;
  }
  try {
    const response = await suggestOsmQuery(engine, { q, size: 8 });
    if (token !== suggestToken || suggestionsSuppressed) return;
    if (!response.suggestions.length) {
      hideSuggestions();
      return;
    }
    suggestList.replaceChildren(...response.suggestions.map(item => {
      const li = document.createElement("li");
      const text = document.createElement("span");
      text.textContent = item.text;
      const count = document.createElement("span");
      count.className = "count";
      count.textContent = item.type === "street"
        ? "street"
        : item.interpolated
        ? "interpolated"
        : item.count > 1 ? `×${formatNumber(item.count)}` : "";
      li.append(text, count);
      li.addEventListener("mousedown", event => {
        event.preventDefault();
        queryInput.value = item.text;
        hideSuggestions();
        // A picked suggestion searches the whole index and flies to the hits.
        areaToggle.checked = false;
        runSearch({ fit: true });
      });
      return li;
    }));
    suggestList.hidden = false;
  } catch {
    hideSuggestions();
  }
}

queryInput.addEventListener("input", () => {
  if (queryInput.value !== suppressedQuery) {
    suggestionsSuppressed = false;
    suppressedQuery = "";
  }
  clearTimeout(suggestTimer);
  if (suggestionsSuppressed) return;
  suggestTimer = setTimeout(showSuggestions, 70);
});
queryInput.addEventListener("keydown", event => {
  if (event.key === "Enter") {
    cancelSuggestions();
    runSearch({ fit: !areaToggle.checked });
  }
  if (event.key === "Escape") cancelSuggestions();
});
queryInput.addEventListener("blur", () => setTimeout(hideSuggestions, 150));
areaToggle.addEventListener("change", () => runSearch());

map.on("moveend", () => {
  if (!areaToggle.checked) return;
  clearTimeout(moveTimer);
  moveTimer = setTimeout(() => runSearch(), 250);
});

async function boot() {
  engine = await createSearch({ baseUrl: new URL("./rangefind/", location.href).href });
  const total = engine.manifest.total || 0;
  const geoField = engine.manifest.geo?.fields?.location;
  indexMeta.textContent = `${formatNumber(total)} places · ${formatNumber(geoField?.total || 0)} geo points · static index over HTTP ranges`;
  let demoView = null;
  try {
    const response = await fetch(new URL("./osm-demo.json", location.href));
    if (response.ok) demoView = await response.json();
  } catch {
    // Custom deployments without fixture metadata retain the bbox fallback.
  }
  if (Array.isArray(demoView?.center) && demoView.center.length === 2) {
    map.jumpTo({ center: demoView.center, zoom: Number(demoView.zoom || 11) });
  } else if (geoField?.bbox) {
    const { minLatE7, maxLatE7, minLonE7, maxLonE7 } = geoField.bbox;
    map.jumpTo({ center: [((minLonE7 + maxLonE7) / 2) / 1e7, ((minLatE7 + maxLatE7) / 2) / 1e7], zoom: 11 });
  }
  rqaAttribution.hidden = !demoView?.rqa;
  await runSearch();
}

boot().catch(error => {
  indexMeta.textContent = error?.message || "Index failed to load";
});
