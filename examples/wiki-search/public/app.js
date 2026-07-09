import { createSearch } from "./runtime.browser.js";

const form = document.querySelector("#searchForm");
const queryInput = document.querySelector("#queryInput");
const sortSelect = document.querySelector("#sortSelect");
const sizeSelect = document.querySelector("#sizeSelect");
const categoryInput = document.querySelector("#categoryInput");
const bodyLengthInput = document.querySelector("#bodyLengthInput");
const hasCategoriesInput = document.querySelector("#hasCategoriesInput");
const longBodyInput = document.querySelector("#longBodyInput");
const shortTitleInput = document.querySelector("#shortTitleInput");
const resultsEl = document.querySelector("#results");
const resultSummary = document.querySelector("#resultSummary");
const correctionSummary = document.querySelector("#correctionSummary");
const runtimeStats = document.querySelector("#runtimeStats");
const docCount = document.querySelector("#docCount");
const resultTemplate = document.querySelector("#resultTemplate");

let engine;
let siteMeta = {};

// Semantic hybrid search: the query embeds in the browser with the same
// MiniLM model the build used, loaded lazily on first use.
const semanticLabel = document.querySelector("#semanticLabel");
const semanticInput = document.querySelector("#semanticInput");
let embedderPromise = null;

async function loadEmbedder() {
  if (!embedderPromise) {
    embedderPromise = (async () => {
      resultSummary.textContent = "Loading semantic model (first use only)...";
      const { pipeline } = await import("https://cdn.jsdelivr.net/npm/@huggingface/transformers@3.3.1");
      return pipeline("feature-extraction", "Xenova/all-MiniLM-L6-v2", { dtype: "q8" });
    })();
    embedderPromise.catch(() => {
      embedderPromise = null;
    });
  }
  return embedderPromise;
}

async function embedQuery(q) {
  const extractor = await loadEmbedder();
  const output = await extractor(q, { pooling: "mean", normalize: true });
  const vector = Array.from(output.data);
  output.dispose?.();
  return vector;
}

function formatNumber(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const order = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  return `${(bytes / (1024 ** order)).toFixed(order ? 1 : 0)} ${units[order]}`;
}

function escapeText(value) {
  return String(value || "");
}

function readUrlState() {
  const params = new URLSearchParams(location.search);
  if (params.has("q")) queryInput.value = params.get("q") || "";
  if (params.has("sort")) sortSelect.value = params.get("sort") || "relevance";
  if (params.has("size")) sizeSelect.value = params.get("size") || "10";
  if (params.has("category")) categoryInput.value = params.get("category") || "";
  if (params.has("bodyLength")) bodyLengthInput.value = params.get("bodyLength") || "";
  hasCategoriesInput.checked = params.get("hasCategories") === "1";
  longBodyInput.checked = params.get("longBody") === "1";
  shortTitleInput.checked = params.get("shortTitle") === "1";
}

function writeUrlState() {
  const params = new URLSearchParams();
  if (queryInput.value.trim()) params.set("q", queryInput.value.trim());
  if (sortSelect.value !== "relevance") params.set("sort", sortSelect.value);
  if (sizeSelect.value !== "10") params.set("size", sizeSelect.value);
  if (categoryInput.value.trim()) params.set("category", categoryInput.value.trim());
  if (bodyLengthInput.value.trim()) params.set("bodyLength", bodyLengthInput.value.trim());
  if (hasCategoriesInput.checked) params.set("hasCategories", "1");
  if (longBodyInput.checked) params.set("longBody", "1");
  if (shortTitleInput.checked) params.set("shortTitle", "1");
  history.replaceState(null, "", `${location.pathname}${params.size ? `?${params}` : ""}`);
}

function searchParams() {
  const filters = { facets: {}, numbers: {}, booleans: {} };
  const category = categoryInput.value.trim();
  const minBodyLength = Number(bodyLengthInput.value);
  const tags = [];

  if (category) filters.facets.category = [category];
  if (hasCategoriesInput.checked) filters.booleans.hasCategories = true;
  if (Number.isFinite(minBodyLength) && minBodyLength > 0) filters.numbers.bodyLength = { min: minBodyLength };
  if (longBodyInput.checked) tags.push("long-body");
  if (shortTitleInput.checked) tags.push("short-title");
  if (tags.length) filters.facets.articleTags = tags;

  const sort = sortSelect.value === "newest"
    ? { field: "revisionDate", order: "desc" }
    : sortSelect.value === "oldest"
      ? { field: "revisionDate", order: "asc" }
      : null;

  return {
    q: queryInput.value.trim(),
    size: Number(sizeSelect.value) || 10,
    filters,
    sort,
    highlight: { fields: ["title", "body"], maxChars: 300 },
    facets: { fields: ["category"], size: 8 },
    trace: true
  };
}

// Highlights arrive as plain text plus offset ranges, so rendering builds
// text nodes and <mark> elements without any HTML passing through.
function renderHighlighted(node, highlight, fallbackText) {
  if (!highlight?.ranges?.length) {
    node.textContent = fallbackText;
    return;
  }
  node.replaceChildren();
  let cursor = 0;
  for (const [start, end] of highlight.ranges) {
    if (start > cursor) node.append(highlight.text.slice(cursor, start));
    const mark = document.createElement("mark");
    mark.textContent = highlight.text.slice(start, end);
    node.append(mark);
    cursor = end;
  }
  if (cursor < highlight.text.length) node.append(highlight.text.slice(cursor));
}

function resultMeta(item) {
  return [
    item.category || "Uncategorized",
    item.revisionDate || "",
    `${formatNumber(item.bodyLength)} chars`,
    item.score != null ? `score ${Number(item.score).toFixed(3)}` : ""
  ].filter(Boolean).join(" / ");
}

function renderResults(items) {
  resultsEl.replaceChildren();
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "No matching articles.";
    resultsEl.append(empty);
    return;
  }

  for (const item of items) {
    const node = resultTemplate.content.firstElementChild.cloneNode(true);
    const title = node.querySelector(".result-title");
    const meta = node.querySelector(".result-meta");
    const body = node.querySelector(".result-body");
    const chips = node.querySelector(".chip-row");

    title.href = item.url || "#";
    renderHighlighted(title, item.highlights?.title, escapeText(item.title || item.id));
    meta.textContent = resultMeta(item);
    renderHighlighted(body, item.highlights?.body, escapeText(item.body || "").slice(0, 520));

    // Display payloads flatten arrays to strings, so guard the chip list.
    const categoryChips = Array.isArray(item.categoryList)
      ? item.categoryList
      : item.category
        ? [item.category]
        : [];
    for (const value of categoryChips.slice(0, 6)) {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.textContent = value;
      chips.append(chip);
    }
    node.dataset.articleId = item.articleId || item.id || "";
    resultsEl.append(node);
  }
}

function renderStats(response, elapsedMs) {
  const stats = response.stats || {};
  const trace = stats.trace || {};
  const fetches = trace.fetches || trace.requests || 0;
  const bytes = trace.bytes || trace.transferBytes || 0;
  const entries = [
    `${elapsedMs.toFixed(1)} ms`,
    stats.plannerLane || "",
    stats.totalExact === false ? "approx total" : "",
    fetches ? `${fetches} requests` : "",
    bytes ? formatBytes(bytes) : ""
  ].filter(Boolean);
  runtimeStats.replaceChildren(...entries.map(value => {
    const pill = document.createElement("span");
    pill.className = "stat-pill";
    pill.textContent = value;
    return pill;
  }));
}

function renderFacetChips(facet) {
  let row = document.querySelector("#facetChips");
  if (!row) {
    row = document.createElement("div");
    row.id = "facetChips";
    row.className = "chip-row facet-chip-row";
    resultSummary.parentElement.after(row);
  }
  row.replaceChildren();
  if (!facet?.values?.length) return;
  for (const item of facet.values) {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "chip facet-chip";
    const active = categoryInput.value.trim() === item.value;
    if (active) chip.classList.add("active");
    chip.textContent = `${item.label} ${facet.exact ? "" : "~"}${formatNumber(item.count)}`;
    chip.addEventListener("click", () => {
      categoryInput.value = active ? "" : item.value;
      runSearch();
    });
    row.append(chip);
  }
}

async function runSearch() {
  if (!engine) return;
  writeUrlState();
  const params = searchParams();
  resultSummary.textContent = "Searching...";
  correctionSummary.textContent = "";
  try {
    if (semanticInput?.checked && params.q && engine.manifest.features?.vectors) {
      params.vector = await embedQuery(params.q);
    }
  } catch (error) {
    resultSummary.textContent = `Semantic model failed to load (${error?.message || error}); using text search`;
  }
  const started = performance.now();
  try {
    const response = await engine.search(params);
    const elapsedMs = performance.now() - started;
    const total = response.approximate ? `${formatNumber(response.total)}+` : formatNumber(response.total);
    resultSummary.textContent = `${total} results`;
    correctionSummary.textContent = response.correctedQuery ? `Showing ${response.correctedQuery}` : "";
    renderStats(response, elapsedMs);
    renderResults(response.results || []);
    renderFacetChips(response.facets?.category);
  } catch (error) {
    resultSummary.textContent = "Search failed";
    runtimeStats.replaceChildren();
    resultsEl.innerHTML = "";
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = error?.message || "Index request failed.";
    resultsEl.append(empty);
  }
}

async function loadSiteMeta() {
  try {
    const response = await fetch("./site-meta.json", { cache: "no-store" });
    if (response.ok) siteMeta = await response.json();
  } catch {
    siteMeta = {};
  }
}

async function boot() {
  readUrlState();
  await loadSiteMeta();
  engine = await createSearch({ baseUrl: new URL("./rangefind/", location.href).href });
  const docs = siteMeta.docs || engine.manifest.total || 0;
  docCount.textContent = docs ? formatNumber(docs) : "-";
  if (engine.manifest.features?.vectors) semanticLabel.hidden = false;
  await runSearch();
}

const suggestList = document.querySelector("#suggestList");
let suggestTimer = null;
let suggestToken = 0;

function hideSuggestions() {
  suggestList.hidden = true;
  suggestList.replaceChildren();
}

async function showSuggestions() {
  const q = queryInput.value.trim();
  const token = ++suggestToken;
  const hasAuthorityTitles = engine?.manifest?.authority?.fields?.length === 1
    && engine.manifest.authority.fields[0].path === "title"
    && engine.manifest.authority.fields[0].exact !== false;
  if ((!engine?.manifest?.features?.suggest && !hasAuthorityTitles) || q.length < 1) {
    hideSuggestions();
    return;
  }
  try {
    const response = await engine.suggest({ q, size: 8 });
    if (token !== suggestToken) return;
    if (!response.suggestions.length) {
      hideSuggestions();
      return;
    }
    suggestList.replaceChildren(...response.suggestions.map(item => {
      const li = document.createElement("li");
      li.textContent = item.text;
      li.addEventListener("mousedown", event => {
        event.preventDefault();
        queryInput.value = item.text;
        hideSuggestions();
        runSearch();
      });
      return li;
    }));
    suggestList.hidden = false;
  } catch {
    hideSuggestions();
  }
}

queryInput.addEventListener("input", () => {
  clearTimeout(suggestTimer);
  suggestTimer = setTimeout(showSuggestions, 80);
});
queryInput.addEventListener("blur", () => setTimeout(hideSuggestions, 150));
queryInput.addEventListener("keydown", event => {
  if (event.key === "Escape") hideSuggestions();
});

form.addEventListener("submit", event => {
  event.preventDefault();
  hideSuggestions();
  runSearch();
});

for (const control of [sortSelect, sizeSelect, hasCategoriesInput, longBodyInput, shortTitleInput, semanticInput]) {
  control.addEventListener("change", runSearch);
}

boot().catch(error => {
  resultSummary.textContent = "Index failed to load";
  const empty = document.createElement("div");
  empty.className = "empty-state";
  empty.textContent = error?.message || "Rangefind manifest could not be loaded.";
  resultsEl.append(empty);
});
