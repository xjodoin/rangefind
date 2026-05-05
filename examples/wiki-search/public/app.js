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
    trace: true
  };
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
    title.textContent = escapeText(item.title || item.id);
    meta.textContent = resultMeta(item);
    body.textContent = escapeText(item.body || "").slice(0, 520);

    for (const value of (item.categoryList || []).slice(0, 6)) {
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

async function runSearch() {
  if (!engine) return;
  writeUrlState();
  const params = searchParams();
  resultSummary.textContent = "Searching...";
  correctionSummary.textContent = "";
  const started = performance.now();
  try {
    const response = await engine.search(params);
    const elapsedMs = performance.now() - started;
    const total = response.approximate ? `${formatNumber(response.total)}+` : formatNumber(response.total);
    resultSummary.textContent = `${total} results`;
    correctionSummary.textContent = response.correctedQuery ? `Showing ${response.correctedQuery}` : "";
    renderStats(response, elapsedMs);
    renderResults(response.results || []);
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
  await runSearch();
}

form.addEventListener("submit", event => {
  event.preventDefault();
  runSearch();
});

for (const control of [sortSelect, sizeSelect, hasCategoriesInput, longBodyInput, shortTitleInput]) {
  control.addEventListener("change", runSearch);
}

boot().catch(error => {
  resultSummary.textContent = "Index failed to load";
  const empty = document.createElement("div");
  empty.className = "empty-state";
  empty.textContent = error?.message || "Rangefind manifest could not be loaded.";
  resultsEl.append(empty);
});
