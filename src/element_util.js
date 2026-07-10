// Pure, DOM-free helpers for <rangefind-search>. Kept separate from the custom
// element so the branching logic (attribute parsing, class merging, highlight
// segmentation, snippet selection, keyboard navigation) is unit-testable under
// plain `node --test` without a DOM shim.

// Namespaced class hooks for every rendered part. Light DOM means the host
// page's CSS (Tailwind, global stylesheets, the optional theme) applies
// directly, and these stable hooks give authors something to target. The keys
// are the camelCase part names used by the `.classNames` JS property; the
// values are the BEM-ish classes always present on the element.
export const PART_HOOKS = {
  root: "rf-search",
  input: "rf-search__input",
  panel: "rf-search__panel",
  list: "rf-search__list",
  option: "rf-search__option",
  optionTitle: "rf-search__option-title",
  optionSnippet: "rf-search__option-snippet",
  optionUrl: "rf-search__option-url",
  empty: "rf-search__empty",
  status: "rf-search__status",
  suggest: "rf-search__suggest",
  suggestItem: "rf-search__suggest-item",
  mark: "rf-search__mark"
};

// The HTML attribute that injects extra classes onto each part, e.g.
// `input-class="w-full border rounded"`. Applied additively on top of the hook.
export const PART_CLASS_ATTRS = {
  root: "root-class",
  input: "input-class",
  panel: "panel-class",
  list: "list-class",
  option: "option-class",
  optionTitle: "option-title-class",
  optionSnippet: "option-snippet-class",
  optionUrl: "option-url-class",
  empty: "empty-class",
  status: "status-class",
  suggest: "suggest-class",
  suggestItem: "suggest-item-class",
  mark: "mark-class"
};

// Fields probed (in order) when choosing a result's snippet / title. The first
// present string field wins; the index's actual display fields decide what is
// available (crawler indexes commonly expose `excerpt`/`body`, the basic
// example exposes `body`).
export const SNIPPET_FIELDS = ["excerpt", "summary", "description", "body", "content", "text", "snippet"];
export const TITLE_FIELDS = ["title", "name", "heading", "label"];

// Splits every part on whitespace, keeps first occurrence, drops duplicates.
// The hook class is passed first so it is always present and always leads.
export function mergeClassNames(...parts) {
  const seen = new Set();
  const out = [];
  for (const part of parts) {
    if (part == null) continue;
    for (const token of String(part).split(/\s+/u)) {
      if (!token || seen.has(token)) continue;
      seen.add(token);
      out.push(token);
    }
  }
  return out.join(" ");
}

// Final class string for a part: namespaced hook + `*-class` attribute value +
// `.classNames[part]`, merged and de-duplicated with the hook leading.
export function partClass(part, { attrs = {}, classNames = {} } = {}) {
  const hook = PART_HOOKS[part];
  if (!hook) return "";
  const attrName = PART_CLASS_ATTRS[part];
  const fromAttr = attrName ? attrs[attrName] : "";
  return mergeClassNames(hook, fromAttr, classNames[part]);
}

// Boolean attribute parsing. Absent → fallback; a bare attribute (`highlight`)
// or truthy token → true; `false`/`0`/`off`/`no` → false. This is what makes
// `highlight="false"` turn highlighting off while `highlight` alone leaves the
// default on.
export function parseBoolAttr(value, fallback = false) {
  if (value == null) return fallback;
  const v = String(value).trim().toLowerCase();
  if (v === "") return true;
  if (v === "false" || v === "0" || v === "off" || v === "no") return false;
  if (v === "true" || v === "1" || v === "on" || v === "yes") return true;
  return fallback;
}

// Integer attribute parsing with a clamp. Blank/absent/non-numeric → fallback.
export function parseIntAttr(value, fallback, { min = -Infinity, max = Infinity } = {}) {
  if (value == null || String(value).trim() === "") return fallback;
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

// The `suggest` attribute is tri-state: "auto" (default) enables autocomplete
// only when the index actually ships an authority autocomplete lexicon;
// explicit true/false
// force it on/off.
export function parseSuggestAttr(value) {
  if (value == null) return "auto";
  const v = String(value).trim().toLowerCase();
  if (v === "" || v === "auto") return "auto";
  if (v === "false" || v === "0" || v === "off" || v === "no") return false;
  if (v === "true" || v === "1" || v === "on" || v === "yes") return true;
  return "auto";
}

// Resolves the full attribute map into a normalized config object. `attrs` is a
// plain name→value map (values are strings, or null when the attribute is
// absent), so this stays testable without an Element.
export function resolveConfig(attrs = {}) {
  const get = name => (name in attrs ? attrs[name] : null);
  return {
    src: get("src") || "",
    placeholder: get("placeholder") ?? "Search",
    label: get("label") || "Search",
    pageSize: parseIntAttr(get("page-size"), 8, { min: 1, max: 100 }),
    debounce: parseIntAttr(get("debounce"), 150, { min: 0, max: 5000 }),
    minLength: parseIntAttr(get("min-length"), 1, { min: 0, max: 100 }),
    highlight: parseBoolAttr(get("highlight"), true),
    suggest: parseSuggestAttr(get("suggest")),
    router: parseBoolAttr(get("router"), false),
    openOnFocus: parseBoolAttr(get("open-on-focus"), false),
    hotkey: parseBoolAttr(get("hotkey"), false),
    emptyText: get("empty-text") ?? "No results",
    loadingText: get("loading-text") ?? "Searching…",
    errorText: get("error-text") ?? "Search is unavailable."
  };
}

// Turns a highlight `{ text, ranges }` into an ordered list of
// `{ text, mark }` segments the renderer converts to text nodes and <mark>
// elements. Ranges are sanitized: non-arrays and NaN dropped, offsets clamped
// to the text bounds, empty/reversed ranges removed, then sorted and merged so
// overlapping or out-of-order ranges produce clean, non-nested marks. Because
// only slices of the original text are emitted (never markup), the caller can
// render them as text and no HTML injection is possible.
export function buildHighlightSegments(highlight) {
  const text = typeof highlight?.text === "string" ? highlight.text : "";
  if (!text) return [];
  const len = text.length;
  const raw = Array.isArray(highlight?.ranges) ? highlight.ranges : [];
  const clean = [];
  for (const range of raw) {
    if (!Array.isArray(range)) continue;
    let start = Math.floor(Number(range[0]));
    let end = Math.floor(Number(range[1]));
    if (!Number.isFinite(start) || !Number.isFinite(end)) continue;
    start = Math.max(0, Math.min(len, start));
    end = Math.max(0, Math.min(len, end));
    if (end <= start) continue;
    clean.push([start, end]);
  }
  clean.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  const merged = [];
  for (const [start, end] of clean) {
    const last = merged[merged.length - 1];
    if (last && start <= last[1]) last[1] = Math.max(last[1], end);
    else merged.push([start, end]);
  }
  const segments = [];
  let cursor = 0;
  for (const [start, end] of merged) {
    if (start > cursor) segments.push({ text: text.slice(cursor, start), mark: false });
    segments.push({ text: text.slice(start, end), mark: true });
    cursor = end;
  }
  if (cursor < len) segments.push({ text: text.slice(cursor), mark: false });
  return segments;
}

// Word-boundary truncation with an ellipsis, used for plain (un-highlighted)
// snippet fallbacks.
export function truncate(text, maxLength = 200) {
  const s = String(text ?? "");
  if (s.length <= maxLength) return s;
  const slice = s.slice(0, maxLength);
  const lastSpace = slice.lastIndexOf(" ");
  const cut = lastSpace > maxLength * 0.6 ? slice.slice(0, lastSpace) : slice;
  return `${cut.replace(/[\s,.;:!?–—-]+$/u, "")}…`;
}

// Picks the field to show as the option title.
export function pickTitleField(result, preferred = TITLE_FIELDS) {
  for (const field of preferred) {
    if (typeof result?.[field] === "string" && result[field]) return field;
  }
  return null;
}

// Chooses the snippet to render under an option title. Prefers a highlighted
// non-title field (so the matched terms show), in `preferred` order and then
// any other highlighted field; otherwise falls back to a plain display string
// field, truncated. Returns `{ field, text, ranges, highlighted }` or null.
export function pickSnippet(result, { titleField = null, preferred = SNIPPET_FIELDS, maxLength = 200 } = {}) {
  const highlights = result?.highlights && typeof result.highlights === "object" ? result.highlights : {};
  const highlightOrder = [...preferred, ...Object.keys(highlights)];
  const seen = new Set();
  for (const field of highlightOrder) {
    if (field === titleField || seen.has(field)) continue;
    seen.add(field);
    const hit = highlights[field];
    if (hit && typeof hit.text === "string" && hit.text) {
      return { field, text: hit.text, ranges: Array.isArray(hit.ranges) ? hit.ranges : [], highlighted: true };
    }
  }
  for (const field of preferred) {
    if (field === titleField) continue;
    const value = result?.[field];
    if (typeof value === "string" && value) {
      return { field, text: truncate(value, maxLength), ranges: [], highlighted: false };
    }
  }
  return null;
}

// Keyboard navigation reducer for the listbox. Given the current `active`
// option index, option `count`, and whether the panel is `open`, returns the
// next `{ active, open }`. ArrowDown/Up wrap; Home/End jump; Escape closes and
// clears the active option; an empty list can only close.
export function reduceNav(state, key) {
  const count = Math.max(0, Math.floor(Number(state?.count) || 0));
  const open = Boolean(state?.open);
  const active = Number.isInteger(state?.active) ? state.active : -1;
  if (key === "Escape") return { active: -1, open: false };
  if (count === 0) return { active: -1, open };
  switch (key) {
    case "ArrowDown":
      return { active: active < 0 ? 0 : (active + 1) % count, open: true };
    case "ArrowUp":
      return { active: active <= 0 ? count - 1 : active - 1, open: true };
    case "Home":
      return { active: 0, open: true };
    case "End":
      return { active: count - 1, open: true };
    default:
      return { active: Math.min(active, count - 1), open };
  }
}
