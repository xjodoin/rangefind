// Pure, Eleventy-free markup builder for the `rangefindSearch` shortcode. Kept
// separate from the plugin registration so the branching logic (attribute
// normalization, boolean handling, escaping, asset-base normalization) is
// unit-testable under plain `node --test` without booting Eleventy.

// The real attribute vocabulary understood by <rangefind-search>, mirrored from
// src/element_util.js in the Rangefind monorepo (CONFIG_ATTRS +
// PART_CLASS_ATTRS). These are the only names the element observes; anything
// else is ignored by the component. We keep the list here so callers get a
// warning when they pass an unrecognized name rather than silently rendering a
// dead attribute — we do not invent new names.
export const CONFIG_ATTRS = [
  "src", "placeholder", "page-size", "debounce", "min-length", "highlight",
  "suggest", "router", "open-on-focus", "hotkey", "label", "empty-text",
  "loading-text", "error-text"
];

export const PART_CLASS_ATTRS = [
  "root-class", "input-class", "panel-class", "list-class", "option-class",
  "option-title-class", "option-snippet-class", "option-url-class",
  "empty-class", "status-class", "suggest-class", "suggest-item-class",
  "mark-class"
];

export const KNOWN_ATTRS = new Set([...CONFIG_ATTRS, ...PART_CLASS_ATTRS]);

// Keys that control markup structure rather than element attributes.
const CONTROL_KEYS = new Set(["src", "assetsBase", "theme", "__keywords"]); // __keywords: Nunjucks kwargs marker

// camelCase / snake_case -> kebab-case, so `pageSize`, `page_size`, and
// `page-size` all resolve to the `page-size` attribute the element observes.
export function toKebab(name) {
  return String(name)
    .replace(/([a-z0-9])([A-Z])/g, "$1-$2")
    .replace(/_/g, "-")
    .toLowerCase();
}

// Escape a value for use inside a double-quoted HTML attribute.
export function escapeAttr(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// Escape a value for use in an unquoted URL-ish attribute (src/href). Same as
// attribute escaping — the element re-resolves `src` against document.baseURI.
export function escapeUrl(value) {
  return escapeAttr(value);
}

// Trailing-slash-trim an assets base URL so we can join a filename with a
// single "/". A bare "" or "/" both normalize to "" -> "/rangefind-search.js".
function normalizeBase(base) {
  return String(base ?? "").replace(/\/+$/, "");
}

// Render one attribute token from a normalized (kebab) name + value.
//   true            -> bare attribute (e.g. `router`)
//   false / null    -> `name="false"` (turns a default-on boolean off, matching
//                       the element's parseBoolAttr semantics; the element reads
//                       `highlight="false"` etc.)
//   string / number -> `name="escaped-value"`
function renderAttr(name, value) {
  if (value === true) return name;
  if (value === false || value === null) return `${name}="false"`;
  return `${name}="${escapeAttr(value)}"`;
}

// Build the full markup fragment for a search box.
//
// `args`     — the shortcode's argument object (per call site).
// `defaults` — plugin-level defaults for `src`, `assetsBase`, `theme`.
//
// Returns a string:
//   [<link rel="stylesheet"> if theme]
//   <script type="module" src="<assetsBase>/rangefind-search.js"></script>
//   <rangefind-search src="..." ...attrs></rangefind-search>
export function renderSearchMarkup(args = {}, defaults = {}) {
  const opts = args && typeof args === "object" ? args : {};

  const src = opts.src ?? defaults.src ?? "/rangefind/";
  const assetsBase = normalizeBase(opts.assetsBase ?? defaults.assetsBase ?? "/_rangefind");
  const theme = (opts.theme ?? defaults.theme ?? false) === true
    || (opts.theme ?? defaults.theme) === "true";

  // Collect element attributes from every non-control key, normalized to kebab.
  const attrs = [];
  for (const [rawKey, value] of Object.entries(opts)) {
    if (CONTROL_KEYS.has(rawKey)) continue;
    if (value === undefined) continue;
    const name = toKebab(rawKey);
    if (name === "src") continue; // src is handled explicitly above
    if (!KNOWN_ATTRS.has(name)) {
      // Unknown to the element — skip it rather than invent a new attribute.
      if (typeof console !== "undefined" && typeof console.warn === "function") {
        console.warn(`eleventy-plugin-rangefind: ignoring unknown rangefindSearch attribute "${rawKey}".`);
      }
      continue;
    }
    attrs.push(renderAttr(name, value));
  }

  const jsHref = `${assetsBase}/rangefind-search.js`;
  const cssHref = `${assetsBase}/rangefind-search.css`;

  const lines = [];
  if (theme) lines.push(`<link rel="stylesheet" href="${escapeUrl(cssHref)}">`);
  lines.push(`<script type="module" src="${escapeUrl(jsHref)}"></script>`);
  const attrString = attrs.length ? ` ${attrs.join(" ")}` : "";
  lines.push(`<rangefind-search src="${escapeUrl(src)}"${attrString}></rangefind-search>`);
  return lines.join("\n");
}
