// <rangefind-search> — a drop-in, framework-agnostic search box.
//
// Design intent: integrate cleanly with TailwindCSS and any other CSS
// framework. To do that the element renders into its **light DOM** (no shadow
// root), so the host page's utility classes and global CSS apply directly, and
// ships **no visual styling** of its own — it provides structure, behavior, and
// the WAI-ARIA combobox pattern only. Authors style it with namespaced hook
// classes (rf-search__*), per-part `*-class` attributes, the `.classNames`
// property, and/or the optional `rangefind-search.css` theme.

import { createSearch } from "./runtime.js";
import {
  PART_CLASS_ATTRS,
  resolveConfig,
  partClass,
  buildHighlightSegments,
  pickSnippet,
  pickTitleField,
  reduceNav
} from "./element_util.js";

const CONFIG_ATTRS = [
  "src", "placeholder", "page-size", "debounce", "min-length", "highlight",
  "suggest", "router", "open-on-focus", "hotkey", "label", "empty-text",
  "loading-text", "error-text"
];
const OBSERVED_ATTRS = [...CONFIG_ATTRS, ...Object.values(PART_CLASS_ATTRS)];

let instanceSeq = 0;

// Only the id/url part of a display field is unsafe as a link target; text is
// always rendered via text nodes, never innerHTML.
function safeHref(url) {
  const value = String(url ?? "").trim();
  return value || null;
}

export class RangefindSearchElement extends HTMLElement {
  static get observedAttributes() {
    return OBSERVED_ATTRS;
  }

  constructor() {
    super();
    this._uid = `rf-${++instanceSeq}`;
    this._config = resolveConfig({});
    this._classNames = {};
    this._searchOptions = {};
    this._engine = null;
    this._enginePromise = null;
    this._token = 0;
    this._open = false;
    this._active = -1;
    this._options = [];
    this._resultByOption = new WeakMap();
    this._debounceTimer = 0;
    this._built = false;
    this._destroyed = false;
    // Bound handlers so add/removeEventListener pair up in disconnectedCallback.
    this._onInput = this._onInput.bind(this);
    this._onKeydown = this._onKeydown.bind(this);
    this._onFocus = this._onFocus.bind(this);
    this._onFocusOut = this._onFocusOut.bind(this);
    this._onDocKeydown = this._onDocKeydown.bind(this);
  }

  // --- Programmatic config (framework / advanced use) -----------------------

  get classNames() {
    return this._classNames;
  }

  set classNames(value) {
    this._classNames = value && typeof value === "object" ? value : {};
    if (this._built) this._applyClasses();
  }

  // Extra params merged into every engine.search() call (e.g. filters, facets).
  get searchOptions() {
    return this._searchOptions;
  }

  set searchOptions(value) {
    this._searchOptions = value && typeof value === "object" ? value : {};
  }

  // --- Lifecycle ------------------------------------------------------------

  connectedCallback() {
    if (this._destroyed) return;
    this._config = this._readConfig();
    if (!this._built) this._build();
    this._applyConfig();
    if (this._config.hotkey) document.addEventListener("keydown", this._onDocKeydown);
    // Router: seed the input from ?q= and run once so a shared URL restores
    // its results without any interaction.
    if (this._config.router) {
      const q = new URLSearchParams(location.search).get("q");
      if (q) {
        this._input.value = q;
        this._scheduleSearch(0);
      }
    }
    // Eagerly warm the engine when a source is known; failures surface as the
    // error state rather than an unhandled rejection.
    if (this._config.src) this._ensureEngine();
  }

  disconnectedCallback() {
    clearTimeout(this._debounceTimer);
    document.removeEventListener("keydown", this._onDocKeydown);
    // Invalidate any in-flight responses so late resolutions are ignored.
    this._token++;
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (oldValue === newValue || !this._built) return;
    const previousSrc = this._config.src;
    const wasHotkey = this._config.hotkey;
    this._config = this._readConfig();
    this._applyConfig();
    if (this._config.hotkey !== wasHotkey) {
      document.removeEventListener("keydown", this._onDocKeydown);
      if (this._config.hotkey) document.addEventListener("keydown", this._onDocKeydown);
    }
    // A changed source invalidates the engine; rebuild it lazily.
    if (name === "src" && this._config.src !== previousSrc) {
      this._engine = null;
      this._enginePromise = null;
      this._close();
      if (this._config.src) this._ensureEngine();
    }
  }

  // --- DOM construction (light DOM) -----------------------------------------

  _build() {
    const listId = `${this._uid}-list`;
    const suggestId = `${this._uid}-suggest`;

    const input = document.createElement("input");
    input.type = "search";
    input.autocomplete = "off";
    input.autocapitalize = "none";
    input.spellcheck = false;
    input.setAttribute("role", "combobox");
    input.setAttribute("aria-autocomplete", "list");
    input.setAttribute("aria-expanded", "false");
    input.setAttribute("aria-haspopup", "listbox");
    input.setAttribute("aria-controls", `${suggestId} ${listId}`);

    // aria-live status for screen readers. Visually hidden via inline styles —
    // this is an accessibility affordance, not opinionated theming, so it is
    // the one bit of style the element sets on its own.
    const status = document.createElement("div");
    status.setAttribute("role", "status");
    status.setAttribute("aria-live", "polite");
    status.style.cssText = "position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap;border:0;";

    const panel = document.createElement("div");
    panel.hidden = true;

    const suggest = document.createElement("ul");
    suggest.id = suggestId;
    suggest.setAttribute("role", "listbox");
    suggest.setAttribute("aria-label", "Suggestions");
    suggest.hidden = true;

    const list = document.createElement("ul");
    list.id = listId;
    list.setAttribute("role", "listbox");
    list.hidden = true;

    const empty = document.createElement("div");
    empty.hidden = true;

    panel.append(suggest, list, empty);
    this.append(input, status, panel);

    this._input = input;
    this._status = status;
    this._panel = panel;
    this._suggest = suggest;
    this._list = list;
    this._empty = empty;

    input.addEventListener("input", this._onInput);
    input.addEventListener("keydown", this._onKeydown);
    input.addEventListener("focus", this._onFocus);
    this.addEventListener("focusout", this._onFocusOut);

    this._built = true;
  }

  _applyConfig() {
    this._applyClasses();
    this._input.placeholder = this._config.placeholder;
    this._input.setAttribute("aria-label", this._config.label);
    this._list.setAttribute("aria-label", this._config.label);
  }

  _applyClasses() {
    const ctx = { attrs: this._attrMap(), classNames: this._classNames };
    this.className = partClass("root", ctx);
    this._input.className = partClass("input", ctx);
    this._panel.className = partClass("panel", ctx);
    this._suggest.className = partClass("suggest", ctx);
    this._list.className = partClass("list", ctx);
    this._empty.className = partClass("empty", ctx);
    // The status hook is applied but its inline sr-only styles keep it hidden.
    this._status.className = partClass("status", ctx);
  }

  _attrMap() {
    const attrs = {};
    for (const name of OBSERVED_ATTRS) {
      if (this.hasAttribute(name)) attrs[name] = this.getAttribute(name);
    }
    return attrs;
  }

  _readConfig() {
    return resolveConfig(this._attrMap());
  }

  // --- Engine ---------------------------------------------------------------

  async _ensureEngine() {
    if (this._engine) return this._engine;
    if (this._enginePromise) return this._enginePromise;
    const src = this._config.src;
    if (!src) return null;
    const baseUrl = new URL(src, document.baseURI).href;
    this._enginePromise = createSearch({ baseUrl, ...(this._searchOptions.create || {}) })
      .then(engine => {
        this._engine = engine;
        return engine;
      })
      .catch(error => {
        this._enginePromise = null;
        this._showError(error);
        this._emit("rangefind:error", { error });
        return null;
      });
    return this._enginePromise;
  }

  _suggestEnabled() {
    const cfg = this._config.suggest;
    if (cfg === false) return false;
    if (cfg === true) return true; // try anyway; degrade silently on failure
    const manifest = this._engine?.manifest;
    const hasAuthorityTitles = manifest?.authority?.fields?.length === 1
      && manifest.authority.fields[0].path === "title"
      && manifest.authority.fields[0].exact !== false;
    return Boolean(manifest?.features?.suggest || manifest?.suggest || hasAuthorityTitles);
  }

  // --- Query flow -----------------------------------------------------------

  _onInput() {
    const query = this._input.value.trim();
    if (this._config.router) this._syncRouter(query);
    if (query.length < this._config.minLength) {
      this._token++;
      this._close();
      this._setStatus("");
      return;
    }
    this._scheduleSearch(this._config.debounce);
  }

  _scheduleSearch(delay) {
    clearTimeout(this._debounceTimer);
    const token = ++this._token;
    this._setStatus(this._config.loadingText);
    this._debounceTimer = setTimeout(() => this._dispatch(token), Math.max(0, delay));
  }

  _dispatch(token) {
    const query = this._input.value.trim();
    if (query.length < this._config.minLength) {
      this._close();
      this._setStatus("");
      return;
    }
    // Fire search + suggest in parallel; each guards on the token so a slower
    // earlier response can never overwrite a newer one.
    this._runSearch(query, token);
    this._runSuggest(query, token);
  }

  async _runSearch(query, token) {
    const engine = await this._ensureEngine();
    if (token !== this._token) return;
    if (!engine) return; // error state already shown by _ensureEngine
    try {
      const response = await engine.search({
        q: query,
        page: 1,
        size: this._config.pageSize,
        highlight: this._config.highlight,
        ...(this._searchOptions.search || {})
      });
      if (token !== this._token) return;
      this._renderResults(query, response);
      this._emit("rangefind:search", { query, response });
    } catch (error) {
      if (token !== this._token) return;
      this._showError(error);
      this._emit("rangefind:error", { error });
    }
  }

  async _runSuggest(query, token) {
    if (!this._suggestEnabled()) {
      this._renderSuggestions([]);
      return;
    }
    const engine = await this._ensureEngine();
    if (token !== this._token || !engine || typeof engine.suggest !== "function") return;
    try {
      const response = await engine.suggest({ q: query, size: 6 });
      if (token !== this._token) return;
      this._renderSuggestions(response?.suggestions || []);
    } catch {
      // The index may lack a sidecar even when asked; fail quietly.
      this._renderSuggestions([]);
    }
  }

  // --- Rendering ------------------------------------------------------------

  _renderResults(query, response) {
    const ctx = { attrs: this._attrMap(), classNames: this._classNames };
    const results = Array.isArray(response?.results) ? response.results : [];
    this._list.replaceChildren();

    if (!results.length) {
      this._list.hidden = true;
      this._empty.hidden = false;
      this._empty.textContent = this._config.emptyText;
      this._setStatus(`No results for “${query}”.`);
      this._openPanel();
      this._rebuildOptions();
      return;
    }

    this._empty.hidden = true;
    this._list.hidden = false;
    for (const result of results) {
      this._list.append(this._buildResultOption(result, ctx));
    }
    const total = response.approximate ? `${response.total}+` : response.total;
    this._setStatus(`${total} result${Number(response.total) === 1 ? "" : "s"} for “${query}”.`);
    this._openPanel();
    this._rebuildOptions();
  }

  _buildResultOption(result, ctx) {
    const option = document.createElement("a");
    option.className = partClass("option", ctx);
    option.setAttribute("role", "option");
    option.setAttribute("aria-selected", "false");
    option.dataset.rfKind = "result";
    const href = safeHref(result.url);
    if (href) option.href = href;

    const titleField = pickTitleField(result);
    const titleEl = document.createElement("span");
    titleEl.className = partClass("optionTitle", ctx);
    const titleHighlight = result.highlights?.[titleField];
    if (titleHighlight) this._renderHighlight(titleEl, titleHighlight, ctx);
    else titleEl.textContent = (titleField && result[titleField]) || result.title || result.url || result.id || "Untitled";
    option.append(titleEl);

    const snippet = pickSnippet(result, { titleField });
    if (snippet) {
      const snippetEl = document.createElement("span");
      snippetEl.className = partClass("optionSnippet", ctx);
      if (snippet.highlighted) this._renderHighlight(snippetEl, snippet, ctx);
      else snippetEl.textContent = snippet.text;
      option.append(snippetEl);
    }

    if (href) {
      const urlEl = document.createElement("span");
      urlEl.className = partClass("optionUrl", ctx);
      urlEl.textContent = href;
      option.append(urlEl);
    }

    this._resultByOption.set(option, result);
    // Native navigation on click; also emit a select event so frameworks can
    // intercept. Do not preventDefault — let the anchor follow its href.
    option.addEventListener("click", () => this._emit("rangefind:select", { result }));
    return option;
  }

  _renderSuggestions(suggestions) {
    const ctx = { attrs: this._attrMap(), classNames: this._classNames };
    this._suggest.replaceChildren();
    const items = Array.isArray(suggestions) ? suggestions : [];
    if (!items.length) {
      this._suggest.hidden = true;
      this._rebuildOptions();
      return;
    }
    for (const item of items) {
      const text = String(item?.text ?? "");
      if (!text) continue;
      const li = document.createElement("li");
      li.className = partClass("suggestItem", ctx);
      li.setAttribute("role", "option");
      li.setAttribute("aria-selected", "false");
      li.dataset.rfKind = "suggest";
      li.dataset.rfText = text;
      li.textContent = text;
      // mousedown (not click) so the input never loses focus first.
      li.addEventListener("mousedown", event => {
        event.preventDefault();
        this._acceptSuggestion(text);
      });
      this._suggest.append(li);
    }
    this._suggest.hidden = this._suggest.children.length === 0;
    if (!this._suggest.hidden) this._openPanel();
    this._rebuildOptions();
  }

  // Renders `{ text, ranges }` as text nodes plus <mark> elements. Nothing from
  // the index is ever treated as HTML.
  _renderHighlight(node, highlight, ctx) {
    node.replaceChildren();
    const segments = buildHighlightSegments(highlight);
    if (!segments.length) {
      node.textContent = highlight?.text || "";
      return;
    }
    const markClass = partClass("mark", ctx);
    for (const segment of segments) {
      if (segment.mark) {
        const mark = document.createElement("mark");
        mark.className = markClass;
        mark.textContent = segment.text;
        node.append(mark);
      } else {
        node.append(document.createTextNode(segment.text));
      }
    }
  }

  _showError(error) {
    this._list.hidden = true;
    this._list.replaceChildren();
    this._suggest.hidden = true;
    this._suggest.replaceChildren();
    this._empty.hidden = false;
    this._empty.textContent = this._config.errorText;
    this._setStatus(this._config.errorText);
    this._openPanel();
    this._rebuildOptions();
    // Surface the reason for debugging without an unhandled rejection.
    if (error) console.warn("rangefind-search:", error?.message || error);
  }

  // --- Panel / navigation ---------------------------------------------------

  // Recomputes the flat, navigable option list (suggestions first, then result
  // anchors) and resets aria state. A single monotonic option-id scheme lets
  // aria-activedescendant reference either kind.
  _rebuildOptions() {
    const nodes = [];
    if (!this._suggest.hidden) nodes.push(...this._suggest.querySelectorAll('[role="option"]'));
    if (!this._list.hidden) nodes.push(...this._list.querySelectorAll('[role="option"]'));
    this._options = nodes;
    nodes.forEach((node, index) => {
      node.id = `${this._uid}-opt-${index}`;
      node.setAttribute("aria-selected", "false");
    });
    this._active = -1;
    this._input.removeAttribute("aria-activedescendant");
  }

  _openPanel() {
    this._panel.hidden = false;
    this._open = true;
    this._input.setAttribute("aria-expanded", "true");
  }

  _close() {
    this._panel.hidden = true;
    this._open = false;
    this._active = -1;
    this._input.setAttribute("aria-expanded", "false");
    this._input.removeAttribute("aria-activedescendant");
    for (const node of this._options) node.setAttribute("aria-selected", "false");
  }

  _setActive(index) {
    for (const node of this._options) node.setAttribute("aria-selected", "false");
    this._active = index;
    const node = this._options[index];
    if (!node) {
      this._input.removeAttribute("aria-activedescendant");
      return;
    }
    node.setAttribute("aria-selected", "true");
    this._input.setAttribute("aria-activedescendant", node.id);
    if (typeof node.scrollIntoView === "function") node.scrollIntoView({ block: "nearest" });
  }

  _activate(index) {
    const node = this._options[index];
    if (!node) return;
    if (node.dataset.rfKind === "suggest") {
      this._acceptSuggestion(node.dataset.rfText || node.textContent || "");
      return;
    }
    const result = this._resultByOption.get(node);
    if (result) this._emit("rangefind:select", { result });
    const href = node.getAttribute("href");
    this._close();
    if (href) window.location.assign(href);
  }

  _acceptSuggestion(text) {
    this._input.value = text;
    this._close();
    if (this._config.router) this._syncRouter(text.trim());
    this._scheduleSearch(0);
  }

  _setStatus(message) {
    this._status.textContent = message || "";
  }

  _syncRouter(query) {
    const params = new URLSearchParams(location.search);
    if (query) params.set("q", query);
    else params.delete("q");
    const qs = params.toString();
    history.replaceState(history.state, "", `${location.pathname}${qs ? `?${qs}` : ""}${location.hash}`);
  }

  // --- Event helpers --------------------------------------------------------

  _emit(type, detail) {
    this.dispatchEvent(new CustomEvent(type, { detail, bubbles: true, composed: true }));
  }

  _onKeydown(event) {
    const { key } = event;
    if (key === "Tab") {
      this._close();
      return;
    }
    if (key === "Enter") {
      if (this._open && this._active >= 0) {
        event.preventDefault();
        this._activate(this._active);
      } else {
        // No active option: run the query immediately, bypassing debounce.
        event.preventDefault();
        this._scheduleSearch(0);
      }
      return;
    }
    if (key === "Escape") {
      if (this._open) {
        event.preventDefault();
        this._close();
      } else if (this._input.value) {
        this._input.value = "";
        this._token++;
        this._setStatus("");
        if (this._config.router) this._syncRouter("");
      }
      return;
    }
    if (!["ArrowDown", "ArrowUp", "Home", "End"].includes(key)) return;
    // Open a closed panel on first arrow if options exist.
    if (!this._open && this._options.length && (key === "ArrowDown" || key === "ArrowUp")) {
      this._openPanel();
    }
    if (!this._options.length) return;
    event.preventDefault();
    const next = reduceNav({ active: this._active, count: this._options.length, open: this._open }, key);
    if (!next.open) this._close();
    else this._setActive(next.active);
  }

  _onFocus() {
    if (this._config.openOnFocus && this._input.value.trim().length >= this._config.minLength) {
      if (this._options.length) this._openPanel();
      else this._scheduleSearch(0);
    }
  }

  _onFocusOut(event) {
    // Close only when focus leaves the whole component (a result click lands
    // inside it briefly before navigating).
    const next = event.relatedTarget;
    if (next && this.contains(next)) return;
    this._close();
  }

  _onDocKeydown(event) {
    if (event.key !== "/" || event.defaultPrevented) return;
    const target = event.target;
    const tag = target?.tagName;
    const typing = tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || target?.isContentEditable;
    if (typing) return;
    event.preventDefault();
    this._input.focus();
  }
}

// Guard against double registration when the bundle loads more than once.
if (typeof customElements !== "undefined" && !customElements.get("rangefind-search")) {
  customElements.define("rangefind-search", RangefindSearchElement);
}
