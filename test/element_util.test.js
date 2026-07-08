import assert from "node:assert/strict";
import test from "node:test";
import {
  resolveConfig,
  parseBoolAttr,
  parseIntAttr,
  parseSuggestAttr,
  partClass,
  mergeClassNames,
  buildHighlightSegments,
  truncate,
  pickTitleField,
  pickSnippet,
  reduceNav
} from "../src/element_util.js";

// Renders segments back to a bracketed string so assertions read naturally and
// prove the ordered plain/marked segmentation.
function render(segments) {
  return segments.map(seg => (seg.mark ? `[${seg.text}]` : seg.text)).join("");
}

test("resolveConfig applies defaults", () => {
  const cfg = resolveConfig({ src: "/rangefind/" });
  assert.equal(cfg.src, "/rangefind/");
  assert.equal(cfg.pageSize, 8);
  assert.equal(cfg.debounce, 150);
  assert.equal(cfg.minLength, 1);
  assert.equal(cfg.highlight, true);
  assert.equal(cfg.suggest, "auto");
  assert.equal(cfg.router, false);
  assert.equal(cfg.placeholder, "Search");
});

test("resolveConfig parses booleans and numbers from attributes", () => {
  const cfg = resolveConfig({
    src: "/x/",
    highlight: "false",
    suggest: "off",
    router: "",
    "open-on-focus": "true",
    "page-size": "20",
    debounce: "0",
    "min-length": "3"
  });
  assert.equal(cfg.highlight, false);
  assert.equal(cfg.suggest, false);
  assert.equal(cfg.router, true);
  assert.equal(cfg.openOnFocus, true);
  assert.equal(cfg.pageSize, 20);
  assert.equal(cfg.debounce, 0);
  assert.equal(cfg.minLength, 3);
});

test("parseBoolAttr distinguishes absent, bare, and explicit values", () => {
  assert.equal(parseBoolAttr(null, true), true);   // absent → fallback
  assert.equal(parseBoolAttr(null, false), false);
  assert.equal(parseBoolAttr("", false), true);    // bare attribute present
  assert.equal(parseBoolAttr("false"), false);
  assert.equal(parseBoolAttr("0"), false);
  assert.equal(parseBoolAttr("yes"), true);
});

test("parseIntAttr clamps and falls back", () => {
  assert.equal(parseIntAttr("20", 8, { min: 1, max: 100 }), 20);
  assert.equal(parseIntAttr("9999", 8, { min: 1, max: 100 }), 100);
  assert.equal(parseIntAttr("0", 8, { min: 1, max: 100 }), 1);
  assert.equal(parseIntAttr("abc", 8), 8);
  assert.equal(parseIntAttr("", 8), 8);
  assert.equal(parseIntAttr(null, 8), 8);
});

test("parseSuggestAttr is tri-state", () => {
  assert.equal(parseSuggestAttr(null), "auto");
  assert.equal(parseSuggestAttr(""), "auto");
  assert.equal(parseSuggestAttr("auto"), "auto");
  assert.equal(parseSuggestAttr("false"), false);
  assert.equal(parseSuggestAttr("on"), true);
});

test("mergeClassNames dedupes and keeps first occurrence order", () => {
  assert.equal(mergeClassNames("a b", "b c", "a"), "a b c");
  assert.equal(mergeClassNames(null, "  x  y ", undefined), "x y");
  assert.equal(mergeClassNames(), "");
});

test("partClass merges hook + attribute + classNames additively", () => {
  const result = partClass("input", {
    attrs: { "input-class": "w-full border" },
    classNames: { input: "px-3 border" } // duplicate 'border' collapses
  });
  assert.equal(result, "rf-search__input w-full border px-3");
});

test("partClass always includes the namespaced hook even with no extras", () => {
  assert.equal(partClass("option", {}), "rf-search__option");
  assert.equal(partClass("mark", { attrs: { "mark-class": "bg-yellow-200" } }), "rf-search__mark bg-yellow-200");
});

test("buildHighlightSegments segments ordered plain and marked text", () => {
  const segments = buildHighlightSegments({ text: "the quick brown fox", ranges: [[4, 9], [16, 19]] });
  assert.equal(render(segments), "the [quick] brown [fox]");
});

test("buildHighlightSegments sorts out-of-order and merges overlapping ranges", () => {
  const segments = buildHighlightSegments({ text: "abcdefgh", ranges: [[5, 7], [1, 3], [2, 4]] });
  // [1,3] and [2,4] merge to [1,4]; [5,7] stays; sorted overall.
  assert.equal(render(segments), "a[bcd]e[fg]h");
});

test("buildHighlightSegments drops empty, reversed, and out-of-bounds ranges", () => {
  const segments = buildHighlightSegments({ text: "hello", ranges: [[2, 2], [4, 1], [3, 99], "bad", [1, 2]] });
  // [2,2] empty, [4,1] reversed → dropped; [3,99] clamps to [3,5]; [1,2] kept.
  assert.equal(render(segments), "h[e]l[lo]");
});

test("buildHighlightSegments cannot inject HTML (text treated as text)", () => {
  const evil = '<img src=x onerror=alert(1)> & "quote"';
  const segments = buildHighlightSegments({ text: evil, ranges: [[0, 5]] });
  // Concatenating segment text reproduces the raw string verbatim; the renderer
  // uses textContent, so markup never becomes DOM.
  assert.equal(segments.map(s => s.text).join(""), evil);
  assert.equal(segments[0].mark, true);
});

test("buildHighlightSegments returns [] for empty text", () => {
  assert.deepEqual(buildHighlightSegments({ text: "", ranges: [[0, 1]] }), []);
  assert.deepEqual(buildHighlightSegments(null), []);
});

test("truncate cuts on a word boundary and adds an ellipsis", () => {
  assert.equal(truncate("short text", 200), "short text");
  const out = truncate("one two three four five six seven eight nine ten", 20);
  assert.ok(out.endsWith("…"));
  assert.ok(out.length <= 21);
  assert.ok(!out.includes("  "));
});

test("pickTitleField prefers the first present title-ish field", () => {
  assert.equal(pickTitleField({ title: "T", name: "N" }), "title");
  assert.equal(pickTitleField({ name: "N" }), "name");
  assert.equal(pickTitleField({ url: "/x" }), null);
});

test("pickSnippet prefers a highlighted non-title field", () => {
  const result = {
    title: "Title",
    body: "plain body text",
    highlights: {
      title: { text: "Title", ranges: [[0, 5]] },
      body: { text: "…matched body…", ranges: [[1, 8]] }
    }
  };
  const snippet = pickSnippet(result, { titleField: "title" });
  assert.equal(snippet.field, "body");
  assert.equal(snippet.highlighted, true);
  assert.equal(snippet.text, "…matched body…");
});

test("pickSnippet falls back to a plain truncated display field", () => {
  const result = { title: "Title", body: "x".repeat(500) };
  const snippet = pickSnippet(result, { titleField: "title", maxLength: 50 });
  assert.equal(snippet.field, "body");
  assert.equal(snippet.highlighted, false);
  assert.ok(snippet.text.length <= 51);
  assert.deepEqual(snippet.ranges, []);
});

test("pickSnippet returns null when nothing suitable exists", () => {
  assert.equal(pickSnippet({ title: "only title" }, { titleField: "title" }), null);
});

test("reduceNav wraps ArrowDown/ArrowUp and opens the panel", () => {
  assert.deepEqual(reduceNav({ active: -1, count: 3, open: false }, "ArrowDown"), { active: 0, open: true });
  assert.deepEqual(reduceNav({ active: 2, count: 3, open: true }, "ArrowDown"), { active: 0, open: true });
  assert.deepEqual(reduceNav({ active: -1, count: 3, open: false }, "ArrowUp"), { active: 2, open: true });
  assert.deepEqual(reduceNav({ active: 0, count: 3, open: true }, "ArrowUp"), { active: 2, open: true });
});

test("reduceNav handles Home, End, and Escape", () => {
  assert.deepEqual(reduceNav({ active: 2, count: 5, open: true }, "Home"), { active: 0, open: true });
  assert.deepEqual(reduceNav({ active: 0, count: 5, open: true }, "End"), { active: 4, open: true });
  assert.deepEqual(reduceNav({ active: 3, count: 5, open: true }, "Escape"), { active: -1, open: false });
});

test("reduceNav with no options can only close", () => {
  assert.deepEqual(reduceNav({ active: -1, count: 0, open: true }, "ArrowDown"), { active: -1, open: true });
  assert.deepEqual(reduceNav({ active: -1, count: 0, open: true }, "Escape"), { active: -1, open: false });
});
