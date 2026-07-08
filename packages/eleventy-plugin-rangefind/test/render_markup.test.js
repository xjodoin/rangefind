// Unit tests for the pure markup builder (no Eleventy, no DOM).
import assert from "node:assert/strict";
import test from "node:test";
import {
  renderSearchMarkup,
  toKebab,
  escapeAttr,
  KNOWN_ATTRS
} from "../src/render_markup.js";

test("toKebab normalizes camelCase and snake_case", () => {
  assert.equal(toKebab("pageSize"), "page-size");
  assert.equal(toKebab("min_length"), "min-length");
  assert.equal(toKebab("inputClass"), "input-class");
  assert.equal(toKebab("src"), "src");
});

test("escapeAttr escapes HTML-significant characters", () => {
  assert.equal(escapeAttr('a"b<c>&d'), "a&quot;b&lt;c&gt;&amp;d");
});

test("defaults produce script + element, no theme link", () => {
  const html = renderSearchMarkup({}, { src: "/rangefind/", assetsBase: "/_rangefind" });
  assert.ok(!html.includes("<link"), "no stylesheet without theme");
  assert.match(html, /<script type="module" src="\/_rangefind\/rangefind-search\.js"><\/script>/);
  assert.match(html, /<rangefind-search src="\/rangefind\/"><\/rangefind-search>/);
});

test("theme:true adds the stylesheet link", () => {
  const html = renderSearchMarkup({ theme: true }, { assetsBase: "/assets/rf" });
  assert.match(html, /<link rel="stylesheet" href="\/assets\/rf\/rangefind-search\.css">/);
  assert.match(html, /src="\/assets\/rf\/rangefind-search\.js"/);
});

test("plugin default theme applies when arg omits it", () => {
  const html = renderSearchMarkup({}, { theme: true });
  assert.ok(html.includes("<link rel=\"stylesheet\""), "plugin-level theme default honored");
});

test("per-call src/assetsBase override plugin defaults", () => {
  const html = renderSearchMarkup(
    { src: "/docs/rangefind/", assetsBase: "/static" },
    { src: "/rangefind/", assetsBase: "/_rangefind" }
  );
  assert.match(html, /<rangefind-search src="\/docs\/rangefind\/"/);
  assert.match(html, /src="\/static\/rangefind-search\.js"/);
});

test("assetsBase trailing slash is normalized", () => {
  const html = renderSearchMarkup({}, { assetsBase: "/_rangefind/" });
  assert.match(html, /src="\/_rangefind\/rangefind-search\.js"/);
  assert.ok(!html.includes("_rangefind//"), "no doubled slash");
});

test("config + class attributes are normalized to kebab and escaped", () => {
  const html = renderSearchMarkup(
    { placeholder: 'Find "stuff"', pageSize: 5, inputClass: "w-full border", markClass: "bg-yellow-200" },
    {}
  );
  assert.match(html, /placeholder="Find &quot;stuff&quot;"/);
  assert.match(html, /page-size="5"/);
  assert.match(html, /input-class="w-full border"/);
  assert.match(html, /mark-class="bg-yellow-200"/);
});

test("boolean true renders a bare attribute; false renders name=\"false\"", () => {
  const on = renderSearchMarkup({ router: true, hotkey: true }, {});
  assert.match(on, /<rangefind-search src="[^"]*" router hotkey>/);
  const off = renderSearchMarkup({ highlight: false }, {});
  assert.match(off, /highlight="false"/);
});

test("unknown attribute names are dropped, not invented", () => {
  const warnings = [];
  const orig = console.warn;
  console.warn = (msg) => warnings.push(msg);
  try {
    const html = renderSearchMarkup({ notARealAttr: "x", placeholder: "ok" }, {});
    assert.ok(!html.includes("not-a-real-attr"), "unknown attr not rendered");
    assert.match(html, /placeholder="ok"/);
    assert.equal(warnings.length, 1);
  } finally {
    console.warn = orig;
  }
});

test("KNOWN_ATTRS covers the documented vocabulary", () => {
  for (const name of ["src", "page-size", "highlight", "input-class", "mark-class", "empty-text"]) {
    assert.ok(KNOWN_ATTRS.has(name), `${name} recognized`);
  }
});
