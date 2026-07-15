import assert from "node:assert/strict";
import test from "node:test";
import { urlKey, createLinkResolver } from "../src/link_resolve.js";

test("urlKey collapses .html, index files, and trailing slashes", () => {
  assert.equal(urlKey("/guide/intro.html"), "/guide/intro");
  assert.equal(urlKey("/guide/index.html"), "/guide");
  assert.equal(urlKey("/guide/"), "/guide");
  assert.equal(urlKey("/index.html"), "/");
  assert.equal(urlKey("/"), "/");
  assert.equal(urlKey("/a//b/"), "/a/b");
});

test("urlKey decodes percent-encoding and tolerates bad input", () => {
  assert.equal(urlKey("/caf%C3%A9"), "/café");
  assert.equal(urlKey("/%E0%A4%A"), "/%E0%A4%A"); // malformed: returned as-is, no throw
});

test("resolver handles relative, root-relative, and dot-segment hrefs", () => {
  const resolve = createLinkResolver("/");
  assert.equal(resolve("hub.html", "/alpha"), "/hub");
  assert.equal(resolve("/hub", "/guide/intro"), "/hub");
  assert.equal(resolve("../hub", "/guide/intro"), "/hub");
  assert.equal(resolve("hub?v=2#frag", "/alpha"), "/hub");
});

test("resolver rejects fragments, external hosts, and non-nav schemes", () => {
  const resolve = createLinkResolver("/");
  assert.equal(resolve("#section", "/alpha"), null);
  assert.equal(resolve("https://example.com/x", "/alpha"), null);
  assert.equal(resolve("//example.com/x", "/alpha"), null);
  assert.equal(resolve("mailto:a@b.com", "/alpha"), null);
  assert.equal(resolve("tel:+15551234", "/alpha"), null);
  assert.equal(resolve("javascript:void(0)", "/alpha"), null);
  assert.equal(resolve("", "/alpha"), null);
});

test("resolver keeps absolute same-origin links when baseUrl is an origin", () => {
  // The regression: a site deployed at example.com/blog/ that emits absolute
  // internal links must still build a graph.
  const resolve = createLinkResolver("https://example.com/blog/");
  assert.equal(resolve("https://example.com/blog/hub", "/alpha"), "/hub");
  assert.equal(resolve("https://example.com/blog/guide/", "/alpha"), "/guide");
  assert.equal(resolve("hub.html", "/alpha"), "/hub"); // relative still works
  // Same origin but outside the base path prefix is not part of this site.
  assert.equal(resolve("https://example.com/other/page", "/alpha"), null);
  // A different origin is external.
  assert.equal(resolve("https://elsewhere.com/blog/hub", "/alpha"), null);
});

test("resolver treats protocol-relative same-origin links as internal", () => {
  const resolve = createLinkResolver("https://example.com/");
  assert.equal(resolve("//example.com/hub", "/alpha"), "/hub");
  assert.equal(resolve("//cdn.other.com/x", "/alpha"), null);
});
