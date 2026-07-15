import assert from "node:assert/strict";
import test from "node:test";
import { extractHtml } from "../src/html_extract.js";

test("extracts title, headings, and body from main content", () => {
  const result = extractHtml(`<!doctype html>
    <html lang="en-US">
      <head><title>Site Title</title><meta name="description" content="A short blurb."></head>
      <body>
        <nav>Home About Contact</nav>
        <main>
          <h1>Primary Heading</h1>
          <h2>Second Heading</h2>
          <p>The quick brown fox jumps.</p>
        </main>
        <footer>Copyright notice</footer>
      </body>
    </html>`);
  assert.equal(result.skip, false);
  assert.equal(result.title, "Primary Heading");
  assert.equal(result.lang, "en");
  assert.equal(result.description, "A short blurb.");
  assert.equal(result.headings, "Primary Heading Second Heading");
  assert.equal(result.body, "Primary Heading Second Heading The quick brown fox jumps.");
});

test("title falls back to <title> when no h1 is present", () => {
  const result = extractHtml("<html><head><title>Only Title</title></head><body><main><p>text</p></main></body></html>");
  assert.equal(result.title, "Only Title");
});

test("data-rangefind-meta title wins over h1 and title", () => {
  const result = extractHtml(`<html><head><title>Tag Title</title></head><body><main>
    <span data-rangefind-meta="title">Meta Title</span>
    <h1>H1 Title</h1></main></body></html>`);
  assert.equal(result.title, "Meta Title");
});

test("excludes script, style, nav, and aside subtrees inside the region", () => {
  const result = extractHtml(`<html><body>
    <nav>menu link</nav>
    <main>
      <p>Real content here.</p>
      <script>var secret = 'do not index';</script>
      <style>.x { color: red; }</style>
      <aside>sidebar junk</aside>
    </main>
  </body></html>`);
  assert.equal(result.body, "Real content here.");
  assert.doesNotMatch(result.body, /secret|menu|sidebar|color/);
});

test("excludes header/footer chrome only when the region is the body fallback", () => {
  const result = extractHtml(`<html><body>
    <header>site header nav</header>
    <p>Page body content.</p>
    <footer>footer junk</footer>
  </body></html>`);
  assert.equal(result.body, "Page body content.");
  assert.doesNotMatch(result.body, /header|footer/);
});

test("indexes an in-article header and its h1 as the page title", () => {
  const result = extractHtml(`<html><body><main><article>
    <header><h1>Post Title</h1><time>2026</time></header>
    <p>Body words here.</p>
  </article></main></body></html>`);
  assert.equal(result.title, "Post Title");
  assert.match(result.headings, /Post Title/);
  assert.match(result.body, /Post Title/);
  assert.match(result.body, /Body words here\./);
});

test("script content with angle brackets does not break parsing", () => {
  const result = extractHtml(`<html><body><main>
    <p>Before</p>
    <script>if (a < b && c > d) { x(); }</script>
    <p>After</p>
  </main></body></html>`);
  assert.equal(result.body, "Before After");
});

test("data-rangefind-ignore drops an element subtree", () => {
  const result = extractHtml(`<html><body><main>
    <p>Keep this.</p>
    <div data-rangefind-ignore><p>Drop this.</p></div>
    <p hidden>Hidden too.</p>
  </main></body></html>`);
  assert.equal(result.body, "Keep this.");
});

test("page-level data-rangefind-ignore skips the whole document", () => {
  assert.deepEqual(extractHtml(`<html data-rangefind-ignore><body><main><p>x</p></main></body></html>`), { skip: true });
  assert.deepEqual(extractHtml(`<html><body data-rangefind-ignore><main><p>x</p></main></body></html>`), { skip: true });
});

test("data-rangefind-body scopes indexing to marked elements only", () => {
  const result = extractHtml(`<html><body>
    <main><p>Main region text.</p></main>
    <div data-rangefind-body><p>Indexed region A.</p></div>
    <div>Not indexed.</div>
    <section data-rangefind-body>Indexed region B.</section>
  </body></html>`);
  assert.equal(result.body, "Indexed region A. Indexed region B.");
});

test("prefers article when there is no main", () => {
  const result = extractHtml(`<html><body>
    <div>chrome</div>
    <article><p>Article body.</p></article>
  </body></html>`);
  assert.equal(result.body, "Article body.");
});

test("collects meta (text and attribute forms), filters, and sorts", () => {
  const result = extractHtml(`<html><body><main>
    <span data-rangefind-meta="author">Ada Lovelace</span>
    <time data-rangefind-meta="date:datetime" datetime="2024-05-01">May 1</time>
    <span data-rangefind-filter="tag">alpha</span>
    <span data-rangefind-filter="tag">beta</span>
    <span data-rangefind-filter="section">Guides</span>
    <span data-rangefind-sort="rank">42</span>
    <p>body</p>
  </main></body></html>`);
  assert.deepEqual(result.meta, { author: "Ada Lovelace", date: "2024-05-01" });
  assert.deepEqual(result.filters, { tag: ["alpha", "beta"], section: ["Guides"] });
  assert.deepEqual(result.sorts, { rank: "42" });
});

test("reduces BCP47 lang tags to the primary subtag", () => {
  assert.equal(extractHtml(`<html lang="fr-CA"><body><main>bonjour</main></body></html>`).lang, "fr");
  assert.equal(extractHtml(`<html lang="PT-BR"><body><main>ola</main></body></html>`).lang, "pt");
});

test("decodes named and numeric HTML entities", () => {
  const result = extractHtml(`<html><body><main><p>Tom &amp; Jerry &lt;3 &#39;quotes&#39; &#x2764; caf&eacute;&nbsp;au lait</p></main></body></html>`);
  assert.equal(result.body, "Tom & Jerry <3 'quotes' ❤ café au lait");
});

test("tolerates unclosed tags and messy markup", () => {
  const result = extractHtml(`<html><body><main>
    <p>First paragraph
    <p>Second paragraph
    <ul><li>one<li>two<li>three</ul>
    <img src="x.png">
    <br>
    <p>a < b is math, not a tag</p>
  </main></body></html>`);
  assert.match(result.body, /First paragraph/);
  assert.match(result.body, /Second paragraph/);
  assert.match(result.body, /one two three/);
  assert.match(result.body, /a < b is math/);
});

test("handles documents with no body tag without leaking head text", () => {
  const result = extractHtml(`<head><title>Frag</title></head><p>Fragment body.</p>`);
  assert.equal(result.title, "Frag");
  assert.equal(result.body, "Fragment body.");
});

test("strips HTML comments before extraction", () => {
  const result = extractHtml(`<html><body><main><p>Visible</p><!-- <p>Commented</p> --></main></body></html>`);
  assert.equal(result.body, "Visible");
});

test("collects hrefs of links inside the indexed region", () => {
  const result = extractHtml(`<html><body><main>
    <p>See the <a href="/guide/">guide</a> and the <a href="intro.html">intro</a>.</p>
    <a href="https://example.com">external</a>
  </main></body></html>`);
  assert.deepEqual(result.links, ["/guide/", "intro.html", "https://example.com"]);
});

test("excludes links in nav/aside/footer chrome from the link set", () => {
  const result = extractHtml(`<html><body>
    <nav><a href="/menu/">menu</a></nav>
    <main><p>Body <a href="/real/">real</a>.</p></main>
    <footer><a href="/legal/">legal</a></footer>
  </body></html>`);
  assert.deepEqual(result.links, ["/real/"]);
});

test("ignores links marked with data-rangefind-ignore", () => {
  const result = extractHtml(`<html><body><main>
    <a href="/kept/">kept</a>
    <a href="/skip/" data-rangefind-ignore>skip</a>
  </main></body></html>`);
  assert.deepEqual(result.links, ["/kept/"]);
});
