// Dependency-free HTML text extraction for the static-site crawler.
//
// Rangefind ships with zero runtime dependencies, so this is a small tolerant
// tokenizer over the raw HTML string rather than a real DOM. Static-site HTML
// is a well-bounded problem: we need robust text extraction plus attribute
// reading, not spec-perfect parsing. The tokenizer is intentionally forgiving
// of unclosed tags, comments, CDATA, and quoted/unquoted attributes.
//
// The attribute vocabulary mirrors Pagefind under a `rangefind` namespace:
//   data-rangefind-body     — index only the text inside these elements
//   data-rangefind-ignore   — drop this subtree (page-level on <html>/<body>)
//   data-rangefind-meta     — "name" (element text) or "name:attr" (attribute)
//   data-rangefind-filter   — multi-valued facet, value = element text
//   data-rangefind-sort     — sort value, value = element text

// `<nav>`/`<aside>` are chrome even inside an article, so they are always
// excluded. `<header>`/`<footer>` are excluded only when the selected content
// region is the `<body>` fallback (no `<main>`/`<article>` present), where they
// are page chrome; inside a real `<main>`/`<article>` region an in-article
// header carries the page's own `<h1>` and must be indexed.
const ALWAYS_EXCLUDE_TAGS = new Set(["nav", "aside"]);
const CHROME_EXCLUDE_TAGS = new Set(["nav", "aside", "header", "footer"]);

// Raw-text elements: their content is not HTML and must be dropped before
// tokenizing so a `<` inside a script or stylesheet never looks like a tag.
const RAWTEXT_TAGS = ["script", "style", "template", "noscript"];

// Precompiled once at module load rather than per document. `stripNonContent`
// runs on every crawled page, so hoisting these 8 patterns out of the hot loop
// removes a `new RegExp` compile per raw-text tag per document — a measurable
// win on large crawls. `paired` removes a well-formed `<tag>…</tag>`; `dangling`
// mops up a truncated raw-text element that never closes.
const RAWTEXT_PATTERNS = RAWTEXT_TAGS.map(tag => ({
  paired: new RegExp(`<${tag}\\b[^>]*>[\\s\\S]*?<\\/${tag}\\s*>`, "gi"),
  dangling: new RegExp(`<${tag}\\b[^>]*>[\\s\\S]*$`, "i")
}));

// Void elements never have a closing tag; treat them as self-closing so the
// tolerant stack does not wait for a `</meta>` that will never come.
const VOID_TAGS = new Set([
  "area", "base", "br", "col", "embed", "hr", "img", "input",
  "link", "meta", "param", "source", "track", "wbr"
]);

const HEADING_TAGS = new Set(["h1", "h2", "h3", "h4"]);

const NAMED_ENTITIES = {
  amp: "&", lt: "<", gt: ">", quot: "\"", apos: "'", nbsp: " ",
  copy: "©", reg: "®", trade: "™", hellip: "…",
  mdash: "—", ndash: "–", rsquo: "’", lsquo: "‘",
  ldquo: "“", rdquo: "”", laquo: "«", raquo: "»",
  eacute: "é", egrave: "è", agrave: "à", ccedil: "ç"
};

function decodeEntities(text) {
  if (text.indexOf("&") === -1) return text;
  return text.replace(/&(#x[0-9a-fA-F]+|#\d+|[a-zA-Z][a-zA-Z0-9]*);/g, (match, body) => {
    if (body[0] === "#") {
      const code = body[1] === "x" || body[1] === "X"
        ? parseInt(body.slice(2), 16)
        : parseInt(body.slice(1), 10);
      if (!Number.isFinite(code) || code <= 0 || code > 0x10ffff) return match;
      try {
        return String.fromCodePoint(code);
      } catch {
        return match;
      }
    }
    const named = NAMED_ENTITIES[body];
    return named === undefined ? match : named;
  });
}

function collapse(text) {
  return text.replace(/\s+/g, " ").trim();
}

// Parse an attribute string into a lowercase-keyed map. HTML attribute names
// are case-insensitive; values are entity-decoded. Handles double/single/
// unquoted values and bare boolean attributes.
function parseAttrs(source) {
  const attrs = {};
  const re = /([^\s=/>]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+)))?/g;
  let match;
  while ((match = re.exec(source))) {
    const name = match[1].toLowerCase();
    if (!name) continue;
    const raw = match[2] ?? match[3] ?? match[4] ?? "";
    attrs[name] = decodeEntities(raw);
  }
  return attrs;
}

// Strip content that must never reach the tokenizer: comments, CDATA, and the
// raw-text elements. A dangling unclosed raw-text tag is dropped to end-of-input
// so a truncated `<script>` cannot swallow the parser.
function stripNonContent(html) {
  let out = html.replace(/<!--[\s\S]*?-->/g, " ");
  out = out.replace(/<!\[CDATA\[[\s\S]*?\]\]>/g, " ");
  for (const pattern of RAWTEXT_PATTERNS) {
    out = out.replace(pattern.paired, " ");
    out = out.replace(pattern.dangling, " ");
  }
  return out;
}

// Tokenize into a flat list of open/close/text tokens. Only a leading letter
// after `<` starts a tag; anything else (e.g. "a < b") is treated as text.
function* tokenize(html) {
  let i = 0;
  const len = html.length;
  while (i < len) {
    const lt = html.indexOf("<", i);
    if (lt === -1) {
      yield { type: "text", value: html.slice(i) };
      break;
    }
    if (lt > i) yield { type: "text", value: html.slice(i, lt) };
    const next = html[lt + 1];
    if (next === "!" || next === "?") {
      const close = html.indexOf(">", lt + 1);
      if (close === -1) break;
      i = close + 1;
      continue;
    }
    const isClose = next === "/";
    const nameStart = lt + (isClose ? 2 : 1);
    if (!/[a-zA-Z]/.test(html[nameStart] || "")) {
      // Not a real tag; emit the "<" as literal text and move on.
      yield { type: "text", value: "<" };
      i = lt + 1;
      continue;
    }
    const close = html.indexOf(">", nameStart);
    if (close === -1) {
      yield { type: "text", value: html.slice(lt) };
      break;
    }
    let inner = html.slice(nameStart, close);
    const selfClosed = inner.endsWith("/");
    if (selfClosed) inner = inner.slice(0, -1);
    const nameMatch = /^([a-zA-Z][a-zA-Z0-9:-]*)/.exec(inner);
    const name = nameMatch ? nameMatch[1].toLowerCase() : "";
    i = close + 1;
    if (!name) continue;
    if (isClose) {
      yield { type: "close", name };
    } else {
      yield { type: "open", name, attrs: parseAttrs(inner.slice(name.length)), selfClosed };
    }
  }
}

function isIgnored(attrs) {
  return "data-rangefind-ignore" in attrs || "hidden" in attrs;
}

function primaryLang(value) {
  return String(value || "").trim().toLowerCase().split(/[-_]/)[0];
}

// Extract structured, indexable content from an HTML document string. Returns
// `{ skip: true }` when the page opts out via a page-level data-rangefind-ignore
// on <html> or <body>.
export function extractHtml(html) {
  const source = String(html || "");
  const hasExplicitBody = /data-rangefind-body/i.test(source);
  const hasMain = /<main[\s/>]/i.test(source);
  const hasArticle = /<article[\s/>]/i.test(source);
  const hasBodyTag = /<body[\s/>]/i.test(source);
  const regionTag = hasMain ? "main" : hasArticle ? "article" : "body";
  // Only tag-exclude header/footer when we fall back to the whole <body>.
  const excludeTags = regionTag === "body" ? CHROME_EXCLUDE_TAGS : ALWAYS_EXCLUDE_TAGS;

  const cleaned = stripNonContent(source);

  const meta = {};
  const filters = {};
  const sorts = {};
  const headingParts = [];
  const bodyParts = [];
  // Raw hrefs of `<a>` links found inside the indexed content region. The
  // crawler resolves these to internal document ids to build the link graph
  // (src/link_graph.js); chrome links in nav/aside/header/footer are excluded
  // by the same region logic that keeps them out of the body text.
  const links = [];
  let titleTag = "";
  let firstH1 = "";
  let lang = "";
  let description = "";
  let skip = false;

  // Stack frames accumulate element text so captures that need element text
  // (data-rangefind-meta text form, -filter, -sort, headings) resolve on close.
  const stack = [];
  let excludeDepth = 0;
  let regionDepth = 0;
  let explicitBodyDepth = 0;
  let headDepth = 0;

  const inBody = () => (hasExplicitBody
    ? explicitBodyDepth > 0
    : regionDepth > 0 || (regionTag === "body" && !hasBodyTag))
    && excludeDepth === 0
    && headDepth === 0;

  for (const token of tokenize(cleaned)) {
    if (token.type === "text") {
      const value = decodeEntities(token.value);
      if (stack.length) stack[stack.length - 1].text.push(value);
      if (inBody()) bodyParts.push(value);
      continue;
    }

    if (token.type === "open") {
      const { name, attrs } = token;
      const selfExcluded = excludeTags.has(name) || isIgnored(attrs);

      // Page-level opt-out short-circuits the whole document.
      if ((name === "html" || name === "body") && "data-rangefind-ignore" in attrs) {
        skip = true;
        break;
      }
      if (name === "html" && attrs.lang) lang = primaryLang(attrs.lang);
      if (name === "meta" && attrs.name && attrs.name.toLowerCase() === "description" && attrs.content) {
        description = collapse(attrs.content);
      }
      if (name === "a" && attrs.href && !isIgnored(attrs) && inBody()) {
        const href = attrs.href.trim();
        if (href) links.push(href);
      }

      // Resolve attribute-form captures now (they need no element text).
      const outerExcluded = excludeDepth > 0;
      const metaSpec = attrs["data-rangefind-meta"];
      let captureMeta = null;
      if (metaSpec && !outerExcluded) {
        const colon = metaSpec.indexOf(":");
        if (colon !== -1) {
          const metaName = metaSpec.slice(0, colon).trim();
          const attrName = metaSpec.slice(colon + 1).trim().toLowerCase();
          const value = collapse(attrs[attrName] || "");
          if (metaName && value && meta[metaName] === undefined) meta[metaName] = value;
        } else {
          captureMeta = metaSpec.trim();
        }
      }
      const filterKey = !outerExcluded ? attrs["data-rangefind-filter"] : undefined;
      const sortKey = !outerExcluded ? attrs["data-rangefind-sort"] : undefined;

      const voidLike = token.selfClosed || VOID_TAGS.has(name);
      if (voidLike) {
        // Void/self-closed elements have no text; only structural counters and
        // attribute-form captures matter, both already handled above.
        continue;
      }

      if (selfExcluded) excludeDepth++;
      if (name === regionTag) regionDepth++;
      if (name === "head") headDepth++;
      if ("data-rangefind-body" in attrs) explicitBodyDepth++;
      stack.push({
        name,
        text: [],
        selfExcluded,
        outerExcluded,
        regionMatch: name === regionTag,
        headMatch: name === "head",
        explicitBody: "data-rangefind-body" in attrs,
        captureMeta: captureMeta && meta[captureMeta] === undefined ? captureMeta : null,
        filterKey: filterKey ? filterKey.trim() : "",
        sortKey: sortKey ? sortKey.trim() : "",
        heading: HEADING_TAGS.has(name),
        title: name === "title"
      });
      continue;
    }

    // Close tag: pop down to the matching frame (tolerant of stray closes).
    let frameIndex = -1;
    for (let s = stack.length - 1; s >= 0; s--) {
      if (stack[s].name === token.name) { frameIndex = s; break; }
    }
    if (frameIndex === -1) continue;
    for (let s = stack.length - 1; s >= frameIndex; s--) {
      const frame = stack.pop();
      const text = collapse(frame.text.join(" "));
      const suppressed = frame.outerExcluded || frame.selfExcluded;
      if (!suppressed) {
        if (frame.captureMeta && text && meta[frame.captureMeta] === undefined) meta[frame.captureMeta] = text;
        if (frame.filterKey && text) (filters[frame.filterKey] ||= []).push(text);
        if (frame.sortKey && text && sorts[frame.sortKey] === undefined) sorts[frame.sortKey] = text;
        if (frame.heading && text) {
          headingParts.push(text);
          if (frame.name === "h1" && !firstH1) firstH1 = text;
        }
        if (frame.title && text && !titleTag) titleTag = text;
      }
      // Merge child text into the parent frame so ancestor captures see it.
      if (stack.length) stack[stack.length - 1].text.push(...frame.text);
      if (frame.selfExcluded) excludeDepth--;
      if (frame.regionMatch) regionDepth--;
      if (frame.headMatch) headDepth--;
      if (frame.explicitBody) explicitBodyDepth--;
    }
  }

  if (skip) return { skip: true };

  const title = collapse(meta.title || firstH1 || titleTag || "");
  return {
    skip: false,
    title,
    headings: collapse(headingParts.join(" ")),
    body: collapse(bodyParts.join(" ")),
    lang,
    description,
    meta,
    filters,
    sorts,
    links
  };
}
