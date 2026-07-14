"use strict";

// docusaurus-plugin-rangefind — index a built Docusaurus site with Rangefind
// (a static search engine: static index + HTTP Range requests, no server) and
// inject the drop-in `<rangefind-search>` web component site-wide.
//
// This module is CommonJS on purpose: Docusaurus loads plugin modules with a
// CJS-first loader, and `require.resolve` gives us the real on-disk paths of
// the `rangefind/element` bundle and its CSS without executing them. The
// crawler (`rangefind/crawler`) is ESM in an otherwise-ESM package, so it is
// pulled in with a dynamic `import()` from inside the async `postBuild` hook —
// that works from CJS regardless of the target module's format.

const fs = require("node:fs");
const path = require("node:path");

const DEFAULT_OUTPUT_DIR = "rangefind"; // index dir, relative to outDir
const DEFAULT_ASSETS_DIR = "_rangefind"; // client asset dir, relative to outDir
const ASSET_JS = "rangefind-search.js";
const ASSET_CSS = "rangefind-search.css";

// Docusaurus base URLs always carry a trailing slash; normalize so callers can
// pass "/base" or "/base/" interchangeably and our string joins never double
// or drop the separator.
function normalizeBaseUrl(baseUrl) {
  const value = String(baseUrl || "/");
  return value.endsWith("/") ? value : `${value}/`;
}

module.exports = function rangefindPlugin(context = {}, options = {}) {
  const enabled = options.enabled !== false;
  const theme = options.theme !== false;
  const assetsDir = options.assetsDir || DEFAULT_ASSETS_DIR;
  const outputDir = options.outputDir || DEFAULT_OUTPUT_DIR;
  // Rangefind config overrides merged into the crawler's generated config,
  // and an optional enrich hook — an async function(docs) or a path to an
  // ES module default-exporting one — run before indexing (embeddings, …).
  const crawlConfig = options.config || null;
  const enrich = options.enrich || null;

  // Base URL for the injected asset tags. Prefer an explicit override, then the
  // site's own configured baseUrl so the tag src matches the deployed prefix.
  const siteBaseUrl = normalizeBaseUrl(
    options.baseUrl != null ? options.baseUrl : context && context.siteConfig && context.siteConfig.baseUrl
  );

  return {
    name: "docusaurus-plugin-rangefind",

    // Official post-build lifecycle hook: the whole site is already emitted to
    // props.outDir, so we crawl that HTML into a Rangefind index and copy the
    // search UI assets alongside it.
    async postBuild(props) {
      if (!enabled) return;

      const outDir = props.outDir;
      // buildFromCrawl's baseUrl is the URL prefix stamped onto every result
      // URL; Docusaurus's own baseUrl is the right default so links resolve on
      // the deployed site (root-relative under any sub-path deploy).
      const crawlBaseUrl = normalizeBaseUrl(
        options.baseUrl != null
          ? options.baseUrl
          : props.baseUrl != null
            ? props.baseUrl
            : props.siteConfig && props.siteConfig.baseUrl != null
              ? props.siteConfig.baseUrl
              : siteBaseUrl
      );

      const output = path.isAbsolute(outputDir) ? outputDir : path.join(outDir, outputDir);

      // ESM crawler pulled in dynamically from this CJS module.
      const { buildFromCrawl } = await import("rangefind/crawler");
      await buildFromCrawl({
        root: outDir,
        scanDir: outDir,
        output,
        baseUrl: crawlBaseUrl,
        config: crawlConfig,
        enrich
      });

      // Copy the two client assets next to the site. Paths resolve through the
      // `rangefind` package exports map to the real dist files.
      const assetsOut = path.join(outDir, assetsDir);
      await fs.promises.mkdir(assetsOut, { recursive: true });
      const jsSrc = require.resolve("rangefind/element");
      const cssSrc = require.resolve("rangefind/element.css");
      await fs.promises.copyFile(jsSrc, path.join(assetsOut, ASSET_JS));
      await fs.promises.copyFile(cssSrc, path.join(assetsOut, ASSET_CSS));
    },

    // Inject the search element's script (and, unless disabled, the optional
    // theme) on every page. The `<rangefind-search>` element itself is NOT
    // injected here — users place it where they want (navbar html item, MDX,
    // or a swizzled component); see the README.
    injectHtmlTags() {
      if (!enabled) return {};
      const tags = [
        {
          tagName: "script",
          attributes: {
            type: "module",
            src: `${siteBaseUrl}${assetsDir}/${ASSET_JS}`
          }
        }
      ];
      if (theme) {
        tags.push({
          tagName: "link",
          attributes: {
            rel: "stylesheet",
            href: `${siteBaseUrl}${assetsDir}/${ASSET_CSS}`
          }
        });
      }
      return { postBodyTags: tags };
    }
  };
};
