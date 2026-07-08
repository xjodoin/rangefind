// eleventy-plugin-rangefind — index a built Eleventy site with Rangefind and
// drop in the <rangefind-search> Web Component.
//
// Rangefind is a static search engine: the build step crawls the already-built
// HTML into a packed static index, and the client fetches only the byte ranges
// it needs via HTTP Range requests — no search server. This plugin wires that
// into Eleventy's real build lifecycle:
//
//   1. On `eleventy.after` (the official post-build hook), crawl the output
//      directory into `<output>/<outputDir>` and copy the two client assets
//      into `<output>/<assetsDir>`.
//   2. A universal `rangefindSearch` shortcode renders the markup that mounts
//      the component.

import { copyFile, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { buildFromCrawl } from "rangefind/crawler";
import { renderSearchMarkup } from "./src/render_markup.js";

// Resolve the real on-disk path of a Rangefind subpath export. Both
// `rangefind/element` and `rangefind/element.css` are declared in Rangefind's
// package.json `exports`; import.meta.resolve returns a file:// URL we convert
// to a path. Resolution is relative to THIS module, so it follows the plugin's
// own dependency tree regardless of the consumer's cwd.
function resolveAsset(specifier) {
  return fileURLToPath(import.meta.resolve(specifier));
}

export default function rangefindPlugin(eleventyConfig, options = {}) {
  const {
    enabled = true,
    // Rangefind index directory, relative to Eleventy's output dir.
    outputDir = "rangefind",
    // Client asset directory (JS + optional CSS), relative to the output dir.
    assetsDir = "_rangefind",
    // URL prefix/origin baked into indexed result URLs.
    baseUrl = "/",
    // Shortcode defaults (overridable per call site).
    src = "/rangefind/",
    assetsBase = "/_rangefind",
    theme = false,
    shortcodeName = "rangefindSearch"
  } = options;

  if (enabled === false) return;

  // Universal shortcode: addShortcode registers across Nunjucks, Liquid,
  // Markdown (via its template engine), Handlebars, and 11ty.js — the widest
  // coverage for a simple options-object shortcode.
  eleventyConfig.addShortcode(shortcodeName, (args = {}) =>
    renderSearchMarkup(args, { src, assetsBase, theme })
  );

  // eleventy.after fires once the whole site is written. The event object is
  // `{ directories, dir, results, runMode, outputMode }`. `directories.output`
  // is the normalized project output dir (Eleventy 3); `dir.output` is the
  // legacy, un-normalized fallback (Eleventy 2). We prefer the former.
  eleventyConfig.on("eleventy.after", async ({ directories, dir } = {}) => {
    const outputRoot = directories?.output || dir?.output;
    if (!outputRoot) {
      throw new Error("eleventy-plugin-rangefind: could not determine Eleventy output directory from the eleventy.after event.");
    }

    // 1. Crawl the built HTML into the static index. The crawler prunes the
    //    nested index dir from its own scan automatically.
    await buildFromCrawl({
      root: outputRoot,
      scanDir: outputRoot,
      output: join(outputRoot, outputDir),
      baseUrl
    });

    // 2. Copy the client assets next to the site.
    const assetDirAbs = join(outputRoot, assetsDir);
    await mkdir(assetDirAbs, { recursive: true });
    await copyFile(resolveAsset("rangefind/element"), join(assetDirAbs, "rangefind-search.js"));
    await copyFile(resolveAsset("rangefind/element.css"), join(assetDirAbs, "rangefind-search.css"));
  });
}
