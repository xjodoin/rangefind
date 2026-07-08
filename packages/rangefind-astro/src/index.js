// Astro integration for Rangefind.
//
// At `astro build` time (the `astro:build:done` hook) this crawls the built
// static output with Rangefind's own crawler, writes the search index nested
// under the site output so it deploys with everything else, and copies the
// drop-in `<rangefind-search>` web component + optional theme into a public
// assets directory. Pair it with the `RangefindSearch.astro` component.

import { mkdir, copyFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join, isAbsolute } from "node:path";
import { buildFromCrawl } from "rangefind/crawler";

const NAME = "rangefind";

// Resolve a client asset shipped by the `rangefind` package to a real file
// path. `import.meta.resolve` returns a `file://` URL for a package export.
function resolveAsset(specifier) {
  return fileURLToPath(import.meta.resolve(specifier));
}

// Join a configured sub-path onto the build output directory. Absolute paths
// are honored as-is; relative paths (the default) stay nested under the site.
function resolveUnderOut(outDir, subPath) {
  return isAbsolute(subPath) ? subPath : join(outDir, subPath);
}

/**
 * @param {object} [options]
 * @param {boolean} [options.enabled=true] Set false to skip indexing entirely.
 * @param {string} [options.baseUrl="/"] URL prefix/origin for result URLs.
 * @param {string} [options.outputDir="rangefind"] Index output dir, relative to the build output (or absolute).
 * @param {string} [options.assetsDir="_rangefind"] Client-asset dir, relative to the build output (or absolute).
 * @returns {import("astro").AstroIntegration}
 */
export default function rangefindAstro(options = {}) {
  const {
    enabled = true,
    baseUrl = "/",
    outputDir = "rangefind",
    assetsDir = "_rangefind"
  } = options;

  return {
    name: NAME,
    hooks: {
      "astro:build:done": async ({ dir, logger }) => {
        // `astro:build:done` only fires for `astro build`, so there is no dev
        // mode to guard against — the `enabled` flag is the per-env opt-out.
        if (enabled === false) {
          logger.info("disabled (options.enabled === false); skipping index build");
          return;
        }
        // `dir` may be missing on server/adapter builds that emit no static
        // HTML output directory; skip cleanly rather than crash.
        if (!dir) {
          logger.warn("no static output directory for this build; skipping index build");
          return;
        }

        const outDir = fileURLToPath(dir);
        const indexOut = resolveUnderOut(outDir, outputDir);
        const assetsOut = resolveUnderOut(outDir, assetsDir);

        try {
          const result = await buildFromCrawl({
            root: outDir,
            scanDir: outDir,
            output: indexOut,
            baseUrl
          });
          logger.info(
            `indexed ${result.docs} doc(s) from ${result.files} file(s) -> ${indexOut}`
          );
        } catch (error) {
          // A site with no indexable HTML (e.g. crawler throws) should fail the
          // build loudly — the developer added this integration on purpose.
          logger.error(`index build failed: ${error?.message || error}`);
          throw error;
        }

        // Copy the client assets so `<rangefind-search>` can load them.
        await mkdir(assetsOut, { recursive: true });
        await copyFile(
          resolveAsset("rangefind/element"),
          join(assetsOut, "rangefind-search.js")
        );
        await copyFile(
          resolveAsset("rangefind/element.css"),
          join(assetsOut, "rangefind-search.css")
        );
        logger.info(`copied search component assets -> ${assetsOut}`);
      }
    }
  };
}
