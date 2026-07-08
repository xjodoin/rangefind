// Fixture Eleventy config used by the real end-to-end build test. Exports a
// config function (the form Eleventy accepts both as a project config file and
// as the programmatic `config` option) that registers the plugin.
import rangefindPlugin from "../../index.js";

export default function (eleventyConfig) {
  eleventyConfig.addPlugin(rangefindPlugin, {
    baseUrl: "/",
    // Defaults for the shortcode; exercised by src/index.njk.
    src: "/rangefind/",
    assetsBase: "/_rangefind"
  });

  // Markdown -> Nunjucks so the {% rangefindSearch %} shortcode also works in
  // .md files (not exercised here, but documents the universal coverage).
  return {
    markdownTemplateEngine: "njk"
  };
}
