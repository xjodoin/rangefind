// Minimal Docusaurus site used only to exercise docusaurus-plugin-rangefind
// end-to-end against the real @docusaurus/core CLI.

const path = require("node:path");

module.exports = {
  title: "Rangefind Fixture",
  tagline: "End-to-end test site",
  url: "https://example.com",
  baseUrl: "/",
  favicon: undefined,
  onBrokenLinks: "warn",
  onBrokenMarkdownLinks: "warn",
  onBrokenAnchors: "warn",
  presets: [
    [
      "classic",
      {
        docs: {
          path: "docs",
          routeBasePath: "docs",
          sidebarPath: require.resolve("./sidebars.js")
        },
        blog: false,
        theme: {}
      }
    ]
  ],
  plugins: [
    // The plugin under test, loaded straight from its real entry point.
    [require.resolve("../../index.js"), { theme: true }]
  ],
  themeConfig: {
    navbar: {
      title: "Fixture",
      items: [
        // Idiomatic placement #1: a built-in `html` navbar item. The globally
        // injected script upgrades the custom element on the client.
        {
          type: "html",
          position: "right",
          value: '<rangefind-search src="/rangefind/" placeholder="Search…"></rangefind-search>'
        }
      ]
    }
  }
};
