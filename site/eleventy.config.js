import { readFileSync, readdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import markdownIt from "markdown-it";
import syntaxHighlight from "@11ty/eleventy-plugin-syntaxhighlight";
import rangefindPlugin from "eleventy-plugin-rangefind";

const siteDir = dirname(fileURLToPath(import.meta.url));

// GitHub Pages serves the site under /rangefind/; local dev serves at /.
// Every internal URL goes through Eleventy's `url` filter, and the search
// plugin gets the same prefix so the index and its assets resolve anywhere.
const prefix = process.env.PATH_PREFIX || "/";

export default function (eleventyConfig) {
  eleventyConfig.addPlugin(syntaxHighlight);

  // Dogfood: after the site builds, rangefind crawls the built HTML into a
  // static index at <output>/rangefind/ and copies the <rangefind-search>
  // web component next to it. The search boxes on this site query it —
  // lexically always, and semantically once the browser has the MiniLM
  // model (assets/semantic.js embeds queries; the enrich hook below embeds
  // the pages at build time with the same model, so both share one space).
  eleventyConfig.addPlugin(rangefindPlugin, {
    baseUrl: prefix,
    src: `${prefix}rangefind/`,
    assetsBase: `${prefix}_rangefind`,
    config: {
      vectors: [{ name: "embedding", path: "embedding", dims: 384 }]
    },
    enrich: async docs => {
      const { pipeline } = await import("@huggingface/transformers");
      const embed = await pipeline("feature-extraction", "Xenova/all-MiniLM-L6-v2", { dtype: "q8" });
      for (const doc of docs) {
        const text = `${doc.title || ""}. ${String(doc.body || "").slice(0, 480)}`;
        const output = await embed(text, { pooling: "mean", normalize: true });
        doc.embedding = Array.from(output.data);
        output.dispose?.();
      }
      console.log(`[site] embedded ${docs.length} page(s) for semantic search`);
      return docs;
    }
  });

  // GitHub-style heading ids so #fragment links between docs pages resolve.
  eleventyConfig.amendLibrary("md", mdLib => {
    const slugify = text =>
      text.toLowerCase().trim().replace(/[^\w\s-]/gu, "").replace(/\s+/gu, "-");
    mdLib.core.ruler.push("heading_ids", state => {
      const seen = new Map();
      for (let i = 0; i < state.tokens.length; i++) {
        if (state.tokens[i].type !== "heading_open") continue;
        const inline = state.tokens[i + 1];
        const text = (inline?.children ?? [])
          .filter(t => t.type === "text" || t.type === "code_inline")
          .map(t => t.content)
          .join("");
        let slug = slugify(text) || "section";
        const count = seen.get(slug) ?? 0;
        seen.set(slug, count + 1);
        if (count) slug = `${slug}-${count}`;
        state.tokens[i].attrSet("id", slug);
      }
    });
  });

  eleventyConfig.addPassthroughCopy({ "src/assets": "assets" });

  // The changelog page renders the repository's CHANGELOG.md verbatim, so
  // the site can never drift from the source of truth. Repo-relative links
  // (e.g. packages/rangefind-astro) only resolve on GitHub, so point them at
  // the repository tree instead of letting them 404 under /changelog/.
  eleventyConfig.addGlobalData("changelogHtml", () => {
    const md = markdownIt({ html: false, linkify: true });
    const repoTree = "https://github.com/xjodoin/rangefind/tree/main/";
    const source = readFileSync(resolve(siteDir, "../CHANGELOG.md"), "utf8")
      .replace(/^# Changelog\s*/u, ""); // the page supplies its own H1
    return md
      .render(source)
      .replace(/href="(?!(?:[a-z][a-z0-9+.-]*:|\/|#))([^"]+)"/gu, `href="${repoTree}$1"`);
  });

  // The public Google Maps migration page renders the repository guide rather
  // than maintaining an SEO-only copy. Keep code links useful on the website
  // and route readers to the equivalent first-party site documentation where
  // one exists.
  eleventyConfig.addGlobalData("googleMapsMigrationHtml", () => {
    const md = markdownIt({ html: false, linkify: true });
    const repoBlob = "https://github.com/xjodoin/rangefind/blob/main/";
    const source = readFileSync(resolve(siteDir, "../docs/google-maps-migration.md"), "utf8")
      .replace(/^# Replace Google Maps search APIs with Rangefind OSM\s*/u, "")
      .replace(/\]\(\.\.\/src\/([^)]+)\)/gu, `](${repoBlob}src/$1)`)
      .replace(/\]\(\.\.\/examples\/([^)]+)\)/gu, `](${repoBlob}examples/$1)`)
      .replace(/\]\(sharded-osm\.md\)/gu, "](/docs/sharded-indexes/)")
      .replace(/\]\(route-graph\.md\)/gu, "](/docs/routing/)")
      .replace(/\]\(pulsemesh\.md\)/gu, "](/docs/pulsemesh/)")
      .replace(/\]\(pulsemesh-threads\.md\)/gu, "](/docs/pulsemesh-threads/)")
      .replace(/\]\(osm-maps-benchmark\.md\)/gu, `](${repoBlob}docs/osm-maps-benchmark.md)`);
    return md.render(source);
  });

  // llms-full.txt: every docs page as raw Markdown (front matter stripped,
  // title/lede promoted to a heading), in nav order.
  eleventyConfig.addGlobalData("llmsFullDocs", () => {
    const dir = resolve(siteDir, "src/docs");
    const pages = [];
    for (const file of readdirSync(dir).filter(name => name.endsWith(".md")).sort()) {
      const raw = readFileSync(resolve(dir, file), "utf8");
      const match = raw.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/u);
      if (!match) continue;
      const front = match[1];
      const title = front.match(/^title:\s*(.*)$/mu)?.[1] || file;
      const lede = front.match(/^lede:\s*(.*)$/mu)?.[1] || "";
      const order = Number(front.match(/^order:\s*(\d+)$/mu)?.[1] || 999);
      pages.push({ order, text: `## ${title}\n\n${lede ? `> ${lede}\n\n` : ""}${match[2].trim()}` });
    }
    return pages.sort((a, b) => a.order - b.order).map(page => page.text).join("\n\n---\n\n");
  });

  eleventyConfig.addCollection("docs", collection =>
    collection
      .getFilteredByGlob("src/docs/*.md")
      .sort((a, b) => (a.data.order || 0) - (b.data.order || 0))
  );

  return {
    dir: { input: "src", includes: "_includes", output: "_site" },
    pathPrefix: prefix,
    markdownTemplateEngine: "njk",
    htmlTemplateEngine: "njk"
  };
}
