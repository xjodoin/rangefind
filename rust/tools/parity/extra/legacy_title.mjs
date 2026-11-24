// Legacy title-only authority suggest: an index with an authority field but
// no `suggest` config, so suggestions come from authority title keys rather
// than the autocomplete lexicon (executeLegacyAuthoritySuggest).
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { build } from "../../../../src/builder.js";

const WORDS = ["museum", "library", "river", "city", "station", "castle", "bibliothèque", "café", "stadium", "museo", "music", "mustard", "lighthouse", "bridge", "park"];

export async function buildLegacyTitleIndex(root) {
  const docs = [];
  for (let i = 0; i < 300; i++) {
    const a = WORDS[i % WORDS.length];
    const b = WORDS[(i * 7 + 3) % WORDS.length];
    docs.push({ id: `d${i}`, title: `${a} ${b}${i % 5 === 0 ? "" : ` ${i % 11}`}`, body: `${b} ${a} text number ${i}`, rank: i % 13 });
  }
  await writeFile(join(root, "docs.jsonl"), docs.map(doc => JSON.stringify(doc)).join("\n"));
  const configPath = join(root, "rangefind.config.json");
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "index",
    fields: [{ name: "title", path: "title", weight: 4 }, { name: "body", path: "body", weight: 1 }],
    authority: [{ name: "title", path: "title" }],
    display: ["id", "title"],
    buildTelemetrySampleMs: 0,
    resumeBuild: false
  }));
  await build({ configPath });
}

export function legacyTitleCases() {
  const cases = [];
  for (const q of ["m", "mu", "mus", "museum", "museum l", "lib", "st", "zz", "", "café", "bibli", "river c", "Museum Library"]) {
    cases.push({ method: "suggest", params: { q, size: 5 } });
    cases.push({ method: "suggest", params: { q, size: 3, hydrate: true } });
    cases.push({ method: "suggest", params: { q, size: 50, authorityMaxShards: 1 } });
    cases.push({ method: "search", params: { q, size: 5 } });
    cases.push({ method: "search", params: { q: `${q}x`, size: 5 } });
    cases.push({ method: "authorityLookup", params: { surface: q, size: 5 } });
  }
  return cases;
}
