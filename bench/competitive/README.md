# Competitive static-search benchmark

Head-to-head of **rangefind** vs [Pagefind](https://pagefind.app/) vs
[Orama](https://orama.com/) on the same French Wikipedia corpus. Kept in its own
workspace so Pagefind/Orama/Playwright never touch the core package's
dependencies.

## Run it

```bash
cd bench/competitive
npm install
npx playwright install chromium     # for the Pagefind (browser/WASM) stage

node run.mjs --docs=10000 --queries=300   # index + size + quality for all three
node browser.mjs --transfer=60            # Pagefind query + transfer in Chromium
cat results.json
```

The corpus is read from `examples/frwiki/scale/50000/data/frwiki.jsonl` (produced
by `node scripts/frwiki_fixture.mjs`). `results.sample.json` is a committed
snapshot of one 10k-doc run for provenance.

## What is measured, and how it is kept fair

- **Same corpus, same analysis.** All three index identical `{id, title, body}`
  records with French stemming + stopwords.
- **Competent configs.** Orama's search is title-weighted (`boost`) with
  `threshold: 0` so recall isn't truncated — not left on defaults.
- **Quality = known-item retrieval.** The query is an article title; the correct
  answer is that article. recall@10 and MRR are analyzer-agnostic this way.
- **Transfer = index bytes to answer a cold query**, counted at the server, with
  each engine's runtime code (rangefind/Pagefind JS, Pagefind WASM) excluded on
  both sides. Orama is in-memory, so its cold per-query transfer is the whole
  index (it must be downloaded before any query).
- Pagefind's search runtime is browser-only WASM, so it is queried in headless
  Chromium (`browser.mjs`); rangefind and Orama are queried in Node.

## What it does NOT measure

Only French text retrieval on known-item queries. It says nothing about geo,
vector/hybrid, or link-graph ranking — Pagefind and Orama don't expose directly
comparable lanes. Both are strong tools built for different goals.
