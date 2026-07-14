// Semantic hybrid search for this site's own boxes — progressive
// enhancement over the always-working lexical search.
//
// The index carries a 384-dim MiniLM embedding per page (built by the
// enrich hook in eleventy.config.js). This module lazily loads the same
// model in the browser via transformers.js, embeds the query, and installs
// a params transform on each <rangefind-search> so results fuse the text
// and vector lanes (reciprocal-rank fusion).
//
// Loading policy: the ~25 MB model is fetched only after someone focuses
// the hero search box (clear intent, explained in its caption); it then
// caches in the browser. The header box upgrades to hybrid automatically
// once the model is available, and stays instant-lexical until then.

let embedderPromise = null;

function loadEmbedder() {
  if (!embedderPromise) {
    embedderPromise = (async () => {
      const { pipeline } = await import("https://cdn.jsdelivr.net/npm/@huggingface/transformers@3.3.1");
      return pipeline("feature-extraction", "Xenova/all-MiniLM-L6-v2", { dtype: "q8" });
    })();
    embedderPromise.catch(() => { embedderPromise = null; });
  }
  return embedderPromise;
}

let ready = false;

async function embed(q) {
  const extractor = await loadEmbedder();
  const output = await extractor(q, { pooling: "mean", normalize: true });
  const vector = Array.from(output.data);
  output.dispose?.();
  ready = true;
  return vector;
}

function transformFor(box, { wait }) {
  return async params => {
    if (!params.q) return params;
    try {
      // The hero box waits for the model (it is the showcase); other boxes
      // use it only once it's already warm, so they never stall.
      if (!wait && !ready) return params;
      params.vector = await embed(params.q);
      box.dataset.semantic = "on";
    } catch {
      // Model unavailable (offline, blocked CDN): lexical results stand.
    }
    return params;
  };
}

customElements.whenDefined("rangefind-search").then(() => {
  const hero = document.querySelector(".hero__demo rangefind-search");
  for (const box of document.querySelectorAll("rangefind-search")) {
    const isHero = box === hero;
    box.searchOptions = { ...box.searchOptions, transform: transformFor(box, { wait: isHero }) };
  }
  // Warm the model on clear intent.
  hero?.addEventListener("focusin", () => { loadEmbedder(); }, { once: true });
});
