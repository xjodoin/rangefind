// Turnkey link-graph authority for any corpus that already knows its own edges.
//
// The static-site crawler builds `linkRank` from HTML links automatically. For a
// JSONL corpus (or a framework integration that passes an `enrich` hook), use
// this module instead: point it at a document field holding each document's
// out-links and it computes the same PageRank prior.
//
//   rangefind build ./dist --enrich ./examples/link-graph-enrich.mjs
//
// or, from a plugin/integration that forwards enrich options, pass this module
// path. It default-exports the async enricher and also exports `config` so the
// `linkRank` number, its sort, and the `linkGraph` boost block are declared for
// you — the same shape the crawler emits.
//
// Contract: each document has an id (default field `id`) and an out-links field
// (default `links`) that is an array of target ids. Unknown targets and
// self-links are ignored. Tune with environment variables:
//   RANGEFIND_LINK_ID_FIELD    (default "id")
//   RANGEFIND_LINK_FIELD       (default "links")
//   RANGEFIND_LINK_BOOST       (default "0.5")

import { computeLinkRank } from "rangefind/link-graph";

const ID_FIELD = process.env.RANGEFIND_LINK_ID_FIELD || "id";
const LINK_FIELD = process.env.RANGEFIND_LINK_FIELD || "links";
const BOOST = Number(process.env.RANGEFIND_LINK_BOOST ?? 0.5);

export const config = {
  numbers: [{ name: "linkRank", path: "linkRank", type: "double", sortable: true }],
  sorts: [{ field: "linkRank", order: "desc" }],
  linkGraph: { field: "linkRank", boost: Number.isFinite(BOOST) ? BOOST : 0.5 }
};

export default async function enrich(docs) {
  const ordinalById = new Map();
  for (let i = 0; i < docs.length; i++) {
    const id = docs[i]?.[ID_FIELD];
    if (id != null && !ordinalById.has(String(id))) ordinalById.set(String(id), i);
  }

  const adjacency = new Array(docs.length);
  for (let i = 0; i < docs.length; i++) {
    const targets = docs[i]?.[LINK_FIELD];
    const seen = new Set();
    const row = [];
    if (Array.isArray(targets)) {
      for (const target of targets) {
        const ordinal = ordinalById.get(String(target));
        if (ordinal === undefined || ordinal === i || seen.has(ordinal)) continue;
        seen.add(ordinal);
        row.push(ordinal);
      }
    }
    adjacency[i] = row;
  }

  const ranks = computeLinkRank(adjacency);
  for (let i = 0; i < docs.length; i++) {
    docs[i].linkRank = Math.round(ranks[i] * 1e6) / 1e6;
  }
  return docs;
}
