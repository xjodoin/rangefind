// The stable epoch for a dataset — `quebec/car`.
//
// The epoch is the topic namespace (§4.2), so it decides whether two
// peers are on the same mesh at all. Naming it by the graph's own
// `sourceHash` answers "is this the same map" exactly, and answers "can
// we find each other" badly: a sourceHash is a hash over node
// coordinates, edge topology and weights, so one new driveway changes it.
// A region tracking a public index rebuilt from Geofabrik would rename
// every running job's channel on every rebuild and strand every ticket
// already issued.
//
// A dataset names the region and profile, which do not change when the
// roads in them do. Hashing it produces a 64-char hex string
// indistinguishable in shape from a sourceHash, which is what lets it be
// handed straight to `epochHex` without touching the protocol.
//
// This lives in its own module because more than one script needs it —
// the keeper and the ingest node must derive identical values or an
// operator's own two processes end up on different meshes.
//
// **The namespace below must stay byte-identical to the product's.** Its
// other definitions are DATASET_NAMESPACE in wayfind's
// packages/core/src/graphs.ts and in packages/engine-host/bridge.js.
// Change it in one place and the parties hash the same dataset to
// different epochs, connect happily, and share nothing.
//
// The prefix is load-bearing for a second reason: without it a dataset id
// and a content hash live in the same space, and a graph could be
// addressed by either. They mean different things and must never be
// confusable.
export const DATASET_NAMESPACE = "wayfind-dataset-v1:";

/**
 * The 64-hex epoch for a dataset id. Every party derives it from the same
 * string, so a dispatcher, a driver, a recipient, a keeper and an ingest
 * node agree without anyone publishing anything — and keep agreeing
 * across a rebuild.
 */
export async function datasetEpoch(datasetId) {
  const name = String(datasetId ?? "").trim().toLowerCase();
  if (!name) throw new Error("A dataset id cannot be empty.");
  const bytes = new TextEncoder().encode(DATASET_NAMESPACE + name);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("");
}
