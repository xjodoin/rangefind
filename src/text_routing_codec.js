import { pushVarint, readVarint } from "./binary.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";

// Platform-neutral codec for the sharded-root text routing directory
// ("rftextroute-v1"): segments of front-coded sorted terms, each carrying the
// delta-encoded ordinals of the shards whose postings contain the term. The
// writer lives in text_routing.js (Node-only); the runtime imports only this
// module so browser bundles stay fs-free.

export const TEXT_ROUTING_FORMAT = "rftextroute-v1";
export const TEXT_ROUTING_VERSION = 1;
const TEXT_ROUTING_SEGMENT_MAGIC = [0x52, 0x46, 0x54, 0x52]; // RFTR

export function encodeTextRoutingSegment(items) {
  const out = [...TEXT_ROUTING_SEGMENT_MAGIC];
  pushVarint(out, TEXT_ROUTING_VERSION);
  pushVarint(out, items.length);
  let previous = "";
  for (const item of items) {
    const term = item.term;
    let prefix = 0;
    const max = Math.min(previous.length, term.length);
    while (prefix < max && previous[prefix] === term[prefix]) prefix++;
    pushVarint(out, prefix);
    pushUtf8(out, term.slice(prefix));
    pushVarint(out, item.shards.length);
    let last = -1;
    for (const ordinal of item.shards) {
      pushVarint(out, ordinal - last - 1);
      last = ordinal;
    }
    previous = term;
  }
  return Uint8Array.from(out);
}

export function parseTextRoutingSegment(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, TEXT_ROUTING_SEGMENT_MAGIC, "Unsupported Rangefind text routing segment");
  const state = { pos: TEXT_ROUTING_SEGMENT_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== TEXT_ROUTING_VERSION) throw new Error(`Unsupported Rangefind text routing segment version ${version}`);
  const count = readVarint(bytes, state);
  const terms = new Map();
  let previous = "";
  for (let index = 0; index < count; index++) {
    const prefix = readVarint(bytes, state);
    const term = previous.slice(0, prefix) + readUtf8(bytes, state);
    const shardCount = readVarint(bytes, state);
    const shards = new Array(shardCount);
    let last = -1;
    for (let i = 0; i < shardCount; i++) {
      last += readVarint(bytes, state) + 1;
      shards[i] = last;
    }
    terms.set(term, shards);
    previous = term;
  }
  return terms;
}

// Floor lookup over an rfdir-v2 root whose keys are segment first-terms: the
// segment owning `term` is the one with the greatest key <= term. The bloom
// (over segment keys) does not apply to member terms and is ignored.
export function floorDirectoryPageIndex(root, term) {
  const pages = root?.pages || [];
  if (!pages.length || term < pages[0].first) return -1;
  let lo = 0;
  let hi = pages.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (pages[mid].first <= term) lo = mid;
    else hi = mid - 1;
  }
  return lo;
}

// Greatest key <= term over a sorted key array (directory pages preserve
// build order, so [...entries.keys()] is sorted).
export function floorSortedKeyIndex(keys, term) {
  if (!keys.length || term < keys[0]) return -1;
  let lo = 0;
  let hi = keys.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (keys[mid] <= term) lo = mid;
    else hi = mid - 1;
  }
  return lo;
}
