// Binary-artifact inspector backing `rangefind inspect`.
//
// Every Rangefind binary artifact opens with a 4-byte RF·· magic. This
// module sniffs that magic and returns a structured report: a full decode
// for the doc-values manifest (rfdvm-v1), a header decode for geo tree
// roots, and identification (name + size) for everything else — enough to
// answer "what is this file and what does it hold" without a hex editor.

import {
  AUTHORITY_HOT_MAGIC,
  AUTHORITY_LEXICON_MAGIC,
  AUTHORITY_LEXICON_SEGMENT_MAGIC,
  AUTHORITY_SHARD_MAGIC,
  CODE_MAGIC,
  DIRECTORY_PAGE_MAGIC,
  DIRECTORY_ROOT_MAGIC,
  DOC_PAGE_PAYLOAD_MAGIC,
  DOC_PAGE_POINTER_MAGIC,
  DOC_POINTER_PAGE_MAGIC,
  DOC_VALUE_MAGIC,
  DOC_VALUE_SORT_DIRECTORY_MAGIC,
  DOC_VALUE_SORT_PAGE_MAGIC,
  FACET_DICT_MAGIC,
  GEO_BRANCH_PAGE_MAGIC,
  GEO_LEAF_PAGE_MAGIC,
  GEO_TREE_ROOT_MAGIC,
  POSTING_SEGMENT_MAGIC,
  QUERY_BUNDLE_MAGIC,
  VECTOR_CLUSTER_PAGE_MAGIC,
  VECTOR_ROOT_MAGIC,
  readVarint
} from "./binary.js";
import { readUtf8 } from "./codec.js";
import { DOC_VALUE_MANIFEST_MAGIC, parseDocValueManifest } from "./doc_value_manifest.js";

const KNOWN_MAGICS = [
  { magic: DOC_VALUE_MANIFEST_MAGIC, name: "doc-value manifest (rfdvm-v1)", decode: decodeDocValueManifest },
  { magic: GEO_TREE_ROOT_MAGIC, name: "geo tree root", decode: decodeGeoTreeRootHeader },
  { magic: POSTING_SEGMENT_MAGIC, name: "posting segment" },
  { magic: DIRECTORY_ROOT_MAGIC, name: "directory root" },
  { magic: DIRECTORY_PAGE_MAGIC, name: "directory page" },
  { magic: CODE_MAGIC, name: "code block" },
  { magic: DOC_VALUE_MAGIC, name: "doc-value chunk" },
  { magic: FACET_DICT_MAGIC, name: "facet dictionary" },
  { magic: DOC_POINTER_PAGE_MAGIC, name: "doc pointer page" },
  { magic: DOC_PAGE_POINTER_MAGIC, name: "doc page pointers" },
  { magic: DOC_PAGE_PAYLOAD_MAGIC, name: "doc page payload" },
  { magic: DOC_VALUE_SORT_DIRECTORY_MAGIC, name: "doc-value sort directory" },
  { magic: DOC_VALUE_SORT_PAGE_MAGIC, name: "doc-value sort page" },
  { magic: QUERY_BUNDLE_MAGIC, name: "query bundle" },
  { magic: AUTHORITY_SHARD_MAGIC, name: "authority shard" },
  { magic: AUTHORITY_LEXICON_MAGIC, name: "authority lexicon" },
  { magic: AUTHORITY_LEXICON_SEGMENT_MAGIC, name: "authority lexicon segment" },
  { magic: AUTHORITY_HOT_MAGIC, name: "authority hot set" },
  { magic: GEO_BRANCH_PAGE_MAGIC, name: "geo branch page" },
  { magic: GEO_LEAF_PAGE_MAGIC, name: "geo leaf page" },
  { magic: VECTOR_ROOT_MAGIC, name: "vector root" },
  { magic: VECTOR_CLUSTER_PAGE_MAGIC, name: "vector cluster page" }
];

export function isGzipBytes(bytes) {
  return bytes?.length > 2 && bytes[0] === 0x1f && bytes[1] === 0x8b;
}

function magicMatches(bytes, magic) {
  if (bytes.length < magic.length) return false;
  for (let i = 0; i < magic.length; i++) {
    if (bytes[i] !== magic[i]) return false;
  }
  return true;
}

function readZigzag(bytes, state) {
  const encoded = readVarint(bytes, state);
  return encoded % 2 === 1 ? -(encoded + 1) / 2 : encoded / 2;
}

function decodeDocValueManifest(bytes, { full = false } = {}) {
  const manifest = parseDocValueManifest(bytes);
  const fields = Object.values(manifest.fields).map(field => ({
    name: field.name,
    kind: field.kind,
    type: field.type,
    words: field.words,
    chunks: field.chunks.length,
    ...(field.lookup_chunks ? { lookupChunks: field.lookup_chunks.length } : {}),
    ...(field.chunks.some(chunk => chunk.min != null) ? {
      min: Math.min(...field.chunks.filter(c => c.min != null).map(c => c.min)),
      max: Math.max(...field.chunks.filter(c => c.max != null).map(c => c.max))
    } : {})
  }));
  return {
    format: manifest.format || "rfdvm-v1",
    storage: manifest.storage,
    compression: manifest.compression,
    total: manifest.total,
    chunkSize: manifest.chunk_size,
    lookupChunkSize: manifest.lookup_chunk_size,
    packs: manifest.packs.length,
    ...(manifest.checksum_rows ? { checksums: manifest.checksum_rows } : {}),
    fields,
    ...(full ? { manifest } : {})
  };
}

// Header-only decode: the full root parse needs the shard manifest's
// block_filters when per-cell filter summaries are present, but everything
// before that point identifies the tree.
function decodeGeoTreeRootHeader(bytes) {
  const state = { pos: GEO_TREE_ROOT_MAGIC.length };
  const version = readVarint(bytes, state);
  const field = readUtf8(bytes, state);
  const total = readVarint(bytes, state);
  const leafSize = readVarint(bytes, state);
  const leafCount = readVarint(bytes, state);
  const minLatE7 = readZigzag(bytes, state);
  const maxLatE7 = minLatE7 + readVarint(bytes, state);
  const minLonE7 = readZigzag(bytes, state);
  const maxLonE7 = minLonE7 + readVarint(bytes, state);
  const packCount = readVarint(bytes, state);
  for (let i = 0; i < packCount; i++) readUtf8(bytes, state);
  const levels = bytes[state.pos++];
  const hasSummaries = bytes[state.pos++] === 1;
  return {
    version,
    field,
    total,
    leafSize,
    leafCount,
    levels,
    hasFilterSummaries: hasSummaries,
    packs: packCount,
    bbox: {
      minLat: minLatE7 / 1e7,
      minLon: minLonE7 / 1e7,
      maxLat: maxLatE7 / 1e7,
      maxLon: maxLonE7 / 1e7
    }
  };
}

export function inspectArtifact(bytes, { full = false } = {}) {
  const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  for (const { magic, name, decode } of KNOWN_MAGICS) {
    if (!magicMatches(view, magic)) continue;
    const report = {
      artifact: name,
      magic: String.fromCharCode(...magic),
      bytes: view.length
    };
    if (!decode) return report;
    try {
      return { ...report, ...decode(view, { full }) };
    } catch (error) {
      return { ...report, decodeError: error?.message || String(error) };
    }
  }
  return {
    artifact: "unknown",
    magic: String.fromCharCode(...view.slice(0, 4)).replace(/[^\x20-\x7e]/gu, "?"),
    bytes: view.length
  };
}
