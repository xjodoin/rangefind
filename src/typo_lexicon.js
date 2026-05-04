import { TYPO_LEXICON_MAGIC, pushVarint, readVarint } from "./binary.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";

export const TYPO_LEXICON_FORMAT = "rftermlex-v2";
const TYPO_LEXICON_VERSION = 2;

export function typoLexiconShardKey(surface, depth) {
  const size = Math.max(1, Math.floor(Number(depth || 1)));
  return String(surface || "").slice(0, size).padEnd(size, "_");
}

export function buildTypoLexiconShard(entries) {
  const sorted = [...entries]
    .filter(entry => entry.surface && entry.term)
    .sort((a, b) => a.surface.localeCompare(b.surface) || a.term.localeCompare(b.term));
  const trie = buildLexiconTrie(sorted);
  const header = [...TYPO_LEXICON_MAGIC];
  pushVarint(header, TYPO_LEXICON_VERSION);
  pushVarint(header, sorted.length);
  for (const entry of sorted) {
    pushUtf8(header, entry.surface);
    pushUtf8(header, entry.term);
    pushVarint(header, entry.df || 0);
  }
  pushVarint(header, trie.nodes.length);
  for (const node of trie.nodes) {
    pushVarint(header, node.entryIds.length);
    for (const entryId of node.entryIds) pushVarint(header, entryId);
    const children = [...node.children.entries()].sort((a, b) => a[0].localeCompare(b[0]));
    pushVarint(header, children.length);
    for (const [char, childIndex] of children) {
      pushUtf8(header, char);
      pushVarint(header, childIndex);
    }
  }
  return {
    format: TYPO_LEXICON_FORMAT,
    buffer: Buffer.from(Uint8Array.from(header)),
    stats: { entries: sorted.length, trie_nodes: trie.nodes.length, trie_arcs: trie.arcs }
  };
}

function buildLexiconTrie(entries) {
  const nodes = [{ children: new Map(), entryIds: [] }];
  let arcs = 0;
  for (let entryId = 0; entryId < entries.length; entryId++) {
    let nodeIndex = 0;
    for (const char of entries[entryId].surface) {
      const node = nodes[nodeIndex];
      let childIndex = node.children.get(char);
      if (childIndex == null) {
        childIndex = nodes.length;
        node.children.set(char, childIndex);
        nodes.push({ children: new Map(), entryIds: [] });
        arcs++;
      }
      nodeIndex = childIndex;
    }
    nodes[nodeIndex].entryIds.push(entryId);
  }
  return { nodes, arcs };
}

export function parseTypoLexiconShard(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, TYPO_LEXICON_MAGIC, "Unsupported Rangefind typo lexicon shard");
  const state = { pos: TYPO_LEXICON_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== TYPO_LEXICON_VERSION) throw new Error("Unsupported Rangefind typo lexicon version");
  const count = readVarint(bytes, state);
  const entries = new Array(count);
  for (let i = 0; i < count; i++) {
    entries[i] = {
      surface: readUtf8(bytes, state),
      term: readUtf8(bytes, state),
      df: readVarint(bytes, state)
    };
  }
  const nodeCount = readVarint(bytes, state);
  const rawNodes = new Array(nodeCount);
  for (let i = 0; i < nodeCount; i++) {
    const entryIds = new Array(readVarint(bytes, state));
    for (let j = 0; j < entryIds.length; j++) entryIds[j] = readVarint(bytes, state);
    const childCount = readVarint(bytes, state);
    const children = new Array(childCount);
    for (let j = 0; j < childCount; j++) children[j] = [readUtf8(bytes, state), readVarint(bytes, state)];
    rawNodes[i] = { entryIds, children };
  }
  const nodes = rawNodes.map(node => ({
    children: new Map(),
    entries: node.entryIds.map(index => entries[index]).filter(Boolean)
  }));
  for (let i = 0; i < rawNodes.length; i++) {
    for (const [char, childIndex] of rawNodes[i].children) {
      if (nodes[childIndex]) nodes[i].children.set(char, nodes[childIndex]);
    }
  }
  return { format: TYPO_LEXICON_FORMAT, entries, trie: nodes[0] || { children: new Map(), entries: [] }, trieNodes: nodes.length };
}
