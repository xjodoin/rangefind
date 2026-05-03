import { TYPO_LEXICON_MAGIC, pushVarint, readVarint } from "./binary.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";

export const TYPO_LEXICON_FORMAT = "rftermlex-v1";
const TYPO_LEXICON_VERSION = 1;

export function typoLexiconShardKey(surface, depth) {
  const size = Math.max(1, Math.floor(Number(depth || 1)));
  return String(surface || "").slice(0, size).padEnd(size, "_");
}

export function buildTypoLexiconShard(entries) {
  const sorted = [...entries]
    .filter(entry => entry.surface && entry.term)
    .sort((a, b) => a.surface.localeCompare(b.surface) || a.term.localeCompare(b.term));
  const header = [...TYPO_LEXICON_MAGIC];
  pushVarint(header, TYPO_LEXICON_VERSION);
  pushVarint(header, sorted.length);
  for (const entry of sorted) {
    pushUtf8(header, entry.surface);
    pushUtf8(header, entry.term);
    pushVarint(header, entry.df || 0);
  }
  return {
    format: TYPO_LEXICON_FORMAT,
    buffer: Buffer.from(Uint8Array.from(header)),
    stats: { entries: sorted.length }
  };
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
  return { format: TYPO_LEXICON_FORMAT, entries };
}
