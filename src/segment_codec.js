const SEGMENT_TERMS_MAGIC = Uint8Array.from([0x52, 0x46, 0x53, 0x47, 0x54, 0x45, 0x52, 0x31]);
const textDecoder = new TextDecoder();

function assertMagic(bytes, magic, label) {
  for (let i = 0; i < magic.length; i++) {
    if (bytes[i] !== magic[i]) throw new Error(label);
  }
}

function readVarint(bytes, state) {
  let value = 0;
  let multiplier = 1;
  while (state.pos < bytes.length) {
    const byte = bytes[state.pos++];
    value += (byte & 0x7f) * multiplier;
    if ((byte & 0x80) === 0) return value;
    multiplier *= 0x80;
  }
  throw new Error("Truncated Rangefind segment varint.");
}

function readUtf8(bytes, state) {
  const length = readVarint(bytes, state);
  if (state.pos + length > bytes.length) throw new Error("Truncated Rangefind segment string.");
  const value = textDecoder.decode(bytes.subarray(state.pos, state.pos + length));
  state.pos += length;
  return value;
}

function segmentImpact(score, df, total) {
  const idf = Math.log(1 + (total - df + 0.5) / (df + 0.5));
  return Math.max(1, Math.round(score * idf / 10));
}

export function parseSegmentTerms(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, SEGMENT_TERMS_MAGIC, "Unsupported Rangefind segment terms file.");
  const state = { pos: SEGMENT_TERMS_MAGIC.length };
  const count = readVarint(bytes, state);
  const terms = new Map();
  for (let i = 0; i < count; i++) {
    const entry = {
      term: readUtf8(bytes, state),
      offset: readVarint(bytes, state),
      bytes: readVarint(bytes, state),
      df: readVarint(bytes, state),
      count: readVarint(bytes, state)
    };
    terms.set(entry.term, entry);
  }
  return { format: "rfsegmentterms-v1", terms };
}

export function decodeSegmentRows(buffer, entry, options = {}) {
  const bytes = new Uint8Array(buffer);
  const total = Math.max(1, Number(options.total || 1));
  const df = Math.max(1, Number(options.df || entry.df || entry.count || 1));
  const state = { pos: 0 };
  const rows = new Int32Array((entry.count || 0) * 2);
  for (let row = 0; row < rows.length; row += 2) {
    rows[row] = readVarint(bytes, state);
    rows[row + 1] = segmentImpact(readVarint(bytes, state), df, total);
  }
  if (state.pos !== bytes.length) throw new Error(`Rangefind segment term ${entry.term || ""} has trailing bytes.`);
  return rows;
}

