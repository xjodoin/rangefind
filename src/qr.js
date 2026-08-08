// A QR encoder, in this repo, because a dispatch ticket has to cross the
// gap between two screens.
//
// A PulseMesh dispatch ticket (threads §20) is a *publish* capability
// handed to exactly one driver: ~168 bytes, ~224 base64url characters,
// and by design it travels over no server — the whole point of putting
// the capability in a URL fragment is that nothing between the two
// parties ever sees it. Which leaves the two devices in the same room
// and the camera one of them already has. So the dispatcher's screen has
// to be scannable, and that means a QR code.
//
// Rangefind ships zero runtime dependencies, and that is a property
// rather than an accident: the search client is code a site owner drops
// on a static host and stops thinking about. A QR encoder is exactly the
// kind of thing that can be written once and left alone — ISO/IEC 18004
// froze Model 2 a long time ago, its block-structure and alignment
// tables are normative constants, and the encoder is pure function with
// no I/O, no state and nothing to keep current. Taking a dependency for
// it would trade a permanent supply-chain edge for code that will never
// need to change.
//
// Byte mode only, and deliberately: everything this repo puts in front
// of a camera is base64url or a URL, and alphanumeric mode would buy a
// version at the cost of a second segment encoder to keep correct.

const EC_LEVELS = Object.freeze({
  // `index` selects the row of the normative tables below; `formatBits`
  // is the 2-bit level code that goes into the format information, and
  // the two orders are famously **not** the same (L is 0 in the tables
  // and 1 on the wire).
  L: { index: 0, formatBits: 1 },
  M: { index: 1, formatBits: 0 },
  Q: { index: 2, formatBits: 3 },
  H: { index: 3, formatBits: 2 }
});

/** ISO/IEC 18004 table 13-22: EC codewords per block, [level][version]. */
const EC_CODEWORDS_PER_BLOCK = [
  // v: 0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20  21  22  23  24  25  26  27  28  29  30  31  32  33  34  35  36  37  38  39  40
  [-1,  7, 10, 15, 20, 26, 18, 20, 24, 30, 18, 20, 24, 26, 30, 22, 24, 28, 30, 28, 28, 28, 28, 30, 30, 26, 28, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30],
  [-1, 10, 16, 26, 18, 24, 16, 18, 22, 22, 26, 30, 22, 22, 24, 24, 28, 28, 26, 26, 26, 26, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28],
  [-1, 13, 22, 18, 26, 18, 24, 18, 22, 20, 24, 28, 26, 24, 20, 30, 24, 28, 28, 26, 30, 28, 30, 30, 30, 30, 28, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30],
  [-1, 17, 28, 22, 16, 22, 28, 26, 26, 24, 28, 24, 28, 22, 24, 24, 30, 28, 28, 26, 28, 30, 24, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30]
];

/** ISO/IEC 18004 table 13-22: number of EC blocks, [level][version]. */
const EC_BLOCKS = [
  // v: 0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20  21  22  23  24  25  26  27  28  29  30  31  32  33  34  35  36  37  38  39  40
  [-1,  1,  1,  1,  1,  1,  2,  2,  2,  2,  4,  4,  4,  4,  4,  6,  6,  6,  6,  7,  8,  8,  9,  9, 10, 12, 12, 12, 13, 14, 15, 16, 17, 18, 19, 19, 20, 21, 22, 24, 25],
  [-1,  1,  1,  1,  2,  2,  4,  4,  4,  5,  5,  5,  8,  9,  9, 10, 10, 11, 13, 14, 16, 17, 17, 18, 20, 21, 23, 25, 26, 28, 29, 31, 33, 35, 37, 38, 40, 43, 45, 47, 49],
  [-1,  1,  1,  2,  2,  4,  4,  6,  6,  8,  8,  8, 10, 12, 16, 12, 17, 16, 18, 21, 20, 23, 23, 25, 27, 29, 34, 34, 35, 38, 40, 43, 45, 48, 51, 53, 56, 59, 62, 65, 68],
  [-1,  1,  1,  2,  4,  4,  4,  5,  5,  8,  8, 11, 11, 16, 16, 18, 16, 19, 21, 25, 25, 25, 34, 30, 32, 35, 37, 40, 42, 45, 48, 51, 57, 60, 63, 66, 70, 74, 77, 81, 85]
];

const PENALTY_N1 = 3;
const PENALTY_N2 = 3;
const PENALTY_N3 = 40;
const PENALTY_N4 = 10;

// The finder-lookalike of penalty rule 3: 1011101 with four light
// modules on one side. Checked in both orientations, in rows and columns.
const FINDER_RUN = [1, 0, 1, 1, 1, 0, 1];
const N3_AFTER = [...FINDER_RUN, 0, 0, 0, 0];
const N3_BEFORE = [0, 0, 0, 0, ...FINDER_RUN];

// --- GF(256), the field the Reed-Solomon parity lives in -------------------

// x^8 + x^4 + x^3 + x^2 + 1, the QR primitive polynomial, with 2 as the
// generator. Built once: 512 bytes of tables against a multiply loop on
// every codeword of every block is not a close call.
const GF_EXP = new Uint8Array(512);
const GF_LOG = new Uint8Array(256);
{
  let value = 1;
  for (let i = 0; i < 255; i++) {
    GF_EXP[i] = value;
    GF_LOG[value] = i;
    value <<= 1;
    if (value & 0x100) value ^= 0x11d;
  }
  for (let i = 255; i < 512; i++) GF_EXP[i] = GF_EXP[i - 255];
}

function gfMul(a, b) {
  if (a === 0 || b === 0) return 0;
  return GF_EXP[GF_LOG[a] + GF_LOG[b]];
}

/** The degree-`count` generator polynomial, coefficients high-order first. */
function generatorPoly(count) {
  const poly = new Uint8Array(count);
  poly[count - 1] = 1;
  let root = 1;
  for (let i = 0; i < count; i++) {
    for (let j = 0; j < count; j++) {
      poly[j] = gfMul(poly[j], root);
      if (j + 1 < count) poly[j] ^= poly[j + 1];
    }
    root = gfMul(root, 2);
  }
  return poly;
}

function reedSolomon(data, count) {
  const generator = generatorPoly(count);
  const remainder = new Uint8Array(count);
  for (const byte of data) {
    const factor = byte ^ remainder[0];
    remainder.copyWithin(0, 1);
    remainder[count - 1] = 0;
    for (let i = 0; i < count; i++) remainder[i] ^= gfMul(generator[i], factor);
  }
  return remainder;
}

// --- Version geometry ------------------------------------------------------

function sizeOf(version) {
  return version * 4 + 17;
}

/**
 * Alignment pattern centres, which are ISO/IEC 18004 annex E's table
 * expressed as the rule that produced it: `numAlign` evenly spaced rows
 * and columns, the first always at 6 and the last always at size-7, with
 * the spacing rounded up to an even number. Version 32 is the one row the
 * rule does not reach, and the standard names its step directly.
 */
function alignmentPositions(version) {
  if (version === 1) return [];
  const count = Math.floor(version / 7) + 2;
  const step = version === 32 ? 26 : Math.ceil((version * 4 + 4) / (count * 2 - 2)) * 2;
  const positions = [6];
  for (let pos = version * 4 + 10; positions.length < count; pos -= step) positions.splice(1, 0, pos);
  return positions;
}

/** Module positions available to data and EC, before function patterns. */
function rawDataModules(version) {
  let modules = (16 * version + 128) * version + 64;
  if (version >= 2) {
    const count = Math.floor(version / 7) + 2;
    modules -= (25 * count - 10) * count - 55;
    if (version >= 7) modules -= 36;
  }
  return modules;
}

function totalCodewords(version) {
  return Math.floor(rawDataModules(version) / 8);
}

/**
 * The block structure of a version at a level: how many blocks, how long
 * each is, and where the split between the short group and the long one
 * falls. The standard tables give this as two columns per row (group 1
 * and group 2); the two are recoverable from the block count alone, so
 * only the two normative arrays above are carried.
 */
function blockStructure(version, ecLevel) {
  const level = EC_LEVELS[ecLevel];
  if (!level) throw new Error(`Unknown QR error-correction level ${JSON.stringify(ecLevel)}.`);
  if (!Number.isInteger(version) || version < 1 || version > 40) {
    throw new Error(`A QR version is 1..40, not ${version}.`);
  }
  const ecPerBlock = EC_CODEWORDS_PER_BLOCK[level.index][version];
  const blocks = EC_BLOCKS[level.index][version];
  const total = totalCodewords(version);
  const dataCodewords = total - ecPerBlock * blocks;
  const shortLength = Math.floor(dataCodewords / blocks);
  const shortBlocks = blocks - (dataCodewords % blocks);
  return {
    version,
    ecLevel,
    blocks,
    ecPerBlock,
    totalCodewords: total,
    dataCodewords,
    // group 1
    shortBlocks,
    shortLength,
    // group 2 — one codeword longer, always the tail of the block list
    longBlocks: blocks - shortBlocks,
    longLength: shortLength + 1
  };
}

/** Byte mode's character-count field, which widens twice across versions. */
function charCountBits(version) {
  return version <= 9 ? 8 : 16;
}

// --- Bit stream ------------------------------------------------------------

function toBytes(data) {
  if (data instanceof Uint8Array) return data;
  if (typeof data === "string") return new TextEncoder().encode(data);
  throw new Error("encodeQr takes a string or a Uint8Array.");
}

function chooseVersion(byteLength, ecLevel, maxVersion) {
  for (let version = 1; version <= maxVersion; version++) {
    const structure = blockStructure(version, ecLevel);
    const needed = 4 + charCountBits(version) + byteLength * 8;
    if (needed <= structure.dataCodewords * 8) return structure;
  }
  throw new Error(
    `${byteLength} bytes do not fit in a QR code at level ${ecLevel} within version ${maxVersion} ` +
    `(the ceiling there is ${blockStructure(maxVersion, ecLevel).dataCodewords - 3} bytes).`
  );
}

function dataCodewordsFor(bytes, structure) {
  const capacityBits = structure.dataCodewords * 8;
  const bits = [];
  const push = (value, count) => {
    for (let i = count - 1; i >= 0; i--) bits.push((value >>> i) & 1);
  };
  push(0b0100, 4);
  push(bytes.length, charCountBits(structure.version));
  for (const byte of bytes) push(byte, 8);
  // Terminator, then the pad to a codeword boundary, then the alternating
  // pad bytes. All three are the standard's, in the standard's order.
  push(0, Math.min(4, capacityBits - bits.length));
  push(0, (8 - (bits.length % 8)) % 8);
  const codewords = new Uint8Array(structure.dataCodewords);
  for (let i = 0; i < bits.length; i++) codewords[i >>> 3] |= bits[i] << (7 - (i & 7));
  for (let i = bits.length >>> 3, pad = 0xec; i < structure.dataCodewords; i++, pad ^= 0xec ^ 0x11) {
    codewords[i] = pad;
  }
  return codewords;
}

/**
 * Split into blocks, compute parity, and interleave — the step that makes
 * a QR code survive a thumb over one corner: consecutive codewords of a
 * block end up scattered across the symbol, so a local smudge spreads its
 * damage thinly over every block rather than destroying one outright.
 */
function interleave(dataCodewords, structure) {
  const blocks = [];
  let offset = 0;
  for (let i = 0; i < structure.blocks; i++) {
    const length = i < structure.shortBlocks ? structure.shortLength : structure.longLength;
    const data = dataCodewords.subarray(offset, offset + length);
    offset += length;
    blocks.push({ data, ec: reedSolomon(data, structure.ecPerBlock) });
  }
  const out = new Uint8Array(structure.totalCodewords);
  let at = 0;
  for (let i = 0; i < structure.longLength; i++) {
    for (const block of blocks) if (i < block.data.length) out[at++] = block.data[i];
  }
  for (let i = 0; i < structure.ecPerBlock; i++) {
    for (const block of blocks) out[at++] = block.ec[i];
  }
  return out;
}

// --- The symbol ------------------------------------------------------------

function createSymbol(version) {
  const size = sizeOf(version);
  return {
    version,
    size,
    modules: new Uint8Array(size * size),
    // Function modules are never masked and never carry data, so which
    // ones they are has to be known separately from what they hold.
    reserved: new Uint8Array(size * size)
  };
}

function setFunction(symbol, x, y, dark) {
  if (x < 0 || y < 0 || x >= symbol.size || y >= symbol.size) return;
  symbol.modules[y * symbol.size + x] = dark ? 1 : 0;
  symbol.reserved[y * symbol.size + x] = 1;
}

function drawFinder(symbol, x, y) {
  // One 9x9 stamp covers the finder and its separator: distance from the
  // centre in the Chebyshev metric is exactly what the concentric rings
  // are, and modules outside the symbol fall off the edge harmlessly.
  for (let dy = -4; dy <= 4; dy++) {
    for (let dx = -4; dx <= 4; dx++) {
      const distance = Math.max(Math.abs(dx), Math.abs(dy));
      setFunction(symbol, x + dx, y + dy, distance !== 2 && distance !== 4);
    }
  }
}

function drawAlignment(symbol, x, y) {
  for (let dy = -2; dy <= 2; dy++) {
    for (let dx = -2; dx <= 2; dx++) {
      setFunction(symbol, x + dx, y + dy, Math.max(Math.abs(dx), Math.abs(dy)) !== 1);
    }
  }
}

function formatBitsFor(ecLevel, mask) {
  const data = (EC_LEVELS[ecLevel].formatBits << 3) | mask;
  let remainder = data;
  for (let i = 0; i < 10; i++) remainder = (remainder << 1) ^ ((remainder >>> 9) * 0x537);
  // The 0x5412 mask exists so that an all-zero format (level M, mask 0)
  // is still visibly a pattern rather than blank.
  return ((data << 10) | remainder) ^ 0x5412;
}

function drawFormat(symbol, ecLevel, mask) {
  const bits = formatBitsFor(ecLevel, mask);
  const bit = i => ((bits >>> i) & 1) !== 0;
  const size = symbol.size;
  for (let i = 0; i <= 5; i++) setFunction(symbol, 8, i, bit(i));
  setFunction(symbol, 8, 7, bit(6));
  setFunction(symbol, 8, 8, bit(7));
  setFunction(symbol, 7, 8, bit(8));
  for (let i = 9; i < 15; i++) setFunction(symbol, 14 - i, 8, bit(i));
  // The second copy: a symbol whose top-left corner is unreadable still
  // has to say which level and mask it used.
  for (let i = 0; i < 8; i++) setFunction(symbol, size - 1 - i, 8, bit(i));
  for (let i = 8; i < 15; i++) setFunction(symbol, 8, size - 15 + i, bit(i));
  setFunction(symbol, 8, size - 8, true); // the dark module, always
}

function versionBitsFor(version) {
  let remainder = version;
  for (let i = 0; i < 12; i++) remainder = (remainder << 1) ^ ((remainder >>> 11) * 0x1f25);
  return (version << 12) | remainder;
}

function drawVersion(symbol) {
  if (symbol.version < 7) return;
  const bits = versionBitsFor(symbol.version);
  for (let i = 0; i < 18; i++) {
    const dark = ((bits >>> i) & 1) !== 0;
    const far = symbol.size - 11 + (i % 3);
    const near = Math.floor(i / 3);
    setFunction(symbol, far, near, dark);
    setFunction(symbol, near, far, dark);
  }
}

function drawFunctionPatterns(symbol, ecLevel) {
  const size = symbol.size;
  for (let i = 0; i < size; i++) {
    setFunction(symbol, 6, i, i % 2 === 0);
    setFunction(symbol, i, 6, i % 2 === 0);
  }
  drawFinder(symbol, 3, 3);
  drawFinder(symbol, size - 4, 3);
  drawFinder(symbol, 3, size - 4);
  const positions = alignmentPositions(symbol.version);
  for (let i = 0; i < positions.length; i++) {
    for (let j = 0; j < positions.length; j++) {
      // The three corners already hold finders.
      const corner = (i === 0 && j === 0)
        || (i === 0 && j === positions.length - 1)
        || (i === positions.length - 1 && j === 0);
      if (!corner) drawAlignment(symbol, positions[i], positions[j]);
    }
  }
  drawVersion(symbol);
  // Mask 0 is a placeholder: the real format goes in once the penalty
  // scoring below has picked a mask.
  drawFormat(symbol, ecLevel, 0);
}

/** The zigzag: two-module columns, right to left, alternating direction. */
function drawCodewords(symbol, codewords) {
  const size = symbol.size;
  let bit = 0;
  for (let right = size - 1; right >= 1; right -= 2) {
    // Column 6 is the vertical timing pattern, so the pairing skips it
    // rather than straddling it.
    if (right === 6) right = 5;
    for (let vertical = 0; vertical < size; vertical++) {
      for (let j = 0; j < 2; j++) {
        const x = right - j;
        const upward = ((right + 1) & 2) === 0;
        const y = upward ? size - 1 - vertical : vertical;
        const at = y * size + x;
        if (symbol.reserved[at]) continue;
        if (bit < codewords.length * 8) {
          symbol.modules[at] = (codewords[bit >>> 3] >>> (7 - (bit & 7))) & 1;
          bit++;
        }
        // Remainder modules stay light, and the mask still applies to
        // them — they are data positions the codewords did not reach.
      }
    }
  }
}

function maskAt(mask, x, y) {
  switch (mask) {
    case 0: return (x + y) % 2 === 0;
    case 1: return y % 2 === 0;
    case 2: return x % 3 === 0;
    case 3: return (x + y) % 3 === 0;
    case 4: return (Math.floor(y / 2) + Math.floor(x / 3)) % 2 === 0;
    case 5: return ((x * y) % 2) + ((x * y) % 3) === 0;
    case 6: return (((x * y) % 2) + ((x * y) % 3)) % 2 === 0;
    case 7: return (((x + y) % 2) + ((x * y) % 3)) % 2 === 0;
    default: throw new Error(`A QR mask is 0..7, not ${mask}.`);
  }
}

function applyMask(symbol, mask) {
  const size = symbol.size;
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      const at = y * size + x;
      if (symbol.reserved[at]) continue;
      if (maskAt(mask, x, y)) symbol.modules[at] ^= 1;
    }
  }
}

function matchesRun(line, start, pattern) {
  for (let i = 0; i < pattern.length; i++) if (line[start + i] !== pattern[i]) return false;
  return true;
}

function linePenalty(line) {
  let penalty = 0;
  let runLength = 1;
  for (let i = 1; i <= line.length; i++) {
    if (i < line.length && line[i] === line[i - 1]) {
      runLength++;
      continue;
    }
    if (runLength >= 5) penalty += PENALTY_N1 + (runLength - 5);
    runLength = 1;
  }
  for (let i = 0; i + N3_AFTER.length <= line.length; i++) {
    if (matchesRun(line, i, N3_AFTER) || matchesRun(line, i, N3_BEFORE)) penalty += PENALTY_N3;
  }
  return penalty;
}

/** The four rules of §8.8.2, whose sum decides which mask is used. */
function penaltyScore(symbol) {
  const size = symbol.size;
  const modules = symbol.modules;
  let penalty = 0;
  const line = new Uint8Array(size);
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) line[x] = modules[y * size + x];
    penalty += linePenalty(line);
  }
  for (let x = 0; x < size; x++) {
    for (let y = 0; y < size; y++) line[y] = modules[y * size + x];
    penalty += linePenalty(line);
  }
  for (let y = 0; y + 1 < size; y++) {
    for (let x = 0; x + 1 < size; x++) {
      const value = modules[y * size + x];
      if (value === modules[y * size + x + 1]
        && value === modules[(y + 1) * size + x]
        && value === modules[(y + 1) * size + x + 1]) penalty += PENALTY_N2;
    }
  }
  let dark = 0;
  for (const value of modules) dark += value;
  const total = size * size;
  // The smallest k with (50 ± 5(k+1))% straddling the dark ratio.
  const k = Math.ceil(Math.abs(dark * 20 - total * 10) / total) - 1;
  return penalty + k * PENALTY_N4;
}

/**
 * Encodes `data` as a QR Model 2 symbol in byte mode.
 *
 * The smallest version that fits at the requested level wins: a courier
 * photographing a phone screen across a counter is helped far more by
 * large modules than by spare capacity.
 */
export function encodeQr(data, { ecLevel = "M", maxVersion = 40 } = {}) {
  const level = String(ecLevel).toUpperCase();
  if (!EC_LEVELS[level]) throw new Error(`Unknown QR error-correction level ${JSON.stringify(ecLevel)}.`);
  if (!Number.isInteger(maxVersion) || maxVersion < 1 || maxVersion > 40) {
    throw new Error("maxVersion is a QR version between 1 and 40.");
  }
  const bytes = toBytes(data);
  // `maxVersion` exists for a policy the caller owns, not a QR limit: a
  // symbol past ~v25 is technically valid and practically unscannable off
  // a phone screen at arm's length, and the dispatch UIs would rather
  // hand over a file than a QR nobody's camera can lock onto.
  const structure = chooseVersion(bytes.length, level, maxVersion);
  const codewords = interleave(dataCodewordsFor(bytes, structure), structure);
  const symbol = createSymbol(structure.version);
  drawFunctionPatterns(symbol, level);
  drawCodewords(symbol, codewords);

  let best = -1;
  let bestPenalty = Infinity;
  for (let mask = 0; mask < 8; mask++) {
    applyMask(symbol, mask);
    drawFormat(symbol, level, mask);
    const penalty = penaltyScore(symbol);
    if (penalty < bestPenalty) {
      bestPenalty = penalty;
      best = mask;
    }
    applyMask(symbol, mask); // XOR is its own inverse
  }
  applyMask(symbol, best);
  drawFormat(symbol, level, best);

  return {
    version: structure.version,
    size: symbol.size,
    ecLevel: level,
    mask: best,
    modules: symbol.modules
  };
}

/**
 * The symbol as a standalone `<svg>` string.
 *
 * The white background covers the quiet zone as well as the symbol,
 * because the quiet zone *is* part of the code: dropped onto a dark page
 * — which the demo is — a transparent margin is a code no scanner will
 * find. Every dark module becomes one subpath of a single merged path,
 * which keeps a version-40 symbol to one DOM node instead of 1,681.
 */
export function qrSvg(qr, { margin = 4 } = {}) {
  const extent = qr.size + margin * 2;
  const path = [];
  for (let y = 0; y < qr.size; y++) {
    for (let x = 0; x < qr.size; x++) {
      if (qr.modules[y * qr.size + x]) path.push(`M${x + margin} ${y + margin}h1v1h-1z`);
    }
  }
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${extent} ${extent}" `
    + `width="100%" height="100%" shape-rendering="crispEdges" role="img">`
    + `<rect width="${extent}" height="${extent}" fill="#ffffff"/>`
    + `<path fill="#000000" d="${path.join("")}"/>`
    + `</svg>`;
}

/**
 * The symbol as a block of text a terminal can display — and a phone
 * camera can read off that terminal.
 *
 * Two modules per character cell, using `▀`: a cell is about twice as
 * tall as it is wide, so one module per cell comes out stretched to
 * double height and scanners lock onto it badly or not at all. Painting
 * the upper module in the foreground colour and the lower one in the
 * background colour of the same `▀` gives square modules, and halves the
 * lines a symbol needs — the difference between a card that fits an ssh
 * session and one that scrolls.
 *
 * Colours are set explicitly (SGR 24-bit) rather than inherited, because
 * a QR code is not decoration: on a dark-themed terminal inherited
 * colours produce an **inverted** symbol, and while some readers cope
 * with inversion, plenty of phone cameras see nothing at all. `ansi:
 * false` drops the escapes for logs and pipes, and is then only correct
 * on a light background.
 *
 * The quiet zone is not optional either: ISO/IEC 18004 requires four
 * light modules of margin, and a symbol printed hard against a terminal
 * edge is one most scanners never find.
 */
export function qrTerminal(qr, { margin = 4, ansi = true } = {}) {
  const extent = qr.size + (margin * 2);
  // Inside the symbol a module may be dark; everything outside it is the
  // quiet zone, which is light.
  const dark = (x, y) => {
    const sx = x - margin;
    const sy = y - margin;
    if (sx < 0 || sy < 0 || sx >= qr.size || sy >= qr.size) return false;
    return qr.modules[(sy * qr.size) + sx] === 1;
  };
  const OPEN = "\u001b[48;2;255;255;255m\u001b[38;2;0;0;0m";
  const CLOSE = "\u001b[0m";
  const lines = [];
  for (let y = 0; y < extent; y += 2) {
    let line = "";
    for (let x = 0; x < extent; x++) {
      const top = dark(x, y);
      // An odd extent leaves the last row half off the symbol; the
      // missing half is quiet zone, which is light.
      const bottom = y + 1 < extent ? dark(x, y + 1) : false;
      line += top ? (bottom ? "█" : "▀") : (bottom ? "▄" : " ");
    }
    lines.push(ansi ? OPEN + line + CLOSE : line);
  }
  return lines.join("\n");
}
