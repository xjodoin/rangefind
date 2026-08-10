// The decoder is the test.
//
// There is no camera in CI, and "the picture looks like a QR code" is not
// a claim a test can make. So this file implements the other half of
// ISO/IEC 18004 — format information, the function-module map, the
// zigzag, de-interleaving, Reed-Solomon syndromes, byte-mode parsing —
// from the spec rather than from src/qr.js, and round-trips through it.
//
// The load-bearing assertion is the syndrome check. A payload can survive
// a wrong mask, a mis-ordered interleave or a misplaced alignment pattern
// if the damage happens to land where the reader is not looking; the
// parity cannot. Every block's syndromes being zero means every codeword
// came back from the modules exactly as the encoder computed it, which is
// the only statement that generalises to a real scanner.

import assert from "node:assert/strict";
import test from "node:test";
import { encodeQr, qrSvg } from "../src/qr.js";
import { sha256Utf8 } from "../src/pulsemesh/sha256.js";
import { decodeThreadTicket, issueTicket } from "../src/pulsemesh/thread_ticket.js";
import { THREAD_MODE } from "../src/pulsemesh/thread_codec.js";

// --- The normative tables, transcribed for the reader ----------------------

/** Table 13-22, EC codewords per block, indexed [level][version]. */
const EC_PER_BLOCK = {
  L: [-1, 7, 10, 15, 20, 26, 18, 20, 24, 30, 18, 20, 24, 26, 30, 22, 24, 28, 30, 28, 28, 28, 28, 30, 30, 26, 28, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30],
  M: [-1, 10, 16, 26, 18, 24, 16, 18, 22, 22, 26, 30, 22, 22, 24, 24, 28, 28, 26, 26, 26, 26, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28],
  Q: [-1, 13, 22, 18, 26, 18, 24, 18, 22, 20, 24, 28, 26, 24, 20, 30, 24, 28, 28, 26, 30, 28, 30, 30, 30, 30, 28, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30],
  H: [-1, 17, 28, 22, 16, 22, 28, 26, 26, 24, 28, 24, 28, 22, 24, 24, 30, 28, 28, 26, 28, 30, 24, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30]
};

/** Table 13-22, number of EC blocks, indexed [level][version]. */
const EC_BLOCK_COUNT = {
  L: [-1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 4, 4, 4, 4, 4, 6, 6, 6, 6, 7, 8, 8, 9, 9, 10, 12, 12, 12, 13, 14, 15, 16, 17, 18, 19, 19, 20, 21, 22, 24, 25],
  M: [-1, 1, 1, 1, 2, 2, 4, 4, 4, 5, 5, 5, 8, 9, 9, 10, 10, 11, 13, 14, 16, 17, 17, 18, 20, 21, 23, 25, 26, 28, 29, 31, 33, 35, 37, 38, 40, 43, 45, 47, 49],
  Q: [-1, 1, 1, 2, 2, 4, 4, 6, 6, 8, 8, 8, 10, 12, 16, 12, 17, 16, 18, 21, 20, 23, 23, 25, 27, 29, 34, 34, 35, 38, 40, 43, 45, 48, 51, 53, 56, 59, 62, 65, 68],
  H: [-1, 1, 1, 2, 4, 4, 4, 5, 5, 8, 8, 11, 11, 16, 16, 18, 16, 19, 21, 25, 25, 25, 34, 30, 32, 35, 37, 40, 42, 45, 48, 51, 57, 60, 63, 66, 70, 74, 77, 81, 85]
};

/**
 * Annex E, alignment pattern centres, written out rather than derived.
 * src/qr.js generates these from the placement rule; a literal table is
 * the only way for this file to disagree with it.
 */
const ALIGNMENT = [
  null, [], [6, 18], [6, 22], [6, 26], [6, 30], [6, 34],
  [6, 22, 38], [6, 24, 42], [6, 26, 46], [6, 28, 50], [6, 30, 54], [6, 32, 58], [6, 34, 62],
  [6, 26, 46, 66], [6, 26, 48, 70], [6, 26, 50, 74], [6, 30, 54, 78], [6, 30, 56, 82], [6, 30, 58, 86], [6, 34, 62, 90],
  [6, 28, 50, 72, 94], [6, 26, 50, 74, 98], [6, 30, 54, 78, 102], [6, 28, 54, 80, 106], [6, 32, 58, 84, 110], [6, 30, 58, 86, 114], [6, 34, 62, 90, 118],
  [6, 26, 50, 74, 98, 122], [6, 30, 54, 78, 102, 126], [6, 26, 52, 78, 104, 130], [6, 30, 56, 82, 108, 134], [6, 34, 60, 86, 112, 138], [6, 30, 58, 86, 114, 142], [6, 34, 62, 90, 118, 146],
  [6, 30, 54, 78, 102, 126, 150], [6, 24, 50, 76, 102, 128, 154], [6, 28, 54, 80, 106, 132, 158], [6, 32, 58, 84, 110, 136, 162], [6, 26, 54, 82, 110, 138, 166], [6, 30, 58, 86, 114, 142, 170]
];

const LEVEL_OF_FORMAT_BITS = { 1: "L", 0: "M", 3: "Q", 2: "H" };

// --- GF(256), by long multiplication rather than by log tables -------------

function gfMul(a, b) {
  let product = 0;
  let left = a;
  let right = b;
  while (right) {
    if (right & 1) product ^= left;
    right >>= 1;
    left <<= 1;
    if (left & 0x100) left ^= 0x11d;
  }
  return product & 0xff;
}

function gfPow2(exponent) {
  let value = 1;
  for (let i = 0; i < exponent; i++) value = gfMul(value, 2);
  return value;
}

/** The codeword polynomial at α^exponent, coefficients high-order first. */
function syndrome(codeword, exponent) {
  const x = gfPow2(exponent);
  let accumulator = 0;
  for (const coefficient of codeword) accumulator = gfMul(accumulator, x) ^ coefficient;
  return accumulator;
}

// --- Reading the symbol ----------------------------------------------------

function readFormat(qr) {
  const dark = (x, y) => qr.modules[y * qr.size + x] === 1;
  let raw = 0;
  for (let i = 0; i <= 5; i++) if (dark(8, i)) raw |= 1 << i;
  if (dark(8, 7)) raw |= 1 << 6;
  if (dark(8, 8)) raw |= 1 << 7;
  if (dark(7, 8)) raw |= 1 << 8;
  for (let i = 9; i < 15; i++) if (dark(14 - i, 8)) raw |= 1 << i;
  const bits = raw ^ 0x5412;
  // A valid format is a multiple of the BCH(15,5) generator, so dividing
  // by it must leave nothing behind.
  let remainder = bits;
  for (let i = 14; i >= 10; i--) if ((remainder >>> i) & 1) remainder ^= 0x537 << (i - 10);
  return {
    bchRemainder: remainder,
    ecLevel: LEVEL_OF_FORMAT_BITS[(bits >>> 13) & 3],
    mask: (bits >>> 10) & 7
  };
}

function readVersionInfo(qr) {
  if (qr.version < 7) return null;
  const size = qr.size;
  let bits = 0;
  for (let i = 0; i < 18; i++) {
    if (qr.modules[Math.floor(i / 3) * size + (size - 11 + (i % 3))]) bits |= 1 << i;
  }
  // Golay(18,6): the codeword divides by x^12 + x^11 + x^10 + x^9 + x^8 + x^5 + x^2 + 1.
  let remainder = bits;
  for (let i = 17; i >= 12; i--) if ((remainder >>> i) & 1) remainder ^= 0x1f25 << (i - 12);
  return { version: bits >>> 12, bchRemainder: remainder, bits };
}

/** Every module a reader must skip: patterns, format, version, timing. */
function functionMap(version) {
  const size = version * 4 + 17;
  const map = new Uint8Array(size * size);
  const block = (x, y, width, height) => {
    for (let dy = 0; dy < height; dy++) {
      for (let dx = 0; dx < width; dx++) {
        const px = x + dx;
        const py = y + dy;
        if (px >= 0 && py >= 0 && px < size && py < size) map[py * size + px] = 1;
      }
    }
  };
  // Finder, separator and the format ring around each of the three corners.
  block(0, 0, 9, 9);
  block(size - 8, 0, 8, 9);
  block(0, size - 8, 9, 8);
  for (let i = 0; i < size; i++) {
    map[6 * size + i] = 1;
    map[i * size + 6] = 1;
  }
  const centres = ALIGNMENT[version];
  const last = centres.length - 1;
  for (let i = 0; i <= last; i++) {
    for (let j = 0; j <= last; j++) {
      const corner = (i === 0 && j === 0) || (i === 0 && j === last) || (i === last && j === 0);
      if (!corner) block(centres[i] - 2, centres[j] - 2, 5, 5);
    }
  }
  if (version >= 7) {
    block(size - 11, 0, 3, 6);
    block(0, size - 11, 6, 3);
  }
  return map;
}

function maskAt(mask, x, y) {
  if (mask === 0) return (x + y) % 2 === 0;
  if (mask === 1) return y % 2 === 0;
  if (mask === 2) return x % 3 === 0;
  if (mask === 3) return (x + y) % 3 === 0;
  if (mask === 4) return (Math.floor(y / 2) + Math.floor(x / 3)) % 2 === 0;
  if (mask === 5) return ((x * y) % 2) + ((x * y) % 3) === 0;
  if (mask === 6) return (((x * y) % 2) + ((x * y) % 3)) % 2 === 0;
  return (((x + y) % 2) + ((x * y) % 3)) % 2 === 0;
}

/**
 * The data bit stream, unmasked, in placement order: upward then downward
 * through two-module columns walking right to left, column 6 excluded
 * because the timing pattern owns it.
 */
function readBitStream(qr, mask) {
  const size = qr.size;
  const skip = functionMap(qr.version);
  const bits = [];
  let column = size - 1;
  let upward = true;
  while (column >= 1) {
    if (column === 6) column = 5;
    for (let step = 0; step < size; step++) {
      const y = upward ? size - 1 - step : step;
      for (const x of [column, column - 1]) {
        const at = y * size + x;
        if (skip[at]) continue;
        const value = qr.modules[at] ^ (maskAt(mask, x, y) ? 1 : 0);
        bits.push(value);
      }
    }
    upward = !upward;
    column -= 2;
  }
  return bits;
}

function structureOf(version, ecLevel) {
  const ecPerBlock = EC_PER_BLOCK[ecLevel][version];
  const blocks = EC_BLOCK_COUNT[ecLevel][version];
  // Total codewords: raw module count over 8, the remainder bits dropped.
  let modules = (16 * version + 128) * version + 64;
  if (version >= 2) {
    const count = Math.floor(version / 7) + 2;
    modules -= (25 * count - 10) * count - 55;
    if (version >= 7) modules -= 36;
  }
  const total = Math.floor(modules / 8);
  const dataCodewords = total - ecPerBlock * blocks;
  const shortLength = Math.floor(dataCodewords / blocks);
  const shortBlocks = blocks - (dataCodewords % blocks);
  const lengths = [];
  for (let i = 0; i < blocks; i++) lengths.push(i < shortBlocks ? shortLength : shortLength + 1);
  return { total, ecPerBlock, blocks, dataCodewords, lengths };
}

/**
 * Reads the symbol back to its payload, asserting everything a scanner
 * would rely on along the way.
 */
function decodeQr(qr) {
  const format = readFormat(qr);
  assert.equal(format.bchRemainder, 0, "format information fails its BCH check");
  assert.equal(format.ecLevel, qr.ecLevel, "the symbol advertises a different EC level");

  const versionInfo = readVersionInfo(qr);
  if (qr.version >= 7) {
    assert.equal(versionInfo.bchRemainder, 0, "version information fails its Golay check");
    assert.equal(versionInfo.version, qr.version);
  }

  const structure = structureOf(qr.version, format.ecLevel);
  const bits = readBitStream(qr, format.mask);
  assert.ok(bits.length >= structure.total * 8, "the data region is too small for this version");
  const codewords = new Uint8Array(structure.total);
  for (let i = 0; i < structure.total * 8; i++) {
    if (bits[i]) codewords[i >>> 3] |= 1 << (7 - (i & 7));
  }

  // De-interleave. Data first, one codeword per block per round, blocks
  // that ran out sitting the round out; then the parity, which is square.
  const blocks = structure.lengths.map(length => ({ length, data: [], ec: [] }));
  let at = 0;
  const longest = Math.max(...structure.lengths);
  for (let i = 0; i < longest; i++) {
    for (const block of blocks) if (i < block.length) block.data.push(codewords[at++]);
  }
  for (let i = 0; i < structure.ecPerBlock; i++) {
    for (const block of blocks) block.ec.push(codewords[at++]);
  }
  assert.equal(at, structure.total, "interleaving did not consume every codeword");

  for (const [index, block] of blocks.entries()) {
    const codeword = [...block.data, ...block.ec];
    assert.equal(codeword.length, block.length + structure.ecPerBlock);
    for (let i = 0; i < structure.ecPerBlock; i++) {
      assert.equal(syndrome(codeword, i), 0, `block ${index} syndrome ${i} is non-zero`);
    }
  }

  // Byte mode, out of the data codewords in block order.
  const stream = [];
  for (const block of blocks) stream.push(...block.data);
  let cursor = 0;
  const take = count => {
    let value = 0;
    for (let i = 0; i < count; i++, cursor++) {
      value = (value << 1) | ((stream[cursor >>> 3] >>> (7 - (cursor & 7))) & 1);
    }
    return value;
  };
  assert.equal(take(4), 0b0100, "the segment is not byte mode");
  const length = take(qr.version <= 9 ? 8 : 16);
  const payload = new Uint8Array(length);
  for (let i = 0; i < length; i++) payload[i] = take(8);

  // The pad, when there is room for one: terminator, alignment to a
  // codeword boundary, then 0xEC/0x11 alternating from the first pad byte.
  const padStart = Math.ceil(cursor / 8);
  for (let i = padStart, pad = 0xec; i < stream.length; i++, pad ^= 0xec ^ 0x11) {
    assert.equal(stream[i], pad, `pad codeword ${i} is not the standard alternation`);
  }
  return { payload, ecLevel: format.ecLevel, mask: format.mask, version: qr.version };
}

function decodeText(qr) {
  return new TextDecoder().decode(decodeQr(qr).payload);
}

// A seeded stream, so a failure is reproducible rather than a story about
// a random string nobody has any more.
function pseudoRandom(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

const BASE64URL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

function base64UrlNoise(length, seed) {
  const next = pseudoRandom(seed);
  let out = "";
  for (let i = 0; i < length; i++) out += BASE64URL[Math.floor(next() * BASE64URL.length)];
  return out;
}

// --- Round trips -----------------------------------------------------------

test("a short string round-trips through a small symbol", () => {
  const qr = encodeQr("rangefind");
  assert.ok(qr.version <= 2, `expected v1-v2, got v${qr.version}`);
  assert.equal(qr.size, qr.version * 4 + 17);
  assert.equal(qr.modules.length, qr.size * qr.size);
  assert.equal(decodeText(qr), "rangefind");
});

test("a dispatch ticket's 224 base64url characters round-trip", () => {
  const text = `wayfind://ticket#${base64UrlNoise(224, 20260807)}`;
  const qr = encodeQr(text);
  assert.equal(decodeText(qr), text);
});

test("a 250-character URL round-trips", () => {
  const prefix = "https://rangefind.dev/demo/?job=";
  const url = `${prefix}${base64UrlNoise(250 - prefix.length, 7)}`;
  assert.equal(url.length, 250);
  assert.equal(decodeText(encodeQr(url)), url);
});

test("a Uint8Array payload round-trips byte for byte", () => {
  const next = pseudoRandom(99);
  const bytes = Uint8Array.from({ length: 120 }, () => Math.floor(next() * 256));
  const decoded = decodeQr(encodeQr(bytes));
  assert.deepEqual(decoded.payload, bytes);
});

test("every error-correction level encodes the same payload", () => {
  const text = base64UrlNoise(180, 3);
  const versions = [];
  for (const ecLevel of ["L", "M", "Q", "H"]) {
    const qr = encodeQr(text, { ecLevel });
    assert.equal(qr.ecLevel, ecLevel);
    const decoded = decodeQr(qr);
    assert.equal(decoded.ecLevel, ecLevel);
    assert.equal(new TextDecoder().decode(decoded.payload), text);
    versions.push(qr.version);
  }
  // More parity buys correction with capacity, so the symbol grows.
  for (let i = 1; i < versions.length; i++) assert.ok(versions[i] >= versions[i - 1]);
});

test("a payload past version 7 carries readable version information", () => {
  const qr = encodeQr(base64UrlNoise(400, 11), { ecLevel: "M" });
  assert.ok(qr.version >= 7, `expected v7+, got v${qr.version}`);
  const info = readVersionInfo(qr);
  assert.equal(info.bchRemainder, 0);
  assert.equal(info.version, qr.version);
  assert.equal(decodeText(qr).length, 400);
});

test("a payload below version 7 carries no version information", () => {
  const qr = encodeQr("short", { ecLevel: "L" });
  assert.ok(qr.version < 7);
  assert.equal(readVersionInfo(qr), null);
});

test("encoding is deterministic", () => {
  const text = `wayfind://ticket#${base64UrlNoise(224, 5)}`;
  const first = encodeQr(text, { ecLevel: "Q" });
  const second = encodeQr(text, { ecLevel: "Q" });
  assert.equal(first.version, second.version);
  assert.equal(first.mask, second.mask);
  assert.deepEqual(first.modules, second.modules);
});

test("the geometry a scanner locks onto is where the spec puts it", () => {
  const qr = encodeQr(base64UrlNoise(300, 42), { ecLevel: "Q" });
  const size = qr.size;
  const dark = (x, y) => qr.modules[y * size + x] === 1;
  for (const [cx, cy] of [[3, 3], [size - 4, 3], [3, size - 4]]) {
    for (let dy = -4; dy <= 4; dy++) {
      for (let dx = -4; dx <= 4; dx++) {
        const x = cx + dx;
        const y = cy + dy;
        if (x < 0 || y < 0 || x >= size || y >= size) continue;
        const ring = Math.max(Math.abs(dx), Math.abs(dy));
        assert.equal(dark(x, y), ring !== 2 && ring !== 4, `finder at ${cx},${cy} module ${dx},${dy}`);
      }
    }
  }
  for (let i = 8; i < size - 8; i++) {
    assert.equal(dark(i, 6), i % 2 === 0, `horizontal timing at ${i}`);
    assert.equal(dark(6, i), i % 2 === 0, `vertical timing at ${i}`);
  }
  assert.equal(dark(8, size - 8), true, "the dark module is missing");
  const centres = ALIGNMENT[qr.version];
  const last = centres.length - 1;
  for (let i = 0; i <= last; i++) {
    for (let j = 0; j <= last; j++) {
      if ((i === 0 && j === 0) || (i === 0 && j === last) || (i === last && j === 0)) continue;
      for (let dy = -2; dy <= 2; dy++) {
        for (let dx = -2; dx <= 2; dx++) {
          const ring = Math.max(Math.abs(dx), Math.abs(dy));
          assert.equal(dark(centres[i] + dx, centres[j] + dy), ring !== 1, `alignment ${centres[i]},${centres[j]}`);
        }
      }
    }
  }
});

test("placement starts in the bottom-right corner and walks up", () => {
  // The one anchor the round-trip cannot supply: a decoder that walked
  // the zigzag backwards in the same way as the encoder would still see
  // clean parity. This pins the start and direction against the figure
  // in the standard instead.
  const qr = encodeQr("rangefind");
  assert.equal(qr.version, 1); // one block, so placement order is codeword order
  const size = qr.size;
  const { mask } = readFormat(qr);
  let first = 0;
  for (let y = size - 1; y >= size - 4; y--) {
    for (const x of [size - 1, size - 2]) {
      first = (first << 1) | (qr.modules[y * size + x] ^ (maskAt(mask, x, y) ? 1 : 0));
    }
  }
  // Mode 0100, then the 8-bit count 00001001 — so codeword 0 is 0100 0000.
  assert.equal(first, 0b01000000);
});

test("a payload that fits nowhere is refused", () => {
  assert.throws(() => encodeQr("x".repeat(3000), { ecLevel: "H" }), /do not fit/u);
  assert.throws(() => encodeQr("x", { ecLevel: "Z" }), /error-correction level/u);
  assert.throws(() => encodeQr(42), /string or a Uint8Array/u);
});

test("the SVG is self-contained and has one rect per dark module", () => {
  const qr = encodeQr("rangefind");
  const svg = qrSvg(qr, { margin: 4 });
  assert.ok(svg.startsWith("<svg"));
  assert.ok(svg.endsWith("</svg>"));
  const extent = qr.size + 8;
  assert.match(svg, new RegExp(`viewBox="0 0 ${extent} ${extent}"`, "u"));
  assert.match(svg, /shape-rendering="crispEdges"/u);
  // A quiet zone that is not painted is a code no scanner finds on a dark
  // page, so the white rect must cover the whole viewBox.
  assert.match(svg, new RegExp(`<rect width="${extent}" height="${extent}" fill="#ffffff"/>`, "u"));
  assert.ok(!svg.includes("http://") || svg.includes("http://www.w3.org/2000/svg"));
  const dark = qr.modules.reduce((sum, value) => sum + value, 0);
  assert.equal([...svg.matchAll(/h1v1h-1z/gu)].length, dark);
});

test("a real dispatch ticket survives the screen", async () => {
  const issued = await issueTicket({
    issuerSeed: sha256Utf8("rangefind-qr-test-dispatcher"),
    epochPrefix8: sha256Utf8("rangefind-qr-test-epoch").subarray(0, 8),
    notAfter: Math.floor(Date.UTC(2030, 0, 1) / 1000),
    mode: THREAD_MODE.FINE,
    plan: {
      dwellSeconds: 120,
      stops: [
        { lat: 45.5019, lon: -73.5674, label: "Pickup" },
        { lat: 45.5088, lon: -73.554, label: "Drop-off" }
      ]
    }
  });
  // The current capability-separated ticket is 228 bytes / 304 base64url
  // characters for this two-stop plan, still within a camera-readable QR.
  assert.ok(issued.base64url.length >= 300 && issued.base64url.length <= 320, `${issued.base64url.length} chars`);

  const url = `https://rangefind.dev/osm-geo/#${issued.base64url}`;
  const qr = encodeQr(url, { ecLevel: "M" });
  const recovered = decodeText(qr);
  assert.equal(recovered, url);

  const ticket = decodeThreadTicket(recovered.slice(recovered.indexOf("#") + 1));
  assert.equal(ticket.mode, THREAD_MODE.FINE);
  assert.equal(ticket.seedPresent, true);
  assert.deepEqual(ticket.bytes, issued.bytes);
  assert.equal(ticket.plan.stops.length, 2);
  assert.equal(ticket.plan.stops[1].label, "Drop-off");
});

test("maxVersion is a policy bound: over it, the encoder refuses rather than densifies", () => {
  const short = encodeQr("wayfind://ticket#abc", { maxVersion: 25 });
  assert.ok(short.version <= 25);
  // ~1.6 KB needs far more than v10 can hold at level M.
  const big = "x".repeat(1600);
  assert.throws(() => encodeQr(big, { maxVersion: 10 }), /within version 10/);
  const unbounded = encodeQr(big);
  assert.ok(unbounded.version > 10, "the same payload still fits without the bound");
  assert.throws(() => encodeQr("a", { maxVersion: 0 }), /between 1 and 40/);
});
