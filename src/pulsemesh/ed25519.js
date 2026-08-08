// Dependency-free RFC 8032 Ed25519, for hosts whose WebCrypto has not got it.
//
// Chrome only shipped Ed25519 in WebCrypto unflagged in 137. Android
// WebView 133 — the emulator the Wayfind app runs on, and a great many
// real phones — throws NotSupportedError from
// `importKey({ name: "Ed25519" })`, and Hermes has no WebCrypto Ed25519
// at all. Before this file existed that surfaced as `verifyThread`
// returning false: an algorithm the host does not have, wearing the
// costume of a bad signature, so a perfectly valid dispatch ticket was
// refused on the device. AES-GCM, HKDF and HMAC-SHA-256 are everywhere,
// so only the signature primitive is reimplemented here.
//
// Correctness over speed, deliberately: threads sign and verify at human
// cadence — one signature per dispatch ticket, one verification per
// record — so a few milliseconds of BigInt arithmetic is invisible, and
// plain textbook formulas are worth far more than saved microseconds
// when the only reviewer is a human reading the RFC beside the code.
//
// NOT CONSTANT TIME. Scalar multiplication branches on the bits of the
// secret scalar and BigInt allocation time varies with operand size, so
// this leaks timing. That is an accepted trade for zero dependencies,
// bounded by what the key actually is: a per-run seed carried inside a
// dispatch ticket, a capability for one job that expires with it — not a
// long-lived identity, not a device key, never reused across runs
// (threads §4.1). The WebCrypto path stays preferred wherever it exists;
// thread_crypto.js only reaches for this file after probing and finding
// the host cannot do Ed25519 at all.

// --- SHA-512 (FIPS 180-4) --------------------------------------------------
//
// Kept in this file rather than a sibling sha512.js: Ed25519 is its only
// caller in the repo (PulseMesh's hashing everywhere else is SHA-256, in
// sha256.js), and RFC 8032 treats the hash as part of the signature
// scheme. 64-bit lanes as BigInt masked to 64 bits — slower than the
// 32-bit-pair dance sha256.js can use, but it hashes at most a few
// hundred bytes per signature and it is impossible to get subtly wrong.

const MASK64 = (1n << 64n) - 1n;

// First 64 bits of the fractional parts of the cube roots of the first
// 80 primes; the init vector is the same over square roots of the first 8.
const K512 = [
  0x428a2f98d728ae22n, 0x7137449123ef65cdn, 0xb5c0fbcfec4d3b2fn, 0xe9b5dba58189dbbcn,
  0x3956c25bf348b538n, 0x59f111f1b605d019n, 0x923f82a4af194f9bn, 0xab1c5ed5da6d8118n,
  0xd807aa98a3030242n, 0x12835b0145706fben, 0x243185be4ee4b28cn, 0x550c7dc3d5ffb4e2n,
  0x72be5d74f27b896fn, 0x80deb1fe3b1696b1n, 0x9bdc06a725c71235n, 0xc19bf174cf692694n,
  0xe49b69c19ef14ad2n, 0xefbe4786384f25e3n, 0x0fc19dc68b8cd5b5n, 0x240ca1cc77ac9c65n,
  0x2de92c6f592b0275n, 0x4a7484aa6ea6e483n, 0x5cb0a9dcbd41fbd4n, 0x76f988da831153b5n,
  0x983e5152ee66dfabn, 0xa831c66d2db43210n, 0xb00327c898fb213fn, 0xbf597fc7beef0ee4n,
  0xc6e00bf33da88fc2n, 0xd5a79147930aa725n, 0x06ca6351e003826fn, 0x142929670a0e6e70n,
  0x27b70a8546d22ffcn, 0x2e1b21385c26c926n, 0x4d2c6dfc5ac42aedn, 0x53380d139d95b3dfn,
  0x650a73548baf63den, 0x766a0abb3c77b2a8n, 0x81c2c92e47edaee6n, 0x92722c851482353bn,
  0xa2bfe8a14cf10364n, 0xa81a664bbc423001n, 0xc24b8b70d0f89791n, 0xc76c51a30654be30n,
  0xd192e819d6ef5218n, 0xd69906245565a910n, 0xf40e35855771202an, 0x106aa07032bbd1b8n,
  0x19a4c116b8d2d0c8n, 0x1e376c085141ab53n, 0x2748774cdf8eeb99n, 0x34b0bcb5e19b48a8n,
  0x391c0cb3c5c95a63n, 0x4ed8aa4ae3418acbn, 0x5b9cca4f7763e373n, 0x682e6ff3d6b2b8a3n,
  0x748f82ee5defb2fcn, 0x78a5636f43172f60n, 0x84c87814a1f0ab72n, 0x8cc702081a6439ecn,
  0x90befffa23631e28n, 0xa4506cebde82bde9n, 0xbef9a3f7b2c67915n, 0xc67178f2e372532bn,
  0xca273eceea26619cn, 0xd186b8c721c0c207n, 0xeada7dd6cde0eb1en, 0xf57d4f7fee6ed178n,
  0x06f067aa72176fban, 0x0a637dc5a2c898a6n, 0x113f9804bef90daen, 0x1b710b35131c471bn,
  0x28db77f523047d84n, 0x32caab7b40c72493n, 0x3c9ebe0a15c9bebcn, 0x431d67c49c100d4cn,
  0x4cc5d4becb3e42b6n, 0x597f299cfc657e2an, 0x5fcb6fab3ad6faecn, 0x6c44198c4a475817n
];

const H512 = [
  0x6a09e667f3bcc908n, 0xbb67ae8584caa73bn, 0x3c6ef372fe94f82bn, 0xa54ff53a5f1d36f1n,
  0x510e527fade682d1n, 0x9b05688c2b3e6c1fn, 0x1f83d9abfb41bd6bn, 0x5be0cd19137e2179n
];

const rotr64 = (value, bits) => ((value >> bits) | (value << (64n - bits))) & MASK64;

/** SHA-512 of a byte array, returning the 64-byte digest. */
export function sha512(input) {
  const message = input instanceof Uint8Array ? input : Uint8Array.from(input);
  // data ‖ 0x80 ‖ zeros ‖ 128-bit big-endian bit length, to a 128-byte multiple.
  const paddedLength = (((message.length + 16) >> 7) << 7) + 128;
  const padded = new Uint8Array(paddedLength);
  padded.set(message);
  padded[message.length] = 0x80;
  let bitLength = BigInt(message.length) * 8n;
  for (let i = 0; i < 16 && bitLength > 0n; i++) {
    padded[paddedLength - 1 - i] = Number(bitLength & 0xffn);
    bitLength >>= 8n;
  }

  const h = H512.slice();
  const w = new Array(80);

  for (let block = 0; block < paddedLength; block += 128) {
    for (let i = 0; i < 16; i++) {
      let word = 0n;
      for (let byte = 0; byte < 8; byte++) word = (word << 8n) | BigInt(padded[block + i * 8 + byte]);
      w[i] = word;
    }
    for (let i = 16; i < 80; i++) {
      const a = w[i - 15];
      const b = w[i - 2];
      const s0 = rotr64(a, 1n) ^ rotr64(a, 8n) ^ (a >> 7n);
      const s1 = rotr64(b, 19n) ^ rotr64(b, 61n) ^ (b >> 6n);
      w[i] = (w[i - 16] + s0 + w[i - 7] + s1) & MASK64;
    }

    let [a, b, c, d, e, f, g, hh] = h;
    for (let i = 0; i < 80; i++) {
      const S1 = rotr64(e, 14n) ^ rotr64(e, 18n) ^ rotr64(e, 41n);
      const ch = (e & f) ^ (~e & MASK64 & g);
      const t1 = (hh + S1 + ch + K512[i] + w[i]) & MASK64;
      const S0 = rotr64(a, 28n) ^ rotr64(a, 34n) ^ rotr64(a, 39n);
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const t2 = (S0 + maj) & MASK64;
      hh = g; g = f; f = e; e = (d + t1) & MASK64;
      d = c; c = b; b = a; a = (t1 + t2) & MASK64;
    }
    const next = [a, b, c, d, e, f, g, hh];
    for (let i = 0; i < 8; i++) h[i] = (h[i] + next[i]) & MASK64;
  }

  const digest = new Uint8Array(64);
  for (let i = 0; i < 8; i++) {
    let word = h[i];
    for (let byte = 7; byte >= 0; byte--) {
      digest[i * 8 + byte] = Number(word & 0xffn);
      word >>= 8n;
    }
  }
  return digest;
}

// --- the field, mod p = 2^255 - 19 ----------------------------------------

const P = (1n << 255n) - 19n;
/** The prime order of the base point's subgroup (RFC 8032 §5.1, `L`). */
const L = (1n << 252n) + 27742317777372353535851937790883648493n;

const mod = (value, modulus = P) => {
  const r = value % modulus;
  return r < 0n ? r + modulus : r;
};

function powMod(base, exponent, modulus = P) {
  let result = 1n;
  let acc = mod(base, modulus);
  let e = exponent;
  while (e > 0n) {
    if (e & 1n) result = (result * acc) % modulus;
    acc = (acc * acc) % modulus;
    e >>= 1n;
  }
  return result;
}

/** p is prime, so Fermat gives the inverse and we need no extended GCD. */
const invert = value => powMod(value, P - 2n);

// d = -121665/121666, and sqrt(-1), both as RFC 8032 §5.1 defines them.
const D = mod(-121665n * invert(121666n));
const SQRT_M1 = powMod(2n, (P - 1n) / 4n);

// --- the curve: twisted Edwards, extended homogeneous (X:Y:Z:T) ------------
//
// A point is { X, Y, Z, T } with x = X/Z, y = Y/Z and X*Y = Z*T. The
// addition law below (Hisil–Wong–Carter–Dawson, a = -1) is unified: it
// is correct for doubling too, which is why there is no separate double.

const IDENTITY = { X: 0n, Y: 1n, Z: 1n, T: 0n };
const D2 = mod(D * 2n);

function addPoints(p1, p2) {
  const a = mod((p1.Y - p1.X) * (p2.Y - p2.X));
  const b = mod((p1.Y + p1.X) * (p2.Y + p2.X));
  const c = mod(p1.T * D2 * p2.T);
  const d = mod(p1.Z * 2n * p2.Z);
  const e = b - a;
  const f = d - c;
  const g = d + c;
  const h = b + a;
  return { X: mod(e * f), Y: mod(g * h), T: mod(e * h), Z: mod(f * g) };
}

function scalarMultiply(point, scalar) {
  let result = IDENTITY;
  let addend = point;
  let k = mod(scalar, L);
  while (k > 0n) {
    if (k & 1n) result = addPoints(result, addend);
    addend = addPoints(addend, addend);
    k >>= 1n;
  }
  return result;
}

/** Projective equality without an inversion: X1*Z2 == X2*Z1 and likewise for Y. */
function pointsEqual(p1, p2) {
  return mod(p1.X * p2.Z) === mod(p2.X * p1.Z) && mod(p1.Y * p2.Z) === mod(p2.Y * p1.Z);
}

/**
 * x from y on the curve (§5.1.3): x^2 = (y^2 - 1)/(d*y^2 + 1). Returns
 * null when that has no square root — i.e. the encoding is not a point.
 */
function recoverX(u, v) {
  const v3 = mod(mod(v * v) * v);
  const v7 = mod(mod(v3 * v3) * v);
  const candidate = mod(mod(u * v3) * powMod(mod(u * v7), (P - 5n) / 8n));
  const check = mod(v * mod(candidate * candidate));
  if (check === mod(u)) return candidate;
  if (check === mod(-u)) return mod(candidate * SQRT_M1);
  return null;
}

function decodePoint(encoded) {
  if (encoded.length !== 32) return null;
  const sign = (encoded[31] >> 7) & 1;
  let y = 0n;
  for (let i = 31; i >= 0; i--) y = (y << 8n) | BigInt(i === 31 ? encoded[i] & 0x7f : encoded[i]);
  // A y at or above p is a second spelling of a point that already has
  // one. Refuse it rather than silently reducing: two encodings that
  // verify the same signature is exactly the malleability threads §7
  // spends a whole validation ladder avoiding.
  if (y >= P) return null;

  const y2 = mod(y * y);
  const x = recoverX(mod(y2 - 1n), mod(D * y2 + 1n));
  if (x === null) return null;
  if (x === 0n && sign === 1) return null;
  const finalX = (x & 1n) === BigInt(sign) ? x : mod(-x);
  return { X: finalX, Y: y, Z: 1n, T: mod(finalX * y) };
}

function encodePoint(point) {
  const zInverse = invert(point.Z);
  const x = mod(point.X * zInverse);
  const y = mod(point.Y * zInverse);
  const out = new Uint8Array(32);
  let remaining = y;
  for (let i = 0; i < 32; i++) {
    out[i] = Number(remaining & 0xffn);
    remaining >>= 8n;
  }
  out[31] |= Number(x & 1n) << 7;
  return out;
}

// The base point B: y = 4/5, and of the two roots the even one (RFC 8032
// §5.1 fixes the sign). Derived rather than transcribed, so there is no
// 78-digit literal to mistype — but the parity choice is not optional:
// take the odd root and every scalar multiple comes out as -[k]B, which
// encodes with the sign bit flipped and agrees with nothing.
const BASE_Y = mod(4n * invert(5n));
const BASE_ROOT = recoverX(mod(BASE_Y * BASE_Y - 1n), mod(D * BASE_Y * BASE_Y + 1n));
const BASE_X = (BASE_ROOT & 1n) === 0n ? BASE_ROOT : mod(-BASE_ROOT);
const BASE = { X: BASE_X, Y: BASE_Y, Z: 1n, T: mod(BASE_X * BASE_Y) };

// --- little-endian scalars -------------------------------------------------

function leToBigInt(bytes) {
  let value = 0n;
  for (let i = bytes.length - 1; i >= 0; i--) value = (value << 8n) | BigInt(bytes[i]);
  return value;
}

function bigIntToLe32(value) {
  const out = new Uint8Array(32);
  let remaining = value;
  for (let i = 0; i < 32; i++) {
    out[i] = Number(remaining & 0xffn);
    remaining >>= 8n;
  }
  return out;
}

function concatBytes(...parts) {
  let length = 0;
  for (const part of parts) length += part.length;
  const out = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

/** §5.1.5 pruning: clear the low 3 bits, clear bit 255, set bit 254. */
function clampScalar(half) {
  const clamped = Uint8Array.from(half);
  clamped[0] &= 248;
  clamped[31] &= 127;
  clamped[31] |= 64;
  return leToBigInt(clamped);
}

function requireSeed(seed) {
  if (!(seed instanceof Uint8Array) || seed.length !== 32) {
    throw new Error("An Ed25519 private seed is 32 bytes.");
  }
  return seed;
}

const asBytes = value => (value instanceof Uint8Array ? value : Uint8Array.from(value || []));

// --- the public surface ----------------------------------------------------

/** RFC 8032 §5.1.5: the public key A = [clamp(SHA-512(seed)[0..32])]B. */
export function ed25519PublicKeyFromSeed(seed) {
  const scalar = clampScalar(sha512(requireSeed(seed)).subarray(0, 32));
  return encodePoint(scalarMultiply(BASE, scalar));
}

/** RFC 8032 §5.1.6, deterministic: the nonce is hashed from the seed, never sampled. */
export function ed25519Sign(message, seed) {
  const bytes = asBytes(message);
  const expanded = sha512(requireSeed(seed));
  const scalar = clampScalar(expanded.subarray(0, 32));
  const prefix = expanded.subarray(32, 64);
  const publicKey = encodePoint(scalarMultiply(BASE, scalar));

  const r = mod(leToBigInt(sha512(concatBytes(prefix, bytes))), L);
  const R = encodePoint(scalarMultiply(BASE, r));
  const k = mod(leToBigInt(sha512(concatBytes(R, publicKey, bytes))), L);
  return concatBytes(R, bigIntToLe32(mod(r + k * scalar, L)));
}

/**
 * RFC 8032 §5.1.7, cofactorless ([S]B == R + [k]A), which is what
 * WebCrypto and OpenSSL check — the fallback must accept and refuse
 * exactly what the native path would, or a ticket's fate would depend
 * on which browser opened it.
 *
 * Never throws. A wrong length, a scalar at or above L, a y that is not
 * on the curve — all of those are a failed verification, because that is
 * how the caller reads them: `verifyThread` returns a boolean and a
 * malformed record is simply not signed by this key.
 */
export function ed25519Verify(message, signature, publicKey) {
  try {
    const sig = asBytes(signature);
    const key = asBytes(publicKey);
    if (sig.length !== 64 || key.length !== 32) return false;

    const s = leToBigInt(sig.subarray(32, 64));
    if (s >= L) return false; // non-canonical S — the malleable half of a signature

    const a = decodePoint(key);
    if (!a) return false;
    const r = decodePoint(sig.subarray(0, 32));
    if (!r) return false;

    const k = mod(leToBigInt(sha512(concatBytes(sig.subarray(0, 32), key, asBytes(message)))), L);
    return pointsEqual(scalarMultiply(BASE, s), addPoints(r, scalarMultiply(a, k)));
  } catch {
    return false;
  }
}
