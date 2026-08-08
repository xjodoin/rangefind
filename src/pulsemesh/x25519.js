// Dependency-free RFC 7748 X25519, for hosts whose WebCrypto has not got it.
//
// The sibling of ed25519.js, and it exists for the same reason. Ed25519
// signs a dispatch ticket; X25519 is what **seals** one to a device
// (threads §20.9), and a driver whose phone cannot do the key agreement
// cannot open the job at all. WebCrypto X25519 is newer than WebCrypto
// Ed25519 on every engine that has either — Chrome shipped it in 133,
// Safari in 17.4, and Hermes has no WebCrypto at all — so the fallback
// here is reached *more* often than ed25519.js's, not less.
//
// Correctness over speed, as in ed25519.js: one key agreement per
// recipient per ticket, at human cadence. A few milliseconds of BigInt
// arithmetic is invisible next to the driver reaching for the phone.
//
// NOT CONSTANT TIME, and the shape of that claim matters, so state it
// precisely. The Montgomery ladder below is written **constant-shaped**:
// it runs all 255 iterations whatever the scalar is, and the conditional
// swap is done with a BigInt mask (`0n - swap`, which is `0n` or `-1n`,
// and BigInt bitwise ops are two's complement with infinite sign
// extension — so `-1n & x === x`) rather than an `if`. What it is not is
// constant *time*: BigInt multiplication allocates, and its running time
// varies with how many limbs the operands actually occupy, so a local
// adversary measuring this code can learn something about the secret.
//
// What that does and does not mean here. The keys this file handles are
// **per-device enrolment keys**, and the attacker in §20.9's model is
// someone who photographs a QR code off a counter or intercepts a
// `.wayfindjob` file — an attacker holding *ciphertext*, with no way to
// ask a victim's device to run this function on chosen input and no way
// to time it if they could. A co-resident timing adversary already on
// the driver's phone, with a stopwatch inside the process, is a threat
// this file does not defend against; neither does anything else in the
// app, because such an adversary can simply read the key out of storage.
// The WebCrypto path stays preferred wherever it exists, and
// thread_crypto.js only reaches for this file after probing and finding
// the host cannot do X25519 at all.

// --- the field, mod p = 2^255 - 19 -----------------------------------------
//
// Deliberately duplicated from ed25519.js rather than shared: the two
// files are each meant to be readable beside their own RFC, and a
// shared field module would put a third file between the reader and
// either one. The whole arithmetic surface X25519 needs is these four
// lines — X25519 never leaves the u-coordinate, so there is no point
// decoding, no sign bit, and no curve equation check to get wrong.

const P = (1n << 255n) - 19n;

const mod = value => {
  const r = value % P;
  return r < 0n ? r + P : r;
};

function powMod(base, exponent) {
  let result = 1n;
  let acc = mod(base);
  let e = exponent;
  while (e > 0n) {
    if (e & 1n) result = (result * acc) % P;
    acc = (acc * acc) % P;
    e >>= 1n;
  }
  return result;
}

/** p is prime, so Fermat gives the inverse (RFC 7748 §5's `1/z`). */
const invert = value => powMod(value, P - 2n);

/** RFC 7748 §5: (486662 - 2) / 4 for curve25519. */
const A24 = 121665n;

// --- little-endian 32-byte encodings ---------------------------------------

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

/** RFC 7748 §5 decodeScalar25519: clear the low 3 bits, clear bit 255, set bit 254. */
function decodeScalar(scalar) {
  if (!(scalar instanceof Uint8Array) || scalar.length !== 32) {
    throw new Error("An X25519 private key is 32 bytes.");
  }
  const clamped = Uint8Array.from(scalar);
  clamped[0] &= 248;
  clamped[31] &= 127;
  clamped[31] |= 64;
  return leToBigInt(clamped);
}

/**
 * RFC 7748 §5 decodeUCoordinate. The high bit of the last byte is
 * **masked, not rejected**: the RFC requires implementations to ignore
 * it, and a peer that sets it must get the same answer here as it would
 * from WebCrypto, or a device would enrol fine on one phone and not on
 * another.
 */
function decodeU(u) {
  if (!(u instanceof Uint8Array) || u.length !== 32) {
    throw new Error("An X25519 public key is 32 bytes.");
  }
  const masked = Uint8Array.from(u);
  masked[31] &= 127;
  return leToBigInt(masked);
}

/**
 * Branch-free conditional swap. `swap` is 0n or 1n, so `0n - swap` is
 * `0n` or `-1n`; BigInt bitwise operations behave as two's complement
 * with infinite sign extension, so `-1n & x === x` and `0n & x === 0n`
 * with no mask width to get wrong. See the timing note at the top of
 * this file for what this buys and what it does not.
 */
function cswap(swap, a, b) {
  const mask = 0n - swap;
  const delta = mask & (a ^ b);
  return [a ^ delta, b ^ delta];
}

// --- the ladder -------------------------------------------------------------

/**
 * RFC 7748 §5, verbatim: the x-coordinate-only Montgomery ladder.
 *
 * Returns 32 bytes, or throws when the result is all zero. That output
 * means the peer sent a small-order point, and the RFC leaves the check
 * optional — but it is not optional for us: an all-zero shared secret is
 * one an attacker can produce without knowing any private key, so
 * accepting it would let anyone mint a wrap that a device "successfully"
 * opens. Since opening is the only authentication in §20.9, that is the
 * whole access control.
 */
export function x25519(privateKey, publicKey) {
  const k = decodeScalar(privateKey);
  const x1 = decodeU(publicKey);
  let x2 = 1n;
  let z2 = 0n;
  let x3 = x1;
  let z3 = 1n;
  let swap = 0n;

  // 255 bits, high to low. Bit 255 is cleared and bit 254 set by the
  // clamp, so the loop starts at 254 and the ladder's step count does
  // not depend on the scalar.
  for (let t = 254; t >= 0; t--) {
    const bit = (k >> BigInt(t)) & 1n;
    swap ^= bit;
    [x2, x3] = cswap(swap, x2, x3);
    [z2, z3] = cswap(swap, z2, z3);
    swap = bit;

    const a = mod(x2 + z2);
    const aa = mod(a * a);
    const b = mod(x2 - z2);
    const bb = mod(b * b);
    const e = mod(aa - bb);
    const c = mod(x3 + z3);
    const d = mod(x3 - z3);
    const da = mod(d * a);
    const cb = mod(c * b);
    x3 = mod(mod(da + cb) * mod(da + cb));
    z3 = mod(x1 * mod(mod(da - cb) * mod(da - cb)));
    x2 = mod(aa * bb);
    z2 = mod(e * mod(aa + mod(A24 * e)));
  }

  [x2, x3] = cswap(swap, x2, x3);
  [z2, z3] = cswap(swap, z2, z3);

  const out = bigIntToLe32(mod(x2 * invert(z2)));
  let accumulated = 0;
  for (const byte of out) accumulated |= byte;
  if (accumulated === 0) throw new Error("X25519 produced an all-zero shared secret (small-order public key).");
  return out;
}

/** RFC 7748 §6.1: the public key is X25519(k, 9). */
export function x25519Base(privateKey) {
  const base = new Uint8Array(32);
  base[0] = 9;
  return x25519(privateKey, base);
}
