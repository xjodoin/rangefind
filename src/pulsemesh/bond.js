// PulseMesh identity bonds (protocol §5.4): a per-peer, per-day admission
// proof that moves the anti-Sybil cost off the record path.
//
// The realignment this exists for: every defense in the protocol punishes
// an identity — the trust ledger penalizes peers, rule 7 rate-limits
// peers, incident scoring counts distinct peers — but per-record PoW
// charges records, and peer identities are free. A bond charges the thing
// the defenses punish. Records from bonded peers carry no proof at all
// (proofType 3), so contribution and hazard-report latency drop to zero,
// and a trust-floor forfeits real work instead of a free keypair — at
// the receiver that saw the evidence, spreading only as corroborated
// testimony (§8.4), because the ledger itself is local by design.
//
// The puzzle is a Momentum-style birthday collision, chosen for one
// measured property (benchmarks §14): verification stays at three hashes
// (~1 µs) at any table size. Memory-hardness was refuted at record
// cadence, where the table must be phone-affordable per emission and is
// therefore cache-sized; at one solve per day the table can be 256 MiB+,
// where a 24 GiB GPU fits 96 concurrent solvers instead of 98,000. Known
// honest limit: Momentum admits time-memory tradeoffs (van Oorschot–
// Wiener collision search), so the RAM bound is an upper estimate of the
// attacker's constraint, and the doctrine stands — corroboration and the
// speed anchor defend, proofs only throttle.

import { sha256 } from "./sha256.js";
import { leadingZeroBits, utf8Bytes } from "./codec.js";

const SEED_TAG = utf8Bytes("rangefind-bond-v1");

const PHASE_TAG = utf8Bytes("rangefind-bond-phase-v1");

/**
 * Per-peer phase offset within the bond lifetime. Without it every bond
 * on earth would share `floor(now / BOND_LIFETIME)` and expire at the
 * same UTC instant — a synchronized global re-mint storm once a day.
 * Hashing the peerId spreads rollovers uniformly across the lifetime;
 * the verifier derives the same phase from the connection's peerId, so
 * nothing about it travels on the wire.
 */
export function bondPhaseMillis(peerId, lifetimeSeconds) {
  const peer = utf8Bytes(String(peerId));
  const message = new Uint8Array(PHASE_TAG.length + peer.length);
  message.set(PHASE_TAG, 0);
  message.set(peer, PHASE_TAG.length);
  const h = sha256(message);
  // 48 bits keeps the value inside Number's safe range.
  const value = ((h[0] << 16) | (h[1] << 8) | h[2]) * 0x1000000 + ((h[3] << 16) | (h[4] << 8) | h[5]);
  return value % (lifetimeSeconds * 1000);
}

/** The peer's current validity bucket, offset by its rollover phase. */
export function bondBucketForPeer(peerId, nowMillis, lifetimeSeconds) {
  return Math.floor((nowMillis - bondPhaseMillis(peerId, lifetimeSeconds)) / (lifetimeSeconds * 1000));
}

/**
 * The puzzle seed. Binding: epoch (a bond dies with the graph epoch),
 * bucket (a bond expires), peerId (a bond is useless to any other peer —
 * the verifier reconstructs this from the live connection, never from the
 * wire), and salt (an unlucky instance is retried at salt+1 instead of
 * failing forever; each salt is an independent puzzle of the same
 * expected cost, so grinding salts buys nothing).
 */
export function bondSeed(epoch32, dayBucket, salt, peerId) {
  if (epoch32.length !== 32) throw new Error("bondSeed requires the full 32-byte epoch.");
  const peer = utf8Bytes(String(peerId));
  const message = new Uint8Array(SEED_TAG.length + 32 + 8 + 1 + peer.length);
  let pos = 0;
  message.set(SEED_TAG, pos); pos += SEED_TAG.length;
  message.set(epoch32, pos); pos += 32;
  for (let shift = 56; shift >= 0; shift -= 8) message[pos++] = Math.floor(dayBucket / 2 ** shift) & 0xff;
  message[pos++] = salt & 0xff;
  message.set(peer, pos);
  return sha256(message);
}

// 48 bits of the hash keep the birthday value inside Number's safe range;
// birthdayBits above 48 would silently truncate, so it is the hard cap.
export const MAX_BIRTHDAY_BITS = 48;

function birthdayHasher(seed32) {
  const message = new Uint8Array(seed32.length + 4);
  message.set(seed32, 0);
  const offset = seed32.length;
  return (nonce, mask) => {
    message[offset] = (nonce >>> 24) & 0xff;
    message[offset + 1] = (nonce >>> 16) & 0xff;
    message[offset + 2] = (nonce >>> 8) & 0xff;
    message[offset + 3] = nonce & 0xff;
    const h = sha256(message);
    const value = ((h[0] << 16) | (h[1] << 8) | h[2]) * 0x1000000 + ((h[3] << 16) | (h[4] << 8) | h[5]);
    return value % mask;
  };
}

function pairMessage(seed32, i, j) {
  const message = new Uint8Array(seed32.length + 8);
  message.set(seed32, 0);
  const offset = seed32.length;
  message[offset] = (i >>> 24) & 0xff;
  message[offset + 1] = (i >>> 16) & 0xff;
  message[offset + 2] = (i >>> 8) & 0xff;
  message[offset + 3] = i & 0xff;
  message[offset + 4] = (j >>> 24) & 0xff;
  message[offset + 5] = (j >>> 16) & 0xff;
  message[offset + 6] = (j >>> 8) & 0xff;
  message[offset + 7] = j & 0xff;
  return message;
}

/**
 * Three hashes, constant memory: two birthday evaluations and one pair
 * hash. This is the property the whole design leans on — a keeper
 * admitting peers pays ~1 µs per bond regardless of how large a table
 * the miner needed.
 */
export function verifyBondProof(seed32, i, j, birthdayBits, pairDifficulty) {
  if (i === j) return false;
  if (birthdayBits > MAX_BIRTHDAY_BITS) return false;
  const mask = 2 ** birthdayBits;
  const birthday = birthdayHasher(seed32);
  if (birthday(i, mask) !== birthday(j, mask)) return false;
  return leadingZeroBits(sha256(pairMessage(seed32, i, j))) >= pairDifficulty;
}

function monotonicNow() {
  return typeof performance === "object" && performance ? performance.now() : Date.now();
}

function defaultYield() {
  if (typeof scheduler === "object" && scheduler && typeof scheduler.yield === "function") {
    return scheduler.yield();
  }
  return new Promise(resolve => setTimeout(resolve, 0));
}

const INITIAL_SLICE = 20000;
const MIN_SLICE = 1000;
const MAX_SLICE = 500000;

/**
 * Mines one bond instance: scans nonces, storing (birthday value, nonce)
 * in an open-addressed table, until a collision whose pair hash meets
 * `pairDifficulty` appears. Sliced with adaptive chunks that yield to
 * the event loop — a mint runs for seconds on a phone and must never own
 * the thread; it is meant for the background, ideally while charging,
 * once per bucket.
 *
 * Returns { i, j, hashes, tableBytes } or null when the scan window,
 * budget, or signal ends the search (the caller retries at salt+1 — a
 * collision-free window happens with probability ~e^-8 per salt).
 */
export async function solveBondProof(seed32, birthdayBits, pairDifficulty, {
  chunkMillis = 10,
  budgetMillis = null,
  signal = null,
  yieldTo = defaultYield,
  now = monotonicNow,
  // First-slice size, before the adaptive sizer has a measurement. Small
  // values force early yields (tests, very slow devices).
  sliceIterations = INITIAL_SLICE
} = {}) {
  if (birthdayBits > MAX_BIRTHDAY_BITS) throw new Error(`birthdayBits above ${MAX_BIRTHDAY_BITS} truncates.`);
  const mask = 2 ** birthdayBits;
  const birthday = birthdayHasher(seed32);
  // A collision is expected after ~1.18 * 2^(B/2) draws; scan 4x that and
  // size the table for the whole window at load factor 0.5, or linear
  // probing degrades exactly when the answer is near.
  const scan = Math.ceil(4 * 2 ** (birthdayBits / 2));
  const slotBits = Math.ceil(Math.log2(scan)) + 1;
  const slots = 2 ** slotBits;
  const slotMask = slots - 1;
  const keys = new Uint32Array(slots);   // low 32 bits of the birthday value
  const vals = new Uint32Array(slots);   // nonce + 1; 0 means empty
  const tableBytes = keys.byteLength + vals.byteLength;
  const started = now();
  let hashes = 0;
  let slice = Math.max(1, sliceIterations);

  for (let nonce = 0; nonce < scan;) {
    if (signal && signal.aborted) return null;
    if (budgetMillis != null && now() - started >= budgetMillis) return null;
    const sliceEnd = Math.min(nonce + slice, scan);
    const sliceStart = now();
    for (; nonce < sliceEnd; nonce++) {
      const value = birthday(nonce, mask);
      hashes++;
      const key = value >>> 0;
      let slot = (value % slots) & slotMask;
      for (let probe = 0; probe < slots; probe++) {
        const occupant = vals[slot];
        if (occupant === 0) {
          keys[slot] = key;
          vals[slot] = nonce + 1;
          break;
        }
        if (keys[slot] === key) {
          const other = occupant - 1;
          // Re-derive to reject a truncated-key false positive.
          if (other !== nonce && birthday(other, mask) === value) {
            hashes++;
            if (leadingZeroBits(sha256(pairMessage(seed32, other, nonce))) >= pairDifficulty) {
              return { i: other, j: nonce, hashes: hashes + 1, tableBytes };
            }
          } else {
            hashes++;
          }
          break;
        }
        slot = (slot + 1) & slotMask;
      }
    }
    const elapsed = now() - sliceStart;
    if (elapsed > 0) {
      const scaled = Math.round((slice * chunkMillis) / elapsed);
      slice = Math.max(MIN_SLICE, Math.min(MAX_SLICE, scaled));
    }
    if (nonce < scan) await yieldTo();
  }
  return null;
}

/**
 * Mints a complete bond for `peerId` in the current bucket, retrying
 * fresh salts across collision-free instances. This is the one call a
 * transport needs; everything else here is its plumbing.
 */
export async function mintBond({
  epoch32,
  peerId,
  constants,
  nowMillis = Date.now(),
  maxSalts = 8,
  chunkMillis = 10,
  budgetMillis = null,
  signal = null,
  yieldTo = defaultYield
}) {
  const dayBucket = bondBucketForPeer(peerId, nowMillis, constants.BOND_LIFETIME);
  for (let salt = 0; salt < maxSalts; salt++) {
    if (signal && signal.aborted) return null;
    const seed = bondSeed(epoch32, dayBucket, salt, peerId);
    const solved = await solveBondProof(seed, constants.BOND_BIRTHDAY_BITS, constants.BOND_PAIR_DIFFICULTY, {
      chunkMillis, budgetMillis, signal, yieldTo
    });
    if (solved) {
      return {
        epochPrefix8: epoch32.subarray(0, 8),
        dayBucket,
        birthdayBits: constants.BOND_BIRTHDAY_BITS,
        pairDifficulty: constants.BOND_PAIR_DIFFICULTY,
        salt,
        i: solved.i,
        j: solved.j
      };
    }
  }
  return null;
}

/**
 * Full admission check for a decoded PMA1 against the verifier's own
 * epoch, constants, clock, and — critically — the peerId of the live
 * connection it arrived on. Returns { ok: true, expiresMillis } or
 * { ok: false, reason }.
 */
export function verifyBond(bond, {
  epoch32,
  previousEpoch32 = null,
  peerId,
  constants,
  nowMillis = Date.now()
}) {
  const prefixMatches = epoch => {
    for (let index = 0; index < 8; index++) if (epoch[index] !== bond.epochPrefix8[index]) return false;
    return true;
  };
  const epoch = prefixMatches(epoch32) ? epoch32
    : previousEpoch32 && prefixMatches(previousEpoch32) ? previousEpoch32
    : null;
  if (!epoch) return { ok: false, reason: "bond names an unknown epoch" };
  // A miner may hold weaker constants from a stale bootstrap; the
  // verifier's floor is what admission means here.
  if (bond.birthdayBits < constants.BOND_BIRTHDAY_BITS) return { ok: false, reason: "birthdayBits below floor" };
  if (bond.pairDifficulty < constants.BOND_PAIR_DIFFICULTY) return { ok: false, reason: "pairDifficulty below floor" };
  const lifetimeMillis = constants.BOND_LIFETIME * 1000;
  const bucketStart = bond.dayBucket * lifetimeMillis + bondPhaseMillis(peerId, constants.BOND_LIFETIME);
  const expiresMillis = bucketStart + lifetimeMillis + constants.BOND_OVERLAP * 1000;
  if (nowMillis >= expiresMillis) return { ok: false, reason: "bond expired" };
  if (bucketStart > nowMillis + constants.MAX_FUTURE_SKEW * 1000) return { ok: false, reason: "bond from the future" };
  const seed = bondSeed(epoch, bond.dayBucket, bond.salt, peerId);
  if (!verifyBondProof(seed, bond.i, bond.j, bond.birthdayBits, bond.pairDifficulty)) {
    return { ok: false, reason: "bond proof invalid for this peer" };
  }
  return { ok: true, expiresMillis };
}
