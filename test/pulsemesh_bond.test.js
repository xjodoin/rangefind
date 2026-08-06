// Identity bonds (§5.4): the per-peer, per-day admission proof that
// replaces per-record PoW with proofType 3. Tests run at tiny birthday
// sizes — the puzzle's structure is identical at every B; only the table
// grows — so the whole file solves in milliseconds.

import assert from "node:assert/strict";
import test from "node:test";
import { DEFAULT_CONSTANTS } from "../src/pulsemesh/bins.js";
import {
  bondBucketForPeer,
  bondPhaseMillis,
  bondSeed,
  mintBond,
  solveBondProof,
  verifyBond,
  verifyBondProof
} from "../src/pulsemesh/bond.js";
import { createValidator } from "../src/pulsemesh/validate.js";
import { PROOF_BOND, decodePMA1, encodePMA1, encodePMC1 } from "../src/pulsemesh/codec.js";
import { sha256Utf8 } from "../src/pulsemesh/sha256.js";

const EPOCH32 = sha256Utf8("pulsemesh-bond-test");
const PREFIX8 = EPOCH32.slice(0, 8);
// B=16: 512-entry table, solves in ~1 ms. The constants clamp keeps every
// other rule at production values so nothing else silently relaxes.
const CONSTANTS = { ...DEFAULT_CONSTANTS, BOND_BIRTHDAY_BITS: 16 };
const NOW = Date.now();

test("bond solve/verify round-trips, and the proof binds every seed input", async () => {
  const seed = bondSeed(EPOCH32, 123, 0, "peer-alpha");
  const solved = await solveBondProof(seed, 16, 0);
  assert.ok(solved, "B=16 solves within the scan window");
  assert.ok(verifyBondProof(seed, solved.i, solved.j, 16, 0));
  assert.ok(verifyBondProof(seed, solved.j, solved.i, 16, 0), "nonce order is not significant");
  assert.equal(verifyBondProof(seed, solved.i, solved.i, 16, 0), false, "i must differ from j");

  // Each seed input — epoch, bucket, salt, peer — invalidates the proof.
  for (const other of [
    bondSeed(sha256Utf8("other-epoch"), 123, 0, "peer-alpha"),
    bondSeed(EPOCH32, 124, 0, "peer-alpha"),
    bondSeed(EPOCH32, 123, 1, "peer-alpha"),
    bondSeed(EPOCH32, 123, 0, "peer-beta")
  ]) {
    assert.equal(verifyBondProof(other, solved.i, solved.j, 16, 0), false);
  }
});

test("PMA1 encodes and decodes byte-exactly", () => {
  const bond = { epochPrefix8: PREFIX8, dayBucket: 20654, birthdayBits: 44, pairDifficulty: 0, salt: 2, i: 0xdeadbeef, j: 17 };
  const decoded = decodePMA1(encodePMA1(bond));
  assert.equal(decoded.kind, "bond");
  assert.equal(decoded.dayBucket, 20654);
  assert.equal(decoded.birthdayBits, 44);
  assert.equal(decoded.salt, 2);
  assert.equal(decoded.i, 0xdeadbeef);
  assert.equal(decoded.j, 17);
  assert.throws(() => decodePMA1(Uint8Array.from([...encodePMA1(bond), 0])), /trailing/);
});

test("mintBond produces a bond verifyBond accepts, for the right peer only", async () => {
  const bond = await mintBond({ epoch32: EPOCH32, peerId: "peer-alpha", constants: CONSTANTS, nowMillis: NOW });
  assert.ok(bond, "mint succeeds at B=16");
  assert.equal(bond.dayBucket, bondBucketForPeer("peer-alpha", NOW, CONSTANTS.BOND_LIFETIME));

  const accept = verifyBond(bond, { epoch32: EPOCH32, peerId: "peer-alpha", constants: CONSTANTS, nowMillis: NOW });
  assert.ok(accept.ok);
  assert.ok(accept.expiresMillis > NOW);

  // The same bytes presented from a different connection are worthless —
  // this is the whole binding. The rejection reason depends on the
  // thief's rollover phase (the bucket window shifts per peer before the
  // proof is even checked), so assert the rejection, not its path.
  const stolen = verifyBond(bond, { epoch32: EPOCH32, peerId: "peer-thief", constants: CONSTANTS, nowMillis: NOW });
  assert.equal(stolen.ok, false);
  assert.match(stolen.reason, /invalid for this peer|expired|future/);

  // Expiry: one lifetime plus overlap after the bucket starts — offset
  // by this peer's rollover phase.
  const phase = bondPhaseMillis("peer-alpha", CONSTANTS.BOND_LIFETIME);
  const later = (bond.dayBucket + 1) * CONSTANTS.BOND_LIFETIME * 1000 + phase + CONSTANTS.BOND_OVERLAP * 1000;
  assert.equal(verifyBond(bond, { epoch32: EPOCH32, peerId: "peer-alpha", constants: CONSTANTS, nowMillis: later }).ok, false);
  assert.equal(
    verifyBond(bond, { epoch32: EPOCH32, peerId: "peer-alpha", constants: CONSTANTS, nowMillis: later - 1000 }).ok,
    true,
    "still valid inside the overlap"
  );

  // A weaker-than-floor bond is refused even with a valid proof.
  const strict = { ...CONSTANTS, BOND_BIRTHDAY_BITS: 20 };
  assert.match(
    verifyBond(bond, { epoch32: EPOCH32, peerId: "peer-alpha", constants: strict, nowMillis: NOW }).reason,
    /below floor/
  );
});

test("solveBondProof yields between slices and honours the abort signal", async () => {
  // B=26 needs several slices at a 1 ms chunk target without being slow.
  const seed = bondSeed(EPOCH32, 5, 0, "peer-yield");
  let yields = 0;
  const solved = await solveBondProof(seed, 26, 0, {
    chunkMillis: 1,
    sliceIterations: 500,
    yieldTo: async () => { yields++; }
  });
  assert.ok(solved, "B=26 solves");
  assert.ok(yields >= 1, `expected sliced mining, saw ${yields} yields`);

  const controller = new AbortController();
  controller.abort();
  assert.equal(await solveBondProof(seed, 26, 0, { signal: controller.signal }), null);
});

test("validator rule 5: proofType 3 rides on the delivering peer's bond", () => {
  const bonded = new Set(["peer-good"]);
  const validator = createValidator({
    constants: CONSTANTS,
    epoch32: EPOCH32,
    isBonded: peer => bonded.has(peer),
    clock: () => NOW
  });
  const record = () => encodePMC1({
    epochPrefix8: PREFIX8,
    leafCell: 5, geomRef: 12, timeBucket: Math.floor(NOW / 15000),
    speedBin: 6, qualityBin: 5, meters: 200, ttlSeconds: 90,
    reportId: sha256Utf8(`r-${Math.random()}`).slice(0, 16),
    proofType: PROOF_BOND, proof: new Uint8Array(0)
  }).bytes;

  assert.ok(validator.validateContribution(record(), { fromPeer: "peer-good" }).ok, "bonded deliverer vouches");
  const rejected = validator.validateContribution(record(), { fromPeer: "peer-anon" });
  assert.equal(rejected.ok, false);
  assert.equal(rejected.rule, 5);
  assert.ok(validator.validateContribution(record(), { fromPeer: null }).ok, "locally produced records need no vouch");
  assert.ok(
    validator.validateContribution(record(), { fromPeer: null, vouchPeer: "peer-good" }).ok,
    "snapshot merge vouches via the provider"
  );
  assert.equal(
    validator.validateContribution(record(), { fromPeer: null, vouchPeer: "peer-anon" }).ok,
    false,
    "an unbonded snapshot provider cannot launder proofless records"
  );

  // A mesh with no bond deployment rejects proofType 3 outright.
  const noBonds = createValidator({ constants: CONSTANTS, epoch32: EPOCH32, clock: () => NOW });
  assert.equal(noBonds.validateContribution(record(), { fromPeer: "peer-good" }).ok, false);
});

test("bond rollovers are staggered per peer, not synchronized at UTC midnight", () => {
  // The bug this guards against: floor(now / lifetime) with no phase
  // gave every bond on earth the same expiry instant — a synchronized
  // global re-mint storm. Phases must spread across the whole lifetime.
  const LIFE = CONSTANTS.BOND_LIFETIME;
  const phases = Array.from({ length: 64 }, (_, i) => bondPhaseMillis(`peer-${i}`, LIFE));
  assert.ok(new Set(phases).size > 60, "phases are effectively distinct");
  const spread = Math.max(...phases) - Math.min(...phases);
  assert.ok(spread > LIFE * 1000 * 0.5, `phases span the lifetime (spread ${spread} ms)`);
  // And the bucket boundary really moves with the phase: at one fixed
  // instant, peers land in different buckets around their own rollovers.
  const t = 20671 * LIFE * 1000 + Math.floor(LIFE * 500);
  const buckets = new Set(phases.map((_, i) => bondBucketForPeer(`peer-${i}`, t, LIFE)));
  assert.ok(buckets.size >= 2, "a fixed instant straddles different peers' buckets");
});
