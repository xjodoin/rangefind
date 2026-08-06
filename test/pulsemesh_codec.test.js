import assert from "node:assert/strict";
import test from "node:test";
import { performance } from "node:perf_hooks";
import {
  MAGIC,
  decodeAny,
  decodePMB1,
  decodePMC1,
  decodePMD1,
  decodePMF1,
  decodePMG1,
  decodePMI1,
  decodePMQ1,
  decodePMS1,
  encodePMB1,
  encodePMC1,
  encodePMD1,
  encodePMF1,
  encodePMG1,
  encodePMI1,
  encodePMQ1,
  encodePMS1,
  gossipMessageId,
  leadingZeroBits,
  parseSegment,
  segmentString,
  encodePMA1,
  decodePMA1
} from "../src/pulsemesh/codec.js";
import { fromHex, sha256, sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";
import { activeWindows, parseTopic, shardOfCell, topicForCell, topicName, windowAcceptable } from "../src/pulsemesh/topics.js";
import { FRESHNESS, QUALITY_WEIGHT, qualityBinFromSnap, speedBinFromKmh } from "../src/pulsemesh/bins.js";

// §13 test-vector epoch.
const EPOCH32 = sha256Utf8("pulsemesh-test-vector");
const PREFIX8 = EPOCH32.slice(0, 8);

test("sha256 reproduces the spec's test-vector epoch", () => {
  assert.equal(toHex(EPOCH32), "f44796c8cc1f3fa797104e925812ff052717f3052b5dbcadb0a36db776e0a4d1");
});

test("§13.1 contribution record encodes byte-identically", () => {
  const { bytes, preimage } = encodePMC1({
    epochPrefix8: PREFIX8,
    leafCell: 3181,
    geomRef: 885,
    timeBucket: 116951040,
    speedBin: 7,
    qualityBin: 6,
    meters: 184,
    ttlSeconds: 90,
    reportId: sha256Utf8("report").slice(0, 16),
    proofType: 3,
    proof: new Uint8Array(0)
  });
  assert.equal(
    toHex(preimage),
    "504d4331f44796c8cc1f3fa7ed18f5068090e2370706b8015a845e91831319e89c4d656bdb80c278ac"
  );
  assert.equal(
    toHex(bytes),
    "504d4331f44796c8cc1f3fa7ed18f5068090e2370706b8015a845e91831319e89c4d656bdb80c278ac0300"
  );
  assert.equal(bytes.length, 43, "a bonded record is 43 bytes on the wire");

  const decoded = decodePMC1(bytes);
  assert.equal(decoded.segment, "3181/442/1");
  assert.equal(decoded.speedBin, 7);
  assert.equal(decoded.qualityBin, 6);
  assert.equal(decoded.meters, 184);
  assert.equal(decoded.ttlSeconds, 90);
  assert.equal(decoded.timeBucket, 116951040);
});

test("§13.4 incident record encodes byte-identically", () => {
  const { bytes, preimage } = encodePMI1({
    epochPrefix8: PREFIX8,
    leafCell: 3181,
    geomRef: 885,
    ratioQ12: 2048,
    timeBucket: 116951040,
    type: 5,
    polarity: 1,
    ttlSeconds: 1800,
    reportId: sha256Utf8("incident").slice(0, 16),
    proofType: 3,
    proof: new Uint8Array(0)
  });
  assert.equal(
    toHex(bytes),
    "504d4931f44796c8cc1f3fa7ed18f50680108090e2370501880ed4191834714542dcf3e5d8a6ab386c9b0300"
  );
  assert.ok(preimage.length > 0);
  const decoded = decodePMI1(bytes);
  assert.equal(decoded.type, 5);
  assert.equal(decoded.polarity, 1);
  assert.equal(decoded.ratioQ12, 2048);
  assert.equal(decoded.ttlSeconds, 1800);
});


test("trailing bytes are a validation failure everywhere", () => {
  const { bytes } = encodePMC1({
    epochPrefix8: PREFIX8, leafCell: 1, geomRef: 2, timeBucket: 3, speedBin: 4,
    qualityBin: 5, meters: 6, ttlSeconds: 7, reportId: new Uint8Array(16),
    proofType: 0, proof: new Uint8Array(0)
  });
  const padded = Uint8Array.from([...bytes, 0]);
  assert.throws(() => decodePMC1(padded), /trailing/);
  const digest = encodePMD1({ epochPrefix8: PREFIX8, zoneX: 1, zoneY: 2, baseBucket: 3, entries: [] });
  assert.throws(() => decodePMD1(Uint8Array.from([...digest, 9])), /trailing/);
});

test("batch, digest, sync, and forward messages round-trip", () => {
  const one = encodePMC1({
    epochPrefix8: PREFIX8, leafCell: 10, geomRef: 21, timeBucket: 1000, speedBin: 8,
    qualityBin: 7, meters: 120, ttlSeconds: 90, reportId: sha256Utf8("r1").slice(0, 16),
    proofType: 0, proof: new Uint8Array(0)
  });
  const two = encodePMC1({
    epochPrefix8: PREFIX8, leafCell: 11, geomRef: 3, timeBucket: 1001, speedBin: 2,
    qualityBin: 6, meters: 80, ttlSeconds: 45, reportId: sha256Utf8("r2").slice(0, 16),
    proofType: 0, proof: new Uint8Array(0)
  });
  const decoded = decodePMB1(encodePMB1([one.bytes, two.bytes]));
  assert.equal(decoded.records.length, 2);
  assert.equal(decoded.records[0].segment, segmentString(10, 21));
  assert.equal(decoded.records[1].speedBin, 2);

  const digest = decodePMD1(encodePMD1({
    epochPrefix8: PREFIX8, zoneX: 144, zoneY: 179, baseBucket: 116951040,
    entries: [
      { localX: 8, localY: 63, count: 12, ageBuckets: 2, idFold: fromHex("0102030405060708") },
      { localX: 1, localY: 2, count: 3, ageBuckets: 0, idFold: fromHex("aabbccddeeff0011") }
    ]
  }));
  assert.equal(digest.entries.length, 2);
  assert.deepEqual([digest.entries[0].localX, digest.entries[0].localY], [1, 2], "entries sorted by (localX, localY)");

  const get = decodePMG1(encodePMG1({ epochPrefix8: PREFIX8, zoneX: 144, zoneY: 179 }));
  assert.equal(get.zoneX, 144);

  const query = decodePMQ1(encodePMQ1({
    epochPrefix8: PREFIX8,
    cells: Array.from({ length: 8 }, (_, i) => ({ x: 9000 + i, y: 11000 + i }))
  }));
  assert.equal(query.cells.length, 8);
  assert.throws(() => encodePMQ1({ epochPrefix8: PREFIX8, cells: [{ x: 1, y: 2 }] }), /exactly 8, 16, or 32/);

  const snapshot = decodePMS1(encodePMS1({
    epochPrefix8: PREFIX8,
    cells: [
      { x: 9000, y: 11000, records: [one.bytes], incidents: [] },
      { x: 9001, y: 11001, records: [], incidents: [] }
    ]
  }));
  assert.equal(snapshot.cells.length, 2);
  assert.equal(snapshot.cells[0].records.length, 1);
  assert.equal(snapshot.cells[1].records.length, 0, "empty cells return zero counts");

  const forward = decodePMF1(encodePMF1({ epochPrefix8: PREFIX8, delayMs: 4000, payload: encodePMB1([one.bytes]) }));
  assert.equal(forward.delayMs, 4000);
  assert.equal(decodeAny(forward.payload).kind, "batch");
});

test("unknown magics are ignored, not errors", () => {
  const reserved = Uint8Array.from([..."PMT1"].map(c => c.charCodeAt(0)));
  assert.equal(decodeAny(reserved).kind, "unknown");
  assert.equal(decodeAny(reserved).magic, "PMT1");
});

test("gossip message id is the first 20 bytes of the payload hash", () => {
  const payload = sha256Utf8("payload");
  assert.equal(gossipMessageId(payload).length, 20);
  assert.equal(toHex(gossipMessageId(payload)), toHex(sha256(payload)).slice(0, 40));
});

test("leading zero bits counts correctly", () => {
  assert.equal(leadingZeroBits(fromHex("0000c8ff")), 16, "0xc8 has its high bit set");
  assert.equal(leadingZeroBits(fromHex("80")), 0);
  assert.equal(leadingZeroBits(fromHex("01")), 7);
  assert.equal(leadingZeroBits(fromHex("0000")), 16);
});

test("§13.2 topic vector and rotation windows", () => {
  const cell = { x: 9256, y: 11515 };
  assert.equal(shardOfCell(cell), 5);
  const topic = topicForCell({
    epochPrefix16hex: "f44796c8cc1f3fa7",
    cell,
    window: 5847552
  });
  assert.equal(topic, "/rangefind/pulsemesh/1/f44796c8cc1f3fa7/144/179/5847552/5");
  const parsed = parseTopic(topic);
  assert.deepEqual(parsed, {
    reserved: false, epochPrefix16hex: "f44796c8cc1f3fa7", zoneX: 144, zoneY: 179, window: 5847552, shard: 5
  });
  assert.deepEqual(parseTopic("/rangefind/pulsemesh/1/t/f44796c8cc1f3fa7/abcd"), { reserved: true });
  assert.equal(parseTopic("/rangefind/pulsemesh/2/x"), null);

  const windowStart = 5847552 * 300 * 1000;
  assert.deepEqual(activeWindows(windowStart + 150 * 1000), [5847552], "mid-window: current only");
  assert.deepEqual(activeWindows(windowStart + 280 * 1000), [5847552, 5847553], "near rotation: next too");
  assert.deepEqual(activeWindows(windowStart + 10 * 1000), [5847552, 5847551], "just after rotation: previous too");
  assert.ok(windowAcceptable(5847551, windowStart + 1000) && windowAcceptable(5847553, windowStart + 1000));
  assert.ok(!windowAcceptable(5847549, windowStart + 1000));
});

test("bins: tables, speed bins, quality bins", () => {
  assert.equal(FRESHNESS.length, 91);
  assert.equal(FRESHNESS[0], 1000);
  assert.equal(FRESHNESS[5], 920);
  assert.equal(FRESHNESS[60], 368);
  assert.equal(FRESHNESS[90], 223);
  assert.deepEqual([...QUALITY_WEIGHT], [0, 250, 400, 550, 700, 800, 900, 1000]);
  assert.equal(speedBinFromKmh(0), 0);
  assert.equal(speedBinFromKmh(37.5), 7);
  assert.equal(speedBinFromKmh(221), 44, "bin 44 covers [220, 225)");
  assert.equal(speedBinFromKmh(225), null, "speeds beyond bin 44 are unrepresentable");
  assert.equal(qualityBinFromSnap(3, 0), 7);
  assert.equal(qualityBinFromSnap(18, 0), 4);
  assert.equal(qualityBinFromSnap(18, 45), 2, "30–60° heading delta costs two bins");
  assert.equal(qualityBinFromSnap(18, 90), 0, "over 60° suppresses");
  assert.equal(qualityBinFromSnap(60, 0), 0, "over 50 m suppresses");
});

test("segment string and wire forms convert both ways", () => {
  assert.equal(segmentString(3181, 885), "3181/442/1");
  assert.deepEqual(parseSegment("3181/442/1"), { leafCell: 3181, geomRef: 885 });
  assert.throws(() => parseSegment("3181/442"), /Bad segment/);
});

test("§13.6 admission bond encodes byte-identically", () => {
  const bytes = encodePMA1({
    epochPrefix8: PREFIX8, dayBucket: 20654, birthdayBits: 44,
    pairDifficulty: 0, salt: 0, i: 0xdeadbeef, j: 17
  });
  assert.equal(toHex(bytes), "504d4131f44796c8cc1f3fa7aea1012c0000deadbeef00000011");
  assert.equal(bytes.length, 26, "a bond is 26 bytes, presented once per session");
  const decoded = decodePMA1(bytes);
  assert.equal(decoded.dayBucket, 20654);
  assert.equal(decoded.birthdayBits, 44);
});
