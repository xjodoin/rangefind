// T1 — thread crypto, codecs and the link, against the §16 vectors.
//
// The property that matters most here is the last one: every subscriber
// holds the capability, so every subscriber can *seal* a well-formed
// record. Only the publisher can sign one. A record that decrypts but
// does not verify is an attack, not an error.

import assert from "node:assert/strict";
import test from "node:test";
import {
  base64UrlToBytes,
  bytesToBase64Url,
  deriveThreadKeys,
  generateThreadKeypair,
  openThreadBody,
  publicKeyFromSeed,
  sealThreadBody,
  signThread,
  threadRendezvous,
  threadTag,
  threadTopic,
  threadWindow,
  verifyThread
} from "../src/pulsemesh/thread_crypto.js";
import {
  STOP_OUTCOME,
  STOP_REASON,
  THREAD_MAX_BODY_BYTES,
  THREAD_MAX_RECORD_BYTES,
  THREAD_MODE,
  THREAD_STATE,
  THREAD_TRAVEL_MODE,
  decodeThreadBody,
  decodeThreadLink,
  decodeThreadRecord,
  decodeThreadRequest,
  decodeThreadResponse,
  encodeThreadBody,
  encodeThreadBodyPreimage,
  encodeThreadLink,
  encodeThreadRecord,
  encodeThreadRequest,
  encodeThreadResponse,
  parseThreadLinkUrl,
  threadLinkUrl,
  threadRecordAad
} from "../src/pulsemesh/thread_codec.js";
import { fromHex, sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const PUBLISHER_SEED = sha256Utf8("pulsemesh-thread-publisher");
const EPOCH32 = sha256Utf8("pulsemesh-test-vector");
const EPOCH_PREFIX8 = EPOCH32.subarray(0, 8);
const VECTOR_BODY = {
  unixSeconds: 1754265600,
  state: THREAD_STATE.EN_ROUTE,
  mode: THREAD_MODE.FINE,
  // §11 `mode` and §5.2 `travelMode` are different questions — what the
  // link reveals, and how the vehicle moves — so the vector gives them
  // different values on purpose.
  travelMode: THREAD_TRAVEL_MODE.CAR,
  leafCell: 3181,
  geomRef: 885,
  ratioQ12: 2048,
  speedBin: 7,
  stopIndex: 8,
  planRef: fromHex("10c2d3bff3368b2e"),
  outcomes: [],
  lastOutcome: null,
  note: new Uint8Array(0)
};

async function vectorFixture() {
  const publicKey = await publicKeyFromSeed(PUBLISHER_SEED);
  const keys = await deriveThreadKeys(publicKey);
  const tag = await threadTag(keys, EPOCH32, 5847552);
  return { publicKey, keys, tag };
}

test("§16.1 the whole key schedule derives from the capability alone", async () => {
  const publicKey = await publicKeyFromSeed(PUBLISHER_SEED);
  assert.equal(toHex(publicKey), "cdf0cfb422cd2faaf24eb8343cff641a41ca4be4a3aa429e7229d690719761bd");
  const keys = await deriveThreadKeys(publicKey);
  assert.equal(toHex(keys.topicKey), "0a3e8b5df4d51e0ce7306d79a309b78c35c21f460ca07cdd5f6cfd7f30453c4b");
  assert.equal(toHex(keys.contentKey), "39413c192f15a7068ac77abdf8a96eb9a769db3d226ed9731c7a201e88b8b3d7");
  assert.equal(toHex(keys.noncePrefix), "576cd8f8");
  await assert.rejects(() => deriveThreadKeys(new Uint8Array(31)), /32-byte/);
});

test("§16.2 topic tag and rendezvous key", async () => {
  const { keys } = await vectorFixture();
  assert.equal(threadWindow(1754265600 * 1000), 5847552);
  const tag = await threadTag(keys, EPOCH32, 5847552);
  assert.equal(toHex(tag), "cbfa3ae2fdc2cf0f");
  const topic = threadTopic(toHex(EPOCH32).slice(0, 16), tag);
  assert.equal(topic, "/rangefind/pulsemesh/1/t/f44796c8cc1f3fa7/cbfa3ae2fdc2cf0f");
  assert.equal(
    toHex(threadRendezvous(topic)),
    "a6213aeb6a36ac6d3380b05b6181124601c2234197377c6a6a086f68f8620849"
  );

  // Tags are unlinkable across rotations without K_topic: an observer
  // cannot follow a thread through the day by watching its address.
  const next = await threadTag(keys, EPOCH32, 5847553);
  assert.notEqual(toHex(next), toHex(tag));

  // And a tag is bound to its epoch, so a republished index re-addresses
  // every thread rather than leaving stale ones reachable.
  const otherEpoch = await threadTag(keys, sha256Utf8("other-epoch"), 5847552);
  assert.notEqual(toHex(otherEpoch), toHex(tag));
});

test("§16.3 the link is 45 bytes and lives in a URL fragment", async () => {
  const { publicKey } = await vectorFixture();
  const link = encodeThreadLink({ publicKey, epochPrefix8: EPOCH_PREFIX8, notAfter: 1754294400 });
  assert.equal(link.length, 45);
  assert.equal(bytesToBase64Url(link), "Ac3wz7QizS-q8k64NDz_ZBpBykvko6pCnnIp1pBxl2G99EeWyMwfP6dokGiA");
  const url = threadLinkUrl("https://track.example/r", link);
  assert.equal(url, "https://track.example/r#Ac3wz7QizS-q8k64NDz_ZBpBykvko6pCnnIp1pBxl2G99EeWyMwfP6dokGiA");
  // The capability must be after the '#': browsers never transmit a
  // fragment, so the page host never receives a tracking key.
  assert.ok(!url.slice(0, url.indexOf("#")).includes("Ac3wz7"));

  const parsed = parseThreadLinkUrl(url);
  assert.equal(toHex(parsed.publicKey), toHex(publicKey));
  assert.equal(toHex(parsed.epochPrefix8), toHex(EPOCH_PREFIX8));
  assert.equal(parsed.notAfter, 1754294400);
  assert.deepEqual(decodeThreadLink(link), parsed);
  assert.throws(() => decodeThreadLink(link.subarray(0, 44)), /45 bytes/);
  assert.throws(() => parseThreadLinkUrl("https://track.example/r"), /fragment/);
  // Version 1 is the §16.3 vector and means "the key in this link signs
  // its own records". Version 2 is §21's delegated form, which a
  // subscriber must treat differently — it says nothing about the bytes
  // here, since the vector is a one-off run and stays version 1.
  assert.equal(decodeThreadLink(link).delegated, false);
  const delegated = Uint8Array.from(link);
  delegated[0] = 2;
  assert.equal(decodeThreadLink(delegated).delegated, true);
  const wrongVersion = Uint8Array.from(link);
  wrongVersion[0] = 3;
  assert.throws(() => decodeThreadLink(wrongVersion), /version/);
});

test("§16.4 signed preimage and sealed record are byte-identical", async () => {
  const { publicKey, keys, tag } = await vectorFixture();
  const preimage = encodeThreadBodyPreimage(VECTOR_BODY);
  assert.equal(toHex(preimage), "504d545080f0bfc406020201ed18f5068010070810c2d3bff3368b2e000000");
  assert.equal(preimage.length, 31);

  const signature = await signThread(preimage, PUBLISHER_SEED);
  assert.equal(
    toHex(signature),
    "ab698bf41fc97ba4555c3a5a0ae790d039273eb0674f6be0ad61140d41edfe9f" +
    "0498a4b4f44a24fefe0b99eaf8953472ebd8b52fc2f89b1a4205943987e30f06"
  );
  const body = encodeThreadBody(VECTOR_BODY, signature);
  assert.equal(body.length, 95);

  const aad = threadRecordAad(EPOCH_PREFIX8, tag, 42);
  assert.equal(toHex(aad), "504d5431f44796c8cc1f3fa7cbfa3ae2fdc2cf0f2a");
  const ciphertext = await sealThreadBody(keys, 42, aad, body);
  const record = encodeThreadRecord({ epochPrefix8: EPOCH_PREFIX8, tag, seq: 42, ciphertext });
  assert.equal(record.bytes.length, 133);
  assert.equal(
    toHex(record.bytes),
    "504d5431f44796c8cc1f3fa7cbfa3ae2fdc2cf0f2a6f7a8da4d85d9063cc5eb7" +
    "1712716a95268c73bc86a24089e44a7a7d62c928a32cbe9fd21d6a288405cb8f" +
    "ea6ddf8df6554e6a47f074ca1de7227eae05ceeeeb8fc01365a1e9bab637909e" +
    "3842d6f1f0e84ea37c031daf6067fc919866eadfaf2e888b8ce54447d1e2bdf7" +
    "5792b13843"
  );

  // Round trip: open, decode, verify.
  const decoded = decodeThreadRecord(record.bytes);
  assert.equal(decoded.seq, 42);
  const opened = await openThreadBody(keys, decoded.seq, decoded.aad, decoded.ciphertext);
  assert.ok(opened, "sealed body opens with the derived content key");
  const parsed = decodeThreadBody(opened);
  assert.equal(parsed.segment, "3181/442/1");
  assert.equal(parsed.ratio, 0.5);
  assert.equal(parsed.stopIndex, 8);
  assert.equal(parsed.state, THREAD_STATE.EN_ROUTE);
  assert.equal(parsed.travelMode, THREAD_TRAVEL_MODE.CAR);
  assert.deepEqual(parsed.outcomes, []);
  assert.equal(parsed.lastOutcome, null);
  assert.ok(await verifyThread(parsed.preimage, parsed.signature, publicKey));

  // Nothing on the wire reveals the capability (§4.1): P must not appear
  // in the envelope, the ciphertext, or anywhere else transmitted.
  assert.ok(!toHex(record.bytes).includes(toHex(publicKey)));
});

test("a tampered record fails to open, and a tampered AAD fails with it", async () => {
  const { keys, tag } = await vectorFixture();
  const signature = await signThread(encodeThreadBodyPreimage(VECTOR_BODY), PUBLISHER_SEED);
  const body = encodeThreadBody(VECTOR_BODY, signature);
  const aad = threadRecordAad(EPOCH_PREFIX8, tag, 42);
  const ciphertext = await sealThreadBody(keys, 42, aad, body);

  const flipped = Uint8Array.from(ciphertext);
  flipped[10] ^= 0x01;
  assert.equal(await openThreadBody(keys, 42, aad, flipped), null, "a flipped ciphertext bit fails the AEAD tag");

  // AAD binds the record to its epoch, tag and seq, so a record cannot be
  // lifted into another thread, window, or sequence position.
  const otherSeqAad = threadRecordAad(EPOCH_PREFIX8, tag, 43);
  assert.equal(await openThreadBody(keys, 42, otherSeqAad, ciphertext), null, "AAD seq mismatch fails");
  const otherTagAad = threadRecordAad(EPOCH_PREFIX8, new Uint8Array(8), 42);
  assert.equal(await openThreadBody(keys, 42, otherTagAad, ciphertext), null, "AAD tag mismatch fails");
  assert.equal(await openThreadBody(keys, 43, aad, ciphertext), null, "a different nonce fails");

  // A different capability derives different keys and opens nothing.
  const stranger = await deriveThreadKeys((await generateThreadKeypair()).publicKey);
  assert.equal(await openThreadBody(stranger, 42, aad, ciphertext), null, "another thread's key opens nothing");
});

test("a link holder can seal a record but cannot sign one", async () => {
  // This is the load-bearing asymmetry. Every subscriber holds the
  // capability, so every subscriber has K_content and can produce a
  // perfectly well-formed sealed record. Only the private key can sign,
  // so the forgery is caught at verification — not at decryption.
  const { publicKey, keys, tag } = await vectorFixture();
  const forgedBody = { ...VECTOR_BODY, stopIndex: 12, ratioQ12: 4000 };
  const preimage = encodeThreadBodyPreimage(forgedBody);

  // The forger signs with a key they generated themselves — the best
  // they can do without the publisher's private key.
  const forger = await generateThreadKeypair();
  const forgedSignature = await signThread(preimage, forger.privateSeed);
  const body = encodeThreadBody(forgedBody, forgedSignature);
  const aad = threadRecordAad(EPOCH_PREFIX8, tag, 99);
  const ciphertext = await sealThreadBody(keys, 99, aad, body);
  const record = encodeThreadRecord({ epochPrefix8: EPOCH_PREFIX8, tag, seq: 99, ciphertext });

  // It decrypts perfectly — the capability is enough for that.
  const decoded = decodeThreadRecord(record.bytes);
  const opened = await openThreadBody(keys, decoded.seq, decoded.aad, decoded.ciphertext);
  assert.ok(opened, "a link holder really can produce a record that opens");
  const parsed = decodeThreadBody(opened);
  assert.equal(parsed.stopIndex, 12, "and it carries whatever they claimed");

  // And it fails the only check that matters.
  assert.equal(
    await verifyThread(parsed.preimage, parsed.signature, publicKey),
    false,
    "a record that decrypts but does not verify is an attack, not an error"
  );
  // Signed with the right key, the same bytes verify — so the failure is
  // the signature, not the encoding.
  const honest = await signThread(preimage, PUBLISHER_SEED);
  assert.ok(await verifyThread(preimage, honest, publicKey));
});

test("catch-up messages: padded requests, indistinguishable empty answers", async () => {
  const tags = Array.from({ length: 8 }, (_, i) => Uint8Array.from({ length: 8 }, () => i + 1));
  const request = encodeThreadRequest({
    epochPrefix8: EPOCH_PREFIX8,
    entries: tags.map((tag, i) => ({ tag, sinceSeq: i }))
  });
  const decodedRequest = decodeThreadRequest(request);
  assert.equal(decodedRequest.entries.length, 8);
  assert.equal(decodedRequest.entries[3].sinceSeq, 3);
  const oneTag = tags[0];
  for (const size of [0, 1, 2, 5, 9, 32]) {
    assert.throws(
      () => encodeThreadRequest({
        epochPrefix8: EPOCH_PREFIX8,
        entries: Array.from({ length: size }, () => ({ tag: oneTag, sinceSeq: 0 }))
      }),
      /exactly 4, 8, or 16/,
      `a ${size}-tag request is not a legal padded size`
    );
  }

  const { keys, tag } = await vectorFixture();
  const signature = await signThread(encodeThreadBodyPreimage(VECTOR_BODY), PUBLISHER_SEED);
  const body = encodeThreadBody(VECTOR_BODY, signature);
  const aad = threadRecordAad(EPOCH_PREFIX8, tag, 7);
  const ciphertext = await sealThreadBody(keys, 7, aad, body);
  const record = encodeThreadRecord({ epochPrefix8: EPOCH_PREFIX8, tag, seq: 7, ciphertext });

  const response = encodeThreadResponse({
    epochPrefix8: EPOCH_PREFIX8,
    entries: [
      { tag, records: [record.bytes] },
      { tag: tags[0], records: [] }
    ]
  });
  const decodedResponse = decodeThreadResponse(response);
  assert.equal(decodedResponse.entries.length, 2);
  assert.equal(decodedResponse.entries[0].records.length, 1);
  assert.equal(decodedResponse.entries[0].records[0].seq, 7);
  assert.equal(decodedResponse.entries[1].records.length, 0,
    "an unheld tag is answered exactly like a held-but-empty one");
  // Records travel verbatim, so signatures and AEAD tags survive a relay
  // that cannot open them.
  assert.equal(toHex(decodedResponse.entries[0].records[0].bytes), toHex(record.bytes));
});

test("codecs reject trailing bytes and malformed bodies", async () => {
  const { keys, tag } = await vectorFixture();
  const signature = await signThread(encodeThreadBodyPreimage(VECTOR_BODY), PUBLISHER_SEED);
  const body = encodeThreadBody(VECTOR_BODY, signature);
  const aad = threadRecordAad(EPOCH_PREFIX8, tag, 1);
  const ciphertext = await sealThreadBody(keys, 1, aad, body);
  const record = encodeThreadRecord({ epochPrefix8: EPOCH_PREFIX8, tag, seq: 1, ciphertext });

  assert.throws(() => decodeThreadRecord(Uint8Array.from([...record.bytes, 0])), /trailing/);
  assert.throws(() => decodeThreadBody(Uint8Array.from([...body, 0])), /trailing/);
  assert.throws(() => decodeThreadBody(body.subarray(0, 10)), /truncated/);
  assert.throws(
    () => encodeThreadBody({ ...VECTOR_BODY, note: new Uint8Array(65) }, signature),
    /64 bytes/
  );
  assert.throws(() => encodeThreadBody(VECTOR_BODY, new Uint8Array(63)), /64 bytes/);
  assert.throws(
    () => encodeThreadRecord({ epochPrefix8: EPOCH_PREFIX8, tag: new Uint8Array(7), seq: 1, ciphertext }),
    /8 bytes/
  );
});

test("a note round-trips and coarse mode withholds position", async () => {
  const note = new TextEncoder().encode("vehicle swapped");
  const coarse = {
    ...VECTOR_BODY,
    mode: THREAD_MODE.COARSE,
    state: THREAD_STATE.DWELLING,
    leafCell: 0,          // §11: coarse withholds position entirely
    geomRef: 0,
    ratioQ12: 0,
    note
  };
  const signature = await signThread(encodeThreadBodyPreimage(coarse), PUBLISHER_SEED);
  const parsed = decodeThreadBody(encodeThreadBody(coarse, signature));
  assert.equal(parsed.segment, null, "no position in a coarse record");
  assert.equal(new TextDecoder().decode(parsed.note), "vehicle swapped");
  assert.equal(parsed.stopIndex, 8, "a coarse record still says which stop");
});

test("leaf 0 carries a real position, and only 0/0/0 means withheld", async () => {
  // §5.2 words the sentinel as `leafCell = 0`, but leaf 0 is an ordinary
  // leaf — a vehicle really can be on segment "0/95/1". Reading the
  // sentinel off leafCell alone silently discards the position of
  // everything inside one leaf of the map.
  const onLeafZero = {
    ...VECTOR_BODY,
    leafCell: 0,
    geomRef: 191,   // polyline 95, direction 1
    ratioQ12: 1200
  };
  const signed = encodeThreadBody(onLeafZero, await signThread(encodeThreadBodyPreimage(onLeafZero), PUBLISHER_SEED));
  const parsed = decodeThreadBody(signed);
  assert.equal(parsed.segment, "0/95/1", "a vehicle in leaf 0 keeps its position");
  assert.ok(Math.abs(parsed.ratio - 1200 / 4096) < 1e-9);

  // The whole triple being zero is what "withheld" means.
  const withheld = { ...VECTOR_BODY, leafCell: 0, geomRef: 0, ratioQ12: 0 };
  const withheldSigned = encodeThreadBody(
    withheld,
    await signThread(encodeThreadBodyPreimage(withheld), PUBLISHER_SEED)
  );
  assert.equal(decodeThreadBody(withheldSigned).segment, null);

  // And the one colliding real position — leaf 0, polyline 0, direction
  // 0, exactly at the start — is nudged one quantum by the publisher so
  // it is never mistaken for a withheld record.
  const { createThreadPublisher } = await import("../src/pulsemesh/thread_publish.js");
  const { generateThreadKeypair: freshKeypair } = await import("../src/pulsemesh/thread_crypto.js");
  const { decodeThreadRecord: decodeRecord } = await import("../src/pulsemesh/thread_codec.js");
  const { openThreadBody: open } = await import("../src/pulsemesh/thread_crypto.js");
  const keypair = await freshKeypair();
  const now = Math.floor(Date.now() / 1000) * 1000;
  const emitted = [];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed,
    epoch32: EPOCH32,
    mode: 2,
    clock: () => now,
    publish: async record => emitted.push(record)
  });
  await publisher.handleFix({
    lat: 45.5, lon: -76.5, speedMps: 10, nowMillis: now,
    match: { segment: "0/0/0", ratio: 0 }
  });
  const record = decodeRecord(emitted[0].bytes);
  const body = decodeThreadBody(await open(publisher.keys, record.seq, record.aad, record.ciphertext));
  assert.equal(body.segment, "0/0/0", "the sentinel collision still reports a position");
  assert.equal(body.ratioQ12, 1, "nudged one quantum — about 10 cm — off the sentinel");
});

test("a 200-stop outcome map, travelMode and lastOutcome survive the wire", async () => {
  const { publicKey, keys, tag } = await vectorFixture();
  // A full delivery day. Two bits a stop, packed LSB-first, so the whole
  // day's outcomes cost 50 bytes — and they are in *every* record, because
  // the gossip channel is lossy and a follower who slept through the
  // record where stop 7 was skipped has no other way to learn it.
  const outcomes = Array.from({ length: 200 }, (_, i) => i % 4);
  const body = {
    ...VECTOR_BODY,
    travelMode: THREAD_TRAVEL_MODE.BIKE,
    stopIndex: 137,
    outcomes,
    lastOutcome: {
      stopIndex: 137,
      outcome: STOP_OUTCOME.SKIPPED,
      reasonCode: STOP_REASON.CUSTOMER_ABSENT
    }
  };
  const preimage = encodeThreadBodyPreimage(body);
  const signature = await signThread(preimage, PUBLISHER_SEED);
  const sealed = encodeThreadBody(body, signature);
  const parsed = decodeThreadBody(sealed);

  assert.equal(parsed.travelMode, THREAD_TRAVEL_MODE.BIKE);
  assert.deepEqual(parsed.outcomes, outcomes, "200 stops, two bits each, LSB-first");
  assert.equal(parsed.outcomes.length, 200);
  assert.deepEqual(parsed.lastOutcome, {
    stopIndex: 137,
    outcome: STOP_OUTCOME.SKIPPED,
    reasonCode: STOP_REASON.CUSTOMER_ABSENT,
    // §20.7: a mark always says whether it carries a photo, and this one
    // does not. One byte, and it is what makes a proof's *existence* part
    // of the signed preimage rather than an optional trailer. Which proof
    // is the accumulator's job (§20.7.1).
    hasPhoto: false
  });
  assert.equal(parsed.photoChain, null, "a run that has taken no photo carries no accumulator");
  // 50 bytes for the day: the whole point of the packed map.
  assert.equal(preimage.length - encodeThreadBodyPreimage(VECTOR_BODY).length, 50 + 2 + 4 + 1,
    "50 map bytes, a 2-byte count, a 4-byte lastOutcome, and a wider stopIndex");

  // The signature covers the new fields, so an outcome cannot be edited
  // by anyone holding only the link.
  assert.ok(await verifyThread(parsed.preimage, parsed.signature, publicKey));
  const forged = { ...body, outcomes: outcomes.map(() => STOP_OUTCOME.DELIVERED) };
  assert.equal(
    await verifyThread(encodeThreadBodyPreimage(forged), signature, publicKey),
    false,
    "flipping the day to all-delivered breaks the signature"
  );

  // And it still fits a PMT1 record with room to spare.
  const aad = threadRecordAad(EPOCH_PREFIX8, tag, 42);
  const record = encodeThreadRecord({
    epochPrefix8: EPOCH_PREFIX8, tag, seq: 42,
    ciphertext: await sealThreadBody(keys, 42, aad, sealed)
  });
  assert.ok(record.bytes.length <= THREAD_MAX_RECORD_BYTES,
    `a 200-stop day is ${record.bytes.length} bytes, inside the ${THREAD_MAX_RECORD_BYTES}-byte limit`);

  // Trailing bytes are still refused, with the new fields in the body.
  assert.throws(() => decodeThreadBody(Uint8Array.from([...sealed, 0])), /trailing/);
  assert.throws(() => decodeThreadBody(sealed.subarray(0, sealed.length - 1)), /truncated|trailing/);
});

test("a plan too large to encode is refused, with the number that fits", () => {
  // The body is signed and sealed before it is ever framed, so a record
  // that overflows 256 has already been signed: the refusal has to happen
  // at encode, and it has to say what would have fitted.
  // The published caps are the worst case: the widest position varints a
  // planet-scale leaf index can produce, and the widest stopIndex. A body
  // with smaller ones fits a few more stops, and the error quotes *that*
  // body's number rather than one universal figure.
  const worst = {
    ...VECTOR_BODY,
    // The widest a planet-scale leaf index makes them…
    leafCell: 268435455, geomRef: 268435455, ratioQ12: 4095,
    // …and the widest a stop index can be, since a stop index is bounded
    // by the stop count and no plan reaches 16384 stops.
    stopIndex: 999,
    lastOutcome: { stopIndex: 999, outcome: STOP_OUTCOME.FAILED, reasonCode: STOP_REASON.OTHER }
  };
  const noted = { ...worst, note: new Uint8Array(64) };

  assert.ok(encodeThreadBodyPreimage({ ...noted, outcomes: new Array(172).fill(1) }).length + 64
    <= THREAD_MAX_BODY_BYTES, "172 stops fit alongside a full 64-byte note");
  assert.throws(
    () => encodeThreadBodyPreimage({ ...noted, outcomes: new Array(600).fill(1) }),
    /600 plan stops cost 150 bytes of outcome map and this note costs 64.*at most 172 stops/su
  );

  // Without a note the same body carries a far longer day.
  assert.ok(encodeThreadBodyPreimage({ ...worst, outcomes: new Array(428).fill(1) }).length + 64
    <= THREAD_MAX_BODY_BYTES);
  assert.throws(
    () => encodeThreadBodyPreimage({ ...worst, outcomes: new Array(600).fill(1) }),
    /at most 428 stops/su
  );

  // The realistic case the feature is sized for: a 200-stop day still
  // leaves 57 bytes of note even at the worst position width.
  assert.ok(encodeThreadBodyPreimage({
    ...worst, outcomes: new Array(200).fill(1), note: new Uint8Array(57)
  }).length + 64 <= THREAD_MAX_BODY_BYTES);
  assert.throws(
    () => encodeThreadBodyPreimage({
      ...worst, outcomes: new Array(200).fill(1), note: new Uint8Array(58)
    }),
    /allows 213/su
  );

  // §20.7.1: the photo accumulator is 32 of those bytes, and it comes out
  // of the same budget the single commitment used to. The same 200-stop
  // day with photos leaves 25 — the number field 19 left, for a field
  // that binds every commitment rather than the newest.
  const withPhoto = {
    ...worst,
    lastOutcome: { ...worst.lastOutcome, hasPhoto: true },
    photoChain: "d3".repeat(32),
    outcomes: new Array(200).fill(1)
  };
  assert.ok(encodeThreadBodyPreimage({ ...withPhoto, note: new Uint8Array(25) }).length + 64
    <= THREAD_MAX_BODY_BYTES);
  assert.throws(
    () => encodeThreadBodyPreimage({ ...withPhoto, note: new Uint8Array(26) }),
    /allows 213/su
  );
  assert.throws(
    () => encodeThreadBodyPreimage({ ...withPhoto, note: new Uint8Array(64), outcomes: new Array(600).fill(1) }),
    /at most 48 stops/su,
    "a full note and a photo together leave very little day"
  );
});

test("base64url round-trips arbitrary bytes", () => {
  for (const length of [0, 1, 2, 3, 45, 64]) {
    const bytes = Uint8Array.from({ length }, (_, i) => (i * 37) & 0xff);
    assert.deepEqual(base64UrlToBytes(bytesToBase64Url(bytes)), bytes);
  }
  assert.ok(!bytesToBase64Url(Uint8Array.from({ length: 45 }, (_, i) => i)).match(/[+/=]/),
    "URL-safe alphabet only, so a link survives a fragment intact");
});
