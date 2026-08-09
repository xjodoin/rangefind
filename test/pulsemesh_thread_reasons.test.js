// T13 — the dead zone (threads §5.2.1, §20.7).
//
// A driver marks a stop out of coverage. Nothing re-sends a thread
// record and §5.5 catch-up reaches back only THREAD_MAX_AGE, so the
// record itself is gone for good. The design already survives that for
// *outcomes*, because the outcome map is cumulative in every record —
// but `lastOutcome` holds one mark, and the next mark replaces it, so
// the reason and the photo commitment of everything published into the
// dead zone used to vanish permanently.
//
// The headline test here is exactly that outage: two stops marked with
// reasons and photos while the transport is dropping every record, then
// a reconnection, then a subscriber that heard none of it recovering
// both stops' outcomes *and* both reasons from the first record after.

import assert from "node:assert/strict";
import test from "node:test";
import { createThreadPublisher } from "../src/pulsemesh/thread_publish.js";
import { createThreadSubscriber } from "../src/pulsemesh/thread_consume.js";
import { estimateArrival } from "../src/pulsemesh/thread_eta.js";
import {
  STOP_OUTCOME,
  STOP_REASON,
  THREAD_MARK_FLAG,
  THREAD_MAX_BODY_BYTES,
  THREAD_MAX_RECORD_BYTES,
  THREAD_MAX_REASONS,
  THREAD_MODE,
  decodeThreadBody,
  decodeThreadLink,
  encodeThreadBody,
  encodeThreadBodyPreimage,
  encodeThreadLink,
  fitStopReasons,
  stopReasonFor
} from "../src/pulsemesh/thread_codec.js";
import { generateThreadKeypair, signThread } from "../src/pulsemesh/thread_crypto.js";
import { sha256Utf8 } from "../src/pulsemesh/sha256.js";

const EPOCH32 = sha256Utf8("pulsemesh-thread-reasons");
const EPOCH_PREFIX8 = EPOCH32.subarray(0, 8);
const PLAN_REF = sha256Utf8("pulsemesh-thread-reasons-plan").subarray(0, 8);

/** A stand-in for a JPEG: nothing in this path looks inside one. */
function fakeJpeg(salt) {
  return Uint8Array.from({ length: 512 }, (_, i) => (i * 31 + salt * 7) & 0xff);
}

function plan(stopCount = 8) {
  return {
    planRef: PLAN_REF,
    dwellSeconds: 30,
    stops: Array.from({ length: stopCount }, (_, i) => ({
      index: i + 1, lat: 45.5 + i * 0.001, lon: -73.6 + i * 0.001
    }))
  };
}

/**
 * A publisher whose transport can be cut, and a subscriber that only
 * ever sees what the transport actually carried.
 */
async function fleet({ stopCount = 8, mode = THREAD_MODE.FINE } = {}) {
  const nowSeconds = Math.floor(Date.now() / 1000);
  let now = nowSeconds * 1000;
  const clock = () => now;
  const keypair = await generateThreadKeypair();
  const wire = [];
  let connected = true;
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed,
    epoch32: EPOCH32,
    mode,
    plan: plan(stopCount),
    clock,
    publish: async record => {
      // A dead zone is not an error the publisher can see: the record is
      // sealed, signed, handed to the radio, and never reaches anyone.
      if (connected) wire.push(record);
    }
  });
  const link = decodeThreadLink(encodeThreadLink({
    publicKey: publisher.publicKey, epochPrefix8: EPOCH_PREFIX8, notAfter: nowSeconds + 7200
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32: EPOCH32, clock });
  return {
    publisher,
    subscriber,
    link,
    wire,
    drop: () => { connected = false; },
    restore: () => { connected = true; },
    advance: seconds => { now += seconds * 1000; },
    /** Everything the transport carried, through the subscriber's §7 checks. */
    async deliverAll() {
      const verdicts = [];
      while (wire.length) verdicts.push(await subscriber.accept(wire.shift().bytes));
      return verdicts;
    }
  };
}

test("two stops marked in a dead zone come back with their reasons", async () => {
  const run = await fleet();

  // The morning goes normally and the dispatcher's board is current.
  await run.publisher.markStop(1, STOP_OUTCOME.DELIVERED);
  assert.ok((await run.deliverAll()).every(verdict => verdict.ok));

  // Into the dead zone. Two stops are marked, each with a reason and a
  // proof photo, and not one record reaches anybody.
  run.drop();
  await run.publisher.markStop(2, STOP_OUTCOME.SKIPPED, {
    reason: STOP_REASON.CUSTOMER_ABSENT, photo: fakeJpeg(2)
  });
  run.advance(20);
  await run.publisher.markStop(3, STOP_OUTCOME.FAILED, {
    reason: STOP_REASON.PARCEL_MISSING, photo: fakeJpeg(3)
  });
  run.advance(20);
  assert.equal(run.wire.length, 0, "the dead zone swallowed both marks");
  assert.equal(run.subscriber.stats.accepted, 1, "the board still shows the morning");

  // Coverage comes back. The very next record — one ordinary fix, not a
  // replay of anything — has to carry the whole day.
  run.restore();
  await run.publisher.handleFix({
    lat: 45.503, lon: -73.603, speedMps: 8, match: { segment: "3181/885/1", ratio: 0.4 }
  });
  const verdicts = await run.deliverAll();
  assert.equal(verdicts.length, 1, "one record, and it is not a retransmission");
  assert.ok(verdicts[0].ok);

  const status = run.subscriber.status();
  // Outcomes already survived this, from the cumulative map.
  assert.equal(status.outcomes[1], STOP_OUTCOME.SKIPPED);
  assert.equal(status.outcomes[2], STOP_OUTCOME.FAILED);
  // The reasons are the part that used to be gone for good.
  const update = run.subscriber.latest();
  assert.deepEqual(stopReasonFor(update, 2), {
    outcome: STOP_OUTCOME.SKIPPED,
    reasonCode: STOP_REASON.CUSTOMER_ABSENT,
    reasonKnown: true,
    hasPhoto: true
  }, "stop 2's reason survived being superseded by stop 3's mark");
  assert.deepEqual(stopReasonFor(update, 3), {
    outcome: STOP_OUTCOME.FAILED,
    reasonCode: STOP_REASON.PARCEL_MISSING,
    reasonKnown: true,
    hasPhoto: true
  });

  // `lastOutcome` is still one mark and still the most recent one — the
  // list did not replace it, it stopped it being the only copy.
  assert.equal(update.lastOutcome.stopIndex, 3);
  assert.equal(update.lastOutcome.photoHash?.length, 64);

  // §20.7: the commitments themselves did not survive, and the record
  // says so rather than letting the board believe it has everything.
  assert.equal(update.photoCount, 2, "the run says it has published two commitments");
  assert.equal(update.stopReasons.filter(entry => entry.photo).length, 2,
    "and names the two stops they belong to");
  const held = new Set([update.lastOutcome.photoHash]);
  assert.equal(update.photoCount - held.size, 1,
    "one proof of delivery is countably missing, which is the honest answer");

  // A record with two carried reasons and a photo count is a handful of
  // bytes bigger than one with none, not a different protocol.
  assert.ok(run.subscriber.latest(), "and it is an ordinary 256-byte record");
});

test("a delivered stop, then a skipped one, then the reason for both", async () => {
  // The ETA path is where a customer actually meets this: their own stop
  // was refused hours ago and `lastOutcome` has moved on many times.
  const run = await fleet();
  await run.publisher.markStop(2, STOP_OUTCOME.SKIPPED, { reason: STOP_REASON.REFUSED });
  await run.publisher.markStop(1, STOP_OUTCOME.DELIVERED);
  await run.publisher.markStop(4, STOP_OUTCOME.FAILED, { reason: STOP_REASON.INACCESSIBLE });
  await run.deliverAll();

  const update = run.subscriber.latest();
  const mine = await estimateArrival({
    engine: null, update, plan: plan(), myStopIndex: 2, nowMillis: update.unixSeconds * 1000
  });
  assert.equal(mine.outcomeName, "skipped");
  assert.equal(mine.arrivalMillis, null, "a refused stop has no arrival time at all");
  assert.equal(mine.reasonCode, STOP_REASON.REFUSED,
    "three marks later, the customer is still told why");
  assert.equal(mine.reasonKnown, true);
  assert.equal(mine.hasPhoto, false);
});

test("skipped for no stated reason is not the same as skipped and never learned why", async () => {
  const run = await fleet();
  // Marked, with nothing said. That is a complete answer.
  await run.publisher.markStop(2, STOP_OUTCOME.SKIPPED);
  await run.deliverAll();
  const stated = run.subscriber.latest();
  assert.deepEqual(stopReasonFor(stated, 2), {
    outcome: STOP_OUTCOME.SKIPPED, reasonCode: STOP_REASON.NONE, reasonKnown: true, hasPhoto: false
  }, "reason NONE is carried explicitly, so it can be read as an answer");

  // The same outcome map, with the reason list stripped — what a record
  // looks like once the cap has evicted this stop, or when it came from
  // a run that predates the list at all.
  const stripped = { ...stated, stopReasons: [], lastOutcome: { ...stated.lastOutcome, stopIndex: 7 } };
  assert.deepEqual(stopReasonFor(stripped, 2), {
    outcome: STOP_OUTCOME.SKIPPED, reasonCode: null, reasonKnown: false, hasPhoto: null
  }, "missing, and it says missing rather than reporting a cheerful zero");

  // The distinction reaches the customer, not just the codec.
  const lost = await estimateArrival({
    engine: null, update: stripped, plan: plan(), myStopIndex: 2, nowMillis: stated.unixSeconds * 1000
  });
  assert.equal(lost.reasonCode, null);
  assert.equal(lost.reasonKnown, false);
  assert.equal(lost.hasPhoto, null, "a proof may well exist; false would be a claim we cannot make");

  // Delivered and pending stops have no reason to be missing.
  assert.equal(stopReasonFor(stated, 1).reasonKnown, true);
  assert.equal(stopReasonFor(stated, 1).reasonCode, null);
});

test("a normal day costs exactly what it costs today", async () => {
  // The whole feature must be free for the run that never fails a stop.
  // Field 14 was a `lastPresent` boolean and is now a flag byte whose
  // bit 0 is that boolean, so a record with nothing new to say is
  // byte-identical — not merely the same length.
  const body = {
    unixSeconds: 1754265600,
    state: 2, mode: THREAD_MODE.FINE, travelMode: 1,
    leafCell: 3181, geomRef: 885, ratioQ12: 2048, speedBin: 7,
    stopIndex: 8, planRef: PLAN_REF,
    outcomes: new Array(200).fill(STOP_OUTCOME.DELIVERED),
    lastOutcome: { stopIndex: 8, outcome: STOP_OUTCOME.DELIVERED, reasonCode: STOP_REASON.NONE },
    note: new Uint8Array(0)
  };
  const today = encodeThreadBodyPreimage(body);
  for (const extra of [{}, { stopReasons: [] }, { photoCount: 0 }, { stopReasons: [], photoCount: 0 }]) {
    assert.deepEqual(
      [...encodeThreadBodyPreimage({ ...body, ...extra })], [...today],
      "an all-delivered day encodes to the bytes it always did"
    );
  }
  assert.equal(today[today.indexOf(0x01, 30)], THREAD_MARK_FLAG.LAST);

  // And a run with nothing marked at all is unchanged too.
  const quiet = { ...body, outcomes: [], lastOutcome: null };
  assert.equal(encodeThreadBodyPreimage(quiet).length, 31, "the §16.4 vector's length, unmoved");

  // What the outage day pays: a count, two pairs, and one byte of photo
  // counter. Six bytes to make the most valuable record of the day
  // survive being published into a dead zone.
  const carrying = encodeThreadBodyPreimage({
    ...body,
    stopReasons: [
      { stopIndex: 2, reasonCode: STOP_REASON.CUSTOMER_ABSENT, photo: true },
      { stopIndex: 3, reasonCode: STOP_REASON.PARCEL_MISSING, photo: true }
    ],
    photoCount: 2
  });
  assert.equal(carrying.length - today.length, 6);
});

test("the cap keeps the newest reasons and the record keeps fitting", async () => {
  // A pathological day: every stop fails. The list is bounded twice —
  // by THREAD_MAX_REASONS and by whatever is left of the 213-byte body —
  // and it is *bounded* rather than refused, because refusing to encode
  // mid-day would take the outcome map and the position down with the
  // reason it was protecting.
  const run = await fleet({ stopCount: 60 });
  for (let index = 1; index <= 60; index++) {
    await run.publisher.markStop(index, STOP_OUTCOME.FAILED, { reason: STOP_REASON.INACCESSIBLE });
    run.advance(1);
  }
  await run.deliverAll();

  const update = run.subscriber.latest();
  assert.equal(update.stopReasons.length, THREAD_MAX_REASONS, "capped, not refused");
  assert.deepEqual(
    update.stopReasons.map(entry => entry.stopIndex),
    Array.from({ length: THREAD_MAX_REASONS }, (_, i) => 60 - THREAD_MAX_REASONS + 1 + i),
    "the newest marks are what every record re-states — a listener already had the old ones"
  );
  // The oldest are gone, and gone visibly.
  assert.equal(stopReasonFor(update, 1).reasonKnown, false);
  assert.equal(stopReasonFor(update, 1).outcome, STOP_OUTCOME.FAILED,
    "the outcome map is untouched by the cap: 60 stops failed and all 60 say so");
  assert.equal(stopReasonFor(update, 60).reasonKnown, true);
  assert.equal(update.outcomes.filter(outcome => outcome === STOP_OUTCOME.FAILED).length, 60);

  // Every record of that day was still a legal PMT1.
  assert.ok(run.publisher.stats.published >= 60);

  // And the budget, not just the count, is a bound. A body with no room
  // left drops reasons rather than overflowing.
  const tight = {
    unixSeconds: 1754265600, state: 2, mode: 2, travelMode: 1,
    leafCell: 268435455, geomRef: 268435455, ratioQ12: 4095, speedBin: 7,
    stopIndex: 999, planRef: PLAN_REF,
    outcomes: new Array(200).fill(STOP_OUTCOME.FAILED),
    lastOutcome: { stopIndex: 199, outcome: STOP_OUTCOME.FAILED, reasonCode: 1 },
    stopReasons: Array.from({ length: THREAD_MAX_REASONS }, (_, i) => ({
      stopIndex: 150 + i, reasonCode: STOP_REASON.OTHER
    })),
    photoCount: 3,
    note: new Uint8Array(0)
  };
  assert.equal(fitStopReasons(tight).length, THREAD_MAX_REASONS, "16 fit a 200-stop day with no note");
  const noted = { ...tight, note: new Uint8Array(40) };
  const kept = fitStopReasons(noted);
  assert.ok(kept.length < THREAD_MAX_REASONS && kept.length > 0, `a 40-byte note squeezes it to ${kept.length}`);
  assert.equal(kept.at(-1).stopIndex, 165, "and what it keeps is the newest end of the list");
  assert.ok(encodeThreadBodyPreimage({ ...noted, stopReasons: kept }).length + 64 <= THREAD_MAX_BODY_BYTES);
  // The note wins that fight, and it can, because the list is cumulative:
  // the next record — an ordinary fix with no note — carries them again.
  assert.equal(fitStopReasons({ ...noted, note: new Uint8Array(0) }).length, THREAD_MAX_REASONS);
});

test("carried reasons are signed, ordered, and refused when malformed", async () => {
  const keypair = await generateThreadKeypair();
  const body = {
    unixSeconds: 1754265600, state: 2, mode: 2, travelMode: 1,
    leafCell: 3181, geomRef: 885, ratioQ12: 2048, speedBin: 7,
    stopIndex: 9, planRef: PLAN_REF,
    outcomes: [1, 2, 1, 3, 1, 1, 1, 1, 1],
    lastOutcome: { stopIndex: 9, outcome: STOP_OUTCOME.DELIVERED, reasonCode: 0 },
    // Mark order in, stop order out.
    stopReasons: [
      { stopIndex: 4, reasonCode: STOP_REASON.OTHER, photo: true },
      { stopIndex: 2, reasonCode: STOP_REASON.CUSTOMER_ABSENT }
    ],
    photoCount: 1,
    note: new Uint8Array(0)
  };
  const preimage = encodeThreadBodyPreimage(body);
  const signature = await signThread(preimage, keypair.privateSeed);
  const parsed = decodeThreadBody(encodeThreadBody(body, signature));
  assert.deepEqual(parsed.stopReasons, [
    { stopIndex: 2, reasonCode: STOP_REASON.CUSTOMER_ABSENT, photo: false },
    { stopIndex: 4, reasonCode: STOP_REASON.OTHER, photo: true }
  ], "the wire is ascending by stop index whatever order the caller marked in");
  assert.equal(parsed.photoCount, 1);

  // Inside the signature, so a link holder cannot rewrite why a parcel
  // came back — which is the whole reason it is not a side channel.
  const forged = { ...body, stopReasons: [{ stopIndex: 4, reasonCode: STOP_REASON.CUSTOMER_ABSENT }] };
  assert.notDeepEqual([...encodeThreadBodyPreimage(forged)], [...preimage]);

  // Malformed lists are refused at encode…
  assert.throws(() => encodeThreadBodyPreimage({
    ...body, stopReasons: [{ stopIndex: 2, reasonCode: 1 }, { stopIndex: 2, reasonCode: 2 }]
  }), /appears twice/);
  assert.throws(() => encodeThreadBodyPreimage({ ...body, stopReasons: [{ stopIndex: 0, reasonCode: 1 }] }),
    /1-based stop index/);
  assert.throws(() => encodeThreadBodyPreimage({ ...body, stopReasons: [{ stopIndex: 3, reasonCode: 200 }] }),
    /reason code is 0\.\.127/);

  // A body that carries none of the three optional blocks ends
  // `… flags noteLen signature`, so the flag byte is at a known offset
  // and can be poked without hunting for it.
  const bare = { ...body, lastOutcome: null, stopReasons: [], photoCount: 0 };
  const barePreimage = encodeThreadBodyPreimage(bare);
  const flagAt = barePreimage.length - 2;
  assert.equal(barePreimage[flagAt], 0, "no optional block, no flags set");
  const sealed = encodeThreadBody(bare, await signThread(barePreimage, keypair.privateSeed));
  assert.equal(decodeThreadBody(sealed).stopReasons.length, 0);

  // A reserved flag bit is refused rather than skipped: an unknown bit
  // that meant "and then four more fields" would otherwise decode as a
  // body with trailing bytes, or as a plausible wrong one.
  const flagged = Uint8Array.from(sealed);
  flagged[flagAt] |= 0x08;
  assert.throws(() => decodeThreadBody(flagged), /reserved bit/);

  // And entries that are not strictly increasing are refused at decode:
  // two for one stop would let a reader's answer depend on which one it
  // happened to win with. Hand-built, because the encoder cannot make it.
  const disordered = Uint8Array.from(sealed);
  disordered[flagAt] |= THREAD_MARK_FLAG.REASONS;
  const spliced = Uint8Array.from([
    ...disordered.subarray(0, flagAt + 1),
    2, 4, STOP_REASON.OTHER, 4, STOP_REASON.REFUSED, // two entries, both stop 4
    ...disordered.subarray(flagAt + 1)
  ]);
  assert.throws(() => decodeThreadBody(spliced), /strictly increasing/);
});

test("a marked stop that is later delivered stops carrying a reason", async () => {
  // A failed attempt retried and delivered is an ordinary day, and the
  // reason for the failure is no longer a fact about the stop.
  const run = await fleet();
  await run.publisher.markStop(3, STOP_OUTCOME.FAILED, { reason: STOP_REASON.CUSTOMER_ABSENT });
  await run.publisher.markStop(5, STOP_OUTCOME.SKIPPED, { reason: STOP_REASON.INACCESSIBLE });
  await run.deliverAll();
  assert.deepEqual(run.subscriber.latest().stopReasons.map(entry => entry.stopIndex), [3, 5]);

  await run.publisher.markStop(3, STOP_OUTCOME.DELIVERED);
  await run.deliverAll();
  const update = run.subscriber.latest();
  assert.deepEqual(update.stopReasons.map(entry => entry.stopIndex), [5],
    "a delivered stop needs no reason and stops paying for one");
  assert.equal(update.outcomes[2], STOP_OUTCOME.DELIVERED);
  assert.equal(stopReasonFor(update, 3).reasonKnown, true, "and nothing about it is missing");
  assert.equal(stopReasonFor(update, 3).reasonCode, null);

  // Re-marking a still-unresolved stop overwrites its reason and makes it
  // the newest, so the cap protects it rather than the stale entry.
  await run.publisher.markStop(5, STOP_OUTCOME.FAILED, { reason: STOP_REASON.PARCEL_MISSING });
  await run.deliverAll();
  assert.deepEqual(run.subscriber.latest().stopReasons, [
    { stopIndex: 5, reasonCode: STOP_REASON.PARCEL_MISSING, photo: false }
  ]);
  assert.deepEqual(run.publisher.stopReasons().map(entry => entry.stopIndex), [5]);
});

test("what the outage costs on the wire, in bytes the encoder produces", async () => {
  // §5.2.2's arithmetic, measured rather than asserted in prose.
  const worst = {
    unixSeconds: 1754265600, state: 2, mode: 2, travelMode: 1,
    // The widest a planet-scale leaf index produces, and the widest a
    // stop index can be.
    leafCell: 268435455, geomRef: 268435455, ratioQ12: 4095, speedBin: 7,
    stopIndex: 999, planRef: PLAN_REF,
    outcomes: new Array(200).fill(STOP_OUTCOME.DELIVERED),
    lastOutcome: { stopIndex: 999, outcome: STOP_OUTCOME.FAILED, reasonCode: STOP_REASON.OTHER },
    note: new Uint8Array(0)
  };
  const THREAD_MAX_NOTE = 64;
  const noteRoom = body => {
    let best = -1;
    for (let n = 0; n <= THREAD_MAX_NOTE; n++) {
      try {
        if (encodeThreadBodyPreimage({ ...body, note: new Uint8Array(n) }).length + 64 <= THREAD_MAX_BODY_BYTES) {
          best = n;
          continue;
        }
      } catch { /* refused is the same answer as "does not fit" */ }
      break;
    }
    return best;
  };

  // Unchanged: a day that carries no reasons pays nothing for them.
  assert.equal(noteRoom(worst), 57, "the published 200-stop headroom is where it was");
  const withPhoto = { ...worst, lastOutcome: { ...worst.lastOutcome, photoHash: "d3".repeat(32) } };
  assert.equal(noteRoom(withPhoto), 25);

  // Each reason is a stop-index varint and one byte, plus one for the
  // count: three failed stops on a 200-stop day cost seven bytes.
  const reasons = n => Array.from({ length: n }, (_, i) => ({ stopIndex: 7 + i, reasonCode: 1 }));
  assert.equal(noteRoom({ ...worst, stopReasons: reasons(3) }), 57 - 7);
  assert.equal(noteRoom({ ...worst, stopReasons: reasons(16) }), 57 - 33);
  assert.equal(noteRoom({ ...worst, photoCount: 4 }), 57 - 1);
  // A 200-stop day with a full 16-entry list at the widest stop indices.
  const wide = Array.from({ length: 16 }, (_, i) => ({ stopIndex: 150 + i, reasonCode: 1 }));
  assert.equal(noteRoom({ ...worst, stopReasons: wide, photoCount: 9 }), 57 - 49 - 1);

  // And the whole thing still frames into one PMT1.
  assert.ok(
    encodeThreadBodyPreimage({ ...worst, stopReasons: wide, photoCount: 9 }).length + 64
      + 43 <= THREAD_MAX_RECORD_BYTES,
    "body plus signature plus the 43 bytes of PMT1 framing"
  );
});
