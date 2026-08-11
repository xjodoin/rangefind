import assert from "node:assert/strict";
import test from "node:test";
import { createThreadCache } from "../src/pulsemesh/thread_cache.js";
import { THREAD_CONSTANTS } from "../src/pulsemesh/thread_publish.js";
import { encodeThreadRecord } from "../src/pulsemesh/thread_codec.js";

function record(seq, tagByte) {
  return encodeThreadRecord({
    epochPrefix8: new Uint8Array(8).fill(1),
    tag: new Uint8Array(8).fill(tagByte),
    generation: 1,
    seq,
    previousHash: new Uint8Array(16),
    ciphertext: new Uint8Array(48).fill(7),
    admissionTag: new Uint8Array(16).fill(3)
  }).bytes;
}

test("a device's own run outlives the relay window; relayed traffic does not", () => {
  let now = 1_000_000;
  const cache = createThreadCache({ constants: THREAD_CONSTANTS, clock: () => now });

  // Somebody else's record, held on the ordinary relay budget.
  cache.admit(record(1, 0xaa), { nowMillis: now });
  // This device's own, retained until its link expires an hour out.
  cache.admit(record(1, 0xbb), {
    openable: true, nowMillis: now, retainUntilMillis: now + 3600_000
  });

  // Past THREAD_CACHE_TTL (600 s) — the relayed one should be gone.
  now += (THREAD_CONSTANTS.THREAD_CACHE_TTL + 60) * 1000;
  cache.sweep(now);

  const relayed = cache.answer({ entries: [{ tag: new Uint8Array(8).fill(0xaa), sinceGeneration: 0, sinceSeq: 0 }] }, { nowMillis: now });
  const own = cache.answer({ entries: [{ tag: new Uint8Array(8).fill(0xbb), sinceGeneration: 0, sinceSeq: 0 }] }, { nowMillis: now });
  assert.equal(relayed.entries[0].records.length, 0, "somebody else's traffic ages out on the relay window");
  assert.equal(own.entries[0].records.length, 1, "this device's own run is still servable after the run ended");

  // And it does not linger past the link's own expiry.
  now += 3600_000;
  cache.sweep(now);
  const expired = cache.answer({ entries: [{ tag: new Uint8Array(8).fill(0xbb), sinceGeneration: 0, sinceSeq: 0 }] }, { nowMillis: now });
  assert.equal(expired.entries[0].records.length, 0, "a capability that no longer opens is no longer served");
});
