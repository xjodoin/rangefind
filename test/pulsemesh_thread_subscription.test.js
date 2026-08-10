import assert from "node:assert/strict";
import test from "node:test";

import {
  decodeManagedSubscription,
  issueManagedSubscription,
  openManagedSubscription
} from "../src/pulsemesh/thread_subscription.js";
import { decodeThreadLink } from "../src/pulsemesh/thread_codec.js";
import { generateDeviceKeypair } from "../src/pulsemesh/thread_seal.js";
import { generateThreadKeypair } from "../src/pulsemesh/thread_crypto.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const EPOCH_PREFIX8 = sha256Utf8("managed-subscription-epoch").subarray(0, 8);
const NOW = 1_786_000_000;

test("managed subscription rotation excludes a revoked device and rejects rollback", async () => {
  const root = await generateThreadKeypair(sha256Utf8("managed-subscription-root"));
  const alice = await generateDeviceKeypair(sha256Utf8("managed-alice"));
  const bob = await generateDeviceKeypair(sha256Utf8("managed-bob"));

  const first = await issueManagedSubscription({
    rootSeed: root.privateSeed,
    recipients: [alice.publicKey, bob.publicKey],
    generation: 1,
    epochPrefix8: EPOCH_PREFIX8,
    notBefore: NOW - 60,
    notAfter: NOW + 3600
  });
  const aliceFirst = await openManagedSubscription(first.sealed, alice.privateKey, {
    expectedRootPublicKey: root.publicKey, nowSeconds: NOW
  });
  const bobFirst = await openManagedSubscription(first.sealed, bob.privateKey, {
    expectedRootPublicKey: root.publicKey, nowSeconds: NOW
  });
  assert.deepEqual(aliceFirst.link, bobFirst.link);

  const second = await issueManagedSubscription({
    rootSeed: root.privateSeed,
    recipients: [alice.publicKey],
    generation: 2,
    epochPrefix8: EPOCH_PREFIX8,
    notBefore: NOW - 60,
    notAfter: NOW + 7200
  });
  const aliceSecond = await openManagedSubscription(second.sealed, alice.privateKey, {
    expectedRootPublicKey: root.publicKey, minimumGeneration: 2, nowSeconds: NOW
  });
  await assert.rejects(
    () => openManagedSubscription(second.sealed, bob.privateKey, { nowSeconds: NOW }),
    /sealed for another device/u
  );
  await assert.rejects(
    () => openManagedSubscription(first.sealed, alice.privateKey, {
      expectedRootPublicKey: root.publicKey, minimumGeneration: 2, nowSeconds: NOW
    }),
    /older generation/u
  );

  const oldLink = decodeThreadLink(aliceFirst.link);
  const newLink = decodeThreadLink(aliceSecond.link);
  assert.notEqual(toHex(oldLink.threadSecret), toHex(newLink.threadSecret), "rotation uses a fresh capability");
  assert.equal(toHex(oldLink.rootPublicKey), toHex(newLink.rootPublicKey), "verification identity stays stable");
});

test("a managed grant is root-authenticated, time-bounded, and tamper evident", async () => {
  const root = await generateThreadKeypair(sha256Utf8("managed-auth-root"));
  const stranger = await generateThreadKeypair(sha256Utf8("managed-auth-stranger"));
  const device = await generateDeviceKeypair(sha256Utf8("managed-auth-device"));
  const issued = await issueManagedSubscription({
    rootSeed: root.privateSeed,
    recipients: [device.publicKey],
    generation: 7,
    epochPrefix8: EPOCH_PREFIX8,
    notBefore: NOW - 10,
    notAfter: NOW + 10,
    delegated: true
  });

  assert.equal(decodeManagedSubscription(issued.grant.bytes).delegated, true);
  await assert.rejects(
    () => openManagedSubscription(issued.sealed, device.privateKey, {
      expectedRootPublicKey: stranger.publicKey, nowSeconds: NOW
    }),
    /different thread root/u
  );
  await assert.rejects(
    () => openManagedSubscription(issued.sealed, device.privateKey, { nowSeconds: NOW + 11 }),
    /expired/u
  );

  const tampered = Uint8Array.from(issued.sealed);
  tampered[tampered.length - 1] ^= 1;
  await assert.rejects(
    () => openManagedSubscription(tampered, device.privateKey, { nowSeconds: NOW }),
    /not a sealed job|unreadable/u
  );
});
