import assert from "node:assert/strict";
import test from "node:test";
import {
  PAIR_MAX_REPLY_BYTES,
  createPairingOffer,
  createPairingReply,
  openPairingReply,
  createPairingAck,
  openPairingAck,
  decodePairingOffer,
  pairingOfferUrl,
  parsePairingOfferUrl,
  pairingTag,
  pairingKindOf,
  PAIR_KIND
} from "../src/pulsemesh/thread_pair.js";
import { generateDeviceKeypair } from "../src/pulsemesh/thread_seal.js";

const EPOCH8 = Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]);
const NOW = 1_800_000_000;

async function offerAndDevice() {
  const offer = await createPairingOffer({
    epochPrefix8: EPOCH8, label: "Dispatch", nowSeconds: NOW,
    addresses: ["/dns4/mesh.example/tcp/443/tls/ws/p2p/12D3KooWabc"]
  });
  const parsed = decodePairingOffer(offer.bytes);
  const device = await generateDeviceKeypair();
  return { offer, parsed, device };
}

test("offer round-trips through the QR url and carries its hints", async () => {
  const { offer, parsed } = await offerAndDevice();
  const viaUrl = parsePairingOfferUrl(pairingOfferUrl(offer.bytes));
  assert.equal(viaUrl.label, "Dispatch");
  assert.equal(viaUrl.addresses.length, 1);
  assert.deepEqual(viaUrl.epochPrefix8, EPOCH8);
  assert.equal(pairingKindOf(offer.bytes), PAIR_KIND.OFFER);
  assert.equal(parsed.notAfter, NOW + 900);
});

test("a reply proves possession and opens to the right card", async () => {
  const { offer, parsed, device } = await offerAndDevice();
  const reply = await createPairingReply({
    offer: parsed, devicePublicKey: device.publicKey, devicePrivateKey: device.privateKey,
    name: "Marco — Pixel", nowSeconds: NOW
  });
  assert.ok(reply.length <= PAIR_MAX_REPLY_BYTES, `reply ${reply.length} within cap`);
  assert.equal(pairingKindOf(reply), PAIR_KIND.REPLY);

  const opened = await openPairingReply({
    bytes: reply, pairingId: offer.pairingId,
    pairingPrivateKey: offer.privateKey, pairingPublicKey: offer.publicKey, nowSeconds: NOW
  });
  assert.ok(opened, "reply opened");
  assert.equal(opened.name, "Marco — Pixel");
  assert.equal(opened.publicKeyHex, Buffer.from(device.publicKey).toString("hex"));
  assert.equal(opened.fingerprint.length, 8);
});

test("a reply for a different pairing is dropped without decryption", async () => {
  const { parsed, device } = await offerAndDevice();
  const other = await createPairingOffer({ epochPrefix8: EPOCH8, nowSeconds: NOW });
  const reply = await createPairingReply({
    offer: parsed, devicePublicKey: device.publicKey, devicePrivateKey: device.privateKey, nowSeconds: NOW
  });
  const opened = await openPairingReply({
    bytes: reply, pairingId: other.pairingId,
    pairingPrivateKey: other.privateKey, pairingPublicKey: other.publicKey, nowSeconds: NOW
  });
  assert.equal(opened, null, "foreign reply rejected on plaintext pairingId");
});

test("a forged pop is rejected", async () => {
  const { offer, parsed, device } = await offerAndDevice();
  const reply = await createPairingReply({
    offer: parsed, devicePublicKey: device.publicKey, devicePrivateKey: device.privateKey, nowSeconds: NOW
  });
  // Flip a byte inside the sealed body (last byte is part of the AEAD tag,
  // so this fails the seal; flip one in the pop region by corrupting the
  // ciphertext middle instead).
  const tampered = Uint8Array.from(reply);
  tampered[tampered.length - 20] ^= 0xff;
  const opened = await openPairingReply({
    bytes: tampered, pairingId: offer.pairingId,
    pairingPrivateKey: offer.privateKey, pairingPublicKey: offer.publicKey, nowSeconds: NOW
  });
  assert.equal(opened, null, "tampered reply does not open");
});

test("a stale reply is rejected", async () => {
  const { offer, parsed, device } = await offerAndDevice();
  const reply = await createPairingReply({
    offer: parsed, devicePublicKey: device.publicKey, devicePrivateKey: device.privateKey, nowSeconds: NOW
  });
  const opened = await openPairingReply({
    bytes: reply, pairingId: offer.pairingId,
    pairingPrivateKey: offer.privateKey, pairingPublicKey: offer.publicKey,
    nowSeconds: NOW + 3600
  });
  assert.equal(opened, null, "a reply outside PAIR_POP_SKEW is refused");
});

test("the ack opens only for the chosen device", async () => {
  const { offer, device } = await offerAndDevice();
  const other = await generateDeviceKeypair();
  const ack = await createPairingAck({
    pairingId: offer.pairingId, devicePublicKey: device.publicKey,
    label: "Marco — Pixel", chosenFingerprint: "aabbccdd", enrolledAt: NOW
  });
  assert.equal(pairingKindOf(ack), PAIR_KIND.ACK);

  const mine = await openPairingAck({ bytes: ack, pairingId: offer.pairingId, devicePrivateKey: device.privateKey });
  assert.ok(mine, "the chosen device opens its ack");
  assert.equal(mine.fingerprint, "aabbccdd");
  assert.equal(mine.label, "Marco — Pixel");

  const notMine = await openPairingAck({ bytes: ack, pairingId: offer.pairingId, devicePrivateKey: other.privateKey });
  assert.equal(notMine, null, "another device learns only that it was not chosen");
});

test("both sides derive the same rendezvous tag", async () => {
  const { offer, parsed } = await offerAndDevice();
  const consoleTag = await pairingTag({ pairingId: offer.pairingId, pairingPub: offer.publicKey });
  const phoneTag = await pairingTag({ pairingId: parsed.pairingId, pairingPub: parsed.pairingPub });
  assert.deepEqual(consoleTag, phoneTag);
  assert.equal(consoleTag.length, 8);
});
