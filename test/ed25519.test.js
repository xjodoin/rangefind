// The pure-JS Ed25519 fallback (src/pulsemesh/ed25519.js).
//
// This code only ever runs where WebCrypto has no Ed25519 — Android
// WebView below 137, Hermes — which is to say: on the hosts where nobody
// is watching and where a wrong answer looks exactly like a refused
// dispatch ticket. So it is pinned from two directions at once. The RFC
// 8032 §7.1 vectors say what the answer is in the abstract; the
// cross-validation against Node's own WebCrypto says the fallback and
// the native path are interchangeable, which is the property that
// actually matters — a ticket's fate must not depend on which browser
// opened it.

import assert from "node:assert/strict";
import test from "node:test";
import {
  ed25519PublicKeyFromSeed,
  ed25519Sign,
  ed25519Verify,
  sha512
} from "../src/pulsemesh/ed25519.js";
import { fromHex, sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";
import {
  publicKeyFromSeed,
  setThreadCryptoImplementation
} from "../src/pulsemesh/thread_crypto.js";
import { THREAD_MODE, threadRecordAad } from "../src/pulsemesh/thread_codec.js";
import { encodeThreadPlan, issueTicket, verifyThreadTicket } from "../src/pulsemesh/thread_ticket.js";

const PKCS8_PREFIX = fromHex("302e020100300506032b657004220420");

// RFC 8032 §7.1, verbatim. TEST 4 (1023 bytes) is left out on purpose —
// it pins nothing these four do not, and a kilobyte of hex in a test
// file is a place for typos to hide.
const RFC_8032 = [
  {
    name: "TEST 1 (empty message)",
    seed: "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
    publicKey: "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
    message: "",
    signature:
      "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc" +
      "61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
  },
  {
    name: "TEST 2 (1 byte)",
    seed: "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb",
    publicKey: "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c",
    message: "72",
    signature:
      "92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da085ac1e43e15996e4" +
      "58f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00"
  },
  {
    name: "TEST 3 (2 bytes)",
    seed: "c5aa8df43f9f837bedb7442f31dcb7b166d38535076f094b85ce3a2e0b4458f7",
    publicKey: "fc51cd8e6218a1a38da47ed00230f0580816ed13ba3303ac5deb911548908025",
    message: "af82",
    signature:
      "6291d657deec24024827e69c3abe01a30ce548a284743a445e3680d7db5ac3ac18ff9b538d16f290a" +
      "e67f760984dc6594a7c15e9716ed28dc027beceea1ec40a"
  },
  {
    name: "TEST SHA(abc)",
    seed: "833fe62409237b9d62ec77587520911e9a759cec1d19755b7da901b96dca3d42",
    publicKey: "ec172b93ad5e563bf4932c70e1245034c35467ef2efd4d64ebf819683467e2bf",
    message:
      "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a83" +
      "6ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
    signature:
      "dc2a4459e7369633a52b1bf277839a00201009a3efbf3ecb69bea2186c26b58909351fc9ac90b3ecf" +
      "dfbc7c66431e0303dca179c138ac17ad9bef1177331a704"
  }
];

const utf8 = text => new TextEncoder().encode(text);

test("SHA-512 reproduces the FIPS 180-4 known answers", () => {
  assert.equal(
    toHex(sha512(new Uint8Array(0))),
    "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce" +
      "47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
    "the empty string — the padding block on its own"
  );
  assert.equal(
    toHex(sha512(utf8("abc"))),
    "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a" +
      "2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
  );
  // 112 bytes: the length field lands in the second block, which is
  // where an off-by-one in the padding shows up and nowhere else.
  const twoBlocks =
    "abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmno" +
    "ijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu";
  assert.equal(twoBlocks.length, 112);
  assert.equal(
    toHex(sha512(utf8(twoBlocks))),
    "8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018" +
      "501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909"
  );
});

test("the RFC 8032 §7.1 vectors come out byte for byte", () => {
  for (const vector of RFC_8032) {
    const seed = fromHex(vector.seed);
    const message = fromHex(vector.message);
    assert.equal(toHex(ed25519PublicKeyFromSeed(seed)), vector.publicKey, `${vector.name}: public key`);
    assert.equal(toHex(ed25519Sign(message, seed)), vector.signature, `${vector.name}: signature`);
    assert.equal(
      ed25519Verify(message, fromHex(vector.signature), fromHex(vector.publicKey)),
      true,
      `${vector.name}: verifies`
    );
  }
});

test("a malformed signature is a false verification, never a thrown error", () => {
  const seed = sha256Utf8("pulsemesh-ed25519-rejection");
  const publicKey = ed25519PublicKeyFromSeed(seed);
  const message = utf8("the courier is two minutes out");
  const signature = ed25519Sign(message, seed);
  assert.equal(ed25519Verify(message, signature, publicKey), true);

  const flip = (bytes, index) => {
    const copy = Uint8Array.from(bytes);
    copy[index] ^= 0x01;
    return copy;
  };

  assert.equal(ed25519Verify(flip(message, 3), signature, publicKey), false, "tampered message");
  assert.equal(ed25519Verify(message, flip(signature, 7), publicKey), false, "tampered R");
  assert.equal(ed25519Verify(message, flip(signature, 40), publicKey), false, "tampered S");
  assert.equal(ed25519Verify(message, signature, flip(publicKey, 5)), false, "tampered public key");

  // S + L, the one forgery the curve equation cannot see: [S+L]B ==
  // [S]B because L is the group order, so this passes the equation and
  // is refused only by the explicit S < L canonicity check.
  const L = (1n << 252n) + 27742317777372353535851937790883648493n;
  let s = 0n;
  for (let i = 63; i >= 32; i--) s = (s << 8n) | BigInt(signature[i]);
  const bigS = Uint8Array.from(signature);
  let inflated = s + L;
  for (let i = 32; i < 64; i++) {
    bigS[i] = Number(inflated & 0xffn);
    inflated >>= 8n;
  }
  assert.equal(inflated, 0n, "S + L still fits in 32 bytes");
  assert.equal(ed25519Verify(message, bigS, publicKey), false, "S >= L");

  assert.equal(ed25519Verify(message, signature.subarray(0, 63), publicKey), false, "truncated signature");
  assert.equal(ed25519Verify(message, new Uint8Array(64), publicKey), false, "an all-zero signature");
  assert.equal(
    ed25519Verify(message, signature, new Uint8Array(32).fill(0xff)),
    false,
    "an all-0xff public key: y >= p, not a canonical encoding of anything"
  );
  assert.equal(
    ed25519Verify(message, new Uint8Array(64).fill(0xff), publicKey),
    false,
    "and an all-0xff signature, whose R does not decode either"
  );
  assert.equal(ed25519Verify(message, signature, publicKey.subarray(0, 31)), false, "short public key");
});

/** Node ≥22 has WebCrypto Ed25519; a build without it skips, it does not fail. */
async function nativeEd25519() {
  try {
    const key = await crypto.subtle.importKey(
      "pkcs8",
      new Uint8Array([...PKCS8_PREFIX, ...new Uint8Array(32)]),
      { name: "Ed25519" },
      true,
      ["sign"]
    );
    await crypto.subtle.sign({ name: "Ed25519" }, key, new Uint8Array(1));
    return true;
  } catch {
    return false;
  }
}

test("the fallback and WebCrypto are the same signer", async t => {
  if (!(await nativeEd25519())) {
    t.skip("this Node build has no WebCrypto Ed25519 to cross-validate against");
    return;
  }
  const base64Url = text => {
    const padded = String(text).replace(/-/g, "+").replace(/_/g, "/");
    return new Uint8Array(Buffer.from(padded + "=".repeat((4 - (padded.length % 4)) % 4), "base64"));
  };

  for (let i = 0; i < 12; i++) {
    const seed = crypto.getRandomValues(new Uint8Array(32));
    const message = crypto.getRandomValues(new Uint8Array(1 + ((i * 37) % 240)));

    const privateKey = await crypto.subtle.importKey(
      "pkcs8", new Uint8Array([...PKCS8_PREFIX, ...seed]), { name: "Ed25519" }, true, ["sign"]
    );
    const nativePublicKey = base64Url((await crypto.subtle.exportKey("jwk", privateKey)).x);
    const fallbackPublicKey = ed25519PublicKeyFromSeed(seed);
    assert.deepEqual(fallbackPublicKey, nativePublicKey, `case ${i}: same seed, same public key`);

    const nativeSignature = new Uint8Array(await crypto.subtle.sign({ name: "Ed25519" }, privateKey, message));
    const fallbackSignature = ed25519Sign(message, seed);
    // Ed25519 is deterministic, so this is stronger than "both verify":
    // the two implementations produce the identical 64 bytes.
    assert.deepEqual(fallbackSignature, nativeSignature, `case ${i}: same signature`);

    const verifier = await crypto.subtle.importKey(
      "raw", fallbackPublicKey, { name: "Ed25519" }, false, ["verify"]
    );
    assert.equal(
      await crypto.subtle.verify({ name: "Ed25519" }, verifier, fallbackSignature, message),
      true,
      `case ${i}: WebCrypto accepts what the fallback signed`
    );
    assert.equal(
      ed25519Verify(message, nativeSignature, nativePublicKey),
      true,
      `case ${i}: the fallback accepts what WebCrypto signed`
    );
  }
});

test("thread_crypto falls back on its own, after one probe", async () => {
  // Android WebView 133 to the letter: AES-GCM, HKDF and HMAC all work,
  // `importKey({ name: "Ed25519" })` throws NotSupportedError. Nothing
  // is injected here — the point is that the host needs no help.
  const subtleProto = Object.getPrototypeOf(crypto.subtle);
  const realImportKey = subtleProto.importKey;
  let ed25519Imports = 0;
  subtleProto.importKey = function patched(format, keyData, algorithm, ...rest) {
    const name = typeof algorithm === "string" ? algorithm : algorithm?.name;
    if (name === "Ed25519") {
      ed25519Imports++;
      throw new Error("NotSupportedError: Unrecognized algorithm name");
    }
    return realImportKey.call(this, format, keyData, algorithm, ...rest);
  };

  try {
    // A fresh module instance, so the probe has not already run.
    const crypto7 = await import("../src/pulsemesh/thread_crypto.js?host=no-ed25519");
    const seed = sha256Utf8("pulsemesh-ed25519-webview133");
    const message = utf8("PMT1 record");

    const publicKey = await crypto7.publicKeyFromSeed(seed);
    assert.deepEqual(publicKey, ed25519PublicKeyFromSeed(seed), "the public key is derived anyway");

    const signature = await crypto7.signThread(message, seed);
    assert.deepEqual(signature, ed25519Sign(message, seed), "and the signature is the RFC's");
    assert.equal(await crypto7.verifyThread(message, signature, publicKey), true);

    const tampered = Uint8Array.from(signature);
    tampered[10] ^= 0x01;
    assert.equal(
      await crypto7.verifyThread(message, tampered, publicKey),
      false,
      "a bad signature is still refused — falling back is not a way of saying yes"
    );

    assert.equal(ed25519Imports, 1, "one probe for the process, not one thrown error per call");

    // The rest of the key schedule never left WebCrypto.
    const threadSecret = await crypto7.deriveThreadSecret(seed);
    const keys = await crypto7.deriveThreadKeys(threadSecret);
    assert.equal(keys.contentKey.length, 32);
    const aad = threadRecordAad(
      new Uint8Array(8), new Uint8Array(8), 1, 1, new Uint8Array(16)
    );
    const sealed = await crypto7.sealThreadBody(keys, 1, aad, message);
    assert.deepEqual(await crypto7.openThreadBody(keys, 1, aad, sealed), message);
  } finally {
    subtleProto.importKey = realImportKey;
  }
});

test("a dispatch ticket survives a host with no WebCrypto Ed25519", async () => {
  // What the Android WebView actually sees: no native algorithm, the
  // fallback carrying every signature and every verification.
  setThreadCryptoImplementation({
    publicKeyFromSeed: seed => ed25519PublicKeyFromSeed(seed),
    sign: (message, seed) => ed25519Sign(message, seed),
    verify: (message, signature, publicKey) => ed25519Verify(message, signature, publicKey)
  });
  try {
    const issuerSeed = sha256Utf8("pulsemesh-ed25519-fallback-dispatcher");
    const epoch32 = sha256Utf8("pulsemesh-ed25519-fallback-epoch");
    const epochPrefix8 = epoch32.subarray(0, 8);
    const planBytes = encodeThreadPlan({
      dwellSeconds: 60,
      stops: [{ lat: 45.5019, lon: -73.5674, label: "Chez Lise" }]
    });

    const issued = await issueTicket({
      issuerSeed,
      epoch32,
      planBytes,
      notAfter: Math.floor(Date.now() / 1000) + 3600,
      mode: THREAD_MODE.FINE
    });
    assert.deepEqual(
      issued.ticket.issuerPublicKey,
      ed25519PublicKeyFromSeed(issuerSeed),
      "the issuer identity is the same key either way"
    );
    assert.deepEqual(
      await verifyThreadTicket(issued.ticket, { epochPrefix8 }),
      { ok: true },
      "a ticket signed on this host verifies on this host"
    );
    assert.deepEqual(
      await publicKeyFromSeed(issued.ticket.privateSeed),
      ed25519PublicKeyFromSeed(issued.ticket.privateSeed),
      "and thread_crypto routes through the injection, not around it"
    );

    // The bug this whole file exists for was a *valid* ticket refused.
    // A forged one must still be refused, for the original reason.
    const forged = Uint8Array.from(issued.bytes);
    forged[forged.length - 70] ^= 0x02;
    const verdict = await verifyThreadTicket(forged, { epochPrefix8 });
    assert.equal(verdict.ok, false);
    assert.match(verdict.reason, /signature does not verify/);
  } finally {
    setThreadCryptoImplementation(null);
  }
});
