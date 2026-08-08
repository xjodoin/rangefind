// Sealed dispatch tickets (threads §20.9): a ticket is encrypted at all
// times, and a device is enrolled with the sender before it can receive
// a job.
//
// The claim under test is the one that made this a security fix rather
// than a privacy one: a photographed QR used to be a stranger who could
// publish as that vehicle, because §20.5 says the protocol cannot tell
// two seed-holders apart. Sealed, the photograph is ciphertext — and the
// byte-scan below is what proves it, the same way the §20.8 wire-privacy
// test proves a stop's phone number never reaches a follower.

import assert from "node:assert/strict";
import test from "node:test";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { createMeshSession } from "../src/pulsemesh/session.js";
import { createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { createThreadChannel } from "../src/pulsemesh/thread_session.js";
import { THREAD_MODE, THREAD_TRAVEL_MODE } from "../src/pulsemesh/thread_codec.js";
import { fromHex, sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";
import { utf8Bytes } from "../src/pulsemesh/codec.js";
import { encodeQr } from "../src/qr.js";
import { x25519, x25519Base } from "../src/pulsemesh/x25519.js";
import {
  bytesToBase64Url,
  nativeX25519Available,
  x25519PublicKey,
  x25519SharedSecret
} from "../src/pulsemesh/thread_crypto.js";
import {
  SEAL_ERROR,
  SEAL_FIXED_BYTES,
  SEAL_MAX_RECIPIENTS,
  SEAL_RECIPIENT_BYTES,
  THREAD_MAX_DEVICE_NAME_BYTES,
  decodeDeviceCard,
  decodeSealedTicketHeader,
  deviceCardUrl,
  encodeDeviceCard,
  fingerprint,
  generateDeviceKeypair,
  isSealedTicket,
  openSealedTicket,
  parseDeviceCardUrl,
  sealTicket,
  sealedTicketUrl
} from "../src/pulsemesh/thread_seal.js";
import {
  classifyThreadArtifact,
  decodeThreadTicket,
  encodeThreadPlan,
  issueSealedTicket,
  issueTicket,
  verifyThreadTicket
} from "../src/pulsemesh/thread_ticket.js";

const GRAPH_DIR = "examples/osm-geo/public/route-graph";
const EPOCH32 = fromHex("f44796c8cc1f3fa797104e925812ff052717f3052b5dbcadb0a36db776e0a4d1");
const ISSUER_SEED = sha256Utf8("pulsemesh-test-dispatcher");

// The stop whose details a leaked ticket used to hand over: an order
// reference, an instruction, and a customer's phone number.
const DELIVERY = {
  dwellSeconds: 90,
  stops: [
    {
      lat: 45.5019,
      lon: -73.5674,
      plannedUnixSeconds: 1754265600,
      label: "Chez Lise",
      orderRef: "4471-B",
      instructions: "porte de côté, sonner deux fois",
      contact: "+15145550134"
    },
    { lat: 45.5088, lon: -73.554, label: "3e étage, sonner" }
  ]
};

const NEEDLES = ["4471-B", "+15145550134", "Chez Lise", "porte de côté"];

function contains(haystack, needle) {
  const target = utf8Bytes(needle);
  outer: for (let i = 0; i + target.length <= haystack.length; i++) {
    for (let j = 0; j < target.length; j++) if (haystack[i + j] !== target[j]) continue outer;
    return true;
  }
  return false;
}

async function plainTicket({ epochPrefix8 = EPOCH32.subarray(0, 8), plan = DELIVERY } = {}) {
  return issueTicket({
    issuerSeed: ISSUER_SEED,
    epochPrefix8,
    plan,
    notAfter: Math.floor(Date.now() / 1000) + 10 * 3600,
    mode: THREAD_MODE.FINE
  });
}

// --- 1. RFC 7748 -----------------------------------------------------------

test("X25519 reproduces RFC 7748 §5.2 and §6.1", () => {
  // §5.2, the two scalar/u-coordinate vectors.
  assert.equal(
    toHex(x25519(
      fromHex("a546e36bf0527c9d3b16154b82465edd62144c0ac1fc5a18506a2244ba449ac4"),
      fromHex("e6db6867583030db3594c1a424b15f7c726624ec26b3353b10a903a6d0ab1c4c")
    )),
    "c3da55379de9c6908e94ea4df28d084f32eccf03491c71f754b4075577a28552"
  );
  assert.equal(
    toHex(x25519(
      fromHex("4b66e9d4d1b4673c5ad22691957d6af5c11b6421e0ea01d42ca4169e7918ba0d"),
      fromHex("e5210f12786811d3f4b7959d0538ae2c31dbe7106fc03c3efc4cd549c715a493")
    )),
    "95cbde9476e8907d7aade45cb4b873f88b595a68799fa152e6f8f7647aac7957"
  );

  // §5.2's iterated vector. This is the one that catches a ladder that
  // is right for most scalars and wrong for a carry — the analogue of
  // the wrong-sign base point ed25519.js's fallback was caught on.
  let k = fromHex("0900000000000000000000000000000000000000000000000000000000000000");
  let u = k.slice();
  for (let i = 0; i < 1000; i++) {
    const next = x25519(k, u);
    u = k;
    k = next;
    if (i === 0) {
      assert.equal(toHex(k), "422c8e7a6227d7bca1350b3e2bb7279f7897b87bb6854b783c60e80311ae3079");
    }
  }
  assert.equal(toHex(k), "684cf59ba83309552800ef566f2f4d3c1c3887c49360e3875f2eb94d99532c51");

  // §6.1, the Diffie-Hellman the seal actually runs.
  const alice = fromHex("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
  const bob = fromHex("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
  assert.equal(toHex(x25519Base(alice)), "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a");
  assert.equal(toHex(x25519Base(bob)), "de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f");
  const shared = "4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742";
  assert.equal(toHex(x25519(alice, x25519Base(bob))), shared);
  assert.equal(toHex(x25519(bob, x25519Base(alice))), shared, "and both sides agree");

  // A small-order peer point yields an all-zero secret, which anyone can
  // produce without holding a private key. Refusing it is what stops a
  // forged wrap from "successfully" opening — and opening is the only
  // authentication §20.9 has.
  assert.throws(() => x25519(alice, new Uint8Array(32)), /small-order/u);
});

test("the pure X25519 agrees with WebCrypto wherever the host has it", async t => {
  if (!(await nativeX25519Available())) {
    t.skip("this host has no WebCrypto X25519 — the fallback is the only path here");
    return;
  }
  // Exactly the cross-check that caught the Ed25519 fallback's
  // wrong-sign base point: the two implementations must agree on every
  // key, or a job would open on one phone and not on another.
  for (let i = 0; i < 12; i++) {
    const a = sha256Utf8(`pulsemesh-x25519-cross-a-${i}`);
    const b = sha256Utf8(`pulsemesh-x25519-cross-b-${i}`);
    assert.deepEqual(await x25519PublicKey(a), x25519Base(a), `public key ${i}`);
    assert.deepEqual(
      await x25519SharedSecret(a, x25519Base(b)),
      x25519(a, x25519Base(b)),
      `shared secret ${i}`
    );
    // And the agreement is symmetric across the two implementations, not
    // merely self-consistent within each.
    assert.deepEqual(await x25519SharedSecret(b, await x25519PublicKey(a)), x25519(a, x25519Base(b)));
  }
});

// --- 2. round trip and the byte scan ---------------------------------------

test("a sealed ticket opens byte-identically and carries none of the plan in the clear", async () => {
  const driver = await generateDeviceKeypair();
  const issued = await plainTicket();

  // The plaintext artifact really does carry all of it — this is what
  // used to travel, and what the scan below has to stop finding.
  for (const needle of NEEDLES) {
    assert.equal(contains(issued.bytes, needle), true, `the plaintext ticket carries ${needle}`);
  }
  assert.equal(contains(issued.bytes, "PMK1"), true);

  const sealed = await sealTicket(issued.bytes, [driver.publicKey]);
  assert.equal(isSealedTicket(sealed), true);
  assert.equal(sealed.length, issued.bytes.length + SEAL_FIXED_BYTES + SEAL_RECIPIENT_BYTES);

  // The claim, byte-scanned. A photographed QR is these bytes.
  for (const needle of NEEDLES) {
    assert.equal(contains(sealed, needle), false, `the sealed ticket does not carry ${needle}`);
  }
  // Nor the run seed, which is what made a leak a publish capability.
  assert.equal(contains(sealed, issued.ticket.privateSeed), false, "nor the run seed");
  assert.equal(contains(sealed, issued.ticket.signature), false, "nor the issuer signature");
  assert.equal(contains(sealed.subarray(4), utf8Bytes("PMK1")), false, "nor the inner magic");

  const opened = await openSealedTicket(sealed, driver.privateKey);
  assert.deepEqual(opened, issued.bytes, "byte-identical inner ticket");
  const decoded = decodeThreadTicket(opened);
  assert.equal(decoded.plan.stops[0].contact, "+15145550134");
  assert.deepEqual(await verifyThreadTicket(decoded, { epochPrefix8: EPOCH32.subarray(0, 8) }), { ok: true });

  // Sealing twice gives different bytes — fresh ephemeral key, fresh
  // content key — and both open to the same ticket.
  const again = await sealTicket(issued.bytes, [driver.publicKey]);
  assert.notDeepEqual(again, sealed, "each sealing is fresh");
  assert.deepEqual(await openSealedTicket(again, driver.privateKey), issued.bytes);

  assert.equal(sealedTicketUrl(sealed), `wayfind://ticket#${bytesToBase64Url(sealed)}`);
});

// --- 3. multi-recipient and the enrolment gate -----------------------------

test("two enrolled devices open one job; a device nobody enrolled is told so", async () => {
  const driver = await generateDeviceKeypair();
  const dispatcher = await generateDeviceKeypair();
  const stranger = await generateDeviceKeypair();
  const issued = await plainTicket();

  const sealed = await sealTicket(issued.bytes, [driver.publicKey, dispatcher.publicKey]);
  // The content-key indirection is the whole point: a second recipient
  // costs one wrap, not a second copy of the ticket.
  const one = await sealTicket(issued.bytes, [driver.publicKey]);
  assert.equal(sealed.length - one.length, SEAL_RECIPIENT_BYTES, "one extra wrap, 64 bytes");
  assert.ok(SEAL_RECIPIENT_BYTES < issued.bytes.length / 4, "far less than re-encrypting");

  assert.deepEqual(await openSealedTicket(sealed, driver.privateKey), issued.bytes);
  assert.deepEqual(await openSealedTicket(sealed, dispatcher.privateKey), issued.bytes, "the issuer kept access");

  await assert.rejects(
    () => openSealedTicket(sealed, stranger.privateKey),
    error => {
      assert.equal(error.code, SEAL_ERROR.NOT_ENROLLED);
      assert.match(error.message, /another device/iu);
      assert.match(error.message, /enrol/iu, "and it says what to do about it");
      return true;
    }
  );

  // The hint is a convenience, not access control: rewriting every hint
  // must not lock the real recipient out, and must not let anyone else in.
  const header = decodeSealedTicketHeader(sealed);
  const rehinted = Uint8Array.from(sealed);
  for (const entry of header.recipients) {
    const offset = entry.hint.byteOffset - sealed.byteOffset;
    for (let i = 0; i < 4; i++) rehinted[offset + i] = 0xff;
  }
  assert.deepEqual(await openSealedTicket(rehinted, driver.privateKey), issued.bytes,
    "a rewritten hint costs a search, not access");
  await assert.rejects(() => openSealedTicket(rehinted, stranger.privateKey), /another device/iu);

  await assert.rejects(() => sealTicket(issued.bytes, []), /at least one enrolled recipient/u);
  await assert.rejects(
    () => sealTicket(issued.bytes, Array.from({ length: SEAL_MAX_RECIPIENTS + 1 }, () => driver.publicKey)),
    /at most 16 recipients/u
  );
});

test("a device card is what gets enrolled, and a fingerprint is what gets compared", async () => {
  const device = await generateDeviceKeypair();
  const card = encodeDeviceCard({ publicKey: device.publicKey, name: "Léa — Pixel 8" });
  const decoded = decodeDeviceCard(card);
  assert.deepEqual(decoded.publicKey, device.publicKey);
  assert.equal(decoded.name, "Léa — Pixel 8", "names are UTF-8, not ASCII");
  assert.equal(decoded.fingerprint, fingerprint(device.publicKey));
  assert.match(decoded.fingerprint, /^[0-9a-f]{8}$/u, "eight characters, to read aloud");

  const url = deviceCardUrl(card);
  assert.ok(url.startsWith("wayfind://device#"), url);
  assert.deepEqual(parseDeviceCardUrl(url).publicKey, device.publicKey);
  // The same three forms every host takes, wrapped by a mail client.
  const text = url.slice(url.indexOf("#") + 1);
  assert.deepEqual(
    parseDeviceCardUrl(`wayfind://device#${text.slice(0, 20)}\n${text.slice(20)}\n`).publicKey,
    device.publicKey
  );

  // Two devices are two fingerprints — that is the only reason to read
  // one aloud.
  const other = await generateDeviceKeypair();
  assert.notEqual(fingerprint(other.publicKey), fingerprint(device.publicKey));

  assert.throws(
    () => encodeDeviceCard({ publicKey: device.publicKey, name: "x".repeat(THREAD_MAX_DEVICE_NAME_BYTES + 1) }),
    /at most 32 bytes/u
  );
  assert.throws(() => decodeDeviceCard(Uint8Array.from([...card, 0])), /trailing bytes/u);
  assert.throws(() => decodeDeviceCard(new Uint8Array(8)), /PMV1/u);
});

// --- 4. tamper -------------------------------------------------------------

test("a flipped byte anywhere in a sealed ticket fails to open", async () => {
  const driver = await generateDeviceKeypair();
  const issued = await plainTicket();
  const sealed = await sealTicket(issued.bytes, [driver.publicKey]);
  const header = decodeSealedTicketHeader(sealed);
  const at = view => view.byteOffset - sealed.byteOffset;

  const spots = [
    ["the ciphertext", at(header.ciphertext) + 3],
    ["the GCM tag", sealed.length - 1],
    ["the body nonce", at(header.bodyNonce)],
    ["a wrap", at(header.recipients[0].wrapped) + 5],
    ["a wrap nonce", at(header.recipients[0].nonce) + 2],
    // The AAD binding: the ephemeral key is authenticated data for every
    // wrap and for the body, so moving it breaks both.
    ["the ephemeral key", at(header.ephemeralPublicKey) + 7],
    ["the version", 4]
  ];
  for (const [what, offset] of spots) {
    const damaged = Uint8Array.from(sealed);
    damaged[offset] ^= 0x01;
    await assert.rejects(
      () => openSealedTicket(damaged, driver.privateKey),
      error => {
        assert.ok(
          error.code === SEAL_ERROR.UNREADABLE || error.code === SEAL_ERROR.NOT_ENROLLED,
          `${what}: ${error.message}`
        );
        return true;
      },
      `flipping a bit in ${what} must not open`
    );
  }

  // Trailing bytes: the AEAD is what refuses them here, since a
  // ciphertext has no length prefix (§5's rule, arrived at differently).
  await assert.rejects(
    () => openSealedTicket(Uint8Array.from([...sealed, 0]), driver.privateKey),
    error => (assert.equal(error.code, SEAL_ERROR.UNREADABLE), true)
  );

  // A wrap lifted from another sealed ticket cannot be pasted in: its
  // key came from a different ephemeral agreement and its AAD names a
  // different ephemeral key.
  const other = await sealTicket(issued.bytes, [driver.publicKey]);
  const otherHeader = decodeSealedTicketHeader(other);
  const swapped = Uint8Array.from(sealed);
  swapped.set(otherHeader.recipients[0].wrapped, at(header.recipients[0].wrapped));
  swapped.set(otherHeader.recipients[0].nonce, at(header.recipients[0].nonce));
  await assert.rejects(() => openSealedTicket(swapped, driver.privateKey), /another device/iu,
    "a wrap does not travel between messages");

  // And bytes that are not a sealed ticket at all get the other answer.
  await assert.rejects(
    () => openSealedTicket(issued.bytes, driver.privateKey),
    error => {
      assert.equal(error.code, SEAL_ERROR.UNREADABLE);
      assert.doesNotMatch(error.message, /another device/iu, "never the enrolment sentence");
      return true;
    }
  );
});

// --- 6. what a host with no key can say ------------------------------------

test("a host holding no device key still knows a sealed job is a job", async () => {
  const driver = await generateDeviceKeypair();
  const issued = await plainTicket();
  const sealed = await sealTicket(issued.bytes, [driver.publicKey]);

  const artifact = classifyThreadArtifact(bytesToBase64Url(sealed));
  assert.equal(artifact.kind, "ticket", "a sealed job is still a job, not 'not a capability'");
  assert.equal(artifact.sealed, true);
  assert.match(artifact.reason, /sealed/iu);
  assert.match(artifact.reason, /device/iu);
  assert.match(artifact.reason, /enrol/iu, "and the action is enrolment");
  assert.doesNotMatch(artifact.reason, /45 bytes/u, "never the other artifact's complaint");

  // The URL form and the mail-client-wrapped form, as everywhere else.
  assert.equal(classifyThreadArtifact(sealedTicketUrl(sealed)).sealed, true);
  const text = bytesToBase64Url(sealed);
  assert.equal(
    classifyThreadArtifact(`wayfind://ticket#${text.slice(0, 40)}\n${text.slice(40)}\n`).kind,
    "ticket"
  );

  // A PME1 blob this build cannot parse is a different sentence again:
  // the magic still says what it is, and "unreadable" is the honest word.
  const truncated = sealed.subarray(0, 30);
  const stale = classifyThreadArtifact(truncated);
  assert.equal(stale.kind, "ticket");
  assert.equal(stale.sealed, true);
  assert.match(stale.reason, /dispatcher/iu);

  // An unsealed ticket says so, which is how a host can refuse to accept
  // one at all.
  assert.equal(classifyThreadArtifact(bytesToBase64Url(issued.bytes)).sealed, false);
});

// --- 5. the whole path -----------------------------------------------------

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

async function until(condition, { attempts = 60 } = {}) {
  for (let i = 0; i < attempts; i++) {
    if (condition()) return true;
    await flush();
  }
  return condition();
}

async function fixture(t) {
  let engine;
  try {
    engine = await openRouteGraphDir(GRAPH_DIR);
  } catch {
    t.skip(`route graph fixture missing at ${GRAPH_DIR}`);
    return null;
  }
  const centre = leaf => {
    const bbox = engine.root.leaves[leaf].bbox;
    return { lat: (bbox.minLat + bbox.maxLat) / 2 / 1e7, lon: (bbox.minLon + bbox.maxLon) / 2 / 1e7 };
  };
  for (let attempt = 0; attempt < 40; attempt++) {
    try {
      const from = centre((attempt * 7) % engine.root.leaves.length);
      const to = centre((attempt * 7 + 11) % engine.root.leaves.length);
      const route = await engine.route({ from, to });
      if ((route.edges || []).length >= 20 && route.geometry?.length > 20) return { engine, route };
    } catch {
      continue;
    }
  }
  t.skip("no suitable route in the fixture graph");
  return null;
}

test("issue sealed, open on the driver's device, publish — and the details survive", async t => {
  const found = await fixture(t);
  if (!found) return;
  const { engine, route } = found;
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });

  const customer = await createMeshSession({
    engine, network, id: "customer", transport: "loopback", readOnly: true, clock
  });
  const courier = await createMeshSession({ engine, network, id: "courier", transport: "loopback", clock });
  const customerThreads = createThreadChannel({ node: customer.node, network, engine, id: "customer", clock });
  const courierThreads = createThreadChannel({ node: courier.node, network, engine, id: "courier", clock });

  // The driver's phone shows its card; the dispatcher scans it, compares
  // the fingerprint aloud, and only then can seal a job to it.
  const device = await generateDeviceKeypair();
  const dispatcherDevice = await generateDeviceKeypair();
  const scanned = parseDeviceCardUrl(deviceCardUrl({ publicKey: device.publicKey, name: "Léa" }));
  assert.equal(scanned.fingerprint, fingerprint(device.publicKey));

  const mid = route.geometry[Math.floor(route.geometry.length / 2)];
  const end = route.geometry[route.geometry.length - 1];
  const job = await issueSealedTicket({
    issuerSeed: ISSUER_SEED,
    epochPrefix8: courier.node.epochPrefix8,
    plan: {
      dwellSeconds: 60,
      travelMode: THREAD_TRAVEL_MODE.BIKE,
      stops: [
        { lat: mid[0], lon: mid[1], label: "Chez Lise", orderRef: "4471-B", parcels: 2, contact: "+15145550134" },
        { lat: end[0], lon: end[1], label: "3e étage, sonner" }
      ]
    },
    notAfter: Math.floor(now / 1000) + 3600,
    mode: THREAD_MODE.FINE,
    recipients: [scanned, dispatcherDevice.publicKey]
  });

  assert.equal(job.recipientCount, 2);
  assert.equal(job.bytes, undefined, "there is no plaintext ticket field to reach for by accident");
  assert.equal(isSealedTicket(job.sealed), true);
  assert.ok(job.driverUrl.startsWith("wayfind://ticket#"));
  for (const needle of ["4471-B", "+15145550134", "Chez Lise"]) {
    assert.equal(contains(job.sealed, needle), false, `${needle} is not in what travels`);
  }
  // The customer's follow link exists at composition time, as ever, and
  // is not sealed: it is a read capability the customer is meant to have.
  assert.equal(job.link.length, 45);

  const opened = await openSealedTicket(job.sealed, device.privateKey);
  const ticket = decodeThreadTicket(opened);
  assert.deepEqual(
    await verifyThreadTicket(ticket, { epochPrefix8: courier.node.epochPrefix8 }),
    { ok: true },
    "opening is confidentiality; the issuer signature is still what says it is real"
  );
  assert.equal(ticket.plan.stops[0].orderRef, "4471-B");
  assert.equal(ticket.plan.stops[0].contact, "+15145550134");
  assert.equal(ticket.plan.stops[0].parcels, 2);

  const updates = [];
  const follow = await customerThreads.follow(job.link, { onUpdate: update => updates.push(update) });

  // A device with no key cannot publish it, and is told exactly that.
  await assert.rejects(
    () => courierThreads.publishTicket(job.sealed),
    /device private key/u
  );
  const stranger = await generateDeviceKeypair();
  await assert.rejects(
    () => courierThreads.publishTicket(job.sealed, { devicePrivateKey: stranger.privateKey }),
    /another device/iu
  );

  const run = await courierThreads.publishTicket(job.sealed, { devicePrivateKey: device.privateKey });
  assert.equal(run.travelMode, THREAD_TRAVEL_MODE.BIKE, "the plan survived the seal");
  assert.deepEqual(run.link, job.link, "onto the thread the customer is already watching");
  assert.equal(run.jobIdHex, job.jobIdHex);
  assert.equal(run.plan.stops[0].contact, "+15145550134", "the driver has the number to ring");

  for (let i = 0; i < route.geometry.length && updates.length < 2; i += Math.ceil(route.geometry.length / 6)) {
    const [lat, lon] = route.geometry[i];
    now += 6000;
    await run.handleFix({ lat, lon, speedMps: 5, nowMillis: now });
    await flush();
  }
  await until(() => updates.length >= 1);
  assert.ok(updates.length >= 1, "the run published");

  // And the wire still carries none of it — sealing the ticket did not
  // change what §20.8 already guaranteed about records.
  const latest = follow.latest();
  for (const needle of ["4471-B", "+15145550134", "Chez Lise"]) {
    assert.equal(contains(latest.bytes ?? new Uint8Array(0), needle), false);
  }
  assert.equal(latest.orderRef, undefined);

  // A base64url string is the other carrier, and it takes the same path.
  const second = await courierThreads.publishTicket(job.driverUrl, {
    devicePrivateKey: device.privateKey,
    catchUp: false
  });
  assert.deepEqual(second.link, job.link);

  follow.stop();
  courierThreads.close();
  customerThreads.close();
});

// --- 7. the measured numbers behind the §20.8 table ------------------------

function referenceStop(i, extra = {}) {
  return {
    lat: 45.4 + (i * 0.0013),
    lon: -73.7 + (i * 0.0011),
    plannedUnixSeconds: 1754265600 + (i * 300),
    label: `12 rue Sainte-Cath ${i}`.slice(0, 20),
    ...extra
  };
}

function fitsScannableQr(base64url) {
  try {
    encodeQr(`wayfind://ticket#${base64url}`, { ecLevel: "M", maxVersion: 25 });
    return true;
  } catch {
    return false;
  }
}

test("what the seal costs the QR budget, measured rather than estimated", async () => {
  const driver = await generateDeviceKeypair();
  const dispatcher = await generateDeviceKeypair();

  // The seal's own size, which is the doc's arithmetic made checkable.
  assert.equal(SEAL_FIXED_BYTES, 66, "magic, version, ephemeral key, count, body nonce, tag");
  assert.equal(SEAL_RECIPIENT_BYTES, 64, "hint, nonce, wrapped key, tag");
  const one = await plainTicket();
  assert.equal(
    (await sealTicket(one.bytes, [driver.publicKey])).length - one.bytes.length,
    130,
    "130 bytes for one recipient"
  );
  assert.equal(
    (await sealTicket(one.bytes, [driver.publicKey, dispatcher.publicKey])).length - one.bytes.length,
    194,
    "194 for two"
  );

  // §20.5's scannability policy, unchanged: 980 base64url characters at
  // version 25 / EC M, which is 735 bytes of *carrier payload*.
  assert.equal(fitsScannableQr("x".repeat(980)), true);
  assert.equal(fitsScannableQr("x".repeat(981)), false);
  assert.equal(Math.floor(980 / 4) * 3, 735);
  // What changed is what the payload carries. The signed ticket inside
  // now has 605 bytes, not 735.
  assert.equal(735 - 130, 605, "the sealed ceiling on a signed ticket");
  assert.equal(735 - 194, 541, "and with the issuer kept as a recipient");

  const ticketOf = async stops => issueTicket({
    issuerSeed: ISSUER_SEED,
    epoch32: EPOCH32,
    plan: { dwellSeconds: 60, stops },
    notAfter: Math.floor(Date.now() / 1000) + 10 * 3600
  });
  const stopsFor = (count, extra) => Array.from({ length: count }, (unused, i) => referenceStop(i, extra));

  const NONE = {};
  const TYPICAL = { orderRef: "4471", parcels: 2 };
  const MAX = { orderRef: "x".repeat(24), parcels: 65535, instructions: "y".repeat(64), contact: "z".repeat(24) };

  // The §20.8 table's QR column, and — the assertion that keeps it
  // honest — that one more stop does not fit.
  for (const [name, extra, max] of [["none", NONE, 12], ["typical", TYPICAL, 10], ["max", MAX, 2]]) {
    const fitting = await sealTicket((await ticketOf(stopsFor(max, extra))).bytes, [driver.publicKey]);
    assert.equal(fitsScannableQr(bytesToBase64Url(fitting)), true, `${max} ${name}-metadata stops fit a sealed QR`);
    assert.ok(fitting.length <= 735, `${fitting.length} bytes is inside the carrier ceiling`);
    const over = await sealTicket((await ticketOf(stopsFor(max + 1, extra))).bytes, [driver.publicKey]);
    assert.equal(fitsScannableQr(bytesToBase64Url(over)), false, `${max + 1} does not — ${max} is the maximum`);
  }

  // Two recipients is two stops fewer again in the typical case.
  const both = [driver.publicKey, dispatcher.publicKey];
  assert.equal(fitsScannableQr(bytesToBase64Url(await sealTicket((await ticketOf(stopsFor(8, TYPICAL))).bytes, both))), true);
  assert.equal(fitsScannableQr(bytesToBase64Url(await sealTicket((await ticketOf(stopsFor(9, TYPICAL))).bytes, both))), false);

  // Past the ceiling the answer is still the file (§20.5) — the same
  // sealed bytes, which still open and still verify.
  const day = await sealTicket((await ticketOf(stopsFor(100, TYPICAL))).bytes, [driver.publicKey]);
  assert.equal(fitsScannableQr(bytesToBase64Url(day)), false);
  const asFile = `${sealedTicketUrl(day)}\n`;
  const fragment = asFile.slice(asFile.lastIndexOf("#") + 1).replace(/\s+/gu, "");
  const decoded = decodeThreadTicket(await openSealedTicket(fragment, driver.privateKey));
  assert.equal(decoded.plan.stops.length, 100);
  assert.deepEqual(await verifyThreadTicket(decoded, { epochPrefix8: EPOCH32.subarray(0, 8) }), { ok: true });
});

test("an offer is broadcast, not sealed", async () => {
  const driver = await generateDeviceKeypair();
  await assert.rejects(
    () => issueSealedTicket({
      issuerSeed: ISSUER_SEED,
      epoch32: EPOCH32,
      plan: DELIVERY,
      notAfter: Math.floor(Date.now() / 1000) + 3600,
      seedless: true,
      recipients: [driver.publicKey]
    }),
    /issueJobOffer/u
  );
  // Belt and braces on the plan encoder the seal wraps: the sealed
  // artifact is a function of the plan bytes and nothing else.
  const a = await sealTicket((await plainTicket()).bytes, [driver.publicKey]);
  assert.deepEqual(
    decodeThreadTicket(await openSealedTicket(a, driver.privateKey)).planBytes,
    encodeThreadPlan(DELIVERY)
  );
});
