// §21.11 — the route-day ticket: handing one service day of a recurring
// route to whoever is driving it today.
//
// §21 defines everything needed to *run* a day — the derivation, the
// certificate, the delegated publisher, the subscriber that verifies the
// chain — and nothing that *carries* one. The obvious workaround is
// actively wrong, and the headline test here is the proof: sealing an
// ordinary PMK1 whose seed happens to be the day seed makes the publisher
// derive its topic tag, its content key and its 78 bytes from the **day**
// public key, so the run publishes onto an address no parent is
// subscribed to. Nothing errors. The driver's phone shows a healthy run
// and every parent's link goes silent.
//
// So the first test does both: it publishes a route-day ticket and
// asserts a subscriber built from the term link *before the day key
// existed* accepts the records, and then publishes the same day seed
// through the ordinary path and asserts that same subscriber hears
// nothing at all.

import assert from "node:assert/strict";
import test from "node:test";

import { createThreadChannel } from "../src/pulsemesh/thread_session.js";
import {
  DAY_CERT_BYTES,
  THREAD_MODE,
  decodeThreadLink,
  encodeDayCertificate,
  encodeDayCertificatePreimage
} from "../src/pulsemesh/thread_codec.js";
import {
  CERT_ERROR,
  THREAD_MAX_CERT_SECONDS,
  mintDayCertificate,
  routeFollowLink,
  serviceDayOf
} from "../src/pulsemesh/thread_route.js";
import {
  TICKET_ERROR,
  TICKET_FLAG_DAY,
  classifyThreadArtifact,
  decodeThreadTicket,
  encodeThreadPlan,
  issueSealedTicket,
  issueTicket,
  planRefOf,
  ticketFollowLink,
  verifyThreadTicket
} from "../src/pulsemesh/thread_ticket.js";
import { generateDeviceKeypair, openSealedTicket, sealTicket } from "../src/pulsemesh/thread_seal.js";
import {
  bytesToBase64Url,
  deriveThreadSecret,
  generateThreadKeypair,
  publicKeyFromSeed,
  signThread
} from "../src/pulsemesh/thread_crypto.js";
import { utf8Bytes } from "../src/pulsemesh/codec.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";
import { encodeQr } from "../src/qr.js";

const EPOCH32 = sha256Utf8("pulsemesh-route-day-ticket");
const EPOCH_PREFIX8 = EPOCH32.subarray(0, 8);
const EPOCH_HEX = toHex(EPOCH32);

/** Route 8A, morning loop. One plan for the term. */
const PLAN = {
  travelMode: 1,
  dwellSeconds: 30,
  stops: [
    { lat: 45.52, lon: -73.59, label: "École Saint-Luc" },
    { lat: 45.53, lon: -73.58, label: "Av. Wiseman" },
    { lat: 45.54, lon: -73.57, label: "Parc Jarry" }
  ]
};
const PLAN_REF = planRefOf(encodeThreadPlan(PLAN));

const DAY_ONE = Date.UTC(2026, 8, 8, 11, 0, 0); // 07:00 America/Montreal
const TERM_END = Math.floor(DAY_ONE / 1000) + (180 * 86400);

function serviceDayAt(millis) {
  const at = new Date(millis);
  return serviceDayOf(at.getUTCFullYear(), at.getUTCMonth() + 1, at.getUTCDate());
}

/** What the depot mints each morning, from the root it never sends. */
async function dispatchFor(rootSeed, atMillis, { planRef = PLAN_REF, hours = 16, maxSeconds } = {}) {
  const notBefore = Math.floor(atMillis / 1000) - 3600;
  return mintDayCertificate({
    rootSeed,
    planRef,
    serviceDay: serviceDayAt(atMillis),
    notBefore,
    notAfter: notBefore + (hours * 3600),
    ...(maxSeconds ? { maxSeconds } : {})
  });
}

function topicOf(tag) {
  return `/rangefind/pulsemesh/1/t/${EPOCH_HEX.slice(0, 16)}/${toHex(tag)}`;
}

async function termLinkFor(root, notAfter = TERM_END) {
  return routeFollowLink({
    threadSecret: await deriveThreadSecret(root.privateSeed),
    rootPublicKey: root.publicKey,
    epochPrefix8: EPOCH_PREFIX8,
    notAfter
  });
}

/** Re-signs a ticket whose preimage a test has edited, as its issuer. */
async function resign(bytes, issuerSeed) {
  const preimage = bytes.subarray(0, bytes.length - 64);
  const signature = await signThread(
    Uint8Array.from([...utf8Bytes("pulsemesh/ticket/1"), ...preimage]),
    issuerSeed
  );
  return Uint8Array.from([...preimage, ...signature]);
}

test("a route-day ticket publishes on the ROOT's topic, and a bare day seed does not", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-8a-root"));
  const driver = await generateDeviceKeypair();
  const now = DAY_ONE;
  const clock = () => now;

  // 1. The term. Minted once, at the start of September, and handed to
  //    every parent. Nothing below ever reissues it.
  const termLink = await termLinkFor(root);
  const parent = createThreadChannel({ epochHex: EPOCH_HEX, id: "parent", clock });
  const updates = [];
  const follow = await parent.follow(
    `https://track.example/r#${bytesToBase64Url(termLink)}`,
    { onUpdate: update => updates.push(update) }
  );
  assert.equal(decodeThreadLink(termLink).delegated, true);

  // 2. Tomorrow morning, at the depot: one day of authority, and one
  //    sealed artifact carrying it to a device the depot enrolled.
  const { daySeed, certificate } = await dispatchFor(root.privateSeed, now);
  const job = await issueSealedTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: TERM_END,
    mode: THREAD_MODE.FINE,
    dayCertificate: certificate,
    privateSeed: daySeed,
    recipients: [driver.publicKey]
  });
  // The dispatcher did not have to pass the day seed at all: it is a
  // function of the root, the plan and the day (§21.2).
  assert.deepEqual([...job.ticket.privateSeed], [...daySeed]);
  assert.equal(job.ticket.routeDay, true);
  assert.equal(job.ticket.serviceDay, serviceDayAt(now));
  // The link this hands back is the *term's* — byte-identical to what the
  // parents were given in September, not a link over the day key.
  assert.deepEqual([...job.link], [...termLink], "the term's 78 bytes, unchanged");

  // 3. On the bus. The driver's phone holds a device key and ciphertext.
  const opened = decodeThreadTicket(await openSealedTicket(job.sealed, driver.privateKey));
  assert.deepEqual(await verifyThreadTicket(opened, { epochPrefix8: EPOCH_PREFIX8, nowMillis: now }), { ok: true });

  const bus = createThreadChannel({ epochHex: EPOCH_HEX, id: "bus", clock });
  const wire = [];
  const run = await bus.publishTicket(job.sealed, {
    devicePrivateKey: driver.privateKey,
    catchUp: false,
    onPublish: emitted => { wire.push(emitted); }
  });
  assert.deepEqual([...run.link], [...termLink], "the run publishes under the route, not under today");

  await run.handleFix({ lat: 45.521, lon: -73.591, speedMps: 9, nowMillis: now });
  await run.markStop(1, 1, { nowMillis: now });
  assert.ok(wire.length >= 3, "a certificate, then the records it covers");

  for (const emitted of wire) await parent.deliver(topicOf(emitted.tag), emitted.bytes, now);
  assert.ok(updates.length >= 2, "the parent tracked the day holding only its September link");
  assert.equal(parent.stats.dropped, 0, "and dropped nothing: the certificate led the records");
  assert.equal(follow.status({ nowMillis: now }).live, true);

  // 4. The regression this whole artifact exists to prevent. Seal the
  //    *same day seed* into an ordinary ticket — the obvious workaround —
  //    and every derivation moves to the day key.
  const wrong = await issueSealedTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: TERM_END,
    mode: THREAD_MODE.FINE,
    privateSeed: daySeed,
    recipients: [driver.publicKey]
  });
  assert.notDeepEqual([...wrong.link], [...termLink], "a different address entirely");
  assert.equal(decodeThreadLink(wrong.link).delegated, false);

  const stray = createThreadChannel({ epochHex: EPOCH_HEX, id: "stray", clock });
  const strayWire = [];
  const strayRun = await stray.publishTicket(wrong.sealed, {
    devicePrivateKey: driver.privateKey,
    catchUp: false,
    onPublish: emitted => { strayWire.push(emitted); }
  });
  await strayRun.handleFix({ lat: 45.522, lon: -73.592, speedMps: 9, nowMillis: now });
  const before = updates.length;
  for (const emitted of strayWire) await parent.deliver(topicOf(emitted.tag), emitted.bytes, now);
  // Nothing thrown, nothing logged, nothing wrong at the driver's end —
  // and not one record reaches a parent. That is the silent failure, and
  // `publishTicket` can no longer produce it from a day certificate.
  assert.equal(updates.length, before, "a bare day seed publishes where nobody is listening");
  assert.equal(follow.subscriber.stats.forgeries ?? 0, 0);

  parent.close();
  bus.close();
  stray.close();
});

test("a day ticket still hands over, and the second device publishes on the same topic", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-8a-handover"));
  const first = await generateDeviceKeypair();
  const spare = await generateDeviceKeypair();
  const now = DAY_ONE;
  const clock = () => now;

  const termLink = await termLinkFor(root);
  const { certificate, daySeed } = await dispatchFor(root.privateSeed, now);
  const job = await issueSealedTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: TERM_END,
    dayCertificate: certificate,
    privateSeed: daySeed,
    recipients: [first.publicKey]
  });

  // The bus breaks down at 08:20 and the spare goes out. The depot holds
  // the inner ticket, so handover is a fresh seal to an enrolled device —
  // §20.9's machinery, unchanged, on a §21 artifact.
  const inner = await openSealedTicket(job.sealed, first.privateKey);
  const resealed = await sealTicket(inner, [spare.publicKey]);
  await assert.rejects(() => openSealedTicket(resealed, first.privateKey), /sealed for another device/u);

  const parent = createThreadChannel({ epochHex: EPOCH_HEX, id: "parent", clock });
  const updates = [];
  await parent.follow(termLink, { onUpdate: update => updates.push(update) });

  const spareBus = createThreadChannel({ epochHex: EPOCH_HEX, id: "spare", clock });
  const wire = [];
  const run = await spareBus.publishTicket(resealed, {
    devicePrivateKey: spare.privateKey,
    catchUp: false,
    onPublish: emitted => { wire.push(emitted); }
  });
  assert.deepEqual([...run.link], [...termLink], "the same route, the same 78 bytes");
  await run.handleFix({ lat: 45.531, lon: -73.581, speedMps: 7, nowMillis: now });

  for (const emitted of wire) await parent.deliver(topicOf(emitted.tag), emitted.bytes, now);
  assert.ok(updates.length >= 1, "and the parent never knew the bus changed");
  assert.equal(parent.stats.dropped, 0);

  parent.close();
  spareBus.close();
});

test("a day ticket whose seed is not the certificate's day key is refused by name", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-8a-mismatch"));
  const now = DAY_ONE;
  const { certificate, daySeed } = await dispatchFor(root.privateSeed, now);

  // The depot cannot mint one by accident: a seed that is not what the
  // plan and the day derive is refused at issue.
  const other = (await generateThreadKeypair(sha256Utf8("route-day-ticket-other"))).privateSeed;
  await assert.rejects(
    () => issueTicket({
      issuerSeed: root.privateSeed,
      epoch32: EPOCH32,
      plan: PLAN,
      notAfter: TERM_END,
      dayCertificate: certificate,
      privateSeed: other
    }),
    /does not vouch for the supplied random day authority seed/u
  );

  // And a forged one — the seed swapped out and the ticket re-signed by
  // the real root — is refused before it can publish, by name, rather
  // than discovered by a driver whose records nobody verifies.
  const good = await issueTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: TERM_END,
    dayCertificate: certificate,
    privateSeed: daySeed
  });
  const seedAt = good.bytes.length - 64 - DAY_CERT_BYTES - 64;
  const swapped = Uint8Array.from(good.bytes);
  swapped.set(other, seedAt);
  const forged = await resign(swapped, root.privateSeed);

  // It decodes — the shape is fine and the issuer signature is real.
  const decoded = decodeThreadTicket(forged);
  assert.equal(decoded.routeDay, true);
  const verdict = await verifyThreadTicket(decoded, { epochPrefix8: EPOCH_PREFIX8, nowMillis: now });
  assert.equal(verdict.ok, false);
  assert.equal(verdict.code, TICKET_ERROR.DAY_SEED_MISMATCH);
  assert.match(verdict.reason, /not the day key its certificate vouches for/u);

  // The publisher refuses it too, so neither layer is load-bearing alone.
  const bus = createThreadChannel({ epochHex: EPOCH_HEX, id: "bus", clock: () => now });
  await assert.rejects(
    () => bus.publishTicket(forged, { catchUp: false }),
    /cannot be published/u
  );
  bus.close();
});

test("only the route's own root grants a day of it", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-8a-root-rule"));
  const stranger = await generateThreadKeypair(sha256Utf8("some-other-depot"));
  const now = DAY_ONE;

  // A dispatcher signing with anything but the route root is refused at
  // issue. It costs nothing to require: deriving the day seed and minting
  // the certificate both need the root seed already, so any party that
  // can issue this ticket holds the root by construction.
  const foreign = await dispatchFor(stranger.privateSeed, now);
  await assert.rejects(
    () => issueTicket({
      issuerSeed: root.privateSeed,
      epoch32: EPOCH32,
      plan: PLAN,
      notAfter: TERM_END,
      dayCertificate: foreign.certificate,
      privateSeed: foreign.daySeed
    }),
    /issued by the route root itself/u
  );

  // Spliced into a valid ticket and re-signed, it does not even decode:
  // the rule is a byte comparison, so it is enforced at the earliest
  // place it can be.
  const validDispatch = await dispatchFor(root.privateSeed, now);
  const good = await issueTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: TERM_END,
    dayCertificate: validDispatch.certificate,
    privateSeed: validDispatch.daySeed
  });
  const certAt = good.bytes.length - 64 - DAY_CERT_BYTES;
  const spliced = Uint8Array.from(good.bytes);
  spliced.set(foreign.certificate.bytes, certAt);
  const forged = await resign(spliced, root.privateSeed);
  assert.throws(() => decodeThreadTicket(forged), /issuer is not the certificate's root/u);

  // A certificate that *claims* this root but was signed by another is a
  // different failure and gets a different name: the shape is right, the
  // signature is not.
  const claimedAuthority = await generateThreadKeypair(sha256Utf8("route-claimed-authority"));
  const dayPublicKey = claimedAuthority.publicKey;
  const notBefore = Math.floor(now / 1000) - 3600;
  const fields = {
    rootPublicKey: root.publicKey,
    dayPublicKey,
    generation: serviceDayAt(now),
    serviceDay: serviceDayAt(now),
    planRef: PLAN_REF,
    notBefore,
    notAfter: notBefore + (16 * 3600)
  };
  const impostor = encodeDayCertificate(
    fields,
    await signThread(encodeDayCertificatePreimage(fields), stranger.privateSeed)
  );
  const claiming = await issueTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: TERM_END,
    dayCertificate: impostor,
    privateSeed: claimedAuthority.privateSeed
  });
  const verdict = await verifyThreadTicket(claiming.ticket, {
    epochPrefix8: EPOCH_PREFIX8, nowMillis: now
  });
  assert.equal(verdict.ok, false);
  assert.equal(verdict.code, CERT_ERROR.BAD_SIGNATURE);
});

test("an expired or over-long day certificate is refused", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-8a-windows"));
  const now = DAY_ONE;

  // Yesterday's day, handed over this morning. The ticket itself is
  // perfectly valid — it runs until the end of the term — and the
  // certificate is what has expired.
  const yesterday = await dispatchFor(root.privateSeed, now - 86400000, { hours: 10 });
  const stale = await issueTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: TERM_END,
    dayCertificate: yesterday.certificate,
    privateSeed: yesterday.daySeed
  });
  const staleVerdict = await verifyThreadTicket(stale.ticket, {
    epochPrefix8: EPOCH_PREFIX8, nowMillis: now
  });
  assert.equal(staleVerdict.ok, false);
  assert.equal(staleVerdict.code, CERT_ERROR.EXPIRED);
  assert.match(staleVerdict.reason, /day certificate/u);

  // A term-long "day": signed by the real root, and refused by the
  // holder. §21.5 rule 4 is a bound the verifier applies, and a ticket is
  // a verifier — otherwise a day key is a root key with extra steps.
  const forever = await dispatchFor(root.privateSeed, now, { hours: 24 * 365, maxSeconds: 86400 * 400 });
  assert.ok(forever.certificate.notAfter - forever.certificate.notBefore > THREAD_MAX_CERT_SECONDS);
  const wide = await issueTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: TERM_END,
    dayCertificate: forever.certificate,
    privateSeed: forever.daySeed
  });
  const wideVerdict = await verifyThreadTicket(wide.ticket, {
    epochPrefix8: EPOCH_PREFIX8, nowMillis: now
  });
  assert.equal(wideVerdict.ok, false);
  assert.equal(wideVerdict.code, CERT_ERROR.WINDOW_TOO_LONG);

  // And a day that starts after the artifact carrying it dies is nobody's
  // day (§21.5 rule 7, with the ticket's expiry standing in for the
  // link's).
  const tomorrow = await dispatchFor(root.privateSeed, now + 86400000);
  const short = await issueTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: tomorrow.certificate.notBefore - 60,
    dayCertificate: tomorrow.certificate,
    privateSeed: tomorrow.daySeed
  });
  const shortVerdict = await verifyThreadTicket(short.ticket, {
    epochPrefix8: EPOCH_PREFIX8, nowMillis: (tomorrow.certificate.notBefore - 120) * 1000
  });
  assert.equal(shortVerdict.ok, false);
  assert.equal(shortVerdict.code, TICKET_ERROR.DAY_AFTER_TICKET_EXPIRY);

  // The other half of rule 7, and the half that fails quietly. This ticket
  // is live right now — the day has begun, the driver is publishing — and
  // it expires an hour before the day ends. Nothing about today looks
  // wrong. It breaks the next time the artifact is opened: a phone
  // restarted mid-shift cannot reopen its own day, the run stops, and
  // every follower's link goes quiet with no error anywhere.
  const today = await dispatchFor(root.privateSeed, now);
  const outlived = await issueTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: today.certificate.notAfter - 3600,
    dayCertificate: today.certificate,
    privateSeed: today.daySeed
  });
  // Mid-day: inside the certificate's window and inside the ticket's own
  // expiry, so every other check here passes.
  const middayMillis = (today.certificate.notBefore + 3600) * 1000;
  assert.ok(Math.floor(middayMillis / 1000) < outlived.ticket.notAfter);
  const outlivedVerdict = await verifyThreadTicket(outlived.ticket, {
    epochPrefix8: EPOCH_PREFIX8, nowMillis: middayMillis
  });
  assert.equal(outlivedVerdict.ok, false);
  assert.equal(outlivedVerdict.code, TICKET_ERROR.DAY_OUTLIVES_TICKET);

  // And the honest version of the same shape is still accepted: a ticket
  // that outlives its day is what a term-long route ticket looks like.
  const covering = await issueTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: TERM_END,
    dayCertificate: today.certificate,
    privateSeed: today.daySeed
  });
  const coveringVerdict = await verifyThreadTicket(covering.ticket, {
    epochPrefix8: EPOCH_PREFIX8, nowMillis: middayMillis
  });
  assert.equal(coveringVerdict.ok, true);
});

test("a route-day ticket says so, and an ordinary one says it is ordinary", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-8a-classify"));
  const driver = await generateDeviceKeypair();
  const now = DAY_ONE;
  const { certificate, daySeed } = await dispatchFor(root.privateSeed, now);

  const day = await issueTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: TERM_END,
    dayCertificate: certificate,
    privateSeed: daySeed
  });
  assert.deepEqual(
    classifyThreadArtifact(bytesToBase64Url(day.bytes)),
    {
      kind: "ticket",
      reason: null,
      sealed: false,
      routeDay: true,
      // What a host puts on the screen: "route 8A, Tuesday's run", not
      // "a job".
      serviceDay: serviceDayAt(now)
    }
  );

  const job = await issueTicket({
    issuerSeed: root.privateSeed,
    epoch32: EPOCH32,
    plan: PLAN,
    notAfter: TERM_END
  });
  assert.deepEqual(
    classifyThreadArtifact(bytesToBase64Url(job.bytes)),
    { kind: "ticket", reason: null, sealed: false, routeDay: false, serviceDay: null }
  );

  // Sealed, which is how one actually travels: the kind is knowable and
  // the *sort* of job is not. `null` rather than `false`, because a host
  // that renders "an ordinary job" here would be inventing a claim.
  const sealed = await sealTicket(day.bytes, [driver.publicKey]);
  const classified = classifyThreadArtifact(bytesToBase64Url(sealed));
  assert.equal(classified.kind, "ticket");
  assert.equal(classified.sealed, true);
  assert.equal(classified.routeDay, null);
  assert.equal(classified.serviceDay, null);

  // The flag is in the artifact, not in the decoder's mood.
  assert.equal((day.bytes[5] & TICKET_FLAG_DAY) !== 0, true);
  assert.equal((job.bytes[5] & TICKET_FLAG_DAY) !== 0, false);

  // And `ticketFollowLink` is the single place the identity is decided,
  // so no caller can derive a link from a day key by forgetting a branch.
  const link = decodeThreadLink(await ticketFollowLink(day.ticket));
  assert.equal(link.delegated, true);
  assert.deepEqual([...link.rootPublicKey], [...root.publicKey]);
  assert.equal(decodeThreadLink(await ticketFollowLink(job.ticket)).delegated, false);
});

test("what the day certificate costs the QR budget, measured rather than estimated", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-8a-qr"));
  const driver = await generateDeviceKeypair();
  const now = DAY_ONE;

  // The certificate is a fixed 157 bytes inside the signed preimage, and
  // that is the entire cost — there is no length prefix and no second
  // artifact to carry.
  assert.equal(DAY_CERT_BYTES, 157);

  const stopsFor = count => Array.from({ length: count }, (unused, i) => ({
    lat: 45.4 + (i * 0.0013),
    lon: -73.7 + (i * 0.0011),
    plannedUnixSeconds: 1754265600 + (i * 300),
    label: `12 rue Sainte-Cath ${i}`.slice(0, 20)
  }));

  const sealedFor = async (count, cert) => {
    const plan = { dwellSeconds: 60, stops: stopsFor(count) };
    const authority = cert
      ? await mintDayCertificate({
          rootSeed: root.privateSeed,
          planRef: planRefOf(encodeThreadPlan(plan)),
          serviceDay: serviceDayAt(now),
          notBefore: Math.floor(now / 1000) - 3600,
          notAfter: Math.floor(now / 1000) + (12 * 3600)
        })
      : null;
    const issued = await issueTicket({
      issuerSeed: root.privateSeed,
      epoch32: EPOCH32,
      plan,
      notAfter: TERM_END,
      dayCertificate: authority?.certificate ?? null,
      privateSeed: authority?.daySeed ?? null
    });
    return sealTicket(issued.bytes, [driver.publicKey]);
  };

  const fits = base64url => {
    try {
      encodeQr(`wayfind://ticket#${base64url}`, { ecLevel: "M", maxVersion: 25 });
      return true;
    } catch {
      return false;
    }
  };

  // The §20.8 ceiling, unchanged: 735 bytes of carrier payload, less the
  // seal's 130 for one recipient, is 605 of signed ticket — and the
  // certificate takes 157 of those, leaving 448 for the plan.
  assert.equal(605 - DAY_CERT_BYTES, 448);

  // Measure the boundary from the actual carrier. The certificate must
  // reduce capacity, and both shapes must have a sharp first failure.
  const maxima = [];
  for (const cert of [false, true]) {
    let max = -1;
    for (let count = 0; count <= 20; count++) {
      const sealed = await sealedFor(count, cert);
      if (!fits(bytesToBase64Url(sealed))) break;
      max = count;
    }
    assert.ok(max >= 1, `at least one stop fits (dayCertificate: ${cert})`);
    assert.equal(fits(bytesToBase64Url(await sealedFor(max + 1, cert))), false);
    maxima.push(max);
  }
  assert.ok(maxima[1] < maxima[0], "the signed day certificate has a measurable QR cost");

  // Past that it is the `.wayfindjob` file, exactly as a hundred-drop
  // delivery day already was. Nothing about the protocol changes.
  assert.equal(fits(bytesToBase64Url(await sealedFor(40, true))), false);
});
