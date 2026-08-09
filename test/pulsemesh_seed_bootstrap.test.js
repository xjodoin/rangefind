// Fleet seeds and bootstrap hints (threads §20.10).
//
// A capability says what a run *is*; nothing in it says how to reach the
// mesh the run lives on. These tests pin the three places an address is
// now allowed to travel and the one place it is not:
//
//   - inside the **signed** PMK1 preimage, so a dispatcher's claim
//     "dial this machine" cannot be edited in flight;
//   - inside the **sealed** PME1, so it reaches the awarded device and
//     is not readable off a photographed QR;
//   - in a URL **query**, never the fragment, as a hint a recipient may
//     ignore;
//   - and **not** in a PMJ1 offer, which is broadcast to strangers.

import assert from "node:assert/strict";
import test from "node:test";
import { encodeQr } from "../src/qr.js";
import { fromHex, sha256Utf8 } from "../src/pulsemesh/sha256.js";
import { utf8Bytes } from "../src/pulsemesh/codec.js";
import { bytesToBase64Url } from "../src/pulsemesh/thread_crypto.js";
import { generateDeviceKeypair, sealTicket, sealedTicketUrl } from "../src/pulsemesh/thread_seal.js";
import {
  BOOTSTRAP_QUERY_KEY,
  THREAD_MAX_BOOTSTRAP_ADDRESSES,
  THREAD_MAX_BOOTSTRAP_BYTES,
  THREAD_MAX_URL_BOOTSTRAP,
  encodeThreadLink,
  parseBootstrapHint,
  parseThreadLinkUrl,
  threadLinkUrl,
  withBootstrapHint
} from "../src/pulsemesh/thread_codec.js";
import {
  SEED_MAGIC,
  THREAD_MAX_SEED_LABEL_BYTES,
  decodeSeedCard,
  encodeSeedCard,
  isDialableAddress,
  parseSeedCardUrl,
  seedCardUrl,
  seedHintUrl
} from "../src/pulsemesh/thread_seed.js";
import {
  TICKET_FLAG_BOOTSTRAP,
  TICKET_FLAG_SEED,
  awardMatchesOffer,
  classifyThreadArtifact,
  decodeThreadTicket,
  issueJobOffer,
  issueSealedTicket,
  issueTicket,
  verifyThreadTicket
} from "../src/pulsemesh/thread_ticket.js";

const EPOCH32 = fromHex("f44796c8cc1f3fa797104e925812ff052717f3052b5dbcadb0a36db776e0a4d1");
const ISSUER_SEED = sha256Utf8("pulsemesh-test-dispatcher");

// A real-shaped address: an IPv4 host, a TCP port, and the peer id that
// is what actually authenticates the far end at the Noise handshake.
const SEED_ADDRESS = "/ip4/203.0.113.7/tcp/4001/p2p/12D3KooWQpTGgHNTn6xQAmGh4WcQwFLQzt2yYQEbfrTfNsPeYcNP";
const SEED_ADDRESS_2 = "/dns4/seed.depot.example/tcp/443/wss/p2p/12D3KooWEyoppNCUx8Yx66oV9fJnrUXo5FQ7q4o4kFqk4bkPcYcN";
const SEED_ADDRESS_3 = "/ip4/198.51.100.9/tcp/4001/p2p/12D3KooWJvyP3VJYymTqG7eH6gtRNRJPFHt6M9CFqBFUqBB2GkuT";

const DELIVERY = {
  dwellSeconds: 60,
  stops: [
    { lat: 45.5019, lon: -73.5674, label: "Chez Lise", contact: "+15145550134" },
    { lat: 45.5088, lon: -73.554, label: "3e étage" }
  ]
};

function ticketOf(overrides = {}) {
  return issueTicket({
    issuerSeed: ISSUER_SEED,
    epoch32: EPOCH32,
    plan: DELIVERY,
    notAfter: Math.floor(Date.now() / 1000) + 3600,
    ...overrides
  });
}

/** The byte scan the §20.8 and §20.9 privacy claims are already tested with. */
function contains(haystack, needle) {
  const target = utf8Bytes(needle);
  outer: for (let i = 0; i + target.length <= haystack.length; i++) {
    for (let j = 0; j < target.length; j++) if (haystack[i + j] !== target[j]) continue outer;
    return true;
  }
  return false;
}

// --- 1. the ticket carries it, under the signature -------------------------

test("a ticket round-trips 1 and 3 bootstrap addresses", async () => {
  const one = await ticketOf({ bootstrap: [SEED_ADDRESS] });
  assert.deepEqual(one.ticket.bootstrap, [SEED_ADDRESS]);
  assert.equal(one.ticket.seedPresent, true, "a bootstrap block does not displace the run seed");
  assert.deepEqual(await verifyThreadTicket(one.ticket, { epochPrefix8: EPOCH32.subarray(0, 8) }), { ok: true });

  const three = await ticketOf({ bootstrap: [SEED_ADDRESS, SEED_ADDRESS_2, SEED_ADDRESS_3] });
  assert.deepEqual(three.ticket.bootstrap, [SEED_ADDRESS, SEED_ADDRESS_2, SEED_ADDRESS_3]);
  assert.deepEqual(decodeThreadTicket(three.bytes).bootstrap, three.ticket.bootstrap, "and re-decodes");
  assert.deepEqual(decodeThreadTicket(three.base64url).bytes, three.bytes);

  // A single string is the shape a host reaching for one address writes.
  assert.deepEqual((await ticketOf({ bootstrap: SEED_ADDRESS })).ticket.bootstrap, [SEED_ADDRESS]);

  // None at all is the default, and costs the flag bit and nothing else.
  const bare = await ticketOf();
  assert.deepEqual(bare.ticket.bootstrap, []);
  assert.equal(bare.bytes[5] & TICKET_FLAG_BOOTSTRAP, 0);
  assert.equal(bare.bytes[5] & TICKET_FLAG_SEED, TICKET_FLAG_SEED);
  assert.equal(one.bytes[5] & TICKET_FLAG_BOOTSTRAP, TICKET_FLAG_BOOTSTRAP);
  // One address of this shape costs 84 bytes: the address, its length
  // varint, and the count.
  assert.equal(utf8Bytes(SEED_ADDRESS).length, 82);
  assert.equal(one.bytes.length - bare.bytes.length, 84);
});

test("the caps and the not-a-multiaddr case are refused by name", async () => {
  await assert.rejects(
    () => ticketOf({ bootstrap: [SEED_ADDRESS, SEED_ADDRESS_2, SEED_ADDRESS_3, SEED_ADDRESS] }),
    new RegExp(`At most ${THREAD_MAX_BOOTSTRAP_ADDRESSES} ticket bootstrap addresses fit here`, "u")
  );
  await assert.rejects(
    () => ticketOf({ bootstrap: [`/ip4/203.0.113.7/tcp/4001/p2p/${"1".repeat(80)}`] }),
    new RegExp(`at most ${THREAD_MAX_BOOTSTRAP_BYTES} bytes; this one is 110`, "u")
  );
  // The mistakes that actually happen: a hostname, a URL, a bare peer id.
  for (const wrong of ["seed.depot.example:4001", "https://seed.depot.example", "12D3KooWQpTGgHNTn6xQ"]) {
    await assert.rejects(
      () => ticketOf({ bootstrap: [wrong] }),
      /is a multiaddr and starts with/u,
      `${wrong} is refused`
    );
  }
});

test("the addresses are inside the signature", async () => {
  const issued = await ticketOf({ bootstrap: [SEED_ADDRESS] });
  const epochPrefix8 = EPOCH32.subarray(0, 8);
  assert.deepEqual(await verifyThreadTicket(issued.ticket, { epochPrefix8 }), { ok: true });

  // Flip one character of the address in place — same length, same
  // framing, a different machine to dial. The address is the last thing
  // in the preimage, so it ends where the 64-byte signature begins.
  const tampered = Uint8Array.from(issued.bytes);
  const start = tampered.length - 64 - utf8Bytes(SEED_ADDRESS).length;
  tampered[start + 5] = "9".charCodeAt(0);
  assert.notDeepEqual(decodeThreadTicket(tampered).bootstrap, [SEED_ADDRESS], "the address really changed");
  const verdict = await verifyThreadTicket(decodeThreadTicket(tampered), { epochPrefix8 });
  assert.equal(verdict.ok, false);
  assert.equal(verdict.reason, "the issuer signature does not verify");
});

test("a set bootstrap flag with nothing behind it is refused, and so are reserved bits", async () => {
  const issued = await ticketOf();
  const lying = Uint8Array.from(issued.bytes);
  lying[5] |= TICKET_FLAG_BOOTSTRAP;
  // The seed's 32 bytes are followed by the signature, so the count byte
  // this reads is signature noise — either way it must not decode as a
  // ticket with zero addresses.
  assert.throws(() => decodeThreadTicket(lying), /bootstrap address|truncated|trailing/u);

  const reserved = Uint8Array.from(issued.bytes);
  reserved[5] |= 0x08;
  assert.throws(() => decodeThreadTicket(reserved), /reserved flag bits must be zero/u);

  // Bit 2 is no longer reserved — it is §21.11's day certificate — and it
  // is refused the same way the bootstrap bit is: a set flag with
  // signature noise behind it never decodes as a route day.
  const claimingADay = Uint8Array.from(issued.bytes);
  claimingADay[5] |= 0x04;
  assert.throws(() => decodeThreadTicket(claimingADay), /PMTC|truncated|trailing/u);
});

// --- 2. the seal hides them ------------------------------------------------

test("the sealed ticket hides the bootstrap address", async () => {
  const driver = await generateDeviceKeypair();
  const job = await issueSealedTicket({
    recipients: [driver.publicKey],
    issuerSeed: ISSUER_SEED,
    epoch32: EPOCH32,
    plan: DELIVERY,
    notAfter: Math.floor(Date.now() / 1000) + 3600,
    bootstrap: [SEED_ADDRESS, SEED_ADDRESS_2]
  });
  assert.deepEqual(job.bootstrap, [SEED_ADDRESS, SEED_ADDRESS_2]);

  // The whole address, its host, and its peer id: none of them are in
  // the bytes that travel, exactly as no stop label or phone number is.
  for (const needle of [
    SEED_ADDRESS, SEED_ADDRESS_2, "203.0.113.7", "seed.depot.example",
    "12D3KooWQpTGgHNTn6xQAmGh4WcQwFLQzt2yYQEbfrTfNsPeYcNP", "/ip4/", "Chez Lise"
  ]) {
    assert.equal(contains(job.sealed, needle), false, `${needle} is not in the sealed bytes`);
  }
  // And the driver URL — the thing that goes in a QR — is that ciphertext.
  assert.equal(job.driverUrl.includes("203.0.113.7"), false);
});

test("an offer never carries one, and awarding one that does still matches", async () => {
  const issued = await ticketOf({ bootstrap: [SEED_ADDRESS] });
  const offer = await issueJobOffer({
    issuerSeed: ISSUER_SEED,
    ticket: issued.ticket,
    totalMeters: 8200
  });
  // A PMJ1 is public and broadcast: the dispatcher's own address in one
  // is a doxxing vector, so there is no field for it and a byte scan
  // finds nothing.
  assert.equal(contains(offer.bytes, "203.0.113.7"), false);
  assert.equal(contains(offer.bytes, "/ip4/"), false);
  assert.equal("bootstrap" in offer.offer, false, "an offer has no bootstrap field at all");
  // `jobId` and `planRef` hash the plan and the binding fields, never the
  // flags or the seed — so adding an address does not move the job.
  assert.deepEqual(awardMatchesOffer(offer.offer, issued.ticket), { ok: true, reason: null });
  const without = await issueJobOffer({
    issuerSeed: ISSUER_SEED,
    ticket: (await ticketOf()).ticket,
    totalMeters: 8200
  });
  assert.equal(without.jobIdHex, offer.jobIdHex, "the same job either way");
});

// --- 3. URLs: the query, never the fragment --------------------------------

test("a bootstrap hint never touches the fragment", async () => {
  const issued = await ticketOf({ bootstrap: [SEED_ADDRESS] });
  const base = "https://track.example/r";
  const plain = threadLinkUrl(base, issued.link);
  const hinted = threadLinkUrl(base, issued.link, { bootstrap: [SEED_ADDRESS] });

  const fragmentOf = url => url.slice(url.indexOf("#") + 1);
  assert.equal(fragmentOf(hinted), fragmentOf(plain), "byte-identical fragments");
  assert.notEqual(hinted, plain);
  assert.equal(hinted.indexOf("?") < hinted.indexOf("#"), true, "the hint is in front of the #");
  assert.equal(fragmentOf(hinted).includes(BOOTSTRAP_QUERY_KEY + "="), false);
  assert.equal(fragmentOf(hinted).includes("203.0.113.7"), false);
  // And the capability still parses out of the fragment untouched.
  assert.deepEqual(parseThreadLinkUrl(hinted).publicKey, parseThreadLinkUrl(plain).publicKey);

  // A sealed ticket URL takes the same option, with the same rule.
  const driver = await generateDeviceKeypair();
  const sealed = await sealTicket(issued.bytes, [driver.publicKey]);
  const driverUrl = sealedTicketUrl(sealed, "wayfind://ticket", { bootstrap: [SEED_ADDRESS] });
  assert.equal(fragmentOf(driverUrl), fragmentOf(sealedTicketUrl(sealed)));
});

test("a hint parses back, and a broken one is ignored rather than thrown", () => {
  const base = "https://track.example/r";
  const link = encodeThreadLink({
    publicKey: new Uint8Array(32).fill(7),
    epochPrefix8: EPOCH32.subarray(0, 8),
    notAfter: 2000000000
  });
  const url = threadLinkUrl(base, link, { bootstrap: [SEED_ADDRESS, SEED_ADDRESS_2] });
  assert.deepEqual(parseBootstrapHint(url), [SEED_ADDRESS, SEED_ADDRESS_2]);
  // Readable in the wild: `/` is not escaped, because a query may carry it.
  assert.ok(url.includes(`${BOOTSTRAP_QUERY_KEY}=/ip4/203.0.113.7/tcp/4001/p2p/`));

  // The URL cap is lower than the artifact's, and the extra is dropped.
  assert.equal(THREAD_MAX_URL_BOOTSTRAP, 2);
  assert.throws(
    () => threadLinkUrl(base, link, { bootstrap: [SEED_ADDRESS, SEED_ADDRESS_2, SEED_ADDRESS_3] }),
    /At most 2 bootstrap hints fit here/u
  );

  // A hint is a hint: nothing here may make a capability fail to parse.
  assert.deepEqual(parseBootstrapHint(`${base}#abc`), []);
  assert.deepEqual(parseBootstrapHint(`${base}?utm_source=sms#abc`), []);
  assert.deepEqual(parseBootstrapHint(`${base}?b=seed.depot.example#abc`), [], "not a multiaddr");
  assert.deepEqual(parseBootstrapHint(`${base}?b=%E0%A4%A#abc`), [], "a broken escape");
  assert.deepEqual(parseBootstrapHint(`${base}?b=/ip4/${"9".repeat(200)}#abc`), [], "over the byte cap");
  assert.deepEqual(parseBootstrapHint(null), []);
  // Comma-separated is accepted on the way in even though the writer
  // repeats the key: both forms turn up in hand-edited links.
  assert.deepEqual(parseBootstrapHint(`${base}?b=${SEED_ADDRESS},${SEED_ADDRESS_2}#abc`), [SEED_ADDRESS, SEED_ADDRESS_2]);
  // A hint on a URL that already has a query joins it rather than replacing it.
  const joined = withBootstrapHint(`${base}?lang=fr`, [SEED_ADDRESS]);
  assert.ok(joined.startsWith(`${base}?lang=fr&b=`));
  assert.deepEqual(parseBootstrapHint(joined), [SEED_ADDRESS]);
  assert.equal(withBootstrapHint(base, null), base, "no hint, no query");
});

// --- 4. the seed card ------------------------------------------------------

test("a seed card round-trips and classifies as a seed", () => {
  const card = encodeSeedCard({ addresses: [SEED_ADDRESS], label: "Depot seed" });
  assert.equal(String.fromCharCode(...card.subarray(0, 4)), SEED_MAGIC.PMH1);
  const decoded = decodeSeedCard(card);
  assert.deepEqual(decoded.addresses, [SEED_ADDRESS]);
  assert.equal(decoded.label, "Depot seed");
  assert.deepEqual(encodeSeedCard(decoded), card, "re-encoding what was decoded is byte-identical");

  const three = encodeSeedCard({ addresses: [SEED_ADDRESS, SEED_ADDRESS_2, SEED_ADDRESS_3] });
  assert.deepEqual(decodeSeedCard(three).addresses, [SEED_ADDRESS, SEED_ADDRESS_2, SEED_ADDRESS_3]);
  assert.equal(decodeSeedCard(three).label, "");

  const url = seedCardUrl(card);
  assert.ok(url.startsWith("wayfind://seed#"));
  assert.deepEqual(parseSeedCardUrl(url).addresses, [SEED_ADDRESS]);
  // However a mail client wrapped it.
  const wrapped = `${url.slice(0, 24)}\n${url.slice(24)}\n`;
  assert.deepEqual(parseSeedCardUrl(wrapped).addresses, [SEED_ADDRESS]);

  // A host branches on the magic, exactly as it does for a ticket or an
  // offer: a seed is a location and goes to "add this peer", never to a
  // job screen and never to a map.
  const asSeed = { kind: "seed", reason: null, sealed: false, routeDay: false, serviceDay: null };
  assert.deepEqual(classifyThreadArtifact(url), asSeed);
  assert.deepEqual(classifyThreadArtifact(card), asSeed);
  const broken = Uint8Array.from(card);
  broken[4] = 9; // an unsupported version
  assert.equal(classifyThreadArtifact(broken).kind, "seed");
  assert.match(classifyThreadArtifact(broken).reason, /cannot be read by this version/u);
});

test("a seed card refuses what it cannot honestly carry", () => {
  assert.throws(() => encodeSeedCard({ addresses: [] }), /at least one bootstrap address/u);
  assert.throws(
    () => encodeSeedCard({ addresses: [SEED_ADDRESS], label: "x".repeat(THREAD_MAX_SEED_LABEL_BYTES + 1) }),
    new RegExp(`A seed label is at most ${THREAD_MAX_SEED_LABEL_BYTES} bytes; this one is 33`, "u")
  );
  // Bytes, not characters — the cap a UI has to explain.
  assert.throws(() => encodeSeedCard({ addresses: [SEED_ADDRESS], label: "é".repeat(17) }), /at most 32 bytes/u);
  assert.doesNotThrow(() => encodeSeedCard({ addresses: [SEED_ADDRESS], label: "é".repeat(16) }));
  assert.throws(
    () => encodeSeedCard({ addresses: [SEED_ADDRESS, SEED_ADDRESS_2, SEED_ADDRESS_3, SEED_ADDRESS] }),
    /At most 3 seed addresses fit here/u
  );
  assert.throws(() => encodeSeedCard({ addresses: ["seed.depot.example"] }), /is a multiaddr/u);
  const card = encodeSeedCard({ addresses: [SEED_ADDRESS] });
  assert.throws(() => decodeSeedCard(Uint8Array.from([...card, 0])), /trailing bytes/u);
  const reserved = Uint8Array.from(card);
  reserved[5] |= 0x02;
  assert.throws(() => decodeSeedCard(reserved), /reserved flag bits must be zero/u);

  // The filter the keeper prints with: an address it was told to listen
  // on is not an address anyone can dial.
  assert.equal(isDialableAddress(SEED_ADDRESS), true);
  assert.equal(isDialableAddress("/ip4/0.0.0.0/tcp/4001"), false);
  assert.equal(isDialableAddress("/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWQpTG"), false);
  assert.equal(isDialableAddress("/ip6/::1/tcp/4001"), false);
  assert.equal(isDialableAddress("/ip6/::/tcp/4001"), false);
  assert.equal(isDialableAddress("/dns4/seed.depot.example/tcp/443/wss"), true);

  // The card's addresses, hinted onto a public link.
  const hinted = seedHintUrl("https://track.example/r", card);
  assert.deepEqual(parseBootstrapHint(hinted), [SEED_ADDRESS]);
  // The card may hold three and a URL only two; the extra is dropped
  // rather than refused, because a hint is a hint.
  const full = encodeSeedCard({ addresses: [SEED_ADDRESS, SEED_ADDRESS_2, SEED_ADDRESS_3] });
  assert.deepEqual(parseBootstrapHint(seedHintUrl("https://track.example/r", full)), [SEED_ADDRESS, SEED_ADDRESS_2]);
});

// --- 5. the re-measured QR budget (§20.8/§20.9 tables) ---------------------

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

test("what a bootstrap address costs the QR budget, measured rather than estimated", async () => {
  const driver = await generateDeviceKeypair();
  const dispatcher = await generateDeviceKeypair();

  // §20.5's ceiling is unchanged: 735 bytes of carrier payload, less the
  // seal's 130 for one recipient, is 605 bytes of signed ticket.
  assert.equal(735 - 130, 605);
  assert.equal(735 - 194, 541);
  // One address of this shape takes 84 of them, so the plan's share of a
  // single-recipient ticket falls to 521.
  assert.equal(605 - 84, 521);

  const sealedFor = async (stops, bootstrap, recipients) => sealTicket(
    (await issueTicket({
      issuerSeed: ISSUER_SEED,
      epoch32: EPOCH32,
      plan: { dwellSeconds: 60, stops },
      notAfter: Math.floor(Date.now() / 1000) + 10 * 3600,
      bootstrap
    })).bytes,
    recipients
  );
  const stopsFor = (count, extra) => Array.from({ length: count }, (unused, i) => referenceStop(i, extra));

  const NONE = {};
  const TYPICAL = { orderRef: "4471", parcels: 2 };
  const MAX = { orderRef: "x".repeat(24), parcels: 65535, instructions: "y".repeat(64), contact: "z".repeat(24) };
  const ONE = [SEED_ADDRESS];
  const both = [driver.publicKey, dispatcher.publicKey];
  const alone = [driver.publicKey];

  // The §20.10 table, and — the assertion that keeps it honest — that one
  // more stop does not fit.
  const rows = [
    ["none · no seed · 1 recipient", NONE, null, alone, 12],
    ["none · one seed · 1 recipient", NONE, ONE, alone, 9],
    ["none · no seed · 2 recipients", NONE, null, both, 10],
    ["none · one seed · 2 recipients", NONE, ONE, both, 8],
    ["typical · no seed · 1 recipient", TYPICAL, null, alone, 10],
    ["typical · one seed · 1 recipient", TYPICAL, ONE, alone, 8],
    ["typical · no seed · 2 recipients", TYPICAL, null, both, 8],
    ["typical · one seed · 2 recipients", TYPICAL, ONE, both, 6],
    ["max · no seed · 1 recipient", MAX, null, alone, 2],
    ["max · one seed · 1 recipient", MAX, ONE, alone, 2],
    ["max · one seed · 2 recipients", MAX, ONE, both, 1]
  ];
  for (const [name, extra, bootstrap, recipients, max] of rows) {
    const fitting = await sealedFor(stopsFor(max, extra), bootstrap, recipients);
    assert.equal(fitsScannableQr(bytesToBase64Url(fitting)), true, `${max} stops fit — ${name}`);
    assert.ok(fitting.length <= 735, `${fitting.length} bytes is inside the carrier ceiling — ${name}`);
    const over = await sealedFor(stopsFor(max + 1, extra), bootstrap, recipients);
    assert.equal(fitsScannableQr(bytesToBase64Url(over)), false, `${max + 1} does not — ${name}`);
  }

  // Past the ceiling the answer is the file, as it has always been, and
  // the address survives the trip intact.
  const day = await sealedFor(stopsFor(100, TYPICAL), ONE, alone);
  assert.equal(fitsScannableQr(bytesToBase64Url(day)), false);
});
