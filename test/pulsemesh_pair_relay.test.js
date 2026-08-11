// Mesh pairing across a keeper (threads §16), on the wire.
//
// The unit tests for §16 run over a loopback network where every peer
// hears every publish. Production never looks like that: a console is a
// browser tab and a phone is a phone, neither can be dialled, and both
// are connected to nothing but the keeper — which relays connections and
// never joins a thread topic. Gossip between two peers one relay hop
// apart does not happen on its own, so §16.2's rendezvous is what has to
// close the gap: both ends provide the pairing topic's key on the DHT,
// look the same key up, and dial whoever answers.
//
// This is the test that would have caught a pairing that worked
// everywhere except where it is used.

import assert from "node:assert/strict";
import test from "node:test";
import { MeshNode } from "../src/pulsemesh/node.js";
import { DEFAULT_CONSTANTS } from "../src/pulsemesh/bins.js";
import { createThreadChannel } from "../src/pulsemesh/thread_session.js";
import { generateDeviceKeypair } from "../src/pulsemesh/thread_seal.js";
import { decodePairingOffer } from "../src/pulsemesh/thread_pair.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const EPOCH32 = sha256Utf8("pulsemesh-pair-relay");
const EPOCH_HEX = toHex(EPOCH32);
const CONSTANTS = { ...DEFAULT_CONSTANTS };
// A pairing carries no traffic records, but a MeshNode is a traffic node
// and insists on knowing how one would be binned. Nothing here exercises
// it.
const cellOf = () => ({ x: 0, y: 0 });
const cellContext = () => ({ x: 0, y: 0 });

async function waitFor(predicate, { label, timeoutMs = 30_000, stepMs = 250 } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await predicate();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise(resolve => setTimeout(resolve, stepMs));
  }
}

test("a phone and a console pair through a keeper neither can see past", async t => {
  let libp2p;
  try {
    libp2p = await import("../src/pulsemesh/libp2p.js");
    await import("libp2p");
  } catch {
    t.skip("libp2p optional dependencies not installed");
    return;
  }
  const { createPulseMeshHost, createLibp2pNetwork } = libp2p;

  const hosts = [];
  const networks = [];
  const channels = [];
  t.after(async () => {
    for (const channel of channels) channel.close();
    for (const network of networks) await network.close();
    await Promise.all(hosts.map(host => host.stop().catch(() => {})));
  });

  // The fleet seed: a relay and a DHT server, and nothing else. It is the
  // only address either party knows.
  const keeper = await createPulseMeshHost({
    listen: ["/ip4/127.0.0.1/tcp/0/ws"],
    relay: true,
    // The seed is the DHT server both parties resolve through — the one
    // in production is, and without it there is nowhere to put a provider
    // record. `scope: "private"` keeps loopback addresses in the routing
    // table, which the public mapper would strip.
    dht: { scope: "private", clientMode: false }
  });
  hosts.push(keeper);
  const keeperAddress = keeper.getMultiaddrs()
    .map(String)
    .find(address => address.includes("/ws"));
  assert.ok(keeperAddress, "the keeper listens on a websocket a browser could dial");

  // The dispatcher's console and the driver's phone. Neither listens.
  const makePeer = async () => {
    const host = await createPulseMeshHost({
      listen: ["/p2p-circuit"],
      bootstrapPeers: [keeperAddress],
      // A phone and a tab are DHT *clients*: they publish and resolve
      // provider records and answer no queries. Same as production.
      dht: { scope: "private", clientMode: true }
    });
    const network = createLibp2pNetwork(host);
    hosts.push(host);
    networks.push(network);
    await network.ready;
    const node = new MeshNode({
      id: host.peerId.toString(),
      epochHex: EPOCH_HEX,
      constants: CONSTANTS,
      cellOf,
      cellContext,
      network,
      transport: "wire"
    });
    const channel = createThreadChannel({
      node, network, host, epoch32: EPOCH32, constants: CONSTANTS
    });
    channels.push(channel);
    return { host, network, node, channel };
  };

  const console_ = await makePeer();
  const phone = await makePeer();

  // Neither has an address of its own: a reservation from the keeper is
  // the only thing that makes either dialable, and until one lands there
  // is nothing to advertise. This is the state a real console and a real
  // phone are in.
  for (const peer of [console_, phone]) {
    assert.equal(
      peer.host.getMultiaddrs().some(address => /\/tcp\/\d+(\/ws)?$/u.test(address.toString())),
      false,
      "neither party has an address of its own to be dialled on"
    );
  }
  await waitFor(
    () => [console_, phone].every(peer =>
      peer.host.getMultiaddrs().some(address => address.toString().includes("/p2p-circuit"))),
    { label: "both parties holding a relay reservation" }
  );

  // The ceremony. The dispatcher shows a code; nobody has told either
  // party where the other is, and nobody will.
  const offer = await console_.channel.startPairing({ label: "Dispatch" });
  const device = await generateDeviceKeypair();
  const scanned = decodePairingOffer(offer.offerBytes);
  const answer = await phone.channel.replyToPairing({
    offer: scanned,
    devicePublicKey: device.publicKey,
    devicePrivateKey: device.privateKey,
    name: "Pixel"
  });

  // Both sides are driven by their host's clock, exactly as the apps do
  // it: the phone re-offers its reply while discovery is still finding
  // the console, and stops as soon as it is answered.
  const candidate = await waitFor(async () => {
    await console_.channel.tick();
    await phone.channel.tick();
    return offer.candidates[0] ?? null;
  }, { label: "the reply reaching the console across the relay" });

  assert.equal(candidate.name, "Pixel");
  assert.equal(candidate.publicKeyHex, toHex(device.publicKey));
  assert.equal(candidate.fingerprint.length, 8, "eight characters for two people to read");

  // The dispatcher reads the fingerprint aloud and presses the button.
  // Only now does anything travel the other way (§16.5).
  assert.equal(answer.ack, null, "nothing is confirmed until a human confirms it");
  const confirmed = await offer.confirm(candidate.publicKeyHex, { name: "Pixel" });
  assert.equal(confirmed.publicKeyHex, toHex(device.publicKey));

  const ack = await waitFor(async () => {
    await console_.channel.tick();
    await phone.channel.tick();
    return answer.ack;
  }, { label: "the ack reaching the phone" });
  assert.equal(ack.fingerprint, candidate.fingerprint, "the phone sees the characters it showed");

  // And the keeper carried a pairing it never joined.
  const topics = keeper.services.pubsub.getTopics().map(String);
  assert.equal(
    topics.some(topic => topic.startsWith("/rangefind/pulsemesh/thread/")),
    false,
    "the keeper relays the ceremony without subscribing to it"
  );
});
