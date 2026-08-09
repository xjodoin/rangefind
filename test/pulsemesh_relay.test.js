// The hop that made every other hop pointless.
//
// A driver's phone cannot accept an inbound connection and neither can a
// customer's browser tab. Something in the middle has to carry the bytes,
// and the thing in the middle is a keeper. But a thread rides a GossipSub
// topic derived from the capability, a keeper must never hold one, and
// GossipSub does not forward for a topic it has not joined — so a keeper
// subscribing is not on the table, and before this the keeper carried
// nothing at all. A real Android driver reached a real keeper, a real
// browser reached the same keeper, and the driver published happily to
// seq 147 into a void.
//
// Circuit Relay v2 is the answer that does not require the keeper to
// understand a single byte it moves: driver and customer each reserve a
// slot, the customer dials the driver *through* the keeper, and GossipSub
// meshes over that connection like any other. The keeper sees an
// encrypted stream between two peer ids and never learns the topic.
//
// The publisher here listens on nothing. That is the whole point — it is
// not reachable except through the relay, so if these assertions pass the
// bytes could only have gone one way.

import assert from "node:assert/strict";
import test from "node:test";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const TOPIC = `/rangefind/pulsemesh/relay-test/${toHex(sha256Utf8("relay")).slice(0, 16)}`;

async function waitFor(check, { timeoutMs = 20000, stepMs = 100, label = "condition" } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await new Promise(resolve => setTimeout(resolve, stepMs));
  }
}

test("a keeper relays a thread between two peers that cannot dial each other", async t => {
  let createPulseMeshHost;
  try {
    ({ createPulseMeshHost } = await import("../src/pulsemesh/libp2p.js"));
    await import("libp2p");
  } catch {
    t.skip("libp2p optional dependencies not installed");
    return;
  }

  const hosts = [];
  t.after(async () => {
    await Promise.all(hosts.map(host => host.stop().catch(() => {})));
  });

  // The keeper: the only peer with an address anyone can dial.
  const keeper = await createPulseMeshHost({
    listen: ["/ip4/127.0.0.1/tcp/0/ws"],
    relay: true
  });
  hosts.push(keeper);
  const keeperAddress = keeper.getMultiaddrs()
    .map(address => address.toString())
    .find(address => address.includes("/ws"));
  assert.ok(keeperAddress, "the keeper listens on a websocket a browser could dial");

  // The driver's phone and the customer's tab. Neither listens on
  // anything, which is the situation the product is actually deployed in.
  // "/p2p-circuit" is not an address they own — it is a request for one,
  // and it is what the browser profile listens on by default.
  const driver = await createPulseMeshHost({
    listen: ["/p2p-circuit"], bootstrapPeers: [keeperAddress]
  });
  hosts.push(driver);
  const customer = await createPulseMeshHost({
    listen: ["/p2p-circuit"], bootstrapPeers: [keeperAddress]
  });
  hosts.push(customer);
  for (const host of [driver, customer]) {
    assert.equal(
      host.getMultiaddrs().some(address => /\/tcp\/\d+(\/ws)?$/u.test(address.toString())),
      false,
      "neither peer has an address of its own to be dialled on"
    );
  }

  // A reservation is what turns "unreachable" into an address. Until the
  // driver holds one there is nothing for the customer to dial.
  const circuitAddress = await waitFor(
    () => driver.getMultiaddrs()
      .map(address => address.toString())
      .find(address => address.includes("/p2p-circuit")),
    { label: "the driver's relayed address" }
  );
  assert.match(circuitAddress, /p2p-circuit/u);
  assert.ok(
    circuitAddress.includes(keeper.peerId.toString()),
    "the driver is reachable *through the keeper*, by name"
  );

  // Neither peer has ever heard of the other. This is the dial a customer
  // makes after the DHT hands it a provider record.
  const { multiaddr } = await import("@multiformats/multiaddr");
  await customer.dial(multiaddr(circuitAddress));

  const heard = [];
  customer.services.pubsub.addEventListener("message", event => {
    if (event.detail.topic === TOPIC) heard.push(new Uint8Array(event.detail.data));
  });
  customer.services.pubsub.subscribe(TOPIC);
  driver.services.pubsub.subscribe(TOPIC);

  await waitFor(
    () => driver.services.pubsub.getSubscribers(TOPIC).length >= 1,
    { label: "the gossip mesh forming across the relayed connection" }
  );

  const record = new Uint8Array([0x50, 0x4d, 0x54, 0x31, 1, 2, 3, 4]);
  await waitFor(async () => {
    await driver.services.pubsub.publish(TOPIC, record).catch(() => {});
    return heard.length > 0;
  }, { label: "a record crossing from driver to customer" });

  assert.deepEqual(heard[0], record, "the bytes arrived unchanged");

  // And the keeper never joined the topic it just carried.
  assert.equal(
    keeper.services.pubsub.getTopics().includes(TOPIC),
    false,
    "the keeper carries a thread without subscribing to it"
  );
});
