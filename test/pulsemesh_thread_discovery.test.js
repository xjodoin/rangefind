// Thread discovery over the DHT (threads §4.2, §8 path 1) — the row that
// was marked `[~]` in the §19 conformance checklist.
//
// The property under test: a holder finds the thread's peers **from the
// link alone**. Nothing in the link is an address, so the subscriber
// derives a tag from `P`, derives a rendezvous key from the tag, asks the
// DHT who provides it, and dials them. No directory, no name server, no
// bootstrap address for the thread itself.

import assert from "node:assert/strict";
import test from "node:test";
import {
  createThreadDiscovery,
  discoveryWindows,
  rendezvousCid,
  threadAddresses
} from "../src/pulsemesh/thread_discovery.js";
import {
  deriveThreadKeys,
  generateThreadKeypair,
  threadRendezvous,
  threadTag,
  threadTopic,
  threadWindow
} from "../src/pulsemesh/thread_crypto.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const EPOCH32 = sha256Utf8("pulsemesh-thread-discovery");
const EPOCH_PREFIX16 = toHex(EPOCH32).slice(0, 16);

async function fixture() {
  const keypair = await generateThreadKeypair();
  const keys = await deriveThreadKeys(keypair.publicKey);
  return { keypair, keys, epoch32: EPOCH32, epochPrefix16hex: EPOCH_PREFIX16 };
}

test("the rendezvous CID is a deterministic function of the link", async () => {
  const { keys } = await fixture();
  const window = threadWindow(Date.now());
  const tag = await threadTag(keys, EPOCH32, window);
  const rendezvous = threadRendezvous(threadTopic(EPOCH_PREFIX16, tag));

  const first = await rendezvousCid(rendezvous);
  const second = await rendezvousCid(rendezvous);
  assert.equal(first.toString(), second.toString(), "same key, same CID — no coordination needed");

  // A different thread addresses a different point in the keyspace.
  const stranger = await deriveThreadKeys((await generateThreadKeypair()).publicKey);
  const strangerTag = await threadTag(stranger, EPOCH32, window);
  const strangerCid = await rendezvousCid(threadRendezvous(threadTopic(EPOCH_PREFIX16, strangerTag)));
  assert.notEqual(strangerCid.toString(), first.toString());
});

test("addresses cover the rotation neighbours and rotate with the window", async () => {
  const context = await fixture();
  const nowMillis = Date.now();
  const windows = discoveryWindows(nowMillis);
  assert.equal(windows.length, 3, "current window plus both neighbours");
  assert.deepEqual(windows, [windows[1] - 1, windows[1], windows[1] + 1]);

  const addresses = await threadAddresses({ ...context, nowMillis });
  assert.equal(addresses.length, 3);
  for (const address of addresses) {
    assert.equal(address.tag.length, 8);
    assert.match(address.topic, /^\/rangefind\/pulsemesh\/1\/t\/[0-9a-f]{16}\/[0-9a-f]{16}$/);
    assert.equal(address.rendezvous.length, 32);
  }

  // An hour later every address has changed: the DHT cannot follow a
  // thread through the day by watching one key.
  const later = await threadAddresses({ ...context, nowMillis: nowMillis + 3600_000 });
  const nowKeys = new Set(addresses.map(address => address.cid.toString()));
  assert.ok(later.every(address => !nowKeys.has(address.cid.toString())));
});

test("a holder finds the thread's peers from the link alone", async t => {
  let createPulseMeshHost;
  try {
    ({ createPulseMeshHost } = await import("../src/pulsemesh/libp2p.js"));
    await import("libp2p");
    await import("@libp2p/kad-dht");
  } catch {
    t.skip("libp2p / kad-dht optional dependencies not installed");
    return;
  }

  const hosts = [];
  t.after(async () => {
    for (const host of hosts) await host.stop().catch(() => {});
  });
  // `scope: "local"` keeps loopback addresses in the routing table. The
  // public default strips them, and peers then connect happily while
  // never entering each other's tables — `provide` waits forever for
  // closest peers that cannot exist.
  const makeHost = async () => {
    const host = await createPulseMeshHost({ dht: { scope: "local" } });
    hosts.push(host);
    return host;
  };

  const publisherHost = await makeHost();
  const subscriberHost = await makeHost();
  await subscriberHost.dial(publisherHost.getMultiaddrs()[0]);

  // Wait for identify to put each peer in the other's routing table.
  const deadline = Date.now() + 15000;
  while ((publisherHost.services.dht?.routingTable?.size ?? 0) === 0 && Date.now() < deadline) {
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  assert.ok(publisherHost.services.dht.routingTable.size > 0, "DHT routing table populated");

  const context = await fixture();
  const publisher = createThreadDiscovery({ host: publisherHost, ...context });
  // The subscriber knows nothing but the link — same `P`, hence the same
  // derived addresses, computed independently.
  const subscriber = createThreadDiscovery({ host: subscriberHost, ...context });
  assert.deepEqual(
    await subscriber.advertisedTags(),
    await publisher.advertisedTags(),
    "both derive identical addresses without exchanging anything"
  );

  const announced = await publisher.provide();
  assert.ok(announced > 0, `publisher advertised itself as a provider (${announced} windows)`);
  assert.equal(publisher.stats.provideTimeouts, 0, "provide completed rather than timing out");

  const found = await subscriber.findPeers({ limit: 4 });
  assert.ok(found.length > 0, "the subscriber found a provider with only the link");
  assert.equal(found[0].id, publisherHost.peerId.toString(), "and it is the publisher");
  assert.ok(!found.some(peer => peer.id === subscriberHost.peerId.toString()), "never returns itself");
});

test("discovery degrades quietly when the DHT cannot help", async t => {
  let createPulseMeshHost;
  try {
    ({ createPulseMeshHost } = await import("../src/pulsemesh/libp2p.js"));
    await import("libp2p");
  } catch {
    t.skip("libp2p not installed");
    return;
  }
  // A host with no DHT at all: a thread must still be usable through
  // gossip and ordinary bootstrap, so discovery returns empty rather
  // than throwing.
  const host = await createPulseMeshHost();
  t.after(() => host.stop().catch(() => {}));
  const discovery = createThreadDiscovery({ host, ...(await fixture()) });
  assert.equal(await discovery.provide(), 0);
  assert.deepEqual(await discovery.findPeers(), []);
  assert.deepEqual(await discovery.connect(), []);

  // And a DHT that simply finds nobody is an ordinary outcome, bounded
  // by a deadline rather than hanging the caller.
  const lonely = await createPulseMeshHost({ dht: { scope: "local" } });
  t.after(() => lonely.stop().catch(() => {}));
  const alone = createThreadDiscovery({
    host: lonely, ...(await fixture()), provideTimeoutMs: 500, findTimeoutMs: 500
  });
  const started = Date.now();
  await alone.provide();
  await alone.findPeers();
  assert.ok(Date.now() - started < 10000, "bounded, not hanging");
});
