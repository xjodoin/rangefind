// Do not advertise a peer nobody can dial.
//
// A provider record is worth exactly the addresses attached to it, and a
// driver's phone has none of its own — it gets one when a relay grants a
// reservation, which happens *after* the run opens and starts publishing.
// Advertising in that window puts a peer id with nowhere to reach it into
// the DHT, where it is then cached: the lookup succeeds, the dial fails,
// and the thread reads as undiscoverable when it is only not reachable
// yet.

import assert from "node:assert/strict";
import test from "node:test";
import { createThreadDiscovery } from "../src/pulsemesh/thread_discovery.js";
import { THREAD_CONSTANTS } from "../src/pulsemesh/thread_publish.js";
import { deriveThreadKeys, generateThreadKeypair } from "../src/pulsemesh/thread_crypto.js";
import { sha256Utf8, fromHex, toHex } from "../src/pulsemesh/sha256.js";

const EPOCH = toHex(sha256Utf8("discovery-reach"));

/** A host whose reachability we control, recording what it advertised. */
function fakeHost({ addresses = [] } = {}) {
  const provided = [];
  let current = addresses;
  return {
    provided,
    setAddresses: next => { current = next; },
    peerId: { toString: () => "12D3KooWTestPeerIdForDiscoveryReachCheck" },
    getMultiaddrs: () => current,
    contentRouting: {
      async provide(cid) { provided.push(cid.toString()); },
      async *findProviders() { /* nothing */ }
    }
  };
}

async function discoveryFor(host) {
  const keypair = await generateThreadKeypair(sha256Utf8("reach-keys"));
  const keys = await deriveThreadKeys(keypair.threadSecret);
  return createThreadDiscovery({
    host,
    keys,
    epoch32: fromHex(EPOCH),
    epochPrefix16hex: EPOCH.slice(0, 16),
    advertise: true,
    constants: THREAD_CONSTANTS,
    clock: () => 1_760_000_000_000
  });
}

test("a run with no address of its own advertises nothing", async () => {
  const host = fakeHost({ addresses: [] });
  const discovery = await discoveryFor(host);

  const announced = await discovery.provide();

  assert.equal(announced, 0);
  assert.deepEqual(host.provided, [], "nothing was published to the DHT");
  assert.ok(discovery.stats.provideSkipped >= 1, "and it was counted rather than swallowed");
});

test("it advertises as soon as a relay reservation gives it one", async () => {
  const host = fakeHost({ addresses: [] });
  const discovery = await discoveryFor(host);
  assert.equal(await discovery.provide(), 0);

  // The reservation lands.
  host.setAddresses(["/ip4/10.0.0.4/tcp/4001/ws/p2p/12D3KooWKeeper/p2p-circuit/p2p/12D3KooWDriver"]);
  const announced = await discovery.provide();

  assert.ok(announced > 0, "the run is now discoverable");
  assert.ok(host.provided.length > 0);
});

test("a changed address replaces the record instead of waiting out the interval", async () => {
  const host = fakeHost({ addresses: ["/ip4/10.0.0.4/tcp/4001/ws/p2p/12D3KooWA/p2p-circuit/p2p/12D3KooWDriver"] });
  const discovery = await discoveryFor(host);
  const first = await discovery.provide();
  assert.ok(first > 0);
  const afterFirst = host.provided.length;

  // Advertising again with the same addresses is rate-limited, which is
  // the whole point of the interval.
  assert.equal(await discovery.provide(), 0);
  assert.equal(host.provided.length, afterFirst);

  // A different relay is a different address, and the old record now
  // points somewhere that does not work — so it is republished at once.
  host.setAddresses(["/ip4/10.0.0.9/tcp/4001/ws/p2p/12D3KooWB/p2p-circuit/p2p/12D3KooWDriver"]);
  assert.ok(await discovery.provide() > 0, "a moved run re-advertises immediately");
  assert.ok(host.provided.length > afterFirst);
});
