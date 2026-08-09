// A publisher must join the topic it publishes on.
//
// This is the bug that made every other hop pointless, and it is invisible
// from the driver's side: GossipSub only maintains a mesh for topics a
// peer has *joined*, so an unsubscribed publisher has no mesh for its own
// topic and `publish` reaches nobody. `allowPublishToZeroTopicPeers` is on
// deliberately — a genuinely alone publisher should not throw — so the
// call succeeds, `seq` climbs, stats look healthy, and every follower
// watches a van that never moves.
//
// It survived a real Android driver, a real keeper and a real browser all
// being up and connected at once, and it was only found by printing both
// peers' topic lists side by side: the subscriber had three, the
// publisher had none.
//
// The demo never caught it because the loopback transport hands bytes
// straight to the other end without a mesh at all.

import assert from "node:assert/strict";
import test from "node:test";
import { createThreadChannel } from "../src/pulsemesh/thread_session.js";
import { THREAD_MODE } from "../src/pulsemesh/thread_codec.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const EPOCH = toHex(sha256Utf8("publish-topic-test"));

/** A transport that records what it was asked to do, and does nothing. */
function recordingNetwork() {
  const subscribed = new Set();
  const published = [];
  return {
    subscribed,
    published,
    subscribe: (_id, topic) => subscribed.add(topic),
    unsubscribe: (_id, topic) => subscribed.delete(topic),
    publish: (topic, bytes, _id) => published.push({ topic, bytes }),
    requestThread: async () => null
  };
}

const PLAN = {
  stops: [
    { lat: 49.6009, lon: 6.1332, label: "Gare", index: 1 },
    { lat: 49.6116, lon: 6.1319, label: "Centre", index: 2 }
  ]
};

test("a run joins its own topic before it publishes anything", async () => {
  const network = recordingNetwork();
  const channel = createThreadChannel({ network, epochHex: EPOCH, id: "driver" });

  const run = await channel.publish({ mode: THREAD_MODE.FINE, plan: PLAN });

  // Joined at open, not on first publish: GossipSub grafts a mesh
  // asynchronously, so a record emitted in the first second would leave
  // before there was anywhere for it to go. A van that reports twice and
  // then parks could otherwise publish its whole existence into that gap.
  assert.ok(
    network.subscribed.size >= 1,
    "the run subscribes to its topic when it opens, not when it first speaks"
  );

  await run.announce("on my way");
  assert.ok(network.published.length >= 1, "something was published");

  // The exact property that was broken: every topic this run published on
  // is a topic it had joined.
  for (const { topic } of network.published) {
    assert.ok(
      network.subscribed.has(topic),
      `published on ${topic} without joining it — those bytes reach nobody`
    );
  }
});

test("all three rotation windows are joined, so a tag rotation never lands on a topic the run is not in", async () => {
  const network = recordingNetwork();
  const channel = createThreadChannel({ network, epochHex: EPOCH, id: "driver" });
  await channel.publish({ mode: THREAD_MODE.FINE, plan: PLAN });
  // Previous, current and next — the same set a follow subscribes to
  // (§4.2), so the five-minute boundary is not a gap at either end.
  assert.equal(network.subscribed.size, 3);
});

test("a finished run lets its topics go", async () => {
  const network = recordingNetwork();
  const channel = createThreadChannel({ network, epochHex: EPOCH, id: "driver" });
  const run = await channel.publish({ mode: THREAD_MODE.FINE, plan: PLAN });
  assert.ok(network.subscribed.size > 0);

  await run.finish();
  // A device that finished its round and kept the subscription would
  // relay someone else's thread traffic for nothing, and on a phone that
  // is battery.
  assert.equal(network.subscribed.size, 0);
});
