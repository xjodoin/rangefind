// The thread channel over a real transport, through the same MeshNode the
// traffic channel is using.
//
// The claim under test is the one that makes "share my drive" a feature
// of a *maps* app rather than of a tracking product: one link, no server,
// and the arrival time computed on the follower's own device by routing
// the reported position to their stop — so the publisher never learns
// which stop anyone cares about.

import assert from "node:assert/strict";
import test from "node:test";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { createMeshSession } from "../src/pulsemesh/session.js";
import { createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { createThreadChannel } from "../src/pulsemesh/thread_session.js";
import { THREAD_MODE, THREAD_STATE, decodeThreadResponse } from "../src/pulsemesh/thread_codec.js";
import { buildThreadRequest } from "../src/pulsemesh/thread_cache.js";
import { DEFAULT_CONSTANTS } from "../src/pulsemesh/bins.js";

const GRAPH_DIR = "examples/osm-geo/public/route-graph";

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
      if ((route.edges || []).length >= 20 && route.geometry?.length > 20) return { engine, route, from, to };
    } catch {
      continue;
    }
  }
  t.skip("no suitable route in the fixture graph");
  return null;
}

// Gossip delivery is asynchronous even over the loopback: accepting a
// record is AEAD plus a signature check, so it lands several promise hops
// after the publish returns — and how many depends on what else the
// runtime is doing, which is why this waits for the condition rather than
// for one turn of the microtask queue.
const flush = () => new Promise(resolve => setTimeout(resolve, 0));

async function until(condition, { attempts = 50 } = {}) {
  for (let i = 0; i < attempts; i++) {
    if (condition()) return true;
    await flush();
  }
  return condition();
}

test("one link shares a drive, and the follower computes its own arrival", async t => {
  const found = await fixture(t);
  if (!found) return;
  const { engine, route } = found;
  // Anchored to real time and shared by everyone: the subscriber refuses
  // updates more than MAX_FUTURE_SKEW ahead of *its* clock, so a
  // publisher stepping a simulated drive forward has to step the
  // receiver's clock with it.
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });

  const driver = await createMeshSession({
    engine, network, id: "driver", transport: "loopback", constants: DEFAULT_CONSTANTS, clock
  });
  const viewer = await createMeshSession({
    engine, network, id: "viewer", transport: "loopback", readOnly: true, clock
  });

  const driverThreads = createThreadChannel({ node: driver.node, network, engine, id: "driver", clock });
  const viewerThreads = createThreadChannel({ node: viewer.node, network, engine, id: "viewer", clock });

  // The destination is the only "stop" a private drive has.
  const end = route.geometry[route.geometry.length - 1];
  const plan = { stops: [{ index: 1, lat: end[0], lon: end[1] }], dwellSeconds: 0 };

  const run = await driverThreads.publish({
    baseUrl: "https://maps.example/t",
    mode: THREAD_MODE.FINE,
    plan
  });
  assert.match(run.url, /#[A-Za-z0-9_-]{60}/, "the whole capability is a URL fragment");

  const updates = [];
  const follow = await viewerThreads.follow(run.url, {
    onUpdate: update => updates.push(update),
    cellContext: viewer.cellContext
  });

  // A read-only follower holds no bond and is in no gossip mesh for the
  // traffic channel, and still receives the thread: it is authenticated
  // by the thread key, not by admission.
  assert.equal(viewer.snapshot().readOnly, true);

  for (let i = 0; i < route.geometry.length; i += Math.ceil(route.geometry.length / 6)) {
    const [lat, lon] = route.geometry[i];
    now += 6000;
    const result = await run.handleFix({ lat, lon, speedMps: 14, nowMillis: now });
    assert.equal(typeof result.contributeTraffic, "boolean", "§10 rule 4 answers on every fix");
    await flush();
  }
  await until(() => updates.length >= 3);
  assert.ok(updates.length >= 3, `the follower received updates (${updates.length})`);

  const status = follow.status({ nowMillis: now });
  assert.equal(status.live, true);
  assert.equal(status.hasPosition, true);

  const position = await follow.position();
  assert.ok(position, "and can put the vehicle on a map");
  assert.ok(Number.isFinite(position.lat) && Number.isFinite(position.lon));

  const eta = await follow.eta({ plan, myStopIndex: 1, nowMillis: now });
  assert.ok(eta, "arrival is computed locally");
  assert.equal(eta.basis, "static-metric", "no live provider passed, so it says so");
  assert.equal(eta.positionBasis, "reported-position");

  // The same query under the traffic channel's provider is the §9 claim.
  const liveEta = await follow.eta({ plan, myStopIndex: 1, live: driver.provider(), nowMillis: now });
  assert.equal(liveEta.basis, "live-traffic");

  now += 1000;
  await run.finish({ nowMillis: now });
  await until(() => follow.latest()?.state === THREAD_STATE.COMPLETED);
  assert.equal(follow.latest().state, THREAD_STATE.COMPLETED);

  // A forged record on the right topic is refused: holding the link lets
  // you read a thread, never write one.
  const forged = new Uint8Array(64);
  await viewerThreads.deliver([...follow.topics][0], forged, now);
  assert.ok(viewerThreads.stats.dropped > 0, "malformed framing is dropped before subscriber crypto");

  follow.stop();
  driverThreads.close();
  viewerThreads.close();
});

test("the traffic channel ignores thread topics, and vice versa", async t => {
  const found = await fixture(t);
  if (!found) return;
  const { engine, route } = found;
  const network = createLoopbackNetwork();

  const a = await createMeshSession({ engine, network, id: "a", transport: "loopback" });
  const b = await createMeshSession({ engine, network, id: "b", transport: "loopback" });
  await a.followRoute(route);
  await b.followRoute(route);

  const threadsA = createThreadChannel({ node: a.node, network, engine, id: "a" });
  const threadsB = createThreadChannel({ node: b.node, network, engine, id: "b" });
  const run = await threadsA.publish({ mode: THREAD_MODE.FINE });
  const follow = await threadsB.follow(run.link);

  const [lat, lon] = route.geometry[4];
  await run.handleFix({ lat, lon, speedMps: 10, nowMillis: Date.now() });
  await until(() => follow.latest() != null);

  assert.ok(follow.latest(), "the thread arrived");
  assert.equal(b.node.store.size(), 0, "and not one byte of it entered the traffic store");
  assert.equal(b.snapshot().gossip.dropped, 0, "nor was it counted as a dropped contribution");

  threadsA.close();
  threadsB.close();
});

test("a late joiner pulls the gap from another follower, not from a server", async t => {
  const found = await fixture(t);
  if (!found) return;
  const { engine, route } = found;
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });

  const driver = await createMeshSession({ engine, network, id: "driver", transport: "loopback", clock });
  const early = await createMeshSession({ engine, network, id: "early", transport: "loopback", clock });
  const late = await createMeshSession({ engine, network, id: "late", transport: "loopback", clock });

  const driverThreads = createThreadChannel({ node: driver.node, network, engine, id: "driver", clock });
  const earlyThreads = createThreadChannel({ node: early.node, network, engine, id: "early", clock });
  const lateThreads = createThreadChannel({ node: late.node, network, engine, id: "late", clock });

  const run = await driverThreads.publish({ mode: THREAD_MODE.FINE });
  const earlyFollow = await earlyThreads.follow(run.link);

  // The drive happens while only one follower is watching.
  for (let i = 0; i < route.geometry.length && earlyFollow.stats.accepted < 4; i += 3) {
    const [lat, lon] = route.geometry[i];
    now += 6000;
    await run.handleFix({ lat, lon, speedMps: 14, nowMillis: now });
    await flush();
  }
  assert.ok(earlyFollow.stats.accepted >= 3, `the early follower saw the run (${earlyFollow.stats.accepted})`);

  // Now someone joins late, holding nothing but the link. The publisher
  // is deliberately taken out of the picture first: the whole claim is
  // that the *audience* is the availability, so recovering from the
  // publisher would prove nothing.
  driverThreads.close();

  const lateFollow = await lateThreads.follow(run.link);
  assert.ok(
    lateFollow.stats.accepted > 0,
    `the late joiner recovered history from another follower (${lateFollow.stats.accepted})`
  );
  assert.equal(
    lateFollow.latest().seq, earlyFollow.latest().seq,
    "and is caught up to the same record"
  );
  assert.ok(earlyThreads.stats.served > 0, "which the early follower served out of its own cache");
  assert.equal(
    lateFollow.status({ nowMillis: now }).hasPosition, true,
    "so it can draw the vehicle immediately rather than waiting for the next update"
  );

  earlyFollow.stop();
  lateFollow.stop();
  earlyThreads.close();
  lateThreads.close();
});

test("catch-up requests are padded, and a responder never reveals what it holds", async t => {
  const found = await fixture(t);
  if (!found) return;
  const { engine, route } = found;
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });

  const a = await createMeshSession({ engine, network, id: "a", transport: "loopback", clock });
  const b = await createMeshSession({ engine, network, id: "b", transport: "loopback", clock });
  const holder = createThreadChannel({ node: a.node, network, engine, id: "a", clock });
  const prober = createThreadChannel({ node: b.node, network, engine, id: "b", clock });

  const run = await holder.publish({ mode: THREAD_MODE.FINE });
  const [lat, lon] = route.geometry[2];
  now += 6000;
  await run.handleFix({ lat, lon, speedMps: 12, nowMillis: now });
  await flush();

  // A request for tags this peer has never seen answers in the same
  // shape as one for a tag it holds: entries, all with zero records.
  const strangers = Array.from({ length: 4 }, (_, i) => ({
    tag: Uint8Array.from([9, 9, 9, 9, 9, 9, 9, i]), sinceSeq: 0
  }));
  const request = buildThreadRequest({ epochPrefix8: a.node.epochPrefix8, wanted: strangers });
  assert.equal(request.entries.length, 4, "requests are padded to a legal size");

  const answer = holder.serveCatchUp(request.bytes, now);
  assert.ok(answer, "an unknown tag still gets a well-formed answer");
  const decoded = decodeThreadResponse(answer);
  assert.equal(decoded.entries.length, 4);
  assert.ok(
    decoded.entries.every(entry => entry.records.length === 0),
    "and it is indistinguishable from a tag with nothing new"
  );

  // Padding is what hides which entry a joiner came for: a request for
  // one real tag still goes out carrying four.
  const oneReal = buildThreadRequest({
    epochPrefix8: a.node.epochPrefix8,
    wanted: [{ tag: strangers[0].tag, sinceSeq: 0 }]
  });
  assert.equal(oneReal.entries.length, 4, "one wanted tag still travels with three decoys");
  assert.equal(oneReal.realCount, 1);

  holder.close();
  prober.close();
});

test("a coarse run never claims a live position, because it withheld one", async t => {
  const found = await fixture(t);
  if (!found) return;
  const { engine, route } = found;
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });

  const bus = await createMeshSession({ engine, network, id: "bus", transport: "loopback", clock });
  const parent = await createMeshSession({ engine, network, id: "parent", transport: "loopback", clock });
  const busThreads = createThreadChannel({ node: bus.node, network, engine, id: "bus", clock });
  const parentThreads = createThreadChannel({ node: parent.node, network, engine, id: "parent", clock });

  // §11: coarse is the setting for anything carrying children, and it is
  // a decision about what a leaked link is worth — not a bandwidth knob.
  const run = await busThreads.publish({ mode: THREAD_MODE.COARSE });
  const follow = await parentThreads.follow(run.link);

  const [lat, lon] = route.geometry[6];
  now += 6000;
  await run.handleFix({ lat, lon, speedMps: 11, nowMillis: now });
  await until(() => follow.latest() != null);

  const update = follow.latest();
  assert.ok(update, "the run is being received");
  assert.equal(update.state, THREAD_STATE.EN_ROUTE, "state and progress still travel");
  assert.equal(update.segment, null, "but no position does — that is the point of coarse");

  const status = follow.status({ nowMillis: now, hasTraffic: true });
  assert.equal(status.live, true, "the thread itself is live");
  assert.equal(status.hasPosition, false);
  // The bug this guards: `live && hasTraffic` alone put a coarse run in
  // the top row, so the card said "live position" for a run whose
  // publisher deliberately withheld it.
  assert.equal(status.row, "traffic-only");
  assert.match(status.claim, /position unknown/);
  assert.doesNotMatch(status.claim, /live position/);

  assert.equal(await follow.position(), null, "and there is nothing to draw");

  follow.stop();
  busThreads.close();
  parentThreads.close();
});

test("pairing carries a device card console<->phone over the mesh, and enrols only on confirm", async t => {
  const found = await fixture(t);
  if (!found) return;
  const { engine } = found;
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });

  const consoleNode = await createMeshSession({
    engine, network, id: "console", transport: "loopback", constants: DEFAULT_CONSTANTS, clock
  });
  const phoneNode = await createMeshSession({
    engine, network, id: "phone", transport: "loopback", constants: DEFAULT_CONSTANTS, clock
  });
  const consoleThreads = createThreadChannel({ node: consoleNode.node, network, engine, id: "console", clock });
  const phoneThreads = createThreadChannel({ node: phoneNode.node, network, engine, id: "phone", clock });

  const { generateDeviceKeypair } = await import("../src/pulsemesh/thread_seal.js");
  const { decodePairingOffer } = await import("../src/pulsemesh/thread_pair.js");

  // Console mints an offer (this is what becomes a QR on its screen).
  const pairing = await consoleThreads.startPairing({
    label: "Dispatch", addresses: ["/dns4/mesh.example/tcp/443/tls/ws/p2p/12D3KooWabc"],
    nowMillis: now
  });

  // Phone scans it and replies over the mesh.
  const device = await generateDeviceKeypair();
  const offer = decodePairingOffer(pairing.offerBytes);
  const phoneHandle = await phoneThreads.replyToPairing({
    offer, devicePublicKey: device.publicKey, devicePrivateKey: device.privateKey,
    name: "Marco — Pixel", nowMillis: now
  });

  // The console sees exactly one candidate, and has enrolled nobody.
  await until(() => pairing.candidates.length === 1);
  assert.equal(pairing.candidates.length, 1, "the reply arrived as a candidate");
  const candidate = pairing.candidates[0];
  assert.equal(candidate.name, "Marco — Pixel");
  assert.equal(candidate.fingerprint.length, 8, "a fingerprint to read aloud");

  // A human confirms it (the read-aloud match). Only now is an ack sent.
  const chosen = await pairing.confirm(candidate.publicKeyHex, { name: "Marco — Pixel" });
  assert.ok(chosen, "confirm sealed an ack to the chosen device");

  // The phone learns it was chosen, and with which fingerprint.
  await until(() => phoneHandle.ack != null);
  assert.ok(phoneHandle.ack, "the phone received its ack");
  assert.equal(phoneHandle.ack.fingerprint, candidate.fingerprint);
  assert.equal(phoneHandle.ack.label, "Marco — Pixel");

  pairing.stop();
  phoneHandle.stop();
  consoleThreads.close();
  phoneThreads.close();
});

test("a second phone answering the same offer is a separate candidate, not an enrolment", async t => {
  const found = await fixture(t);
  if (!found) return;
  const { engine } = found;
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });

  const consoleNode = await createMeshSession({ engine, network, id: "c", transport: "loopback", constants: DEFAULT_CONSTANTS, clock });
  const p1 = await createMeshSession({ engine, network, id: "p1", transport: "loopback", constants: DEFAULT_CONSTANTS, clock });
  const p2 = await createMeshSession({ engine, network, id: "p2", transport: "loopback", constants: DEFAULT_CONSTANTS, clock });
  const consoleThreads = createThreadChannel({ node: consoleNode.node, network, engine, id: "c", clock });
  const t1 = createThreadChannel({ node: p1.node, network, engine, id: "p1", clock });
  const t2 = createThreadChannel({ node: p2.node, network, engine, id: "p2", clock });

  const { generateDeviceKeypair } = await import("../src/pulsemesh/thread_seal.js");
  const { decodePairingOffer } = await import("../src/pulsemesh/thread_pair.js");

  const pairing = await consoleThreads.startPairing({ label: "Dispatch", nowMillis: now });
  const offer = decodePairingOffer(pairing.offerBytes);

  const d1 = await generateDeviceKeypair();
  const d2 = await generateDeviceKeypair();
  await t1.replyToPairing({ offer, devicePublicKey: d1.publicKey, devicePrivateKey: d1.privateKey, name: "Phone one", nowMillis: now });
  await t2.replyToPairing({ offer, devicePublicKey: d2.publicKey, devicePrivateKey: d2.privateKey, name: "Phone two", nowMillis: now });

  await until(() => pairing.candidates.length === 2);
  assert.equal(pairing.candidates.length, 2, "both phones show as distinct candidates for a human to choose between");
  const names = pairing.candidates.map(c => c.name).sort();
  assert.deepEqual(names, ["Phone one", "Phone two"]);

  pairing.stop();
  consoleThreads.close();
  t1.close();
  t2.close();
});
