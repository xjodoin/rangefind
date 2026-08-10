// M3 — wire: real js-libp2p transports over TCP. A bonded contributor
// gossips proofless records to two consumers through GossipSub
// (StrictNoSign, sha256 message ids); a late joiner recovers the set
// through the /rangefind/pulsemesh/1/sync stream protocol; killing the
// contributor loses nothing that other peers already replicated.
//
// Skips cleanly when the optional libp2p peer dependencies are absent.

import assert from "node:assert/strict";
import test from "node:test";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { DEFAULT_CONSTANTS, zoneOfDetailCell } from "../src/pulsemesh/bins.js";
import { MeshNode } from "../src/pulsemesh/node.js";
import { createContributor } from "../src/pulsemesh/contribute.js";
import { encodePMD1 } from "../src/pulsemesh/codec.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const EPOCH_HEX = toHex(sha256Utf8("pulsemesh-wire-test"));
// EMIT_INTERVAL is effectively disabled: cadence is covered by the
// pipeline tests; this test is about bytes crossing real sockets.
// BOND_BIRTHDAY_BITS 16 keeps every admission mint at ~1 ms while still
// exercising the full §5.4 path: real mint, real PMA1 stream, real
// verification against the connection's peerId.
const CONSTANTS = { ...DEFAULT_CONSTANTS, BOND_BIRTHDAY_BITS: 16, EMIT_INTERVAL: 0.01 };

// A tiny static world: every leaf maps to one z15 cell in one zone.
const CELL = { x: 144 * 64 + 5, y: 180 * 64 + 9 };
const cellOf = record => (record.leafCell < 64 ? { x: CELL.x + (record.leafCell % 8), y: CELL.y } : null);
const cellContext = leafCell => (leafCell < 64
  ? { polylineCount: 64, classOf: () => "secondary", metersOf: () => 400 }
  : null);
const ZONE = zoneOfDetailCell(CELL);

async function waitFor(check, { timeoutMs = 15000, stepMs = 100, label = "condition" } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await new Promise(resolve => setTimeout(resolve, stepMs));
  }
}

test("M3: nodes converge over real libp2p TCP transports", async t => {
  let createPulseMeshHost;
  let createLibp2pNetwork;
  try {
    ({ createPulseMeshHost, createLibp2pNetwork } = await import("../src/pulsemesh/libp2p.js"));
    await import("libp2p");
  } catch {
    t.skip("libp2p optional dependencies not installed");
    return;
  }

  const hosts = [];
  const networks = [];
  const makePeer = async () => {
    const host = await createPulseMeshHost();
    const network = createLibp2pNetwork(host);
    hosts.push(host);
    networks.push(network);
    return { host, network };
  };
  t.after(async () => {
    for (const network of networks) await network.close();
    await Promise.all(hosts.map(host => host.stop().catch(() => {})));
  });

  const carPeer = await makePeer();
  const consumerA = await makePeer();
  const consumerB = await makePeer();

  // Star-plus-edge topology: the car talks to both consumers, and the
  // consumers know each other (gossip and sync need routes after churn).
  await carPeer.host.dial(consumerA.host.getMultiaddrs()[0]);
  await carPeer.host.dial(consumerB.host.getMultiaddrs()[0]);
  await consumerA.host.dial(consumerB.host.getMultiaddrs()[0]);

  const makeNode = ({ host, network }) => {
    const node = new MeshNode({
      id: host.peerId.toString(),
      epochHex: EPOCH_HEX,
      constants: CONSTANTS,
      cellOf,
      cellContext,
      network,
      transport: "wire"
    });
    node.subscribeZones([ZONE]);
    return node;
  };
  const carNode = makeNode(carPeer);
  const nodeA = makeNode(consumerA);
  const nodeB = makeNode(consumerB);

  // §5.4: every peer mints and presents its admission bond — consumers
  // too, because they relay gossip and serve snapshots, and both paths
  // are vouched by the delivering peer's bond.
  for (const network of networks) await network.ready;
  for (const network of networks) assert.ok(await network.mintBond(), "bond mint");
  await waitFor(
    () => [carNode, nodeA, nodeB].every(node => node.stats.bondsAccepted >= 2),
    { label: "bonds exchanged across the mesh" }
  );

  // GossipSub meshes need the subscriptions to propagate before publishing.
  const topicWithPeers = () => [...carNode.subscribedTopics]
    .some(topic => carPeer.host.services.pubsub.getSubscribers(topic).length >= 2);
  await waitFor(topicWithPeers, { label: "gossip mesh formation" });

  // Three simulated vehicles drive eight segments through the real
  // contributor pipeline — snap, quality, smoothing, cadence — all
  // publishing proofless records from the bonded car host.
  const emitted = [];
  for (let vehicle = 0; vehicle < 3; vehicle++) {
    const contributor = createContributor({
      epoch32: carNode.epoch32,
      epochPrefix8: carNode.epochPrefix8,
      snap: async fix => fix.match,
      publish: async result => {
        emitted.push(result);
        carNode.publishRecord(result.record);
      },
      constants: CONSTANTS,
      metersOf: () => 400
    });
    for (let segment = 0; segment < 8; segment++) {
      await contributor.handleFix({
        match: { segment: `${segment}/3/0`, distMeters: 3, ratio: 0.5, snappedLatE7: 0, snappedLonE7: 0 },
        speedMps: 2,
        nowMillis: Date.now()
      });
      await new Promise(resolve => setTimeout(resolve, 60));
    }
  }
  assert.equal(emitted.length, 24, "every fix emitted");

  // Both consumers converge on the full set via gossip alone.
  await waitFor(() => nodeA.store.size() === 24 && nodeB.store.size() === 24, {
    label: `gossip convergence (A=${nodeA.store.size()}, B=${nodeB.store.size()})`
  });
  const digestOf = node => toHex(encodePMD1({ epochPrefix8: node.epochPrefix8, ...node.store.digestForZone(ZONE, Date.now()) }));
  assert.equal(digestOf(nodeA), digestOf(nodeB), "consumer digests identical over the wire");
  assert.ok(nodeA.stats.gossipAccepted >= 24, "records passed full wire validation including rule 5 bond vouching");

  // Aggregates come out corroborated: 3 vehicles per segment.
  const aggregates = nodeA.provider.aggregates(Date.now());
  assert.equal(aggregates.size, 8, "one aggregate per driven segment");
  for (const aggregate of aggregates.values()) {
    assert.equal(aggregate.n, 3, "three distinct reports per segment");
  }

  // Churn: the contributor vanishes. Nothing already replicated is lost.
  await carPeer.host.stop();
  assert.equal(nodeA.store.size(), 24, "records survive the contributor leaving");
  assert.equal(nodeB.store.size(), 24);

  // A late joiner connects to one survivor and recovers the whole set
  // through the padded PMQ1/PMS1 sync-stream path.
  const latePeer = await makePeer();
  await latePeer.host.dial(consumerA.host.getMultiaddrs()[0]);
  const lateNode = makeNode(latePeer);
  await waitFor(() => latePeer.host.getPeers().length >= 1, { label: "late joiner connectivity" });
  const survivorId = consumerA.host.peerId.toString();
  await waitFor(async () => {
    await lateNode.antiEntropyWith(survivorId, ZONE);
    return lateNode.store.size() === 24;
  }, { label: `late joiner recovery (${lateNode.store.size()}/24)`, stepMs: 250 });
  assert.equal(digestOf(lateNode), digestOf(nodeA), "late joiner digest matches the survivor");
  assert.ok(lateNode.stats.cellsRequested >= lateNode.stats.cellsWanted, "sync fetches were padded");
  assert.ok(latePeer.network.stats.requests > 0, "recovery actually used the sync stream protocol");
});

test("M3: framed sync messages survive arbitrary TCP fragmentation", async t => {
  let internals;
  try {
    internals = await import("../src/pulsemesh/libp2p.js");
  } catch {
    t.skip("libp2p adapter unavailable");
    return;
  }
  // The adapter's frame assembler is not exported; exercise it through a
  // real pair of hosts by sending a large PMQ1 (a 32-cell request, whose
  // length prefix needs two varint bytes) and a large PMS1 response.
  // A byte-at-a-time delivery is the worst case a TCP stream can produce,
  // and it is the case a naive varint read gets wrong: the first byte of
  // a two-byte prefix parses as a complete, much smaller length.
  const { createPulseMeshHost, createLibp2pNetwork, frameAssembler } = internals;

  // First, the assembler directly, against every split that matters.
  const { pushVarint } = await import("../src/binary.js");
  const frame = payload => {
    const header = [];
    pushVarint(header, payload.length);
    return Uint8Array.from([...header, ...payload]);
  };
  // 300 bytes needs the two-byte prefix `ac 02`; a reader that treats
  // `ac` as complete sees length 44. A payload of 128 needs `80 01`,
  // whose first byte alone reads as length 0 — framing an empty message
  // and swallowing the real one.
  for (const size of [1, 127, 128, 300, 16384]) {
    const payload = Uint8Array.from({ length: size }, (_, i) => (i * 7) & 0xff);
    const bytes = frame(payload);
    for (const chunkSize of [1, 2, 3, 7, bytes.length]) {
      const received = [];
      const assemble = frameAssembler(complete => received.push(Uint8Array.from(complete)));
      for (let offset = 0; offset < bytes.length; offset += chunkSize) {
        assemble(bytes.subarray(offset, Math.min(bytes.length, offset + chunkSize)));
      }
      assert.equal(received.length, 1, `size ${size} in ${chunkSize}-byte chunks: exactly one frame`);
      assert.deepEqual(received[0], payload, `size ${size} in ${chunkSize}-byte chunks: payload intact`);
    }
  }
  // Several frames arriving coalesced in one chunk must all be recovered.
  {
    const payloads = [Uint8Array.of(1), Uint8Array.from({ length: 200 }, () => 9), Uint8Array.of(2, 3)];
    const joined = payloads.map(frame);
    const all = new Uint8Array(joined.reduce((sum, part) => sum + part.length, 0));
    let offset = 0;
    for (const part of joined) { all.set(part, offset); offset += part.length; }
    const received = [];
    frameAssembler(complete => received.push(Uint8Array.from(complete)))(all);
    assert.equal(received.length, 3, "coalesced frames are all recovered");
    assert.deepEqual(received[1], payloads[1]);
  }

  const hosts = [];
  const networks = [];
  t.after(async () => {
    for (const network of networks) await network.close();
    await Promise.all(hosts.map(host => host.stop().catch(() => {})));
  });

  const server = await createPulseMeshHost();
  const client = await createPulseMeshHost();
  hosts.push(server, client);
  const serverNetwork = createLibp2pNetwork(server);
  const clientNetwork = createLibp2pNetwork(client);
  networks.push(serverNetwork, clientNetwork);

  const makeNode = (host, network) => {
    const node = new MeshNode({
      id: host.peerId.toString(),
      epochHex: EPOCH_HEX,
      constants: CONSTANTS,
      cellOf,
      cellContext,
      network,
      transport: "wire"
    });
    node.subscribeZones([ZONE]);
    return node;
  };
  const serverNode = makeNode(server, serverNetwork);
  const clientNode = makeNode(client, clientNetwork);
  await serverNetwork.ready;
  await clientNetwork.ready;
  await client.dial(server.getMultiaddrs()[0]);
  // The client pulls snapshots from the server, so the server's bond is
  // what vouches for every record in them (§5.4 snapshot vouching).
  assert.ok(await serverNetwork.mintBond(), "server bond mint");
  await waitFor(() => clientNode.isBonded(serverNode.id), { label: "server bond registered on the client" });

  // Fill the server's store so the snapshot response is large enough to
  // need a multi-byte length prefix of its own.
  const { PROOF_BOND, decodePMC1, encodePMC1, encodePMQ1 } = await import("../src/pulsemesh/codec.js");
  const nowMillis = Date.now();
  for (let i = 0; i < 40; i++) {
    const reportId = new Uint8Array(16);
    globalThis.crypto.getRandomValues(reportId);
    const fields = {
      epochPrefix8: serverNode.epochPrefix8,
      leafCell: i % 8,
      geomRef: i * 2,
      timeBucket: Math.floor(nowMillis / 15000),
      speedBin: 5,
      qualityBin: 7,
      meters: 400,
      ttlSeconds: 90,
      reportId,
      proofType: PROOF_BOND,
      proof: new Uint8Array(0)
    };
    serverNode.store.addContribution(decodePMC1(encodePMC1(fields).bytes), { nowMillis });
  }
  assert.equal(serverNode.store.size(), 40);

  // A 32-cell PMQ1 exceeds 128 bytes, so its own length prefix is two
  // bytes — the exact shape that a byte-at-a-time reader must not
  // misparse.
  const cells = Array.from({ length: 32 }, (_, i) => ({ x: CELL.x + (i % 8), y: CELL.y }));
  const request = encodePMQ1({ epochPrefix8: clientNode.epochPrefix8, cells });
  assert.ok(request.length > 128, `request needs a multi-byte length prefix (${request.length} bytes)`);

  const response = await clientNetwork.request(clientNode.id, serverNode.id, request);
  assert.ok(response, "large request answered");
  const merged = clientNode.mergeSnapshot(response, serverNode.id, Date.now());
  assert.equal(merged, 40, "every record in the large response survived framing");
  assert.equal(clientNode.store.size(), 40);
});

test("M3: two OS processes converge (keeper child + contributor parent)", async t => {
  let createPulseMeshHost;
  let createLibp2pNetwork;
  try {
    ({ createPulseMeshHost, createLibp2pNetwork } = await import("../src/pulsemesh/libp2p.js"));
    await import("libp2p");
  } catch {
    t.skip("libp2p optional dependencies not installed");
    return;
  }

  // A real §12 keeper in its own OS process.
  const repoRoot = join(fileURLToPath(import.meta.url), "..", "..");
  const keeper = spawn(process.execPath, [
    join(repoRoot, "scripts", "pulsemesh_keeper.mjs"),
    `--epoch=${EPOCH_HEX}`,
    "--test-world",
    "--bond-bits=16",
    "--stats-seconds=3600"
  ], { cwd: repoRoot, stdio: ["ignore", "pipe", "pipe"] });
  t.after(() => keeper.kill("SIGTERM"));

  const keeperInfo = await new Promise((resolve, reject) => {
    let buffered = "";
    const timer = setTimeout(() => reject(new Error(`keeper never announced. stderr: ${errors}`)), 20000);
    let errors = "";
    keeper.stderr.on("data", chunk => { errors += chunk; });
    keeper.stdout.on("data", chunk => {
      buffered += chunk;
      for (const line of buffered.split("\n")) {
        try {
          const parsed = JSON.parse(line);
          if (parsed.event === "listening") {
            clearTimeout(timer);
            resolve(parsed);
            return;
          }
        } catch {
          // partial line; keep buffering
        }
      }
    });
    keeper.on("exit", code => reject(new Error(`keeper exited early (${code}). stderr: ${errors}`)));
  });
  assert.ok(keeperInfo.multiaddrs.length, "keeper listening");

  // This process: a contributor node dialing the keeper.
  const host = await createPulseMeshHost({ bootstrapPeers: [keeperInfo.multiaddrs[0]] });
  const network = createLibp2pNetwork(host);
  t.after(async () => {
    await network.close();
    await host.stop().catch(() => {});
  });
  const node = new MeshNode({
    id: host.peerId.toString(),
    epochHex: EPOCH_HEX,
    constants: CONSTANTS,
    cellOf,
    cellContext,
    network,
    transport: "wire"
  });
  node.subscribeZones([ZONE]);
  await network.ready;
  // §5.4: the keeper only accepts this contributor's proofless records if
  // this peer's bond reached it — mintBond presents to the already-dialed
  // keeper immediately.
  assert.ok(await network.mintBond(), "contributor bond mint");
  await waitFor(
    () => [...node.subscribedTopics].some(topic => host.services.pubsub.getSubscribers(topic).length >= 1),
    { label: "keeper joins the gossip mesh" }
  );

  const contributor = createContributor({
    epoch32: node.epoch32,
    epochPrefix8: node.epochPrefix8,
    snap: async fix => fix.match,
    publish: async result => node.publishRecord(result.record),
    constants: CONSTANTS,
    metersOf: () => 400
  });
  for (let segment = 0; segment < 6; segment++) {
    await contributor.handleFix({
      match: { segment: `${segment}/2/1`, distMeters: 3, ratio: 0.5, snappedLatE7: 0, snappedLonE7: 0 },
      speedMps: 3,
      nowMillis: Date.now()
    });
    await new Promise(resolve => setTimeout(resolve, 40));
  }

  // Cross-process convergence, checked the strong way: this process asks
  // the keeper process for its zone digest over the sync stream and
  // compares byte-for-byte against its own.
  const localDigest = () => toHex(encodePMD1({
    epochPrefix8: node.epochPrefix8,
    ...node.store.digestForZone(ZONE, Date.now())
  }));
  const { encodePMG1 } = await import("../src/pulsemesh/codec.js");
  const remoteDigest = async () => {
    const response = await network.request(
      node.id,
      keeperInfo.peerId,
      encodePMG1({ epochPrefix8: node.epochPrefix8, zoneX: ZONE.x, zoneY: ZONE.y })
    );
    return response ? toHex(response) : null;
  };
  await waitFor(async () => (await remoteDigest()) === localDigest(), {
    label: "two OS processes hold byte-identical zone digests",
    stepMs: 250
  });
  assert.equal(node.store.size(), 6, "all six records live locally");
});

test("M3 + §5.4: bonded peers exchange PMA1 and proofless records flow", async t => {
  let createPulseMeshHost;
  let createLibp2pNetwork;
  try {
    ({ createPulseMeshHost, createLibp2pNetwork } = await import("../src/pulsemesh/libp2p.js"));
    await import("libp2p");
  } catch {
    t.skip("libp2p optional dependencies not installed");
    return;
  }
  const { PROOF_BOND } = await import("../src/pulsemesh/codec.js");
  // B=16 keeps the mint at ~1 ms while exercising the full path: real
  // mint, real PMA1 over a real TCP stream, real verification against the
  // connection's peerId, real proofless records over GossipSub.
  const constants = { ...CONSTANTS, BOND_BIRTHDAY_BITS: 16 };

  const hosts = [];
  const networks = [];
  const makePeer = async () => {
    const host = await createPulseMeshHost();
    const network = createLibp2pNetwork(host);
    hosts.push(host);
    networks.push(network);
    return { host, network };
  };
  t.after(async () => {
    for (const network of networks) await network.close();
    await Promise.all(hosts.map(host => host.stop().catch(() => {})));
  });

  const carPeer = await makePeer();
  const consumer = await makePeer();
  await carPeer.host.dial(consumer.host.getMultiaddrs()[0]);

  const makeNode = ({ host, network }) => {
    const node = new MeshNode({
      id: host.peerId.toString(),
      epochHex: EPOCH_HEX,
      constants,
      cellOf,
      cellContext,
      network,
      transport: "wire"
    });
    node.subscribeZones([ZONE]);
    return node;
  };
  const carNode = makeNode(carPeer);
  const consumerNode = makeNode(consumer);
  await carPeer.network.ready;
  await consumer.network.ready;

  // The car mints and presents its bond; the consumer's node verifies it
  // against the car's connection peerId, never against anything the car
  // claimed.
  assert.ok(await carPeer.network.mintBond(), "mint succeeds at B=16");
  await waitFor(() => consumerNode.isBonded(carNode.id), { label: "bond registered on the consumer" });
  assert.equal(consumerNode.stats.bondsAccepted, 1);

  await waitFor(
    () => [...carNode.subscribedTopics].some(topic => carPeer.host.services.pubsub.getSubscribers(topic).length >= 1),
    { label: "gossip mesh formation" }
  );

  // Proofless contributor: proofType 3, zero mining, instant emission.
  const contributor = createContributor({
    epoch32: carNode.epoch32,
    epochPrefix8: carNode.epochPrefix8,
    snap: async fix => fix.match,
    publish: async result => carNode.publishRecord(result.record),
    constants,
    proofType: PROOF_BOND,
    metersOf: () => 400
  });
  for (let segment = 0; segment < 4; segment++) {
    await contributor.handleFix({
      match: { segment: `${segment}/4/0`, distMeters: 3, ratio: 0.5, snappedLatE7: 0, snappedLonE7: 0 },
      speedMps: 2,
      nowMillis: Date.now()
    });
    await new Promise(resolve => setTimeout(resolve, 50));
  }

  await waitFor(() => consumerNode.store.size() === 4, {
    label: `proofless records accepted over the wire (have ${consumerNode.store.size()})`
  });
  assert.ok(consumerNode.stats.gossipAccepted >= 4, "rule 5 accepted proofType 3 from the bonded deliverer");

  // Control: an unbonded third peer publishing proofType 3 is dropped by
  // rule 5 on every receiver — the bond is what admits, not the format.
  const rogue = await makePeer();
  await rogue.host.dial(consumer.host.getMultiaddrs()[0]);
  const rogueNode = makeNode(rogue);
  await rogue.network.ready;
  await waitFor(
    () => [...rogueNode.subscribedTopics].some(topic => rogue.host.services.pubsub.getSubscribers(topic).length >= 1),
    { label: "rogue joins the gossip mesh" }
  );
  const rogueContributor = createContributor({
    epoch32: rogueNode.epoch32,
    epochPrefix8: rogueNode.epochPrefix8,
    snap: async fix => fix.match,
    publish: async result => rogueNode.publishRecord(result.record),
    constants,
    proofType: PROOF_BOND,
    metersOf: () => 400
  });
  await rogueContributor.handleFix({
    match: { segment: "9/4/0", distMeters: 3, ratio: 0.5, snappedLatE7: 0, snappedLonE7: 0 },
    speedMps: 2,
    nowMillis: Date.now()
  });
  // Give gossip time to deliver, then confirm the drop landed on rule 5.
  await waitFor(() => (consumerNode.stats.dropsByRule.rule5 ?? 0) >= 1, {
    label: "rogue proofless record dropped by rule 5"
  });
  assert.equal(consumerNode.store.size(), 4, "the rogue record was not stored");
});

test("§11.6: a read-only consumer converges with no bond and no gossip membership", async t => {
  let createPulseMeshHost;
  let createLibp2pNetwork;
  try {
    ({ createPulseMeshHost, createLibp2pNetwork } = await import("../src/pulsemesh/libp2p.js"));
    await import("libp2p");
  } catch {
    t.skip("libp2p optional dependencies not installed");
    return;
  }

  const hosts = [];
  const networks = [];
  const makePeer = async () => {
    const host = await createPulseMeshHost();
    const network = createLibp2pNetwork(host);
    hosts.push(host);
    networks.push(network);
    return { host, network };
  };
  t.after(async () => {
    for (const network of networks) await network.close();
    await Promise.all(hosts.map(host => host.stop().catch(() => {})));
  });

  // A bonded contributor and a bonded relay form the working mesh.
  const carPeer = await makePeer();
  const relayPeer = await makePeer();
  await carPeer.host.dial(relayPeer.host.getMultiaddrs()[0]);
  const makeNode = ({ host, network }, options = {}) => {
    const node = new MeshNode({
      id: host.peerId.toString(),
      epochHex: EPOCH_HEX,
      constants: CONSTANTS,
      cellOf,
      cellContext,
      network,
      transport: "wire",
      ...options
    });
    node.subscribeZones([ZONE]);
    return node;
  };
  const carNode = makeNode(carPeer);
  const relayNode = makeNode(relayPeer);
  await carPeer.network.ready;
  await relayPeer.network.ready;
  assert.ok(await carPeer.network.mintBond());
  assert.ok(await relayPeer.network.mintBond());
  await waitFor(() => relayNode.isBonded(carNode.id), { label: "car bonded on the relay" });
  await waitFor(
    () => [...carNode.subscribedTopics].some(topic => carPeer.host.services.pubsub.getSubscribers(topic).length >= 1),
    { label: "gossip mesh formation" }
  );

  const contributor = createContributor({
    epoch32: carNode.epoch32,
    epochPrefix8: carNode.epochPrefix8,
    snap: async fix => fix.match,
    publish: async result => carNode.publishRecord(result.record),
    constants: CONSTANTS,
    metersOf: () => 400
  });
  for (let segment = 0; segment < 5; segment++) {
    await contributor.handleFix({
      match: { segment: `${segment}/6/0`, distMeters: 3, ratio: 0.5, snappedLatE7: 0, snappedLonE7: 0 },
      speedMps: 2,
      nowMillis: Date.now()
    });
    await new Promise(resolve => setTimeout(resolve, 40));
  }
  await waitFor(() => relayNode.store.size() === 5, { label: "relay converged over gossip" });

  // The browser at home: read-only, no bond ever minted, no gossip topic
  // ever joined — it pulls from the bonded relay on tick().
  const homePeer = await makePeer();
  await homePeer.host.dial(relayPeer.host.getMultiaddrs()[0]);
  await homePeer.network.ready;
  const homeNode = makeNode(homePeer, { readOnly: true });
  assert.equal(homeNode.subscribedTopics.size, 0, "a read-only node joins no gossip topics");
  assert.throws(() => homeNode.publishRecord({ bytes: new Uint8Array(4) }), /read-only/i);

  await waitFor(async () => {
    await homeNode.tick();
    return homeNode.store.size() === 5;
  }, { label: `read-only pull convergence (${homeNode.store.size()}/5)`, stepMs: 250 });

  const digestOf = node => toHex(encodePMD1({ epochPrefix8: node.epochPrefix8, ...node.store.digestForZone(ZONE, Date.now()) }));
  assert.equal(digestOf(homeNode), digestOf(relayNode), "read-only digest matches the mesh");
  assert.equal(homePeer.network.stats.bondsSent, 0, "no bond was ever minted or presented");
  // One vehicle per segment stays below AGG_MIN_REPORTS by design; the
  // records themselves being present and digest-identical is the claim.
  assert.equal(homeNode.store.contributionsForSegment("0/12").length, 1, "pulled records are queryable");
});

test("§11.6 + threads: a read-only home viewer follows a thread with zero bonds", async t => {
  let createPulseMeshHost;
  let createLibp2pNetwork;
  try {
    ({ createPulseMeshHost, createLibp2pNetwork } = await import("../src/pulsemesh/libp2p.js"));
    await import("libp2p");
  } catch {
    t.skip("libp2p optional dependencies not installed");
    return;
  }
  const { generateThreadKeypair } = await import("../src/pulsemesh/thread_crypto.js");
  const { encodeThreadLink, decodeThreadLink, THREAD_MODE } = await import("../src/pulsemesh/thread_codec.js");
  const { createThreadPublisher } = await import("../src/pulsemesh/thread_publish.js");
  const { createThreadSubscriber } = await import("../src/pulsemesh/thread_consume.js");
  const { fromHex } = await import("../src/pulsemesh/sha256.js");

  const hosts = [];
  const networks = [];
  const makePeer = async () => {
    const host = await createPulseMeshHost();
    const network = createLibp2pNetwork(host);
    hosts.push(host);
    networks.push(network);
    return { host, network };
  };
  t.after(async () => {
    for (const network of networks) await network.close();
    await Promise.all(hosts.map(host => host.stop().catch(() => {})));
  });

  // The bus and the home viewer. The viewer's traffic node is read-only —
  // no traffic-zone topics, no bond — and the thread channel rides beside
  // it: thread topics are the reserved `t` namespace that MeshNode
  // ignores, subscribed directly on pubsub, and every record is verified
  // end-to-end by the thread key's signature, so the §5.4 bond machinery
  // has no role on this channel at all.
  const busPeer = await makePeer();
  const homePeer = await makePeer();
  await homePeer.host.dial(busPeer.host.getMultiaddrs()[0]);
  const homeNode = new MeshNode({
    id: homePeer.host.peerId.toString(),
    epochHex: EPOCH_HEX,
    constants: CONSTANTS,
    cellOf,
    cellContext,
    network: homePeer.network,
    transport: "wire",
    readOnly: true
  });
  homeNode.subscribeZones([ZONE]);
  assert.equal(homeNode.subscribedTopics.size, 0, "read-only: no traffic topics");

  const epoch32 = fromHex(EPOCH_HEX);
  const { threadTopic } = await import("../src/pulsemesh/thread_crypto.js");
  const keypair = await generateThreadKeypair();
  const now = Date.now();
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed,
    epoch32,
    mode: THREAD_MODE.FINE,
    publish: async emitted => {
      // The bus publishes each PMT1 to its tag's derived topic — the
      // ordinary transport wiring for the thread channel.
      const topic = threadTopic(EPOCH_HEX.slice(0, 16), emitted.tag);
      await busPeer.host.services.pubsub.publish(topic, emitted.bytes).catch(() => {});
    }
  });

  // The link — a 78-byte capability-separated fragment — is all the home
  // viewer holds.
  const link = decodeThreadLink(encodeThreadLink({
    threadSecret: publisher.threadSecret, rootPublicKey: publisher.rootPublicKey,
    epochPrefix8: epoch32.subarray(0, 8),
    notAfter: Math.floor(now / 1000) + 3600
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32 });
  const threadTopics = await subscriber.topics();
  assert.ok(threadTopics.length >= 1, "link derives the thread topics");

  const accepted = [];
  homePeer.host.services.pubsub.addEventListener("gossipsub:message", async event => {
    const { msg } = event.detail;
    if (!threadTopics.includes(msg.topic)) return;
    const verdict = await subscriber.accept(msg.data, { nowMillis: Date.now() });
    if (verdict.ok) accepted.push(verdict.update);
  });
  for (const topic of threadTopics) homePeer.host.services.pubsub.subscribe(topic);
  await waitFor(
    () => threadTopics.some(topic => busPeer.host.services.pubsub.getSubscribers(topic).length >= 1),
    { label: "thread topic mesh formation" }
  );

  // The bus drives; every published fix goes out through the publish
  // callback above.
  let publishedCount = 0;
  for (let i = 0; i < 4; i++) {
    const result = await publisher.handleFix({
      lat: 45.5 + i * 0.001,
      lon: -73.6,
      speedMps: 11,
      nowMillis: Date.now(),
      match: { segment: `${i}/2/0`, distMeters: 3, ratio: 0.4, snappedLatE7: 455000000, snappedLonE7: -736000000 }
    });
    if (result.published) publishedCount++;
    await new Promise(resolve => setTimeout(resolve, 80));
  }
  assert.ok(publishedCount >= 1, `the bus published updates (${publishedCount})`);

  await waitFor(() => accepted.length >= 1, { label: `thread updates verified at home (${accepted.length})` });
  assert.ok(Number.isFinite(accepted[0].seq), "the update decrypted and verified under the thread key");
  assert.equal(homePeer.network.stats.bondsSent, 0, "the home viewer never minted a bond");
  assert.equal(homeNode.stats.bondsAccepted, 0, "and never needed anyone else's");
});
