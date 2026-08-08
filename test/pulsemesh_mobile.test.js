// The mobile contributor (pulsemesh.md delta 4): the app side, where the
// design says the realistic sustained contributions come from.
//
// The properties worth pinning are the ones a careless integration would
// get wrong: contribution is off unless asked for, a low battery stops
// it, and a consume-only phone still routes.

import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildRouteGraph } from "../src/route_graph_build.js";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { checkMobileHost, createMobileMesh } from "../src/pulsemesh/mobile.js";
import { createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { DEFAULT_CONSTANTS } from "../src/pulsemesh/bins.js";

function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function syntheticGraph(width, height, seed = 11) {
  const random = lcg(seed);
  const nodeCount = width * height;
  const nodeLat = new Int32Array(nodeCount);
  const nodeLon = new Int32Array(nodeCount);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const index = y * width + x;
      nodeLat[index] = 45.5e7 + y * 9000 + Math.floor(random() * 2000);
      nodeLon[index] = -73.6e7 + x * 12000 + Math.floor(random() * 2000);
    }
  }
  const from = [], to = [], weight = [], dist = [], name = [], offsets = [0], geom = [];
  const add = (a, b) => {
    from.push(a); to.push(b);
    weight.push(10 + Math.floor(random() * 600));
    dist.push(500 + Math.floor(random() * 8000));
    name.push(1); geom.push(0); offsets.push(geom.length);
  };
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = y * width + x;
      if (x + 1 < width) { add(i, i + 1); add(i + 1, i); }
      if (y + 1 < height) { add(i, i + width); add(i + width, i); }
    }
  }
  return {
    nodeLat, nodeLon,
    edgeFrom: Uint32Array.from(from), edgeTo: Uint32Array.from(to),
    edgeWeightDs: Uint32Array.from(weight), edgeDistDm: Uint32Array.from(dist),
    edgeName: Uint32Array.from(name),
    edgeClass: Uint8Array.from(from.map((_, i) => i % 2)),
    geomOffsets: Uint32Array.from(offsets), geomBytes: Uint8Array.from(geom),
    names: ["", "Rue A"], profile: "car", classes: ["arterial", "local"]
  };
}

async function engineFixture(t) {
  const graph = syntheticGraph(14, 12);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-pm-mobile-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 40, fanout: 4, topMaxCells: 5 });
  const engine = await openRouteGraphDir(dir);
  const point = node => ({ lat: graph.nodeLat[node] / 1e7, lon: graph.nodeLon[node] / 1e7 });
  return { engine, graph, point };
}

test("the host capability check names what is missing", () => {
  const report = checkMobileHost();
  assert.equal(report.ok, true, "Node satisfies the traffic channel's requirements");
  assert.deepEqual(report.missing, []);
  assert.equal(typeof report.threadsAvailable, "boolean",
    "thread support is reported separately — the traffic channel does not need subtle");
});

test("contribution is off unless the app asks for it", async t => {
  const { engine, point } = await engineFixture(t);
  const network = createLoopbackNetwork();
  const mobile = await createMobileMesh({ engine, network, id: "phone", profile: "cadence" });

  assert.equal(mobile.contributing, false, "a phone that only reads traffic must not publish");
  const fix = { ...point(20), speedMps: 12 };
  const refused = await mobile.onLocation(fix);
  assert.equal(refused.emitted, false);
  assert.match(refused.reason, /disabled/);
  assert.equal(mobile.stats.emitted, 0);

  // Turning it on is an explicit act, and then the pipeline runs.
  mobile.setContributing(true);
  assert.equal(mobile.contributing, true);
  const emitted = await mobile.onLocation({ ...point(20), speedMps: 12 });
  assert.equal(typeof emitted.emitted, "boolean", "the fix reached the contributor pipeline");
  assert.ok(mobile.stats.fixes >= 2);
});

test("a low battery stops contribution, charging resumes it", async t => {
  const { engine, point } = await engineFixture(t);
  const network = createLoopbackNetwork();
  let level = 0.15;
  let plugged = false;
  const mobile = await createMobileMesh({
    engine, network, id: "phone", profile: "cadence", contribute: true,
    // POW at 8 bits so the test does not spend a second per emission.
    constants: { ...DEFAULT_CONSTANTS, EMIT_INTERVAL: 0.001 },
    batteryLevel: () => level,
    charging: () => plugged
  });

  const low = await mobile.onLocation({ ...point(25), speedMps: 12 });
  assert.equal(low.emitted, false);
  assert.equal(low.reason, "battery", "§10.1 rule 5: pause below 20% unless charging");

  plugged = true;
  const charged = await mobile.onLocation({ ...point(25), speedMps: 12 });
  assert.notEqual(charged.reason, "battery", "plugged in, the battery gate is lifted");

  plugged = false;
  level = 0.8;
  const healthy = await mobile.onLocation({ ...point(25), speedMps: 12 });
  assert.notEqual(healthy.reason, "battery");
});

test("a consume-only phone still routes, and gossip reaches it", async t => {
  const { engine, point } = await engineFixture(t);
  const network = createLoopbackNetwork();
  const from = point(15);
  const to = point(14 * 12 - 20);
  const base = await engine.route({ from, to });

  const constants = { ...DEFAULT_CONSTANTS, EMIT_INTERVAL: 0.001 };

  // One phone reading, one contributing — the ordinary shape. The
  // loopback network is one trusted process, so the loopback transport
  // applies and no §5.4 bond is required; the wire tests cover admission.
  const reader = await createMobileMesh({ engine, network, id: "reader", constants, transport: "loopback" });
  const driver = await createMobileMesh({
    engine, network, id: "driver", profile: "cadence", contribute: true, constants, transport: "loopback"
  });
  // Following a corridor now warms the static facts for its leaves as
  // well as subscribing to its zones, so it is asynchronous.
  const zones = await reader.followRoute(base);
  assert.ok(zones.length > 0, "the reader subscribes to the corridor's zones");
  await driver.followRoute(base);

  // The driver crawls the corridor. Fixes are real coordinates; snap()
  // turns them into segment ids exactly as on a phone.
  const geometry = base.geometry || [];
  let sent = 0;
  for (let i = 0; i < geometry.length && sent < 12; i += Math.max(1, Math.floor(geometry.length / 12))) {
    const [lat, lon] = geometry[i];
    const result = await driver.onLocation({ lat, lon, speedMps: 1.8, nowMillis: Date.now() });
    if (result.emitted) sent++;
  }
  assert.ok(sent > 0, `the driver published contributions (${sent})`);
  assert.ok(reader.node.store.size() > 0, "and the reader received them over the mesh");

  // The reader routes under the live metric without ever contributing.
  const live = await engine.route({ from, to, live: reader.provider() });
  assert.equal(live.live.provider, "pulsemesh");
  assert.equal(reader.stats.emitted, 0, "a reader publishes nothing, ever");

  // And with no mesh at all it is still a working router.
  const solo = await createMobileMesh({ engine });
  assert.equal(solo.provider(), null, "consume-only with no network has no provider");
  const staticRoute = await engine.route({ from, to, live: solo.provider() });
  assert.equal(staticRoute.seconds, base.seconds, "which degrades to the static metric");
});

test("the reticent profile is the mobile default", async t => {
  const { engine, point } = await engineFixture(t);
  const network = createLoopbackNetwork();
  const mobile = await createMobileMesh({
    engine, network, id: "phone", contribute: true,
    constants: { ...DEFAULT_CONSTANTS }
  });
  // A phone's owner did not sign up to publish a trajectory, so the
  // profile that suppresses one is the default rather than an opt-in.
  const results = [];
  for (let i = 0; i < 6; i++) {
    results.push(await mobile.onLocation({ ...point(30 + i), speedMps: 12, nowMillis: Date.now() + i * 100 }));
  }
  const reasons = results.filter(result => !result.emitted).map(result => result.reason);
  assert.ok(
    reasons.some(reason => String(reason).startsWith("reticent:")),
    `the reticent gates are what suppressed emissions (${[...new Set(reasons)].join(", ")})`
  );
});
