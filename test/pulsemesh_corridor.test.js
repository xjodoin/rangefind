import assert from "node:assert/strict";
import test from "node:test";
import { existsSync } from "node:fs";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { createMeshSession } from "../src/pulsemesh/session.js";

const GRAPH_DIR = "examples/osm-geo/public/route-graph";

async function graphFixture(t) {
  if (!existsSync(`${GRAPH_DIR}/manifest.json`)) {
    t.skip(`route graph fixture missing at ${GRAPH_DIR}`);
    return null;
  }
  return openRouteGraphDir(GRAPH_DIR);
}

const centreOf = (engine, leaf) => {
  const bbox = engine.root.leaves[leaf].bbox;
  return { lat: (bbox.minLat + bbox.maxLat) / 2 / 1e7, lon: (bbox.minLon + bbox.maxLon) / 2 / 1e7 };
};

async function someRoute(engine, minEdges = 20) {
  for (let attempt = 0; attempt < 40; attempt++) {
    const from = centreOf(engine, (attempt * 7) % engine.root.leaves.length);
    const to = centreOf(engine, (attempt * 7 + 11) % engine.root.leaves.length);
    const route = await engine.route({ from, to, geometry: true }).catch(() => null);
    if (route?.edges?.length >= minEdges) return route;
  }
  return null;
}

/**
 * A network whose peers can be taken away, which is what a phone in a rail
 * underpass looks like from inside the session.
 */
function flakyNetwork(clock) {
  const inner = createLoopbackNetwork({ clock });
  let connected = true;
  return {
    network: {
      ...inner,
      register: node => inner.register(node),
      peersOf: id => (connected ? inner.peersOf(id) : [])
    },
    cut() { connected = false; },
    restore() { connected = true; }
  };
}

/**
 * The corridor a route asks for, when there is nobody to ask yet.
 *
 * `followRoute` fetched the cells only if a peer happened to be connected at
 * that instant, with no retry and nothing recorded. A recorded drive lost its
 * peers for six seconds on the minute; a reroute landing in a window like that
 * would have adopted its corridor into silence and asked nobody for the rest
 * of the drive, and the trace would have shown only a traffic view that stayed
 * empty. Whether a peer is there right now has nothing to do with the route.
 */
test("a corridor nobody was there to answer is asked for again", async (t) => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { network, cut, restore } = flakyNetwork(clock);

  const session = await createMeshSession({
    engine, network, id: "driver", transport: "loopback", clock
  });
  // Somebody to answer, once anybody is reachable at all.
  await createMeshSession({ engine, network, id: "keeper", transport: "loopback", clock });

  const route = await someRoute(engine);
  assert.ok(route, "the fixture graph has a routable pair");

  // The route is adopted in the six seconds where the peers were gone.
  cut();
  await session.followRoute(route);
  const asleep = session.node.stats.cellsWanted;
  assert.equal(asleep, 0, "nothing can be asked of nobody");
  // The scope was still adopted: the zones are subscribed and the cells are
  // known. Only the asking failed, which is the part that used to vanish.
  assert.ok(session.snapshot().cells > 0, "the corridor is known even when it cannot be fetched");

  // The peers come back, and the next beat of the maintenance loop asks.
  restore();
  now += 1000;
  await session.tick(now);
  assert.ok(
    session.node.stats.cellsWanted > asleep,
    "the corridor is asked for on the first beat that has a peer on it"
  );

  // And it is asked for once, not on every beat forever.
  const asked = session.node.stats.cellsWanted;
  now += 1000;
  await session.tick(now);
  assert.equal(session.node.stats.cellsWanted, asked, "a corridor already fetched is not re-fetched");
});

test("a corridor fetched at once leaves nothing outstanding", async (t) => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { network } = flakyNetwork(clock);
  const session = await createMeshSession({
    engine, network, id: "driver", transport: "loopback", clock
  });
  await createMeshSession({ engine, network, id: "keeper", transport: "loopback", clock });

  const route = await someRoute(engine);
  assert.ok(route);
  await session.followRoute(route);
  const asked = session.node.stats.cellsWanted;
  assert.ok(asked > 0, "the ordinary case still asks straight away");

  now += 1000;
  await session.tick(now);
  assert.equal(session.node.stats.cellsWanted, asked, "and does not ask a second time on the next beat");
});
