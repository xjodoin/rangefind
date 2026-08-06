// Thread discovery over the DHT (threads §4.2, §8 path 1).
//
// This is what makes a link self-sufficient. A holder derives the topic
// tag from `P`, derives the rendezvous key from the tag, asks the DHT who
// is providing that key, connects to them, and subscribes. No directory,
// no name server, no address in the link — "a link is a key, not a
// location".
//
// The DHT learns only that some peers provide an opaque, rotating key.
// It cannot enumerate threads (the tag is pseudorandom), cannot follow
// one across a rotation (tags are unlinkable without `K_topic`), and
// cannot open anything (the payload is sealed). A provider record says
// "this peer has something addressed by this hash" and nothing else.
//
// The rendezvous key is 32 raw bytes; libp2p content routing speaks
// CIDs, so it is wrapped as a CIDv1 with the raw codec over its SHA-256
// multihash. That wrapping is deterministic, so two holders of the same
// link compute the same CID without coordinating.

import { threadRendezvous, threadTag, threadTopic, threadWindow } from "./thread_crypto.js";
import { THREAD_CONSTANTS } from "./thread_publish.js";
import { toHex } from "./sha256.js";

/** Wraps a 32-byte rendezvous key as the CID the DHT is addressed by. */
export async function rendezvousCid(rendezvous) {
  const [{ CID }, raw, { sha256 }] = await Promise.all([
    import("multiformats/cid"),
    import("multiformats/codecs/raw"),
    import("multiformats/hashes/sha2")
  ]);
  return CID.createV1(raw.code, await sha256.digest(rendezvous));
}

/**
 * The windows a holder should currently be advertising or looking on:
 * the current one, plus its neighbours so a rotation boundary does not
 * briefly orphan a thread.
 */
export function discoveryWindows(nowMillis) {
  const window = threadWindow(nowMillis);
  return [window - 1, window, window + 1];
}

/**
 * Derives every address for a thread at a moment in time: tags, topics,
 * rendezvous keys, and the CIDs those map to.
 */
export async function threadAddresses({ keys, epoch32, epochPrefix16hex, nowMillis = Date.now() }) {
  const windows = discoveryWindows(nowMillis);
  return Promise.all(windows.map(async window => {
    const tag = await threadTag(keys, epoch32, window);
    const topic = threadTopic(epochPrefix16hex, tag);
    const rendezvous = threadRendezvous(topic);
    return { window, tag, topic, rendezvous, cid: await rendezvousCid(rendezvous) };
  }));
}

/**
 * Discovery bound to one libp2p host.
 *
 * - `provide()` advertises this peer as holding the thread, for every
 *   currently-valid window. A publisher calls it; so does any subscriber
 *   willing to answer catch-up requests, which is what makes availability
 *   scale with the number of peers asked (benchmarks §9c).
 * - `findPeers()` returns providers for the same keys, and is the whole
 *   of "how do I find this thread" — the caller then dials and
 *   subscribes.
 *
 * Both are best-effort by design: a DHT lookup that finds nobody is a
 * normal outcome on a small mesh, and the caller falls back to its
 * ordinary bootstrap peers.
 */
export function createThreadDiscovery({
  host,
  keys,
  epoch32,
  epochPrefix16hex,
  constants = THREAD_CONSTANTS,
  clock = Date.now,
  provideTimeoutMs = 10_000,
  findTimeoutMs = 10_000
} = {}) {
  const provided = new Map(); // cid string -> millis last advertised
  const stats = { provideCalls: 0, provideErrors: 0, provideTimeouts: 0, lookups: 0, peersFound: 0 };
  let timer = null;

  // A DHT operation must never block its caller. `provide` in particular
  // waits on the K closest peers, which on a small or partitioned mesh
  // simply never arrive — and a thread publisher cannot stop publishing
  // because a routing table is thin. Every call is bounded, and a
  // timeout is an ordinary outcome rather than an error.
  const deadline = ms => (typeof AbortSignal?.timeout === "function"
    ? AbortSignal.timeout(ms)
    : undefined);

  async function addresses(nowMillis = clock()) {
    return threadAddresses({ keys, epoch32, epochPrefix16hex, nowMillis });
  }

  /** Advertises this peer as a provider for every current window. */
  async function provide({ nowMillis = clock() } = {}) {
    const routing = host.contentRouting;
    if (!routing) return 0;
    let announced = 0;
    for (const address of await addresses(nowMillis)) {
      const key = address.cid.toString();
      const last = provided.get(key);
      if (last != null && nowMillis - last < constants.THREAD_PROVIDE_INTERVAL * 1000) continue;
      stats.provideCalls++;
      try {
        await routing.provide(address.cid, { signal: deadline(provideTimeoutMs) });
        provided.set(key, nowMillis);
        announced++;
      } catch (error) {
        // No DHT peers yet, a thin routing table, or the network refused:
        // the thread is still reachable through gossip and ordinary
        // bootstrap, so this is best-effort by design.
        if (error?.name === "TimeoutError" || error?.name === "AbortError") stats.provideTimeouts++;
        else stats.provideErrors++;
      }
    }
    // Forget windows that have rotated away.
    const live = new Set((await addresses(nowMillis)).map(address => address.cid.toString()));
    for (const key of [...provided.keys()]) if (!live.has(key)) provided.delete(key);
    return announced;
  }

  /**
   * Providers for this thread right now, deduplicated by peer id and
   * excluding ourselves. `limit` bounds the DHT walk; `signal` aborts it.
   */
  async function findPeers({ nowMillis = clock(), limit = 8, signal = null, timeoutMs = findTimeoutMs } = {}) {
    const routing = host.contentRouting;
    if (!routing) return [];
    const abort = signal ?? deadline(timeoutMs);
    const found = new Map();
    for (const address of await addresses(nowMillis)) {
      stats.lookups++;
      try {
        for await (const provider of routing.findProviders(address.cid, { signal: abort })) {
          const id = provider.id?.toString?.() ?? String(provider.id);
          if (!id || id === host.peerId.toString()) continue;
          if (!found.has(id)) {
            found.set(id, { id, multiaddrs: (provider.multiaddrs || []).map(String), window: address.window });
          }
          if (found.size >= limit) break;
        }
      } catch {
        // A lookup that finds nobody is normal on a small mesh.
      }
      if (found.size >= limit) break;
    }
    stats.peersFound += found.size;
    return [...found.values()];
  }

  /**
   * Finds providers and dials them, so the caller ends up connected to
   * peers holding this thread with nothing but the link. Returns the peer
   * ids actually connected.
   */
  async function connect({ nowMillis = clock(), limit = 8 } = {}) {
    const peers = await findPeers({ nowMillis, limit });
    const connected = [];
    for (const peer of peers) {
      try {
        const { multiaddr } = await import("@multiformats/multiaddr");
        for (const address of peer.multiaddrs) {
          try {
            await host.dial(multiaddr(address));
            connected.push(peer.id);
            break;
          } catch {
            // Try the peer's next address.
          }
        }
      } catch {
        // multiaddr unavailable; nothing to dial with.
      }
    }
    return connected;
  }

  /** Re-advertises every THREAD_PROVIDE_INTERVAL until stopped. */
  function start() {
    if (timer) return;
    const tick = () => {
      provide().catch(() => {});
    };
    tick();
    timer = setInterval(tick, constants.THREAD_PROVIDE_INTERVAL * 1000);
    if (typeof timer.unref === "function") timer.unref();
  }

  function stop() {
    if (timer) clearInterval(timer);
    timer = null;
  }

  return { addresses, provide, findPeers, connect, start, stop, stats,
    /** For tests and diagnostics: the tag hexes currently advertised. */
    async advertisedTags(nowMillis = clock()) {
      return (await addresses(nowMillis)).map(address => toHex(address.tag));
    }
  };
}
