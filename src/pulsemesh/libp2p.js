// PulseMesh wire transport (protocol §5.1, milestone M3): the js-libp2p
// binding of the MeshNetwork interface. Everything protocol-level lives
// in MeshNode; this file only maps its four transport verbs onto a
// libp2p host:
//
//   subscribe/unsubscribe  -> GossipSub topics
//   publish                -> GossipSub publish
//   request                -> one framed request/response per stream on
//                             /rangefind/pulsemesh/1/sync
//
// GossipSub runs the §5.1 profile: message signing disabled (records are
// self-validating; peer identity is deliberately not bound to data) and
// message id = first 20 bytes of SHA-256 of the payload. The libp2p
// packages are optional peer dependencies, loaded dynamically — engine
// consumers that never touch the wire mesh install nothing.

import { SYNC_PROTOCOL } from "./topics.js";
import { sha256 } from "./sha256.js";
import { pushVarint } from "../binary.js";

const REQUEST_TIMEOUT_MS = 10_000;
// A responder must not be pinnable by a peer that opens a stream and then
// says nothing (or dribbles bytes). Keepers exist to answer PMG1/PMQ1, so
// they are exactly the node an unbounded read would let you stall for
// free.
const SERVE_TIMEOUT_MS = 15_000;
const MAX_FRAME_BYTES = 4 * 1024 * 1024;

/**
 * Creates a libp2p host with the §5.1 profile (TCP for keepers and
 * tests; a browser build swaps the transports for WebRTC/WebSockets and
 * keeps everything else). Pass `listen` multiaddrs and optional
 * `bootstrapPeers` to dial on start.
 */
export async function createPulseMeshHost({ listen = ["/ip4/127.0.0.1/tcp/0"], bootstrapPeers = [] } = {}) {
  const [{ createLibp2p }, { tcp }, { noise }, { yamux }, { identify }, { gossipsub }] = await Promise.all([
    import("libp2p"),
    import("@libp2p/tcp"),
    import("@chainsafe/libp2p-noise"),
    import("@chainsafe/libp2p-yamux"),
    import("@libp2p/identify"),
    import("@chainsafe/libp2p-gossipsub")
  ]);
  const host = await createLibp2p({
    addresses: { listen },
    transports: [tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: {
      identify: identify(),
      pubsub: gossipsub({
        globalSignaturePolicy: "StrictNoSign",
        msgIdFn: message => sha256(message.data).subarray(0, 20),
        allowPublishToZeroTopicPeers: true,
        emitSelf: false
      })
    }
  });
  for (const address of bootstrapPeers) {
    try {
      const { multiaddr } = await import("@multiformats/multiaddr");
      await host.dial(multiaddr(address));
    } catch {
      // Bootstrap peers are best-effort; the mesh degrades, the router
      // keeps working on the static metric.
    }
  }
  return host;
}

function toBytes(chunk) {
  return chunk instanceof Uint8Array ? chunk : chunk.subarray();
}

/**
 * Reads a length prefix, distinguishing "not all here yet" from a real
 * value. `readVarint` in src/binary.js cannot: given the first byte of a
 * two-byte prefix it returns the low 7 bits as if the number were
 * complete, so 300 (`ac 02`) arriving one byte at a time reads as 44,
 * and a prefix beginning `80` reads as 0 — which frames an empty
 * message and consumes the byte, silently eating the message that
 * followed. A TCP stream may split anywhere, so this has to be exact.
 */
function tryReadLength(buffer) {
  let value = 0;
  let multiplier = 1;
  for (let pos = 0; pos < buffer.length; pos++) {
    const byte = buffer[pos];
    value += (byte & 0x7f) * multiplier;
    if ((byte & 0x80) === 0) return { length: value, headerBytes: pos + 1 };
    multiplier *= 0x80;
    if (multiplier > 2 ** 35) return { malformed: true };
  }
  return null; // every byte so far had the continuation bit set
}

/**
 * Collects framed messages from a byte stream that ignores boundaries.
 * Exported so the fragmentation behaviour can be tested directly: a live
 * socket rarely reproduces the one-byte-at-a-time split that breaks a
 * naive reader, and "rarely" is the worst kind of bug.
 */
export function frameAssembler(onFrame) {
  let buffer = new Uint8Array(0);
  return chunk => {
    const incoming = toBytes(chunk);
    const merged = new Uint8Array(buffer.length + incoming.length);
    merged.set(buffer, 0);
    merged.set(incoming, buffer.length);
    buffer = merged;
    for (;;) {
      if (!buffer.length) return;
      const header = tryReadLength(buffer);
      if (!header) return;            // length prefix still incomplete
      if (header.malformed) throw new Error("PulseMesh sync frame length is malformed.");
      if (header.length > MAX_FRAME_BYTES) throw new Error("PulseMesh sync frame too large.");
      const end = header.headerBytes + header.length;
      if (end > buffer.length) return; // payload still incomplete
      onFrame(buffer.subarray(header.headerBytes, end));
      buffer = buffer.slice(end);
    }
  };
}

function framed(payload) {
  const out = [];
  pushVarint(out, payload.length);
  const bytes = new Uint8Array(out.length + payload.length);
  bytes.set(out, 0);
  bytes.set(payload, out.length);
  return bytes;
}

/**
 * Wraps a libp2p host as a MeshNetwork for exactly one MeshNode. Call
 * `close()` when done (leaves the host itself to the caller).
 */
export function createLibp2pNetwork(host) {
  let meshNode = null;
  const subscriptions = new Set();
  const stats = { gossipIn: 0, gossipOut: 0, requests: 0, responses: 0, served: 0 };

  // "gossipsub:message", not "message". Both fire once per delivered
  // message, but only this one carries `propagationSource` — the peer
  // that actually handed us the bytes. That id is what the per-peer rate
  // limiter (rule 7), the trust ledger (§8.4), and the incident
  // distinct-peer cap (§8.5, `score = min(raw, sources)`) are all keyed
  // on. Switching to the simpler "message" event would compile, pass a
  // naive test, and quietly disable every Sybil defense at once.
  const onGossip = event => {
    const { msg, propagationSource } = event.detail;
    if (!meshNode || !subscriptions.has(msg.topic)) return;
    stats.gossipIn += msg.data.length;
    meshNode.onGossip(msg.topic, msg.data, propagationSource?.toString() ?? "unknown", meshNode.clock());
  };

  // One request per stream: read a single framed message, answer with a
  // single framed response (or none — PMF1 has no response), close.
  const onSyncStream = ({ stream, connection }) => {
    const fromPeer = connection.remotePeer.toString();
    const responses = [];
    let answered = false;
    let expired = false;
    const deadline = setTimeout(() => {
      expired = true;
      stream.abort?.(new Error("PulseMesh sync read timed out."));
    }, SERVE_TIMEOUT_MS);
    if (typeof deadline.unref === "function") deadline.unref();
    const assemble = frameAssembler(payload => {
      if (answered || !meshNode) return;
      answered = true;
      stats.served++;
      const response = meshNode.onStream(payload, fromPeer, meshNode.clock());
      if (response) responses.push(framed(response));
    });
    stream.sink((async function* () {
      try {
        for await (const chunk of stream.source) {
          if (expired) return;
          try {
            assemble(chunk);
          } catch {
            return;
          }
          if (answered) break;
        }
        if (!expired) yield* responses;
      } finally {
        clearTimeout(deadline);
      }
    })()).catch(() => {});
  };

  host.services.pubsub.addEventListener("gossipsub:message", onGossip);
  // Registration is async. Callers that announce readiness (a keeper
  // printing "listening") must await `ready` first, or a peer that dials
  // immediately has its first stream rejected. The rejection is caught
  // here rather than left floating: an unhandled one — from a duplicate
  // protocol registration, say — would take the process down.
  let registrationError = null;
  const ready = Promise.resolve(host.handle(SYNC_PROTOCOL, onSyncStream))
    .catch(error => { registrationError = error; });
  const scheduled = new Set();

  return {
    ready,
    get registrationError() {
      return registrationError;
    },
    register(node) {
      if (meshNode && meshNode !== node) throw new Error("One MeshNode per libp2p host.");
      meshNode = node;
    },
    subscribe(_nodeId, topic) {
      subscriptions.add(topic);
      host.services.pubsub.subscribe(topic);
    },
    unsubscribe(_nodeId, topic) {
      subscriptions.delete(topic);
      try {
        host.services.pubsub.unsubscribe(topic);
      } catch {
        // Unsubscribing from an unknown topic is not an error worth surfacing.
      }
    },
    publish(topic, payload) {
      stats.gossipOut += payload.length;
      host.services.pubsub.publish(topic, payload).catch(() => {
        // No peers on the topic yet — gossip is best-effort by design.
      });
    },
    async request(_fromId, toId, payload) {
      const connection = host.getConnections().find(candidate => candidate.remotePeer.toString() === toId);
      if (!connection) return null;
      stats.requests++;
      let stream;
      try {
        stream = await connection.newStream(SYNC_PROTOCOL);
      } catch {
        return null;
      }
      try {
        return await new Promise(resolve => {
          const timer = setTimeout(() => resolve(null), REQUEST_TIMEOUT_MS);
          // Counted here, not in `finally`: a timeout and a stream that
          // closed without answering are both non-responses, and a
          // counter that ticks for them is a counter that can never
          // reveal the failure it exists to reveal.
          const finish = value => {
            clearTimeout(timer);
            if (value) stats.responses++;
            resolve(value);
          };
          const assemble = frameAssembler(response => finish(response.slice()));
          stream.sink((async function* () {
            yield framed(payload);
          })()).catch(() => finish(null));
          (async () => {
            try {
              for await (const chunk of stream.source) assemble(chunk);
              finish(null); // stream ended without a response frame
            } catch {
              finish(null);
            }
          })();
        });
      } finally {
        stream.close().catch(() => {});
      }
    },
    peersOf() {
      return host.getPeers().map(String);
    },
    schedule(fn, delayMs) {
      const timer = setTimeout(() => {
        scheduled.delete(timer);
        fn();
      }, delayMs);
      scheduled.add(timer);
      return timer;
    },
    stats,
    /**
     * Full teardown. The host outlives the network by contract, so
     * everything this network attached to it has to come back off:
     * leaving the topics subscribed would keep the host in every
     * PulseMesh gossip mesh, receiving and forwarding traffic nothing is
     * listening to, and a pending forwarder timer would fire into a
     * stopped host.
     */
    async close() {
      host.services.pubsub.removeEventListener("gossipsub:message", onGossip);
      for (const timer of scheduled) clearTimeout(timer);
      scheduled.clear();
      for (const topic of subscriptions) {
        try {
          host.services.pubsub.unsubscribe(topic);
        } catch {
          // Already gone with the host; nothing to undo.
        }
      }
      subscriptions.clear();
      await ready;
      await host.unhandle(SYNC_PROTOCOL).catch(() => {});
      meshNode = null;
    }
  };
}
