// PulseMesh wire transport (protocol §5.1, milestone M3): the js-libp2p
// binding of the MeshNetwork interface. Everything protocol-level lives
// in MeshNode; this file only maps its four transport verbs onto a
// libp2p host:
//
//   subscribe/unsubscribe  -> GossipSub topics (+ a topic validator)
//   publish                -> GossipSub publish
//   request                -> one framed request/response per stream on
//                             /rangefind/pulsemesh/1/sync
//
// GossipSub runs the §5.1 profile: message signing disabled (records are
// self-validating; peer identity is deliberately not bound to data) and
// message id = first 20 bytes of SHA-256 of the payload. The libp2p
// packages are optional peer dependencies, loaded dynamically — engine
// consumers that never touch the wire mesh install nothing.
//
// **Relaying implies validating.** GossipSub forwards a message to its
// mesh peers the moment it arrives, before any application handler sees
// it, so a node that validates only on delivery relays records it never
// checked — and rule 5 has the far side accept them because the
// *delivering* peer is bonded. That is a laundering path through any
// honest relay. Every topic this network subscribes to therefore carries
// a GossipSub topic validator (`pubsub.topicValidators`), which runs the
// full §6 pipeline before the forward decision; the delivery listener
// then consumes that verdict instead of running validation a second
// time.

import { BOND_PROTOCOL, PHOTO_PROTOCOL, SYNC_PROTOCOL, THREAD_PROTOCOL } from "./topics.js";
import { encodePMA1 } from "./codec.js";
import { mintBond as mineBondProof } from "./bond.js";
import { GOSSIP_ACCEPT, GOSSIP_IGNORE, GOSSIP_REJECT } from "./validate.js";
import { sha256, toHex } from "./sha256.js";
import { pushVarint } from "../binary.js";

/** Browsers have no sockets to listen on, so the transport set differs. */
function detectProfile() {
  return typeof globalThis.window !== "undefined" && typeof globalThis.document !== "undefined"
    ? "browser"
    : "node";
}

const REQUEST_TIMEOUT_MS = 10_000;
// A responder must not be pinnable by a peer that opens a stream and then
// says nothing (or dribbles bytes). Keepers exist to answer PMG1/PMQ1, so
// they are exactly the node an unbounded read would let you stall for
// free.
const SERVE_TIMEOUT_MS = 15_000;
const MAX_FRAME_BYTES = 4 * 1024 * 1024;
/**
 * Opt in to running over a relayed circuit, which libp2p classifies as a
 * "limited" connection and otherwise refuses protocols on. Applied to
 * both ends of every stream this file opens or answers: for a phone or a
 * browser tab the circuit is the only connection there is, so a protocol
 * that declines one does not degrade, it disappears.
 */
const LIMITED_OK = Object.freeze({ runOnLimitedConnection: true });

/**
 * Creates a libp2p host with the §5.1 profile (TCP for keepers and
 * tests; a browser build swaps the transports for WebRTC/WebSockets and
 * keeps everything else). Pass `listen` multiaddrs and optional
 * `bootstrapPeers` to dial on start.
 */
export async function createPulseMeshHost({
  listen = null,
  bootstrapPeers = [],
  // Peers this host met last time and the *host* chose to remember
  // (threads §20.10). Dialled exactly like the configured bootstrap and
  // with the same best-effort semantics; the difference is only where
  // they came from. The library persists nothing — there is no peer
  // store here, and deliberately so: what a device is allowed to
  // remember about who it has talked to is the embedding app's decision,
  // not a default this file gets to make.
  rememberedPeers = [],
  // "node" — TCP, for keepers and servers. "browser" — WebSockets to
  // reach keepers plus WebRTC for peer-to-peer, and Circuit Relay v2 for
  // NAT traversal, which is the only combination a browser tab can use.
  // Product copy never calls relayed traffic anonymous (§5.1).
  profile = detectProfile(),
  // Thread discovery (threads §4.2) needs content routing. Off by
  // default: the traffic channel finds peers by zone and does not want
  // the extra chatter.
  dht = false,
  // Be a Circuit Relay v2 *server*, so peers that cannot accept an
  // inbound connection can be reached through this one.
  //
  // This is the hop the whole product rests on and it did not exist: a
  // driver's phone and a customer's browser can each reach a keeper and
  // neither can reach the other, and a keeper cannot bridge them with
  // GossipSub because a thread's topic is derived from the capability —
  // which a keeper must never hold — and GossipSub does not forward for
  // topics it has not joined. Relaying needs none of that. The keeper
  // moves an encrypted stream between two peer ids and never learns the
  // topic, which is the same bargain it already makes with everything
  // else it carries.
  //
  // Off by default because it spends this machine's bandwidth on other
  // people's traffic, and that is a decision an operator makes. A keeper
  // turns it on (scripts/pulsemesh_keeper.mjs), because a keeper nobody
  // can be reached through is a keeper that carries nothing.
  relay = false
} = {}) {
  const browser = profile === "browser";
  const [{ createLibp2p }, { noise }, { yamux }, { identify }, { gossipsub }] = await Promise.all([
    import("libp2p"),
    import("@chainsafe/libp2p-noise"),
    import("@chainsafe/libp2p-yamux"),
    import("@libp2p/identify"),
    import("@chainsafe/libp2p-gossipsub")
  ]);

  const transports = [];
  if (browser) {
    const [{ webSockets }, filters, { webRTC }, { circuitRelayTransport }] = await Promise.all([
      import("@libp2p/websockets"),
      import("@libp2p/websockets/filters"),
      import("@libp2p/webrtc"),
      import("@libp2p/circuit-relay-v2")
    ]);
    // WebSockets reaches keepers over WSS; WebRTC carries browser↔browser
    // once a relay has introduced them.
    //
    // `filters.all` rather than the default, which is `wss` only. That
    // default silently makes every plain-`ws` keeper undialable from a
    // browser — a depot's own seed on its own LAN, and every development
    // machine — and it fails as a bare "WebSocket connection failed" with
    // no hint that the address was refused rather than unreachable.
    //
    // This is not the downgrade it looks like: **the browser already
    // enforces the rule that matters**. A page served over https cannot
    // open a ws:// socket whatever we pass here, so an https deployment
    // is still wss-only, by the one mechanism an attacker cannot argue
    // with. What the default actually blocks is the http page talking to
    // the machine on its own desk.
    transports.push(
      webSockets({ filter: filters.all }),
      webRTC(),
      circuitRelayTransport({ discoverRelays: 1 })
    );
  } else {
    // TCP for keeper↔keeper. WebSockets **as well**, because it is the
    // only transport a browser or a WebView driver can dial and a keeper
    // that cannot listen on one is a keeper no product client can reach:
    // the browser profile above ships webSockets/webRTC/circuit-relay and
    // shares nothing with a TCP-only listener. `webSockets()` listens in
    // Node and is inert until a `/ws` or `/wss` address is passed to
    // `listen`, so a plain TCP keeper is unchanged.
    const [{ tcp }, { webSockets }, { circuitRelayTransport }] = await Promise.all([
      import("@libp2p/tcp"),
      import("@libp2p/websockets"),
      import("@libp2p/circuit-relay-v2")
    ]);
    // Circuit relay on the node side as well, because "cannot accept an
    // inbound connection" is not a browser condition — it is a NAT
    // condition. A fleet seed on the depot's own box behind a home router
    // is the documented deployment (§12.1) and has exactly the same
    // problem a phone does. A host listening on "/p2p-circuit" takes a
    // reservation on a relay it can reach and becomes dialable through
    // it; one that listens on a real socket never needs to.
    transports.push(tcp(), webSockets(), circuitRelayTransport({ discoverRelays: 1 }));
  }

  const services = {
    identify: identify(),
    pubsub: gossipsub({
      globalSignaturePolicy: "StrictNoSign",
      msgIdFn: message => sha256(message.data).subarray(0, 20),
      allowPublishToZeroTopicPeers: true,
      emitSelf: false,
      // libp2p marks a relayed connection "limited" and refuses to run
      // protocols over it unless they say so. For this mesh the relayed
      // connection is not a fallback, it is *the* connection: a driver's
      // phone and a customer's tab can reach a keeper and never each
      // other, so a gossip mesh that declines to form over a circuit is a
      // gossip mesh that never forms at all. Without this the reservation
      // succeeds, the dial succeeds, and not one record crosses.
      //
      // The bytes are small — a thread record is tens of bytes and the
      // relay's own limits bound the rest — so what is being spent here
      // is the relay's allowance, deliberately, on the only path there is.
      runOnLimitedConnection: true
    })
  };
  if (relay && !browser) {
    // A tab cannot be a relay — it has no socket to accept the hop on —
    // so this is a node-only service and asking for it in a browser is a
    // configuration mistake, not something to half-do silently.
    const { circuitRelayServer } = await import("@libp2p/circuit-relay-v2");
    services.relay = circuitRelayServer({
      reservations: {
        // A keeper's whole job is availability, and the v2 defaults are
        // sized for an incidental relay: 15 reservations and a two-minute
        // circuit would put a depot's drivers in a queue and cut their
        // customers off mid-round. A run lasts a shift, so the circuit
        // has to as well.
        maxReservations: 512,
        reservationTtl: 60 * 60 * 1000,
        defaultDataLimit: BigInt(1 << 30),
        defaultDurationLimit: 8 * 60 * 60 * 1000
      }
    });
  } else if (relay && browser) {
    throw new Error("A browser cannot be a Circuit Relay server; only a node host can relay.");
  }
  if (dht) {
    const { kadDHT, passthroughMapper, removePrivateAddressesMapper } = await import("@libp2p/kad-dht");
    const options = typeof dht === "object" ? dht : {};
    // The default mapper strips private addresses, which is right for a
    // public DHT and fatal on loopback or a LAN: peers connect happily
    // and never enter each other's routing tables, so `provide` waits
    // forever for closest peers that can never be found. `scope` picks
    // the mapper explicitly rather than leaving it to a default that
    // silently does nothing on the network you are testing on.
    const scope = options.scope ?? "public";
    services.dht = kadDHT({
      // `clientMode` in the browser: a tab answers no routing queries but
      // can still publish and resolve provider records.
      clientMode: options.clientMode ?? browser,
      protocol: options.protocol ?? "/rangefind/pulsemesh/kad/1",
      peerInfoMapper: options.peerInfoMapper
        ?? (scope === "public" ? removePrivateAddressesMapper : passthroughMapper),
      ...(options.kadOptions || {})
    });
  }

  const host = await createLibp2p({
    // A browser cannot listen on a socket; it dials out and accepts
    // relayed circuits.
    addresses: { listen: listen ?? (browser ? ["/p2p-circuit", "/webrtc"] : ["/ip4/127.0.0.1/tcp/0"]) },
    transports,
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services
  });
  // Configured first, remembered second: a fleet that just moved its
  // seed wants the address it was handed today tried before the one this
  // device happened to talk to last week. Deduplicated so a remembered
  // peer that is also the configured seed is one dial.
  const dialList = [];
  for (const address of [...bootstrapPeers, ...rememberedPeers]) {
    const text = String(address ?? "").trim();
    if (text && !dialList.includes(text)) dialList.push(text);
  }
  if (dialList.length) {
    const { multiaddr } = await import("@multiformats/multiaddr");
    for (const address of dialList) {
      try {
        await host.dial(multiaddr(address));
      } catch {
        // Bootstrap peers are best-effort; the mesh degrades, the router
        // keeps working on the static metric.
      }
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

// Validate-once bookkeeping. Only an *accepted* message is ever
// delivered — GossipSub neither dispatches nor forwards on Ignore or
// Reject — so the cache only ever holds verdicts that are about to be
// consumed microtasks later, in the same turn that produced them.
// Bounded and expiring all the same, because "about to be" is a
// property of this GossipSub version and not a guarantee we own: a
// message accepted for a topic we have just unsubscribed from, or one
// GossipSub drops between validation and dispatch, would otherwise
// leave an entry behind for a flood to accumulate.
const VERDICT_CACHE_MAX = 512;
const VERDICT_TTL_MS = 30_000;

/**
 * Wraps a libp2p host as a MeshNetwork for exactly one MeshNode. Call
 * `close()` when done (leaves the host itself to the caller).
 *
 * `validate` is the seam that makes relaying imply validating. It is
 * called as `(topic, payload, propagationSource, nowMillis)` from a
 * GossipSub topic validator — before the message is forwarded — and
 * returns one of the `GOSSIP_*` verdicts, or `null` for "not mine to
 * judge", which leaves the message to the ordinary delivery path exactly
 * as before. The default is the registered MeshNode's own `judgeGossip`,
 * so every existing caller gets the property without changing a line.
 */
export function createLibp2pNetwork(host, { validate = null } = {}) {
  let meshNode = null;
  const subscriptions = new Set();
  const stats = {
    gossipIn: 0, gossipOut: 0, requests: 0, responses: 0, served: 0,
    bondsSent: 0, bondsReceived: 0, threadsServed: 0, threadRequests: 0,
    photosServed: 0, photoRequests: 0,
    // §5.1 topic-validator accounting: how many messages this node
    // judged before forwarding, and how each judgement came out. A
    // relay's `gossipIgnored` is the honest cost of the rule — a peer
    // holding a sparse map cannot vouch for areas it has no leaf for.
    gossipJudged: 0, gossipRelayed: 0, gossipIgnored: 0, gossipRejected: 0
  };

  // msgKey -> expiry. See VERDICT_CACHE_MAX above.
  const verdicts = new Map();
  // The same bytes GossipSub's msgIdFn hashes, so one message has one
  // key on both sides of the forward decision.
  const verdictKey = data => toHex(sha256(data).subarray(0, 20));

  const rememberVerdict = key => {
    if (verdicts.size >= VERDICT_CACHE_MAX) {
      const now = Date.now();
      for (const [stale, expires] of verdicts) if (expires <= now) verdicts.delete(stale);
      // Still full: drop oldest-first (Map iterates in insertion order).
      while (verdicts.size >= VERDICT_CACHE_MAX) {
        const oldest = verdicts.keys().next().value;
        if (oldest === undefined) break;
        verdicts.delete(oldest);
      }
    }
    verdicts.set(key, Date.now() + VERDICT_TTL_MS);
  };

  // Presence is consumption, expired or not: an entry that outlived its
  // TTL still means this message was validated once, and re-validating
  // it would charge one peer twice for one record — the failure mode
  // that wrongly forfeits honest peers. Expiry bounds memory, nothing
  // else.
  const takeVerdict = key => verdicts.delete(key);

  const judge = validate
    ?? ((topic, payload, fromPeer, nowMillis) => meshNode?.judgeGossip?.(topic, payload, fromPeer, nowMillis) ?? null);

  /**
   * The GossipSub topic validator (libp2p's `TopicValidatorFn`: it is
   * awaited inside `validateReceivedMessage`, which runs to completion
   * before `forwardMessage`, so an async verdict genuinely gates the
   * forward). Registered per subscribed topic and removed on
   * unsubscribe.
   */
  const topicValidator = async (propagationSource, message) => {
    // A topic we are not subscribed to is a topic we have no business
    // vouching for, whatever a peer chose to send us.
    if (!meshNode || !subscriptions.has(message.topic)) return GOSSIP_IGNORE;
    let action;
    try {
      action = await judge(
        message.topic,
        message.data,
        propagationSource?.toString?.() ?? "unknown",
        meshNode.clock()
      );
    } catch {
      // A validator that throws must not turn into a forward.
      return GOSSIP_IGNORE;
    }
    // Null: another channel's topic (the reserved thread namespace).
    // Nothing was consumed, so the delivery listener handles it as it
    // always did — the thread channel is authenticated end to end by its
    // own key and no bond of ours vouches for it.
    if (action == null) return GOSSIP_ACCEPT;
    stats.gossipJudged++;
    stats.gossipIn += message.data.length;
    if (action === GOSSIP_ACCEPT) {
      stats.gossipRelayed++;
      rememberVerdict(verdictKey(message.data));
      return GOSSIP_ACCEPT;
    }
    if (action === GOSSIP_REJECT) {
      stats.gossipRejected++;
      return GOSSIP_REJECT;
    }
    stats.gossipIgnored++;
    return GOSSIP_IGNORE;
  };

  const registerValidator = topic => {
    host.services.pubsub?.topicValidators?.set(topic, topicValidator);
  };
  const unregisterValidator = topic => {
    host.services.pubsub?.topicValidators?.delete(topic);
  };

  // §5.4 admission. `ownBondBytes` is set by mintBond() and then pushed
  // to every peer we are connected to now or connect to later. Presenting
  // is idempotent per peer — the receiver's registry just extends expiry.
  let ownBondBytes = null;
  const presentedTo = new Set();

  const presentBond = async peerIdStr => {
    if (!ownBondBytes || presentedTo.has(peerIdStr)) return;
    const connection = host.getConnections().find(candidate => candidate.remotePeer.toString() === peerIdStr);
    if (!connection) return;
    presentedTo.add(peerIdStr);
    try {
      const stream = await connection.newStream(BOND_PROTOCOL, LIMITED_OK);
      await stream.sink((async function* () {
        yield framed(ownBondBytes);
      })());
      stats.bondsSent++;
      stream.close().catch(() => {});
    } catch {
      // The peer may not speak the bond protocol (older build, consumer
      // profile); it will simply never accept our proofType-3 records,
      // which is its right. Retry on the next connect event.
      presentedTo.delete(peerIdStr);
    }
  };

  const onPeerConnect = event => {
    const peerIdStr = event.detail?.toString?.();
    if (peerIdStr) presentBond(peerIdStr);
  };

  // One framed PMA1 per stream, no response. The peerId comes from the
  // connection — the single input a sender cannot forge — and is the
  // value the bond's seed is bound to.
  const onBondStream = ({ stream, connection }) => {
    const fromPeer = connection.remotePeer.toString();
    let handled = false;
    const deadline = setTimeout(() => {
      stream.abort?.(new Error("PulseMesh bond read timed out."));
    }, SERVE_TIMEOUT_MS);
    if (typeof deadline.unref === "function") deadline.unref();
    const assemble = frameAssembler(payload => {
      if (handled || !meshNode) return;
      handled = true;
      stats.bondsReceived++;
      meshNode.registerBond(payload, fromPeer, meshNode.clock());
    });
    (async () => {
      try {
        for await (const chunk of stream.source) {
          assemble(chunk);
          if (handled) break;
        }
      } catch {
        // A malformed frame or an aborted stream registers nothing.
      } finally {
        clearTimeout(deadline);
        stream.close?.().catch?.(() => {});
      }
    })();
  };

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
    // Validate once. The topic validator has already run the whole §6
    // pipeline for this message — including rule 7's token bucket and
    // the §8.4 trust/forfeiture path, neither of which is side-effect
    // free — and stored it. Running it again here would charge one peer
    // twice for one record and could forfeit an honest one.
    if (takeVerdict(verdictKey(msg.data))) return;
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

  /**
   * One framed request, one framed response, off whichever MeshNode seam
   * the caller names. Both thread-side protocols have exactly this
   * shape and neither consults a bond — the sync stream's contract is
   * the one that differs, which is why it stays written out above.
   */
  const streamResponder = (label, seam, count) => ({ stream, connection }) => {
    const fromPeer = connection.remotePeer.toString();
    const responses = [];
    let answered = false;
    let expired = false;
    const deadline = setTimeout(() => {
      expired = true;
      stream.abort?.(new Error(`PulseMesh ${label} read timed out.`));
    }, SERVE_TIMEOUT_MS);
    if (typeof deadline.unref === "function") deadline.unref();
    const assemble = frameAssembler(payload => {
      if (answered || !meshNode?.[seam]) return;
      answered = true;
      count();
      const response = meshNode[seam](payload, fromPeer, meshNode.clock());
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

  // Threads §5.5: PMR1 in, PMM1 out, on its own protocol. Deliberately
  // not folded into the sync stream — a responder here serves sealed
  // bytes it may not be able to open and never consults a bond, and any
  // subscriber answers, which is what makes availability scale with the
  // audience instead of against it.
  const onThreadStream = streamResponder("thread", "onOtherStream", () => { stats.threadsServed++; });

  // Threads §20.7: PMTF in, PMTB out. Sealed proof-of-delivery bytes, on
  // demand, and only from a peer publishing the run — relays never cache
  // these, so this seam answers for one device's own photos or answers
  // "not held".
  const onPhotoStream = streamResponder("photo", "onPhotoStream", () => { stats.photosServed++; });

  /**
   * One framed request, one framed response, on whichever protocol the
   * caller names. The two channels share the framing and nothing else:
   * the sync stream answers a bonded traffic peer, the thread stream
   * answers anyone at all with bytes it cannot read.
   */
  async function requestOn(protocol, toId, payload) {
    const connection = host.getConnections().find(candidate => candidate.remotePeer.toString() === toId);
    if (!connection) return null;
    stats.requests++;
    let stream;
    try {
      stream = await connection.newStream(protocol, LIMITED_OK);
    } catch {
      return null;
    }
    try {
      return await new Promise(resolve => {
        const timer = setTimeout(() => resolve(null), REQUEST_TIMEOUT_MS);
        // Counted here, not in `finally`: a timeout and a stream that
        // closed without answering are both non-responses, and a counter
        // that ticks for them is a counter that can never reveal the
        // failure it exists to reveal.
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
  }

  host.services.pubsub.addEventListener("gossipsub:message", onGossip);
  host.addEventListener("peer:connect", onPeerConnect);
  // Registration is async. Callers that announce readiness (a keeper
  // printing "listening") must await `ready` first, or a peer that dials
  // immediately has its first stream rejected. The rejection is caught
  // here rather than left floating: an unhandled one — from a duplicate
  // protocol registration, say — would take the process down.
  let registrationError = null;
  // Every one of these has to be willing to run over a relayed circuit,
  // for the same reason the gossip mesh does: for a phone or a tab the
  // circuit is not a degraded path, it is the only path. A handler that
  // declines one is a §5.5 catch-up that recovers nothing, a §20.7 photo
  // that cannot be fetched, and a §5.4 bond that never gets presented —
  // each failing silently, because "the peer did not answer" and "the
  // peer was never asked" look identical from here.

  const ready = Promise.all([
    Promise.resolve(host.handle(SYNC_PROTOCOL, onSyncStream, LIMITED_OK)),
    Promise.resolve(host.handle(BOND_PROTOCOL, onBondStream, LIMITED_OK)),
    Promise.resolve(host.handle(THREAD_PROTOCOL, onThreadStream, LIMITED_OK)),
    Promise.resolve(host.handle(PHOTO_PROTOCOL, onPhotoStream, LIMITED_OK))
  ]).catch(error => { registrationError = error; });
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
    /**
     * §5.4: mines this host's admission bond for the current bucket and
     * presents it to every connected peer (and, via peer:connect, every
     * future one). Chunked — safe to call on a UI thread; meant for the
     * background, once per BOND_LIFETIME, ideally while charging. Returns
     * true when minted, false when the budget or signal ended the search.
     */
    async mintBond({ budgetMillis = null, signal = null, chunkMillis = 10 } = {}) {
      if (!meshNode) throw new Error("register(node) before mintBond().");
      const bond = await mineBondProof({
        epoch32: meshNode.epoch32,
        peerId: host.peerId.toString(),
        constants: meshNode.constants,
        nowMillis: meshNode.clock(),
        budgetMillis,
        signal,
        chunkMillis
      });
      if (!bond) return false;
      ownBondBytes = encodePMA1(bond);
      presentedTo.clear();
      await Promise.all(host.getPeers().map(peer => presentBond(peer.toString())));
      return true;
    },
    subscribe(_nodeId, topic) {
      subscriptions.add(topic);
      // Validator first: a subscribe that raced a message in flight
      // would otherwise forward one unjudged.
      registerValidator(topic);
      host.services.pubsub.subscribe(topic);
    },
    unsubscribe(_nodeId, topic) {
      subscriptions.delete(topic);
      unregisterValidator(topic);
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
      return requestOn(SYNC_PROTOCOL, toId, payload);
    },
    /** Threads §5.5 catch-up: the same framing on its own protocol. */
    async requestThread(_fromId, toId, payload) {
      stats.threadRequests++;
      return requestOn(THREAD_PROTOCOL, toId, payload);
    },
    /**
     * Threads §20.7: one sealed photo by content hash. Availability is
     * bounded by the driver being online — nothing relays or caches a
     * blob this size, so there is no second holder to fall back to.
     */
    async requestPhoto(_fromId, toId, payload) {
      stats.photoRequests++;
      return requestOn(PHOTO_PROTOCOL, toId, payload);
    },
    peersOf() {
      return host.getPeers().map(String);
    },
    /**
     * The multiaddrs this host is **actually connected to**, each ending
     * in `/p2p/<peerId>` so it can be dialled again as-is (threads
     * §20.10).
     *
     * The seam a host needs to stop depending on a seed. A fleet seed
     * matters at first contact and should matter at no other time: once a
     * device has met the mesh it has met peers, and dialling one of those
     * next time is both faster and one less thing that has to still be
     * running. Reporting is all this does — persisting the list, ageing
     * it, capping it and deciding whether a device is allowed to keep it
     * at all belong to the host, which is the only layer that knows
     * whether this is a fleet's own phone or a stranger's browser.
     *
     * Relayed and loopback addresses come back as they are: a circuit
     * address is a perfectly good thing to remember behind NAT, and a
     * host running its own tests connects to 127.0.0.1 on purpose.
     */
    knownPeers() {
      const out = [];
      for (const connection of host.getConnections()) {
        const peer = connection.remotePeer.toString();
        const address = connection.remoteAddr?.toString?.();
        if (!address) continue;
        const dialable = address.includes(`/p2p/${peer}`) ? address : `${address}/p2p/${peer}`;
        if (!out.includes(dialable)) out.push(dialable);
      }
      return out;
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
      host.removeEventListener("peer:connect", onPeerConnect);
      for (const timer of scheduled) clearTimeout(timer);
      scheduled.clear();
      for (const topic of subscriptions) {
        unregisterValidator(topic);
        try {
          host.services.pubsub.unsubscribe(topic);
        } catch {
          // Already gone with the host; nothing to undo.
        }
      }
      subscriptions.clear();
      verdicts.clear();
      await ready;
      await host.unhandle(SYNC_PROTOCOL).catch(() => {});
      await host.unhandle(BOND_PROTOCOL).catch(() => {});
      await host.unhandle(THREAD_PROTOCOL).catch(() => {});
      await host.unhandle(PHOTO_PROTOCOL).catch(() => {});
      meshNode = null;
    }
  };
}
