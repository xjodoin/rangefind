// The thread channel, wired to a transport (threads §4.2, §6, §9).
//
// thread_publish.js seals updates and thread_consume.js validates them,
// but neither touches a network: a publisher hands you bytes and a tag,
// and a subscriber wants bytes for the tags it is listening on. Every
// host was going to have to write the same three things between them —
// derive the topic from the tag, keep the subscription following the
// 5-minute rotation, and route arriving payloads to the right
// subscriber — so they live here once.
//
// Two properties this preserves and a host could easily lose:
//
//   - Threads need no bond and work in read-only mode. Records are
//     authenticated end to end by the thread key, so bonds play no part
//     on this channel in either direction; a home viewer subscribes and
//     verifies per record. Publishing needs gossip, not admission — the
//     private seed is the whole credential.
//   - §10 rule 4: while a vehicle is publishing a thread, its *traffic*
//     contributions must stop near planned stops. A dwelling bus reports
//     0 km/h on a flowing road, and several of them corroborate each
//     other into a convincing, entirely false standstill. handleFix
//     returns that verdict; the caller must honour it.
//
// It also carries §5.5 catch-up, and that is not an optimisation: a
// phone that slept for four minutes has a hole in a thread, and the only
// alternative to filling it from other peers is a mailbox host — the
// exact server this design exists to avoid. Every follower caches the
// sealed bytes it receives and answers PMR1 for the tags it holds, so
// **availability scales with audience size**, which is backwards from a
// server, where more viewers cost more. A relay cannot open what it
// caches and does not need to be trusted: records travel verbatim, so
// tampering fails the AEAD, forgery fails the signature, and replay
// fails `seq` — at the joiner, which validates everything itself.

import { fromHex, toHex } from "./sha256.js";
import { THREAD_MODE, decodeThreadLink, encodeThreadLink, threadLinkUrl } from "./thread_codec.js";
import { THREAD_CONSTANTS, createThreadPublisher } from "./thread_publish.js";
import { THREAD_DROP, createThreadSubscriber } from "./thread_consume.js";
import { estimateArrival, scheduledArrival } from "./thread_eta.js";
import {
  applyThreadResponse,
  buildThreadRequest,
  createThreadCache,
  encodeThreadCacheResponse
} from "./thread_cache.js";
import {
  decodeDayCertificate,
  decodePhotoListRequest,
  decodePhotoListResponse,
  decodePhotoRequest,
  decodePhotoResponse,
  decodeThreadRecord,
  encodePhotoListRequest,
  encodePhotoListResponse,
  encodePhotoRequest,
  encodePhotoResponse
} from "./thread_codec.js";
import { routeFollowLink } from "./thread_route.js";
import { photoCommitment, photoHashHex, verifyPhotoChain } from "./thread_crypto.js";
import {
  decodeThreadTicket,
  jobIdHexOf,
  ticketFollowLink,
  verifyThreadTicket
} from "./thread_ticket.js";
import { isSealedTicket, openSealedTicket } from "./thread_seal.js";
import { THREAD_PROTOCOL } from "./topics.js";
import {
  base64UrlToBytes,
  generateThreadKeypair,
  publicKeyFromSeed,
  threadTopic
} from "./thread_crypto.js";
import { GOSSIP_ACCEPT, GOSSIP_IGNORE, GOSSIP_REJECT } from "./validate.js";

// How many peers a joiner asks for the gap. Measured (benchmarks §9c):
// 3 peers recover 95% of a two-minute hole, 8 recover 100% — the bound
// is how many you ask, not how large the audience is. Nothing can
// recover more than THREAD_MAX_AGE of history, because nothing older
// than that is cached anywhere.
const CATCHUP_PEERS = 8;

/**
 * How often a follow that has heard nothing looks for the run again, and
 * how many times. Ten seconds is well under the patience of somebody
 * staring at a tracking link, and twenty attempts covers a driver who
 * accepts the job a few minutes after the customer opened theirs.
 */
const FIND_RUN_INTERVAL_MS = 10_000;
const FIND_RUN_ATTEMPTS = 20;

/** The sanity ceiling on a ticket-bounded run (§20.6). */
const MAX_TICKET_RUN_SECONDS = 86400;

/** A link as a URL, a bare fragment, raw bytes, or already decoded. */
function toThreadLink(value) {
  if (!value) throw new Error("A thread follow needs a capability link.");
  if (value instanceof Uint8Array) return decodeThreadLink(value);
  if (typeof value !== "string") return value;
  const text = value.trim();
  const hash = text.indexOf("#");
  // All whitespace, not just the ends: a link that travelled through a
  // mail client or a .wayfindjob file arrives hard-wrapped, and a
  // newline in the middle of base64url is a broken paste rather than a
  // broken capability.
  const fragment = (hash >= 0 ? text.slice(hash + 1) : text).replace(/\s+/gu, "");
  return decodeThreadLink(base64UrlToBytes(fragment));
}

/**
 * Owns one host's thread traffic: the gossip tap, the subscriptions, and
 * the runs and follows currently open on it.
 *
 * - `node`: the MeshNode sharing the transport. Its `onOtherTopic` hook
 *   is where thread payloads arrive — the traffic channel parses the
 *   topic, finds the reserved `t` namespace, and hands it here.
 * - `network`: the MeshNetwork, for subscribe/publish and §5.5 catch-up.
 * - `engine`: the route graph, for positions and ETAs.
 * - `engines`: optionally `{ car, bike, foot }`, when the host holds
 *   more than one routing profile. A run's `travelMode` then picks the
 *   graph its ETA is computed on — a car graph applied to a bike courier
 *   overstates every arrival.
 * - `engineMode`: what `engine` is, for a host with only one graph.
 *   Without it an ETA reports `profileBasis: "unstated"` rather than
 *   quietly implying it matched the run.
 * - `host`: a libp2p host, when there is one. Only used for §4.2 DHT
 *   discovery, which is what makes a link self-sufficient — without it
 *   threads still work over whatever peers the transport already has.
 * - `relay`: cache sealed records for tags this peer cannot open, so a
 *   stranger's late joiner can pull the gap from here. On by default,
 *   bounded by the cache's own admission caps, and the reason
 *   availability scales with audience rather than against it.
 */
export function createThreadChannel({
  node = null,
  network = null,
  engine = null,
  engines = null,
  engineMode = null,
  host = null,
  id = node?.id ?? "threads",
  epochHex = null,
  clock = Date.now,
  rng = Math.random,
  relay = true,
  constants = THREAD_CONSTANTS
} = {}) {
  const epoch = epochHex || node?.epochHex || engine?.root?.sourceHash;
  if (!epoch) throw new Error("A thread channel needs the graph epoch.");
  const epoch32 = fromHex(epoch);
  const epochPrefix16hex = epoch.slice(0, 16);
  const epochPrefix8 = epoch32.subarray(0, 8);

  const follows = new Set();
  const runs = new Set();
  // topic -> how many follows want it. Subscriptions rotate every five
  // minutes and two follows can share a window, so this is refcounted
  // rather than owned by whichever follow subscribed last.
  const subscribed = new Map();
  const stats = {
    delivered: 0, accepted: 0, dropped: 0, published: 0,
    cached: 0, served: 0, caughtUp: 0, catchUpRounds: 0, provided: 0,
    photosServed: 0, photosFetched: 0, photosRefused: 0,
    photoListsServed: 0, photoListsFetched: 0, photoListsRefused: 0
  };
  // §5.5. Holds sealed bytes for our own threads *and*, when relaying,
  // for tags we cannot open — the second is what lets a stranger's late
  // joiner recover from this peer.
  const cache = createThreadCache({ constants, clock, rng });

  function subscribe(topic) {
    const count = subscribed.get(topic) || 0;
    subscribed.set(topic, count + 1);
    if (count === 0) network?.subscribe(id, topic);
  }

  function unsubscribe(topic) {
    const count = subscribed.get(topic) || 0;
    if (count <= 1) {
      subscribed.delete(topic);
      network?.unsubscribe(id, topic);
      return;
    }
    subscribed.set(topic, count - 1);
  }

  /** Which of our follows, if any, listens on this topic. */
  function followsOn(topicName) {
    const listening = [];
    for (const follow of follows) if (follow.topics.has(topicName)) listening.push(follow);
    return listening;
  }

  async function deliver(topicName, payload, nowMillis = clock(), fromPeer = null) {
    stats.delivered++;
    const listening = followsOn(topicName);
    let record;
    try {
      record = payload instanceof Uint8Array ? decodeThreadRecord(payload) : payload;
    } catch {
      stats.dropped++;
      return GOSSIP_IGNORE;
    }
    // A valid record on the wrong reserved topic is still not ours to
    // vouch for. Bind the envelope to the topic before any AEAD work.
    const parts = String(topicName).split("/");
    if (
      parts.length !== 7
      || parts[1] !== "rangefind"
      || parts[2] !== "pulsemesh"
      || parts[3] !== "1"
      || parts[4] !== "t"
      || parts[5] !== toHex(record.epochPrefix8)
      || parts[6] !== toHex(record.tag)
    ) {
      stats.dropped++;
      return GOSSIP_IGNORE;
    }
    // Cache before validating: a relay holds bytes it cannot open, and
    // whether *we* can read a record has nothing to do with whether the
    // next joiner needs it. Records for our own threads are `openable`
    // and exempt from the relay budget — dropping your own thread to
    // make room for someone else's is never right.
    try {
      const admitted = cache.admit(record, {
        fromPeer,
        openable: listening.length > 0,
        // A follower can authenticate before retaining. A blind relay
        // cannot, so its bounded cache is necessarily best-effort.
        retain: relay && listening.length === 0,
        nowMillis
      });
      if (!admitted.admitted) {
        stats.dropped++;
        return GOSSIP_IGNORE;
      }
      if (admitted.retained) stats.cached++;
    } catch {
      // Garbage addressed to an invented tag. The caps exist for
      // exactly this, and a decode failure is one of them working.
      stats.dropped++;
      return GOSSIP_IGNORE;
    }
    // A record is offered to every follow listening on that topic. Tags
    // are key-derived, so at most one can decrypt it; the rest reject at
    // step 3 for the cost of a byte comparison, which is the whole cost
    // of a flood from someone without the capability.
    let action = GOSSIP_ACCEPT;
    for (const follow of listening) {
      const verdict = await follow.subscriber.accept(record, { nowMillis });
      if (verdict.ok) {
        stats.accepted++;
        // A §21 certificate carries no update of its own, but accepting
        // one can release records that were waiting on it — so the host
        // hears about those, in order, rather than about the
        // certificate.
        if (verdict.update) follow.onUpdate?.(verdict.update);
        for (const released of verdict.released || []) follow.onUpdate?.(released);
      } else {
        stats.dropped++;
        if (verdict.code === THREAD_DROP.BAD_SIGNATURE) action = GOSSIP_REJECT;
        else if (verdict.code !== THREAD_DROP.AWAITING_CERTIFICATE && action !== GOSSIP_REJECT) {
          action = GOSSIP_IGNORE;
        }
      }
    }
    if (action === GOSSIP_ACCEPT && listening.length) {
      // Store only after AEAD/shape/signature validation (or the explicit
      // awaiting-certificate state) so a link holder cannot evict useful
      // catch-up history with authenticated-address garbage.
      const retained = cache.admit(record, { openable: true, nowMillis });
      if (retained.retained) stats.cached++;
    }
    return action;
  }

  /**
   * Answers a PMR1 from anyone. There is no admission check here on
   * purpose: the responder is handing back sealed bytes it may not be
   * able to open, addressed by tags that are indistinguishable from
   * random, and the joiner validates every record itself. Unknown tags
   * answer count 0 — a responder must never let a prober tell "a tag I
   * do not hold" from "a tag with nothing new", because that difference
   * is exactly which threads this peer follows.
   */
  function serveCatchUp(payload, nowMillis = clock()) {
    let answered;
    try {
      answered = cache.answer(payload, { nowMillis });
    } catch {
      return null; // not a PMR1, or malformed: silence is the answer
    }
    stats.served += answered.entries.reduce((sum, entry) => sum + entry.records.length, 0);
    return encodeThreadCacheResponse(epochPrefix8, answered);
  }

  /**
   * Answers a PMTF (§20.7): one sealed proof-of-delivery blob, by the
   * commitment the driver signed.
   *
   * Only runs *this device publishes* are consulted. A relay never holds
   * a photo — 100 KB is orders of magnitude over the thread cache's
   * admission caps and it would be the wrong thing to spend them on —
   * so availability is bounded by the driver being online, and a
   * dispatcher fetches as the marks arrive rather than at leisure.
   *
   * A hash we do not hold answers zero-length, exactly as PMR1 answers
   * count 0 for an unknown tag: a prober must not be able to tell "I am
   * not publishing that run" from "that photo does not exist".
   */
  function servePhoto(payload) {
    let request;
    try {
      request = decodePhotoRequest(payload);
    } catch {
      return null; // not a PMTF: silence, and the next seam gets a turn
    }
    for (const run of runs) {
      const sealed = run.publisher.photoFor(request.hash);
      if (sealed) {
        stats.photosServed++;
        return encodePhotoResponse({ sealed });
      }
    }
    return encodePhotoResponse({});
  }

  /**
   * Fetches one sealed photo by commitment (§20.7).
   *
   * Hash-addressed, so it takes no link: the commitment is a SHA-256 of
   * ciphertext and identifies the blob on its own. The bytes are checked
   * against it here, before anyone sees them — a peer that answers with
   * something else is answering nothing.
   *
   * Returns the **sealed** bytes. Opening them needs the run seed, which
   * this channel does not have and must not be given: pass them to
   * `openPhoto` on whichever side holds it (the driver, or the
   * dispatcher who minted the ticket).
   */
  async function fetchPhoto(hash, { peers: given = null } = {}) {
    const wanted = photoHashHex(hash);
    // Our own run first: the driver's device rendering back what it just
    // sent should not go near the network for it.
    const mine = servePhotoLocally(wanted);
    if (mine) return mine;
    if (!network?.requestPhoto) return null;
    const peers = (given ?? network.peersOf?.(id) ?? []).filter(peer => peer !== id);
    const request = encodePhotoRequest({ hash: wanted });
    for (const peer of peers) {
      const response = await Promise.resolve(network.requestPhoto(id, peer, request)).catch(() => null);
      if (!response) continue;
      let decoded;
      try {
        decoded = decodePhotoResponse(response);
      } catch {
        continue;
      }
      if (!decoded.sealed) continue;
      if (toHex(photoCommitment(decoded.sealed)) !== wanted) {
        // Not the photo that was committed to. Counted rather than
        // thrown: one lying peer must not end the fetch, and the next
        // one may be honest.
        stats.photosRefused++;
        continue;
      }
      stats.photosFetched++;
      return decoded.sealed;
    }
    return null;
  }

  function servePhotoLocally(hash) {
    for (const run of runs) {
      const sealed = run.publisher.photoFor(hash);
      if (sealed) return sealed;
    }
    return null;
  }

  /**
   * Answers a PMTL (§20.7.1): every commitment a run has published, in
   * publication order, for a holder that names one of its accumulators.
   *
   * Same protocol id and same seam as PMTF, magic-discriminated, because
   * it has the photo protocol's shape and not catch-up's: only the peer
   * publishing the run can answer, and the answer exists to make the
   * blobs behind it fetchable.
   *
   * An accumulator we do not recognise gets an empty list, exactly as an
   * unknown photo hash gets a zero-length blob — a prober must not be
   * able to tell "not publishing that run" from "no such run".
   */
  function servePhotoList(payload) {
    let request;
    try {
      request = decodePhotoListRequest(payload);
    } catch {
      return null; // not a PMTL: silence, and the next seam gets a turn
    }
    for (const run of runs) {
      const entries = run.publisher.photoListFor(request.accumulator);
      if (entries) {
        stats.photoListsServed++;
        return encodePhotoListResponse({ entries });
      }
    }
    return encodePhotoListResponse({});
  }

  /**
   * Fetches and **verifies** the commitment list behind an accumulator.
   *
   * This is what makes a commitment published into a dead zone
   * recoverable (§20.7.1). The publisher restates its list, which on its
   * own would be the publisher marking its own homework — so nothing here
   * trusts it: the chain is recomputed from A₀ and the prefix that
   * reproduces the accumulator the caller holds is the part that is
   * evidence. That accumulator came out of a signed record, so a
   * publisher that inserts, drops, reorders or backdates an entry cannot
   * reach it.
   *
   * Entries past the match are returned as `unverified` rather than
   * dropped: a holder whose newest record predates the last two photos
   * should be told those two exist and that it cannot yet vouch for
   * them. Fetch nothing on their strength.
   */
  async function fetchPhotoList(accumulator, { peers: given = null } = {}) {
    const wanted = photoHashHex(accumulator);
    const empty = { matchedAt: 0, verified: [], unverified: [] };
    const mine = photoListLocally(wanted);
    if (mine) return verifyPhotoChain(mine, wanted);
    if (!network?.requestPhoto) return empty;
    const peers = (given ?? network.peersOf?.(id) ?? []).filter(peer => peer !== id);
    const request = encodePhotoListRequest({ accumulator: wanted });
    for (const peer of peers) {
      const response = await Promise.resolve(network.requestPhoto(id, peer, request)).catch(() => null);
      if (!response) continue;
      let decoded;
      try {
        decoded = decodePhotoListResponse(response);
      } catch {
        continue;
      }
      if (!decoded.entries.length) continue;
      const checked = verifyPhotoChain(decoded.entries, wanted);
      if (!checked.matchedAt) {
        // No prefix reaches the accumulator we hold, so none of it is
        // this run's list. Counted rather than thrown: one lying peer
        // must not end the fetch, and the next may be honest.
        stats.photoListsRefused++;
        continue;
      }
      stats.photoListsFetched++;
      return checked;
    }
    return empty;
  }

  function photoListLocally(accumulator) {
    for (const run of runs) {
      const entries = run.publisher.photoListFor(accumulator);
      if (entries) return entries;
    }
    return null;
  }

  if (node) {
    const previousPhoto = node.onPhotoStream;
    // Both photo records share one seam, discriminated by magic — PMTF
    // for a blob, PMTL for the commitment list behind an accumulator.
    // Neither invents transport: the list is the prelude to the fetch and
    // travels the same way (§20.7.1).
    node.onPhotoStream = (payload, fromPeer, nowMillis) =>
      servePhoto(payload, nowMillis)
      ?? servePhotoList(payload)
      ?? previousPhoto?.(payload, fromPeer, nowMillis)
      ?? null;
    const previousStream = node.onOtherStream;
    node.onOtherStream = (payload, fromPeer, nowMillis) =>
      serveCatchUp(payload, nowMillis) ?? previousStream?.(payload, fromPeer, nowMillis) ?? null;
    const previous = node.onOtherTopic;
    node.onOtherTopic = (topicName, payload, fromPeer, nowMillis) => {
      previous?.(topicName, payload, fromPeer, nowMillis);
      deliver(topicName, payload, nowMillis, fromPeer).catch(() => {
        // A record that throws on the way in is a dropped record, never
        // a dead channel.
      });
    };
    // libp2p calls this from its GossipSub topic validator. Delivery and
    // cryptographic verification happen here, before forwarding; the
    // transport's validate-once cache then suppresses the ordinary
    // delivery callback for the same bytes.
    node.onOtherGossip = (topicName, payload, fromPeer, nowMillis) =>
      deliver(topicName, payload, nowMillis, fromPeer);
  }

  /**
   * The parts every run shares, whatever minted its key: the publisher,
   * its gossip callback, DHT discovery, and the entry in `runs`. The
   * link is passed in rather than derived here because a ticket's link
   * (§20) was handed to followers before this device saw the job.
   */
  async function openRun({
    privateSeed = null,
    daySeed = null,
    certificate = null,
    mode,
    plan = null,
    link,
    notAfter,
    startSeq = 0,
    startPreviousHash = null,
    maxRunSeconds = constants.THREAD_MAX_RUN_SECONDS,
    travelMode = null,
    baseUrl = null,
    onPublish = null
  }) {
    // The topics this run has joined in order to publish on them. Held
    // for the life of the run and released when it ends — see the note
    // in `publish` below for why an unsubscribed publisher is silent.
    const publishedTopics = new Set();
    const decodedLink = link instanceof Uint8Array ? decodeThreadLink(link) : link;
    const publisher = await createThreadPublisher({
      privateSeed,
      daySeed,
      certificate,
      threadSecret: decodedLink.threadSecret,
      epoch32,
      mode,
      plan,
      startSeq,
      startPreviousHash,
      maxRunSeconds,
      travelMode,
      clock,
      constants,
      snap: engine
        ? async point => {
            const result = await engine.snap(point).catch(() => null);
            return result?.matches?.[0] ?? null;
          }
        : null,
      publish: async emitted => {
        stats.published++;
        const topic = threadTopic(epochPrefix16hex, emitted.tag);
        // **A publisher subscribes to its own topic**, and without this
        // the run publishes into nothing at all.
        //
        // GossipSub only maintains a mesh for topics a peer has joined.
        // An unsubscribed publisher has no mesh for this topic, so
        // `publish` succeeds locally — `allowPublishToZeroTopicPeers` is
        // on, deliberately, for the genuinely-alone case — and the bytes
        // reach nobody. That is silent by construction: the driver's own
        // seq keeps climbing, its stats look healthy, and every follower
        // sees a van that never moved. It is the exact shape of the
        // failure that survived a real Android driver, a real keeper and
        // a real browser all being up at once and connected.
        //
        // Refcounted through the same map the follows use, and held for
        // the run rather than the record, because the tag rotates every
        // five minutes (§4.2) and dropping the old one immediately would
        // cut the mesh at each boundary.
        if (!publishedTopics.has(topic)) {
          subscribe(topic);
          publishedTopics.add(topic);
        }
        network?.publish(topic, emitted.bytes, id);
        // The publisher is the best catch-up source there is: it holds
        // every record by construction. Cached as `openable` so the
        // relay budget never evicts a run this device is publishing.
        try {
          if (cache.admit(emitted.bytes, { openable: true, nowMillis: clock() }).admitted) stats.cached++;
        } catch {
          // Its own record failing to decode would be a bug elsewhere.
        }
        await onPublish?.(emitted);
      }
    });
    // Join the topic **before** the first record rather than on it.
    //
    // Subscribing inside `publish` is too late by one mesh heartbeat:
    // GossipSub grafts asynchronously, so the records emitted in the
    // first second leave before there is anywhere for them to go. A run
    // that reports a few fixes and then sits still — a van parked at its
    // first drop — could publish its whole existence into that gap.
    //
    // All three windows, matching what a follow subscribes to, so the
    // five-minute tag rotation never lands on a topic this run has not
    // joined yet.
    try {
      const { threadAddresses } = await import("./thread_discovery.js");
      const addresses = await threadAddresses({
        keys: publisher.keys, epoch32, epochPrefix16hex, nowMillis: clock()
      });
      for (const address of addresses) {
        if (publishedTopics.has(address.topic)) continue;
        subscribe(address.topic);
        publishedTopics.add(address.topic);
      }
    } catch {
      // Falls back to subscribing on first publish, below. A run that
      // cannot pre-join still publishes; it just loses the first record
      // to mesh formation, which §5.5 catch-up can recover.
    }
    const discovery = await startDiscovery(publisher.keys, { advertise: true });
    const run = {
      publisher,
      link,
      notAfter,
      discovery,
      privateSeed,
      url: baseUrl ? threadLinkUrl(baseUrl, link) : null,
      mode,
      plan,
      /**
       * One GPS fix. The returned `contributeTraffic` is §10 rule 4 and
       * is not advisory: pass it to the traffic session, or a dwelling
       * vehicle publishes a standstill onto a road that is flowing.
       */
      handleFix: fix => publisher.handleFix(fix),
      announce: (note, options) => publisher.announce(note, options),
      /**
       * What happened at a stop, asserted by the driver (§5.2). Emits
       * immediately: a customer whose parcel went to a neighbour should
       * not wait out a heartbeat to hear it.
       */
      markStop: (index, outcome, options) => publisher.markStop(index, outcome, options),
      outcomes: () => publisher.outcomes(),
      /** The sealed blobs this run committed to (§20.7), by commitment. */
      photoFor: hash => publisher.photoFor(hash),
      /** §20.7.1. The accumulator this run's records currently carry. */
      photoChain: () => publisher.photoChain(),
      /** Every commitment published, in the order the chain binds them in. */
      photoChainEntries: () => publisher.photoChainEntries(),
      async finish(options) {
        const emitted = await publisher.finish(options);
        // The final record still needs to reach anyone catching up, so
        // the run leaves the set but its bytes stay in the cache until
        // THREAD_CACHE_TTL sweeps them.
        discovery?.stop();
        // Let the topics go. A device that finishes a round and keeps
        // the subscription is relaying somebody else's thread traffic
        // for nothing, and on a phone that is battery.
        for (const topic of publishedTopics) unsubscribe(topic);
        publishedTopics.clear();
        runs.delete(run);
        return emitted;
      },
      get stats() { return publisher.stats; },
      get state() { return publisher.state; },
      get seq() { return publisher.seq; },
      get stopIndex() { return publisher.stopIndex; },
      get travelMode() { return publisher.travelMode; }
    };
    runs.add(run);
    return run;
  }

  /**
   * Starts publishing a run. Returns the capability URL, which is the
   * only thing that ever needs to be shared: one capability in a fragment, no
   * host, no mailbox, no bootstrap address.
   */
  async function publish({
    baseUrl,
    mode = THREAD_MODE.COARSE,
    plan = null,
    ttlSeconds = 3 * 3600,
    privateSeed = null,
    travelMode = null,
    onPublish = null
  } = {}) {
    const keypair = await generateThreadKeypair(privateSeed);
    const notAfter = Math.floor(clock() / 1000) + ttlSeconds;
    const link = encodeThreadLink({
      threadSecret: keypair.threadSecret,
      rootPublicKey: keypair.publicKey,
      epochPrefix8,
      notAfter
    });
    return openRun({
      privateSeed: keypair.privateSeed, mode, plan, link, notAfter, travelMode, baseUrl, onPublish
    });
  }

  /**
   * Publishes one **service day** of a recurring route (§21).
   *
   * Takes the day seed and its certificate — never the route root, which
   * stays at the depot. The follow link is not minted here and must not
   * be: it is the term's link, made once by the depot from the root, and
   * the whole feature is that it does not move. Pass it in, or let this
   * rebuild the identical link from the certificate root and thread secret.
   */
  async function publishRouteDay({
    baseUrl,
    daySeed,
    certificate,
    mode = THREAD_MODE.COARSE,
    plan = null,
    link = null,
    threadSecret = null,
    notAfter = null,
    travelMode = null,
    onPublish = null
  } = {}) {
    const cert = certificate instanceof Uint8Array ? decodeDayCertificate(certificate) : certificate;
    if (!cert) throw new Error("A route day publishes under a day certificate (§21).");
    // Without an explicit term expiry the run is bounded by the day the
    // certificate covers, which is the conservative reading: a depot
    // that did not say how long the term is has not authorised one.
    const linkNotAfter = notAfter ?? cert.notAfter;
    const followLink = link || routeFollowLink({
      threadSecret,
      rootPublicKey: cert.rootPublicKey,
      epochPrefix8,
      notAfter: linkNotAfter
    });
    return openRun({
      daySeed,
      certificate: cert,
      mode,
      plan,
      link: followLink,
      notAfter: linkNotAfter,
      // The run stops when the day's authority does, not when the term
      // does: a certificate is the bound on how long this device may
      // move the bus.
      maxRunSeconds: Math.max(0, cert.notAfter - Math.floor(clock() / 1000)),
      travelMode,
      baseUrl,
      onPublish
    });
  }

  /**
   * Where a previous holder of this ticket got to (§20 handover).
   *
   * The catch-up machinery wants a follow entry, so this builds one —
   * but it must never join `follows`: it exists for a single round of
   * PMR1 against the peers we already have, and leaving it there would
   * keep a subscription open on a thread this device is about to
   * publish. No peers is not a failure; a first assignment resumes at 0.
   */
  async function resumeAuthority(link, nowMillis = clock()) {
    const subscriber = await createThreadSubscriber({
      link: decodeThreadLink(link), epoch32, clock, constants
    });
    const entry = { link, subscriber, onUpdate: null, topics: new Set(), lastCatchUpMillis: -Infinity };
    await catchUp(entry, { nowMillis }).catch(() => 0);
    return {
      seq: subscriber.highestSeq,
      previousHash: subscriber.latest()?.recordHash ?? null
    };
  }

  /**
   * Publishes a run from a dispatch ticket (§20). The link followers
   * already hold came from the ticket, so this device is joining a run
   * that already has an audience rather than starting one.
   *
   * Handover assumes the previous holder has stopped. Both devices hold
   * the same seed, so interleaved publishing is not corruption — every
   * follower still enforces strictly increasing `seq` and simply drops
   * the loser of each race — but it is nonsense, and stopping the old
   * device is the dispatcher's job, out of band.
   *
   * `travelMode` is only a fallback: a dispatcher who wrote "bike" into
   * the plan decided that, and the ticket wins. It exists because a
   * courier app knows which vehicle it is running on and a plan that
   * left the field at 0 makes every follower's ETA `unstated`.
   *
   * **What a host passes is a sealed PME1 ticket and this device's
   * private key** (§20.9). Once opened, everything below is exactly what
   * it was: the seal is confidentiality and device-addressing, and the
   * issuer's signature is still the only thing that says the job is
   * real. An already-decoded inner ticket is also accepted, for the
   * internal callers and tests that hold one — but a host that reaches
   * for that path is handing plaintext tickets around, which is the
   * thing §20.9 exists to stop.
   *
   * A **route-day** ticket (§21.11) arrives here too, and it must not go
   * down the ordinary path. Its seed is a *day* key, so deriving topics
   * and content keys from it would publish the route onto an address no
   * parent is subscribed to — a run that looks healthy on the driver's
   * phone while every term link goes silent. The branch is made here,
   * from the artifact, rather than left to a caller to route by hand:
   * `link` and the publisher come off the certificate's **root**, and
   * the day seed only signs.
   */
  async function publishTicket(input, {
    baseUrl = null,
    onPublish = null,
    catchUp: doCatchUp = true,
    travelMode = null,
    devicePrivateKey = null,
    link: termLink = null
  } = {}) {
    // A string is base64url, possibly hard-wrapped by a mail client and
    // possibly still wearing its `wayfind://ticket#` prefix — decoded to
    // bytes here rather than in decodeThreadTicket, because the magic is
    // what decides whether these bytes need opening first.
    let bytes = input instanceof Uint8Array ? input : null;
    if (typeof input === "string") {
      const raw = input.trim();
      const hash = raw.lastIndexOf("#");
      bytes = base64UrlToBytes((hash >= 0 ? raw.slice(hash + 1) : raw).replace(/\s+/gu, ""));
    }
    if (bytes && isSealedTicket(bytes)) {
      if (!devicePrivateKey) {
        throw new Error(
          "This job is sealed to a device: publishTicket needs the device private key to open it."
        );
      }
      bytes = await openSealedTicket(bytes, devicePrivateKey);
    }
    const ticket = bytes ? decodeThreadTicket(bytes) : input;
    const verdict = await verifyThreadTicket(ticket, { epochPrefix8, nowMillis: clock() });
    if (!verdict.ok) throw new Error(`This dispatch ticket cannot be published: ${verdict.reason}.`);
    if (!ticket.privateSeed) {
      throw new Error("This ticket carries no run seed: an offer is not a publish capability.");
    }
    // The term capability on a route day, the run's own on a job —
    // `ticketFollowLink` reads which from the certificate, so there is
    // no arm of this function that can derive a link from a day key.
    const link = termLink || await ticketFollowLink(ticket);
    const resume = doCatchUp ? await resumeAuthority(link) : { seq: 0, previousHash: null };
    const run = await openRun({
      // §21.11: identity from the certificate's root, authority from the
      // seed. Passing the day seed as `privateSeed` would make the *day*
      // key the run's identity, which is the silent-parent failure the
      // route-day ticket exists to make unrepresentable.
      privateSeed: ticket.dayCertificate ? null : ticket.privateSeed,
      daySeed: ticket.dayCertificate ? ticket.privateSeed : null,
      certificate: ticket.dayCertificate,
      mode: ticket.mode,
      plan: ticket.plan,
      link,
      notAfter: ticket.notAfter,
      startSeq: resume.seq,
      startPreviousHash: resume.previousHash,
      travelMode: ticket.plan?.travelMode || travelMode,
      // The ticket's expiry *is* the run bound: a dispatched delivery day
      // is 8–10 hours and the publisher's own 6 h default would stop it
      // mid-afternoon, mid-route, with customers still watching. Clamped
      // at 24 h because `notAfter` is a uint32 a dispatcher can fat-finger
      // into next year, and no run should outlive a shift by that much.
      //
      // A route day is bounded by its *certificate* instead, as
      // `publishRouteDay` is: the ticket's `notAfter` is the term there,
      // and a run that outlived the day's authority would go on emitting
      // records no subscriber can verify. The 24 h clamp still applies —
      // a certificate may span 48 h (§21.5) and no shift does.
      maxRunSeconds: Math.min(
        Math.max(0, (ticket.dayCertificate?.notAfter ?? ticket.notAfter) - Math.floor(clock() / 1000)),
        MAX_TICKET_RUN_SECONDS
      ),
      baseUrl,
      onPublish
    });
    run.ticket = ticket;
    run.jobIdHex = jobIdHexOf(ticket);
    return run;
  }

  /**
   * Follows a run from its link. `linkOrUrl` is a URL with the capability
   * in its fragment, a raw 78-byte link, or an already-decoded one.
   */
  async function follow(linkOrUrl, { onUpdate = null, cellContext = null } = {}) {
    const link = toThreadLink(linkOrUrl);
    const subscriber = await createThreadSubscriber({ link, epoch32, clock, constants, cellContext });
    const entry = {
      link,
      subscriber,
      onUpdate,
      topics: new Set(),
      /** Where the vehicle is, or null when the run withholds position. */
      async position() {
        const update = subscriber.latest();
        if (!update?.segment || !engine) return null;
        const point = await engine.locate(update.segment, update.ratio).catch(() => null);
        return point ? { ...point, update } : null;
      },
      /**
       * When it reaches `myStopIndex`, routed locally from the reported
       * position under whatever live metric the caller passes. The
       * publisher never learns which stop anyone asked about.
       */
      async eta({ plan, myStopIndex, live = null, nowMillis = clock() }) {
        const update = subscriber.latest();
        if (!update) return scheduledArrival({ plan, myStopIndex });
        return (await estimateArrival({
          engine, engines, engineMode, update, plan, myStopIndex, live, constants, nowMillis
        })) ?? scheduledArrival({ plan, myStopIndex });
      },
      status: options => subscriber.status(options),
      latest: () => subscriber.latest(),
      history: () => subscriber.history(),
      /** Pull whatever this follow missed from other peers, on demand. */
      catchUp: options => catchUp(entry, options),
      /**
       * One sealed proof-of-delivery photo by the commitment on a mark
       * (§20.7). Sealed, not open: only a holder of the run seed — the
       * driver, or the dispatcher who minted the ticket — can call
       * `openPhoto` on the result, and a follower holding only the
       * follow link cannot. Null when nobody reachable holds it.
       */
      fetchPhoto: (hash, options) => fetchPhoto(hash, options),
      /**
       * Every commitment the run has published, recovered from the
       * publisher and checked against an accumulator this follow holds
       * (§20.7.1). Defaults to the newest accumulator in the newest
       * record — the strongest one available, and the one that leaves
       * the fewest entries unverified.
       *
       * Returns `{ matchedAt, verified, unverified }`. Fetch photos on
       * the strength of `verified` only.
       */
      fetchPhotoList: (accumulator = null, options) => {
        const head = accumulator ?? subscriber.latest()?.photoChain ?? null;
        if (!head) return Promise.resolve({ matchedAt: 0, verified: [], unverified: [] });
        return fetchPhotoList(head, options);
      },
      get stats() { return subscriber.stats; },
      lastCatchUpMillis: -Infinity,
      discovery: null,
      stop() {
        for (const topic of entry.topics) unsubscribe(topic);
        entry.topics.clear();
        entry.discovery?.stop();
        if (entry.seekTimer) {
          clearInterval(entry.seekTimer);
          entry.seekTimer = null;
        }
        follows.delete(entry);
      }
    };
    follows.add(entry);
    await refresh(entry);
    // §4.2: the link is a key, not a location. With a DHT the follower
    // finds and dials peers holding this thread from the capability
    // alone; without one it uses whatever peers the transport has.
    if (host?.contentRouting) {
      entry.discovery = await startDiscovery(entry.subscriber.keys, { advertise: false });
      await entry.discovery?.connect().catch(() => []);
      // **And keep looking, until something is heard.**
      //
      // One lookup at follow time assumes the run is already advertised,
      // and the ordinary sequence is the other way round: a dispatcher
      // sends the link when the job is created, the driver accepts it
      // later, and the driver is not reachable until a relay grants a
      // reservation after that. A customer who opens their link in that
      // window — which is most customers — would find nobody and never
      // ask again, leaving a page that is correctly wired and silent
      // forever.
      //
      // Stops on the first record, because from then on gossip carries
      // the run and re-dialling is noise. Bounded so a link to a run that
      // never starts does not search for the rest of the day.
      let attempts = 0;
      entry.seekTimer = setInterval(() => {
        if (entry.subscriber.latest() || attempts >= FIND_RUN_ATTEMPTS) {
          clearInterval(entry.seekTimer);
          entry.seekTimer = null;
          return;
        }
        attempts++;
        void entry.discovery?.connect().catch(() => []);
      }, FIND_RUN_INTERVAL_MS);
      if (typeof entry.seekTimer.unref === "function") entry.seekTimer.unref();
    }
    // A follow that starts mid-run has a hole by definition. Filling it
    // from other followers is the whole of §8 path 2 — the alternative
    // is a mailbox host, which is the server this design exists without.
    await catchUp(entry).catch(() => 0);
    return entry;
  }

  /**
   * Fills the gap in one follow from other peers (§5.5, §8 path 2).
   *
   * The request is padded to 4/8/16 tags with CSPRNG decoys — free on
   * this channel, since a tag is indistinguishable from random, so even
   * a peer holding the same thread cannot tell which entry we came for.
   * How much is recovered depends on how many peers are asked, not on
   * how large the audience is (benchmarks §9c: 3 → 95%, 8 → 100%), and
   * nothing can recover more than THREAD_MAX_AGE of history because
   * nothing older is cached anywhere.
   */
  async function catchUp(entry, { nowMillis = clock(), peers: given = null } = {}) {
    if (!network?.requestThread) return 0;
    const peers = (given ?? network.peersOf?.(id) ?? []).filter(peer => peer !== id);
    if (!peers.length) return 0;
    const tags = await entry.subscriber.currentTags(nowMillis);
    const cursor = entry.subscriber.cursor();
    let request;
    try {
      request = buildThreadRequest({
        epochPrefix8,
        wanted: tags.map(tag => ({
          tag,
          sinceGeneration: cursor.generation,
          sinceSeq: cursor.seq
        })),
        rng
      });
    } catch {
      return 0;
    }
    stats.catchUpRounds++;
    // Shuffled, so a joiner does not always lean on the same neighbour.
    const asked = [...peers];
    for (let i = asked.length - 1; i > 0; i--) {
      const j = Math.floor(rng() * (i + 1));
      [asked[i], asked[j]] = [asked[j], asked[i]];
    }
    let accepted = 0;
    for (const peer of asked.slice(0, CATCHUP_PEERS)) {
      const response = await Promise.resolve(network.requestThread(id, peer, request.bytes))
        .catch(() => null);
      if (!response) continue;
      accepted += await applyThreadResponse(entry.subscriber, response, { nowMillis, wantedTags: tags })
        .catch(() => 0);
    }
    if (accepted) {
      stats.caughtUp += accepted;
      entry.onUpdate?.(entry.subscriber.latest());
    }
    entry.lastCatchUpMillis = nowMillis;
    return accepted;
  }

  /**
   * DHT discovery for one thread, when the host has content routing.
   * Only publishers advertise. Followers can answer padded catch-up
   * requests from connected peers without publishing their interest to
   * the DHT.
   */
  async function startDiscovery(keys, { advertise = false } = {}) {
    if (!host?.contentRouting) return null;
    const { createThreadDiscovery } = await import("./thread_discovery.js");
    const discovery = createThreadDiscovery({
      host, keys, epoch32, epochPrefix16hex, constants, clock, advertise
    });
    discovery.start();
    stats.provided++;
    return discovery;
  }

  /** Moves one follow's subscriptions onto the current tag window. */
  async function refresh(entry, nowMillis = clock()) {
    const wanted = new Set(await entry.subscriber.topics(nowMillis));
    for (const topic of wanted) {
      if (entry.topics.has(topic)) continue;
      entry.topics.add(topic);
      subscribe(topic);
    }
    for (const topic of [...entry.topics]) {
      if (wanted.has(topic)) continue;
      entry.topics.delete(topic);
      unsubscribe(topic);
    }
  }

  /**
   * Follows the tag rotation. Cheap and idempotent — call it on the same
   * cadence as the traffic channel's tick, and drop follows whose link
   * has expired rather than listening to a capability that is over.
   */
  async function tick(nowMillis = clock()) {
    cache.sweep(nowMillis);
    // Publishing keeps advertising. Discovery has its own interval
    // timer, but that is a *browser* timer, and the platform this
    // matters most on — a driver's phone, screen off, half a day into a
    // run — throttles a backgrounded WebView's timers to nothing while
    // the host keeps ticking on its own clock. Without this line the
    // run keeps publishing, the windows rotate, the provider records
    // lapse, and a customer opening their link mid-afternoon finds
    // nobody — while everyone already connected keeps hearing records,
    // so nothing on the phone looks wrong. `provide` rate-limits itself
    // per window, so ticking it is idempotent, not chatty.
    for (const run of [...runs]) {
      if (run.discovery) await run.discovery.provide({ nowMillis }).catch(() => 0);
    }
    for (const entry of [...follows]) {
      if (entry.subscriber.expired(nowMillis)) {
        entry.stop();
        continue;
      }
      await refresh(entry, nowMillis);
      // A thread that has gone quiet is either over or missed — and the
      // two look identical from here, so ask. Bounded by the poll
      // interval so a genuinely finished run costs one request per
      // window, not one per tick.
      const status = entry.subscriber.status({ nowMillis });
      const due = nowMillis - entry.lastCatchUpMillis >= constants.THREAD_POLL_INTERVAL * 1000;
      if (!status.live && due) await catchUp(entry, { nowMillis }).catch(() => 0);
    }
  }

  function close() {
    for (const entry of [...follows]) entry.stop();
    for (const run of runs) run.discovery?.stop();
    runs.clear();
    if (node) {
      node.onOtherTopic = null;
      node.onOtherGossip = null;
      node.onOtherStream = null;
      node.onPhotoStream = null;
    }
  }

  return {
    publish,
    publishRouteDay,
    publishTicket,
    follow,
    tick,
    close,
    stats,
    epoch,
    get runs() { return [...runs]; },
    get follows() { return [...follows]; },
    /** Answers a PMR1 directly, for hosts wiring their own transport. */
    serveCatchUp,
    /** Answers a PMTF directly, for the same reason (§20.7). */
    servePhoto,
    /** Answers a PMTL — the commitment list behind an accumulator (§20.7.1). */
    servePhotoList,
    /** Fetches one sealed photo by commitment; verifies before returning. */
    fetchPhoto,
    /** Fetches a commitment list and verifies it against a signed accumulator. */
    fetchPhotoList,
    cache,
    /** For hosts that receive thread bytes off some other transport. */
    deliver
  };
}
