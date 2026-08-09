// PulseMesh in the browser, for the OSM demo.
//
// Two modes, both real protocol bytes through the real store, validator,
// aggregation and provider:
//
//   "local" — peers inside this tab over an in-memory network, fed by
//             simulated vehicles crawling the corridor you routed. No
//             infrastructure at all, so it works from `file://` and from
//             a static host. This is a demonstration of the mechanism,
//             and the UI says so.
//   "mesh"  — a real libp2p host over WebSockets to a keeper, joining the
//             actual mesh. Needs a keeper multiaddr and the transport
//             bundle; falls back to "local" when either is absent.
//
// A browser joins the real mesh **read-only** (§11.6): it mints no
// admission bond, joins no gossip topic, and pulls everything it shows
// over the padded sync path. That is not a limitation of the demo, it is
// the design — a tab that will never contribute should not pay a 256 MiB
// mint, and an unbonded peer sitting in the gossip mesh would only punch
// holes in other peers' delivery paths.
//
// What is *not* faked in either mode: contributions go through the
// contributor pipeline (snap → quality → speed bin → reticent gates),
// cross a MeshNode's gossip path, get validated by the twelve rules
// against this device's own copy of the static index, aggregate by
// weighted median, and reach the router as an ordinary
// LiveTrafficProvider. The router does not know which mode it is in.

import {
  DEFAULT_CONSTANTS,
  createCorridorTraffic,
  createLoopbackNetwork,
  createMeshSession
} from "./pulsemesh.browser.js";

// Cadence is the contributor's own rule and has its own tests; a demo
// that respected it would take a quarter of an hour to show anything.
const DEMO_CONSTANTS = Object.freeze({ ...DEFAULT_CONSTANTS, EMIT_INTERVAL: 0.01 });
const SIM_INTERVAL_MS = 4000;

// The dispatcher's Ed25519 identity, kept in localStorage so that every
// ticket this browser issues verifies against one issuer key: §20's
// `expectedIssuerHex` is how a driver app refuses a well-formed ticket
// from someone it does not work for, and that check is worth nothing if
// the issuer is regenerated per page load. It is a demo identity, not a
// credential — a real dispatcher's seed lives wherever its accountability
// already lives (§4.3).
const DISPATCHER_SEED_KEY = "rangefind.dispatcher.seed";

// The jobs this browser has issued, by `planRef` hex. §20.7 photos are
// dispatcher-only *by construction*: the key derives from the run seed,
// which lives in the ticket this dispatcher minted and kept. So the only
// way this tab can show a delivery photo is by still holding the ticket
// for the run it is watching — which is exactly the property the design
// claims, made visible. A page that has never issued the job sees the
// commitment and nothing else, however hard it tries.
//
// What is stored is the **sealed** artifact (§20.9), never the plaintext
// ticket: this browser can read it back only because it sealed a wrap to
// its own device key, which is the same mechanism a driver's phone uses
// and is worth demonstrating rather than bypassing.
const DISPATCHER_JOBS_KEY = "rangefind.dispatcher.jobs";

// This browser's device identity (§20.9) — an X25519 keypair whose only
// job is to be the address a job can be sealed to. It sits beside the
// dispatcher seed and follows its naming, and like it, it is a demo
// identity: localStorage is not a Keystore and cannot be made into one.
// **Clearing site data destroys this key**, and every job sealed to it
// becomes unopenable by anybody — the page says so where it shows it.
const DEVICE_KEY_KEY = "rangefind.device.key";
const DEVICE_NAME_KEY = "rangefind.device.name";
// The devices this browser has enrolled: `{ publicKey, name, fingerprint,
// addedAt }`. Enrolment is the gate on transfer — a job can only be
// sealed to a device whose card is in here.
const DEVICE_ROSTER_KEY = "rangefind.devices";
// Offers this browser has received as a courier (§20.4), keyed by jobId
// hex, plus the jobId of the one it last replied to. The second is what
// makes the award check honest: a courier is asking "is this the job I
// agreed to", not "does this ticket happen to match something I have" —
// and under a bait-and-switch the awarded ticket's jobId is precisely
// the thing that does *not* match.
const HELD_OFFERS_KEY = "rangefind.courier.offers";
const BID_OFFER_KEY = "rangefind.courier.bid";

// The fleet's own seed (§20.10): the multiaddr(s) of the keeper this
// operator runs, kept beside the device key because they are the same
// kind of thing — settings about *this installation*, not about any job.
//
// A seed is a **location**, not a capability. Losing this list costs a
// cold start and nothing else; publishing it discloses an address, which
// is why the demo puts it in the sealed ticket and never in an offer.
const FLEET_SEED_KEY = "rangefind.fleet.seed";
// Peers this browser has actually connected to, so the seed matters at
// first contact and at no other time. The library reports them
// (`knownPeers()`); persisting them is this host's business, and this
// host is a demo, so localStorage is where they go.
const KNOWN_PEERS_KEY = "rangefind.mesh.peers";
const KNOWN_PEERS_MAX = 8;

function hexToBytes(hex) {
  const text = String(hex || "").trim();
  const out = new Uint8Array(text.length >> 1);
  for (let i = 0; i < out.length; i++) out[i] = Number.parseInt(text.substr(i * 2, 2), 16);
  return out;
}

function bytesToHex(bytes) {
  let out = "";
  for (const byte of bytes) out += byte.toString(16).padStart(2, "0");
  return out;
}

/**
 * The fragment of a URL, a bare fragment, or a bare capability string.
 *
 * All whitespace goes, not just the ends: a ticket that arrived as a
 * .wayfindjob file or through a mail client comes back hard-wrapped, and
 * a newline in the middle of base64url is a broken paste rather than a
 * broken capability.
 */
function fragmentOf(text) {
  const raw = String(text || "").trim();
  const hash = raw.lastIndexOf("#");
  return (hash >= 0 ? raw.slice(hash + 1) : raw).replace(/\s+/gu, "");
}

/**
 * Wraps a route-graph engine with a PulseMesh session.
 *
 * `engine` is the already-open route graph; its `root.sourceHash` is the
 * mesh epoch, so live states are bound to exactly this index build.
 */
export function createPulseMeshDemo({
  engine,
  mode = "local",
  keeperAddress = null,
  onChange = null
} = {}) {
  let session = null;      // what this tab consumes and routes with
  let sim = null;          // local mode only: the simulated drivers
  let network = null;
  let host = null;
  let threads = null;
  let viewer = null;           // local mode: the "other device" that follows
  let viewerThreads = null;
  let run = null;              // this tab's published drive, if any
  // Whether `run` came from a dispatch ticket. Deliberately a flag and
  // not the ticket bytes: the inner artifact lives on `run.ticket` for
  // the one operation that needs it (re-sealing on handover) and is never
  // exposed as something a caller could render, download or paste (§20.9).
  let runTicket = false;
  let follow = null;           // someone else's drive, if we are watching
  let activeMode = null;
  let simTimer = null;
  const state = { error: null };

  // --- Transports --------------------------------------------------------

  async function startLocal() {
    network = createLoopbackNetwork();
    session = await createMeshSession({
      engine, network, id: "you", constants: DEMO_CONSTANTS, transport: "loopback"
    });
    sim = await createCorridorTraffic({
      engine, network, constants: DEMO_CONSTANTS, transport: "loopback"
    });
    activeMode = "local";
  }

  async function startMesh(address) {
    // The transport bundle is an order of magnitude larger than the core
    // and is only needed on this path, so it loads on demand: a page that
    // never joins the real mesh never pays for libp2p.
    const { createPulseMeshHost, createLibp2pNetwork } = await import("./pulsemesh-libp2p.browser.js");
    host = await createPulseMeshHost({
      profile: "browser",
      // The `?keeper=` address this page was opened with, plus whatever
      // seed this installation was given (§20.10). Either alone is
      // enough; a fleet that has enrolled its own seed does not need the
      // URL to carry one.
      bootstrapPeers: [address, ...fleetSeeds()].filter(Boolean),
      // And the peers this browser met last time, which is what makes a
      // second start not depend on the seed being up.
      rememberedPeers: rememberedPeers()
    });
    network = createLibp2pNetwork(host);
    await network.ready;
    rememberPeers();
    session = await createMeshSession({
      engine,
      network,
      id: host.peerId.toString(),
      // §11.6. A tab reads; it does not publish and does not relay.
      readOnly: true,
      transport: "wire"
    });
    sim = null;
    activeMode = "mesh";
  }

  /** Starts the mesh. Returns the mode actually achieved. */
  async function start() {
    if (activeMode) return activeMode;
    if (mode === "mesh" && keeperAddress) {
      try {
        await startMesh(keeperAddress);
        session.start();
        return activeMode;
      } catch (error) {
        // A keeper that is down must not break the page: the whole
        // contract is that the router keeps working without live data.
        state.error = `keeper unreachable (${error?.message || error}) — using in-tab peers`;
      }
    }
    await startLocal();
    session.start();
    return activeMode;
  }

  async function stop() {
    stopSimulation();
    if (run) await run.finish().catch(() => {});
    if (threads) { threads.close(); threads = null; }
    if (viewerThreads) { viewerThreads.close(); viewerThreads = null; }
    if (viewer) { await viewer.close().catch(() => {}); viewer = null; }
    run = null; runTicket = false; follow = null;
    sim?.stop();
    if (session) await session.close().catch(() => {});
    if (host?.stop) await host.stop().catch(() => {});
    session = null; sim = null; network = null; host = null;
    activeMode = null;
    state.error = null;
  }

  // --- Corridor ----------------------------------------------------------

  /**
   * Points the mesh at the routes on screen: subscribe to their zones,
   * pull their cells, warm their leaves. Only the z9 zone (≈55 km) is
   * ever visible to a peer, and the cell requests leave padded, shuffled
   * and split across peers.
   */
  async function followRoutes(routes) {
    if (!session) return [];
    const list = (Array.isArray(routes) ? routes : [routes]).filter(Boolean);
    const zones = await session.followRoute(list);
    if (sim) await sim.follow(list);
    return zones;
  }

  // --- The simulated traffic (local mode only) ---------------------------

  async function simulateOnce(options) {
    if (!sim) return 0;
    const published = await sim.tick(options);
    onChange?.();
    return published;
  }

  function startSimulation() {
    if (simTimer || !sim) return false;
    simulateOnce().catch(() => {});
    simTimer = setInterval(() => { simulateOnce().catch(() => {}); }, SIM_INTERVAL_MS);
    return true;
  }

  function stopSimulation() {
    if (!simTimer) return;
    clearInterval(simTimer);
    simTimer = null;
  }

  // --- Threads -----------------------------------------------------------

  async function threadChannel() {
    if (threads) return threads;
    if (!session?.node) return null;
    const { createThreadChannel } = await import("./pulsemesh-threads.browser.js");
    threads = createThreadChannel({
      node: session.node,
      network,
      engine,
      // This demo ships one graph and it is a driving graph. Naming it is
      // what lets an ETA for a bike run say it was computed on a car
      // profile instead of quietly implying it fits.
      engineMode: "car",
      id: session.node.id
    });
    return threads;
  }

  /**
   * The channel a *follower* uses.
   *
   * A publisher and a follower on one peer cannot see each other: no
   * transport loops a publish back to its own sender, in gossipsub or
   * anywhere else. On the real mesh the follower is simply a different
   * device; in this tab it has to be a different peer, so local mode
   * gets a "viewer" — its own mesh node, holding nothing but the link,
   * exactly as a phone across town would.
   */
  async function viewerChannel() {
    if (activeMode !== "local") return threadChannel();
    if (viewerThreads) return viewerThreads;
    const { createThreadChannel } = await import("./pulsemesh-threads.browser.js");
    viewer = await createMeshSession({
      engine, network, id: "viewer", constants: DEMO_CONSTANTS, transport: "loopback"
    });
    viewerThreads = createThreadChannel({
      node: viewer.node, network, engine, engineMode: "car", id: viewer.node.id
    });
    return viewerThreads;
  }

  /**
   * Starts publishing this drive, and returns the shareable link.
   *
   * `mode` is not a bandwidth setting (threads §11): coarse publishes
   * stop events and a heartbeat with **no position at all**, so a leaked
   * link is worth roughly what a printed timetable is; fine publishes a
   * live locator. The right default follows the harm, which is why the
   * page asks rather than choosing.
   */
  async function shareDrive({
    plan = null, fine = true, travelMode = 0, baseUrl = location.href.split("#")[0]
  } = {}) {
    const channel = await threadChannel();
    if (!channel) return null;
    const { THREAD_MODE } = await import("./pulsemesh-threads.browser.js");
    if (run) await run.finish().catch(() => {});
    run = await channel.publish({
      baseUrl,
      mode: fine ? THREAD_MODE.FINE : THREAD_MODE.COARSE,
      plan,
      travelMode,
      ttlSeconds: 3 * 3600
    });
    // A drive this device started owns its own seed (§4.1); there is no
    // ticket behind it and nothing to hand over.
    runTicket = false;
    return run;
  }

  /** Ends the published run: one final record, after which it is dead. */
  async function endDrive() {
    if (!run) return false;
    await run.finish().catch(() => {});
    run = null;
    runTicket = false;
    return true;
  }

  // --- Dispatch tickets (§20) --------------------------------------------

  /**
   * This browser's dispatcher identity, minted on first use.
   *
   * `issuerSeed` is not the run's seed and never becomes one: it only
   * ever signs. A ticket's run seed is generated fresh inside
   * `issueTicket` per job, which is what bounds a leak to one delivery.
   */
  function dispatcherSeed() {
    const stored = localStorage.getItem(DISPATCHER_SEED_KEY);
    if (stored && /^[0-9a-f]{64}$/iu.test(stored.trim())) return hexToBytes(stored.trim());
    const seed = crypto.getRandomValues(new Uint8Array(32));
    localStorage.setItem(DISPATCHER_SEED_KEY, bytesToHex(seed));
    return seed;
  }

  function epochPrefix8() {
    const hex = session?.epoch ?? engine.root.sourceHash;
    return hexToBytes(String(hex).slice(0, 16));
  }

  // --- The fleet's own seed (§20.10) -------------------------------------
  //
  // A 45-byte follow link and a sealed ticket are capabilities: they say
  // what a run *is*, and nothing about how to reach the mesh it lives
  // on. A device with no dialable peer joins nothing. For a fleet the
  // answer is not a DHT — ten drivers who can all dial one machine have
  // a working mesh — it is one keeper the operator runs, and this is
  // where its address lives.

  /** The seed addresses this installation was given. */
  function fleetSeeds() {
    try {
      const parsed = JSON.parse(localStorage.getItem(FLEET_SEED_KEY) || "[]");
      return Array.isArray(parsed) ? parsed.filter(entry => typeof entry === "string" && entry.startsWith("/")) : [];
    } catch {
      return [];
    }
  }

  /**
   * Sets them from typed text: newline- or comma-separated multiaddrs.
   *
   * Validated by the codec rather than by this page, so the sentence a
   * dispatcher reads when they paste a hostname is the same one the
   * ticket encoder would have refused them with.
   */
  async function setFleetSeeds(text) {
    const { normalizeBootstrapAddresses } = await import("./pulsemesh-threads.browser.js");
    const list = normalizeBootstrapAddresses(
      String(text || "").split(/[\n,]+/u).map(part => part.trim()).filter(Boolean),
      { what: "seed address" }
    );
    try {
      localStorage.setItem(FLEET_SEED_KEY, JSON.stringify(list));
    } catch {
      // Private browsing: the seed works for this tab and dies with it.
    }
    return list;
  }

  /**
   * Enrols a seed from a `wayfind://seed#…` card — the link the keeper
   * prints, as a QR on its own terminal.
   *
   * A card is a location, so enrolling one grants nothing and needs no
   * fingerprint ceremony: an address that names the wrong machine does
   * not impersonate a seed, it fails to connect, because the
   * `/p2p/<peerId>` in the multiaddr is what libp2p authenticates.
   */
  async function enrolSeed(cardText) {
    const { parseSeedCardUrl } = await import("./pulsemesh-threads.browser.js");
    const card = parseSeedCardUrl(cardText);
    const merged = [...card.addresses];
    for (const existing of fleetSeeds()) {
      if (merged.length >= 3) break;
      if (!merged.includes(existing)) merged.push(existing);
    }
    try {
      localStorage.setItem(FLEET_SEED_KEY, JSON.stringify(merged));
    } catch {
      // As above.
    }
    return { addresses: card.addresses, label: card.label, seeds: merged };
  }

  /** Peers this browser has met, newest first. */
  function rememberedPeers() {
    try {
      const parsed = JSON.parse(localStorage.getItem(KNOWN_PEERS_KEY) || "[]");
      return Array.isArray(parsed) ? parsed.filter(entry => typeof entry === "string") : [];
    } catch {
      return [];
    }
  }

  /**
   * Records whatever this host is connected to right now.
   *
   * The library reports; the host persists. That split is the whole
   * seam: what a device is allowed to remember about who it has talked
   * to is a product decision, and a browser tab, a driver's phone and a
   * keeper will all answer it differently.
   */
  function rememberPeers() {
    const seen = network?.knownPeers?.() ?? [];
    if (!seen.length) return rememberedPeers();
    const merged = [...seen];
    for (const previous of rememberedPeers()) {
      if (merged.length >= KNOWN_PEERS_MAX) break;
      if (!merged.includes(previous)) merged.push(previous);
    }
    try {
      localStorage.setItem(KNOWN_PEERS_KEY, JSON.stringify(merged));
    } catch {
      // Quota or private browsing: next start pays a cold bootstrap.
    }
    return merged;
  }

  // --- This device, and the devices it has enrolled (§20.9) --------------

  let deviceKeys = null;

  /**
   * This browser's device keypair, minted on first use.
   *
   * Not the dispatcher seed and not any run seed: it is X25519, it never
   * signs, and its only job is to be the address a sealed job can be
   * addressed to. It is persisted because enrolment is worthless if the
   * key changes per page load — a card handed out yesterday must still
   * name this browser today.
   *
   * The honest caveat, which the page repeats to the user: this is
   * localStorage. A browser has no Keystore and no Secure Enclave, so
   * clearing site data deletes this private key, and every job sealed to
   * it becomes unopenable — by anybody, including the dispatcher, unless
   * it also sealed a wrap to itself.
   */
  async function device() {
    if (deviceKeys) return deviceKeys;
    const { generateDeviceKeypair } = await import("./pulsemesh-threads.browser.js");
    const stored = localStorage.getItem(DEVICE_KEY_KEY);
    if (stored && /^[0-9a-f]{64}$/iu.test(stored.trim())) {
      deviceKeys = await generateDeviceKeypair(hexToBytes(stored.trim()));
      return deviceKeys;
    }
    deviceKeys = await generateDeviceKeypair();
    try {
      localStorage.setItem(DEVICE_KEY_KEY, bytesToHex(deviceKeys.privateKey));
    } catch {
      // Private browsing. The key works for this tab and dies with it,
      // which is exactly what the page's warning already describes.
    }
    return deviceKeys;
  }

  function deviceName() {
    return localStorage.getItem(DEVICE_NAME_KEY) || "This browser";
  }

  /**
   * What this device shows another device: its name, the fingerprint two
   * people read aloud, and its PMV1 card as a `wayfind://device#…` link.
   */
  async function deviceIdentity() {
    const { publicKey } = await device();
    const { deviceCardUrl, fingerprint } = await import("./pulsemesh-threads.browser.js");
    const name = deviceName();
    return {
      publicKeyHex: bytesToHex(publicKey),
      fingerprint: fingerprint(publicKey),
      name,
      cardUrl: deviceCardUrl({ publicKey, name })
    };
  }

  /**
   * Renames this device. The cap is the protocol's (32 **bytes**, not
   * characters) and the encoder is the authority, so the refusal the
   * caller shows is the encoder's own sentence.
   */
  async function setDeviceName(name) {
    const { encodeDeviceCard } = await import("./pulsemesh-threads.browser.js");
    const { publicKey } = await device();
    const trimmed = String(name || "").trim();
    // Throws on an over-long name rather than truncating: a device whose
    // name was silently cut is a device somebody enrols by the wrong name.
    encodeDeviceCard({ publicKey, name: trimmed });
    localStorage.setItem(DEVICE_NAME_KEY, trimmed);
    return deviceIdentity();
  }

  function roster() {
    try {
      const parsed = JSON.parse(localStorage.getItem(DEVICE_ROSTER_KEY) || "[]");
      return Array.isArray(parsed) ? parsed.filter(entry => /^[0-9a-f]{64}$/iu.test(entry?.publicKey || "")) : [];
    } catch {
      return [];
    }
  }

  function saveRoster(list) {
    try {
      localStorage.setItem(DEVICE_ROSTER_KEY, JSON.stringify(list));
    } catch {
      // Quota or private browsing: the roster is this session's only.
    }
  }

  /**
   * Enrols a device from its card — a `wayfind://device#…` link, a bare
   * fragment, or the card bytes.
   *
   * Re-enrolling a card that is already here updates its name rather
   * than adding a second row: a device is its key, and the name is only
   * the label a human recognises.
   */
  async function enrolDevice(cardText) {
    const { parseDeviceCardUrl } = await import("./pulsemesh-threads.browser.js");
    const card = parseDeviceCardUrl(cardText);
    const publicKeyHex = bytesToHex(card.publicKey);
    const mine = await deviceIdentity();
    const list = roster();
    const existing = list.find(entry => entry.publicKey.toLowerCase() === publicKeyHex);
    const entry = existing || { publicKey: publicKeyHex, addedAt: Date.now() };
    entry.name = card.name || "Unnamed device";
    entry.fingerprint = card.fingerprint;
    if (!existing) list.push(entry);
    saveRoster(list);
    return { ...entry, self: publicKeyHex === mine.publicKeyHex, alreadyEnrolled: Boolean(existing) };
  }

  /**
   * The recipients a job is sealed to: the chosen device, and this one so
   * the issuer keeps access. Deduplicated, because in this demo the two
   * are often the same browser — sealing twice to one key would work and
   * would put two identical hints in the record for nothing.
   */
  async function recipientsFor(targetPublicKeyHex) {
    const mine = await device();
    const target = hexToBytes(targetPublicKeyHex);
    return bytesToHex(mine.publicKey) === String(targetPublicKeyHex).toLowerCase()
      ? [mine.publicKey]
      : [target, mine.publicKey];
  }

  function removeDevice(publicKeyHex) {
    const wanted = String(publicKeyHex || "").toLowerCase();
    const list = roster().filter(entry => entry.publicKey.toLowerCase() !== wanted);
    saveRoster(list);
    return list;
  }

  /**
   * Issues a dispatch ticket for a job, and both links that come out of
   * it (§20.1).
   *
   * The two links are deliberately unequal and the UI must say so: the
   * driver link carries the run's private seed and is a **publish**
   * capability, while the customer link is the ordinary 45-byte
   * read-only follow link — derived from the same ticket, so it can be
   * printed on the order confirmation before any driver exists.
   *
   * The mesh does not have to be running to issue: composing a job is
   * not publishing one, and a dispatcher who had to join a mesh to write
   * an order confirmation would be a dispatcher waiting on a network for
   * no reason.
   */
  /** Tickets this browser issued, keyed by planRef hex. */
  function issuedJobs() {
    try {
      const parsed = JSON.parse(localStorage.getItem(DISPATCHER_JOBS_KEY) || "{}");
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch {
      return {};
    }
  }

  function rememberIssuedJob(planRefHex, ticketBase64) {
    const jobs = issuedJobs();
    jobs[planRefHex] = ticketBase64;
    // A demo, not an archive: keep the last handful so localStorage does
    // not grow without bound across a day of clicking.
    const keys = Object.keys(jobs);
    for (const stale of keys.slice(0, Math.max(0, keys.length - 12))) delete jobs[stale];
    try {
      localStorage.setItem(DISPATCHER_JOBS_KEY, JSON.stringify(jobs));
    } catch {
      // Private-browsing quota. The job still works; only the photo does not.
    }
  }

  async function issueJob({
    stops = [],
    fine = true,
    travelMode = 0,
    ttlSeconds = 3 * 3600,
    recipient = null,
    baseUrl = location.href.split("#")[0]
  } = {}) {
    if (!stops.length) throw new Error("A job needs at least one stop.");
    // §20.9's gate, enforced here rather than in the page: a job is
    // sealed to a device the dispatcher has already met, and there is no
    // argument meaning "seal to whoever ends up holding this".
    const wanted = String(recipient || "").toLowerCase();
    const target = roster().find(entry => entry.publicKey.toLowerCase() === wanted);
    if (!target) {
      throw new Error(
        "A job is sealed to one enrolled device. Enrol the driver's device card first, then pick it here."
      );
    }
    const {
      THREAD_MODE, bytesToBase64Url, encodeThreadPlan, issueSealedTicket, threadLinkUrl
    } = await import("./pulsemesh-threads.browser.js");
    let mode = fine ? THREAD_MODE.FINE : THREAD_MODE.COARSE;
    const plan = {
      dwellSeconds: 0,
      travelMode,
      stops: stops.map((stop, index) => ({ index: index + 1, ...stop }))
    };
    let planBytes = encodeThreadPlan(plan);
    let notAfter = Math.floor(Date.now() / 1000) + Math.round(ttlSeconds);
    // If this round was advertised, award the job that was advertised.
    // `jobId` hashes the expiry, so re-deriving `notAfter` a minute later
    // would move it and the courier's check would fail on an honest job —
    // the offer's own plan and window are reused verbatim. Edit the round
    // between broadcasting and issuing and the bytes differ, which is
    // exactly the mismatch `awardMatchesOffer` exists to catch.
    const advertised = pendingOffer
      && pendingOffer.planBytes.length === planBytes.length
      && pendingOffer.planBytes.every((byte, i) => byte === planBytes[i]);
    if (advertised) {
      planBytes = pendingOffer.planBytes;
      notAfter = pendingOffer.notAfter;
      mode = pendingOffer.mode;
    }
    const issued = await issueSealedTicket({
      // The driver's device *and* this dispatcher's own, at one 64-byte
      // wrap each rather than a second copy of the ticket. Sealing to
      // itself is what keeps the dispatcher able to read back what it
      // dispatched — and to derive the §20.7 photo key later. A
      // dispatcher that leaves itself out has minted a job it can never
      // open again, which the library will do and never does by default.
      recipients: await recipientsFor(target.publicKey),
      issuerSeed: dispatcherSeed(),
      epochPrefix8: epochPrefix8(),
      notAfter,
      mode,
      // §20.2: how the round is to be made travels in the plan, so it is
      // hashed into planRef and the follower's ETA can name the profile
      // it used rather than silently quoting a car's arrival for a bike.
      // The spread carries §20.8's per-stop metadata — `orderRef`,
      // `parcels`, `instructions`, `contact` — straight into the plan
      // codec, and nowhere else. The plan lives inside the ticket; what
      // goes on the wire is the 8-byte `planRef` hash of it, so none of
      // these fields can reach a follower or a relay.
      planBytes,
      // §20.10: the fleet's own seed, inside the signed ticket. Because
      // the ticket is sealed, the address reaches the one device this
      // job was awarded to and nobody else — which is why it is here and
      // deliberately not in the offer, an artifact broadcast to
      // strangers.
      bootstrap: fleetSeeds()
    });
    // Keyed by `planRef`, because that is the only identity of the job
    // that reaches a follower: it is in every body (§5.2), and it is what
    // lets this tab recognise a run it dispatched from an update alone.
    // The **sealed** bytes are what is kept; opening them later needs
    // this device's key, exactly as the driver's phone needs its own.
    rememberIssuedJob(bytesToHex(issued.ticket.plan.planRef), issued.base64url);
    return {
      ticketBase64: issued.base64url,
      sealed: true,
      driverUrl: `${baseUrl}#${issued.base64url}`,
      customerUrl: `${baseUrl}#${bytesToBase64Url(issued.link)}`,
      // The same capability with the seed hinted in the **query**
      // (§20.10). Two placements, one difference: the fragment is the
      // capability and is never transmitted, so the page host never sees
      // it; the query is a public address the recipient's device may
      // dial or ignore, and the page host does see it. The fragments of
      // these two URLs are byte-identical.
      customerHintUrl: fleetSeeds().length
        ? threadLinkUrl(baseUrl, issued.link, { bootstrap: fleetSeeds().slice(0, 2) })
        : null,
      bootstrap: issued.bootstrap,
      jobIdHex: issued.jobIdHex,
      issuerHex: bytesToHex(issued.ticket.issuerPublicKey),
      notAfter: issued.ticket.notAfter,
      recipientCount: issued.recipientCount,
      recipient: { name: target.name, fingerprint: target.fingerprint, publicKey: target.publicKey },
      mode,
      travelMode,
      /** True when this ticket awards a round this dispatcher advertised. */
      advertised: Boolean(advertised)
    };
  }

  // --- Offers (§20.4) ------------------------------------------------------
  //
  // The dispatcher side. An offer is the only artifact this demo hands
  // out in the clear on purpose: it goes to couriers nobody has enrolled
  // yet, so there is no key to seal it to, and it is safe to post
  // because it contains a commitment and coarse descriptors rather than
  // anything worth stealing.
  //
  // The offer is minted from a plan and an expiry that are then **kept**,
  // so that pressing "Create a job ticket" afterwards awards the job that
  // was actually advertised. Without that the demo would re-derive
  // `notAfter` a few seconds later, `jobId` would move, and the award
  // check would fail on every honest run — teaching the opposite of the
  // lesson. Change the round after broadcasting and the plan no longer
  // matches, which is the mismatch this page is for.
  let pendingOffer = null;

  async function broadcastOffer({
    stops = [],
    fine = true,
    travelMode = 0,
    ttlSeconds = 3 * 3600,
    totalMeters = 0,
    payMinor = null,
    currency = null,
    label = null,
    baseUrl = location.href.split("#")[0]
  } = {}) {
    if (!stops.length) throw new Error("An offer describes a round, so it needs at least one stop.");
    const {
      THREAD_MODE, encodeThreadPlan, issueJobOffer
    } = await import("./pulsemesh-threads.browser.js");
    const mode = fine ? THREAD_MODE.FINE : THREAD_MODE.COARSE;
    // The whole plan, metadata and all — exactly the plan the ticket will
    // carry, because `planRef` is a hash of these bytes. None of it goes
    // into the offer; only the hash does.
    const planBytes = encodeThreadPlan({
      dwellSeconds: 0,
      travelMode,
      stops: stops.map((stop, index) => ({ index: index + 1, ...stop }))
    });
    const notAfter = Math.floor(Date.now() / 1000) + Math.round(ttlSeconds);
    const issued = await issueJobOffer({
      issuerSeed: dispatcherSeed(),
      epochPrefix8: epochPrefix8(),
      planBytes,
      notAfter,
      mode,
      totalMeters,
      payMinor,
      currency,
      label
    });
    pendingOffer = { planBytes, notAfter, mode, travelMode, jobIdHex: issued.jobIdHex };
    return {
      offerBase64: issued.base64url,
      offerUrl: `${baseUrl}#${issued.base64url}`,
      jobIdHex: issued.jobIdHex,
      bytes: issued.bytes.length,
      ...describeOfferFields(issued.offer)
    };
  }

  /** The coarse summary both sides render, from the decoded offer alone. */
  function describeOfferFields(offer) {
    return {
      stopCount: offer.stopCount,
      centroid: { lat: offer.centroid.lat, lon: offer.centroid.lon, gridDegrees: offer.centroid.gridDegrees },
      spread: offer.spread,
      spreadLabel: offer.spreadLabel,
      spreadMaxMeters: offer.spreadMaxMeters,
      totalMeters: offer.totalMeters,
      travelMode: offer.travelMode,
      mode: offer.mode,
      payMinor: offer.payMinor,
      currency: offer.currency,
      label: offer.label,
      notAfter: offer.notAfter,
      issuerHex: bytesToHex(offer.issuerPublicKey)
    };
  }

  /**
   * The courier side: what an offer says, and what it deliberately does
   * not. Verified before it is shown — an offer is public, so anybody can
   * post one, and an unsigned one is somebody's guess about a job.
   */
  async function describeOffer(offerInput) {
    const { decodeJobOffer, verifyJobOffer } = await import("./pulsemesh-threads.browser.js");
    const offer = decodeJobOffer(fragmentOf(offerInput));
    const verdict = await verifyJobOffer(offer, { epochPrefix8: epochPrefix8() });
    return {
      jobIdHex: offer.jobIdHex,
      planRefHex: bytesToHex(offer.planRef),
      ok: verdict.ok,
      reason: verdict.reason || null,
      ...describeOfferFields(offer)
    };
  }

  function heldOffers() {
    try {
      const parsed = JSON.parse(localStorage.getItem(HELD_OFFERS_KEY) || "{}");
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch {
      return {};
    }
  }

  /**
   * Keeps an offer this device has replied to. `bid` records *which* one,
   * because that is the question the award check answers: not "does this
   * ticket match something I hold" but "is this the job I agreed to".
   */
  function rememberOffer(offerInput, { bid = false } = {}) {
    const fragment = fragmentOf(offerInput);
    const offers = heldOffers();
    return import("./pulsemesh-threads.browser.js").then(({ decodeJobOffer }) => {
      const offer = decodeJobOffer(fragment);
      offers[offer.jobIdHex] = fragment;
      const keys = Object.keys(offers);
      for (const stale of keys.slice(0, Math.max(0, keys.length - 8))) delete offers[stale];
      try {
        localStorage.setItem(HELD_OFFERS_KEY, JSON.stringify(offers));
        if (bid) localStorage.setItem(BID_OFFER_KEY, offer.jobIdHex);
      } catch {
        // Private-browsing quota. The offer still reads; only the award
        // check loses its reference point, and it says so rather than
        // passing a job it cannot check.
      }
      return offer.jobIdHex;
    });
  }

  /**
   * Does the awarded ticket match the offer this device bid on?
   *
   * Returns `null` when there is nothing to check against — a job that
   * arrived without an offer is the ordinary §20.9 dispatch path and is
   * not suspicious. Returns a verdict otherwise, and the caller refuses
   * on `ok: false` with the reason the core named.
   */
  async function checkAward(ticket) {
    const { awardMatchesOffer, jobIdHexOf } = await import("./pulsemesh-threads.browser.js");
    const offers = heldOffers();
    let held = offers[jobIdHexOf(ticket)];
    let advertised = null;
    if (!held) {
      // Nothing matches by jobId. If this device bid on something, that
      // is the offer to check — and the mismatch is the whole point.
      try {
        advertised = localStorage.getItem(BID_OFFER_KEY);
      } catch {
        advertised = null;
      }
      held = advertised ? offers[advertised] : null;
    }
    if (!held) return null;
    return { ...awardMatchesOffer(held, ticket), bidOnly: Boolean(advertised) };
  }

  /**
   * Re-seals the job this device is publishing, to another enrolled
   * device (§20.9 handover).
   *
   * The inner ticket never becomes an artifact: it is opened in memory
   * when the job is accepted, and sealed straight back to the next
   * device here. There is no point in the flow where a plaintext ticket
   * exists as something a host could show, download or forward — which
   * is the whole rule. No card, no transfer: a device that was never
   * enrolled has no key to seal to, and the job cannot go to it.
   */
  async function handOverJob(recipient, { baseUrl = location.href.split("#")[0] } = {}) {
    if (!run?.ticket?.bytes) {
      throw new Error("No job is being published from this device, so there is nothing to hand over.");
    }
    const wanted = String(recipient || "").toLowerCase();
    const target = roster().find(entry => entry.publicKey.toLowerCase() === wanted);
    if (!target) {
      throw new Error(
        "That device is not enrolled here. Ask for its device card, enrol it, then hand the job over."
      );
    }
    const { bytesToBase64Url, sealTicket } = await import("./pulsemesh-threads.browser.js");
    const recipients = await recipientsFor(target.publicKey);
    const sealed = await sealTicket(run.ticket.bytes, recipients);
    const base64url = bytesToBase64Url(sealed);
    return {
      ticketBase64: base64url,
      sealed: true,
      driverUrl: `${baseUrl}#${base64url}`,
      customerUrl: null,
      jobIdHex: run.jobIdHex || "",
      recipientCount: recipients.length,
      recipient: { name: target.name, fingerprint: target.fingerprint, publicKey: target.publicKey },
      mode: run.ticket.mode,
      travelMode: run.ticket.plan?.travelMode ?? 0,
      notAfter: run.ticket.notAfter
    };
  }

  /**
   * The inner ticket behind whatever arrived, opened with **this**
   * device's key.
   *
   * A sealed job is the only form a ticket travels in since §20.9, so
   * this is where the enrolment gate is felt: a job sealed for another
   * device comes back as `SEAL_ERROR.NOT_ENROLLED`, and the sentence the
   * caller shows is the core's own — "sealed for another device" — not a
   * decode failure, because the two have different fixes.
   */
  async function openIncomingTicket(ticketInput) {
    const {
      SEAL_ERROR, base64UrlToBytes, decodeThreadTicket, isSealedTicket, openSealedTicket
    } = await import("./pulsemesh-threads.browser.js");
    const bytes = base64UrlToBytes(fragmentOf(ticketInput));
    if (!isSealedTicket(bytes)) {
      // A plaintext PMK1 is an artifact that should not exist: it
      // carries the run seed and the customers' phone numbers, and
      // everything it passed through could read both. Refusing it is the
      // rule, not caution.
      throw new Error(
        "This job arrived unsealed. Since §20.9 a job is encrypted to the device it is for — "
        + "an unsealed one was readable by everything it travelled over. Ask the dispatcher for a sealed job."
      );
    }
    const { privateKey } = await device();
    try {
      return { ticket: decodeThreadTicket(await openSealedTicket(bytes, privateKey)), sealed: bytes };
    } catch (error) {
      if (error?.code === SEAL_ERROR.NOT_ENROLLED) {
        throw new Error(
          `${error.message} This device's fingerprint is ${(await deviceIdentity()).fingerprint} — `
          + "show it its card and ask for the job again."
        );
      }
      throw error;
    }
  }

  /**
   * What a ticket says, without taking it.
   *
   * A driver is being handed a publish capability and told to drive
   * somewhere; being shown where, until when, and by whom before
   * pressing accept is the whole difference between an offer and an
   * instruction. The verdict comes from `verifyThreadTicket` rather than
   * from a well-formed decode: a ticket for last week's epoch parses
   * perfectly and cannot be published. Reading it at all needs this
   * device's key — a job sealed for somebody else never reaches here.
   */
  async function describeJob(ticketInput) {
    const { classifyThreadArtifact, jobIdHexOf, verifyThreadTicket } =
      await import("./pulsemesh-threads.browser.js");
    // An unreadable ticket is still a ticket, and saying so is the whole
    // difference between "ask the dispatcher for a fresh one" and a
    // decoder's complaint about some other artifact's byte length. A
    // sealed one carries a reason too, but it is the *sealed* sentence
    // and it is only true for a device that cannot open it — so the
    // opening happens first and this reason is used only when it fails.
    const artifact = classifyThreadArtifact(ticketInput);
    if (artifact.reason && !artifact.sealed) throw new Error(artifact.reason);
    const { ticket } = await openIncomingTicket(ticketInput);
    const verdict = await verifyThreadTicket(ticket, { epochPrefix8: epochPrefix8() });
    // §20.4: if this device bid on an offer, the ticket has to be the job
    // that was advertised. A well-formed, correctly signed ticket for
    // some *other* round is exactly the attack the commitment exists for,
    // and it must be refused by name rather than accepted quietly.
    const award = await checkAward(ticket).catch(() => null);
    return {
      jobIdHex: jobIdHexOf(ticket),
      stops: ticket.plan.stops,
      mode: ticket.mode,
      travelMode: ticket.plan.travelMode,
      notAfter: ticket.notAfter,
      issuerHex: bytesToHex(ticket.issuerPublicKey),
      seedPresent: ticket.seedPresent,
      award,
      ok: verdict.ok && ticket.seedPresent && (award ? award.ok : true),
      reason: verdict.ok
        ? (ticket.seedPresent
          ? (award && !award.ok ? `it is not the job that was offered — ${award.reason}` : null)
          : "this ticket carries no run seed, so it is not a publish capability")
        : verdict.reason
    };
  }

  /**
   * Takes the job: publishes the run the ticket names.
   *
   * `publishTicket` verifies the signature and epoch, then resumes above
   * whatever `seq` the audience has already seen (§20.5), so a handover
   * continues the customer's link instead of orphaning it. The run then
   * becomes this controller's ordinary `run`, which is what makes
   * `threadFix`, `endDrive` and the `sharing` snapshot work unchanged.
   *
   * The **sealed** bytes go to `publishTicket` with this device's private
   * key: opening is the channel's job, and doing it here would mean a
   * plaintext ticket travelling one more hop than it has to (§20.9). A
   * job sealed for another device throws before anything is published.
   */
  async function acceptJob(ticketInput) {
    const channel = await threadChannel();
    if (!channel) {
      throw new Error("Turn live traffic on before accepting a job — a run publishes onto the mesh.");
    }
    const { base64UrlToBytes, jobIdHexOf } = await import("./pulsemesh-threads.browser.js");
    // Opened once here only so the offer this device is about to take can
    // be refused with the enrolment sentence rather than a channel error;
    // `publishTicket` opens the sealed bytes itself.
    const { ticket } = await openIncomingTicket(ticketInput);
    // The same check the accept card already ran, run again where the
    // consequence is: a page bug that showed the card and then published
    // anyway would put a courier on a road they never agreed to.
    const award = await checkAward(ticket).catch(() => null);
    if (award && !award.ok) {
      throw new Error(
        `This is not the job that was offered — ${award.reason}. Ask the dispatcher to send the round `
        + "they advertised, or to broadcast a new offer for this one."
      );
    }
    const { privateKey } = await device();
    if (run) await run.finish().catch(() => {});
    run = await channel.publishTicket(base64UrlToBytes(fragmentOf(ticketInput)), {
      baseUrl: location.href.split("#")[0],
      devicePrivateKey: privateKey
    });
    runTicket = true;
    return {
      run,
      job: {
        jobIdHex: jobIdHexOf(ticket),
        stops: ticket.plan.stops,
        mode: ticket.mode,
        travelMode: ticket.plan.travelMode,
        notAfter: ticket.notAfter,
        issuerHex: bytesToHex(ticket.issuerPublicKey)
      }
    };
  }

  /**
   * What an incoming fragment is: a job to take, a job to bid on, a
   * drive to watch, a device to enrol, or none of them. They all live in
   * the same place — the URL fragment, which browsers never transmit —
   * and they are worth wildly different things, so the page must branch
   * on the artifact rather than on how it arrived. An **offer** (§20.4)
   * is its own kind precisely because it is the one that is safe to have
   * arrived from a stranger.
   *
   * Returns `{ kind, reason }`, not a bare string: an artifact this build
   * cannot read is still one of the two, and the page owes its holder a
   * sentence about the thing they actually have.
   */
  async function classifyFragment(text) {
    const fragment = fragmentOf(text);
    if (!fragment) return { kind: null, reason: null };
    const { classifyThreadArtifact, decodeDeviceCard } = await import("./pulsemesh-threads.browser.js");
    // A third artifact now arrives the same way (§20.9): a device card,
    // which is neither a capability to watch nor one to publish — it is
    // an address to seal *to*. Its magic decides it, like the others.
    try {
      const card = decodeDeviceCard(fragment);
      return { kind: "device", reason: null, sealed: false, card };
    } catch {
      // Not a card. Fall through: the thread decoders own the answer.
    }
    // `{ kind, reason }`, and the reason matters: identity comes from the
    // magic, so an artifact this build cannot decode is still named for
    // what it is rather than reported as whatever failed last.
    return classifyThreadArtifact(fragment);
  }

  /** Follows someone else's drive from their link. */
  async function followDrive(link, { onUpdate = null } = {}) {
    const channel = await viewerChannel();
    if (!channel) return null;
    follow = await channel.follow(link, { onUpdate, cellContext: session.cellContext });
    return follow;
  }

  function stopFollowing() {
    follow?.stop();
    follow = null;
  }

  /**
   * When the followed vehicle reaches a stop, routed locally from the
   * position it reported under *this* device's live-traffic metric. The
   * publisher broadcasts one position and never learns which stop
   * anyone asked about — unlike every server-side ETA, which by
   * construction knows each recipient's address.
   */
  async function followedEta(plan, myStopIndex = 1) {
    if (!follow || !plan?.stops?.length) return null;
    return follow.eta({ plan, myStopIndex, live: session?.provider() ?? null });
  }

  /**
   * What happened at a stop on the run this tab is publishing.
   *
   * Delivery is the one thing the vehicle's movement cannot establish:
   * dwelling by a door says the van stopped, not that the parcel was
   * handed over, refused, or taken back. So this is a button the driver
   * presses, it publishes at once rather than on the next heartbeat, and
   * nothing anywhere infers an outcome from a dwell.
   */
  /**
   * What happened at a stop on the run this tab is publishing.
   *
   * `photo` is optional proof of delivery (§20.7) and is **already
   * compressed and stripped** by the caller: this controller does not
   * touch image bytes and `markStop` cannot — the recompression that
   * removes the camera's EXIF GPS fix is the page's job, and it is a
   * MUST, because an untouched camera file publishes a customer's front
   * door past every granularity control the run has.
   */
  async function markStop(index, outcome, { reason = 0, note = null, photo = null } = {}) {
    if (!run) throw new Error("No drive is being published, so there is no stop to mark.");
    await run.markStop(index, outcome, { reason, note, photo });
    onChange?.();
    return run.outcomes();
  }

  /**
   * The delivery photo behind a mark on the run this tab is *watching*.
   *
   * Returns `{ holdsSeed: false }` when this browser did not issue the
   * job — which is the ordinary case for a customer, and is not a
   * failure to be worked around. The commitment is visible to them; the
   * bytes are not, because the key comes from a seed they were never
   * given (§20.7). The page shows the difference rather than hiding it.
   */
  async function followedPhoto({ photoHash, stopIndex, planRef }) {
    if (!photoHash || !follow) return { holdsSeed: false, bytes: null };
    const planRefHex = bytesToHex(planRef || new Uint8Array(8));
    const sealedBase64 = issuedJobs()[planRefHex];
    if (!sealedBase64) return { holdsSeed: false, bytes: null };
    const { openPhoto } = await import("./pulsemesh-threads.browser.js");
    // The stored artifact is sealed, so reading back a job this browser
    // issued goes through the same device key a driver's phone uses. A
    // pre-§20.9 plaintext ticket left in localStorage simply fails to
    // open here, which is correct: that artifact should not exist.
    const opened = await openIncomingTicket(sealedBase64).catch(() => null);
    const ticket = opened?.ticket;
    if (!ticket?.privateSeed) return { holdsSeed: false, bytes: null };
    // Sealed bytes, fetched by content hash from whoever holds them —
    // in practice the driver, because nothing relays a blob this size.
    const sealed = await follow.fetchPhoto(photoHash).catch(() => null);
    if (!sealed) return { holdsSeed: true, bytes: null };
    const bytes = await openPhoto(sealed, {
      privateSeed: ticket.privateSeed,
      planRef: planRef || new Uint8Array(8),
      stopIndex,
      hash: photoHash
    });
    return { holdsSeed: true, bytes };
  }

  /** The plan and progress of the run this tab publishes, for the driver row. */
  function drivingSnapshot() {
    if (!run?.plan?.stops?.length) return null;
    return {
      stops: run.plan.stops,
      outcomes: run.outcomes(),
      stopIndex: run.stopIndex,
      travelMode: run.travelMode
    };
  }

  async function tickThreads() {
    if (threads) await threads.tick().catch(() => {});
    if (viewerThreads) await viewerThreads.tick().catch(() => {});
    // Connections come and go; the point of remembering them is that the
    // seed stops mattering after first contact, so the list is refreshed
    // from whatever this host is actually connected to now.
    if (activeMode === "mesh") rememberPeers();
  }

  // --- What the page reads -----------------------------------------------

  const snapshot = () => ({
    mode: activeMode,
    sharing: run ? { url: run.url, seq: run.seq, state: run.state, notAfter: run.notAfter } : null,
    // Deliberately without a `claim`: §12's sentence depends on whether
    // *this device* also has live traffic, which the page knows and this
    // controller does not. One claim, composed in one place.
    following: follow ? (({ claim, ...rest }) => rest)(follow.status()) : null,
    threads: threads?.stats ?? null,
    viewer: viewerThreads?.stats ?? null,
    simulating: simTimer != null,
    corridor: sim?.corridor.length ?? 0,
    jamSamples: sim?.jamCount ?? 0,
    vehicles: sim?.vehicles.length ?? 0,
    error: state.error,
    ...(session ? session.snapshot() : { available: false })
  });

  return {
    start,
    stop,
    followRoutes,
    simulateOnce,
    startSimulation,
    stopSimulation,
    shareDrive,
    endDrive,
    markStop,
    followedPhoto,
    drivingSnapshot,
    issueJob,
    broadcastOffer,
    describeOffer,
    rememberOffer,
    handOverJob,
    describeJob,
    acceptJob,
    classifyFragment,
    deviceIdentity,
    setDeviceName,
    roster,
    enrolDevice,
    removeDevice,
    fleetSeeds,
    setFleetSeeds,
    enrolSeed,
    rememberedPeers,
    followDrive,
    stopFollowing,
    followedEta,
    tickThreads,
    /** One GPS fix to the published run; honour `contributeTraffic`. */
    threadFix: fix => (run ? run.handleFix(fix) : Promise.resolve(null)),
    get run() { return run; },
    /**
     * Whether the current run came from a dispatch ticket — which is all
     * a page needs to know. The ticket itself stays inside: handing it on
     * goes through `handOverJob`, which seals it to a named device rather
     * than returning bytes anyone could show (§20.9).
     */
    get hasActiveTicket() { return Boolean(run && runTicket); },
    get follow() { return follow; },
    get threadStats() { return threads?.stats ?? null; },
    snapshot,
    traffic: options => (session ? session.traffic(options) : Promise.resolve([])),
    incidents: options => (session ? session.incidents(options) : Promise.resolve([])),
    /**
     * Watch the corridor a drive is on and say when it changes under it.
     * Returns null with no session, so a caller never has to branch on
     * whether the mesh happens to be up.
     */
    watchRoute: (routes, options) => (session ? session.watchRoute(routes, options) : null),
    /** One GPS fix from the page (the demo drive, or real geolocation). */
    onLocation: fix => (session ? session.onLocation(fix) : Promise.resolve({ emitted: false })),
    report: options => (session ? session.reportIncident(options) : Promise.resolve({ emitted: false })),
    answer: options => (session ? session.answerIncident(options) : Promise.resolve({ emitted: false })),
    setContributing: value => session?.setContributing(value) ?? false,
    provider: () => session?.provider() ?? null,
    get contributing() { return Boolean(session?.contributing); },
    get session() { return session; },
    get mode() { return activeMode; },
    get state() { return state; },
    get epoch() { return session?.epoch ?? engine.root.sourceHash; }
  };
}
