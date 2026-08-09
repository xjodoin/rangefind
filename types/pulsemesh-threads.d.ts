/**
 * PulseMesh Threads — authenticated tracking of one moving thing for a
 * bounded audience. See docs/pulsemesh-threads.md.
 *
 * The capability model in one line: the private key writes, the public
 * key reads. `P` is distributed, never published — it must not be logged,
 * reused across runs, used as a peer identity, or put on the wire.
 */

/** Derived from the capability `P`; none of it is ever transmitted. */
export interface ThreadKeys {
  publicKey: Uint8Array;
  topicKey: Uint8Array;
  contentKey: Uint8Array;
  noncePrefix: Uint8Array;
}

export interface ThreadKeypair {
  privateSeed: Uint8Array;
  publicKey: Uint8Array;
}

export declare const THREAD_TOPIC_PREFIX: string;

export declare function generateThreadKeypair(seed?: Uint8Array | null): Promise<ThreadKeypair>;
export declare function publicKeyFromSeed(privateSeed: Uint8Array): Promise<Uint8Array>;
export declare function deriveThreadKeys(publicKey: Uint8Array): Promise<ThreadKeys>;
export declare function threadWindow(unixMillis: number): number;
export declare function threadTag(keys: ThreadKeys, epoch32: Uint8Array, window: number): Promise<Uint8Array>;
export declare function threadTagsForWindows(keys: ThreadKeys, epoch32: Uint8Array, windows: number[]): Promise<Uint8Array[]>;
export declare function threadTopic(epochPrefix16hex: string, tag: Uint8Array): string;
/** The DHT key holders advertise as providers for — what makes a link self-sufficient. */
export declare function threadRendezvous(topic: string): Uint8Array;
export declare function threadNonce(noncePrefix: Uint8Array, seq: number): Uint8Array;
export declare function sealThreadBody(keys: ThreadKeys, seq: number, aad: Uint8Array, plaintext: Uint8Array): Promise<Uint8Array>;
/** Returns null on any AEAD failure — a wrong key, a tampered record. */
export declare function openThreadBody(keys: ThreadKeys, seq: number, aad: Uint8Array, ciphertext: Uint8Array): Promise<Uint8Array | null>;

/**
 * §20.7's per-stop photo key, and the whole privacy model in one line.
 *
 * It derives from the run's **private seed**, not from the capability
 * `P`. A customer holds the 45-byte link, which carries `P`, an epoch
 * and an expiry — no seed — so a customer structurally cannot derive
 * this. The driver holds the seed from the ticket, and the dispatcher
 * kept it when minting that ticket (§20.1's inversion of §4.1), so
 * photos are dispatcher-only in v1 by construction.
 */
export declare function photoKeyFor(
  privateSeed: Uint8Array, planRef: Uint8Array | null, stopIndex: number
): Promise<Uint8Array>;
/** AES-256-GCM with a fresh 12-byte IV prepended (not the §5.1 nonce rule). */
export declare function sealPhoto(
  key: Uint8Array, plaintext: Uint8Array, options?: { iv?: Uint8Array | null }
): Promise<Uint8Array>;
/** The commitment that goes on the wire: SHA-256 of the *sealed* bytes. */
export declare function photoCommitment(sealed: Uint8Array): Uint8Array;
/**
 * Opens a fetched blob. `hash` is required and checked against the
 * sealed bytes **before** a key is imported, so bytes that do not match
 * the commitment the driver signed are refused however well they would
 * decrypt. Throws — never returns null — so a caller rendering a proof
 * of delivery cannot ignore the failure by accident.
 */
export declare function openPhoto(sealed: Uint8Array, options: {
  privateSeed: Uint8Array;
  planRef: Uint8Array | null;
  stopIndex: number;
  hash: string | Uint8Array;
}): Promise<Uint8Array>;
export declare function photoHashBytes(hash: string | Uint8Array): Uint8Array;
export declare function photoHashHex(hash: string | Uint8Array): string;
export declare function signThread(message: Uint8Array, privateSeed: Uint8Array): Promise<Uint8Array>;
export declare function verifyThread(message: Uint8Array, signature: Uint8Array, publicKey: Uint8Array): Promise<boolean>;
/**
 * Overrides Ed25519 with a host-supplied implementation. Hosts whose
 * WebCrypto lacks Ed25519 (Android WebView < 137, Hermes) no longer need
 * this — a pure-JS RFC 8032 fallback takes over automatically — so it is
 * now only for a host that would rather bring its own, e.g. a native one.
 * Pass `null` to go back to the built-in behaviour.
 */
export declare function setThreadCryptoImplementation(implementation: {
  sign?: (message: Uint8Array, privateSeed: Uint8Array) => Promise<Uint8Array> | Uint8Array;
  verify?: (message: Uint8Array, signature: Uint8Array, publicKey: Uint8Array) => Promise<boolean> | boolean;
  publicKeyFromSeed?: (privateSeed: Uint8Array) => Promise<Uint8Array> | Uint8Array;
}): void;
export declare function bytesToBase64Url(input: Uint8Array): string;
export declare function base64UrlToBytes(text: string): Uint8Array;

// --- Records --------------------------------------------------------------

export declare const THREAD_STATE: Readonly<{
  SCHEDULED: 1; EN_ROUTE: 2; DWELLING: 3; COMPLETED: 4; CANCELED: 5; OFF_PLAN: 6;
}>;
/** Not a bandwidth setting: it decides what a leaked capability is worth. */
export declare const THREAD_MODE: Readonly<{ COARSE: 1; FINE: 2 }>;
/** How the vehicle moves. Not §11's `mode`, which is what the link reveals. */
export declare const THREAD_TRAVEL_MODE: Readonly<{
  UNSPECIFIED: 0; CAR: 1; BIKE: 2; FOOT: 3;
}>;
/** Driver-asserted, never inferred from dwelling. */
export declare const STOP_OUTCOME: Readonly<{
  PENDING: 0; DELIVERED: 1; SKIPPED: 2; FAILED: 3;
}>;
export declare const STOP_REASON: Readonly<{
  NONE: 0; CUSTOMER_ABSENT: 1; REFUSED: 2; INACCESSIBLE: 3; PARCEL_MISSING: 4; OTHER: 5;
}>;
export declare const THREAD_MAGIC: Readonly<Record<string, string>>;
export declare const THREAD_MAX_RECORD_BYTES: 256;
/** PMT1 framing around a sealed body: magic, epoch, tag, seq, ctLen, AEAD tag. */
export declare const THREAD_RECORD_OVERHEAD: 43;
/** What the plaintext body may spend, signature included. */
export declare const THREAD_MAX_BODY_BYTES: 213;
export declare const THREAD_MAX_NOTE_BYTES: 64;
/** SHA-256 of the sealed proof-of-delivery blob, on the mark (§20.7). */
export declare const THREAD_PHOTO_HASH_BYTES: 32;
/** The largest sealed photo this channel carries (§20.7). */
export declare const THREAD_MAX_PHOTO_BYTES: 131072;
/** A photo seal's own bytes: a 12-byte IV plus the 16-byte GCM tag. */
export declare const PHOTO_SEAL_OVERHEAD: 28;
export declare const THREAD_REQUEST_SIZES: readonly number[];
export declare const LINK_BYTES: 45;
/** A v1 link: the key it carries signs the run's own records (§4.1). */
export declare const LINK_VERSION_SELF: 1;
/**
 * A v2 link: the key it carries is a route **root** and records are
 * signed by a certified, short-lived day key (§21). A holder knows this
 * from the artifact rather than from what turns up on the topic —
 * guessing per record would make a downgrade one forged record away.
 */
export declare const LINK_VERSION_DELEGATED: 2;
/** The highest link version this build understands. */
export declare const LINK_VERSION: 2;
export declare const LINK_VERSIONS: readonly [1, 2];
/** §21 PMTC day certificate: version byte and total size. */
export declare const DAY_CERT_VERSION: 1;
export declare const DAY_CERT_BYTES: 145;

/** The most recent explicit mark, so a follower renders it without diffing. */
export interface StopMark {
  /** 1-based, into the run plan. */
  stopIndex: number;
  outcome: 1 | 2 | 3;
  reasonCode: number;
  /**
   * §20.7: lowercase hex SHA-256 of the **sealed** proof-of-delivery
   * blob, or null. A commitment, never content — the bytes are fetched
   * on demand with `fetchPhoto` and opened with `openPhoto`, which needs
   * the run seed. A holder of the 45-byte link has the commitment and
   * structurally cannot have the image.
   *
   * Hex rather than bytes because every consumer treats it as a content
   * address: a map key, a DOM attribute, an equality test. The encoder
   * accepts either, so a decoded body re-encodes unchanged.
   */
  photoHash?: string | Uint8Array | null;
}

/** What `markStop` accepts beyond the stop and the outcome. */
export interface StopMarkOptions {
  reason?: number;
  note?: Uint8Array | string | null;
  /**
   * §20.7 proof of delivery: **already-compressed** image bytes, sealed
   * under a key derived from the run seed and committed to in the signed
   * record. The image never rides gossip.
   *
   * The host owns recompression and, above all, **EXIF removal** — a
   * phone camera's JPEG carries a GPS fix, and passing one through
   * publishes a customer's front door past every §11 control. This
   * library has no image decoder and does not strip anything. Re-encode
   * through a canvas (or the platform equivalent) first.
   *
   * Refused when the sealed size would exceed THREAD_MAX_PHOTO_BYTES.
   */
  photo?: Uint8Array | null;
  /**
   * A coarse run withholds the vehicle's position (§11), and a doorstep
   * photo hands it over with a house number on it — so a photo on a
   * coarse run is refused unless this is true. Default false.
   */
  allowCoarsePhoto?: boolean;
  nowMillis?: number;
}

export interface ThreadBodyFields {
  unixSeconds: number;
  state: number;
  mode: number;
  travelMode: number;
  leafCell: number;
  geomRef: number;
  ratioQ12: number;
  speedBin: number;
  stopIndex: number;
  planRef: Uint8Array;
  /**
   * Two bits per plan stop, `outcomes[i]` being stop `i + 1`. Cumulative
   * in every record: the gossip channel is lossy and catch-up reaches
   * back only THREAD_MAX_AGE, so a record that carried only the delta
   * would leave a late joiner permanently blind to earlier marks.
   */
  outcomes?: number[];
  lastOutcome?: StopMark | null;
  note?: Uint8Array;
}

export interface ThreadBody extends ThreadBodyFields {
  magic: "PMTP";
  note: Uint8Array;
  outcomes: number[];
  lastOutcome: StopMark | null;
  signature: Uint8Array;
  preimage: Uint8Array;
  /** Null when the position was withheld (§11). */
  segment: string | null;
  ratio: number;
}

export interface ThreadRecord {
  magic: "PMT1";
  epochPrefix8: Uint8Array;
  tag: Uint8Array;
  seq: number;
  ciphertext: Uint8Array;
  aad: Uint8Array;
  bytes: Uint8Array;
}

export interface ThreadLink {
  version: number;
  /** The capability `P` — on a v2 link, the route **root** (§21). */
  publicKey: Uint8Array;
  epochPrefix8: Uint8Array;
  notAfter: number;
  /** True on a v2 link: records are signed by a certified day key. */
  delegated: boolean;
}

/**
 * §21 PMTC — a day certificate: the root's signed statement that this
 * day key may publish this route between these two instants.
 *
 * Sealed like any other body and carried on the run's own topic, so
 * §5.5 catch-up delivers it to a late joiner with no second protocol.
 */
export interface DayCertificate {
  magic: "PMTC";
  version: number;
  /** The route identity. A subscriber checks this against its link. */
  rootPublicKey: Uint8Array;
  /** The delegated signing key. Records verify against this, not the root. */
  dayPublicKey: Uint8Array;
  /** The operator's civil date for the run, as the integer YYYYMMDD. */
  serviceDay: number;
  notBefore: number;
  notAfter: number;
  signature: Uint8Array;
  preimage: Uint8Array;
  bytes: Uint8Array;
}

export declare function encodeDayCertificatePreimage(certificate: {
  version?: number;
  rootPublicKey: Uint8Array;
  dayPublicKey: Uint8Array;
  serviceDay: number;
  notBefore: number;
  notAfter: number;
}): Uint8Array;
export declare function encodeDayCertificate(
  certificate: Parameters<typeof encodeDayCertificatePreimage>[0], signature: Uint8Array
): Uint8Array;
export declare function decodeDayCertificate(bytes: Uint8Array): DayCertificate;
/**
 * The four magic characters an opened body starts with, or null. One
 * topic now carries two body shapes, so a subscriber peeks before it
 * commits to a decoder.
 */
export declare function threadBodyMagic(bytes: Uint8Array | null | undefined): string | null;

export declare function encodeThreadBodyPreimage(body: ThreadBodyFields): Uint8Array;
export declare function encodeThreadBody(body: ThreadBodyFields, signature: Uint8Array): Uint8Array;
export declare function decodeThreadBody(bytes: Uint8Array): ThreadBody;
export declare function threadRecordAad(epochPrefix8: Uint8Array, tag: Uint8Array, seq: number): Uint8Array;
export declare function encodeThreadRecord(fields: {
  epochPrefix8: Uint8Array; tag: Uint8Array; seq: number; ciphertext: Uint8Array;
}): { bytes: Uint8Array; aad: Uint8Array };
export declare function decodeThreadRecord(bytes: Uint8Array): ThreadRecord;
/**
 * §5.2 words the sentinel as `leafCell = 0`, which collides with the real
 * leaf 0; the whole triple being zero is the wire-compatible reading.
 */
export declare function isWithheldPosition(leafCell: number, geomRef: number, ratioQ12: number): boolean;

export declare function encodeThreadRequest(request: {
  epochPrefix8: Uint8Array; entries: Array<{ tag: Uint8Array; sinceSeq: number }>;
}): Uint8Array;
export declare function decodeThreadRequest(bytes: Uint8Array): {
  magic: "PMR1"; epochPrefix8: Uint8Array; entries: Array<{ tag: Uint8Array; sinceSeq: number }>;
};
export declare function encodeThreadResponse(response: {
  epochPrefix8: Uint8Array; entries: Array<{ tag: Uint8Array; records: Array<Uint8Array> }>;
}): Uint8Array;
export declare function decodeThreadResponse(bytes: Uint8Array): {
  magic: "PMM1"; epochPrefix8: Uint8Array; entries: Array<{ tag: Uint8Array; records: ThreadRecord[] }>;
};

/** PMTF (§20.7): magic and a 32-byte commitment, and nothing else. */
export declare function encodePhotoRequest(request: { hash: string | Uint8Array }): Uint8Array;
export declare function decodePhotoRequest(bytes: Uint8Array): { magic: "PMTF"; hash: string };
/** PMTB (§20.7). A zero length means "not held", as PMM1's count 0 does. */
export declare function encodePhotoResponse(response?: { sealed?: Uint8Array | null }): Uint8Array;
export declare function decodePhotoResponse(bytes: Uint8Array): {
  magic: "PMTB"; sealed: Uint8Array | null;
};

/**
 * `version` says what the key in field 2 **is**, not how new the writer
 * is — which is why the default is still 1. A one-off run's key really
 * does sign its own records, and a v2 link over it would leave every
 * subscriber waiting for a certificate that never comes.
 */
export declare function encodeThreadLink(fields: {
  publicKey: Uint8Array; epochPrefix8: Uint8Array; notAfter: number; version?: 1 | 2;
}): Uint8Array;
export declare function decodeThreadLink(bytes: Uint8Array): ThreadLink;
/**
 * The capability belongs in a fragment: browsers never transmit it.
 * `bootstrap` is the optional §20.10 hint and lands in the **query**, in
 * front of the `#` — the fragment is byte-identical either way.
 */
export declare function threadLinkUrl(
  baseUrl: string,
  link: Uint8Array,
  options?: { bootstrap?: string | string[] | null }
): string;
export declare function parseThreadLinkUrl(url: string): ThreadLink;

// --- §20.10 bootstrap hints ------------------------------------------------

/** A multiaddr with a `/p2p/<peerId>` suffix fits comfortably. */
export declare const THREAD_MAX_BOOTSTRAP_BYTES: 96;
/** How many a signed artifact may carry. */
export declare const THREAD_MAX_BOOTSTRAP_ADDRESSES: 3;
/** How many a URL may hint at — lower, because a link gets typed and texted. */
export declare const THREAD_MAX_URL_BOOTSTRAP: 2;
export declare const BOOTSTRAP_QUERY_KEY: "b";

/**
 * Validates and normalizes bootstrap addresses, refusing rather than
 * truncating. The only syntactic check is a leading `/`: this repo does
 * not parse multiaddrs, because the parser is an optional peer
 * dependency and a ticket must decode on a host that never dials.
 */
export declare function normalizeBootstrapAddresses(
  value: string | string[] | null | undefined,
  options?: { max?: number; what?: string }
): string[];

/** Adds `?b=…` in front of any fragment. Never touches the fragment. */
export declare function withBootstrapHint(baseUrl: string, bootstrap: string | string[] | null): string;

/**
 * Reads a hint back off an incoming URL, **leniently**: a malformed,
 * oversized or over-long hint yields fewer addresses and never throws.
 * The capability is in the fragment and must keep working when the query
 * is garbage.
 */
export declare function parseBootstrapHint(url: string | null | undefined): string[];

// --- Publisher, subscriber, ETA -------------------------------------------

export interface ThreadConstants {
  THREAD_UPDATE_FINE: number;
  THREAD_COARSE_HEARTBEAT: number;
  THREAD_MAX_RECORD_BYTES: number;
  THREAD_MAX_AGE: number;
  THREAD_MAX_FUTURE_SKEW: number;
  THREAD_STALE: number;
  THREAD_CACHE_TTL: number;
  THREAD_CACHE_RING: number;
  THREAD_CACHE_TAGS: number;
  THREAD_PROVIDE_INTERVAL: number;
  THREAD_POLL_INTERVAL: number;
  THREAD_MAX_RUN_SECONDS: number;
  THREAD_TAG_BUDGET: number;
  THREAD_CACHE_RATE: number;
  THREAD_STOP_RADIUS: number;
  THREAD_STOP_LINGER: number;
  /** §21: how often a route-day publisher re-emits its certificate. */
  THREAD_CERT_INTERVAL: number;
  /** §21: how many records a subscriber holds awaiting the day's certificate. */
  THREAD_HELD_RECORDS: number;
  /**
   * §21, optional: a host may **tighten** the 48 h certificate lifetime
   * bound. It cannot loosen it — the subscriber takes the minimum of this
   * and `THREAD_MAX_CERT_SECONDS`.
   */
  THREAD_MAX_CERT_SECONDS?: number;
  /** §21, optional: clock tolerance on a certificate's window. Default 300 s. */
  THREAD_CERT_SKEW?: number;
}
export declare const THREAD_CONSTANTS: Readonly<ThreadConstants>;

export interface RunStop {
  index: number;
  lat: number;
  lon: number;
  plannedUnixSeconds?: number;
}

export interface RunPlan {
  planRef?: Uint8Array;
  stops: RunStop[];
  dwellSeconds?: number;
  /** The issuer's decision, carried in PMP1 and published in every body. */
  travelMode?: number;
}

export interface ThreadEmission {
  bytes: Uint8Array;
  tag: Uint8Array;
  seq: number;
  window: number;
  body: ThreadBodyFields;
}

export declare function createThreadPublisher(options: {
  /**
   * A one-off run's Ed25519 seed (§4.1). Mutually exclusive with
   * `daySeed`/`certificate`: passing it alongside a certificate is
   * **refused**, because the route root has no business on a driver's
   * device — a phone that held it would cost the whole term.
   */
  privateSeed?: Uint8Array | null;
  /** §21: this service day's delegated signing seed. Needs `certificate`. */
  daySeed?: Uint8Array | null;
  /**
   * §21: the root's signed statement that `daySeed` may publish this
   * route today. Its `rootPublicKey` becomes the run's identity, so the
   * topic tag, the content key and the link stay where they were.
   */
  certificate?: DayCertificate | Uint8Array | null;
  epoch32: Uint8Array;
  mode?: number;
  plan?: RunPlan | null;
  publish?: ((emitted: ThreadEmission) => unknown) | null;
  constants?: Readonly<ThreadConstants>;
  clock?: () => number;
  snap?: ((point: { lat: number; lon: number }) => Promise<unknown>) | null;
  /** Resume point for a §20 ticket handover; the first record is startSeq + 1. */
  startSeq?: number;
  /**
   * How long this run may keep publishing, in seconds. Defaults to
   * `THREAD_MAX_RUN_SECONDS` (6 h), which is sized for a self-started
   * run; a ticket-born run is bounded by its own `notAfter` instead,
   * clamped to 24 h (§20.6).
   */
  maxRunSeconds?: number;
  /** Defaults to the plan's, then to 0. */
  travelMode?: number | null;
}): Promise<{
  /** The run's **identity**: on a route day the root, never the day key. */
  publicKey: Uint8Array;
  keys: ThreadKeys;
  /** §21: the certificate this run publishes under, or null on a one-off run. */
  certificate: DayCertificate | null;
  /** The key that actually signs, or null when `publicKey` does. */
  dayPublicKey: Uint8Array | null;
  /** Push the certificate now, outside the cadence. Null on a one-off run. */
  emitCertificate(nowMillis?: number): Promise<ThreadEmission | null>;
  /** `contributeTraffic` is §10 rule 4: false while serving a stop. */
  handleFix(fix: {
    lat?: number; lon?: number; speedMps?: number; nowMillis?: number;
    match?: { segment: string; ratio?: number } | null; note?: Uint8Array | string | null;
  }): Promise<{ published: boolean; reason?: string; record?: ThreadEmission; contributeTraffic: boolean }>;
  finish(options?: { nowMillis?: number; canceled?: boolean; note?: Uint8Array | string | null }): Promise<ThreadEmission>;
  announce(note: Uint8Array | string, options?: { nowMillis?: number }): Promise<ThreadEmission>;
  /**
   * What happened at a stop, asserted by the driver. The only writer of
   * the outcome map — dwelling near a door says the van stopped, which is
   * not the claim "delivered". Emits immediately, bypassing the cadence;
   * re-marking overwrites. `stopIndex` advances only over the contiguous
   * run of resolved stops in front of it, so pre-marking a far stop
   * records its outcome without claiming the vehicle got there.
   */
  markStop(index: number, outcome: 1 | 2 | 3, options?: StopMarkOptions): Promise<ThreadEmission>;
  outcomes(): number[];
  lastOutcome(): StopMark | null;
  /** Sealed proof-of-delivery blobs this run committed to, by commitment hex. */
  photoStore: Map<string, Uint8Array>;
  photoFor(hash: string | Uint8Array): Uint8Array | null;
  stats: {
    published: number; suppressedTraffic: number; stopEvents: number; marked: number;
    certificates: number;
  };
  readonly seq: number;
  readonly state: number;
  readonly stopIndex: number;
  readonly mode: number;
  readonly travelMode: number;
}>;

/** Stable codes for the failures a host branches on, alongside the prose. */
export declare const THREAD_DROP: Readonly<{
  BAD_SIGNATURE: "bad-signature";
  AWAITING_CERTIFICATE: "awaiting-certificate";
  CERT_ON_V1_LINK: "cert-on-self-signed-link";
  CERT_AFTER_LINK_EXPIRY: "cert-after-link-expiry";
}>;

export type ThreadUpdate = ThreadBody & { seq: number; receivedAt: number };

export type ThreadAcceptResult =
  | { ok: true; update: ThreadUpdate; certificate?: undefined; released?: undefined }
  /**
   * §21: a day certificate was accepted. It carries no update of its
   * own, but it may release records that were held waiting for it —
   * those come back in `released`, in `seq` order.
   */
  | { ok: true; update?: undefined; certificate: DayCertificate; released: ThreadUpdate[] }
  /**
   * `code` is null for the failures that predate §21. The one worth
   * branching on is `awaiting-certificate`: it means "no certified day
   * key covers this signature *yet*", which is a joiner who has not
   * heard today's certificate — **not** a forgery, and not reported as
   * one.
   */
  | { ok: false; step: number; reason: string; code: string | null };

/** §12's four rows, so a UI cannot accidentally upgrade its claim. */
export interface ThreadStatus {
  row: "thread+traffic" | "thread-only" | "traffic-only" | "neither";
  live: boolean;
  hasPosition: boolean;
  ageSeconds: number | null;
  state: number;
  stopIndex: number;
  /**
   * Cumulative, so the newest update carries the whole day. Outcomes are
   * deliberately *not* part of the four rows: a stop marked delivered
   * stays delivered whether or not this device still sees a position.
   */
  outcomes: number[];
  lastOutcome: StopMark | null;
  travelMode: number;
  claim: string;
}

export declare function createThreadSubscriber(options: {
  link: ThreadLink;
  epoch32: Uint8Array;
  constants?: Readonly<ThreadConstants>;
  clock?: () => number;
  cellContext?: ((leafCell: number) => { polylineCount: number } | null) | null;
}): Promise<{
  keys: ThreadKeys;
  accept(payload: Uint8Array | ThreadRecord, options?: { nowMillis?: number; knownTags?: Uint8Array[] | null }): Promise<ThreadAcceptResult>;
  currentTags(nowMillis?: number): Promise<Uint8Array[]>;
  topics(nowMillis?: number): Promise<string[]>;
  rendezvousKeys(nowMillis?: number): Promise<Uint8Array[]>;
  status(options?: { nowMillis?: number; hasTraffic?: boolean }): ThreadStatus;
  expired(nowMillis?: number): boolean;
  stats: {
    accepted: number; dropped: number; dropsByStep: Record<string, number>; forgeries: number;
    /** §21 counters: certificates accepted, and the held-record buffer. */
    certificates: number; held: number; heldEvicted: number; heldReleased: number;
  };
  latest(): ThreadUpdate | null;
  history(): ThreadUpdate[];
  /** §21: true when this link delegates signing to certified day keys. */
  delegated: boolean;
  /** The live day certificates this subscriber holds. */
  certificates(): DayCertificate[];
  /** Whether anything currently held can authorise a record. */
  certified(): boolean;
  /** How many records are waiting on the day's certificate. */
  readonly heldCount: number;
  /**
   * A high-water mark across the whole thread, which is what a §20.5
   * handover resumes above. Step 7's *ledger* is scoped per signing
   * authority (§21) — a new day key starts its own count, because
   * nothing coordinates two service days across an overnight gap.
   */
  readonly highestSeq: number;
}>;

export type RoutingProfileName = "car" | "bike" | "foot";

export interface RouteProfileFacts {
  /** The graph the estimate was actually computed on, or null if unnamed. */
  profile: string | null;
  /**
   * "matched" — the run's travel mode and the graph agree.
   * "mismatched" — they are known and differ; the number is a car's.
   * "unstated" — the run or the host did not say, so nothing is claimed.
   */
  profileBasis: "matched" | "mismatched" | "unstated";
  travelModeName: RoutingProfileName | null;
  travelModeMismatch: boolean;
}

export interface ArrivalEstimate extends RouteProfileFacts {
  arrivalMillis: number;
  secondsFromObservation: number;
  secondsFromNow: number;
  stopsAway: number;
  outcome: 0;
  outcomeName: "pending";
  reasonCode: null;
  basis: "live-traffic" | "static-metric";
  positionBasis: "reported-position" | "last-stop";
  observationAgeSeconds: number;
  stale: boolean;
}

/**
 * A stop the driver marked skipped or failed. No `arrivalMillis` and no
 * `secondsFromNow` exist on this shape at all: the vehicle is not coming,
 * and there must be no field a caller can accidentally render a time out
 * of. `reasonCode` is null when the wire's single `lastOutcome` belongs
 * to some other stop — the reason is genuinely unknown, not inferrable.
 */
export interface MarkedStopResult extends RouteProfileFacts {
  arrivalMillis: null;
  secondsFromObservation: null;
  secondsFromNow: null;
  stopsAway: null;
  outcome: 2 | 3;
  outcomeName: "skipped" | "failed";
  reasonCode: number | null;
  basis: "marked";
  positionBasis: "none";
  observationAgeSeconds: number;
  stale: false;
}

/**
 * §9 — the whole reason threads live in rangefind. Uses `matrix()` over
 * the planned order, never `itinerary()`, which reorders stops and would
 * silently produce optimistic ETAs.
 *
 * Stops marked skipped or failed leave the remaining chain: left in, one
 * skipped delivery inflates every downstream customer's ETA by a leg and
 * a dwell for the rest of the day.
 */
export declare function estimateArrival(options: {
  engine?: { locate(segment: string, ratio: number): Promise<{ lat: number; lon: number }>; matrix(params: unknown): Promise<{ seconds: number[][] }> };
  /** Several profiles, when the host holds them. The run's mode picks one. */
  engines?: Partial<Record<RoutingProfileName, unknown>> | null;
  /** What the single `engine` is. Without it the result says "unstated". */
  engineMode?: RoutingProfileName | null;
  update: ThreadBody | null;
  plan: RunPlan;
  myStopIndex: number;
  live?: unknown;
  constants?: Readonly<ThreadConstants>;
  nowMillis?: number;
}): Promise<ArrivalEstimate | MarkedStopResult | null>;

/** The name of a wire travel mode, or null when the run did not say. */
export declare function travelModeName(travelMode: number): RoutingProfileName | null;

export declare function scheduledArrival(options: { plan: RunPlan; myStopIndex: number }):
  { arrivalMillis: number; basis: "schedule"; positionBasis: "none"; stale: false } | null;

// --- Dispatch tickets (§20) ------------------------------------------------

export declare const TICKET_MAGIC: Readonly<{ PMP1: "PMP1"; PMK1: "PMK1"; PMJ1: "PMJ1" }>;
export declare const TICKET_VERSION: 1;
export declare const PLAN_VERSION: 1;
export declare const JOB_OFFER_VERSION: 1;
/** flags bit0: the run seed is present. A seedless PMK1 is not an offer (§20.4). */
export declare const TICKET_FLAG_SEED: 1;
/** flags bit1: the issuer named bootstrap addresses (§20.10). */
export declare const TICKET_FLAG_BOOTSTRAP: 2;
/**
 * flags bit2: this ticket hands over one service day of a recurring
 * route (§21.11). The field it adds is the PMTC day certificate, and
 * `privateSeed` is then the **day** seed rather than a run seed.
 */
export declare const TICKET_FLAG_DAY: 4;
/**
 * Why a route-day ticket was refused, alongside the `CERT_ERROR` codes
 * the embedded certificate can fail with (§21.11).
 */
export declare const TICKET_ERROR: Readonly<{
  DAY_SEED_MISMATCH: "ticket-day-seed-mismatch";
  DAY_FOREIGN_ROOT: "ticket-day-foreign-root";
  DAY_AFTER_TICKET_EXPIRY: "cert-after-link-expiry";
}>;
export declare const THREAD_MAX_LABEL_BYTES: 48;
/** §20.8 per-stop delivery metadata caps, in UTF-8 bytes. */
export declare const THREAD_MAX_ORDER_REF_BYTES: 24;
export declare const THREAD_MAX_INSTRUCTIONS_BYTES: 64;
export declare const THREAD_MAX_CONTACT_BYTES: 24;
/** Which optional per-stop fields a stop's flags byte says are present. */
export declare const THREAD_STOP_FIELD: {
  readonly ORDER_REF: 1;
  readonly PARCELS: 2;
  readonly INSTRUCTIONS: 4;
  readonly CONTACT: 8;
};

/**
 * §20.8 delivery metadata. **`null` means unstated throughout** — for
 * `parcels` that is a different claim from `0`, which means "nothing to
 * carry, and the dispatcher said so"; a UI must not render one as the
 * other. All of it is driver-side: the plan never goes on the wire, only
 * its 8-byte `planRef`.
 */
export interface ThreadStopDetails {
  /** ≤ 24 bytes UTF-8. */
  orderRef: string | null;
  /** 0..65535, or null when the dispatcher gave no count. */
  parcels: number | null;
  /** ≤ 64 bytes UTF-8. */
  instructions: string | null;
  /** ≤ 24 bytes UTF-8. **Personal data** — see §20.8. */
  contact: string | null;
}

export interface ThreadPlanStop extends RunStop, ThreadStopDetails {
  /** E7 integers are what went on the wire; the degrees are derived. */
  latE7: number;
  lonE7: number;
  plannedUnixSeconds: number | null;
  label: string;
}

export interface DecodedThreadPlan {
  version: number;
  travelMode: number;
  stops: ThreadPlanStop[];
  dwellSeconds: number;
  planRef: Uint8Array;
  bytes: Uint8Array;
}

/** PMP1. Coordinates travel as E7 integers — `planRef` is a hash of these bytes. */
export declare function encodeThreadPlan(plan: {
  stops: Array<Partial<RunStop> & Partial<ThreadStopDetails> & {
    latE7?: number;
    lonE7?: number;
    label?: string;
  }>;
  dwellSeconds?: number;
  /** 0 unspecified, 1 car, 2 bike, 3 foot. Part of `planRef`, so part of the job. */
  travelMode?: number;
}): Uint8Array;
export declare function decodeThreadPlan(bytes: Uint8Array): DecodedThreadPlan;
/** The 8-byte `planRef` every PMT1 body carries (§5.2). */
export declare function planRefOf(planBytes: Uint8Array): Uint8Array;

/** PMK1, decoded. `privateSeed` is null on an offer. */
export interface DispatchTicket {
  version: number;
  seedPresent: boolean;
  mode: number;
  epochPrefix8: Uint8Array;
  notAfter: number;
  issuerPublicKey: Uint8Array;
  planBytes: Uint8Array;
  plan: DecodedThreadPlan;
  privateSeed: Uint8Array | null;
  /**
   * §20.10. The peers the issuer says this device may dial to reach the
   * mesh; empty when it named none. Inside the signed preimage, because
   * a bootstrap address is a claim the dispatcher is making.
   */
  bootstrap: string[];
  /**
   * §21.11. Null on an ordinary job. When present this ticket hands over
   * one service day of a recurring route: identity comes from the
   * certificate's `rootPublicKey`, authority from `privateSeed` (which is
   * then the **day** seed), and the certificate itself is what the run
   * publishes alongside its records.
   */
  dayCertificate: DayCertificate | null;
  routeDay: boolean;
  /** The certificate's `YYYYMMDD`, or null on an ordinary job. */
  serviceDay: number | null;
  signature: Uint8Array;
  /** Magic through the day certificate — exactly what the issuer signed. */
  preimage: Uint8Array;
  bytes: Uint8Array;
}

export interface IssuedTicket {
  bytes: Uint8Array;
  base64url: string;
  ticket: DispatchTicket;
  /** The follow link, derivable before any driver exists. Null when seedless. */
  link: Uint8Array | null;
  url: string | null;
  jobIdHex: string;
}

/**
 * Issues a ticket. `seedless` produces a runless PMK1, which is **not**
 * an offer — offers are PMJ1 (§20.4) and carry no plan at all.
 *
 * **The bytes this returns MUST NOT leave the process unsealed** (§20.9):
 * a plaintext PMK1 is a publish capability *and* a customer list. Hosts
 * call `issueSealedTicket`; this exists to make the artifact that gets
 * sealed.
 */
export declare function issueTicket(options: {
  issuerSeed: Uint8Array;
  epoch32?: Uint8Array | null;
  epochPrefix8?: Uint8Array | null;
  plan?: unknown;
  planBytes?: Uint8Array | null;
  notAfter: number;
  mode?: number;
  privateSeed?: Uint8Array | null;
  seedless?: boolean;
  /**
   * §20.10: up to 3 multiaddrs, each ≤96 bytes. Signed and — through
   * `issueSealedTicket` — sealed, so it reaches the awarded device only.
   * It is deliberately **not** placed in `url`, the public follow link.
   */
  bootstrap?: string | string[] | null;
  /**
   * §21.11: makes this a **route-day ticket**. `issuerSeed` is then the
   * route root seed — only the root can mint this certificate or derive
   * the day seed — and `privateSeed` is the day seed, re-derived here
   * when it is not passed. `link` comes back as the term's v2 link over
   * the root, never a link over the day key.
   */
  dayCertificate?: DayCertificate | Uint8Array | null;
  baseUrl?: string | null;
}): Promise<IssuedTicket>;

/** What `issueSealedTicket` hands back. There is no plaintext ticket field. */
export interface SealedIssuedTicket {
  /** PME1 bytes — the only form that may leave the process. */
  sealed: Uint8Array;
  base64url: string;
  /** `wayfind://ticket#<base64url>`, ciphertext in the fragment. */
  driverUrl: string;
  /** The decoded artifact, in memory: the dispatcher keeps the run seed. */
  ticket: DispatchTicket;
  /** The customer's 45-byte follow link — a read capability, not sealed. */
  link: Uint8Array | null;
  url: string | null;
  jobIdHex: string;
  /** What the awarded device may dial (§20.10); empty when none was named. */
  bootstrap: string[];
  recipientCount: number;
}

/**
 * Mints a ticket and seals it to enrolled devices in one step — the only
 * ticket-minting entry point a host should call (§20.9). `recipients`
 * are X25519 device public keys, decoded PMV1 cards, or anything with a
 * `publicKey`. Enrolment is the gate on transfer: there is no argument
 * meaning "seal to whoever ends up holding this".
 */
export declare function issueSealedTicket(options: {
  issuerSeed: Uint8Array;
  epoch32?: Uint8Array | null;
  epochPrefix8?: Uint8Array | null;
  plan?: unknown;
  planBytes?: Uint8Array | null;
  notAfter: number;
  mode?: number;
  privateSeed?: Uint8Array | null;
  /** §20.10 bootstrap addresses, signed inside the sealed ticket. */
  bootstrap?: string | string[] | null;
  /** §21.11: one service day of a recurring route. See `issueTicket`. */
  dayCertificate?: DayCertificate | Uint8Array | null;
  baseUrl?: string | null;
  recipients: Array<Uint8Array | string | { publicKey: Uint8Array }>;
}): Promise<SealedIssuedTicket>;

export declare function decodeThreadTicket(bytesOrBase64url: Uint8Array | string): DispatchTicket;

/**
 * What an incoming capability *is*, decided by the magic rather than by
 * whether it decodes — so a ticket minted before a wire change is
 * reported as an unreadable ticket rather than as a malformed link.
 * `reason` is null when the artifact decodes.
 */
export declare function classifyThreadArtifact(
  fragmentOrBytes: Uint8Array | string | null | undefined
): {
  kind: "ticket" | "offer" | "link" | "seed" | null;
  reason: string | null;
  sealed: boolean;
  /**
   * §21.11. True for a route-day ticket, false for an ordinary one, and
   * **null** whenever the artifact is a ticket this function could not
   * read — a sealed PME1 hides which kind it is by design.
   */
  routeDay: boolean | null;
  serviceDay: number | null;
};

/**
 * Signature, expiry, epoch, and — when a host pins one — the issuer. A
 * route-day ticket (§21.11) is checked twice over: the embedded
 * certificate must stand up on its own, and the seed must be the day key
 * it vouches for. `code` carries the stable reason for those, from
 * `TICKET_ERROR` or `CERT_ERROR`.
 */
export declare function verifyThreadTicket(
  ticket: DispatchTicket | Uint8Array | string,
  options?: { epochPrefix8?: Uint8Array | null; expectedIssuerHex?: string | null; nowMillis?: number }
): Promise<{ ok: true } | { ok: false; reason: string; code?: string | null }>;

/**
 * The 45-byte follow link this ticket's run publishes under. Throws on a
 * seedless ticket — there is no run. On a route-day ticket it is the
 * route's **v2 link over the root** (§21.11), never a link over the day
 * key: deriving one from the day key would publish the route where no
 * subscriber is listening.
 */
export declare function ticketFollowLink(ticket: DispatchTicket | Uint8Array | string): Promise<Uint8Array>;

/**
 * 16 bytes identifying the job, not the artifact: it hashes neither the
 * seed nor the signature, so a PMJ1 offer and the ticket that fulfils it
 * share one id. Claims and awards are not in protocol v1; this is what
 * they would join on.
 */
export declare function jobIdOf(ticket: DispatchTicket | Uint8Array | string): Uint8Array;
export declare function jobIdHexOf(ticket: DispatchTicket | Uint8Array | string): string;

// --- §20.4 PMJ1 job offers -------------------------------------------------
//
// The one artifact on this channel that is **public by design**: it is
// broadcast to couriers nobody has enrolled, so it carries a commitment
// to the plan and coarse descriptors, and not one address.

/** Which optional fields an offer's flags byte says are present. */
export declare const OFFER_FIELD: { readonly PAY: 1; readonly LABEL: 2 };
export declare const THREAD_MAX_OFFER_LABEL_BYTES: 48;
/** The locality grid in E7 units: 0.01°, about 1.1 km. */
export declare const OFFER_GRID_E7: 100000;
/** Five buckets for how far a round ranges: 1 / 3 / 10 / 30 km, then over. */
export declare const OFFER_SPREAD: ReadonlyArray<
  Readonly<{ bucket: number; maxMeters: number | null; label: string }>
>;

export interface JobOffer {
  version: number;
  /** The §11 granularity the awarded courier would publish under. */
  mode: number;
  travelMode: number;
  epochPrefix8: Uint8Array;
  notAfter: number;
  /** The join key an award, a claim or a payment record would use. */
  jobId: Uint8Array;
  jobIdHex: string;
  /** The commitment: SHA-256 over the plan bytes, first 8. */
  planRef: Uint8Array;
  stopCount: number;
  /** The gridded centroid of the stops. Never a stop, never a doorstep. */
  centroid: {
    latCenti: number;
    lonCenti: number;
    latE7: number;
    lonE7: number;
    lat: number;
    lon: number;
    gridDegrees: number;
  };
  spread: number;
  spreadLabel: string;
  spreadMaxMeters: number | null;
  totalMeters: number;
  /** Minor units, or null when unstated. `0` is a statement, not silence. */
  payMinor: number | null;
  currency: string | null;
  /** A short line the dispatcher chose to make public, or null. */
  label: string | null;
  issuerPublicKey: Uint8Array;
  signature: Uint8Array;
  preimage: Uint8Array;
  bytes: Uint8Array;
}

export interface IssuedJobOffer {
  bytes: Uint8Array;
  base64url: string;
  offer: JobOffer;
  jobIdHex: string;
  /** `wayfind://offer#<base64url>` — safe to post in a group chat. */
  url: string;
}

/**
 * Mints the public advertisement of a job. Pass `ticket` to advertise a
 * job that already exists, or the plan and binding fields directly.
 * Nothing from the plan is copied in: the offer commits to it by hash.
 */
export declare function issueJobOffer(options: {
  issuerSeed: Uint8Array;
  ticket?: DispatchTicket | Uint8Array | string | null;
  epoch32?: Uint8Array | null;
  epochPrefix8?: Uint8Array | null;
  plan?: unknown;
  planBytes?: Uint8Array | null;
  notAfter?: number | null;
  mode?: number;
  /** The dispatcher's own routed distance — not in the plan. */
  totalMeters?: number;
  payMinor?: number | null;
  currency?: string | null;
  label?: string | null;
  baseUrl?: string | null;
}): Promise<IssuedJobOffer>;

export declare function jobOfferUrl(bytes: Uint8Array, baseUrl?: string): string;
export declare function decodeJobOffer(bytesOrBase64url: Uint8Array | string): JobOffer;

/** Signature, expiry, epoch, and — when a host pins one — the issuer. */
export declare function verifyJobOffer(
  offer: JobOffer | Uint8Array | string,
  options?: { epochPrefix8?: Uint8Array | null; expectedIssuerHex?: string | null; nowMillis?: number }
): Promise<{ ok: true } | { ok: false; reason: string }>;

/**
 * Is the job that was awarded the job that was advertised? Compares the
 * opened ticket against the offer's commitment — issuer, epoch,
 * `planRef`, `notAfter`, `jobId` — and names the check that failed.
 * Signatures are `verifyJobOffer`/`verifyThreadTicket`'s business; this
 * answers only the content question, so it holds even against a
 * dispatcher who re-signs its own forgery.
 */
export declare function awardMatchesOffer(
  offer: JobOffer | Uint8Array | string,
  ticket: DispatchTicket | Uint8Array | string
): { ok: true; reason: null } | { ok: false; reason: string };

// --- §21 recurring routes: identity split from authority --------------------
//
// A school bus keeps one plan for a term, the driver changes some days
// and not most, and a parent must be able to watch the same link all term
// without ever resubscribing. §4.1 makes one key the identity *and* the
// publish authority, so rotating the authority rotates every parent's
// link. This splits them: the **root** stays the identity — it derives
// the topic tag, the content key and the 45 bytes — and a short-lived,
// certified **day key** carries the authority.

/** The HKDF label for the day-seed schedule. */
export declare const ROUTE_DAY_INFO: "pulsemesh/route-day/1";
/**
 * The longest certificate window a **subscriber** honours: 48 h.
 *
 * A bound the verifier applies, not one the issuer is trusted to
 * respect. Without it a root mints a day key valid until 2038 and the
 * split buys nothing, because a lost driver phone is back to costing the
 * whole term. A host may tighten it through
 * `constants.THREAD_MAX_CERT_SECONDS`; nothing can loosen it.
 */
export declare const THREAD_MAX_CERT_SECONDS: 172800;
/**
 * Clock tolerance on a certificate's own window, both edges: 300 s.
 * Deliberately looser than `THREAD_MAX_FUTURE_SKEW`, which bounds how
 * fresh a *position* is — this bounds a window measured in hours, and
 * being mean has one failure mode: a parent whose phone clock drifted is
 * told the bus is not being tracked.
 */
export declare const THREAD_CERT_SKEW: 300;

export declare const CERT_ERROR: Readonly<{
  VERSION: "cert-version";
  FOREIGN_ROOT: "cert-foreign-root";
  EMPTY_WINDOW: "cert-empty-window";
  WINDOW_TOO_LONG: "cert-window-too-long";
  BAD_SIGNATURE: "cert-bad-signature";
  NOT_YET_VALID: "cert-not-yet-valid";
  EXPIRED: "cert-expired";
}>;

/**
 * A service day is the operator's **civil date for the run**, packed as
 * the integer `YYYYMMDD`, chosen in the route's own local timezone. A
 * 07:00 departure straddles UTC midnight in much of the world and a
 * 23:10–01:20 run straddles local midnight — so a night run keeps the
 * date it *departed* on, and one service day is one contiguous block of
 * work.
 *
 * Safe to leave to the operator because it is never compared against
 * anybody's clock: it is an HKDF input and a display label. The only
 * validity claim on the wire is `notBefore`/`notAfter` in absolute unix
 * seconds, which is unambiguous everywhere.
 */
export declare function assertServiceDay(serviceDay: number): number;
export declare function serviceDayOf(year: number, month: number, day: number): number;

/**
 * `HKDF(rootSeed, "pulsemesh/route-day/1" ‖ planRef ‖ serviceDay)`.
 *
 * **Depot-side.** Deterministic, so a depot with 40 routes and a 180-day
 * term re-derives instead of storing 7,200 secrets, and recovery is
 * re-derivation. One-way, so a day seed does not invert to the root and
 * Tuesday's says nothing about Wednesday's.
 */
export declare function deriveDaySeed(options: {
  rootSeed: Uint8Array; planRef?: Uint8Array | null; serviceDay: number;
}): Promise<Uint8Array>;

/**
 * Mints one day's authority. **Depot-side** — the only function here
 * that takes a root seed.
 *
 * What it returns is exactly what travels to the driver: a day seed and
 * the certificate that vouches for it, and deliberately nothing else.
 * There is no option meaning "and also the root".
 */
export declare function mintDayCertificate(options: {
  rootSeed: Uint8Array;
  planRef?: Uint8Array | null;
  serviceDay: number;
  notBefore: number;
  notAfter: number;
  maxSeconds?: number;
}): Promise<{
  daySeed: Uint8Array;
  certificate: DayCertificate;
  rootPublicKey: Uint8Array;
  dayPublicKey: Uint8Array;
}>;

/**
 * The subscriber's side of the chain. Refuses, by name: a certificate
 * for a different root, an over-long window, a signature that does not
 * verify against the root, and a window this clock is outside.
 */
export declare function verifyDayCertificate(
  certificate: DayCertificate | Uint8Array,
  options?: {
    rootPublicKey: Uint8Array;
    nowSeconds?: number;
    maxSeconds?: number;
    skew?: number;
  }
): Promise<
  | { ok: true; code: null; reason: null; certificate: DayCertificate }
  | { ok: false; code: string; reason: string }
>;

/**
 * The term-long follow link: a v2 link over the route **root**, minted
 * once by the depot. `notAfter` is the end of the term rather than the
 * end of a run — which is safe precisely because the root cannot publish
 * anything a subscriber accepts without a certificate whose own window
 * is at most 48 h.
 */
export declare function routeFollowLink(fields: {
  rootPublicKey: Uint8Array; epochPrefix8: Uint8Array; notAfter: number;
}): Uint8Array;

// --- §20.9 sealed tickets and device enrolment -----------------------------

export declare const SEAL_MAGIC: Readonly<{ PME1: "PME1"; PMV1: "PMV1" }>;
export declare const SEAL_VERSION: number;
export declare const DEVICE_CARD_VERSION: number;
export declare const THREAD_MAX_DEVICE_NAME_BYTES: number;
export declare const SEAL_MAX_RECIPIENTS: number;
/** hint ‖ nonce ‖ wrapped content key ‖ tag: what one more recipient costs. */
export declare const SEAL_RECIPIENT_BYTES: number;
/** magic ‖ version ‖ ephemeral key ‖ count ‖ body nonce ‖ tag. */
export declare const SEAL_FIXED_BYTES: number;
/** `error.code` on a failed open. */
export declare const SEAL_ERROR: Readonly<{
  NOT_ENROLLED: "sealed-for-another-device";
  UNREADABLE: "unreadable";
}>;

/**
 * A device's long-lived X25519 enrolment keypair. Not the run seed and
 * not the issuer seed: it never leaves the device and never signs.
 * Storing `privateKey` is the host's obligation — Keystore, Secure
 * Enclave, or an honest admission that a browser has neither.
 */
export declare function generateDeviceKeypair(
  seed?: Uint8Array | null
): Promise<{ privateKey: Uint8Array; publicKey: Uint8Array }>;

/** First 4 bytes of SHA-256(publicKey), hex. Compare it aloud before enrolling. */
export declare function fingerprint(publicKey: Uint8Array | string): string;

export interface DeviceCard {
  version: number;
  publicKey: Uint8Array;
  name: string;
  fingerprint: string;
  bytes: Uint8Array;
}

export declare function encodeDeviceCard(card: { publicKey: Uint8Array | string; name?: string }): Uint8Array;
export declare function decodeDeviceCard(bytesOrText: Uint8Array | string): DeviceCard;
/** `wayfind://device#<base64url>` — the fragment, as every capability here. */
export declare function deviceCardUrl(
  card: Uint8Array | { publicKey: Uint8Array | string; name?: string },
  baseUrl?: string
): string;
export declare function parseDeviceCardUrl(url: string): DeviceCard;

/** PME1 framing, without any key — enough to say what a blob is. */
export interface SealedTicketHeader {
  version: number;
  ephemeralPublicKey: Uint8Array;
  recipients: Array<{ hint: Uint8Array; nonce: Uint8Array; wrapped: Uint8Array }>;
  bodyNonce: Uint8Array;
  ciphertext: Uint8Array;
  bytes: Uint8Array;
}

/**
 * Seals signed PMK1 bytes to enrolled devices. One content key, one wrap
 * per recipient — a second recipient costs 64 bytes, not a second copy.
 */
export declare function sealTicket(
  ticketBytes: Uint8Array | string,
  recipientPublicKeys: Array<Uint8Array | string | { publicKey: Uint8Array }>,
  options?: { ephemeralPrivateKey?: Uint8Array | null }
): Promise<Uint8Array>;

/**
 * Returns the inner signed PMK1 bytes verbatim. Throws with
 * `error.code === SEAL_ERROR.NOT_ENROLLED` when no wrap opens with this
 * key, and `SEAL_ERROR.UNREADABLE` when the bytes are not a sealed
 * ticket — different situations needing different words for a driver.
 */
export declare function openSealedTicket(
  sealed: Uint8Array | string,
  devicePrivateKey: Uint8Array
): Promise<Uint8Array>;

export declare function isSealedTicket(bytes: unknown): boolean;
export declare function decodeSealedTicketHeader(bytesOrText: Uint8Array | string): SealedTicketHeader;
/** `wayfind://ticket#<base64url>`, now carrying ciphertext. */
export declare function sealedTicketUrl(
  sealed: Uint8Array | string,
  baseUrl?: string,
  options?: { bootstrap?: string | string[] | null }
): string;

// --- §20.10 PMH1 seed card -------------------------------------------------

/**
 * A **location**, not a capability: 1–3 multiaddrs and an optional
 * label. Holding one grants nothing — every thread on the far side still
 * needs its own key — which is why it is unsigned. The `/p2p/<peerId>`
 * in a multiaddr is what authenticates the far end, so a card naming the
 * wrong machine does not impersonate a seed; it fails to connect.
 */
export declare const SEED_MAGIC: { readonly PMH1: "PMH1" };
export declare const SEED_CARD_VERSION: 1;
export declare const THREAD_MAX_SEED_LABEL_BYTES: 32;
export declare const SEED_FIELD: { readonly LABEL: 1 };

export interface SeedCard {
  version: number;
  addresses: string[];
  /** "" when unlabelled. ≤32 bytes UTF-8, refused rather than truncated. */
  label: string;
  bytes: Uint8Array;
}

export declare function encodeSeedCard(card: { addresses: string[]; label?: string }): Uint8Array;
export declare function decodeSeedCard(bytesOrText: Uint8Array | string): SeedCard;
/** `wayfind://seed#<base64url>` — the uniform carrier every artifact uses. */
export declare function seedCardUrl(card: Uint8Array | { addresses: string[]; label?: string }, baseUrl?: string): string;
export declare function parseSeedCardUrl(url: string): SeedCard;
/** A public link with the card's first two addresses hinted in its query. */
export declare function seedHintUrl(baseUrl: string, card: SeedCard | Uint8Array | string): string;
/**
 * Is this an address another device could dial? Loopback and unspecified
 * addresses look fine in a terminal and reach nobody, so tooling that
 * prints a card filters with this.
 */
export declare function isDialableAddress(address: string): boolean;

/** True when this host's WebCrypto does X25519; false routes to the pure fallback. */
export declare function nativeX25519Available(): Promise<boolean>;
export declare function x25519PublicKey(privateKey: Uint8Array): Promise<Uint8Array>;
export declare function x25519SharedSecret(privateKey: Uint8Array, peerPublicKey: Uint8Array): Promise<Uint8Array>;

// --- Catch-up --------------------------------------------------------------

export declare function createThreadCache(options?: {
  constants?: Readonly<ThreadConstants>;
  clock?: () => number;
  rng?: () => number;
}): {
  admit(record: Uint8Array | ThreadRecord, options?: {
    fromPeer?: string | null; openable?: boolean; nowMillis?: number;
  }): { admitted: boolean; reason?: string };
  /** Unknown tags answer exactly like held-but-empty ones. */
  answer(request: Uint8Array | { entries: Array<{ tag: Uint8Array; sinceSeq: number }> }, options?: { nowMillis?: number }):
    { entries: Array<{ tag: Uint8Array; records: Uint8Array[] }> };
  sweep(nowMillis?: number): void;
  has(tag: Uint8Array): boolean;
  recordsFor(tag: Uint8Array): Uint8Array[];
  stats: { admitted: number; rejected: number; evictedTags: number; served: number; requests: number };
  readonly size: number;
  readonly tagHexes: string[];
};

/** Padding is free here: a tag is indistinguishable from random bytes. */
export declare function buildThreadRequest(params: {
  epochPrefix8: Uint8Array;
  wanted: Array<{ tag: Uint8Array; sinceSeq?: number }>;
  rng?: () => number;
}): { bytes: Uint8Array; entries: Array<{ tag: Uint8Array; sinceSeq: number }>; realCount: number };

export declare function encodeThreadCacheResponse(
  epochPrefix8: Uint8Array,
  answered: { entries: Array<{ tag: Uint8Array; records: Uint8Array[] }> }
): Uint8Array;

export declare function applyThreadResponse(
  subscriber: { accept(record: unknown, options?: { nowMillis?: number }): Promise<ThreadAcceptResult> },
  payload: Uint8Array | { entries: Array<{ tag: Uint8Array; records: ThreadRecord[] }> },
  options?: { nowMillis?: number; wantedTags?: Uint8Array[] | null }
): Promise<number>;

// --- Rules between the channels (§10) --------------------------------------

export declare const ROUTE_PUBLICITY: Readonly<{ PUBLISHED: "published"; UNPUBLISHED: "unpublished" }>;

/** Off by default, never inferred from vehicle class. */
export declare function resolveContributionPolicy(options?: {
  enabled?: boolean;
  publicity?: string | null;
}): { contribute: boolean; profile: "cadence" | "reticent" | null; reason: string };

export declare function createStopSuppressor(options?: {
  stops?: RunStop[];
  constants?: Readonly<ThreadConstants>;
  clock?: () => number;
}): {
  evaluate(fix: { lat?: number; lon?: number; speedMps?: number; dwelling?: boolean; nowMillis?: number }):
    { contribute: boolean; reason?: string };
  stats: { evaluated: number; suppressed: number; byReason: Record<string, number> };
};

/** §10 rule 1, enforced rather than documented. */
export declare function assertNeverBridged(threadUpdate: unknown): void;

/**
 * The thread channel wired to a transport: the gossip tap, the tag
 * rotation, and the routing of arriving payloads to the right follow.
 *
 * Needs no admission bond and works in read-only mode — thread records
 * are authenticated end to end by the thread key, so membership plays no
 * part in either direction.
 */
export declare function createThreadChannel(options: {
  node?: unknown;
  network?: unknown;
  engine?: unknown;
  /** Several routing profiles, when the host holds them (see estimateArrival). */
  engines?: Partial<Record<RoutingProfileName, unknown>> | null;
  /** What the single `engine` is, so an ETA can be honest about it. */
  engineMode?: RoutingProfileName | null;
  /** A libp2p host, for §4.2 DHT discovery. Optional. */
  host?: unknown;
  id?: string;
  epochHex?: string | null;
  clock?: () => number;
  rng?: () => number;
  /**
   * Cache sealed records for tags this peer cannot open, so a stranger's
   * late joiner can pull the gap from here. On by default: it is what
   * makes availability scale with the audience rather than against it.
   */
  relay?: boolean;
  constants?: unknown;
}): {
  /** Starts publishing a run; `url` is the whole capability. */
  publish(options?: {
    baseUrl?: string;
    mode?: number;
    plan?: unknown;
    ttlSeconds?: number;
    privateSeed?: Uint8Array | null;
    /** Overrides the plan's; 0 when neither says. */
    travelMode?: number | null;
    onPublish?: ((emitted: unknown) => unknown) | null;
  }): Promise<{
    publisher: unknown;
    link: Uint8Array;
    url: string | null;
    notAfter: number;
    /** §10 rule 4 rides on the answer: honour `contributeTraffic`. */
    handleFix(fix: unknown): Promise<{ published: boolean; contributeTraffic: boolean }>;
    announce(note: Uint8Array | string, options?: unknown): Promise<unknown>;
    /** Driver-asserted delivery outcome; emits immediately. */
    markStop(index: number, outcome: 1 | 2 | 3, options?: StopMarkOptions): Promise<unknown>;
    /** The sealed blob this run committed to, by commitment hex (§20.7). */
    photoFor(hash: string | Uint8Array): Uint8Array | null;
    outcomes(): number[];
    finish(options?: unknown): Promise<unknown>;
    readonly seq: number;
    readonly state: number;
    readonly stopIndex: number;
    readonly travelMode: number;
  }>;

  /**
   * Publishes a run from a dispatch ticket (§20). Refuses a bad
   * signature, a foreign epoch, an expired ticket, or an offer — an
   * offer is not a publish capability. Resumes above the previous
   * holder's last `seq`, so a mid-run handover is invisible to followers.
   *
   * Hosts pass the sealed PME1 bytes plus `devicePrivateKey` (§20.9);
   * an already-opened inner ticket is accepted for internal callers.
   *
   * A **route-day** ticket (§21.11) is routed down the §21 delegated
   * path automatically: topics and sealing derive from the certificate's
   * root and the day seed only signs, so the run lands on the route's
   * permanent address and the term link never moves.
   */
  publishTicket(ticket: Uint8Array | string | DispatchTicket, options?: {
    baseUrl?: string | null;
    onPublish?: ((emitted: unknown) => unknown) | null;
    /** Off skips the resume round and starts at 0 — a first assignment. */
    catchUp?: boolean;
    /** Only used when the ticket's plan left the field unspecified. */
    travelMode?: number | null;
    /** Required to open a sealed PME1 job (§20.9). */
    devicePrivateKey?: Uint8Array | null;
    /**
     * The term's link, for a host that holds the exact 45 bytes the
     * depot published (§21.11). Omit and it is rebuilt from the
     * certificate's root — identical bytes whenever the ticket's
     * `notAfter` is the term's.
     */
    link?: Uint8Array | null;
  }): Promise<{
    publisher: unknown;
    link: Uint8Array;
    url: string | null;
    notAfter: number;
    ticket: DispatchTicket;
    jobIdHex: string;
    plan: DecodedThreadPlan;
    handleFix(fix: unknown): Promise<{ published: boolean; contributeTraffic: boolean }>;
    announce(note: Uint8Array | string, options?: unknown): Promise<unknown>;
    markStop(index: number, outcome: 1 | 2 | 3, options?: StopMarkOptions): Promise<unknown>;
    /** The sealed blob this run committed to, by commitment hex (§20.7). */
    photoFor(hash: string | Uint8Array): Uint8Array | null;
    outcomes(): number[];
    finish(options?: unknown): Promise<unknown>;
    readonly seq: number;
    readonly state: number;
    readonly stopIndex: number;
    /** The plan's, not the driver's app's (§20.2). */
    readonly travelMode: number;
  }>;

  /**
   * Publishes one **service day** of a recurring route (§21). Takes the
   * day seed and its certificate — never the route root, which stays at
   * the depot. The follow link is the *term's* link and is not minted
   * here: pass it, or let this rebuild the identical 45 bytes from the
   * certificate's root key.
   */
  publishRouteDay(options?: {
    baseUrl?: string;
    daySeed: Uint8Array;
    certificate: DayCertificate | Uint8Array;
    mode?: number;
    plan?: unknown;
    /** The term's link, minted once by the depot. */
    link?: Uint8Array | null;
    /** The term's expiry. Defaults to the certificate's, the conservative reading. */
    notAfter?: number | null;
    travelMode?: number | null;
    onPublish?: ((emitted: unknown) => unknown) | null;
  }): Promise<{
    publisher: unknown;
    link: Uint8Array;
    url: string | null;
    notAfter: number;
    handleFix(fix: unknown): Promise<{ published: boolean; contributeTraffic: boolean }>;
    announce(note: Uint8Array | string, options?: unknown): Promise<unknown>;
    markStop(index: number, outcome: 1 | 2 | 3, options?: StopMarkOptions): Promise<unknown>;
    outcomes(): number[];
    finish(options?: unknown): Promise<unknown>;
    readonly seq: number;
    readonly state: number;
    readonly stopIndex: number;
  }>;

  /** Follows a run from a URL, a bare fragment, raw bytes, or a link. */
  follow(linkOrUrl: string | Uint8Array | unknown, options?: {
    onUpdate?: ((update: unknown) => unknown) | null;
    cellContext?: ((leafCell: number) => unknown) | null;
  }): Promise<{
    topics: Set<string>;
    position(): Promise<{ lat: number; lon: number } | null>;
    eta(options: { plan: unknown; myStopIndex: number; live?: unknown; nowMillis?: number }): Promise<unknown>;
    status(options?: unknown): ThreadStatus;
    latest(): unknown;
    history(): unknown[];
    /** Pull whatever this follow missed from other peers, on demand. */
    catchUp(options?: { nowMillis?: number; peers?: string[] | null }): Promise<number>;
    /**
     * One sealed proof-of-delivery photo by the commitment on a mark
     * (§20.7). Verified against that commitment before it is returned;
     * null when nobody reachable holds it — availability here is "the
     * driver is online", because relays never cache a blob this size.
     * **Sealed**: opening it needs the run seed, which only the driver
     * and the dispatcher that minted the ticket have.
     */
    fetchPhoto(hash: string | Uint8Array, options?: { peers?: string[] | null }): Promise<Uint8Array | null>;
    stop(): void;
    readonly stats: { accepted: number; dropped: number; forgeries: number };
  }>;

  /** Follows the 5-minute tag rotation; drops expired links. */
  tick(nowMillis?: number): Promise<void>;
  /** Answers a PMR1 directly, for hosts wiring their own transport. */
  serveCatchUp(payload: Uint8Array, nowMillis?: number): Uint8Array | null;
  /** Answers a PMTF the same way (§20.7). Null when it is not a PMTF. */
  servePhoto(payload: Uint8Array, nowMillis?: number): Uint8Array | null;
  /** Fetches one sealed photo by commitment; verifies before returning. */
  fetchPhoto(hash: string | Uint8Array, options?: { peers?: string[] | null }): Promise<Uint8Array | null>;
  /** The §5.5 ring buffers this peer serves catch-up from. */
  cache: unknown;
  close(): void;
  /** For hosts that receive thread bytes off some other transport. */
  deliver(topicName: string, payload: Uint8Array, nowMillis?: number): Promise<void>;
  readonly runs: unknown[];
  readonly follows: unknown[];
  stats: {
    delivered: number; accepted: number; dropped: number; published: number;
    cached: number; served: number; caughtUp: number; catchUpRounds: number; provided: number;
    photosServed: number; photosFetched: number; photosRefused: number;
  };
};
