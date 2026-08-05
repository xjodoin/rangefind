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
export declare function signThread(message: Uint8Array, privateSeed: Uint8Array): Promise<Uint8Array>;
export declare function verifyThread(message: Uint8Array, signature: Uint8Array, publicKey: Uint8Array): Promise<boolean>;
/** For hosts whose WebCrypto lacks Ed25519 (older Hermes). */
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
export declare const THREAD_MAGIC: Readonly<Record<string, string>>;
export declare const THREAD_MAX_RECORD_BYTES: 256;
export declare const THREAD_MAX_NOTE_BYTES: 64;
export declare const THREAD_REQUEST_SIZES: readonly number[];
export declare const LINK_BYTES: 45;
export declare const LINK_VERSION: 1;

export interface ThreadBodyFields {
  unixSeconds: number;
  state: number;
  mode: number;
  leafCell: number;
  geomRef: number;
  ratioQ12: number;
  speedBin: number;
  stopIndex: number;
  planRef: Uint8Array;
  note?: Uint8Array;
}

export interface ThreadBody extends ThreadBodyFields {
  magic: "PMTP";
  note: Uint8Array;
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
  /** The capability `P`. */
  publicKey: Uint8Array;
  epochPrefix8: Uint8Array;
  notAfter: number;
}

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

export declare function encodeThreadLink(fields: {
  publicKey: Uint8Array; epochPrefix8: Uint8Array; notAfter: number;
}): Uint8Array;
export declare function decodeThreadLink(bytes: Uint8Array): ThreadLink;
/** The capability belongs in a fragment: browsers never transmit it. */
export declare function threadLinkUrl(baseUrl: string, link: Uint8Array): string;
export declare function parseThreadLinkUrl(url: string): ThreadLink;

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
}

export interface ThreadEmission {
  bytes: Uint8Array;
  tag: Uint8Array;
  seq: number;
  window: number;
  body: ThreadBodyFields;
}

export declare function createThreadPublisher(options: {
  privateSeed: Uint8Array;
  epoch32: Uint8Array;
  mode?: number;
  plan?: RunPlan | null;
  publish?: ((emitted: ThreadEmission) => unknown) | null;
  constants?: Readonly<ThreadConstants>;
  clock?: () => number;
  snap?: ((point: { lat: number; lon: number }) => Promise<unknown>) | null;
}): Promise<{
  publicKey: Uint8Array;
  keys: ThreadKeys;
  /** `contributeTraffic` is §10 rule 4: false while serving a stop. */
  handleFix(fix: {
    lat?: number; lon?: number; speedMps?: number; nowMillis?: number;
    match?: { segment: string; ratio?: number } | null; note?: Uint8Array | null;
  }): Promise<{ published: boolean; reason?: string; record?: ThreadEmission; contributeTraffic: boolean }>;
  finish(options?: { nowMillis?: number; canceled?: boolean; note?: Uint8Array | null }): Promise<ThreadEmission>;
  announce(note: Uint8Array, options?: { nowMillis?: number }): Promise<ThreadEmission>;
  stats: { published: number; suppressedTraffic: number; stopEvents: number };
  readonly seq: number;
  readonly state: number;
  readonly mode: number;
}>;

export type ThreadAcceptResult =
  | { ok: true; update: ThreadBody & { seq: number; receivedAt: number } }
  | { ok: false; step: number; reason: string };

/** §12's four rows, so a UI cannot accidentally upgrade its claim. */
export interface ThreadStatus {
  row: "thread+traffic" | "thread-only" | "traffic-only" | "neither";
  live: boolean;
  hasPosition: boolean;
  ageSeconds: number | null;
  state: number;
  stopIndex: number;
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
  stats: { accepted: number; dropped: number; dropsByStep: Record<string, number>; forgeries: number };
  latest(): (ThreadBody & { seq: number; receivedAt: number }) | null;
  history(): Array<ThreadBody & { seq: number; receivedAt: number }>;
  readonly highestSeq: number;
}>;

export interface ArrivalEstimate {
  arrivalMillis: number;
  secondsFromObservation: number;
  secondsFromNow: number;
  stopsAway: number;
  basis: "live-traffic" | "static-metric" | "schedule";
  positionBasis: "reported-position" | "last-stop" | "none";
  observationAgeSeconds: number;
  stale: boolean;
}

/**
 * §9 — the whole reason threads live in rangefind. Uses `matrix()` over
 * the planned order, never `itinerary()`, which reorders stops and would
 * silently produce optimistic ETAs.
 */
export declare function estimateArrival(options: {
  engine: { locate(segment: string, ratio: number): Promise<{ lat: number; lon: number }>; matrix(params: unknown): Promise<{ seconds: number[][] }> };
  update: ThreadBody | null;
  plan: RunPlan;
  myStopIndex: number;
  live?: unknown;
  constants?: Readonly<ThreadConstants>;
  nowMillis?: number;
}): Promise<ArrivalEstimate | null>;

export declare function scheduledArrival(options: { plan: RunPlan; myStopIndex: number }):
  { arrivalMillis: number; basis: "schedule"; positionBasis: "none"; stale: false } | null;

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
