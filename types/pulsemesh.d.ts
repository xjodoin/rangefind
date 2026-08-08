import type { LiveSegmentState, LiveTrafficProvider } from "./route.js";

/** A Web-Mercator XYZ cell (z15 detail cell or z9 zone), integer coords. */
export interface MeshCell {
  x: number;
  y: number;
}

/** Protocol §3 constants. Tunable rows may be overridden by the signed bootstrap. */
export interface PulseMeshConstants {
  MAX_AGE_RECEIPT: number;
  MAX_FUTURE_SKEW: number;
  CONTRIB_TTL: number;
  DISPLAY_MAX_AGE: number;
  BUCKET_SECONDS: number;
  RETAINED_BUCKETS: number;
  TOPIC_WINDOW: number;
  TOPIC_OVERLAP: number;
  ANTI_ENTROPY_SECONDS: number;
  SHARDS: number;
  EMIT_INTERVAL: number;
  RATE_SUSTAINED: number;
  RATE_BURST: number;
  MAX_RECORD_BYTES: number;
  MAX_GOSSIP_BYTES: number;
  CELL_CONTRIB_CAP: number;
  STORE_CONTRIB_CAP: number;
  SEG_CONTRIB_CAP: number;
  BATCH_SIZES: number[];
  DECOY_FRACTION: number;
  SPLIT_PEERS: number;
  ENDPOINT_RINGS: number;
  UNSUB_LINGER: number;
  EPOCH_OVERLAP: number;
  INCIDENT_TTL_MAX: number;
  TRUST_INIT: number;
  TRUST_MIN: number;
  TRUST_MAX: number;
  AGG_MIN_REPORTS: number;
  AGG_HINT_REPORTS: number;
  SURPRISE_BINS: number;
  SURPRISE_CONFIDENCE: number;
  BASE_SAMPLE_RATE: number;
  RETICENT_GAP: number;
  COMPANY_WINDOW: number;
  STOP_RADIUS: number;
  STOP_SECONDS: number;
  FORWARD_POOL: number;
  FORWARD_MAX_DELAY: number;
  FORWARD_RATE: number;
  INCIDENT_SHOW_SCORE: number;
  INCIDENT_HINT_SCORE: number;
  INCIDENT_ANCHOR_RATIO: number;
  INCIDENT_WINDOW: number;
  INCIDENT_TTL_MIN: number;
  INCIDENT_CELL_CAP: number;
  INCIDENT_PEER_RATE: number;
  INCIDENT_PEER_RATE_WINDOW: number;
  REFUTE_WEIGHT: number;
  CONTRADICTION_DECAY: number;
  BOND_BIRTHDAY_BITS: number;
  BOND_PAIR_DIFFICULTY: number;
  BOND_LIFETIME: number;
  BOND_OVERLAP: number;
  BAN_MIN_SOURCES: number;
  BAN_REMOTE_PENALTY: number;
  BAN_PEER_RATE: number;
  BAN_PEER_RATE_WINDOW: number;
  BAN_TTL: number;
  BAN_TARGET_CAP: number;
}

export declare const DEFAULT_CONSTANTS: Readonly<PulseMeshConstants>;
export declare const FRESHNESS: readonly number[];
export declare const QUALITY_WEIGHT: readonly number[];
export declare const DETAIL_ZOOM: 15;
export declare const ZONE_ZOOM: 9;

export declare function applyBootstrapConstants(
  overrides: Partial<PulseMeshConstants> | null | undefined,
  base?: Readonly<PulseMeshConstants>
): Readonly<PulseMeshConstants>;

export declare function detailCellForE7(latE7: number, lonE7: number): MeshCell;
export declare function zoneOfDetailCell(cell: MeshCell): MeshCell;
export declare function localOfDetailCell(cell: MeshCell): MeshCell;
export declare function detailCellRings(cell: MeshCell, rings: number): MeshCell[];
export declare function bucketAgeSeconds(bucket: number, nowMillis: number): number;
export declare function timeBucketFromMillis(unixMillis: number): number;
export declare function topicWindowFromMillis(unixMillis: number): number;
export declare function speedBinFromKmh(kmh: number): number | null;
export declare function speedBinFromMps(mps: number): number | null;
export declare function speedBinKmh(bin: number): number;
export declare function speedBinMps(bin: number): number;
/** 1..7, or 0 meaning "suppress — do not emit" (0 is never legal on the wire). */
export declare function qualityBinFromSnap(distMeters: number, headingDeltaDeg?: number): number;

// --- Records --------------------------------------------------------------

/** A decoded PMC1 contribution (§4.1), carrying its verbatim bytes. */
export interface ContributionRecord {
  kind: "contribution";
  magic: "PMC1";
  epochPrefix8: Uint8Array;
  leafCell: number;
  geomRef: number;
  timeBucket: number;
  speedBin: number;
  qualityBin: number;
  meters: number;
  ttlSeconds: number;
  reportId: Uint8Array;
  proofType: number;
  proof: Uint8Array;
  /** Physical directed-segment id "leaf/polyline/direction". */
  segment: string;
  bytes: Uint8Array;
  preimage: Uint8Array;
}

/** A decoded PMI1 incident (§4.3), carrying its verbatim bytes. */
export interface IncidentRecord {
  kind: "incident";
  magic: "PMI1";
  epochPrefix8: Uint8Array;
  leafCell: number;
  geomRef: number;
  ratioQ12: number;
  timeBucket: number;
  type: number;
  polarity: 1 | 2 | 3;
  ttlSeconds: number;
  reportId: Uint8Array;
  proofType: number;
  proof: Uint8Array;
  segment: string;
  bytes: Uint8Array;
  preimage: Uint8Array;
}

export interface EncodedRecord {
  bytes: Uint8Array;
  /** The proof preimage: every field before proofType. */
  preimage: Uint8Array;
}

export declare const MAGIC: Readonly<Record<string, string>>;
export declare const PROOF_NONE: 0;
/** Value 1 was per-record proof-of-work in the drafts; burned, never reassigned. */
export declare const PROOF_BLIND_TOKEN: 2;
/** §5.4: proofless record vouched for by the delivering peer's bond. */
export declare const PROOF_BOND: 3;

// --- §5.4 identity bonds -------------------------------------------------

export interface BondFields {
  epochPrefix8: Uint8Array;
  dayBucket: number;
  birthdayBits: number;
  pairDifficulty: number;
  salt: number;
  i: number;
  j: number;
}
export declare const MAX_BIRTHDAY_BITS: 48;
export declare function bondPhaseMillis(peerId: string, lifetimeSeconds: number): number;
export declare function bondBucketForPeer(peerId: string, nowMillis: number, lifetimeSeconds: number): number;
export declare function bondSeed(epoch32: Uint8Array, dayBucket: number, salt: number, peerId: string): Uint8Array;
export declare function encodePMA1(bond: BondFields): Uint8Array;
export declare function decodePMA1(bytes: Uint8Array): BondFields & { kind: "bond"; magic: "PMA1" };
export declare function verifyBondProof(
  seed32: Uint8Array, i: number, j: number, birthdayBits: number, pairDifficulty: number
): boolean;
export declare function solveBondProof(
  seed32: Uint8Array,
  birthdayBits: number,
  pairDifficulty: number,
  options?: {
    chunkMillis?: number;
    budgetMillis?: number | null;
    signal?: AbortSignal | null;
    yieldTo?: () => Promise<void>;
    now?: () => number;
    sliceIterations?: number;
  }
): Promise<{ i: number; j: number; hashes: number; tableBytes: number } | null>;
export declare function mintBond(options: {
  epoch32: Uint8Array;
  peerId: string;
  constants: Readonly<PulseMeshConstants>;
  nowMillis?: number;
  maxSalts?: number;
  chunkMillis?: number;
  budgetMillis?: number | null;
  signal?: AbortSignal | null;
  yieldTo?: () => Promise<void>;
}): Promise<BondFields | null>;
// --- §8.4 ban announcements ---------------------------------------------

export declare const BAN_REASON_INVALID_RECORDS: 1;
export interface BanAnnouncement {
  kind: "ban";
  magic: "PMX1";
  epochPrefix8: Uint8Array;
  targetHash16: Uint8Array;
  reason: number;
  timeBucket: number;
  reportId: Uint8Array;
}
export declare function encodePMX1(fields: {
  epochPrefix8: Uint8Array; targetHash16: Uint8Array; reason: number;
  timeBucket: number; reportId: Uint8Array;
}): Uint8Array;
export declare function decodePMX1(bytes: Uint8Array): BanAnnouncement;
/** The 16-byte handle PMX1 names — a hash, never the peerId itself. */
export declare function banPeerHash16(peerId: string): Uint8Array;

export declare function verifyBond(bond: BondFields, context: {
  epoch32: Uint8Array;
  previousEpoch32?: Uint8Array | null;
  peerId: string;
  constants: Readonly<PulseMeshConstants>;
  nowMillis?: number;
}): { ok: true; expiresMillis: number } | { ok: false; reason: string };
export declare const POLARITY_REPORT: 1;
export declare const POLARITY_CONFIRM: 2;
export declare const POLARITY_REFUTE: 3;

export declare function encodePMC1(fields: Record<string, unknown>): EncodedRecord;
export declare function decodePMC1(bytes: Uint8Array): ContributionRecord;
export declare function encodePMI1(fields: Record<string, unknown>): EncodedRecord;
export declare function decodePMI1(bytes: Uint8Array): IncidentRecord;
export declare function encodePMB1(records: Array<Uint8Array | EncodedRecord>): Uint8Array;
export declare function decodePMB1(bytes: Uint8Array): { kind: "batch"; magic: "PMB1"; records: ContributionRecord[] };
export declare function encodePMD1(digest: Record<string, unknown>): Uint8Array;
export declare function decodePMD1(bytes: Uint8Array): ZoneDigest & { kind: "digest"; epochPrefix8: Uint8Array };
export declare function encodePMG1(request: { epochPrefix8: Uint8Array; zoneX: number; zoneY: number }): Uint8Array;
export declare function decodePMG1(bytes: Uint8Array): { kind: "getDigest"; zoneX: number; zoneY: number };
export declare function encodePMQ1(request: { epochPrefix8: Uint8Array; cells: MeshCell[] }): Uint8Array;
export declare function decodePMQ1(bytes: Uint8Array): { kind: "getCells"; cells: MeshCell[] };
export declare function encodePMS1(response: { epochPrefix8: Uint8Array; cells: unknown[] }): Uint8Array;
export declare function decodePMS1(bytes: Uint8Array): { kind: "cellSnapshots"; cells: Array<MeshCell & { records: ContributionRecord[]; incidents: IncidentRecord[] }> };
export declare function encodePMF1(message: { epochPrefix8: Uint8Array; delayMs: number; payload: Uint8Array }): Uint8Array;
export declare function decodePMF1(bytes: Uint8Array): { kind: "forward"; delayMs: number; payload: Uint8Array };
/** Decodes by magic; unknown magics return { kind: "unknown" } rather than throwing. */
export declare function decodeAny(bytes: Uint8Array): { kind: string; magic?: string; [key: string]: unknown };

export declare function segmentString(leafCell: number, geomRef: number): string;
export declare function parseSegment(segment: string): { leafCell: number; geomRef: number };
export declare function gossipMessageId(payload: Uint8Array): Uint8Array;
export declare function sha256(input: Uint8Array | number[]): Uint8Array;
export declare function sha256Hex(input: Uint8Array | number[]): string;
export declare function sha256Utf8(text: string): Uint8Array;
export declare function toHex(bytes: Uint8Array): string;
export declare function fromHex(hex: string): Uint8Array;

// --- Topics ---------------------------------------------------------------

export declare const TOPIC_PREFIX: string;
export declare const SYNC_PROTOCOL: string;
/** Threads §5.5 catch-up: PMR1 in, PMM1 out. */
export declare const THREAD_PROTOCOL: string;
export declare function shardOfCell(cell: MeshCell): number;
export declare function topicName(parts: {
  epochPrefix16hex: string; zoneX: number; zoneY: number; window: number; shard: number;
}): string;
export declare function topicForCell(parts: { epochPrefix16hex: string; cell: MeshCell; window: number }): string;
export declare function parseTopic(topic: string):
  | { reserved: true }
  | { reserved: false; epochPrefix16hex: string; zoneX: number; zoneY: number; window: number; shard: number }
  | null;
export declare function activeWindows(nowMillis: number, overlapSeconds?: number): number[];
export declare function windowAcceptable(topicWindow: number, nowMillis: number): boolean;
export declare function topicsForZones(params: {
  epochPrefix16hex: string; zones: MeshCell[]; nowMillis: number; overlapSeconds?: number; shards?: number[] | null;
}): string[];

// --- Store, aggregation, trust -------------------------------------------

export interface StoreEntry<R = ContributionRecord> {
  record: R;
  cell: MeshCell;
  cellKey: string;
  idHex: string;
  /** Peer that delivered it; null for locally produced or snapshot-merged. */
  deliveredBy: string | null;
  viaForward: boolean;
  receivedAt: number;
  expiresAt: number;
}

export interface ZoneDigest {
  zoneX: number;
  zoneY: number;
  baseBucket: number;
  entries: Array<{ localX: number; localY: number; count: number; ageBuckets: number; idFold: Uint8Array }>;
}

export declare class PulseMeshStore {
  constructor(options: {
    cellOf: (record: { leafCell: number; geomRef: number }) => MeshCell | null;
    constants?: Readonly<PulseMeshConstants>;
    trustOf?: ((peerId: string) => number) | null;
  });
  readonly stats: { added: number; duplicates: number; evicted: number; expired: number };
  size(): number;
  incidentCount(): number;
  hasReport(reportId: Uint8Array | string): boolean;
  addContribution(record: ContributionRecord, options: {
    nowMillis: number; deliveredBy?: string | null; viaForward?: boolean; cell?: MeshCell | null;
  }): { added: boolean; reason: string | null };
  addIncident(record: IncidentRecord, options: {
    nowMillis: number; deliveredBy?: string | null; viaForward?: boolean; cell?: MeshCell | null;
    scoreOf?: ((key: string) => number) | null;
  }): { added: boolean; reason: string | null };
  sweep(nowMillis: number): void;
  /** Wire segment key is "leafCell/geomRef" — direction lives inside geomRef. */
  contributionsForSegment(segKey: string): Array<StoreEntry<ContributionRecord>>;
  incidentsForSegment(segKey: string): Array<StoreEntry<IncidentRecord>>;
  incidentsForKey(key: string): Array<StoreEntry<IncidentRecord>>;
  liveSegmentKeys(): string[];
  digestForZone(zone: MeshCell, nowMillis: number): ZoneDigest;
  snapshotForCells(cells: MeshCell[]): Array<MeshCell & { records: ContributionRecord[]; incidents: IncidentRecord[] }>;
}

export interface SegmentAggregate {
  speedBin: number;
  speedKmh: number;
  speedMps: number;
  n: number;
  /** True when n is below AGG_MIN_REPORTS: confidence is capped at 0.30. */
  hint: boolean;
  totalWeight: number;
  agreementNum: number;
  agreement: number;
  diversity: number;
  freshnessFactor: number;
  confidence: number;
  confidenceBin: number;
  newestBucket: number;
  observedAt: number;
  meters: number;
}

export declare function aggregateSegment(
  entries: Array<StoreEntry<ContributionRecord>>,
  options: { nowMillis: number; trustOf?: ((peerId: string) => number) | null; constants?: Readonly<PulseMeshConstants> }
): SegmentAggregate | null;
export declare function contributionWeight(
  record: { qualityBin: number }, ageSeconds: number, trustMilli?: number
): number;
export declare function congestionRatio(aggregate: SegmentAggregate | null, freeflowKmh: number): number | null;

export declare class TrustLedger {
  constructor(options?: { constants?: Readonly<PulseMeshConstants>; clock?: () => number });
  get(peerId: string | null): number;
  penalizeValidation(peerId: string | null): void;
  /** §8.4 corroborated remote testimony: bounded, clamped, never revokes. */
  penalizeRemoteBan(peerId: string | null): void;
  isFloored(peerId: string | null, nowMillis?: number): boolean;
  applyAggregateFeedback(aggregate: SegmentAggregate, entries: Array<StoreEntry<ContributionRecord>>): void;
}

// --- Validation -----------------------------------------------------------

export type ValidationResult<R> =
  | { ok: true; record: R }
  | { ok: false; rule: number; reason: string; trustPenalty: boolean };

export interface LeafContext {
  polylineCount: number;
  classOf?: (geomRef: number) => string | null;
  metersOf?: (geomRef: number) => number | null;
}

export declare const CLASS_SPEED_CAP_KMH: Readonly<Record<string, number>>;
export declare const DENIED_CLASSES: ReadonlySet<string>;

export declare function createValidator(options: {
  epoch32: Uint8Array;
  previousEpoch32?: Uint8Array | null;
  constants?: Readonly<PulseMeshConstants>;
  cellOf?: ((record: { leafCell: number; geomRef: number }) => MeshCell | null) | null;
  cellContext?: ((leafCell: number) => LeafContext | null) | null;
  /** "wire" rejects proofType 0; "loopback" accepts it for tests and demos. */
  transport?: "wire" | "loopback";
  suppressedTypes?: number[];
  clock?: () => number;
}): {
  validateContribution(
    payload: Uint8Array | ContributionRecord,
    context?: { store?: PulseMeshStore | null; fromPeer?: string | null; vouchPeer?: string | null; topic?: unknown; nowMillis?: number }
  ): ValidationResult<ContributionRecord>;
  validateIncident(
    payload: Uint8Array | IncidentRecord,
    context?: { store?: PulseMeshStore | null; fromPeer?: string | null; vouchPeer?: string | null; topic?: unknown; nowMillis?: number }
  ): ValidationResult<IncidentRecord>;
  /** §8.4: gate for PMX1 testimony — bonded deliverers only, rate-bounded. */
  validateBan(
    payload: Uint8Array | BanAnnouncement,
    context?: { fromPeer?: string | null; nowMillis?: number }
  ): ValidationResult<BanAnnouncement>;
  constants: Readonly<PulseMeshConstants>;
};

// --- Incidents ------------------------------------------------------------

export interface IncidentType {
  name: string;
  /** Stored and displayed for both directions regardless of the reported bit. */
  appliesBoth: boolean;
  defaultTtlSeconds: number;
  /** 0 means informational: never influences a route under any score. */
  penaltySeconds: number;
}

export declare const INCIDENT_TYPES: Readonly<Record<number, IncidentType>>;
export declare const MAX_SEGMENT_PENALTY_SECONDS: 300;

export interface ScoredIncident {
  segment: string;
  segKey: string;
  type: number;
  typeName: string;
  ratioQ12: number;
  raw: number;
  sources: number;
  /** min(raw, distinct delivering peers); forwarded-only keys cap at the hint score. */
  score: number;
  reports: number;
  confirms: number;
  refutes: number;
  newestBucket: number;
  displayed: boolean;
  hint: boolean;
  entries: Array<StoreEntry<IncidentRecord>>;
}

export declare function scoreIncidentKey(
  entries: Array<StoreEntry<IncidentRecord>>,
  options: { nowMillis: number; trustOf?: ((peerId: string) => number) | null; constants?: Readonly<PulseMeshConstants> }
): ScoredIncident | null;
export declare function applyContradictionDecay(
  scored: ScoredIncident | null,
  aggregate: SegmentAggregate | null,
  freeflowKmh: number | null,
  options: { nowMillis: number; constants?: Readonly<PulseMeshConstants> }
): boolean;
export declare function incidentPenaltySeconds(
  scoredIncidents: ScoredIncident[],
  aggregate: SegmentAggregate | null,
  freeflowKmh: number | null,
  options?: { constants?: Readonly<PulseMeshConstants> }
): number;

// --- Provider -------------------------------------------------------------

export interface PulseMeshProvider extends LiveTrafficProvider {
  name: "pulsemesh";
  fetch(query: { epoch: string; areas?: unknown[]; maxAgeSeconds?: number }): Promise<LiveSegmentState[]>;
  /** Display-only incidents, including informational types. Never routed. */
  displayIncidents(options?: { nowMillis?: number }): ScoredIncident[];
  aggregates(nowMillis?: number): Map<string, SegmentAggregate>;
  congestionRatioOf(segKey: string, nowMillis?: number): number | null;
}

export declare function createPulseMeshProvider(options: {
  epochHex: string;
  store: PulseMeshStore;
  trust?: TrustLedger | null;
  constants?: Readonly<PulseMeshConstants>;
  fetchCells?: ((cells: MeshCell[]) => Promise<unknown>) | null;
  freeflowKmhOf?: ((segKey: string) => number | null) | null;
  clock?: () => number;
  applyTrustFeedback?: boolean;
}): PulseMeshProvider;

// --- Contributor and the reticent profile --------------------------------

export interface SnapLikeMatch {
  segment: string;
  distMeters: number;
  ratio?: number;
  bearingDeg?: number;
  snappedLatE7?: number;
  snappedLonE7?: number;
}

export interface ContributorEmission {
  emitted: boolean;
  reason?: string;
  record?: EncodedRecord;
  forwarder?: string | null;
  segKey?: string;
  speedBin?: number;
}

export declare function createContributor(options: {
  epoch32: Uint8Array;
  epochPrefix8: Uint8Array;
  snap: (fix: unknown) => Promise<SnapLikeMatch | null>;
  publish?: ((result: ContributorEmission) => unknown) | null;
  constants?: Readonly<PulseMeshConstants>;
  profile?: "cadence" | ReticentProfile | null;
  metersOf?: ((segKey: string) => number) | null;
  classOf?: ((segKey: string) => string | null) | null;
  proofType?: number;
  randomBytes?: (length: number) => Uint8Array;
  clock?: () => number;
  batteryLevel?: (() => number) | null;
  charging?: (() => boolean) | null;
  suppressedTypes?: number[];
}): {
  handleFix(fix: { lat?: number; lon?: number; speedMps?: number; courseDeg?: number; nowMillis?: number; [key: string]: unknown }): Promise<ContributorEmission>;
  reportIncident(params: { type: number; polarity?: 1 | 2 | 3; incidentKey?: string | null; nowMillis?: number; acknowledgedPublic?: boolean }):
    { emitted: boolean; reason?: string; record?: EncodedRecord };
  stats: { fixes: number; offRoad: number; suppressed: number; emitted: number; incidents: number };
};

export interface ReticentProfile {
  evaluate(observation: {
    segKey: string;
    speedBin: number;
    roadClass?: string | null;
    snappedLatE7?: number | null;
    snappedLonE7?: number | null;
    nowMillis?: number;
  }): { emit: boolean; surprise: boolean; gate: string | null; forwarder: string | null };
  suppressAfterReport(nowMillis?: number): void;
  stats: { evaluated: number; gate1: number; gate2Surprise: number; gate3Pass: number; gate3Blocked: number; emitted: number };
}

export declare function createReticentProfile(options: {
  expectedBinOf: (segKey: string) => number;
  companyCountOf: (segKey: string, nowMillis: number) => number;
  constants?: Readonly<PulseMeshConstants>;
  isNearOwnStop?: ((latE7: number, lonE7: number) => boolean) | null;
  forwarderPool?: (() => string[]) | null;
  rng?: () => number;
  clock?: () => number;
}): ReticentProfile;

// --- Forwarding and sync --------------------------------------------------

export declare function createForwardHandler(options: {
  validator: ReturnType<typeof createValidator>;
  publishAsOwn: (payload: Uint8Array, meta: { records: unknown[]; viaForward: true }) => void;
  constants?: Readonly<PulseMeshConstants>;
  schedule?: (fn: () => void, delayMs: number) => unknown;
  clock?: () => number;
}): {
  handle(message: { delayMs: number; payload: Uint8Array }, context: { fromPeer: string; nowMillis?: number }):
    { forwarded: boolean; reason?: string };
  stats: { accepted: number; dropped: number; republished: number };
};

export declare function diffDigest(localDigest: ZoneDigest, remoteDigest: ZoneDigest): MeshCell[];
export declare function buildDecoyPool(wanted: MeshCell[], visited?: MeshCell[], rings?: number): MeshCell[];
/** Every request is padded to 8/16/32, shuffled, and split across peers (§11.3). */
export declare function buildCellRequests(params: {
  wanted: MeshCell[];
  peers: string[];
  decoyPool?: MeshCell[];
  constants?: Readonly<PulseMeshConstants>;
  rng?: () => number;
}): Array<{ peer: string; cells: MeshCell[] }>;
export declare function requestOverhead(
  requests: Array<{ cells: MeshCell[] }>, wantedCount: number
): { wanted: number; fetched: number; decoys: number; overheadRatio: number };

// --- Mesh node ------------------------------------------------------------

/** Transport contract: libp2p, the loopback network, or a simulation. */
export interface MeshNetwork {
  register(node: MeshNode): void;
  subscribe(nodeId: string, topic: string): void;
  unsubscribe(nodeId: string, topic: string): void;
  publish(topic: string, payload: Uint8Array, fromId: string): void;
  request(fromId: string, toId: string, payload: Uint8Array): Promise<Uint8Array | null> | Uint8Array | null;
  peersOf(nodeId: string): string[];
  /**
   * The multiaddrs this host is actually connected to, each ending in
   * `/p2p/<peerId>` so it can be dialled again as-is (threads §20.10).
   * Only the wire transport has these. The library reports; the host
   * persists — there is no peer store here, because what a device may
   * remember about who it has talked to is a product decision.
   */
  knownPeers?(): string[];
  schedule?(fn: () => void, delayMs: number): unknown;
}

export declare class MeshNode {
  constructor(options: {
    id: string;
    /** The route graph root's sourceHash: 64 lowercase hex characters. */
    epochHex: string;
    previousEpochHex?: string | null;
    cellOf: (record: { leafCell: number; geomRef: number }) => MeshCell | null;
    network: MeshNetwork;
    constants?: Readonly<PulseMeshConstants>;
    cellContext?: ((leafCell: number) => LeafContext | null) | null;
    freeflowKmhOf?: ((segKey: string) => number | null) | null;
    clock?: () => number;
    rng?: () => number;
    transport?: "wire" | "loopback";
    suppressedTypes?: number[];
    keeper?: boolean;
    /** §11.6: no gossip membership, no publishing, no bond — pull-only via tick(). */
    readOnly?: boolean;
  });
  readonly id: string;
  readonly epochHex: string;
  readonly epoch32: Uint8Array;
  readonly epochPrefix8: Uint8Array;
  readonly store: PulseMeshStore;
  readonly trust: TrustLedger;
  /** Plug straight into engine.route({ live }). */
  readonly provider: PulseMeshProvider;
  readonly stats: {
    gossipAccepted: number; gossipDropped: number; dropsByRule: Record<string, number>;
    bondsAccepted: number; bondsRejected: number;
    bansForfeited: number; bansPublished: number; bansAccepted: number; bansCorroborated: number;
    snapshotsMerged: number; snapshotRecordsAccepted: number;
    antiEntropyRounds: number; cellsRequested: number; cellsWanted: number;
  };
  clock(): number;
  subscribeZones(zones: MeshCell[], nowMillis?: number): void;
  publishRecord(encoded: EncodedRecord, options?: { forwarder?: string | null; nowMillis?: number }): void;
  /** §5.4: verifies a PMA1 from `fromPeer`'s live connection and marks the peer bonded. */
  registerBond(payload: Uint8Array | BondFields, fromPeer: string, nowMillis?: number):
    { ok: true; expiresMillis: number } | { ok: false; reason: string };
  isBonded(peerId: string, nowMillis?: number): boolean;
  /** §5.4: peers whose bond this node verified (or a gateway admitted), and until when. */
  readonly bondedPeers: Map<string, number>;
  /** §8.4: peers forfeited on first-hand evidence, and until when. */
  readonly locallyBanned: Map<string, number>;
  /**
   * Optional tap for gateways (§16 LoRa bridges, §12.1 fleet seeds).
   * Fires only for records that passed every rule and entered the store —
   * from gossip and from merged snapshots alike — which is what makes it
   * usable as a bridge's gate: what it hands you is what this node has
   * already staked its bond on.
   */
  onRecordAccepted?: ((
    record: ContributionRecord | IncidentRecord,
    meta: { fromPeer: string | null; nowMillis: number; viaSnapshot?: boolean }
  ) => void) | null;
  onGossip(topic: string, payload: Uint8Array, fromPeer: string, nowMillis?: number): void;
  onStream(payload: Uint8Array, fromPeer: string, nowMillis?: number): Uint8Array | null;
  fetchCells(wanted: MeshCell[], options?: { nowMillis?: number }): Promise<number>;
  mergeSnapshot(payload: Uint8Array, fromPeer: string, nowMillis?: number): number;
  antiEntropyWith(peerId: string, zone: MeshCell, nowMillis?: number): Promise<number>;
  /** One maintenance tick: TTL sweep plus a jittered anti-entropy round. */
  tick(nowMillis?: number): Promise<void>;
}

export declare function createLoopbackNetwork(options?: { clock?: () => number }): MeshNetwork & {
  counters(nodeId: string): { gossipIn: number; gossipOut: number; streamIn: number; streamOut: number; messages: number };
  nodes: Map<string, MeshNode>;
};

// The integration boundary a host actually wires (session + simulator).
export * from "./pulsemesh-session.js";

/** UTF-8, keys sorted at every level, no whitespace — the signing encoding. */
export declare function canonicalJson(value: unknown): string;
export declare function verifyBootstrap(
  bootstrap: Record<string, unknown>, expectedPublicKeyHex: string
): Promise<{ ok: true } | { ok: false; reason: string }>;
export declare function signBootstrap(
  unsigned: Record<string, unknown>, privateSeedHex: string
): Promise<Record<string, unknown> & { signature: string }>;
