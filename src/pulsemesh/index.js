// PulseMesh protocol v1 — the anonymous peer-to-peer live-traffic
// channel for the rangefind route graph. See docs/pulsemesh.md for why
// and docs/pulsemesh-protocol.md for the normative wire specification.
//
// The mesh terminates in an ordinary LiveTrafficProvider, so the routing
// engine neither knows nor cares that its live data came from peers:
//
//   const node = new MeshNode({ id, epochHex: engine.root.sourceHash, ... });
//   await engine.route({ from, to, live: node.provider });

export {
  DEFAULT_CONSTANTS,
  FRESHNESS,
  QUALITY_WEIGHT,
  DETAIL_ZOOM,
  ZONE_ZOOM,
  applyBootstrapConstants,
  bucketAgeSeconds,
  detailCellForE7,
  detailCellRings,
  localOfDetailCell,
  qualityBinFromSnap,
  speedBinFromKmh,
  speedBinFromMps,
  speedBinKmh,
  speedBinMps,
  timeBucketFromMillis,
  topicWindowFromMillis,
  zoneOfDetailCell
} from "./bins.js";

export {
  MAGIC,
  PROOF_BLIND_TOKEN,
  PROOF_BOND,
  PROOF_NONE,
  POLARITY_CONFIRM,
  POLARITY_REFUTE,
  POLARITY_REPORT,
  BAN_REASON_INVALID_RECORDS,
  decodeAny,
  decodePMA1,
  decodePMB1,
  decodePMX1,
  decodePMC1,
  decodePMD1,
  decodePMF1,
  decodePMG1,
  decodePMI1,
  decodePMQ1,
  decodePMS1,
  encodePMB1,
  encodePMA1,
  encodePMC1,
  encodePMX1,
  encodePMD1,
  encodePMF1,
  encodePMG1,
  encodePMI1,
  encodePMQ1,
  encodePMS1,
  gossipMessageId,
  parseSegment,
  segmentString
} from "./codec.js";

export { sha256, sha256Hex, sha256Utf8, fromHex, toHex } from "./sha256.js";

export {
  MAX_BIRTHDAY_BITS,
  bondBucketForPeer,
  bondPhaseMillis,
  bondSeed,
  mintBond,
  solveBondProof,
  verifyBond,
  verifyBondProof
} from "./bond.js";

export {
  PHOTO_PROTOCOL,
  SYNC_PROTOCOL,
  THREAD_PROTOCOL,
  TOPIC_PREFIX,
  activeWindows,
  parseTopic,
  shardOfCell,
  topicForCell,
  topicName,
  topicsForZones,
  windowAcceptable
} from "./topics.js";

export { PulseMeshStore } from "./store.js";
export { TrustLedger, aggregateSegment, congestionRatio, contributionWeight } from "./aggregate.js";
export { CLASS_SPEED_CAP_KMH, DENIED_CLASSES, createValidator } from "./validate.js";
export {
  INCIDENT_TYPES,
  MAX_SEGMENT_PENALTY_SECONDS,
  applyContradictionDecay,
  incidentPenaltySeconds,
  scoreIncidentKey
} from "./incidents.js";
export { createPulseMeshProvider } from "./provider.js";
export { createContributor } from "./contribute.js";
export { checkMeshHost, congestionLevel, createMeshSession } from "./session.js";
export { createCorridorTraffic } from "./simulate.js";
export { createReticentProfile } from "./reticent.js";
export { createForwardHandler } from "./forward.js";
export { buildCellRequests, buildDecoyPool, corridorCells, diffDigest, requestOverhead } from "./sync.js";
export {
  MeshNode,
  banPeerHash16,
  canonicalJson,
  createLoopbackNetwork,
  signBootstrap,
  verifyBootstrap
} from "./node.js";
