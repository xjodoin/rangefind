// PulseMesh Threads — the authenticated tracking channel.
//
// Where the traffic channel answers "how fast is this road" from many
// anonymous corroborated observations, this one answers "where is this
// specific vehicle, and when does it reach my stop" for an audience that
// is authorized to know and for nobody else. See docs/pulsemesh-threads.md.
//
// A link separates two roles: a random thread secret derives discovery,
// admission and content keys, while an Ed25519 public key only verifies.
// Holding the link lets a follower find and decrypt the thread but never
// sign it. The fragment contains no host, mailbox, or bootstrap address.
//
//   const publisher = await createThreadPublisher({ privateSeed, epoch32, plan });
//   const url = threadLinkUrl("https://track.example/r", encodeThreadLink({
//     threadSecret: publisher.threadSecret,
//     rootPublicKey: publisher.rootPublicKey, epochPrefix8, notAfter
//   }));
//   // …the subscriber, holding only that URL:
//   const subscriber = await createThreadSubscriber({ link: parseThreadLinkUrl(url), epoch32 });
//   const eta = await estimateArrival({ engine, update: subscriber.latest(), plan, myStopIndex, live });
//
// A dispatch ticket is the one artifact that is never in the clear
// (§20.9): it carries the run seed and the customers' phone numbers, so
// `issueSealedTicket` seals it to devices enrolled by their PMV1 card,
// and `publishTicket` opens it with that device's private key. There is
// no plaintext ticket form for a host to hand around.
//
// A **seed card** (PMH1, §20.10) is the one artifact here that is a
// *location* rather than a capability: one to three multiaddrs a cold
// device can dial to reach the mesh at all. A fleet that runs its own
// keeper hands one out — by QR off the keeper's own terminal — and the
// same addresses ride inside the sealed ticket, so a driver's phone gets
// them without any of it ever being broadcast to strangers.
//
// A **recurring route** (§21) is the one run whose link must outlive its
// driver. A school bus keeps one plan for a term; the driver changes some
// days and not most; and a parent must be able to watch the same link all
// term without ever resubscribing. So identity and authority come apart:
// the *root* key stays the verification identity while a separate thread
// secret keeps discovery and encryption stable. A CSPRNG-generated,
// short-lived **day key**, scoped and certified by the root, carries the
// publish authority. The driver's phone holds the day seed and
// its PMTC certificate and never the root, so a lost phone costs a day.
//
//   // at the depot, once a term:
//   const link = routeFollowLink({ threadSecret, rootPublicKey, epochPrefix8, notAfter: termEnd });
//   // at the depot, each morning — a fresh, independently random authority:
//   const { daySeed, certificate, threadSecret } = await mintDayCertificate({
//     rootSeed, planRef, serviceDay: 20260908, notBefore, notAfter
//   });
//   // on the driver's phone, holding only those two:
//   await channel.publishRouteDay({ daySeed, certificate, threadSecret, plan });
//
// Getting those two *to* the phone is a **route-day ticket** (§21.11):
// the same sealed PMK1 the dispatch channel already uses, carrying the
// day certificate under `TICKET_FLAG_DAY` and the day seed as its
// `privateSeed`. One artifact, sealed to an enrolled device, holding
// identity (the certificate's root), authority (the seed) and the proof
// to publish with. `publishTicket` routes it down the §21 delegated path
// on its own, so a driver's records land on the route's own topic and
// the parents' term link never moves:
//
//   const job = await issueSealedTicket({
//     issuerSeed: rootSeed, epoch32, plan, notAfter: termEnd,
//     dayCertificate: certificate, recipients: [driverDeviceKey]
//   });
//   await channel.publishTicket(job.sealed, { devicePrivateKey });
//
// A **job offer** (PMJ1, §20.4) is the mirror image: public and unsealed
// by design, because it is broadcast to couriers nobody has enrolled
// yet. `issueJobOffer` publishes a commitment to the plan plus coarse
// descriptors — stop count, distance, gridded centroid, spread bucket —
// and not one address; `awardMatchesOffer` is how the winner's device
// checks that the ticket it was finally handed is the job that was
// advertised.

export {
  PHOTO_CHAIN_ZERO,
  PHOTO_SEAL_OVERHEAD,
  THREAD_MAX_PHOTO_BYTES,
  THREAD_TOPIC_PREFIX,
  base64UrlToBytes,
  bytesToBase64Url,
  deriveThreadSecret,
  deriveThreadKeys,
  equalThreadAuth,
  generateThreadKeypair,
  nativeX25519Available,
  openPhoto,
  openThreadBody,
  photoChainOf,
  photoChainStep,
  photoCommitment,
  photoHashBytes,
  photoHashHex,
  photoKeyFor,
  publicKeyFromSeed,
  sealPhoto,
  sealThreadBody,
  setThreadCryptoImplementation,
  signThread,
  threadAdmissionTag,
  threadRecordSigningMessage,
  threadRendezvous,
  threadTag,
  threadTagsForWindows,
  threadTopic,
  threadWindow,
  verifyPhotoChain,
  verifyThread,
  x25519PublicKey,
  x25519SharedSecret
} from "./thread_crypto.js";

export {
  DEVICE_CARD_VERSION,
  SEAL_ERROR,
  SEAL_FIXED_BYTES,
  SEAL_MAGIC,
  SEAL_MAX_RECIPIENTS,
  SEAL_RECIPIENT_BYTES,
  SEAL_VERSION,
  THREAD_MAX_DEVICE_NAME_BYTES,
  decodeDeviceCard,
  decodeSealedTicketHeader,
  deviceCardUrl,
  encodeDeviceCard,
  fingerprint,
  generateDeviceKeypair,
  isSealedTicket,
  openSealedTicket,
  parseDeviceCardUrl,
  sealTicket,
  sealedTicketUrl
} from "./thread_seal.js";

export {
  SUBSCRIPTION_BYTES,
  SUBSCRIPTION_FLAG_DELEGATED,
  SUBSCRIPTION_MAGIC,
  SUBSCRIPTION_VERSION,
  decodeManagedSubscription,
  encodeManagedSubscription,
  encodeManagedSubscriptionPreimage,
  issueManagedSubscription,
  openManagedSubscription
} from "./thread_subscription.js";

export {
  ASSIGNMENT_ID_BYTES,
  ASSIGNMENT_MAGIC,
  ASSIGNMENT_MAX_OFFER_BYTES,
  ASSIGNMENT_RECIPIENT_COMMITMENT_BYTES,
  ASSIGNMENT_VERSION,
  JOB_CLAIM_BYTES,
  awardFirstJobClaim,
  createJobClaim,
  decodeAssignmentOffer,
  decodeJobClaim,
  issueMultiRecipientJobOffer,
  openMultiRecipientJobOffer,
  verifyAssignmentOffer,
  verifyJobClaim
} from "./thread_assignment.js";

export {
  BOOTSTRAP_QUERY_KEY,
  DAY_CERT_BYTES,
  DAY_CERT_VERSION,
  LINK_BYTES,
  LINK_FLAG_DELEGATED,
  LINK_VERSION,
  STOP_OUTCOME,
  STOP_REASON,
  THREAD_MAGIC,
  THREAD_MARK_FLAG,
  THREAD_MAX_REASONS,
  THREAD_MAX_PHOTO_LIST,
  THREAD_MAX_BOOTSTRAP_ADDRESSES,
  THREAD_MAX_BOOTSTRAP_BYTES,
  THREAD_MAX_URL_BOOTSTRAP,
  THREAD_MAX_BODY_BYTES,
  THREAD_MAX_NOTE_BYTES,
  THREAD_MAX_RECORD_BYTES,
  THREAD_PHOTO_HASH_BYTES,
  THREAD_MODE,
  THREAD_RECORD_OVERHEAD,
  THREAD_REQUEST_SIZES,
  THREAD_STATE,
  THREAD_TRAVEL_MODE,
  decodeDayCertificate,
  decodePhotoListRequest,
  decodePhotoListResponse,
  decodePhotoRequest,
  decodePhotoResponse,
  decodeThreadBody,
  decodeThreadLink,
  decodeThreadRecord,
  decodeThreadRequest,
  decodeThreadResponse,
  encodeDayCertificate,
  encodeDayCertificatePreimage,
  encodePhotoListRequest,
  encodePhotoListResponse,
  encodePhotoRequest,
  encodePhotoResponse,
  encodeThreadBody,
  encodeThreadBodyPreimage,
  encodeThreadLink,
  encodeThreadRecord,
  encodeThreadRequest,
  encodeThreadResponse,
  fitStopReasons,
  isWithheldPosition,
  normalizeBootstrapAddresses,
  parseBootstrapHint,
  parseThreadLinkUrl,
  stopReasonFor,
  threadBodyMagic,
  threadLinkUrl,
  threadRecordAad,
  withBootstrapHint
} from "./thread_codec.js";

// §21 recurring routes: identity (the root) split from authority (a
// certified, short-lived day key), so a term-long link never moves.
// `mintDayCertificate` is **depot-side** — it takes the route root, which
// must never reach a driver's device. Authority keys are CSPRNG-generated,
// so compromise of one generation cannot expose another.
export {
  CERT_ERROR,
  THREAD_CERT_SKEW,
  THREAD_MAX_CERT_SECONDS,
  assertServiceDay,
  mintDayCertificate,
  routeFollowLink,
  serviceDayOf,
  verifyDayCertificate
} from "./thread_route.js";

export {
  SEED_CARD_VERSION,
  SEED_FIELD,
  SEED_MAGIC,
  THREAD_MAX_SEED_LABEL_BYTES,
  decodeSeedCard,
  encodeSeedCard,
  isDialableAddress,
  parseSeedCardUrl,
  seedCardUrl,
  seedHintUrl
} from "./thread_seed.js";

export {
  JOB_OFFER_VERSION,
  OFFER_FIELD,
  OFFER_GRID_E7,
  OFFER_SPREAD,
  PLAN_VERSION,
  THREAD_MAX_CONTACT_BYTES,
  THREAD_MAX_INSTRUCTIONS_BYTES,
  THREAD_MAX_LABEL_BYTES,
  THREAD_MAX_OFFER_LABEL_BYTES,
  THREAD_MAX_ORDER_REF_BYTES,
  THREAD_STOP_FIELD,
  TICKET_ERROR,
  TICKET_FLAG_BOOTSTRAP,
  TICKET_FLAG_DAY,
  TICKET_FLAG_SEED,
  TICKET_MAGIC,
  TICKET_VERSION,
  awardMatchesOffer,
  classifyThreadArtifact,
  decodeJobOffer,
  decodeThreadPlan,
  decodeThreadTicket,
  encodeThreadPlan,
  issueJobOffer,
  issueSealedTicket,
  issueTicket,
  jobIdHexOf,
  jobIdOf,
  jobOfferUrl,
  planRefOf,
  ticketFollowLink,
  verifyJobOffer,
  verifyThreadTicket
} from "./thread_ticket.js";

export { THREAD_CONSTANTS, createThreadPublisher } from "./thread_publish.js";
export { createThreadChannel } from "./thread_session.js";
export { THREAD_DROP, createThreadSubscriber } from "./thread_consume.js";
export { estimateArrival, scheduledArrival, travelModeName } from "./thread_eta.js";
export {
  applyThreadResponse,
  buildThreadRequest,
  createThreadCache,
  encodeThreadCacheResponse
} from "./thread_cache.js";
export {
  ROUTE_PUBLICITY,
  assertNeverBridged,
  createStopSuppressor,
  resolveContributionPolicy
} from "./thread_contribute.js";
