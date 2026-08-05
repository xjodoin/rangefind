// PulseMesh Threads — the authenticated tracking channel.
//
// Where the traffic channel answers "how fast is this road" from many
// anonymous corroborated observations, this one answers "where is this
// specific vehicle, and when does it reach my stop" for an audience that
// is authorized to know and for nobody else. See docs/pulsemesh-threads.md.
//
// A run's whole capability is one Ed25519 public key, 45 bytes in a URL
// fragment. The private key signs, so no link holder can impersonate the
// publisher; the public key derives the content key, the rotating topic
// tag, and the DHT rendezvous key, so holding the link is what lets you
// find, decrypt, and verify the thread. Nothing else is in the link — no
// host, no mailbox, no bootstrap address.
//
//   const publisher = await createThreadPublisher({ privateSeed, epoch32, plan });
//   const url = threadLinkUrl("https://track.example/r", encodeThreadLink({
//     publicKey: publisher.publicKey, epochPrefix8, notAfter
//   }));
//   // …the subscriber, holding only that URL:
//   const subscriber = await createThreadSubscriber({ link: parseThreadLinkUrl(url), epoch32 });
//   const eta = await estimateArrival({ engine, update: subscriber.latest(), plan, myStopIndex, live });

export {
  THREAD_TOPIC_PREFIX,
  base64UrlToBytes,
  bytesToBase64Url,
  deriveThreadKeys,
  generateThreadKeypair,
  openThreadBody,
  publicKeyFromSeed,
  sealThreadBody,
  setThreadCryptoImplementation,
  signThread,
  threadNonce,
  threadRendezvous,
  threadTag,
  threadTagsForWindows,
  threadTopic,
  threadWindow,
  verifyThread
} from "./thread_crypto.js";

export {
  LINK_BYTES,
  LINK_VERSION,
  THREAD_MAGIC,
  THREAD_MAX_NOTE_BYTES,
  THREAD_MAX_RECORD_BYTES,
  THREAD_MODE,
  THREAD_REQUEST_SIZES,
  THREAD_STATE,
  decodeThreadBody,
  decodeThreadLink,
  decodeThreadRecord,
  decodeThreadRequest,
  decodeThreadResponse,
  encodeThreadBody,
  encodeThreadBodyPreimage,
  encodeThreadLink,
  encodeThreadRecord,
  encodeThreadRequest,
  encodeThreadResponse,
  isWithheldPosition,
  parseThreadLinkUrl,
  threadLinkUrl,
  threadRecordAad
} from "./thread_codec.js";

export { THREAD_CONSTANTS, createThreadPublisher } from "./thread_publish.js";
export { createThreadSubscriber } from "./thread_consume.js";
export { estimateArrival, scheduledArrival } from "./thread_eta.js";
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
