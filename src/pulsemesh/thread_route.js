// Recurring routes (threads §21): identity split from authority.
//
// A recurring route keeps one verification identity and capability for
// a term while its publishing authority changes with the driver:
//
//   identity   = the long-lived root Ed25519 verification key;
//   capability = a separate secret for discovery, admission and content;
//   authority  = a CSPRNG-generated, short-lived day key certified by the
//                root and scoped to one plan and generation.
//
// Everything here is depot-side. `mintDayCertificate` takes the root
// seed; nothing that runs on a
// driver's phone may call them, and `createThreadPublisher` refuses a
// root seed on a route run for exactly that reason. A phone that held
// the root would be a phone whose compromise costs the whole term, which
// is the failure short-lived certificates exist to prevent.

import {
  DAY_CERT_VERSION,
  decodeDayCertificate,
  encodeDayCertificate,
  encodeDayCertificatePreimage,
  encodeThreadLink
} from "./thread_codec.js";
import {
  deriveThreadSecret,
  publicKeyFromSeed,
  signThread,
  verifyThread
} from "./thread_crypto.js";

/**
 * The longest certificate window a **subscriber** will honour: 48 hours.
 *
 * This is a bound the verifier applies, not one the issuer is trusted to
 * respect. If it lived only in `mintDayCertificate` a root could mint
 * `notAfter = 2038` and the day key would be an effectively permanent
 * publish credential — at which point the whole split has bought
 * nothing, because a compromised driver phone is back to owning the
 * route. Refusing it at the subscriber is what makes "short-lived" a
 * property of the protocol rather than of the depot's good manners.
 *
 * 48 rather than 24 because a service day is not a calendar day: a route
 * whose last run lands at 01:20 needs a window that started the previous
 * morning, and a depot minting tomorrow's certificate this afternoon
 * wants it live before the driver's phone is out of the charger. Two
 * days is the smallest number that covers both without argument, and it
 * still means a leaked day seed is worthless by the day after tomorrow.
 *
 * A host may *tighten* this through `constants.THREAD_MAX_CERT_SECONDS`;
 * the subscriber takes the minimum of the two, so no configuration can
 * loosen it.
 */
export const THREAD_MAX_CERT_SECONDS = 172800;

/**
 * Clock tolerance on a certificate's own window, both edges, in seconds.
 *
 * Deliberately not `THREAD_MAX_FUTURE_SKEW` (15 s), which bounds how
 * fresh a *position* is and is tight because a stale position rendered
 * as live is the harm there. This bounds a window measured in hours, so
 * five minutes of grace costs nothing against the 48-hour cap and covers
 * the phone that has been in a drawer with no NTP since Friday. Being
 * mean here has one failure mode and it is the bad one: a parent whose
 * clock is four minutes slow is told the bus is not being tracked.
 */
export const THREAD_CERT_SKEW = 300;

/** Why a certificate was refused. Stable strings — hosts branch on these. */
export const CERT_ERROR = Object.freeze({
  VERSION: "cert-version",
  FOREIGN_ROOT: "cert-foreign-root",
  EMPTY_WINDOW: "cert-empty-window",
  WINDOW_TOO_LONG: "cert-window-too-long",
  BAD_SIGNATURE: "cert-bad-signature",
  NOT_YET_VALID: "cert-not-yet-valid",
  EXPIRED: "cert-expired"
});

/**
 * A service day is the operator's **civil date for the run**, packed as
 * the decimal integer `YYYYMMDD` (2026-09-08 → 20260908).
 *
 * The timezone question has to be answered out loud, because a 07:00
 * local departure straddles UTC midnight for a good part of the world
 * and a bus that runs 23:10–01:20 straddles local midnight too. The
 * answer: **the operator chooses it, in the route's own local
 * timezone, and the protocol never computes it from a clock.** The
 * depot's timetable already has a name for the day a run belongs to —
 * that is the number that goes here — and a night run keeps the civil
 * date it *departed* on, so one service day is one contiguous block of
 * work and never two.
 *
 * That choice is safe to leave to the operator because `serviceDay` is
 * never compared against anybody's clock. It is an HKDF input and a
 * display label, and nothing else; the only time-of-validity claim on
 * the wire is `notBefore`/`notAfter` in absolute unix seconds, which are
 * unambiguous everywhere. So a depot in Montréal and a parent's phone
 * roaming in Lisbon can disagree completely about what day it is and
 * still agree, to the second, about whether this certificate is live.
 */
export function assertServiceDay(serviceDay) {
  if (!Number.isInteger(serviceDay)) {
    throw new Error(`A serviceDay is the integer YYYYMMDD; got ${serviceDay}.`);
  }
  const year = Math.floor(serviceDay / 10000);
  const month = Math.floor(serviceDay / 100) % 100;
  const day = serviceDay % 100;
  if (year < 1970 || year > 9999 || month < 1 || month > 12 || day < 1 || day > 31) {
    throw new Error(`A serviceDay is the operator's civil date as YYYYMMDD; got ${serviceDay}.`);
  }
  return serviceDay;
}

/** `YYYYMMDD` from calendar parts the operator already has. */
export function serviceDayOf(year, month, day) {
  return assertServiceDay(year * 10000 + month * 100 + day);
}

/**
 * Mints one day's authority. **Depot-side**: this is the only function
 * in the channel that takes a root seed and produces something to send.
 *
 * What it returns is exactly what travels to the driver — a day seed and
 * the certificate that vouches for it — and deliberately nothing else.
 * There is no option meaning "and also the root", because the point of
 * the split is that no such payload exists.
 *
 * The window is checked here as well as at the subscriber, so a depot
 * that fat-fingers `notAfter` finds out at mint time rather than by
 * having a whole term's buses silently unverifiable.
 */
export async function mintDayCertificate({
  rootSeed,
  planRef = null,
  serviceDay,
  generation = serviceDay,
  daySeed = null,
  notBefore,
  notAfter,
  maxSeconds = THREAD_MAX_CERT_SECONDS
}) {
  if (rootSeed?.length !== 32) throw new Error("A route root is a 32-byte Ed25519 seed.");
  assertServiceDay(serviceDay);
  const scopedPlanRef = planRef ?? new Uint8Array(8);
  if (scopedPlanRef.length !== 8) throw new Error("A route planRef is 8 bytes.");
  if (!Number.isInteger(generation) || generation < 1 || generation > 0xffffffff) {
    throw new Error("A day authority generation is a positive uint32.");
  }
  for (const [name, value] of [["notBefore", notBefore], ["notAfter", notAfter]]) {
    if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
      throw new Error(`A day certificate's ${name} is unix seconds as a uint32; got ${value}.`);
    }
  }
  if (notAfter <= notBefore) throw new Error("A day certificate's window is empty.");
  if (notAfter - notBefore > maxSeconds) {
    throw new Error(
      `That certificate window is ${Math.round((notAfter - notBefore) / 3600)} h and a subscriber `
      + `refuses anything over ${Math.round(maxSeconds / 3600)} h: a day key that outlives the day `
      + "is a root key with extra steps."
    );
  }
  const authoritySeed = daySeed ?? globalThis.crypto.getRandomValues(new Uint8Array(32));
  if (authoritySeed.length !== 32) throw new Error("A day authority seed is 32 bytes.");
  const [rootPublicKey, dayPublicKey, threadSecret] = await Promise.all([
    publicKeyFromSeed(rootSeed),
    publicKeyFromSeed(authoritySeed),
    deriveThreadSecret(rootSeed)
  ]);
  const fields = {
    version: DAY_CERT_VERSION,
    rootPublicKey,
    dayPublicKey,
    generation,
    serviceDay,
    planRef: scopedPlanRef,
    notBefore,
    notAfter
  };
  const signature = await signThread(encodeDayCertificatePreimage(fields), rootSeed);
  return {
    daySeed: authoritySeed,
    certificate: decodeDayCertificate(encodeDayCertificate(fields, signature)),
    rootPublicKey,
    dayPublicKey,
    threadSecret
  };
}

/**
 * The subscriber's side of the chain, and the security-critical half.
 *
 * Order is cheap-to-expensive with one exception: the root identity
 * check runs before the signature, so a certificate minted by some other
 * route is refused *by name* rather than as an anonymous bad signature.
 * A depot that pasted the wrong route's certificate into tomorrow's
 * dispatch should be told which mistake it made.
 */
export async function verifyDayCertificate(certificate, {
  rootPublicKey,
  nowSeconds = Math.floor(Date.now() / 1000),
  maxSeconds = THREAD_MAX_CERT_SECONDS,
  skew = THREAD_CERT_SKEW
} = {}) {
  const cert = certificate instanceof Uint8Array ? decodeDayCertificate(certificate) : certificate;
  if (rootPublicKey?.length !== 32) throw new Error("Verifying a day certificate needs the route root's public key.");
  if (cert.version !== DAY_CERT_VERSION) {
    return { ok: false, code: CERT_ERROR.VERSION, reason: `unsupported day certificate version ${cert.version}` };
  }
  for (let i = 0; i < 32; i++) {
    if (cert.rootPublicKey[i] !== rootPublicKey[i]) {
      return {
        ok: false,
        code: CERT_ERROR.FOREIGN_ROOT,
        reason: "this certificate was issued by a different route root"
      };
    }
  }
  if (cert.notAfter <= cert.notBefore) {
    return { ok: false, code: CERT_ERROR.EMPTY_WINDOW, reason: "the certificate's window is empty" };
  }
  const span = cert.notAfter - cert.notBefore;
  if (span > maxSeconds) {
    return {
      ok: false,
      code: CERT_ERROR.WINDOW_TOO_LONG,
      reason: `the certificate is valid for ${Math.round(span / 3600)} h and the bound is `
        + `${Math.round(maxSeconds / 3600)} h`
    };
  }
  if (!(await verifyThread(cert.preimage, cert.signature, rootPublicKey))) {
    return {
      ok: false,
      code: CERT_ERROR.BAD_SIGNATURE,
      reason: "the certificate's signature does not verify against the route root"
    };
  }
  if (nowSeconds + skew < cert.notBefore) {
    return { ok: false, code: CERT_ERROR.NOT_YET_VALID, reason: "the certificate is not valid yet" };
  }
  if (nowSeconds - skew > cert.notAfter) {
    return { ok: false, code: CERT_ERROR.EXPIRED, reason: "the certificate has expired" };
  }
  return { ok: true, code: null, reason: null, certificate: cert };
}

/**
 * The term-long follow link: a delegated capability with the route root
 * as its stable verification identity.
 *
 * Minted once, by the depot, at the start of the term. `notAfter` is the
 * end of the term rather than the end of a run — that is the whole
 * feature, and it is safe precisely because the root cannot publish
 * anything a subscriber will accept without a certificate whose own
 * window is at most 48 h.
 */
export function routeFollowLink({ threadSecret, rootPublicKey, epochPrefix8, notAfter }) {
  return encodeThreadLink({
    threadSecret, rootPublicKey, epochPrefix8, notAfter, delegated: true
  });
}
