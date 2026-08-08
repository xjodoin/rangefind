package dev.rangefind.wayfind.region

import dev.rangefind.wayfind.engine.AwardCheck
import dev.rangefind.wayfind.engine.OfferSummary
import org.json.JSONArray
import org.json.JSONObject

/**
 * An offer this device has bid on (threads §20.4).
 *
 * Held because the commitment is worth nothing at the moment it is made:
 * the award turns up hours later, on a different day, after a process
 * death or two, and the whole point of `planRef` is that the courier's
 * device can compare *the plan that arrived* against *the plan that was
 * advertised* before a wheel turns. A bid the phone forgets is a
 * dispatcher free to advertise three stops downtown and award thirty
 * across the province.
 *
 * [offerBase64] is the artifact itself, kept for exactly that check —
 * `awardMatchesOffer` compares bytes, not summaries. Holding it costs
 * nothing: an offer is public and unsealed by design, carries no seed,
 * no address and no customer, and grants its holder nothing at all.
 *
 * The coarse fields beside it exist so the bid can be *shown* — with the
 * offer's own words, months of forgetting later — without decoding
 * anything.
 */
data class HeldOffer(
    val jobIdHex: String,
    /** The commitment the award will be checked against. */
    val planRefHex: String,
    val issuerHex: String,
    /** Absolute expiry, unix seconds. What makes a bid prunable. */
    val notAfter: Long,
    val stopCount: Int,
    val totalMeters: Long,
    val travelModeName: String? = null,
    val centroidLat: Double,
    val centroidLon: Double,
    val gridDegrees: Double,
    val spread: Int,
    val payMinor: Long? = null,
    val currency: String? = null,
    val label: String? = null,
    /** The offer as it arrived, base64url. */
    val offerBase64: String,
    /** When this device sent its card, in device milliseconds. */
    val bidAt: Long
)

/** The bid a courier is about to record, from the offer they were shown. */
fun heldOfferOf(offer: OfferSummary, bidAt: Long): HeldOffer = HeldOffer(
    jobIdHex = offer.jobIdHex,
    planRefHex = offer.planRefHex,
    issuerHex = offer.issuerHex,
    notAfter = offer.notAfter,
    stopCount = offer.stopCount,
    totalMeters = offer.totalMeters,
    travelModeName = offer.travelModeName,
    centroidLat = offer.centroidLat,
    centroidLon = offer.centroidLon,
    gridDegrees = offer.gridDegrees,
    spread = offer.spread,
    payMinor = offer.payMinor,
    currency = offer.currency,
    label = offer.label,
    offerBase64 = offer.offerBase64,
    bidAt = bidAt
)

/**
 * The bids as one preference string, in the roster's own style.
 *
 * JSON rather than a key per bid for the same reason the roster is: the
 * list is variable length, so a key run cannot be replaced atomically,
 * and a half-written list of bids is a phone that would check tomorrow's
 * award against a commitment it only partly remembers.
 */
fun heldOffersToJson(offers: List<HeldOffer>): String {
    val list = JSONArray()
    for (offer in offers) {
        list.put(
            JSONObject()
                .put(KEY_OFFER_JOB_ID, offer.jobIdHex)
                .put(KEY_OFFER_PLAN_REF, offer.planRefHex)
                .put(KEY_OFFER_ISSUER, offer.issuerHex)
                .put(KEY_OFFER_NOT_AFTER, offer.notAfter)
                .put(KEY_OFFER_STOP_COUNT, offer.stopCount)
                .put(KEY_OFFER_TOTAL_METERS, offer.totalMeters)
                .put(KEY_OFFER_CENTROID_LAT, offer.centroidLat)
                .put(KEY_OFFER_CENTROID_LON, offer.centroidLon)
                .put(KEY_OFFER_GRID, offer.gridDegrees)
                .put(KEY_OFFER_SPREAD, offer.spread)
                .put(KEY_OFFER_BYTES, offer.offerBase64)
                .put(KEY_OFFER_BID_AT, offer.bidAt)
                // Absent rather than empty when the dispatcher did not
                // say: `""` would be a stated blank label and `0` a
                // stated pay of nothing, and both are claims nobody made.
                .apply {
                    offer.travelModeName?.let { put(KEY_OFFER_TRAVEL_MODE, it) }
                    offer.payMinor?.let { put(KEY_OFFER_PAY, it) }
                    offer.currency?.let { put(KEY_OFFER_CURRENCY, it) }
                    offer.label?.let { put(KEY_OFFER_LABEL, it) }
                }
        )
    }
    return JSONObject().put(KEY_OFFERS, list).toString()
}

/**
 * The bids back out again, with the dead ones dropped.
 *
 * Pruning on the way out rather than on a timer: an expired offer is not
 * a bid the courier is still waiting on, it is a job that can no longer
 * be awarded to anybody, and keeping it would leave a card on screen
 * that no dispatcher can act on.
 *
 * An entry with no expiry, no job id or no bytes is dropped as well.
 * None of the three can do the one thing a held bid exists for — a bid
 * with no expiry could never be pruned, and one with no bytes cannot be
 * checked against an award — so keeping it would only mean showing a row
 * that silently fails at the moment it is needed.
 *
 * Defensive throughout, like every other stored artifact here: this
 * string was written by an older build as often as by this one.
 */
fun heldOffersFromJson(
    text: String?,
    nowMillis: Long = System.currentTimeMillis()
): List<HeldOffer> {
    if (text.isNullOrBlank()) return emptyList()
    return runCatching {
        val list = JSONObject(text).optJSONArray(KEY_OFFERS) ?: JSONArray()
        (0 until list.length()).mapNotNull { position ->
            val item = list.optJSONObject(position) ?: return@mapNotNull null
            val jobId = item.optString(KEY_OFFER_JOB_ID).takeIf { it.isNotBlank() }
                ?: return@mapNotNull null
            val bytes = item.optString(KEY_OFFER_BYTES).takeIf { it.isNotBlank() }
                ?: return@mapNotNull null
            val notAfter = item.optLong(KEY_OFFER_NOT_AFTER)
            if (notAfter <= 0L || notAfter * 1000L <= nowMillis) return@mapNotNull null
            HeldOffer(
                jobIdHex = jobId,
                planRefHex = item.optString(KEY_OFFER_PLAN_REF),
                issuerHex = item.optString(KEY_OFFER_ISSUER),
                notAfter = notAfter,
                stopCount = item.optInt(KEY_OFFER_STOP_COUNT),
                totalMeters = item.optLong(KEY_OFFER_TOTAL_METERS),
                travelModeName = item.offerText(KEY_OFFER_TRAVEL_MODE),
                centroidLat = item.optDouble(KEY_OFFER_CENTROID_LAT, 0.0),
                centroidLon = item.optDouble(KEY_OFFER_CENTROID_LON, 0.0),
                gridDegrees = item.optDouble(KEY_OFFER_GRID, 0.0),
                spread = item.optInt(KEY_OFFER_SPREAD),
                // A missing key is a dispatcher who stated no pay, and an
                // absent pay must never come back as a stated zero.
                payMinor = if (item.has(KEY_OFFER_PAY) && !item.isNull(KEY_OFFER_PAY)) {
                    item.optLong(KEY_OFFER_PAY)
                } else {
                    null
                },
                currency = item.offerText(KEY_OFFER_CURRENCY),
                label = item.offerText(KEY_OFFER_LABEL),
                offerBase64 = bytes,
                bidAt = item.optLong(KEY_OFFER_BID_AT)
            )
        }
    }.getOrDefault(emptyList())
}

/** A stored optional field, where unstated has exactly one form: null. */
private fun JSONObject.offerText(key: String): String? =
    if (isNull(key)) null else optString(key).takeIf { it.isNotBlank() }

/**
 * The bids with [offer] among them, replacing any earlier bid on the
 * same job.
 *
 * Keyed on the job id: a dispatcher who re-posts the same job — a fresh
 * QR in the depot, the same offer forwarded twice through a group chat —
 * must not leave two rows that are one commitment.
 */
fun List<HeldOffer>.withOffer(offer: HeldOffer): List<HeldOffer> =
    filterNot { it.jobIdHex == offer.jobIdHex } + offer

fun List<HeldOffer>.withoutOffer(jobIdHex: String): List<HeldOffer> =
    filterNot { it.jobIdHex == jobIdHex }

private const val KEY_OFFERS = "offers"
private const val KEY_OFFER_JOB_ID = "jobIdHex"
private const val KEY_OFFER_PLAN_REF = "planRefHex"
private const val KEY_OFFER_ISSUER = "issuerHex"
private const val KEY_OFFER_NOT_AFTER = "notAfter"
private const val KEY_OFFER_STOP_COUNT = "stopCount"
private const val KEY_OFFER_TOTAL_METERS = "totalMeters"
private const val KEY_OFFER_TRAVEL_MODE = "travelModeName"
private const val KEY_OFFER_CENTROID_LAT = "centroidLat"
private const val KEY_OFFER_CENTROID_LON = "centroidLon"
private const val KEY_OFFER_GRID = "gridDegrees"
private const val KEY_OFFER_SPREAD = "spread"
private const val KEY_OFFER_PAY = "payMinor"
private const val KEY_OFFER_CURRENCY = "currency"
private const val KEY_OFFER_LABEL = "label"
private const val KEY_OFFER_BYTES = "offer"
private const val KEY_OFFER_BID_AT = "bidAt"

/**
 * How an award stands against the bids this device is holding.
 *
 * [MatchesBid] is worth saying out loud rather than passing over in
 * silence: a check that passed is the one piece of evidence the courier
 * has that the day they are about to drive is the day they priced.
 *
 * [MismatchedBid] and [UnrecognizedJob] are the two halves of the
 * bait-and-switch §20.4 describes, and they are deliberately not the same
 * outcome — see [awardVerdictFor].
 */
enum class AwardBidVerdict { NoBids, MatchesBid, MismatchedBid, UnrecognizedJob }

/**
 * The verdict, plus what it is about. [bid] is the offer that decided it,
 * so the sheet can say *which* job in the courier's own terms; [reason]
 * is `awardMatchesOffer`'s named check.
 */
data class AwardBid(
    val verdict: AwardBidVerdict,
    val reason: String? = null,
    val bid: HeldOffer? = null,
    /** How many bids were open when this was decided. */
    val heldCount: Int = 0
) {
    /** Whether this award may be taken at all. */
    val refuses: Boolean get() = verdict == AwardBidVerdict.MismatchedBid
}

/**
 * What a courier's own device makes of an award, given what they bid on.
 *
 * Three outcomes, and the difference between the last two is the whole
 * design:
 *
 * - A bid whose commitment the award **satisfies** — same issuer, same
 *   epoch, same `planRef`, no later expiry, same `jobId` — is the job
 *   that was advertised, and the courier is told so.
 * - A bid with the **same `jobId`** whose commitment the award fails is
 *   unambiguous: two artifacts claiming to be one job that are not one
 *   job. Nothing legitimate produces it — `jobId` hashes the plan bytes,
 *   so an honest change of plan changes the id too — so this is refused
 *   outright, naming the check that failed.
 * - Held bids that the award matches **none** of is the case the core
 *   warns about, and it is genuinely ambiguous. A swapped plan moves the
 *   `jobId`, so a swap arrives looking exactly like this — but so does a
 *   second, unrelated dispatch, which is an ordinary Tuesday. Refusing
 *   would make the app unusable for any courier who works for more than
 *   one dispatcher; staying quiet would hand the swap through. So it
 *   warns, prominently, and leaves the driver an informed choice.
 * - No bids at all is a direct dispatch, and behaves exactly as it did
 *   before offers existed: silently.
 *
 * Pure, and separate from the view model, because it is the one place
 * this decision lives — [checks] is `awardMatchesOffer`'s answer per held
 * bid, aligned to [held] by [AwardCheck.index].
 */
fun awardVerdictFor(
    held: List<HeldOffer>,
    awardJobIdHex: String,
    checks: List<AwardCheck>
): AwardBid {
    if (held.isEmpty()) return AwardBid(AwardBidVerdict.NoBids)
    val byIndex = checks.associateBy { it.index }
    held.forEachIndexed { position, offer ->
        if (byIndex[position]?.ok == true) {
            return AwardBid(AwardBidVerdict.MatchesBid, bid = offer, heldCount = held.size)
        }
    }
    val sameJob = held.indexOfFirst {
        awardJobIdHex.isNotBlank() && it.jobIdHex.equals(awardJobIdHex, ignoreCase = true)
    }
    if (sameJob >= 0) {
        return AwardBid(
            AwardBidVerdict.MismatchedBid,
            // The core orders its checks so a swapped plan reads as
            // `planRef`. Null only when no check came back for this bid
            // at all, and the sheet then says what it does know.
            reason = byIndex[sameJob]?.reason,
            bid = held[sameJob],
            heldCount = held.size
        )
    }
    return AwardBid(AwardBidVerdict.UnrecognizedJob, heldCount = held.size)
}
