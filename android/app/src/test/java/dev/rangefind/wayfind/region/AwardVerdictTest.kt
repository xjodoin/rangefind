package dev.rangefind.wayfind.region

import dev.rangefind.wayfind.engine.AwardCheck
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * What a courier's own device makes of an award, given what they bid on
 * (threads §20.4).
 *
 * This is where the commitment is spent. Without it an offer is a
 * marketing line: a dispatcher advertises three stops downtown, the pool
 * bids, and the winner opens a ticket with thirty drops across the
 * province — by which point they have agreed, the customers have been
 * told, and the argument is theirs to lose. The three cases below are
 * deliberately three different outcomes, and collapsing any two of them
 * either makes the app unusable for a courier with two dispatchers or
 * waves the swap through.
 */
class AwardVerdictTest {

    private fun bid(jobIdHex: String, label: String? = null) = HeldOffer(
        jobIdHex = jobIdHex,
        planRefHex = "0011223344556677",
        issuerHex = "aabbccdd",
        notAfter = 2_000_000_000L,
        stopCount = 3,
        totalMeters = 8_000,
        travelModeName = "bike",
        centroidLat = 45.51,
        centroidLon = -73.57,
        gridDegrees = 0.01,
        spread = 1,
        payMinor = 2500,
        currency = "CAD",
        label = label,
        offerBase64 = "UE1KMQ-$jobIdHex",
        bidAt = 1_800_000_000_000L
    )

    @Test
    fun `no bids at all is a direct dispatch and says nothing`() {
        // The ordinary case, and the one that must behave exactly as it
        // did before offers existed: a dispatcher who sealed a job
        // straight to this device never advertised anything, and there is
        // no comparison to report.
        val verdict = awardVerdictFor(emptyList(), "9f1c33aa", emptyList())

        assertEquals(AwardBidVerdict.NoBids, verdict.verdict)
        assertFalse(verdict.refuses)
        assertNull(verdict.bid)
    }

    @Test
    fun `an award that satisfies a bid is said out loud`() {
        // A check that passed is the only evidence the courier has that
        // the day in front of them is the day they priced, and silence
        // about it is silence about the whole point of the offer.
        val held = listOf(bid("aaa1"), bid("9f1c33aa", label = "12 colis"))
        val checks = listOf(
            AwardCheck(index = 0, ok = false, reason = "the award is a different job from the one offered (jobId)"),
            AwardCheck(index = 1, ok = true, reason = null)
        )

        val verdict = awardVerdictFor(held, "9f1c33aa", checks)

        assertEquals(AwardBidVerdict.MatchesBid, verdict.verdict)
        assertFalse(verdict.refuses)
        assertEquals("9f1c33aa", verdict.bid?.jobIdHex)
        assertEquals("12 colis", verdict.bid?.label)
    }

    @Test
    fun `the same job id with a broken commitment is refused, naming the check`() {
        // Two artifacts claiming to be one job that are not one job.
        // Nothing honest produces this — `jobId` hashes the plan bytes,
        // so a real change of plan changes the id with it — and the core
        // orders its checks so the courier is told *planRef*, which says
        // the plan was swapped, rather than "this does not match", which
        // says nothing they can argue with.
        val held = listOf(bid("9f1c33aa"))
        val checks = listOf(
            AwardCheck(
                index = 0,
                ok = false,
                reason = "the awarded plan is not the plan the offer committed to (planRef)"
            )
        )

        val verdict = awardVerdictFor(held, "9F1C33AA", checks)

        assertEquals(AwardBidVerdict.MismatchedBid, verdict.verdict)
        assertTrue(verdict.refuses)
        assertEquals(
            "the awarded plan is not the plan the offer committed to (planRef)",
            verdict.reason
        )
        assertEquals("9f1c33aa", verdict.bid?.jobIdHex)
    }

    @Test
    fun `a refusal with no named check still refuses`() {
        // A check that never came back — a bid this build could not
        // decode, an engine call that failed — is not a reason to take
        // the job. The verdict stands and the sheet says what it knows.
        val verdict = awardVerdictFor(listOf(bid("9f1c33aa")), "9f1c33aa", emptyList())

        assertEquals(AwardBidVerdict.MismatchedBid, verdict.verdict)
        assertTrue(verdict.refuses)
        assertNull(verdict.reason)
    }

    @Test
    fun `an award matching none of the open bids warns rather than refusing`() {
        // The case the core warns about, and the genuinely ambiguous one:
        // a swapped plan moves the `jobId` too, so the swap arrives
        // looking exactly like this — and so does a second, unrelated
        // dispatch, which is an ordinary Tuesday for a courier who works
        // for two dispatchers. Refusing would break that courier's app;
        // silence would hand the swap through. So it warns, and says how
        // many bids it compared against.
        val held = listOf(bid("aaa1"), bid("bbb2"))
        val checks = listOf(
            AwardCheck(index = 0, ok = false, reason = "the award is a different job from the one offered (jobId)"),
            AwardCheck(index = 1, ok = false, reason = "the award is a different job from the one offered (jobId)")
        )

        val verdict = awardVerdictFor(held, "cccc3", checks)

        assertEquals(AwardBidVerdict.UnrecognizedJob, verdict.verdict)
        assertFalse(verdict.refuses)
        assertEquals(2, verdict.heldCount)
        assertNull(verdict.bid)
    }

    @Test
    fun `a bid that could not be decoded holds its place in the list`() {
        // The checks come back indexed so an unreadable bid occupies its
        // own slot: dropping it would shift every later verdict onto the
        // wrong job, which is how a passing check ends up attributed to
        // an offer nobody made.
        val held = listOf(bid("aaa1"), bid("9f1c33aa"))
        val checks = listOf(
            AwardCheck(index = 0, ok = false, reason = "Unsupported PMJ1 offer version 9."),
            AwardCheck(index = 1, ok = true, reason = null)
        )

        val verdict = awardVerdictFor(held, "9f1c33aa", checks)

        assertEquals(AwardBidVerdict.MatchesBid, verdict.verdict)
        assertEquals("9f1c33aa", verdict.bid?.jobIdHex)
    }

    @Test
    fun `an award with no job id at all cannot claim to be a bid`() {
        // A ticket whose id did not survive the bridge matches nothing by
        // id, so it lands in the warned case rather than being refused
        // against a bid it was never compared to.
        val verdict = awardVerdictFor(
            listOf(bid("9f1c33aa")),
            "",
            listOf(AwardCheck(index = 0, ok = false, reason = "the award is a different job from the one offered (jobId)"))
        )

        assertEquals(AwardBidVerdict.UnrecognizedJob, verdict.verdict)
        assertFalse(verdict.refuses)
    }
}
