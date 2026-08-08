package dev.rangefind.wayfind.region

import dev.rangefind.wayfind.engine.OfferSummary
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * The bids on disk (threads §20.4).
 *
 * A bid is a commitment held against the future: the award turns up hours
 * later, after a phone call, a low-memory kill or a night, and the whole
 * worth of `planRef` is that the courier's own device can then compare the
 * job that arrived against the job that was advertised. A bid the phone
 * forgets is a dispatcher free to advertise three stops downtown and award
 * thirty across the province — so what this file protects is that the
 * commitment survives the process, and that a dead one does not linger.
 */
class HeldOfferJsonTest {

    private val hour = 3_600_000L
    private val now = 1_800_000_000_000L

    private fun offer(
        jobIdHex: String = "9f1c33aa",
        notAfter: Long = now / 1000 + 3600,
        payMinor: Long? = 4250,
        currency: String? = "CAD",
        label: String? = "12 colis, centre-ville",
        bidAt: Long = now - hour
    ) = HeldOffer(
        jobIdHex = jobIdHex,
        planRefHex = "0011223344556677",
        issuerHex = "aabbccdd",
        notAfter = notAfter,
        stopCount = 12,
        totalMeters = 41_320,
        travelModeName = "bike",
        centroidLat = 45.51,
        centroidLon = -73.57,
        gridDegrees = 0.01,
        spread = 2,
        payMinor = payMinor,
        currency = currency,
        label = label,
        offerBase64 = "UE1KMS1vZmZlci1ieXRlcw",
        bidAt = bidAt
    )

    @Test
    fun `a bid survives the round trip field for field`() {
        val original = listOf(offer(), offer(jobIdHex = "5e2200ff", label = null, payMinor = null, currency = null))

        val restored = heldOffersFromJson(heldOffersToJson(original), now)

        assertEquals(original, restored)
        // The bytes are the point: `awardMatchesOffer` compares the offer
        // itself, not a summary of it, so a lost or mangled capability is
        // a commitment that can no longer be checked at all.
        assertEquals("UE1KMS1vZmZlci1ieXRlcw", restored[0].offerBase64)
        assertEquals("0011223344556677", restored[0].planRefHex)
    }

    @Test
    fun `a pay of nothing is a statement and an absent pay is silence`() {
        // §20.4 flags the pay field precisely so the two can be told
        // apart. A dispatcher who named no figure has not offered zero,
        // and a courier shown "0" for silence is being told something
        // nobody said.
        val stated = heldOffersFromJson(heldOffersToJson(listOf(offer(payMinor = 0, currency = "EUR"))), now)
        assertEquals(0L, stated[0].payMinor)
        assertEquals("EUR", stated[0].currency)

        val silent = heldOffersFromJson(
            heldOffersToJson(listOf(offer(payMinor = null, currency = null))),
            now
        )
        assertNull(silent[0].payMinor)
        assertNull(silent[0].currency)
    }

    @Test
    fun `an expired bid is dropped on the way back in`() {
        val live = offer(jobIdHex = "live0001", notAfter = now / 1000 + 60)
        val dead = offer(jobIdHex = "dead0001", notAfter = now / 1000 - 1)
        // Exactly at the expiry is expired: `notAfter` is the last second
        // the job is valid *through* on the wire, and the honest reading
        // of "now is past it" includes the boundary rather than leaving a
        // bid alive for a job nobody can be awarded.
        val edge = offer(jobIdHex = "edge0001", notAfter = now / 1000)

        val restored = heldOffersFromJson(heldOffersToJson(listOf(live, dead, edge)), now)

        assertEquals(listOf("live0001"), restored.map { it.jobIdHex })
    }

    @Test
    fun `a bid that cannot do its job is not kept`() {
        // No expiry can never be pruned, no job id can never be matched,
        // and no bytes can never be checked against an award. Each would
        // put a row on screen that fails silently at the one moment it is
        // needed.
        val broken = """
            {"offers":[
              {"planRefHex":"00","notAfter":${now / 1000 + 60},"offer":"aGk"},
              {"jobIdHex":"aa","offer":"aGk"},
              {"jobIdHex":"bb","notAfter":${now / 1000 + 60}},
              {"jobIdHex":"cc","notAfter":${now / 1000 + 60},"offer":"aGk"}
            ]}
        """.trimIndent()

        val restored = heldOffersFromJson(broken, now)

        assertEquals(listOf("cc"), restored.map { it.jobIdHex })
        // A file from before some of these fields existed reads as
        // unstated rather than as a claim.
        assertNull(restored[0].label)
        assertNull(restored[0].travelModeName)
    }

    @Test
    fun `nothing usable reads as no bids rather than a crash`() {
        assertTrue(heldOffersFromJson(null, now).isEmpty())
        assertTrue(heldOffersFromJson("", now).isEmpty())
        assertTrue(heldOffersFromJson("not json", now).isEmpty())
        assertTrue(heldOffersFromJson("""{"offers":[]}""", now).isEmpty())
    }

    @Test
    fun `re-bidding on one job replaces its row instead of doubling it`() {
        // The same offer forwarded twice through a group chat, or a fresh
        // QR of it in the depot, is one commitment — and two rows for it
        // would mean two bids checked against one award.
        val first = offer(jobIdHex = "9f1c33aa", bidAt = now - 2 * hour)
        val again = offer(jobIdHex = "9f1c33aa", bidAt = now)
        val other = offer(jobIdHex = "5e2200ff")

        val held = listOf(first).withOffer(other).withOffer(again)

        assertEquals(2, held.size)
        assertEquals(listOf("5e2200ff", "9f1c33aa"), held.map { it.jobIdHex })
        assertEquals(now, held.last().bidAt)
        assertEquals(listOf("5e2200ff"), held.withoutOffer("9f1c33aa").map { it.jobIdHex })
    }

    @Test
    fun `a bid is made of what the offer said and nothing else`() {
        val summary = OfferSummary(
            offerBase64 = "UE1KMQ",
            jobIdHex = "9f1c33aa",
            planRefHex = "0011223344556677",
            stopCount = 12,
            travelModeName = "bike",
            fine = true,
            centroidLat = 45.51,
            centroidLon = -73.57,
            gridDegrees = 0.01,
            spread = 2,
            spreadMaxMeters = 10_000,
            totalMeters = 41_320,
            payMinor = 4250,
            currency = "CAD",
            label = "12 colis, centre-ville",
            notAfter = now / 1000 + 3600,
            issuerHex = "aabbccdd",
            ok = true
        )

        val bid = heldOfferOf(summary, bidAt = now)

        assertEquals(summary.jobIdHex, bid.jobIdHex)
        assertEquals(summary.planRefHex, bid.planRefHex)
        assertEquals(summary.offerBase64, bid.offerBase64)
        assertEquals(summary.centroidLat, bid.centroidLat, 0.0)
        assertEquals(summary.spread, bid.spread)
        assertEquals(now, bid.bidAt)
    }
}
