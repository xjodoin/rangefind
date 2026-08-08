package dev.rangefind.wayfind.engine

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Which final record a run ending now is entitled to publish.
 *
 * COMPLETED is a claim about the world: every customer holding the link and
 * every dispatcher watching is told the run arrived. A plan with stops nobody
 * has spoken for did not arrive, whichever button ended it — so the invariant
 * under test is that no shape of abandoned day can produce COMPLETED.
 */
class JobFinalRecordTest {

    private fun stop(index: Int) = JobStop(
        index = index,
        lat = 45.5 + index / 1000.0,
        lon = -73.6 - index / 1000.0,
        label = "Stop $index"
    )

    private fun job(total: Int, outcomes: List<Int>) = ActiveJob(
        stops = (1..total).map(::stop),
        nextIndex = 1,
        ticketBase64 = "AAAABBBBCCCC",
        jobIdHex = "0f1e2d3c4b5a6978",
        notAfter = 1_800_000_000L,
        outcomes = outcomes
    )

    @Test
    fun `a day with every stop answered is completed`() {
        val done = job(
            total = 3,
            outcomes = listOf(StopOutcome.DELIVERED, StopOutcome.FAILED, StopOutcome.DELIVERED)
        )
        assertEquals(JobFinalRecord.COMPLETED, finalRecordFor(done))
        assertEquals(0, unresolvedStops(done))
    }

    @Test
    fun `skipped and failed count as answered — nobody is coming back either way`() {
        val done = job(
            total = 2,
            outcomes = listOf(StopOutcome.SKIPPED, StopOutcome.FAILED)
        )
        assertEquals(JobFinalRecord.COMPLETED, finalRecordFor(done))
    }

    @Test
    fun `a day abandoned part way through is canceled, never completed`() {
        val abandoned = job(
            total = 4,
            outcomes = listOf(
                StopOutcome.DELIVERED,
                StopOutcome.PENDING,
                StopOutcome.PENDING,
                StopOutcome.PENDING
            )
        )
        assertEquals(JobFinalRecord.CANCELED, finalRecordFor(abandoned))
        assertEquals(3, unresolvedStops(abandoned))
    }

    @Test
    fun `a day nobody started is canceled`() {
        val untouched = job(total = 5, outcomes = List(5) { StopOutcome.PENDING })
        assertEquals(JobFinalRecord.CANCELED, finalRecordFor(untouched))
        assertEquals(5, unresolvedStops(untouched))
    }

    @Test
    fun `an outcome list shorter than the plan is pending, not resolved`() {
        // A truncated list is missing assertions rather than carrying them:
        // reading past its end as "delivered" would publish COMPLETED for
        // stops no driver ever spoke about.
        val ragged = job(total = 3, outcomes = listOf(StopOutcome.DELIVERED))
        assertEquals(JobFinalRecord.CANCELED, finalRecordFor(ragged))
        assertEquals(2, unresolvedStops(ragged))
    }

    @Test
    fun `a plain shared drive has no plan to be dishonest about`() {
        assertEquals(JobFinalRecord.COMPLETED, finalRecordFor(null))
        assertEquals(0, unresolvedStops(null))
        val planless = ActiveJob(
            stops = emptyList(),
            nextIndex = 1,
            ticketBase64 = null,
            jobIdHex = "",
            notAfter = 0L
        )
        assertEquals(JobFinalRecord.COMPLETED, finalRecordFor(planless))
    }

    @Test
    fun `a blank reason is the absence of a note`() {
        assertNull(truncateNote(null))
        assertNull(truncateNote(""))
        assertNull(truncateNote("   \n "))
    }

    @Test
    fun `a short reason travels as written, trimmed`() {
        assertEquals("Van broke down", truncateNote("  Van broke down  "))
    }

    @Test
    fun `a reason at the bound is kept whole`() {
        val exact = "x".repeat(MAX_NOTE_BYTES)
        assertEquals(exact, truncateNote(exact))
        assertEquals(MAX_NOTE_BYTES, truncateNote(exact)!!.toByteArray(Charsets.UTF_8).size)
    }

    @Test
    fun `a reason over the bound is cut to what a record carries`() {
        val long = "y".repeat(MAX_NOTE_BYTES + 40)
        val cut = truncateNote(long)!!
        assertEquals(MAX_NOTE_BYTES, cut.toByteArray(Charsets.UTF_8).size)
        assertTrue(long.startsWith(cut))
    }

    @Test
    fun `multi-byte characters are counted in bytes and never split`() {
        // "é" is two bytes, so 40 of them are 80 — over the bound. Cutting by
        // characters would fit 64 of them and hand the customer a note the
        // codec refuses; cutting mid-character would hand them a replacement
        // glyph.
        val accented = "é".repeat(40)
        val cut = truncateNote(accented)!!
        assertEquals(32, cut.length)
        assertEquals(64, cut.toByteArray(Charsets.UTF_8).size)
        assertTrue(accented.startsWith(cut))
    }

    @Test
    fun `a surrogate pair is kept or dropped whole`() {
        // A four-byte emoji straddling the bound: 62 bytes of text leaves two
        // bytes, and half an emoji is not a note.
        val text = "z".repeat(62) + "🚗"
        val cut = truncateNote(text)!!
        assertEquals(62, cut.length)
        assertEquals(62, cut.toByteArray(Charsets.UTF_8).size)
    }
}
