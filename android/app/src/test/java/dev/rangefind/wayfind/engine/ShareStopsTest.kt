package dev.rangefind.wayfind.engine

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * What a published run says its plan is.
 *
 * The wire keys its outcome map by position in this list and validates a
 * mark against `1..stops.size`, while the app marks a stop by its absolute
 * position in the [ActiveJob]. So the list published for a dispatch run has
 * to be the whole plan in plan order: drop the stops already dealt with and
 * every mark after that lands on the wrong doorstep, which no follower can
 * see happening.
 */
class ShareStopsTest {

    private fun stop(index: Int) = JobStop(
        index = index,
        lat = 45.5 + index / 1000.0,
        lon = -73.6 - index / 1000.0,
        label = "Stop $index"
    )

    private fun job(total: Int, nextIndex: Int, outcomes: List<Int> = emptyList()) = ActiveJob(
        stops = (1..total).map(::stop),
        nextIndex = nextIndex,
        ticketBase64 = "AAAABBBBCCCC",
        jobIdHex = "0f1e2d3c4b5a6978",
        notAfter = 1_800_000_000L,
        outcomes = if (outcomes.isEmpty()) List(total) { StopOutcome.PENDING } else outcomes
    )

    @Test
    fun `a dispatch run publishes the whole day in plan order`() {
        val plan = job(total = 5, nextIndex = 1)
        val stops = shareStopsFor(plan, selected = LatLon(0.0, 0.0))
        assertEquals(5, stops.size)
        assertEquals(plan.stops.map { it.point }, stops)
    }

    @Test
    fun `a run part way through keeps every plan position`() {
        val plan = job(
            total = 5,
            nextIndex = 3,
            outcomes = listOf(
                StopOutcome.DELIVERED,
                StopOutcome.SKIPPED,
                StopOutcome.PENDING,
                StopOutcome.PENDING,
                StopOutcome.PENDING
            )
        )
        val stops = shareStopsFor(plan, selected = null)
        // The stop being driven to is the third entry, exactly as
        // `nextIndex` says: the marking path passes that number straight to
        // the wire, so anything shorter mis-attributes the delivery.
        assertEquals(5, stops.size)
        assertEquals(plan.stops[2].point, stops[2])
    }

    @Test
    fun `the pinned destination is the plan when there is no run`() {
        val here = LatLon(45.64015, -73.79837)
        assertEquals(listOf(here), shareStopsFor(job = null, selected = here))
    }

    @Test
    fun `a run with no stops falls back to the destination`() {
        val empty = ActiveJob(
            stops = emptyList(),
            nextIndex = 1,
            ticketBase64 = null,
            jobIdHex = "",
            notAfter = 0L
        )
        val here = LatLon(45.5, -73.6)
        assertEquals(listOf(here), shareStopsFor(empty, here))
    }

    @Test
    fun `nothing to go to is no plan at all rather than a stop at zero`() {
        assertTrue(shareStopsFor(job = null, selected = null).isEmpty())
    }
}
