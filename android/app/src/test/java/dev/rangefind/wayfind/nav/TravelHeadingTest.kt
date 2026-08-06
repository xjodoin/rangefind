package dev.rangefind.wayfind.nav

import dev.rangefind.wayfind.engine.LatLon
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test
import kotlin.math.abs

class TravelHeadingTest {

    /** Walks a track from a start point along a bearing, one fix a second. */
    private fun drive(
        heading: TravelHeading,
        bearing: Double,
        speedMps: Double,
        seconds: Int,
        startMs: Long = 0L,
        from: LatLon = LatLon(45.6, -73.8)
    ): LatLon {
        var at = from
        for (second in 0..seconds) {
            heading.onFix(at, startMs + second * 1_000L)
            at = advance(at, bearing, speedMps)
        }
        return at
    }

    @Test
    fun `no fixes means no heading`() {
        assertNull(TravelHeading().fromMovement())
    }

    @Test
    fun `a stationary vehicle has no heading`() {
        val heading = TravelHeading()
        repeat(6) { heading.onFix(LatLon(45.6, -73.8), it * 1_000L) }
        assertNull("noise was turned into a direction", heading.fromMovement())
    }

    /**
     * The case the router was missing: too slow for a satellite course, but
     * unambiguously going somewhere.
     */
    @Test
    fun `a crawling vehicle still has a heading`() {
        val heading = TravelHeading()
        // 2 m/s — under MIN_HEADING_SPEED_MPS, so the fix's own course is
        // discarded, which is what left the router with nothing.
        drive(heading, bearing = 90.0, speedMps = 2.0, seconds = 6)
        val course = heading.fromMovement()
        assertNotNull("a moving car reported no direction", course)
        assertEquals(90.0, course!!, 5.0)
    }

    @Test
    fun `a shuffle shorter than the noise floor gives nothing`() {
        val heading = TravelHeading()
        // 1 m/s for three seconds is three metres — inside GPS error.
        drive(heading, bearing = 270.0, speedMps = 1.0, seconds = 3)
        assertNull(heading.fromMovement())
    }

    @Test
    fun `the heading follows the direction actually travelled`() {
        for (bearing in listOf(0.0, 45.0, 137.0, 250.0, 355.0)) {
            val heading = TravelHeading()
            drive(heading, bearing = bearing, speedMps = 8.0, seconds = 5)
            val course = heading.fromMovement()!!
            val off = abs(bearingDelta(bearing, course))
            assert(off < 5.0) { "asked for $bearing, got $course" }
        }
    }

    /**
     * A car that turns should report where it is going now, not the average of
     * where it has been, so the window has to age out.
     */
    @Test
    fun `an old leg does not hold the heading back`() {
        val heading = TravelHeading()
        val corner = drive(heading, bearing = 0.0, speedMps = 10.0, seconds = 6, startMs = 0L)
        drive(heading, bearing = 90.0, speedMps = 10.0, seconds = 6, startMs = 7_000L, from = corner)
        val course = heading.fromMovement()!!
        assert(abs(bearingDelta(90.0, course)) < 15.0) { "still pointing north: $course" }
    }

    @Test
    fun `a new trip does not inherit the last one's course`() {
        val heading = TravelHeading()
        drive(heading, bearing = 180.0, speedMps = 10.0, seconds = 5)
        heading.reset()
        assertNull(heading.fromMovement())
    }
}
