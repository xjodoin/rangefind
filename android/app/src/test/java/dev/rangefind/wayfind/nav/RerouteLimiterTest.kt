package dev.rangefind.wayfind.nav

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class RerouteLimiterTest {

    /**
     * The recorded failure, replayed: a car the map cannot place, going
     * off-route again three seconds after every reroute, for the length of the
     * drive. Before the limiter this produced one request per cycle.
     */
    @Test
    fun `a car that stays off route is not asked about every three seconds`() {
        val limiter = RerouteLimiter()
        var reroutes = 0
        var announcements = 0
        // 300 s of trouble, an off-route verdict every 3 s, as recorded.
        for (t in 0L until 300_000L step 3_000L) {
            if (limiter.shouldReroute(t)) {
                reroutes++
                if (limiter.shouldAnnounce(t)) announcements++
            }
        }
        // The unlimited loop asks 100 times over this window and speaks 100
        // times. Backoff settles at one attempt a minute once it is clear the
        // reroutes are not helping.
        assertEquals(8, reroutes)
        assertEquals(6, announcements)
    }

    @Test
    fun `the first attempt is immediate`() {
        val limiter = RerouteLimiter()
        assertTrue(limiter.shouldReroute(1_000L))
    }

    @Test
    fun `a retry waits, then is allowed`() {
        val limiter = RerouteLimiter()
        assertTrue(limiter.shouldReroute(0L))
        assertFalse("retried before the backoff elapsed", limiter.shouldReroute(3_000L))
        assertFalse(limiter.shouldReroute(5_999L))
        assertTrue("never retried", limiter.shouldReroute(6_000L))
    }

    @Test
    fun `each failed attempt waits longer than the last`() {
        val limiter = RerouteLimiter()
        assertTrue(limiter.shouldReroute(0L))
        assertTrue(limiter.shouldReroute(6_000L))
        // Second backoff is 15 s, so 6 s later is still too soon.
        assertFalse(limiter.shouldReroute(12_000L))
        assertTrue(limiter.shouldReroute(21_000L))
        // Third is 30 s.
        assertFalse(limiter.shouldReroute(40_000L))
        assertTrue(limiter.shouldReroute(51_000L))
    }

    /**
     * Getting back on the line is the only evidence a reroute worked, so it is
     * the only thing that clears the backoff. A driver who misses a second
     * turn ten minutes later deserves the same prompt first attempt.
     */
    @Test
    fun `returning to the route restores a prompt first attempt`() {
        val limiter = RerouteLimiter()
        assertTrue(limiter.shouldReroute(0L))
        assertTrue(limiter.shouldReroute(6_000L))
        assertFalse(limiter.shouldReroute(9_000L))

        repeat(RerouteLimiter.CLEAR_RUN) { limiter.onRoute() }

        assertTrue("a fresh problem waited on the old backoff", limiter.shouldReroute(10_000L))
    }

    /**
     * Replays the flag pattern recorded on the thrashing drive: two on-route
     * fixes between every off-route one, because adopting a route clears the
     * strikes that produced the verdict. A limiter that treated a single
     * on-route fix as recovery would be reset twice per cycle and would hold
     * nothing back at all.
     */
    @Test
    fun `the recorded off-route pattern does not defeat the backoff`() {
        val limiter = RerouteLimiter()
        var reroutes = 0
        // "..X" repeating, one fix a second, for five minutes.
        for (second in 0 until 300) {
            val offRoute = second % 3 == 2
            if (offRoute) {
                if (limiter.shouldReroute(second * 1_000L)) reroutes++
            } else {
                limiter.onRoute()
            }
        }
        assertTrue("backoff was defeated by the gaps: $reroutes reroutes", reroutes <= 10)
    }

    @Test
    fun `a run of on-route fixes clears the backoff`() {
        val limiter = RerouteLimiter()
        assertTrue(limiter.shouldReroute(0L))
        assertTrue(limiter.shouldReroute(6_000L))
        assertFalse(limiter.shouldReroute(9_000L))
        repeat(RerouteLimiter.CLEAR_RUN) { limiter.onRoute() }
        assertTrue("a recovered car was still held back", limiter.shouldReroute(10_000L))
    }

    @Test
    fun `a new trip does not inherit the last one's backoff`() {
        val limiter = RerouteLimiter()
        repeat(4) { limiter.shouldReroute(it * 60_000L) }
        limiter.reset()
        assertTrue(limiter.shouldReroute(1_000_000L))
    }

    /** Thirteen announcements in thirty-seven seconds was the complaint. */
    @Test
    fun `rerouting is announced once per run of trouble`() {
        val limiter = RerouteLimiter()
        assertTrue(limiter.shouldAnnounce(0L))
        assertFalse(limiter.shouldAnnounce(3_000L))
        assertFalse(limiter.shouldAnnounce(37_000L))
        assertTrue("silent long after the first", limiter.shouldAnnounce(45_000L))
    }
}
