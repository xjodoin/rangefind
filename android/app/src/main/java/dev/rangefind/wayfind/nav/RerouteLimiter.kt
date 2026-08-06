package dev.rangefind.wayfind.nav

/**
 * Decides when asking for a new route is worth doing again.
 *
 * Off-route detection and rerouting form a loop with nothing damping it: three
 * consecutive fixes off the line ask for a new route, adopting that route
 * clears the strike count, and if the car is still not on the new line the
 * next three fixes ask again. At a 1 Hz fix rate that is a request every three
 * seconds, indefinitely.
 *
 * A recorded drive did exactly this — thirteen reroutes between t+20s and
 * t+57s, all at the first step, all at walking-to-cycling pace, each one
 * spoken aloud. The router was not wrong thirteen times; it was asked the same
 * question thirteen times and gave the same answer, while the driver heard
 * "rerouting" every three seconds.
 *
 * Repeating a request that has already failed to help is the thing to stop, so
 * each attempt that does not get the car back on the line waits longer than
 * the last. Getting back on route is the signal that the last reroute worked,
 * and the backoff resets there. A driver who genuinely misses a turn still
 * gets a prompt first attempt; a car that the map simply cannot place stops
 * being nagged about it.
 */
class RerouteLimiter {

    private var attempts = 0
    // Flags rather than sentinel timestamps: a trip whose first trouble lands
    // at t=0 is not a trip that has never had any, and conflating the two let
    // the first backoff be skipped entirely.
    private var attempted = false
    private var announced = false
    private var lastAttemptMs = 0L
    private var announcedAtMs = 0L
    private var onRouteRun = 0

    /** Forget everything — a new trip is not the last one's third attempt. */
    fun reset() {
        attempts = 0
        attempted = false
        announced = false
        lastAttemptMs = 0L
        announcedAtMs = 0L
        onRouteRun = 0
    }

    /**
     * Whether to ask for a new route now. Records the attempt if so, since a
     * caller that asks is a caller that is about to act on the answer.
     */
    fun shouldReroute(nowMs: Long): Boolean {
        // Zeroed before the backoff check, not after: being off-route is proof
        // the car is not following the line whether or not a request goes out,
        // and counting blocked attempts as progress toward "recovered" let the
        // run reach its threshold mid-thrash and clear the backoff.
        onRouteRun = 0
        if (attempted && nowMs - lastAttemptMs < waitMs()) return false
        attempts++
        attempted = true
        lastAttemptMs = nowMs
        return true
    }

    /**
     * A fix that is not off-route. Takes a run of them to clear the backoff,
     * and the length of that run is the whole point.
     *
     * Off-route is itself a verdict on the last three fixes, and adopting a
     * new route clears those strikes, so a thrashing drive reads as
     * `..X..X..X` — two perfectly on-route fixes between every reroute. A
     * limiter that reset on any one of them would be reset twice per cycle and
     * would change nothing at all, which is exactly what the recorded drive
     * does. Only a run longer than the strike cycle means the car is genuinely
     * back on the line.
     */
    fun onRoute() {
        if (!attempted) return
        onRouteRun++
        if (onRouteRun >= CLEAR_RUN) reset()
    }

    /**
     * Whether to say "rerouting" out loud. Once per run of trouble, not once
     * per attempt — the second and third announcements carry no information
     * the first did not, and arrive while the driver is still dealing with it.
     */
    fun shouldAnnounce(nowMs: Long): Boolean {
        if (announced && nowMs - announcedAtMs < ANNOUNCE_QUIET_MS) return false
        announced = true
        announcedAtMs = nowMs
        return true
    }

    /** How long this attempt must wait, given how many came before it. */
    private fun waitMs(): Long {
        val step = attempts.coerceAtMost(BACKOFF_MS.size) - 1
        return if (step < 0) 0L else BACKOFF_MS[step]
    }

    companion object {
        /**
         * The wait after the 1st, 2nd, 3rd… failed attempt. The first retry is
         * quick because a driver who has just missed a turn wants the new line
         * immediately; past that, more requests are not the answer.
         */
        private val BACKOFF_MS = longArrayOf(6_000, 15_000, 30_000, 60_000)

        /** How long one "rerouting" covers before another is worth saying. */
        private const val ANNOUNCE_QUIET_MS = 45_000L

        /**
         * Consecutive on-route fixes that mean the trouble is over. Must be
         * longer than the strike cycle that produces off-route in the first
         * place, or the backoff clears between every strike.
         */
        internal const val CLEAR_RUN = 5
    }
}
