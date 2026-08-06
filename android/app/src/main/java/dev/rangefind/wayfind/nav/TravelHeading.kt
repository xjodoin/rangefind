package dev.rangefind.wayfind.nav

import dev.rangefind.wayfind.engine.LatLon

/**
 * Which way the vehicle is actually going, for the router to start from.
 *
 * A reroute that does not know the direction of travel is free to send the
 * driver back the way they came. The router already charges a U-turn's worth
 * of time to any start that opposes the heading it is given — but only when it
 * is given one, and the heading is dropped whenever the satellite course is
 * untrustworthy, which is exactly when a car is crawling and most likely to be
 * off the line. A recorded drive rerouted with no heading at all six times.
 *
 * The gap that leaves is the case this fills: a course from where the vehicle
 * was a few seconds ago to where it is now. It needs no satellite course and
 * no compass, only that the car has covered enough ground for the displacement
 * to mean something — under that, two fixes' noise would point anywhere, and a
 * confidently wrong heading is worse than none, since it would charge the
 * U-turn penalty to the direction the driver is actually going.
 *
 * Deliberately not the matched road's bearing: on an off-route event that is
 * the road being left, which may be at any angle to the way the car is now
 * pointing.
 */
class TravelHeading {

    private class Sample(val point: LatLon, val atMs: Long)

    private val recent = ArrayDeque<Sample>()

    fun reset() = recent.clear()

    /** Files a fix, dropping anything too old to describe the present. */
    fun onFix(point: LatLon, atMs: Long) {
        recent.addLast(Sample(point, atMs))
        while (recent.size > MAX_SAMPLES) recent.removeFirst()
        while (recent.size > 2 && atMs - recent.first().atMs > WINDOW_MS) recent.removeFirst()
    }

    /**
     * The course over the recent window, or null if the vehicle has not moved
     * far enough for one to be meaningful.
     */
    fun fromMovement(): Double? {
        val newest = recent.lastOrNull() ?: return null
        // The oldest sample still inside the window gives the longest baseline,
        // and a longer baseline is a steadier bearing.
        val oldest = recent.firstOrNull { newest.atMs - it.atMs <= WINDOW_MS } ?: return null
        if (oldest === newest) return null
        if (haversineMeters(oldest.point, newest.point) < MIN_DISPLACEMENT_M) return null
        return bearingDegrees(oldest.point, newest.point)
    }

    private companion object {
        /** How far back a course may be drawn from. */
        const val WINDOW_MS = 6_000L
        const val MAX_SAMPLES = 12

        /**
         * Ground that must have been covered for the displacement to beat the
         * noise in it. Ten metres is a second and a half of city driving and
         * several times a good fix's error.
         */
        const val MIN_DISPLACEMENT_M = 10.0
    }
}
