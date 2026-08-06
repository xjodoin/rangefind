package dev.rangefind.wayfind.nav

import dev.rangefind.wayfind.engine.LatLon
import org.junit.Assert.assertEquals
import org.junit.Test

class SnapBlendTest {

    /**
     * The drawn position must be a continuous function of how far the fix sits
     * from the route. It was not: the arrow was either on the line or at the
     * fix, chosen at a threshold, so crossing that threshold moved it by the
     * whole cross-track distance at once. Recorded drives caught the jump at
     * up to 24 m — a car flung sideways across four lanes between one second
     * and the next.
     *
     * This walks the fix away from the road a centimetre at a time and watches
     * what gets drawn. No step may exceed what that centimetre could justify.
     */
    @Test
    fun `the drawn position never jumps as the fix drifts off the line`() {
        // A point on the route, and a direction to drift away in. One degree
        // of latitude is ~111.32 km, so this is metres expressed as degrees.
        val onRoute = LatLon(45.5, -73.6)
        fun fixAt(crossTrackMeters: Double) =
            LatLon(onRoute.lat + crossTrackMeters / 111_320.0, onRoute.lon)
        fun drawnAt(crossTrackMeters: Double) = interpolate(
            onRoute,
            fixAt(crossTrackMeters),
            NavigationCore.snapBlend(crossTrackMeters)
        )

        var worst = 0.0
        var worstAt = 0.0
        var previous = drawnAt(0.0)
        var cross = 0.0
        while (cross <= 40.0) {
            cross += 0.01
            val drawn = drawnAt(cross)
            val step = haversineMeters(previous, drawn)
            if (step > worst) {
                worst = step
                worstAt = cross
            }
            previous = drawn
        }
        // A centimetre of drift may move the arrow a few centimetres while the
        // blend is opening; it may never move it metres.
        assert(worst < 0.05) { "arrow jumped ${"%.2f".format(worst)} m at ${"%.1f".format(worstAt)} m cross-track" }
    }

    @Test
    fun `close to the line the arrow is on the road`() {
        assertEquals(0.0, NavigationCore.snapBlend(0.0), 1e-9)
        assertEquals(0.0, NavigationCore.snapBlend(NavigationCore.SNAP_HOLD_METERS), 1e-9)
    }

    /**
     * The endpoint that must not move: once the car has plainly left the
     * route, the arrow shows where it actually is. Snapping it to a road the
     * driver is no longer on is the failure this whole path exists to avoid.
     */
    @Test
    fun `well off the line the arrow is at the fix`() {
        assertEquals(1.0, NavigationCore.snapBlend(NavigationCore.SNAP_TRUST_METERS), 1e-9)
        assertEquals(1.0, NavigationCore.snapBlend(100.0), 1e-9)
    }

    @Test
    fun `between the two the arrow is between the two`() {
        val middle = (NavigationCore.SNAP_HOLD_METERS + NavigationCore.SNAP_TRUST_METERS) / 2
        assertEquals(0.5, NavigationCore.snapBlend(middle), 1e-9)
    }
}
