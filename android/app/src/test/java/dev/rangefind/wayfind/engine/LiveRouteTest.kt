package dev.rangefind.wayfind.engine

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * A route's duration has to be one number, and it has to be the one the
 * map agrees with.
 *
 * The failure this guards against is subtle and would look like nothing:
 * a jam drawn in red on the map while the ETA beside it still shows the
 * static metric. `seconds` is therefore the live-adjusted total whenever
 * the mesh had anything to say, with the static figure kept beside it so
 * the difference can be *stated* rather than silently folded in.
 */
class LiveRouteTest {

    private fun route(seconds: Double, staticSeconds: Double, liveSegments: Int) = Route(
        seconds = seconds,
        distanceMeters = 4200.0,
        geometry = emptyList(),
        steps = emptyList(),
        junctions = emptyList(),
        speedLimits = emptyList(),
        httpRequests = 0,
        bytesFetched = 0,
        staticSeconds = staticSeconds,
        liveSegments = liveSegments
    )

    @Test
    fun `with no live data the two durations are the same number`() {
        val plain = Route(
            seconds = 600.0,
            distanceMeters = 4200.0,
            geometry = emptyList(),
            steps = emptyList(),
            junctions = emptyList(),
            speedLimits = emptyList(),
            httpRequests = 0,
            bytesFetched = 0
        )
        assertEquals(600.0, plain.staticSeconds, 0.0)
        assertEquals(0.0, plain.liveDelaySeconds, 0.0)
        assertFalse("nothing observed means nothing to say about traffic", plain.hasLive)
    }

    @Test
    fun `a jam shows up as a delay against the static metric`() {
        val jammed = route(seconds = 900.0, staticSeconds = 600.0, liveSegments = 12)
        assertTrue(jammed.hasLive)
        assertEquals(300.0, jammed.liveDelaySeconds, 0.0)
        assertEquals(5, (jammed.liveDelaySeconds / 60).toInt())
    }

    @Test
    fun `a clear road can also read faster than the static metric`() {
        // Free-flow at 3am is a real observation, not an error to clamp
        // away: the delay is signed, and the UI phrases it accordingly.
        val clear = route(seconds = 540.0, staticSeconds = 600.0, liveSegments = 8)
        assertTrue(clear.hasLive)
        assertEquals(-60.0, clear.liveDelaySeconds, 0.0)
        assertEquals(0, (clear.liveDelaySeconds / 60).toInt().coerceAtLeast(0))
    }

    @Test
    fun `live status only counts as running when a transport is up`() {
        assertFalse(PulseMeshStatus(available = false).running)
        assertFalse(
            "available with no transport is the degraded state, not a live mesh",
            PulseMeshStatus(available = true, mode = "off").running
        )
        assertTrue(PulseMeshStatus(available = true, mode = "demo").running)
        assertTrue(PulseMeshStatus(available = true, mode = "mesh").running)
    }
}
