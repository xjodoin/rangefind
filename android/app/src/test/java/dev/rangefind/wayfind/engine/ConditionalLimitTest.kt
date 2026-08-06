package dev.rangefind.wayfind.engine

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.LocalDateTime

class ConditionalLimitTest {

    /** Rue du Sourcin as OSM carries it: 50, dropping to 30 on school days. */
    private val schoolZone = ConditionalLimit(
        limitKmh = 30,
        days = 0b0011111,
        startMinute = 7 * 60,
        endMinute = 17 * 60,
        monthStart = 9,
        monthEnd = 6
    )

    @Test
    fun `a school morning is inside the window`() {
        assertTrue(schoolZone.appliesAt(LocalDateTime.of(2026, 10, 8, 8, 15)))
    }

    @Test
    fun `the same hour in July is not`() {
        assertFalse(schoolZone.appliesAt(LocalDateTime.of(2026, 7, 8, 8, 15)))
    }

    /**
     * September to June is one range that crosses the new year. Read as a
     * plain low-to-high span it is empty, which would silence every school
     * zone in the province — and silently, since the zone would simply never
     * apply rather than fail.
     */
    @Test
    fun `a school year wraps the new year`() {
        assertTrue(schoolZone.appliesAt(LocalDateTime.of(2026, 1, 13, 8, 15)))
        assertTrue(schoolZone.appliesAt(LocalDateTime.of(2026, 12, 8, 8, 15)))
        assertFalse(schoolZone.appliesAt(LocalDateTime.of(2026, 8, 11, 8, 15)))
    }

    @Test
    fun `a weekend is not a school day`() {
        assertFalse(schoolZone.appliesAt(LocalDateTime.of(2026, 10, 10, 8, 15)))
        assertFalse(schoolZone.appliesAt(LocalDateTime.of(2026, 10, 11, 8, 15)))
    }

    @Test
    fun `the window is closed at its end, not open`() {
        assertTrue(schoolZone.appliesAt(LocalDateTime.of(2026, 10, 8, 7, 0)))
        assertFalse(schoolZone.appliesAt(LocalDateTime.of(2026, 10, 8, 6, 59)))
        assertTrue(schoolZone.appliesAt(LocalDateTime.of(2026, 10, 8, 16, 59)))
        assertFalse(schoolZone.appliesAt(LocalDateTime.of(2026, 10, 8, 17, 0)))
    }

    /**
     * The sign has to answer with the limit in force, which is the whole
     * point: 30 during school hours and 50 outside them, on the same metre of
     * the same road.
     */
    @Test
    fun `the route reports whichever limit is in force`() {
        val route = Route(
            seconds = 60.0,
            distanceMeters = 500.0,
            geometry = emptyList(),
            steps = emptyList(),
            junctions = emptyList(),
            speedLimits = listOf(
                SpeedLimitChange(atMeters = 0.0, limitKmh = 50),
                SpeedLimitChange(atMeters = 200.0, limitKmh = 50, conditional = schoolZone),
                SpeedLimitChange(atMeters = 400.0, limitKmh = 50)
            ),
            httpRequests = 0,
            bytesFetched = 0L
        )
        val schoolHours = LocalDateTime.of(2026, 10, 8, 8, 15)
        val evening = LocalDateTime.of(2026, 10, 8, 20, 15)

        assertEquals(50, route.speedLimitAt(100.0, schoolHours))
        assertEquals(30, route.speedLimitAt(250.0, schoolHours))
        assertEquals(50, route.speedLimitAt(450.0, schoolHours))
        assertEquals(50, route.speedLimitAt(250.0, evening))
    }
}
