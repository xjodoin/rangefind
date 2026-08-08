package dev.rangefind.wayfind.nav

import dev.rangefind.wayfind.engine.Route
import dev.rangefind.wayfind.engine.RouteStep
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * A junction is not a road, however many steps the router splits it into.
 *
 * Recorded on a drive through Sainte-Thérèse: turning left off Boulevard du
 * Domaine onto Boulevard René-A.-Robert came back as three steps — the
 * boulevard, twelve metres of Rue Saint-Pierre, then René-A.-Robert. Read step
 * by step the instruction at the junction is "continue onto Rue Saint-Pierre"
 * (the turn onto it measures −3.5°, which is straight), and the left turn that
 * is the entire point of the maneuver has twelve metres of runway. The driver
 * flagged it twice on one drive: they never got the turn.
 */
class JunctionStubTest {

    private fun step(name: String, meters: Double, at: Int, roundabout: Boolean = false) = RouteStep(
        name = name,
        meters = meters,
        seconds = meters / 10,
        at = at,
        speedLimitKmh = 40,
        roadClass = "residential",
        ref = "",
        exitRef = "",
        destinationRef = "",
        destination = "",
        roundabout = roundabout,
        roundaboutExit = 0
    )

    private fun route(steps: List<RouteStep>) = Route(
        seconds = 100.0,
        distanceMeters = steps.sumOf { it.meters },
        geometry = emptyList(),
        steps = steps,
        junctions = emptyList(),
        speedLimits = emptyList(),
        httpRequests = 0,
        bytesFetched = 0
    )

    /** The drive as it was recorded, stub and all. */
    private val domaine = route(
        listOf(
            step("Rue Lavoie", 365.2, at = 0),
            step("Boulevard du Domaine", 272.9, at = 10),
            step("Rue Saint-Pierre", 12.0, at = 20),
            step("Boulevard René-A.-Robert", 1407.3, at = 21)
        )
    )

    @Test
    fun `a twelve-metre street between two boulevards is the junction, not a leg`() {
        // Standing on Boulevard du Domaine, the step that matters is the one
        // past the stub — the boulevard being turned onto.
        assertEquals(3, stepThroughJunction(domaine, 2))
        assertEquals(
            "Boulevard René-A.-Robert",
            domaine.steps[stepThroughJunction(domaine, 2)].name
        )
    }

    @Test
    fun `the turn is the whole heading change across the junction`() {
        // −3.5° into the stub and −86° out of it is one 90° left turn, not a
        // straight-on followed by something the driver is never warned about.
        val deltas = mapOf(20 to -3.5, 21 to -86.0)
        val delta = turnDeltaThrough(domaine, 2, 3) { at -> deltas[at] ?: 0.0 }
        assertEquals(-89.5, delta, 0.001)
        assertTrue("a 90° left has to read as a left", delta < -45)
    }

    @Test
    fun `an ordinary next step is left exactly as it was`() {
        // The common case must be untouched: no stub, no collapsing, and the
        // turn is the single delta at that one junction.
        val plain = route(
            listOf(
                step("Rue Cedar", 250.0, at = 0),
                step("Rue de Rosemère", 750.0, at = 8),
                step("Boulevard Roland-Durand", 500.0, at = 19)
            )
        )
        assertEquals(1, stepThroughJunction(plain, 1))
        val delta = turnDeltaThrough(plain, 1, 1) { at -> if (at == 8) 42.0 else 999.0 }
        assertEquals(42.0, delta, 0.001)
    }

    @Test
    fun `a roundabout arc is short by construction and is never collapsed`() {
        // Rue Frenette's 14 m arc is a roundabout, which has its own
        // instruction ("take the first exit"). Collapsing it would throw that
        // away and announce the road past it as a plain turn.
        val circle = route(
            listOf(
                step("Rue Northcote", 108.0, at = 0),
                step("Rue Frenette", 14.2, at = 5, roundabout = true),
                step("Rue Frenette", 294.3, at = 9)
            )
        )
        assertEquals(1, stepThroughJunction(circle, 1))
    }

    @Test
    fun `a run of stubs collapses to the first real road`() {
        // Big junctions produce more than one. Whatever they are, the driver
        // is told about the road they come out on.
        val messy = route(
            listOf(
                step("Rue A", 400.0, at = 0),
                step("link one", 8.0, at = 4),
                step("link two", 11.0, at = 5),
                step("Boulevard B", 900.0, at = 6)
            )
        )
        assertEquals(3, stepThroughJunction(messy, 1))
    }

    @Test
    fun `the last step is never collapsed away`() {
        // A short final step is the destination approach, not a junction, and
        // there is nothing past it to name.
        val ending = route(
            listOf(
                step("Rue Frenette", 294.3, at = 0),
                step("", 14.2, at = 9)
            )
        )
        assertEquals(1, stepThroughJunction(ending, 1))
    }
}
