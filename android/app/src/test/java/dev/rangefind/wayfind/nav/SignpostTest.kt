package dev.rangefind.wayfind.nav

import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.engine.Route
import dev.rangefind.wayfind.engine.RouteStep
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * How an instruction is written down.
 *
 * A motorway's name is the one thing never posted on a sign. Guidance that
 * reads the name announces roads by a label the driver cannot see: nobody in
 * Québec is looking for "Autoroute Félix-Leclerc", they are looking for a
 * green panel that says **40 Ouest**, and above the slip road, **Sortie 32**.
 */
class SignpostTest {

    private fun step(
        name: String = "",
        roadClass: String = "primary",
        ref: String = "",
        exitRef: String = "",
        destinationRef: String = "",
        destination: String = "",
        roundabout: Boolean = false,
        roundaboutExit: Int = 0,
        meters: Double = 100.0
    ) = RouteStep(
        name = name,
        meters = meters,
        seconds = meters / 10,
        at = 0,
        speedLimitKmh = 0,
        roadClass = roadClass,
        ref = ref,
        exitRef = exitRef,
        destinationRef = destinationRef,
        destination = destination,
        roundabout = roundabout,
        roundaboutExit = roundaboutExit
    )

    @Test
    fun `a numbered road is written by its number, with the name behind it`() {
        assertEquals(
            "40 · Autoroute Félix-Leclerc",
            signpostName(step(name = "Autoroute Félix-Leclerc", roadClass = "motorway", ref = "40"))
        )
        // An unnamed numbered road is just its number.
        assertEquals("13", signpostName(step(roadClass = "motorway", ref = "13")))
        // And an ordinary street keeps its name — most roads have no number,
        // and inventing a distinction where the map draws none is noise.
        assertEquals("Rue Libersan", signpostName(step(name = "Rue Libersan", roadClass = "residential")))
        // A road whose name *is* its number must not be written twice.
        assertEquals("A13", signpostName(step(name = "A13", roadClass = "motorway", ref = "A13")))
    }

    @Test
    fun `a slip road is written by where it leads, because it has no name`() {
        // This is the whole point: a ramp is unnamed far more often than not,
        // and the sign over it says where it goes, with the cardinal that is
        // the only direction OSM actually carries in quantity.
        val ramp = step(
            roadClass = "motorway_link",
            exitRef = "32",
            destinationRef = "40 Ouest;20",
            destination = "Montréal;Québec"
        )
        assertEquals("40 Ouest", signpostName(ramp))

        // A semicolon list is cut to its first entry: a banner has room for
        // one answer, and the first is the one the sign leads with.
        assertEquals("40 Ouest", ramp.towardLabel)

        // With no numbered destination, the place named on the sign will do.
        assertEquals(
            "Montréal",
            signpostName(step(roadClass = "motorway_link", destination = "Montréal;Québec"))
        )
        // With nothing at all, there is nothing to say — and the caller falls
        // back to what the ramp joins rather than inventing a label.
        assertEquals("", signpostName(step(roadClass = "motorway_link")))
    }

    @Test
    fun `a ferry is recognised as a crossing rather than a road`() {
        assertTrue(step(roadClass = "ferry", name = "Sorel–Saint-Ignace-de-Loyola").isFerry)
        assertTrue(!step(roadClass = "primary").isFerry)
    }

    /**
     * The itinerary is a list of things to do. A roundabout is one of them,
     * and it is not the road before it or the road after it.
     */
    @Test
    fun `a roundabout is its own line, labelled by the road it is left on`() {
        val steps = listOf(
            step(name = "Chemin d'Oka", roadClass = "secondary", meters = 800.0),
            // The circle: unnamed by construction, and the arcs run straight
            // enough that the fold would otherwise absorb them.
            step(roadClass = "secondary", roundabout = true, roundaboutExit = 2, meters = 40.0),
            step(name = "Boulevard Labelle", roadClass = "secondary", meters = 600.0),
            step(name = "Rue Libersan", roadClass = "residential", meters = 90.0)
        )
        val route = route(steps)
        // Straight angles throughout, so nothing here is a line because of
        // its geometry — the roundabout has to earn its own line from the flag.
        val lines = itineraryOf(route) { 0.0 }

        val circle = lines.single { it.maneuver == Maneuver.Roundabout }
        assertEquals(2, circle.roundaboutExit)
        assertEquals(
            "the roundabout line names the road it is left on, not the circle",
            "Boulevard Labelle",
            circle.name
        )
        // And the road before it is a separate line, not swallowed by it.
        assertEquals("Chemin d'Oka", lines.first().name)
    }

    @Test
    fun `an exit line carries its number`() {
        val steps = listOf(
            step(name = "Autoroute Chomedey", roadClass = "motorway", ref = "13", meters = 5000.0),
            step(roadClass = "motorway_link", exitRef = "6", destinationRef = "40 Ouest", meters = 500.0),
            step(name = "Autoroute Félix-Leclerc", roadClass = "motorway", ref = "40", meters = 9000.0)
        )
        val lines = itineraryOf(route(steps)) { 0.0 }
        val exit = lines.single { it.maneuver == Maneuver.Exit }
        assertEquals("6", exit.exitRef)
        assertEquals("40 Ouest", exit.name)
    }

    /** Geometry the maneuver classifier needs; the angles come from the caller. */
    private fun route(steps: List<RouteStep>) = Route(
        seconds = steps.sumOf { it.seconds },
        distanceMeters = steps.sumOf { it.meters },
        geometry = List(steps.size + 2) { LatLon(45.5 + it * 0.001, -73.6) },
        steps = steps,
        junctions = emptyList(),
        speedLimits = emptyList(),
        httpRequests = 0,
        bytesFetched = 0
    )
}
