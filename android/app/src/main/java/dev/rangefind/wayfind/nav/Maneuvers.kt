package dev.rangefind.wayfind.nav

import dev.rangefind.wayfind.engine.Route
import dev.rangefind.wayfind.engine.RouteStep
import kotlin.math.abs

/**
 * What a driver actually has to do at the start of a step.
 *
 * A route is a list of roads, and the app was treating every change of road
 * as an instruction. Most are not. A named bridge carrying the road you are
 * already on, a forty-metre connector between two halves of one junction, a
 * street that changes name at a boundary — none of these are decisions, and
 * listing them turns four instructions into nine.
 *
 * The ones that genuinely are decisions include the case the road network is
 * worst at describing: a slip road, which is almost always unnamed. Leaving a
 * motorway and joining one look identical from the geometry — both are a
 * gentle curve onto a link road — and only the classes on either side say
 * which is which.
 */
enum class Maneuver {
    /** Straight on; the road's name changed but nothing else did. */
    Continue,
    Turn,
    /** Leaving a faster road by a slip road. */
    Exit,
    /** Joining a faster road by a slip road. */
    Merge,
    /**
     * Entering a roundabout, which is one instruction and not the two or
     * three nameless arcs it is built from.
     *
     * The turn angle is useless here: it describes the curve of the circle,
     * not which way the driver ends up going, so a roundabout followed by a
     * left turn was drawn — and spoken — as "bear right". What a driver
     * needs is the exit to count to, which is why the index carries it.
     */
    Roundabout,
    /** Boarding a ferry. */
    Ferry,
    Arrive
}

/** Road classes ranked by how major they are, for telling exits from entrances. */
private fun rankOf(roadClass: String): Int = when (roadClass) {
    "motorway", "motorway_link" -> 5
    "trunk", "trunk_link" -> 4
    "primary", "primary_link" -> 3
    "secondary", "secondary_link" -> 2
    "tertiary", "tertiary_link" -> 1
    else -> 0
}

private fun isRamp(step: RouteStep?): Boolean = step?.roadClass?.endsWith("_link") == true

/** Below this the road is going straight on, whatever it is called. */
const val STRAIGHT_DEGREES = 22.0

/**
 * Classifies the maneuver onto [index], given the turn angle there.
 *
 * The angle alone cannot separate a motorway exit from staying on the
 * motorway — both are nearly straight — so the classes before and after the
 * slip road decide it.
 */
fun maneuverOnto(route: Route, index: Int, turnDelta: Double): Maneuver {
    val step = route.steps.getOrNull(index) ?: return Maneuver.Continue
    if (index >= route.steps.lastIndex) return Maneuver.Arrive
    if (step.roundabout) return Maneuver.Roundabout
    if (step.isFerry) return Maneuver.Ferry
    if (isRamp(step)) {
        val from = rankOf(route.steps.getOrNull(index - 1)?.roadClass.orEmpty())
        // The road the ramp actually leads to, past any further ramps.
        var next = index + 1
        while (next < route.steps.size && isRamp(route.steps[next])) next++
        val to = rankOf(route.steps.getOrNull(next)?.roadClass.orEmpty())
        return when {
            to > from -> Maneuver.Merge
            to < from -> Maneuver.Exit
            // Ramp to ramp at the same rank: a junction transfer, which is
            // still a leaving as far as the driver is concerned.
            else -> Maneuver.Exit
        }
    }
    return if (abs(turnDelta) >= STRAIGHT_DEGREES) Maneuver.Turn else Maneuver.Continue
}

/**
 * Whether a step is worth telling the driver about.
 *
 * A straight-through change of name never is, however long the road or
 * whatever it is called. Driving over a named bridge, past a boundary where
 * the street is renamed, or through a connector inside one junction are all
 * the same act — carrying on — and each of them was being presented as an
 * instruction of its own.
 */
fun isWorthAnnouncing(route: Route, index: Int, turnDelta: Double): Boolean =
    maneuverOnto(route, index, turnDelta) != Maneuver.Continue

/**
 * The steps a driver should be shown.
 *
 * An itinerary is a list of things to do, not a list of roads driven along.
 * A named bridge, a connector inside a junction and a street that changes
 * name at a boundary are all the same instruction — carry on — so they fold
 * into the line before them and their distance is added to it. "Continue
 * 2.6 km" has to still read 2.6 km once the bridge in the middle of it stops
 * being its own line.
 */
data class ItineraryLine(
    val index: Int,
    val name: String,
    val meters: Double,
    val maneuver: Maneuver,
    /** Exit number off the sign, when the maneuver has one. */
    val exitRef: String = "",
    /** Which exit of a roundabout to take; 0 when unknown. */
    val roundaboutExit: Int = 0
)

fun itineraryOf(route: Route, turnDeltaAt: (Int) -> Double): List<ItineraryLine> {
    val lines = mutableListOf<ItineraryLine>()
    route.steps.forEachIndexed { index, step ->
        val delta = turnDeltaAt(index)
        val maneuver = maneuverOnto(route, index, delta)
        val fold = index > 0 && !isWorthAnnouncing(route, index, delta)
        if (fold && lines.isNotEmpty()) {
            val last = lines.removeAt(lines.lastIndex)
            lines += last.copy(
                meters = last.meters + step.meters,
                // Keep the name the driver is on unless it had none — except
                // on a roundabout, where the circle's own name (when it even
                // has one) is not the instruction. What the driver needs is
                // the road they come off onto, which is this step.
                name = if (last.maneuver == Maneuver.Roundabout && step.name.isNotBlank()) step.name
                else last.name.ifBlank { step.name }
            )
        } else {
            lines += ItineraryLine(
                index = index,
                // A motorway is signed by its number, never by its name. The
                // itinerary reads the same way the panels do.
                name = signpostName(step),
                meters = step.meters,
                maneuver = maneuver,
                exitRef = step.exitRef,
                roundaboutExit = step.roundaboutExit
            )
        }
    }
    return lines
}

/**
 * How a step should be written down.
 *
 * A slip road is where the sign does the most work: it has no name, so the
 * only useful thing to say is where it goes — "40 Ouest", "Montréal". The
 * road it leads onto says its number first and its name second, because that
 * is the order they appear overhead.
 */
fun signpostName(step: RouteStep): String {
    if (step.roadClass.endsWith("_link")) {
        val toward = step.towardLabel
        if (toward.isNotBlank()) return toward
    }
    val ref = step.signLabel
    if (ref.isBlank() || ref == step.name) return step.name
    // Both, when the road has a number and a name that differ and there is
    // something to be gained from saying so — "40 · Autoroute Félix-Leclerc".
    return if (step.name.isBlank()) ref else "$ref · ${step.name}"
}
