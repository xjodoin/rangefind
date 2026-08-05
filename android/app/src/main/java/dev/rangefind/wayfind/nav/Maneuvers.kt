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
    val maneuver: Maneuver
)

fun itineraryOf(route: Route, turnDeltaAt: (Int) -> Double): List<ItineraryLine> {
    val lines = mutableListOf<ItineraryLine>()
    route.steps.forEachIndexed { index, step ->
        val delta = turnDeltaAt(index)
        val maneuver = maneuverOnto(route, index, delta)
        val fold = index > 0 && !isWorthAnnouncing(route, index, delta)
        if (fold && lines.isNotEmpty()) {
            val last = lines.removeAt(lines.lastIndex)
            // Keep the name the driver is on unless it had none.
            lines += last.copy(
                meters = last.meters + step.meters,
                name = last.name.ifBlank { step.name }
            )
        } else {
            lines += ItineraryLine(index, step.name, step.meters, maneuver)
        }
    }
    return lines
}
