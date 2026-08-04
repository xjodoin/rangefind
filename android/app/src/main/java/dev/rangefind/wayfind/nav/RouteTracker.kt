package dev.rangefind.wayfind.nav

import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.engine.Route
import kotlin.math.max

/**
 * Precomputed along-route distances plus nearest-point matching, the two
 * primitives navigation needs every second: where the driver is on the line,
 * and how far the next maneuver is.
 */
class RouteTracker(val route: Route) {

    private val points: List<LatLon> = route.geometry
    private val cumulative: DoubleArray = DoubleArray(points.size)

    val totalMeters: Double

    init {
        var running = 0.0
        for (i in 1 until points.size) {
            running += haversineMeters(points[i - 1], points[i])
            cumulative[i] = running
        }
        totalMeters = running
    }

    val isUsable: Boolean get() = points.size >= 2

    /**
     * Nearest point on the route. [searchFrom] biases the scan forward from the
     * last known position so a route that doubles back on itself cannot snap
     * the driver backwards onto an earlier crossing of the same street.
     */
    fun match(point: LatLon, searchFrom: Double = 0.0): RouteMatch? {
        if (!isUsable) return null

        val startIndex = indexAtDistance(max(0.0, searchFrom - FORGIVENESS_M))
        var bestIndex = -1
        var bestT = 0.0
        var bestDistance = Double.MAX_VALUE
        var bestSnapped = points[0]

        for (i in startIndex until points.size - 1) {
            val projection = projectOntoSegment(point, points[i], points[i + 1])
            if (projection.distanceMeters < bestDistance) {
                bestDistance = projection.distanceMeters
                bestIndex = i
                bestT = projection.t
                bestSnapped = projection.snapped
            }
            // Once we are comfortably on the line, stop scanning the rest of a
            // long route: the first local minimum ahead is the right one.
            if (bestDistance < EARLY_EXIT_M && cumulative[i] > searchFrom + LOOKAHEAD_M) break
        }

        if (bestIndex < 0) return null
        // A biased scan that finds nothing close re-scans the whole line, which
        // is what recovers the match after a big jump (tunnel exit, GPS reset).
        if (bestDistance > RESCAN_M && searchFrom > 0.0) return match(point, 0.0)

        val segmentMeters = cumulative[bestIndex + 1] - cumulative[bestIndex]
        return RouteMatch(
            segmentIndex = bestIndex,
            snapped = bestSnapped,
            distanceAlong = cumulative[bestIndex] + segmentMeters * bestT,
            crossTrackMeters = bestDistance,
            bearing = bearingDegrees(points[bestIndex], points[bestIndex + 1])
        )
    }

    fun indexAtDistance(meters: Double): Int {
        if (meters <= 0.0) return 0
        var low = 0
        var high = cumulative.size - 1
        while (low < high) {
            val mid = (low + high) / 2
            if (cumulative[mid] < meters) low = mid + 1 else high = mid
        }
        return max(0, low - 1)
    }

    /** Index of the step being driven at [meters] along the route. */
    fun stepIndexAt(meters: Double): Int {
        val geometryIndex = indexAtDistance(meters)
        var step = 0
        for (i in route.steps.indices) {
            if (route.steps[i].at <= geometryIndex) step = i else break
        }
        return step
    }

    /** Distance from [meters] to the start of the following step, or null at the end. */
    fun metersToNextManeuver(meters: Double): Double? {
        val step = stepIndexAt(meters)
        val next = route.steps.getOrNull(step + 1) ?: return null
        val at = next.at.coerceIn(0, cumulative.size - 1)
        return max(0.0, cumulative[at] - meters)
    }

    fun pointAtDistance(meters: Double): LatLon {
        if (!isUsable) return points.firstOrNull() ?: LatLon(0.0, 0.0)
        val clamped = meters.coerceIn(0.0, totalMeters)
        val i = indexAtDistance(clamped).coerceAtMost(points.size - 2)
        val segment = cumulative[i + 1] - cumulative[i]
        val t = if (segment <= 0.0) 0.0 else (clamped - cumulative[i]) / segment
        return interpolate(points[i], points[i + 1], t)
    }

    /** Splits the line at [meters] so the traveled part can be drawn dimmed. */
    fun split(meters: Double): Pair<List<LatLon>, List<LatLon>> {
        if (!isUsable) return emptyList<LatLon>() to points
        val clamped = meters.coerceIn(0.0, totalMeters)
        val here = pointAtDistance(clamped)
        val i = indexAtDistance(clamped).coerceAtMost(points.size - 2)
        val traveled = points.subList(0, i + 1) + here
        val ahead = listOf(here) + points.subList(i + 1, points.size)
        return traveled to ahead
    }

    companion object {
        /** Allow matching slightly behind the last position (GPS jitter). */
        private const val FORGIVENESS_M = 40.0
        private const val EARLY_EXIT_M = 12.0
        private const val LOOKAHEAD_M = 60.0
        private const val RESCAN_M = 60.0
    }
}

data class RouteMatch(
    val segmentIndex: Int,
    val snapped: LatLon,
    val distanceAlong: Double,
    val crossTrackMeters: Double,
    val bearing: Double
)
