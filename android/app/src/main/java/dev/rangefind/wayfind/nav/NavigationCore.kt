package dev.rangefind.wayfind.nav

import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.engine.Route
import kotlin.math.max
import kotlin.math.roundToInt

/** A candidate the car could still take from where it actually is. */
data class NavAlternative(
    val index: Int,
    val remainingSeconds: Double,
    val deltaSeconds: Double,
    val alongMeters: Double
)

/** Everything a navigating surface needs from one location fix. */
data class NavUpdate(
    val stepIndex: Int,
    val stepName: String,
    val nextStepName: String,
    val metersToManeuver: Double,
    val remainingMeters: Double,
    val remainingSeconds: Double,
    val traveled: List<LatLon>,
    val ahead: List<LatLon>,
    val position: LatLon,
    val bearing: Double,
    val speedMps: Double,
    val speedLimitKmh: Int,
    val turnDelta: Double,
    val offRoute: Boolean,
    val arrived: Boolean,
    val alternatives: List<NavAlternative>,
    /** A phrase to speak this tick, or null. */
    val voice: String?
)

/**
 * The shared navigation state machine.
 *
 * The phone and the head unit are the same drive seen from two seats, so the
 * rules about what counts as off-route, when to speak, and which alternates
 * are still reachable live here once. Side effects stay with the caller: this
 * decides *that* a reroute is needed or *that* a phrase is due, and the
 * surface owning the engine and the speaker does it.
 */
class NavigationCore {

    private var trackers: List<RouteTracker> = emptyList()
    private var routes: List<Route> = emptyList()
    private var activeIndex = 0
    private var tracker: RouteTracker? = null

    private var lastAlong = 0.0
    private val altAlong = mutableMapOf<Int, Double>()
    private var announcedStep = -1
    private var announcedThreshold = Int.MAX_VALUE
    private var offRouteStrikes = 0
    private var announcedArrival = false

    val isRunning: Boolean get() = tracker != null
    val activeRoute: Route? get() = routes.getOrNull(activeIndex)

    fun start(candidates: List<Route>, index: Int): String? {
        routes = candidates
        trackers = candidates.map { RouteTracker(it) }
        activeIndex = index.coerceIn(0, maxOf(0, candidates.lastIndex))
        tracker = trackers.getOrNull(activeIndex)
        resetProgress()
        val first = activeRoute?.steps?.firstOrNull()?.name?.takeIf { it.isNotBlank() }
        return "Starting navigation." + (first?.let { " Head onto $it." } ?: "")
    }

    fun stop() {
        tracker = null
        trackers = emptyList()
        routes = emptyList()
        resetProgress()
    }

    /** Switch mid-drive; re-tracks from where the car already is. */
    fun selectRoute(index: Int): Boolean {
        val next = trackers.getOrNull(index) ?: return false
        activeIndex = index
        tracker = next
        lastAlong = altAlong[index] ?: 0.0
        announcedStep = -1
        announcedThreshold = Int.MAX_VALUE
        offRouteStrikes = 0
        return true
    }

    /** Adopt a freshly computed route after a reroute. */
    fun replaceRoutes(candidates: List<Route>) {
        routes = candidates
        trackers = candidates.map { RouteTracker(it) }
        activeIndex = 0
        tracker = trackers.firstOrNull()
        resetProgress()
    }

    private fun resetProgress() {
        lastAlong = 0.0
        altAlong.clear()
        announcedStep = -1
        announcedThreshold = Int.MAX_VALUE
        offRouteStrikes = 0
        announcedArrival = false
    }

    fun onLocation(
        point: LatLon,
        speedMps: Double,
        gpsBearing: Double?,
        hasBearing: Boolean
    ): NavUpdate? {
        val active = tracker ?: return null
        val route = active.route
        val match = active.match(point, lastAlong) ?: return null

        // GPS bearing is unreliable at low speed; the road's own is better.
        val moving = speedMps > 1.5
        val heading = if (moving && hasBearing && gpsBearing != null) gpsBearing else match.bearing
        val headingWrong = speedMps > 2.0 && absBearingDelta(match.bearing, heading) > 110.0

        // A trip that starts in a pedestrian zone or a car park begins tens of
        // metres from the nearest routable road, so a standstill can never be
        // off-route; only a wild distance overrides that.
        val off = (match.crossTrackMeters > OFF_ROUTE_METERS && moving) ||
            match.crossTrackMeters > OFF_ROUTE_HARD_METERS ||
            headingWrong
        offRouteStrikes = if (off) offRouteStrikes + 1 else 0

        lastAlong = max(lastAlong, match.distanceAlong)
        val (traveled, ahead) = active.split(lastAlong)

        // Arrival is judged on geometry, but the numbers shown are scaled from
        // the route's own totals: summing haversine over a simplified polyline
        // drifts from the index's road distances.
        val geometric = max(0.0, active.totalMeters - lastAlong)
        val fraction = if (active.totalMeters <= 0) 0.0
        else (geometric / active.totalMeters).coerceIn(0.0, 1.0)
        val remainingMeters = route.distanceMeters * fraction
        val remainingSeconds = route.seconds * fraction
        val stepIndex = active.stepIndexAt(lastAlong)
        val next = route.steps.getOrNull(stepIndex + 1)
        val toManeuver = active.metersToNextManeuver(lastAlong) ?: geometric
        val arrived = geometric < ARRIVAL_METERS

        val offRoute = offRouteStrikes >= OFF_ROUTE_STRIKES
        val voice = when {
            arrived && !announcedArrival -> {
                announcedArrival = true
                "You have arrived."
            }
            offRoute -> null
            else -> announcement(stepIndex, toManeuver, next?.name.orEmpty())
        }

        return NavUpdate(
            stepIndex = stepIndex,
            stepName = route.steps.getOrNull(stepIndex)?.name.orEmpty(),
            nextStepName = next?.name.orEmpty(),
            metersToManeuver = toManeuver,
            remainingMeters = remainingMeters,
            remainingSeconds = remainingSeconds,
            traveled = traveled,
            ahead = ahead,
            position = match.snapped,
            bearing = heading,
            speedMps = speedMps,
            speedLimitKmh = route.steps.getOrNull(stepIndex)?.speedLimitKmh ?: 0,
            turnDelta = turnDeltaAt(route, next?.at),
            offRoute = offRoute,
            arrived = arrived,
            alternatives = liveAlternatives(point, remainingSeconds, match.crossTrackMeters),
            voice = voice
        )
    }

    private fun announcement(stepIndex: Int, meters: Double, nextName: String): String? {
        if (nextName.isBlank()) return null
        val threshold = ANNOUNCE_THRESHOLDS.firstOrNull { meters <= it } ?: return null
        if (stepIndex == announcedStep && threshold >= announcedThreshold) return null
        announcedStep = stepIndex
        announcedThreshold = threshold
        return if (threshold <= 60) "Now, continue onto $nextName."
        else "In ${(meters / 50).roundToInt() * 50} meters, continue onto $nextName."
    }

    /**
     * An alternate stays live only while the car could still take it. Its
     * distance is judged against the active line's own offset: if the car is
     * 80 m from the route because of where it is parked, it is equally 80 m
     * from the alternate, and that alternate is still a real option.
     */
    private fun liveAlternatives(
        point: LatLon,
        activeRemainingSeconds: Double,
        activeCrossTrackMeters: Double
    ): List<NavAlternative> {
        val limit = max(ALT_LIVE_METERS, activeCrossTrackMeters + 25.0)
        return trackers.mapIndexedNotNull { index, candidate ->
            if (index == activeIndex || !candidate.isUsable) return@mapIndexedNotNull null
            val match = candidate.match(point, altAlong[index] ?: 0.0)
                ?: return@mapIndexedNotNull null
            if (match.crossTrackMeters > limit) {
                altAlong.remove(index)
                return@mapIndexedNotNull null
            }
            val along = max(altAlong[index] ?: 0.0, match.distanceAlong)
            altAlong[index] = along
            val total = candidate.totalMeters
            val fraction = if (total <= 0) 0.0 else (1 - along / total).coerceIn(0.0, 1.0)
            val remaining = (routes.getOrNull(index)?.seconds ?: 0.0) * fraction
            NavAlternative(index, remaining, remaining - activeRemainingSeconds, along)
        }
    }

    /** Signed turn angle at a step boundary, for the maneuver glyph. */
    private fun turnDeltaAt(route: Route, at: Int?): Double {
        val index = at ?: return 0.0
        val points = route.geometry
        if (index <= 0 || index >= points.size - 1) return 0.0
        return bearingDelta(
            bearingDegrees(points[index - 1], points[index]),
            bearingDegrees(points[index], points[index + 1])
        )
    }

    companion object {
        const val OFF_ROUTE_METERS = 45.0
        /** Distance that means "off route" even at a standstill. */
        const val OFF_ROUTE_HARD_METERS = 150.0
        const val OFF_ROUTE_STRIKES = 3
        const val ARRIVAL_METERS = 25.0
        /** How far off an alternate the car can be before it stops being one. */
        const val ALT_LIVE_METERS = 60.0
        private val ANNOUNCE_THRESHOLDS = listOf(60, 200, 400)
    }
}
