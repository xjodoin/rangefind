package dev.rangefind.wayfind.nav

import android.content.Context
import androidx.annotation.StringRes
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.engine.LaneTurn
import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.engine.Route
import kotlin.math.abs
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
    /** Lanes of the approach to the next maneuver, left to right. */
    val lanes: List<Int>,
    /** Which of those lanes lead where the route goes. */
    val laneUsable: List<Boolean>,
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
 *
 * The [context] is only ever used to resolve the spoken phrases, so guidance
 * is spoken in the user's language rather than the one it was written in.
 */
class NavigationCore(private val context: Context) {

    /**
     * How the trip is being made. Every threshold below reads from this
     * rather than assuming a car: the same numbers that keep a driver on
     * their road would have a pedestrian rerouted for crossing the street.
     */
    var mode: TravelMode = TravelMode.Car

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
    private var announcedFirst = false

    val isRunning: Boolean get() = tracker != null
    val activeRoute: Route? get() = routes.getOrNull(activeIndex)

    fun start(candidates: List<Route>, index: Int): String? {
        routes = candidates
        trackers = candidates.map { RouteTracker(it) }
        activeIndex = index.coerceIn(0, maxOf(0, candidates.lastIndex))
        tracker = trackers.getOrNull(activeIndex)
        resetProgress()
        val first = activeRoute?.steps?.firstOrNull()?.name?.takeIf { it.isNotBlank() }
        return if (first != null) context.getString(R.string.nav_starting_onto, first)
        else context.getString(R.string.nav_starting)
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
        announcedFirst = false
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
        announcedFirst = false
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
        val moving = speedMps > mode.movingSpeedMps
        val heading = when {
            moving && hasBearing && gpsBearing != null -> gpsBearing
            // Off the line, the route's bearing describes a road the car is
            // not on. A stale GPS bearing is at least still about this car.
            match.crossTrackMeters > SNAP_TRUST_METERS && gpsBearing != null -> gpsBearing
            else -> match.bearing
        }
        // Going the wrong way down the road only means something for traffic
        // that has a way to be on. Someone on foot turning to look at a shop
        // window is not driving into oncoming cars.
        val headingWrong = mode != TravelMode.Walk &&
            speedMps > 2.0 &&
            absBearingDelta(match.bearing, heading) > 110.0

        // How far the car still is from where the route ends. Needed before the
        // off-route test, not just for arrival.
        val destination = route.geometry.lastOrNull()
        val toDestination = destination?.let { haversineMeters(point, it) } ?: Double.MAX_VALUE

        // The last hundred metres of a trip are usually not on the road
        // network at all: a supermarket car park, a forecourt, a driveway.
        // Leaving the line there is arriving, not straying, and rerouting a
        // driver who has already parked is worse than saying nothing — so
        // inside the destination's apron nothing counts as off-route.
        val arriving = toDestination < mode.arrivalApronMeters

        // A trip that starts in a pedestrian zone or a car park begins tens of
        // metres from the nearest routable road, so a standstill can never be
        // off-route; only a wild distance overrides that.
        val off = !arriving && (
            (match.crossTrackMeters > mode.offRouteMeters && moving) ||
                match.crossTrackMeters > mode.offRouteHardMeters ||
                headingWrong
            )
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
        // Arrival used to depend purely on progress along the line. When the
        // match saturates short of the end — a destination set back from the
        // road, a polyline that stops at the kerb — the remaining distance
        // never reaches zero and the drive never completes. Standing at the
        // destination counts regardless of what the odometer says.
        // Parking is how a drive ends. Inside the apron, a car that has come
        // to a stop and is no longer on the road line has arrived: waiting for
        // it to roll within 25 m of a kerb point it may never approach leaves
        // guidance talking to an empty car park. Stopped *on* the line is a
        // red light rather than an arrival, which is what the cross-track test
        // separates.
        val parked = arriving &&
            speedMps < PARKED_SPEED_MPS &&
            match.crossTrackMeters > mode.offRouteMeters
        val arrived = geometric < mode.arrivalMeters ||
            toDestination < mode.arrivalMeters ||
            parked

        val offRoute = offRouteStrikes >= mode.offRouteStrikes
        val turnDelta = turnDeltaAt(route, next?.at)
        // Lane guidance only means anything close to the junction it belongs
        // to; further back it is a row of arrows about nothing yet, and it
        // would sit on screen for kilometres of open road.
        val lanes = if (mode.showsRoadSigns && toManeuver <= LANE_GUIDANCE_METERS) {
            next?.lanes.orEmpty()
        } else emptyList()
        val laneUsable = usableLanes(lanes, turnDelta)
        val voice = when {
            arrived && !announcedArrival -> {
                announcedArrival = true
                context.getString(R.string.nav_arrived)
            }
            offRoute -> null
            else -> announcement(stepIndex, toManeuver, next?.name.orEmpty(), turnDelta)
        }

        // Snapping the arrow to the route line is what keeps it steady on the
        // road while the fix jitters around. Once the car has genuinely left
        // that line the same snap becomes a lie: the arrow sits on a road the
        // driver is no longer on and stays there until a reroute lands,
        // which is the whole time they most need to see where they actually
        // are. Past the point where the deviation is larger than GPS error,
        // show the fix itself.
        val position = if (match.crossTrackMeters > SNAP_TRUST_METERS) point else match.snapped

        return NavUpdate(
            stepIndex = stepIndex,
            stepName = route.steps.getOrNull(stepIndex)?.name.orEmpty(),
            nextStepName = next?.name.orEmpty(),
            metersToManeuver = toManeuver,
            remainingMeters = remainingMeters,
            remainingSeconds = remainingSeconds,
            traveled = traveled,
            ahead = ahead,
            position = position,
            bearing = heading,
            speedMps = speedMps,
            // Posted limits and lane markings are addressed to drivers.
            speedLimitKmh = if (mode.showsRoadSigns) {
                route.steps.getOrNull(stepIndex)?.speedLimitKmh ?: 0
            } else 0,
            turnDelta = turnDelta,
            lanes = lanes,
            laneUsable = laneUsable,
            offRoute = offRoute,
            arrived = arrived,
            alternatives = liveAlternatives(point, remainingSeconds, match.crossTrackMeters),
            voice = voice
        )
    }

    private fun announcement(
        stepIndex: Int,
        meters: Double,
        nextName: String,
        turnDelta: Double
    ): String? {
        if (nextName.isBlank()) return null
        val threshold = ANNOUNCE_THRESHOLDS.firstOrNull { meters <= it }
        // The first instruction goes out as soon as the drive starts, however
        // far off the first maneuver is. Waiting for the 400 m threshold meant
        // a driver pulling away was told about the road *after* the one they
        // had to take first, and never about that first one at all.
        val opening = !announcedFirst
        if (threshold == null && !opening) return null
        if (!opening && stepIndex == announcedStep && threshold!! >= announcedThreshold) return null
        announcedFirst = true
        announcedStep = stepIndex
        announcedThreshold = threshold ?: Int.MAX_VALUE
        // "Now" only once the maneuver is genuinely at the windscreen; an
        // opening call from 2 km out still has to say how far.
        return if (threshold != null && threshold <= IMMINENT_METERS) {
            context.getString(nowPhraseFor(turnDelta), nextName)
        } else {
            context.getString(
                inMetersPhraseFor(turnDelta),
                (meters / 50).roundToInt() * 50,
                nextName
            )
        }
    }

    /**
     * Guidance used to say "continue onto X" for every maneuver, including
     * hard turns: the turn angle was computed for the on-screen glyph and
     * never reached the voice. A driver listening rather than looking got no
     * turn at all.
     */
    @StringRes
    private fun nowPhraseFor(delta: Double): Int = when {
        abs(delta) > U_TURN_DEGREES -> R.string.nav_uturn_now
        delta <= -TURN_DEGREES -> R.string.nav_turn_left_now
        delta <= -BEAR_DEGREES -> R.string.nav_bear_left_now
        delta >= TURN_DEGREES -> R.string.nav_turn_right_now
        delta >= BEAR_DEGREES -> R.string.nav_bear_right_now
        else -> R.string.nav_continue_now
    }

    @StringRes
    private fun inMetersPhraseFor(delta: Double): Int = when {
        abs(delta) > U_TURN_DEGREES -> R.string.nav_uturn_in_meters
        delta <= -TURN_DEGREES -> R.string.nav_turn_left_in_meters
        delta <= -BEAR_DEGREES -> R.string.nav_bear_left_in_meters
        delta >= TURN_DEGREES -> R.string.nav_turn_right_in_meters
        delta >= BEAR_DEGREES -> R.string.nav_bear_right_in_meters
        else -> R.string.nav_continue_in_meters
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

    /**
     * Which lanes of an approach lead where the route goes.
     *
     * A lane is usable when it allows the movement the route is about to
     * make. Lanes tagged with no movement at all say only how many lanes
     * there are, and marking some of those as wrong would be inventing
     * guidance the map never carried — so when nothing matches, every lane
     * is left unmarked rather than shown as forbidden.
     */
    private fun usableLanes(lanes: List<Int>, turnDelta: Double): List<Boolean> {
        if (lanes.isEmpty()) return emptyList()
        val wanted = when {
            abs(turnDelta) > U_TURN_DEGREES -> LaneTurn.REVERSE
            turnDelta <= -TURN_DEGREES -> LaneTurn.LEFT or LaneTurn.SHARP_LEFT
            turnDelta <= -BEAR_DEGREES -> LaneTurn.SLIGHT_LEFT or LaneTurn.LEFT
            turnDelta >= TURN_DEGREES -> LaneTurn.RIGHT or LaneTurn.SHARP_RIGHT
            turnDelta >= BEAR_DEGREES -> LaneTurn.SLIGHT_RIGHT or LaneTurn.RIGHT
            else -> LaneTurn.THROUGH
        }
        val matches = lanes.map { it and wanted != 0 }
        return if (matches.any { it }) matches else lanes.map { false }
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
        /**
         * Below this the reported bearing is noise, not a direction of travel:
         * a stationary GPS heading wanders freely, and handing that to the
         * router would price a U-turn against a driver who is free to pull
         * away whichever way they like.
         */
        const val MIN_HEADING_SPEED_MPS = 2.5

        /**
         * How far the fix may sit from the route before the arrow stops being
         * snapped to it. Comfortably beyond ordinary GPS error on a road, and
         * well below the off-route threshold, so the arrow leaves the line as
         * soon as the driver does rather than waiting for a reroute.
         */
        const val SNAP_TRUST_METERS = 25.0

        /** Slow enough to be stationary rather than crawling. */
        const val PARKED_SPEED_MPS = 0.8
        /** How far off an alternate the car can be before it stops being one. */
        const val ALT_LIVE_METERS = 60.0
        private val ANNOUNCE_THRESHOLDS = listOf(60, 200, 400)
        /** Close enough that the instruction is "now" rather than a distance. */
        private const val IMMINENT_METERS = 60
        /** Turn-angle bands, matching the on-screen maneuver glyph. */
        private const val BEAR_DEGREES = 22.0
        private const val TURN_DEGREES = 60.0
        private const val U_TURN_DEGREES = 150.0
        /** How close the maneuver must be before lane guidance is shown. */
        private const val LANE_GUIDANCE_METERS = 700.0
    }
}
