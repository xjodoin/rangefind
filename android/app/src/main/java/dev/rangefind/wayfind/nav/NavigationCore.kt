package dev.rangefind.wayfind.nav

import android.content.Context
import androidx.annotation.StringRes
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.engine.LaneTurn
import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.engine.Route
import dev.rangefind.wayfind.engine.speedLimitAt
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
    /**
     * How far the fix sat from the route line, and how far along the route the
     * match landed. Neither drives anything on screen; both go into the trace.
     *
     * A drawn position that leapt was unattributable without them: the trace
     * recorded where the arrow ended up, not why. A jump with the cross-track
     * steady is the projection hopping, a jump with the distance-along going
     * backwards is a rescan, and the two want opposite fixes.
     */
    val crossTrackMeters: Double,
    val distanceAlongMeters: Double,
    /**
     * Direction of the road the route matched to, independent of any fused
     * heading. The motion model needs a heading it did not itself produce.
     */
    val roadBearing: Double,
    val speedLimitKmh: Int,
    val turnDelta: Double,
    /** What the driver has to do next: turn, exit, merge or carry on. */
    val maneuver: Maneuver,
    /**
     * The road the next maneuver leads onto — past a slip road to the road it
     * actually joins. The banner and the spoken instruction both read this,
     * so they cannot disagree.
     */
    val maneuverTarget: String,
    /**
     * The exit number on the sign, when the next maneuver is a slip road
     * that has one. This is the largest thing written on the panel and the
     * only unambiguous way to identify an exit; a driver checking whether
     * this is theirs is looking for a number, not a road name.
     */
    val exitRef: String,
    /** Which exit to take at a roundabout; 0 when unknown. */
    val roundaboutExit: Int,
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
        hasBearing: Boolean,
        /**
         * The fix's own error bar, in metres. Off-route is a claim about
         * where the car is, and a claim cannot be more certain than its
         * evidence: on the recorded drives, 14 of 40 reroutes fired on a
         * cross-track smaller than the accuracy of the fix that produced it.
         * Nothing was wrong on the road — the receiver was guessing, and the
         * app rerouted on the guess.
         */
        accuracyMeters: Double = 0.0
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
        // window is not driving into oncoming cars. And it only means
        // anything once the car is somewhere the road is not: sitting on the
        // line with a noisy compass is not a wrong-way manoeuvre, and two of
        // the recorded reroutes fired from 7 m off the line on that alone.
        val headingWrong = mode != TravelMode.Walk &&
            speedMps > 2.0 &&
            match.crossTrackMeters > SNAP_TRUST_METERS &&
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

        // How far the fix could be wrong by, capped: a receiver claiming a
        // hundred-metre error is not a licence to stop noticing that the car
        // has left the road, only a reason to want better evidence first.
        val doubt = accuracyMeters.coerceIn(0.0, ACCURACY_ALLOWANCE_MAX_METERS)
        // A trip that starts in a pedestrian zone or a car park begins tens of
        // metres from the nearest routable road, so a standstill can never be
        // off-route; only a wild distance overrides that.
        val off = !arriving && (
            (match.crossTrackMeters > mode.offRouteMeters + doubt && moving) ||
                match.crossTrackMeters > mode.offRouteHardMeters + doubt ||
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
        // What the driver turns onto, looking through any junction stub the
        // router split out between here and there. [stepThroughJunction] has
        // the reasoning; where the next step is a real road — the ordinary
        // case — this is the next step and nothing below changes.
        val throughIndex = stepThroughJunction(route, stepIndex + 1)
        val next = route.steps.getOrNull(throughIndex)
        // Lane markings are the exception to looking through the junction:
        // they describe the approach *to* a step, so the ones a driver can see
        // through the windscreen belong to the step immediately ahead — the
        // stub's own approach — not to the road past it.
        val approaching = route.steps.getOrNull(stepIndex + 1)
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
        // The whole heading change through the junction, not just the half of
        // it past the stub — otherwise a left turn split into "straight" then
        // "left" is announced as the straight bit.
        val turnDelta = turnDeltaThrough(route, stepIndex + 1, throughIndex) { turnDeltaAt(route, it) }
        // Lane guidance only means anything close to the junction it belongs
        // to; further back it is a row of arrows about nothing yet, and it
        // would sit on screen for kilometres of open road.
        val maneuverKind = maneuverOnto(route, throughIndex, turnDelta)
        val maneuverTarget = when (maneuverKind) {
            // What the ramp is signed with beats where it structurally goes:
            // the panel says "40 Ouest / Montréal", and the road it joins is
            // called Autoroute Félix-Leclerc, which appears nowhere a driver
            // can see. Fall through to the road itself only when the ramp
            // carries no sign at all.
            Maneuver.Exit, Maneuver.Merge -> next?.towardLabel.orEmpty()
                .ifBlank { roadBeyondRamp(route, throughIndex) }
                .ifBlank { next?.name.orEmpty() }
            // At a roundabout the instruction is the road you come off onto.
            Maneuver.Roundabout -> roadBeyondRoundabout(route, throughIndex)
            else -> next?.let { signpostName(it) }.orEmpty()
        }
        val exitRef = next?.exitRef.orEmpty()
        val lanes = if (mode.showsRoadSigns && toManeuver <= LANE_GUIDANCE_METERS) {
            approaching?.lanes.orEmpty()
        } else emptyList()
        val laneUsable = usableLanes(lanes, turnDelta)
        val voice = when {
            arrived && !announcedArrival -> {
                announcedArrival = true
                context.getString(R.string.nav_arrived)
            }
            offRoute -> null
            else -> announcement(
                stepIndex = stepIndex,
                meters = toManeuver,
                nextName = next?.let { signpostName(it) }.orEmpty(),
                turnDelta = turnDelta,
                maneuver = maneuverKind,
                beyondName = maneuverTarget,
                exitRef = exitRef,
                roundaboutExit = next?.roundaboutExit ?: 0
            )
        }

        // Snapping the arrow to the route line is what keeps it steady on the
        // road while the fix jitters around. Once the car has genuinely left
        // that line the same snap becomes a lie: the arrow sits on a road the
        // driver is no longer on and stays there until a reroute lands,
        // which is the whole time they most need to see where they actually
        // are. Past the point where the deviation is larger than GPS error,
        // show the fix itself.
        //
        // Blended across a band rather than switched at a line. Choosing
        // between the two sources at a threshold means the drawn position
        // jumps by the whole cross-track distance the moment it is crossed —
        // measured at up to 24 m on recorded drives, which reads as the car
        // being flung sideways. Between the two distances the arrow sits
        // between the road and the fix, which is the honest picture of not
        // yet knowing which one is right.
        val position = interpolate(match.snapped, point, snapBlend(match.crossTrackMeters))

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
            crossTrackMeters = match.crossTrackMeters,
            distanceAlongMeters = match.distanceAlong,
            roadBearing = match.bearing,
            // Posted limits and lane markings are addressed to drivers.
            //
            // Read at the car's position along the route, not from the step it
            // is in. A street is not a limit: an autoroute drops through an
            // interchange and climbs back without changing its name, and the
            // step's single number is whichever limit covers most of it — so
            // the sign read 100 while the driver was in the 70.
            speedLimitKmh = if (mode.showsRoadSigns) {
                route.speedLimitAt(match.distanceAlong)
                    .takeIf { it > 0 }
                    ?: route.steps.getOrNull(stepIndex)?.speedLimitKmh ?: 0
            } else 0,
            turnDelta = turnDelta,
            maneuver = maneuverKind,
            maneuverTarget = maneuverTarget,
            exitRef = exitRef,
            roundaboutExit = next?.roundaboutExit ?: 0,
            lanes = lanes,
            laneUsable = laneUsable,
            offRoute = offRoute,
            arrived = arrived,
            alternatives = liveAlternatives(point, remainingSeconds, match.crossTrackMeters),
            voice = voice
        )
    }

    /** The first named road past a run of slip roads, which is where the ramp goes. */
    private fun roadBeyondRamp(route: Route, from: Int): String {
        var index = from
        while (index < route.steps.size && route.steps[index].roadClass.endsWith("_link")) index++
        return route.steps.getOrNull(index)?.let { signpostName(it) }.orEmpty()
    }

    /**
     * The road the roundabout is left by.
     *
     * The circle itself is the thing being entered, never the answer: a
     * driver told to "continue onto (unnamed)" at a giratoire has been told
     * nothing. What matters is the arm they come off on.
     */
    private fun roadBeyondRoundabout(route: Route, from: Int): String {
        var index = from
        while (index < route.steps.size && route.steps[index].roundabout) index++
        return route.steps.getOrNull(index)?.let { signpostName(it) }.orEmpty()
    }

    private fun announcement(
        stepIndex: Int,
        meters: Double,
        nextName: String,
        turnDelta: Double,
        maneuver: Maneuver,
        beyondName: String,
        exitRef: String,
        roundaboutExit: Int
    ): String? {
        val ramp = maneuver == Maneuver.Exit || maneuver == Maneuver.Merge
        val circle = maneuver == Maneuver.Roundabout
        val ferry = maneuver == Maneuver.Ferry
        // A slip road almost never has a name, and bailing on that left every
        // motorway entrance and exit silent — the two instructions on a long
        // drive a driver most needs spoken. A ramp is announced by what it is
        // and where it leads, not by a name it does not have. The same is
        // true of a roundabout, whose arcs are nameless by construction.
        if (nextName.isBlank() && !ramp && !circle && !ferry) return null
        // Nothing to say about a road merely changing name underfoot.
        if (maneuver == Maneuver.Continue && nextName.isBlank()) return null
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
        val imminent = threshold != null && threshold <= IMMINENT_METERS
        val rounded = (meters / 50).roundToInt() * 50
        if (ferry) {
            val target = beyondName.ifBlank { nextName }
            return if (imminent) context.getString(R.string.nav_ferry_now, target)
            else context.getString(R.string.nav_ferry_in_meters, rounded, target)
        }
        if (circle) {
            val target = beyondName.ifBlank { nextName }
            return when {
                // "Take the second exit" is the whole instruction, and it is
                // the one thing the turn angle could never supply.
                roundaboutExit > 0 && target.isNotBlank() ->
                    if (imminent) context.getString(R.string.nav_roundabout_exit_onto_now, roundaboutExit, target)
                    else context.getString(R.string.nav_roundabout_exit_onto_in_meters, rounded, roundaboutExit, target)
                roundaboutExit > 0 ->
                    if (imminent) context.getString(R.string.nav_roundabout_exit_now, roundaboutExit)
                    else context.getString(R.string.nav_roundabout_exit_in_meters, rounded, roundaboutExit)
                target.isNotBlank() ->
                    if (imminent) context.getString(R.string.nav_roundabout_onto_now, target)
                    else context.getString(R.string.nav_roundabout_onto_in_meters, rounded, target)
                else ->
                    if (imminent) context.getString(R.string.nav_roundabout_now)
                    else context.getString(R.string.nav_roundabout_in_meters, rounded)
            }
        }
        if (ramp) {
            val target = beyondName.ifBlank { nextName }
            // The exit number is what the driver is scanning the panels for,
            // so it leads. "Take exit 32 toward 40 Ouest" is the sign read
            // aloud; "take the exit" is a description of a road layout.
            if (maneuver == Maneuver.Exit && exitRef.isNotBlank()) {
                return when {
                    target.isBlank() ->
                        if (imminent) context.getString(R.string.nav_exit_number_now, exitRef)
                        else context.getString(R.string.nav_exit_number_in_meters, rounded, exitRef)
                    imminent -> context.getString(R.string.nav_exit_number_onto_now, exitRef, target)
                    else -> context.getString(R.string.nav_exit_number_onto_in_meters, rounded, exitRef, target)
                }
            }
            return when {
                maneuver == Maneuver.Exit && target.isBlank() ->
                    if (imminent) context.getString(R.string.nav_exit_now)
                    else context.getString(R.string.nav_exit_in_meters, rounded)
                maneuver == Maneuver.Exit ->
                    if (imminent) context.getString(R.string.nav_exit_onto_now, target)
                    else context.getString(R.string.nav_exit_onto_in_meters, rounded, target)
                target.isBlank() ->
                    if (imminent) context.getString(R.string.nav_merge_now)
                    else context.getString(R.string.nav_merge_in_meters, rounded)
                else ->
                    if (imminent) context.getString(R.string.nav_merge_onto_now, target)
                    else context.getString(R.string.nav_merge_onto_in_meters, rounded, target)
            }
        }
        return if (imminent) {
            context.getString(nowPhraseFor(turnDelta), nextName)
        } else {
            context.getString(inMetersPhraseFor(turnDelta), rounded, nextName)
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

        /**
         * Below this the fix is close enough to the line that the road is the
         * better answer, and the arrow is snapped outright. Ordinary GPS error
         * on an open road lives here.
         */
        const val SNAP_HOLD_METERS = 15.0

        /**
         * How much of the raw fix to show, from none of it on the line to all
         * of it once the car has plainly left. Continuous by construction: the
         * drawn position is a continuous function of the cross-track distance,
         * which is what stops the arrow teleporting when a drift crosses a
         * threshold.
         */
        fun snapBlend(crossTrackMeters: Double): Double = when {
            crossTrackMeters <= SNAP_HOLD_METERS -> 0.0
            crossTrackMeters >= SNAP_TRUST_METERS -> 1.0
            else -> (crossTrackMeters - SNAP_HOLD_METERS) / (SNAP_TRUST_METERS - SNAP_HOLD_METERS)
        }

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

        /**
         * The most a poor fix may excuse. Beyond this the receiver is not
         * measuring anything and its own error bar stops being informative;
         * the hard distance still catches a car that has genuinely gone.
         */
        const val ACCURACY_ALLOWANCE_MAX_METERS = 60.0
    }
}
