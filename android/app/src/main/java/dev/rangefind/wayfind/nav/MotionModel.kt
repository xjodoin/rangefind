package dev.rangefind.wayfind.nav

import android.location.Location
import dev.rangefind.wayfind.engine.LatLon
import kotlin.math.abs
import kotlin.math.cos
import kotlin.math.max
import kotlin.math.min
import kotlin.math.sin

/**
 * Turns a stream of location fixes into a pose that moves like a vehicle.
 *
 * A fix arrives about once a second and is a jump: the last one was here,
 * this one is twelve metres further on. Drawn straight, that is a puck that
 * sits still and then leaps, sixty times a second showing the same frame.
 * Easing towards each fix hides the leap but adds lag, and neither approach
 * knows that a car at 30 km/h covers eight metres a second and cannot be
 * somewhere else in between.
 *
 * So the pose here is dead-reckoned. Between fixes it carries on at the speed
 * and heading it had, which is what the vehicle is actually doing, and each
 * new fix corrects that estimate rather than replacing it. Corrections are
 * weighted by how much the fix deserves to be believed — a fix reporting
 * fifty metres of accuracy moves the estimate far less than one reporting
 * five — and a fix implying a speed no vehicle in this mode could reach is
 * treated as the noise it is.
 *
 * Heading gets the same treatment. A satellite course is only meaningful once
 * moving; below that it wanders freely, which is why a stationary car used to
 * spin on the map. Slow or stopped, the road's own direction and the phone's
 * compass are better answers, and whatever the source, the heading may only
 * change as fast as a vehicle at that speed could turn.
 */
class MotionModel {

    data class Pose(
        val position: LatLon,
        val bearing: Double,
        val speedMps: Double
    )

    private var lat = Double.NaN
    private var lon = Double.NaN
    private var bearing = 0.0
    private var speed = 0.0
    private var atMs = 0L

    val hasFix: Boolean get() = !lat.isNaN()

    fun reset() {
        lat = Double.NaN
        lon = Double.NaN
        bearing = 0.0
        speed = 0.0
        atMs = 0L
    }

    /**
     * Where the vehicle is now, carried forward from the last corrected
     * estimate. Extrapolation is capped: with the signal gone, a confidently
     * wrong position that keeps sailing down the road is worse than one that
     * stops and waits.
     */
    fun pose(nowMs: Long): Pose? {
        if (!hasFix) return null
        val elapsed = ((nowMs - atMs).coerceAtLeast(0L)).toDouble() / 1000.0
        val carried = min(elapsed, MAX_DEAD_RECKON_SECONDS)
        val travelled = speed * carried
        val position = if (travelled > 0.05) advance(LatLon(lat, lon), bearing, travelled)
        else LatLon(lat, lon)
        return Pose(position, bearing, speed)
    }

    /**
     * Folds a fix into the estimate.
     *
     * [roadBearing] is the direction of the road the route matched to, and
     * [compassBearing] the phone's own idea of which way it is pointing;
     * both are used where a satellite course is not trustworthy.
     */
    fun onFix(
        location: Location,
        roadBearing: Double?,
        compassBearing: Double?,
        mode: TravelMode,
        nowMs: Long
    ) {
        val fix = LatLon(location.latitude, location.longitude)
        val fixSpeed = if (location.hasSpeed()) location.speed.toDouble() else 0.0
        val accuracy = if (location.hasAccuracy()) location.accuracy.toDouble() else UNKNOWN_ACCURACY

        if (!hasFix) {
            lat = fix.lat
            lon = fix.lon
            speed = fixSpeed
            bearing = firstBearing(location, roadBearing, compassBearing)
            atMs = nowMs
            return
        }

        val elapsed = max(0.001, (nowMs - atMs).toDouble() / 1000.0)
        val predicted = pose(nowMs)?.position ?: fix
        val gap = haversineMeters(predicted, fix)

        // What the fix claims the vehicle did since the last one. Beyond what
        // the mode can manage it is the fix that is wrong, not the vehicle
        // that is extraordinary — unless the fix is also confident, in which
        // case the estimate is the thing that has drifted.
        val impliedSpeed = gap / elapsed
        val implausible = impliedSpeed > mode.topPlausibleSpeedMps * IMPLAUSIBLE_FACTOR
        val trust = when {
            implausible && accuracy > COARSE_ACCURACY_METERS -> STRAY_WEIGHT
            implausible -> LOOSE_WEIGHT
            accuracy <= FINE_ACCURACY_METERS -> TIGHT_WEIGHT
            accuracy >= COARSE_ACCURACY_METERS -> LOOSE_WEIGHT
            else -> {
                val span = COARSE_ACCURACY_METERS - FINE_ACCURACY_METERS
                val t = (accuracy - FINE_ACCURACY_METERS) / span
                TIGHT_WEIGHT + (LOOSE_WEIGHT - TIGHT_WEIGHT) * t
            }
        }

        val corrected = interpolate(predicted, fix, trust)
        lat = corrected.lat
        lon = corrected.lon
        // Speed follows the fix but not instantly: a reading that halves for
        // one second is usually the receiver, not the brakes.
        speed = speed + (fixSpeed - speed) * SPEED_EASE
        bearing = steer(
            towards = chooseBearing(location, roadBearing, compassBearing),
            elapsed = elapsed
        )
        atMs = nowMs
    }

    private fun firstBearing(
        location: Location,
        roadBearing: Double?,
        compassBearing: Double?
    ): Double = chooseBearing(location, roadBearing, compassBearing) ?: 0.0

    /**
     * Which heading to believe.
     *
     * A satellite course is derived from movement, so it is excellent once
     * moving and meaningless when not. The road's direction is the next best
     * thing for anything on it, and the compass is what remains for a vehicle
     * that has stopped.
     */
    private fun chooseBearing(
        location: Location,
        roadBearing: Double?,
        compassBearing: Double?
    ): Double? {
        val course = if (location.hasBearing()) location.bearing.toDouble() else null
        val fixSpeed = if (location.hasSpeed()) location.speed.toDouble() else 0.0
        return when {
            course != null && fixSpeed >= COURSE_TRUST_MPS -> course
            roadBearing != null && fixSpeed >= ROAD_TRUST_MPS -> roadBearing
            compassBearing != null -> compassBearing
            roadBearing != null -> roadBearing
            else -> course
        }
    }

    /**
     * Rotates towards a heading no faster than the vehicle could turn.
     *
     * Yaw rate falls away with speed — a car at a walking pace can spin on the
     * spot, one at motorway speed changes direction slowly — so the limit is
     * tied to it. This is what stops the view swinging about on a noisy fix
     * while still letting it come round a corner in time.
     */
    private fun steer(towards: Double?, elapsed: Double): Double {
        val target = towards ?: return bearing
        val limit = if (speed < CRAWL_MPS) FREE_TURN_DEGREES_PER_SECOND
        else min(FREE_TURN_DEGREES_PER_SECOND, TURN_RATE_CONSTANT / speed)
        val allowed = limit * elapsed
        val delta = bearingDelta(bearing, target)
        val step = if (abs(delta) <= allowed) delta else allowed * (if (delta < 0) -1.0 else 1.0)
        return normalizeBearing(bearing + step)
    }

    private companion object {
        /** How long a position may be carried on nothing but its last heading. */
        const val MAX_DEAD_RECKON_SECONDS = 2.5

        const val FINE_ACCURACY_METERS = 12.0
        const val COARSE_ACCURACY_METERS = 45.0
        const val UNKNOWN_ACCURACY = 60.0

        /** How far the estimate moves towards a fix, by how much it is believed. */
        const val TIGHT_WEIGHT = 0.55
        const val LOOSE_WEIGHT = 0.18
        /** A fix that cannot be true gets a nudge, not a jump. */
        const val STRAY_WEIGHT = 0.04
        const val IMPLAUSIBLE_FACTOR = 1.8

        const val SPEED_EASE = 0.45

        /** Above this a satellite course describes real movement. */
        const val COURSE_TRUST_MPS = 2.5
        const val ROAD_TRUST_MPS = 0.8
        const val CRAWL_MPS = 2.0

        const val FREE_TURN_DEGREES_PER_SECOND = 180.0
        /** Divided by speed: about 50°/s at 30 km/h, 14°/s at 100 km/h. */
        const val TURN_RATE_CONSTANT = 400.0
    }
}

/** Moves a point [meters] along [bearing]. */
fun advance(from: LatLon, bearing: Double, meters: Double): LatLon {
    val radius = 6371000.0
    val angular = meters / radius
    val heading = Math.toRadians(bearing)
    val lat1 = Math.toRadians(from.lat)
    val lon1 = Math.toRadians(from.lon)
    val lat2 = kotlin.math.asin(
        sin(lat1) * cos(angular) + cos(lat1) * sin(angular) * cos(heading)
    )
    val lon2 = lon1 + kotlin.math.atan2(
        sin(heading) * sin(angular) * cos(lat1),
        cos(angular) - sin(lat1) * sin(lat2)
    )
    return LatLon(Math.toDegrees(lat2), Math.toDegrees(lon2))
}
