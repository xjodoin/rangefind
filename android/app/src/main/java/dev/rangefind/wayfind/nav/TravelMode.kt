package dev.rangefind.wayfind.nav

import androidx.annotation.StringRes
import dev.rangefind.wayfind.R

/**
 * How the trip is being made.
 *
 * Each mode is a separate route index — a pedestrian may cross a square a car
 * cannot enter, and a car may take a dual carriageway no cyclist should be on,
 * so the graphs genuinely differ rather than sharing one with filters applied.
 * The [profile] is the name the extractor and the index header use, and the
 * region id is that name appended to the place, which is how the indexes are
 * published.
 *
 * The differences that follow are not cosmetic. Guidance tuned for a car
 * misjudges a walk badly: a pedestrian crosses mid-block, cuts corners and
 * doubles back, all of which reads as leaving the route to rules written for
 * something that has to stay between kerbs.
 */
enum class TravelMode(
    val profile: String,
    @StringRes val labelRes: Int
) {
    Car("car", R.string.mode_drive),
    Bike("bike", R.string.mode_cycle),
    Walk("foot", R.string.mode_walk);

    /** Region id for a place in this mode, matching how indexes are published. */
    fun regionId(place: String): String = if (this == Car) place else "$place-$profile"

    /** The place a region id belongs to, whichever mode published it. */
    val suffix: String get() = if (this == Car) "" else "-$profile"

    /**
     * How far off the line counts as having left the route.
     *
     * A car that is fifty metres from its road is on a different road. A
     * pedestrian fifty metres away has crossed a square, cut through a car
     * park, or is on the other pavement — the network simply does not describe
     * where people actually walk in that much detail, and rerouting them for
     * it would nag the whole way.
     */
    val offRouteMeters: Double
        get() = when (this) {
            Car -> 45.0
            Bike -> 40.0
            Walk -> 80.0
        }

    /** Distance that means off-route even at a standstill. */
    val offRouteHardMeters: Double
        get() = when (this) {
            Car -> 150.0
            Bike -> 150.0
            Walk -> 250.0
        }

    /**
     * How many consecutive fixes must agree before the route is recomputed.
     * Walking gives noisier fixes at lower speed, so it waits longer rather
     * than recomputing on a wander.
     */
    val offRouteStrikes: Int
        get() = if (this == Walk) 5 else 3

    /**
     * How close counts as arrived. A car parks near the destination; someone
     * on foot reaches the door, and being told they have arrived from fifty
     * metres away is an instruction to stop short of it.
     */
    val arrivalMeters: Double
        get() = when (this) {
            Car -> 25.0
            Bike -> 20.0
            Walk -> 12.0
        }

    /**
     * The apron around the destination inside which leaving the route is
     * arriving rather than straying. Sized for a supermarket car park when
     * driving; a walk ends where it ends, so it barely needs one.
     */
    val arrivalApronMeters: Double
        get() = when (this) {
            Car -> 150.0
            Bike -> 60.0
            Walk -> 25.0
        }

    /** Below this the reported bearing is noise rather than a direction. */
    val movingSpeedMps: Double
        get() = if (this == Walk) 0.4 else 1.5

    /** Posted limits and lane markings are addressed to drivers. */
    val showsRoadSigns: Boolean get() = this == Car

    companion object {
        fun ofProfile(profile: String?): TravelMode =
            entries.firstOrNull { it.profile == profile } ?: Car
    }
}
