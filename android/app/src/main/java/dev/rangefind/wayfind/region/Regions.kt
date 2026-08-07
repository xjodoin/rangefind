package dev.rangefind.wayfind.region

import android.content.Context
import androidx.annotation.StringRes
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.nav.TravelMode

/**
 * A route index the user can keep on the device. The label is a place name and
 * stays as authored; the note is prose, so it travels as a string resource.
 */
data class RegionSpec(
    val id: String,
    val label: String,
    @StringRes val noteRes: Int,
    /** Which way of travelling this index describes. */
    val mode: TravelMode = TravelMode.Car
)

enum class RegionStatus { Absent, Downloading, Ready, Failed }

data class RegionEntry(
    val spec: RegionSpec,
    val status: RegionStatus,
    val bytes: Long = 0,
    val done: Int = 0,
    val total: Int = 0,
    val updatedAt: Long = 0,
    val error: String? = null,
    val active: Boolean = false,
    /**
     * The server is publishing a different index than the one stored here.
     * Only ever set from an answer actually received: unreachable is unknown,
     * and an offline phone must not be told its offline region is stale.
     */
    val stale: Boolean = false
) {
    val progress: Float get() = if (total <= 0) 0f else done.toFloat() / total
}

/**
 * The catalogue is deliberately small and explicit rather than discovered:
 * an index is a deliberate multi-megabyte download, so the user should see
 * exactly what they are about to keep.
 */
val REGION_CATALOG = listOf(
    RegionSpec("luxembourg", "Luxembourg", R.string.region_note_luxembourg),
    RegionSpec("quebec", "Québec", R.string.region_note_quebec),
    // Walking and cycling are separate graphs, not the driving one filtered:
    // a pedestrian crosses squares a car cannot enter, and a cyclist avoids
    // roads a car belongs on. Each is its own download for that reason.
    RegionSpec("quebec-foot", "Québec", R.string.region_note_quebec, TravelMode.Walk),
    RegionSpec("quebec-bike", "Québec", R.string.region_note_quebec, TravelMode.Bike)
)

/**
 * A drive the app was in the middle of when it was interrupted.
 *
 * Only the destination, deliberately. Resuming means routing again from
 * wherever the car is now — a saved route computed before a five-minute phone
 * call starts somewhere the car has long since left.
 */
data class ActiveTrip(
    val lat: Double,
    val lon: Double,
    val name: String,
    val address: String,
    val startedAtMillis: Long
)

/**
 * Remembers which region is in use and where they are fetched from. The host
 * is editable because a phone on Wi-Fi cannot reach the emulator's 10.0.2.2
 * loopback alias — it needs the development machine's LAN address.
 */
class RegionPreferences(context: Context) {

    private val prefs = context.getSharedPreferences("wayfind.regions", Context.MODE_PRIVATE)

    var host: String
        get() = prefs.getString(KEY_HOST, DEFAULT_HOST) ?: DEFAULT_HOST
        set(value) = prefs.edit().putString(KEY_HOST, value.trim().trimEnd('/')).apply()

    var activeRegion: String?
        get() = prefs.getString(KEY_ACTIVE, null)
        set(value) = prefs.edit().apply {
            if (value == null) remove(KEY_ACTIVE) else putString(KEY_ACTIVE, value)
        }.apply()

    /**
     * Whether the user has ever picked a routing source. A null [activeRegion]
     * otherwise means two different things — "chose the network" and "has not
     * chosen yet" — and only the second is the app's to resolve.
     */
    var hasChosenSource: Boolean
        get() = prefs.getBoolean(KEY_CHOSEN, false)
        set(value) = prefs.edit().putBoolean(KEY_CHOSEN, value).apply()

    /**
     * Whether drives are traced for diagnostics. Off by default: a trace is a
     * precise record of where the user drove, so it is only ever written when
     * they have deliberately asked for one.
     */
    var recordTrips: Boolean
        get() = prefs.getBoolean(KEY_RECORD, false)
        set(value) = prefs.edit().putBoolean(KEY_RECORD, value).apply()

    /** The way of travelling the user last chose. */
    var travelMode: TravelMode
        get() = TravelMode.ofProfile(prefs.getString(KEY_MODE, null))
        set(value) = prefs.edit().putString(KEY_MODE, value.profile).apply()

    /**
     * The drive in progress, so it survives the app being taken away.
     *
     * A phone call can cost the app its activity, and with the activity goes
     * the view model and everything the drive knew — where it was going most
     * of all. The driver comes back to a search box. Storing the destination
     * is what lets guidance be picked up again from wherever the car has got
     * to by then, which is the only sensible place to resume from anyway:
     * the old route describes a position several minutes stale.
     *
     * Cleared when the drive ends, by arrival or by hand, so returning to the
     * app the next morning does not offer to resume yesterday.
     */
    var activeTrip: ActiveTrip?
        get() {
            // Stored as text: SharedPreferences has no double, and a float
            // rounds a coordinate by about a metre.
            val lat = prefs.getString(KEY_TRIP_LAT, null)?.toDoubleOrNull()
            val lon = prefs.getString(KEY_TRIP_LON, null)?.toDoubleOrNull()
            if (lat == null || lon == null) return null
            return ActiveTrip(
                lat = lat,
                lon = lon,
                name = prefs.getString(KEY_TRIP_NAME, "").orEmpty(),
                address = prefs.getString(KEY_TRIP_ADDRESS, "").orEmpty(),
                startedAtMillis = prefs.getLong(KEY_TRIP_AT, 0L)
            )
        }
        set(value) = prefs.edit().apply {
            if (value == null) {
                remove(KEY_TRIP_LAT); remove(KEY_TRIP_LON)
                remove(KEY_TRIP_NAME); remove(KEY_TRIP_ADDRESS); remove(KEY_TRIP_AT)
            } else {
                putString(KEY_TRIP_LAT, value.lat.toString())
                putString(KEY_TRIP_LON, value.lon.toString())
                putString(KEY_TRIP_NAME, value.name)
                putString(KEY_TRIP_ADDRESS, value.address)
                putLong(KEY_TRIP_AT, value.startedAtMillis)
            }
        }.apply()

    fun sourceUrlOf(id: String) = "$host/$id-index/"

    private companion object {
        const val KEY_HOST = "host"
        const val KEY_ACTIVE = "active"
        const val KEY_CHOSEN = "chosen"
        const val KEY_RECORD = "recordTrips"
        const val KEY_MODE = "travelMode"
        const val KEY_TRIP_LAT = "tripLat"
        const val KEY_TRIP_LON = "tripLon"
        const val KEY_TRIP_NAME = "tripName"
        const val KEY_TRIP_ADDRESS = "tripAddress"
        const val KEY_TRIP_AT = "tripAt"
        /** The emulator's alias for the development machine's loopback. */
        const val DEFAULT_HOST = "http://10.0.2.2:5185"
    }
}
