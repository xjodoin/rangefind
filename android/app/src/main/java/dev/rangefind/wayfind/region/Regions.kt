package dev.rangefind.wayfind.region

import android.content.Context
import androidx.annotation.StringRes
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.engine.ActiveJob
import dev.rangefind.wayfind.engine.JobStop
import dev.rangefind.wayfind.engine.StopOutcome
import dev.rangefind.wayfind.engine.stopInt
import dev.rangefind.wayfind.engine.stopText
import dev.rangefind.wayfind.nav.TravelMode
import org.json.JSONArray
import org.json.JSONObject

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
 * The day's plan as one preference string.
 *
 * JSON rather than a key per field because the stop list is variable
 * length: a run of `stop0Lat`, `stop1Lat`, … is a schema that cannot be
 * replaced atomically, so a five-stop job stored over yesterday's twelve
 * would leave seven addresses behind and send the driver back to them.
 * Coordinates go in as doubles here rather than as text — unlike
 * [ActiveTrip], which predates this and is stored a key at a time — because
 * JSON's own number syntax is exact and locale-free.
 */
fun ActiveJob.toJson(): String {
    val list = JSONArray()
    for ((position, stop) in stops.withIndex()) {
        list.put(
            JSONObject()
                .put(KEY_STOP_INDEX, stop.index)
                .put(KEY_STOP_LAT, stop.lat)
                .put(KEY_STOP_LON, stop.lon)
                .put(KEY_STOP_LABEL, stop.label)
                // Beside its stop rather than in a parallel array: a stop
                // that fails to read is dropped on the way back in, and an
                // array of outcomes would then be off by one for every
                // stop after it — marking the wrong doorstep delivered.
                .put(KEY_STOP_OUTCOME, outcomeOf(position + 1))
                // §20.8, and the reason it is stored at all: the ticket is
                // decoded once, by the WebView, and that WebView dies with
                // the process while the driver still has to know which
                // parcel this door is and who to ring. An unstated field is
                // an absent key — never "" and never 0, which are things
                // the dispatcher would have had to say.
                .apply {
                    stop.orderRef?.let { put(KEY_STOP_ORDER_REF, it) }
                    stop.parcels?.let { put(KEY_STOP_PARCELS, it) }
                    stop.instructions?.let { put(KEY_STOP_INSTRUCTIONS, it) }
                    stop.contact?.let { put(KEY_STOP_CONTACT, it) }
                }
        )
    }
    return JSONObject()
        .put(KEY_JOB_ID, jobIdHex)
        .put(KEY_JOB_NOT_AFTER, notAfter)
        .put(KEY_JOB_NEXT, nextIndex)
        .put(KEY_JOB_TICKET, ticketBase64 ?: JSONObject.NULL)
        // §11, which outlives the process that took the ticket: what the
        // run publishes decides whether a doorstep photo is even offered,
        // and re-deriving it from the ticket on every restart would mean
        // decoding a capability to answer a question about a checkbox.
        .put(KEY_JOB_FINE, fine)
        .put(KEY_JOB_STOPS, list)
        .toString()
}

/**
 * The plan back out again, or null when there is nothing usable in it.
 *
 * Everything here is defensive on purpose: this string was written by an
 * older build of the app as often as by this one, and a driver whose phone
 * cannot parse yesterday's file should get a fresh app rather than a crash
 * on the way to the depot.
 */
fun activeJobFromJson(text: String?): ActiveJob? {
    if (text.isNullOrBlank()) return null
    return runCatching {
        val root = JSONObject(text)
        val list = root.optJSONArray(KEY_JOB_STOPS) ?: JSONArray()
        val entries = (0 until list.length()).mapNotNull { position ->
            val item = list.optJSONObject(position) ?: return@mapNotNull null
            val lat = item.optDouble(KEY_STOP_LAT)
            val lon = item.optDouble(KEY_STOP_LON)
            // A stop with no coordinates is not an address, and a run that
            // carried one would navigate to the middle of the Atlantic.
            if (lat.isNaN() || lon.isNaN()) return@mapNotNull null
            JobStop(
                index = item.optInt(KEY_STOP_INDEX, position),
                lat = lat,
                lon = lon,
                // An unnamed stop is ordinary — a dispatcher numbers what it
                // does not name — and JSON says so with a null.
                label = if (item.isNull(KEY_STOP_LABEL)) "" else item.optString(KEY_STOP_LABEL),
                // A file from a build before §20.8 has none of these keys,
                // which reads as unstated — the honest answer, and the same
                // one an explicit null gives. [stopInt] is what keeps a
                // missing parcel count from arriving as a stated zero.
                orderRef = item.stopText(KEY_STOP_ORDER_REF),
                parcels = item.stopInt(KEY_STOP_PARCELS),
                instructions = item.stopText(KEY_STOP_INSTRUCTIONS),
                contact = item.stopText(KEY_STOP_CONTACT)
            ) to item.optInt(KEY_STOP_OUTCOME, StopOutcome.PENDING)
        }
        val stops = entries.map { it.first }
        if (stops.isEmpty()) return null
        ActiveJob(
            stops = stops,
            // A file written before outcomes existed carries none, and a
            // run with nothing said about it is a run of pending stops —
            // which is the honest reading, not a loss.
            outcomes = entries.map { (_, outcome) ->
                if (outcome in StopOutcome.PENDING..StopOutcome.FAILED) outcome
                else StopOutcome.PENDING
            },
            // A stored index is only as trustworthy as the file it came in:
            // out of range would either redeliver stop 1 for ever or index
            // past the end of the run.
            nextIndex = root.optInt(KEY_JOB_NEXT, 1).coerceIn(1, stops.size),
            ticketBase64 = if (root.isNull(KEY_JOB_TICKET)) null
            else root.optString(KEY_JOB_TICKET).takeIf { it.isNotEmpty() },
            jobIdHex = root.optString(KEY_JOB_ID),
            notAfter = root.optLong(KEY_JOB_NOT_AFTER),
            // A file written before §11 was recorded says nothing about
            // granularity, and fine is what every ticket defaults to. The
            // cost of guessing wrong here is bounded in the safe
            // direction: an offered photo on a coarse run is refused by the
            // library before anything is sealed or published, whereas
            // defaulting to coarse would silently withdraw proof of
            // delivery from every run in progress across an upgrade.
            fine = root.optBoolean(KEY_JOB_FINE, true)
        )
    }.getOrNull()
}

private const val KEY_JOB_ID = "jobIdHex"
private const val KEY_JOB_NOT_AFTER = "notAfter"
private const val KEY_JOB_NEXT = "nextIndex"
private const val KEY_JOB_TICKET = "ticket"
private const val KEY_JOB_FINE = "fine"
private const val KEY_JOB_STOPS = "stops"
private const val KEY_STOP_INDEX = "index"
private const val KEY_STOP_LAT = "lat"
private const val KEY_STOP_LON = "lon"
private const val KEY_STOP_LABEL = "label"
private const val KEY_STOP_OUTCOME = "outcome"
private const val KEY_STOP_ORDER_REF = "orderRef"
private const val KEY_STOP_PARCELS = "parcels"
private const val KEY_STOP_INSTRUCTIONS = "instructions"
private const val KEY_STOP_CONTACT = "contact"

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

    /**
     * Whether live traffic runs. Remembered because a driver who turned it
     * on meant "show me traffic", not "show me traffic until I close the
     * app"; contribution is a separate decision and is deliberately NOT
     * remembered here — it is re-asked from the mesh's own state each time.
     */
    var liveTraffic: Boolean
        get() = prefs.getBoolean(KEY_LIVE_TRAFFIC, false)
        set(value) = prefs.edit().putBoolean(KEY_LIVE_TRAFFIC, value).apply()

    /**
     * Whether the demo vehicles run when the mesh comes up without a keeper.
     *
     * On by default, and only because the alternative reads as a broken
     * feature: a keeperless phone with no simulation draws an empty traffic
     * layer. Remembered because a driver who turned invented traffic off
     * meant it — coming back to fake cars on the next launch would be the
     * app overruling them once a day.
     */
    var simulatedTraffic: Boolean
        get() = prefs.getBoolean(KEY_SIMULATED, true)
        set(value) = prefs.edit().putBoolean(KEY_SIMULATED, value).apply()

    /**
     * Whether a shared drive carries a live position (§11 fine) or only
     * stop events (coarse). Remembered because it is a standing decision
     * about what a leaked link would be worth, not a per-trip one.
     */
    var shareFine: Boolean
        get() = prefs.getBoolean(KEY_SHARE_FINE, true)
        set(value) = prefs.edit().putBoolean(KEY_SHARE_FINE, value).apply()

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

    /**
     * The dispatch ticket being worked through, and how far along it is.
     *
     * A delivery day is measured in hours, and an app process is not: a
     * phone call, a low-memory kill or a driver switching to the depot's
     * own app all end this one. Without the plan on disk, coming back means
     * a search box and twelve addresses the driver would have to retype —
     * and a run already published to every customer holding the link, which
     * the phone would no longer know it was in the middle of.
     *
     * Cleared when the last stop is delivered or the run is abandoned.
     */
    var activeJob: ActiveJob?
        get() = activeJobFromJson(prefs.getString(KEY_JOB, null))
        set(value) = prefs.edit().apply {
            if (value == null) remove(KEY_JOB) else putString(KEY_JOB, value.toJson())
        }.apply()

    /**
     * This device's own X25519 key (threads §20.9), wrapped by a Keystore
     * key it cannot export.
     *
     * Long-lived in a way nothing else here is: a run seed dies with its
     * day, and this opens every job that was ever sealed to this phone. It
     * is minted once, inside the engine, and crosses the bridge exactly
     * once on the way here — nothing above this line reads it, and the UI
     * never sees it at all.
     */
    private val deviceKeys = DeviceKeyVault(prefs)

    var devicePrivateKey: String?
        get() = deviceKeys.read()
        set(value) {
            if (value.isNullOrBlank()) return
            deviceKeys.write(value)
        }

    /** Whether the stored key is Keystore-wrapped rather than plain text. */
    val deviceKeyHardwareBacked: Boolean get() = deviceKeys.hardwareBacked

    /**
     * What this phone calls itself on its card. Editable, because the
     * recognition it supports is a human one — "Léa — Pixel 8" is what the
     * dispatcher is looking for in a list, and a model number is not.
     */
    var deviceName: String
        get() = prefs.getString(KEY_DEVICE_NAME, "").orEmpty()
        set(value) = prefs.edit().putString(KEY_DEVICE_NAME, cappedDeviceName(value)).apply()

    /**
     * The devices this phone may hand a job to (§20.9).
     *
     * Persisted because enrolment is a *prior* act by construction: the
     * replacement courier has to have been enrolled before the first
     * one's bike breaks, which is worth nothing if it does not survive
     * the app being killed between the two.
     */
    var enrolledDevices: List<EnrolledDevice>
        get() = enrolledDevicesFromJson(prefs.getString(KEY_ENROLLED, null))
        set(value) = prefs.edit().apply {
            if (value.isEmpty()) remove(KEY_ENROLLED)
            else putString(KEY_ENROLLED, enrolledDevicesToJson(value))
        }.apply()

    /**
     * The offers this device has bid on (§20.4).
     *
     * Persisted because a bid is a commitment held *against the future*:
     * the dispatcher's award arrives hours later, after a phone call, a
     * low-memory kill or a night, and the whole worth of `planRef` is
     * that the courier's own device can then compare the job that
     * arrived against the job that was advertised. A bid the phone
     * forgets is a dispatcher free to advertise three stops downtown and
     * award thirty across the province.
     *
     * Read prunes: an expired offer is not a bid still outstanding, it
     * is a job that can no longer be awarded to anyone.
     */
    var heldOffers: List<HeldOffer>
        get() = heldOffersFromJson(prefs.getString(KEY_OFFERS, null))
        set(value) = prefs.edit().apply {
            if (value.isEmpty()) remove(KEY_OFFERS)
            else putString(KEY_OFFERS, heldOffersToJson(value))
        }.apply()

    fun sourceUrlOf(id: String) = "$host/$id-index/"

    private companion object {
        const val KEY_HOST = "host"
        const val KEY_ACTIVE = "active"
        const val KEY_CHOSEN = "chosen"
        const val KEY_RECORD = "recordTrips"
        const val KEY_LIVE_TRAFFIC = "liveTraffic"
        const val KEY_SIMULATED = "simulatedTraffic"
        const val KEY_SHARE_FINE = "shareFine"
        const val KEY_MODE = "travelMode"
        const val KEY_TRIP_LAT = "tripLat"
        const val KEY_TRIP_LON = "tripLon"
        const val KEY_TRIP_NAME = "tripName"
        const val KEY_TRIP_ADDRESS = "tripAddress"
        const val KEY_TRIP_AT = "tripAt"
        const val KEY_JOB = "activeJob"
        const val KEY_DEVICE_NAME = "deviceName"
        const val KEY_ENROLLED = "enrolledDevices"
        const val KEY_OFFERS = "heldOffers"
        /** The emulator's alias for the development machine's loopback. */
        const val DEFAULT_HOST = "http://10.0.2.2:5185"
    }
}
