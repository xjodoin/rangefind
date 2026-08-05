package dev.rangefind.wayfind.nav

import android.content.Context
import android.location.Location
import dev.rangefind.wayfind.engine.Route
import org.json.JSONArray
import org.json.JSONObject
import java.io.File

/**
 * Records a drive to a file so a bad one can be examined afterwards.
 *
 * Navigation faults are the hardest kind to report: they happen at 90 km/h,
 * they depend on the exact sequence of fixes, and by the time the driver can
 * describe what went wrong the state that caused it is gone. A trace keeps
 * every fix alongside what the state machine decided from it — which step it
 * thought it was on, how far it thought the maneuver was, what it said out
 * loud — so "it told me to continue when I had to turn" becomes a specific
 * line with a turn angle on it.
 *
 * One JSON object per line: a header for the route, then one per fix. Line
 * oriented so a truncated file from a crash is still readable up to the fault.
 */
class TripRecorder(context: Context) {

    private val root = File(context.filesDir, "traces")
    private var sink: File? = null

    val isRecording: Boolean get() = sink != null

    /** Traces newest first; the drive that just went wrong is the first one. */
    fun traces(): List<File> =
        root.listFiles()?.filter { it.isFile }?.sortedByDescending { it.lastModified() } ?: emptyList()

    fun deleteAll() {
        root.deleteRecursively()
    }

    fun start(route: Route?, startedAtMillis: Long) {
        stop()
        root.mkdirs()
        prune()
        val file = File(root, "trip-$startedAtMillis.jsonl")
        val header = JSONObject()
            .put("kind", "route")
            .put("at", startedAtMillis)
            .put("seconds", route?.seconds ?: 0.0)
            .put("distanceMeters", route?.distanceMeters ?: 0.0)
            .put("stepCount", route?.steps?.size ?: 0)
            .put("steps", JSONArray().apply {
                route?.steps?.forEach { step ->
                    put(
                        JSONObject()
                            .put("name", step.name)
                            .put("meters", step.meters)
                            .put("seconds", step.seconds)
                            .put("at", step.at)
                            .put("speedLimitKmh", step.speedLimitKmh)
                    )
                }
            })
            .put("geometry", JSONArray().apply {
                route?.geometry?.forEach { point ->
                    put(JSONArray().put(point.lat).put(point.lon))
                }
            })
        runCatching {
            file.appendText(header.toString() + "\n")
            sink = file
        }
    }

    /**
     * One fix and what came of it. The raw location is kept separately from
     * the derived state: a wrong instruction is either a bad fix or a bad
     * decision, and the trace has to be able to tell those apart.
     */
    fun log(location: Location, update: NavUpdate?, atMillis: Long) {
        val file = sink ?: return
        val row = JSONObject()
            .put("kind", "fix")
            .put("at", atMillis)
            .put("lat", location.latitude)
            .put("lon", location.longitude)
            .put("accuracy", if (location.hasAccuracy()) location.accuracy else -1f)
            .put("hasSpeed", location.hasSpeed())
            .put("speedMps", if (location.hasSpeed()) location.speed else 0f)
            .put("hasBearing", location.hasBearing())
            .put("bearing", if (location.hasBearing()) location.bearing else -1f)
        if (update != null) row.put("nav", navJson(update))
        runCatching { file.appendText(row.toString() + "\n") }
    }

    /**
     * The driver saying "that was wrong, right now".
     *
     * A trace without marks means reading thousands of fixes looking for the
     * one that misbehaved. A mark turns that into a timestamp: whatever the
     * state machine believed at this instant is what needs explaining.
     */
    fun mark(ordinal: Int, update: NavUpdate?, atMillis: Long) {
        val file = sink ?: return
        val row = JSONObject()
            .put("kind", "mark")
            .put("at", atMillis)
            .put("ordinal", ordinal)
        if (update != null) row.put("nav", navJson(update))
        runCatching { file.appendText(row.toString() + "\n") }
    }

    private fun navJson(update: NavUpdate) = JSONObject()
        // Where the arrow was drawn, which is not always where the fix was:
        // on route it is snapped to the line, off route it is the fix itself.
        // Without both, "the arrow was stuck" is unfalsifiable.
        .put("shownLat", update.position.lat)
        .put("shownLon", update.position.lon)
        .put("stepIndex", update.stepIndex)
        .put("stepName", update.stepName)
        .put("nextStepName", update.nextStepName)
        .put("metersToManeuver", update.metersToManeuver)
        .put("remainingMeters", update.remainingMeters)
        .put("remainingSeconds", update.remainingSeconds)
        .put("turnDelta", update.turnDelta)
        .put("speedLimitKmh", update.speedLimitKmh)
        .put("heading", update.bearing)
        .put("offRoute", update.offRoute)
        .put("arrived", update.arrived)
        .put("voice", update.voice ?: JSONObject.NULL)

    /** Note something the caller did, so the trace explains its own gaps. */
    fun note(event: String, detail: String? = null, atMillis: Long) {
        val file = sink ?: return
        val row = JSONObject()
            .put("kind", "event")
            .put("at", atMillis)
            .put("event", event)
        if (detail != null) row.put("detail", detail)
        runCatching { file.appendText(row.toString() + "\n") }
    }

    fun stop(): File? {
        val file = sink
        sink = null
        return file
    }

    /** Traces are diagnostic scratch, not archives; keep only the recent ones. */
    private fun prune() {
        val existing = traces()
        if (existing.size < MAX_TRACES) return
        existing.drop(MAX_TRACES - 1).forEach { runCatching { it.delete() } }
    }

    private companion object {
        const val MAX_TRACES = 5
    }
}
