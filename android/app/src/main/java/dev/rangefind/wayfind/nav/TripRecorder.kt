package dev.rangefind.wayfind.nav

import android.content.Context
import android.location.Location
import dev.rangefind.wayfind.engine.Route
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

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
    private var markCount = 0

    val isRecording: Boolean get() = sink != null

    /** Traces newest first; the drive that just went wrong is the first one. */
    fun traces(): List<File> =
        root.listFiles()
            ?.filter { it.isFile && it.extension == "jsonl" }
            ?.sortedByDescending { it.lastModified() }
            ?: emptyList()

    fun deleteAll() {
        root.deleteRecursively()
    }

    fun start(route: Route?, startedAtMillis: Long, environment: JSONObject? = null) {
        stop()
        root.mkdirs()
        prune()
        markCount = 0
        val file = File(root, "trip-$startedAtMillis.jsonl")
        val header = JSONObject()
            .put("kind", "route")
            .put("at", startedAtMillis)
            // Which build, which device, which index. A trace that cannot say
            // what produced it can only be read as a guess.
            .put("environment", environment ?: JSONObject())
            .put("httpRequests", route?.httpRequests ?: 0)
            .put("bytesFetched", route?.bytesFetched ?: 0L)
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
                            .put("lanes", JSONArray().apply { step.lanes.forEach { put(it) } })
                    )
                }
            })
            .put("geometry", JSONArray().apply {
                route?.geometry?.forEach { point ->
                    put(JSONArray().put(point.lat).put(point.lon))
                }
            })
            // Signals, stops and crossings as the route reports them, so a
            // complaint about what the map drew can be checked against it.
            .put("junctions", JSONArray().apply {
                route?.junctions?.forEach { junction ->
                    put(
                        JSONObject()
                            .put("kind", junction.kind)
                            .put("lat", junction.lat)
                            .put("lon", junction.lon)
                            .put("atMeters", junction.atMeters)
                    )
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
    fun mark(ordinal: Int, update: NavUpdate?, atMillis: Long): File? {
        val file = sink ?: return null
        markCount = ordinal
        // The screenshot is written by the surface that can see the map; the
        // trace only promises where it will be. A mark stays readable if the
        // capture fails, which matters more than the picture.
        val shot = File(file.parentFile, "${file.nameWithoutExtension}-mark-$ordinal.png")
        val row = JSONObject()
            .put("kind", "mark")
            .put("at", atMillis)
            .put("ordinal", ordinal)
            .put("screenshot", shot.name)
        if (update != null) row.put("nav", navJson(update))
        runCatching { file.appendText(row.toString() + "\n") }
        return shot
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
        // Why the arrow was where it was, not just where it ended up.
        .put("crossTrackMeters", update.crossTrackMeters)
        .put("distanceAlongMeters", update.distanceAlongMeters)
        .put("turnDelta", update.turnDelta)
        .put("speedLimitKmh", update.speedLimitKmh)
        .put("heading", update.bearing)
        .put("offRoute", update.offRoute)
        .put("arrived", update.arrived)
        .put("voice", update.voice ?: JSONObject.NULL)

    /**
     * How the map performed over the drive, written once at the end.
     *
     * The counterpart to the fix rows: those say what the app decided, this
     * says whether the screen kept up with it. Read together they separate a
     * renderer that stutters from a position that only moves once a second.
     */
    fun renderSummary(summary: JSONObject, atMillis: Long) {
        val file = sink ?: return
        val row = JSONObject()
            .put("kind", "render")
            .put("at", atMillis)
            .put("render", summary)
        runCatching { file.appendText(row.toString() + "\n") }
    }

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

    /**
     * A trace and its screenshots as one file to hand over.
     *
     * Sharing a JSONL and four PNGs separately means four chances to send the
     * wrong set; a zip keeps a report whole. Rebuilt on demand rather than
     * kept, since it is derivable from what is already on disk.
     */
    fun bundle(trace: File): File? {
        if (!trace.isFile) return null
        val zip = File(root, "${trace.nameWithoutExtension}.zip")
        val shots = root.listFiles()
            ?.filter { it.isFile && it.name.startsWith("${trace.nameWithoutExtension}-mark-") }
            ?.sortedBy { it.name }
            .orEmpty()
        return runCatching {
            ZipOutputStream(zip.outputStream().buffered()).use { out ->
                for (file in listOf(trace) + shots) {
                    out.putNextEntry(ZipEntry(file.name))
                    file.inputStream().use { it.copyTo(out) }
                    out.closeEntry()
                }
            }
            zip
        }.getOrNull()
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
        for (trace in existing.drop(MAX_TRACES - 1)) {
            // A trace's screenshots and bundle are part of it; leaving them
            // behind would grow the directory the cap exists to bound.
            root.listFiles()
                ?.filter { it.name.startsWith(trace.nameWithoutExtension) }
                ?.forEach { runCatching { it.delete() } }
        }
    }

    private companion object {
        const val MAX_TRACES = 5
    }
}
