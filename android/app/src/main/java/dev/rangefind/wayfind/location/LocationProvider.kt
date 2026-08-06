package dev.rangefind.wayfind.location

import android.annotation.SuppressLint
import android.content.Context
import android.location.Location
import android.location.LocationListener
import android.location.LocationManager
import android.os.Looper
import android.os.SystemClock
import dev.rangefind.wayfind.BuildConfig
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.flow
import org.json.JSONObject
import java.io.File

/**
 * Location updates straight from the platform LocationManager.
 *
 * Deliberately not Play Services: this keeps the app dependency-free of
 * Google APIs, works on any emulator image, and `adb emu geo fix` drives it
 * directly for navigation testing.
 *
 * On a phone there is no such lever. `adb emu geo fix` is emulator-only, and
 * injecting positions into a physical device otherwise means a mock-location
 * app and a developer setting. That left everything downstream of a fix —
 * matching, guidance, and above all how the map actually renders while moving
 * — measurable only by driving somewhere, which is a slow way to answer "did
 * that frame arrive late".
 *
 * So a debug build will replay a recorded trip instead of listening to the
 * satellites: drop a trace at `files/replay.jsonl` and its fixes are handed
 * back at the intervals they were recorded at, with their real speeds and
 * bearings. The whole stack then runs on the real device, real GPU, real
 * layers, against motion that actually happened. Release builds have no such
 * path — it is guarded on [BuildConfig.DEBUG] and a file the app never writes.
 */
class LocationProvider(context: Context) {

    private val appContext = context.applicationContext
    private val manager =
        appContext.getSystemService(Context.LOCATION_SERVICE) as LocationManager

    @SuppressLint("MissingPermission")
    fun lastKnown(): Location? = runCatching {
        listOf(LocationManager.GPS_PROVIDER, LocationManager.NETWORK_PROVIDER)
            .firstNotNullOfOrNull { provider ->
                runCatching { manager.getLastKnownLocation(provider) }.getOrNull()
            }
    }.getOrNull()

    /** Caller must hold a location permission before collecting. */
    fun updates(minIntervalMs: Long = 1000L, minDistanceM: Float = 0f): Flow<Location> =
        replayFile()?.let { replay(it) } ?: live(minIntervalMs, minDistanceM)

    /** The trace to replay, or null when there is none and on release builds. */
    private fun replayFile(): File? {
        if (!BuildConfig.DEBUG) return null
        return File(appContext.filesDir, "replay.jsonl").takeIf { it.isFile }
    }

    /**
     * Hands back a recorded drive at the pace it happened.
     *
     * Timing comes from the trace's own timestamps rather than a fixed rate,
     * because the gaps are part of what is being reproduced — a fix that
     * arrived late on the road should arrive late here.
     */
    private fun replay(trace: File): Flow<Location> = flow {
        // Local to the collection: a flow may be collected more than once, and
        // a shared buffer would hand back the previous run's fixes as well.
        val pending = mutableListOf<Pair<Long, Location>>()
        var previousAt = 0L
        trace.forEachLine { line ->
            if (!line.contains("\"kind\":\"fix\"")) return@forEachLine
            val row = runCatching { JSONObject(line) }.getOrNull() ?: return@forEachLine
            val at = row.optLong("at")
            val wait = if (previousAt == 0L) 0L else (at - previousAt).coerceIn(0L, 5_000L)
            previousAt = at
            pending += wait to Location("replay").apply {
                latitude = row.optDouble("lat")
                longitude = row.optDouble("lon")
                if (row.optBoolean("hasSpeed")) speed = row.optDouble("speedMps").toFloat()
                if (row.optBoolean("hasBearing")) bearing = row.optDouble("bearing").toFloat()
                accuracy = row.optDouble("accuracy", 8.0).toFloat().coerceAtLeast(1f)
            }
        }
        for ((wait, location) in pending) {
            delay(wait)
            location.time = System.currentTimeMillis()
            location.elapsedRealtimeNanos = SystemClock.elapsedRealtimeNanos()
            emit(location)
        }
    }

    @SuppressLint("MissingPermission")
    private fun live(minIntervalMs: Long, minDistanceM: Float): Flow<Location> =
        callbackFlow {
            // Both providers are registered, but a network fix is a cell-tower
            // guess good to hundreds of metres. Letting one through moments
            // after a satellite fix teleports the vehicle across town and back.
            var lastFine = 0L
            val listener = LocationListener { location ->
                val fine = location.provider == LocationManager.GPS_PROVIDER
                val now = location.elapsedRealtimeNanos / 1_000_000L
                if (fine) {
                    lastFine = now
                    trySend(location)
                } else if (now - lastFine > COARSE_GRACE_MS) {
                    trySend(location)
                }
            }

            val enabled = buildList {
                if (manager.isProviderEnabled(LocationManager.GPS_PROVIDER)) add(LocationManager.GPS_PROVIDER)
                if (manager.isProviderEnabled(LocationManager.NETWORK_PROVIDER)) add(LocationManager.NETWORK_PROVIDER)
            }

            enabled.forEach { provider ->
                runCatching {
                    manager.requestLocationUpdates(
                        provider,
                        minIntervalMs,
                        minDistanceM,
                        listener,
                        Looper.getMainLooper()
                    )
                }
            }

            lastKnown()?.let { trySend(it) }

            awaitClose { runCatching { manager.removeUpdates(listener) } }
        }

    private companion object {
        /** How long a satellite fix keeps a coarse one out. */
        const val COARSE_GRACE_MS = 15_000L
    }
}
