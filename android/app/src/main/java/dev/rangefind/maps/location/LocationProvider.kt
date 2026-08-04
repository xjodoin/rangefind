package dev.rangefind.maps.location

import android.annotation.SuppressLint
import android.content.Context
import android.location.Location
import android.location.LocationListener
import android.location.LocationManager
import android.os.Looper
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow

/**
 * Location updates straight from the platform LocationManager.
 *
 * Deliberately not Play Services: this keeps the app dependency-free of
 * Google APIs, works on any emulator image, and `adb emu geo fix` drives it
 * directly for navigation testing.
 */
class LocationProvider(context: Context) {

    private val manager =
        context.applicationContext.getSystemService(Context.LOCATION_SERVICE) as LocationManager

    @SuppressLint("MissingPermission")
    fun lastKnown(): Location? = runCatching {
        listOf(LocationManager.GPS_PROVIDER, LocationManager.NETWORK_PROVIDER)
            .firstNotNullOfOrNull { provider ->
                runCatching { manager.getLastKnownLocation(provider) }.getOrNull()
            }
    }.getOrNull()

    /** Caller must hold a location permission before collecting. */
    @SuppressLint("MissingPermission")
    fun updates(minIntervalMs: Long = 1000L, minDistanceM: Float = 0f): Flow<Location> =
        callbackFlow {
            val listener = LocationListener { location -> trySend(location) }

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
}
