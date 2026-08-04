package dev.rangefind.wayfind.ui

import java.util.Calendar
import java.util.Locale
import kotlin.math.roundToInt
import kotlin.math.roundToLong

/** "80 m", "450 m", "1.2 km", "24 km" — the granularity drivers actually read. */
fun formatDistance(meters: Double): String = when {
    meters.isNaN() -> "—"
    // Rounding to the nearest 10 turns a genuine 4 m leg into "0 m", which
    // reads as a failure rather than a very short trip.
    meters < 50 -> "${meters.roundToInt()} m"
    meters < 100 -> "${(meters / 10).roundToInt() * 10} m"
    meters < 1000 -> "${(meters / 50).roundToInt() * 50} m"
    meters < 10_000 -> String.format(Locale.getDefault(), "%.1f km", meters / 1000)
    else -> "${(meters / 1000).roundToInt()} km"
}

/** Maneuver callouts round harder than trip distances. */
fun formatManeuverDistance(meters: Double): String = when {
    meters < 30 -> "Now"
    meters < 100 -> "${(meters / 10).roundToInt() * 10} m"
    meters < 1000 -> "${(meters / 50).roundToInt() * 50} m"
    else -> String.format(Locale.getDefault(), "%.1f km", meters / 1000)
}

fun formatDuration(seconds: Double): String {
    if (seconds.isNaN()) return "—"
    val total = seconds.roundToLong()
    val hours = total / 3600
    val minutes = ((total % 3600) / 60.0).roundToLong()
    return when {
        hours > 0 && minutes > 0 -> "$hours h $minutes min"
        hours > 0 -> "$hours h"
        total < 60 -> "1 min"
        else -> "$minutes min"
    }
}

fun formatArrivalClock(seconds: Double): String {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.SECOND, seconds.roundToInt())
    return String.format(
        Locale.getDefault(),
        "%d:%02d",
        calendar.get(Calendar.HOUR).let { if (it == 0) 12 else it },
        calendar.get(Calendar.MINUTE)
    ) + if (calendar.get(Calendar.AM_PM) == Calendar.AM) " AM" else " PM"
}

fun formatSpeed(metersPerSecond: Double): String =
    "${(metersPerSecond * 3.6).roundToInt()}"

fun formatBytes(bytes: Long): String = when {
    bytes < 1024 -> "$bytes B"
    bytes < 1024 * 1024 -> "${(bytes / 1024.0).roundToInt()} KB"
    else -> String.format(Locale.getDefault(), "%.1f MB", bytes / (1024.0 * 1024.0))
}

/** Turns an OSM type token ("fast_food", "bus_stop") into a readable label. */
fun humanizeType(value: String): String = value
    .replace('_', ' ')
    .split(' ')
    .filter { it.isNotBlank() }
    .joinToString(" ") { word -> word.replaceFirstChar { it.uppercase(Locale.getDefault()) } }
