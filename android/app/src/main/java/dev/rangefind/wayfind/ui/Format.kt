package dev.rangefind.wayfind.ui

import android.content.Context
import dev.rangefind.wayfind.R
import java.util.Calendar
import java.util.Locale
import kotlin.math.roundToInt
import kotlin.math.roundToLong

/** "80 m", "450 m", "1.2 km", "24 km" — the granularity drivers actually read. */
fun formatDistance(context: Context, meters: Double): String = when {
    meters.isNaN() -> context.getString(R.string.format_placeholder)
    // Rounding to the nearest 10 turns a genuine 4 m leg into "0 m", which
    // reads as a failure rather than a very short trip.
    meters < 50 -> context.getString(R.string.format_meters, meters.roundToInt())
    meters < 100 -> context.getString(R.string.format_meters, (meters / 10).roundToInt() * 10)
    meters < 1000 -> context.getString(R.string.format_meters, (meters / 50).roundToInt() * 50)
    meters < 10_000 -> context.getString(R.string.format_kilometers_fraction, meters / 1000)
    else -> context.getString(R.string.format_kilometers, (meters / 1000).roundToInt())
}

/** Maneuver callouts round harder than trip distances. */
fun formatManeuverDistance(context: Context, meters: Double): String = when {
    meters < 30 -> context.getString(R.string.format_maneuver_now)
    meters < 100 -> context.getString(R.string.format_meters, (meters / 10).roundToInt() * 10)
    meters < 1000 -> context.getString(R.string.format_meters, (meters / 50).roundToInt() * 50)
    else -> context.getString(R.string.format_kilometers_fraction, meters / 1000)
}

fun formatDuration(context: Context, seconds: Double): String {
    if (seconds.isNaN()) return context.getString(R.string.format_placeholder)
    val total = seconds.roundToLong()
    val hours = total / 3600
    val minutes = ((total % 3600) / 60.0).roundToLong()
    return when {
        hours > 0 && minutes > 0 ->
            context.getString(R.string.format_duration_hours_minutes, hours, minutes)
        hours > 0 -> context.getString(R.string.format_duration_hours, hours)
        total < 60 -> context.getString(R.string.format_duration_minutes, 1)
        else -> context.getString(R.string.format_duration_minutes, minutes)
    }
}

/** Relative ETA for an alternate route, the way a driver compares them. */
fun formatEtaDelta(context: Context, seconds: Double): String {
    val minutes = (seconds / 60).roundToInt()
    return when {
        minutes >= 1 -> context.getString(R.string.format_eta_later, minutes)
        minutes <= -1 -> context.getString(R.string.format_eta_earlier, minutes)
        else -> context.getString(R.string.format_eta_same)
    }
}

fun formatArrivalClock(context: Context, seconds: Double): String {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.SECOND, seconds.roundToInt())
    val morning = calendar.get(Calendar.AM_PM) == Calendar.AM
    // A 12-hour hour, the minute, and the 24-hour hour are all supplied so a
    // locale can pick the convention it actually uses.
    return context.getString(
        if (morning) R.string.format_clock_am else R.string.format_clock_pm,
        calendar.get(Calendar.HOUR).let { if (it == 0) 12 else it },
        calendar.get(Calendar.MINUTE),
        calendar.get(Calendar.HOUR_OF_DAY)
    )
}

fun formatSpeed(metersPerSecond: Double): String =
    "${(metersPerSecond * 3.6).roundToInt()}"

fun formatBytes(context: Context, bytes: Long): String = when {
    bytes < 1024 -> context.getString(R.string.format_bytes, bytes)
    bytes < 1024 * 1024 -> context.getString(R.string.format_kilobytes, (bytes / 1024.0).roundToInt())
    else -> context.getString(R.string.format_megabytes, bytes / (1024.0 * 1024.0))
}

/** Turns an OSM type token ("fast_food", "bus_stop") into a readable label. */
fun humanizeType(value: String): String = value
    .replace('_', ' ')
    .split(' ')
    .filter { it.isNotBlank() }
    .joinToString(" ") { word -> word.replaceFirstChar { it.uppercase(Locale.getDefault()) } }
