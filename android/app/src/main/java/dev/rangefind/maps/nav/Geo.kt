package dev.rangefind.maps.nav

import dev.rangefind.maps.engine.LatLon
import kotlin.math.abs
import kotlin.math.atan2
import kotlin.math.cos
import kotlin.math.max
import kotlin.math.min
import kotlin.math.sin
import kotlin.math.sqrt

private const val EARTH_RADIUS_M = 6371008.8
private const val DEG = Math.PI / 180.0

fun haversineMeters(a: LatLon, b: LatLon): Double {
    val dLat = (b.lat - a.lat) * DEG
    val dLon = (b.lon - a.lon) * DEG
    val lat1 = a.lat * DEG
    val lat2 = b.lat * DEG
    val h = sin(dLat / 2) * sin(dLat / 2) + cos(lat1) * cos(lat2) * sin(dLon / 2) * sin(dLon / 2)
    return 2 * EARTH_RADIUS_M * atan2(sqrt(h), sqrt(1 - h))
}

fun bearingDegrees(from: LatLon, to: LatLon): Double {
    val lat1 = from.lat * DEG
    val lat2 = to.lat * DEG
    val dLon = (to.lon - from.lon) * DEG
    val y = sin(dLon) * cos(lat2)
    val x = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dLon)
    return (Math.toDegrees(atan2(y, x)) + 360.0) % 360.0
}

/** Smallest signed difference between two bearings, in (-180, 180]. */
fun bearingDelta(from: Double, to: Double): Double {
    var delta = (to - from + 540.0) % 360.0 - 180.0
    if (delta <= -180.0) delta += 360.0
    return delta
}

/**
 * Projects [point] onto segment a→b in a local planar frame. Accurate well
 * past the length of any single route segment, and far cheaper than a proper
 * geodesic solve at 1 Hz.
 */
fun projectOntoSegment(point: LatLon, a: LatLon, b: LatLon): SegmentProjection {
    val latScale = cos(((a.lat + b.lat) / 2.0) * DEG)
    val ax = 0.0
    val ay = 0.0
    val bx = (b.lon - a.lon) * latScale
    val by = (b.lat - a.lat)
    val px = (point.lon - a.lon) * latScale
    val py = (point.lat - a.lat)

    val lengthSq = bx * bx + by * by
    val t = if (lengthSq <= 0.0) 0.0 else max(0.0, min(1.0, ((px - ax) * bx + (py - ay) * by) / lengthSq))
    val snapped = LatLon(a.lat + by * t, a.lon + (bx * t) / (if (latScale == 0.0) 1.0 else latScale))
    return SegmentProjection(t, snapped, haversineMeters(point, snapped))
}

data class SegmentProjection(val t: Double, val snapped: LatLon, val distanceMeters: Double)

/** Great-circle interpolation is unnecessary at these scales; linear is exact enough. */
fun interpolate(a: LatLon, b: LatLon, t: Double) =
    LatLon(a.lat + (b.lat - a.lat) * t, a.lon + (b.lon - a.lon) * t)

fun normalizeBearing(value: Double) = ((value % 360.0) + 360.0) % 360.0

fun absBearingDelta(from: Double, to: Double) = abs(bearingDelta(from, to))
