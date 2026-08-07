package dev.rangefind.wayfind.engine

/**
 * The app's whole dependency on rangefind. The JS host behind it is an
 * implementation detail: today a headless WebView, tomorrow an embedded V8 or
 * Node runtime, without touching anything above this line.
 */
interface RangefindEngine {
    suspend fun init(searchBase: String, routeBase: String): EngineInfo
    /** Repoint routing at another index (a preloaded region, or the network). */
    suspend fun useRouteBase(routeBase: String): RoutingInfo
    /** Every file that makes up the index at [baseUrl], for offline preload. */
    suspend fun regionFiles(baseUrl: String): RegionManifest
    suspend fun search(
        query: String,
        anchor: LatLon?,
        size: Int = 20,
        shards: List<String> = emptyList()
    ): SearchOutcome
    suspend fun suggest(query: String, anchor: LatLon?): List<Suggestion>
    suspend fun reverse(point: LatLon): Place?
    /**
     * [fromHeading] is the direction of travel in degrees, and belongs only to
     * a vehicle that is actually moving: it stops a reroute from snapping to
     * the opposite side of the road and routing the driver back the way they
     * came. Null means no opinion, so both directions stay equally good.
     */
    suspend fun route(
        from: LatLon,
        to: LatLon,
        alternatives: Int = 2,
        fromHeading: Double? = null,
        /**
         * The error bar on the fix [from] came from. The snap widens its
         * candidate band to match: a band narrower than the error discards
         * the road the car is on before anything can weigh it, which is how
         * a car on a service road beside a motorway gets routed onto the
         * motorway.
         */
        accuracyMeters: Double? = null
    ): RouteBundle
    suspend fun snap(point: LatLon): SnapPoint?

    /**
     * Live traffic (PulseMesh). The phone is the contributor the design
     * counts on — background location is what a browser tab cannot offer —
     * but publishing where you drive is a decision, so it stays off until
     * [setContributing] turns it on.
     */
    suspend fun pulseMeshStatus(): PulseMeshStatus
    suspend fun setContributing(enabled: Boolean): PulseMeshStatus
    /** One GPS fix through the contributor pipeline. */
    suspend fun offerLocation(point: LatLon, speedMps: Double?, courseDeg: Double?): PulseMeshStatus
}

/**
 * What the mesh is doing right now. [contributing] false means this phone
 * only reads traffic; [suppressed] counts fixes the protocol's own gates
 * declined to publish, which is the normal case under the reticent profile.
 */
data class PulseMeshStatus(
    val available: Boolean,
    val contributing: Boolean = false,
    val epoch: String = "",
    val fixes: Int = 0,
    val emitted: Int = 0,
    val suppressed: Int = 0,
    val lastReason: String? = null,
    val error: String? = null
)

data class LatLon(val lat: Double, val lon: Double)

data class RoutingInfo(
    val routing: Boolean,
    val routingError: String?,
    val profile: String,
    val routeBounds: RouteBounds?
)

data class RegionManifest(
    val files: List<String>,
    val profile: String,
    val nodes: Long,
    val leaves: Int
)

data class EngineInfo(
    val attribution: String,
    val license: String,
    val total: Long,
    val routing: Boolean,
    val routingError: String?,
    val profile: String,
    /** Extent of the route graph. Search and the basemap are worldwide; this is not. */
    val routeBounds: RouteBounds?
)

/**
 * Extent of the route graph: the overall envelope plus the leaf boxes that
 * tile it. The envelope alone spans neighbouring countries the extract never
 * covered, so containment is tested per leaf.
 */
data class RouteBounds(
    val minLat: Double,
    val maxLat: Double,
    val minLon: Double,
    val maxLon: Double,
    /** Flat [minLat, maxLat, minLon, maxLon] runs, one per leaf cell. */
    val cells: DoubleArray
) {
    fun contains(point: LatLon): Boolean {
        // Cheap envelope reject first.
        if (point.lat < minLat - MARGIN_DEG || point.lat > maxLat + MARGIN_DEG) return false
        if (point.lon < minLon - MARGIN_DEG || point.lon > maxLon + MARGIN_DEG) return false
        if (cells.isEmpty()) return true

        var i = 0
        while (i + 3 < cells.size) {
            if (point.lat >= cells[i] - MARGIN_DEG && point.lat <= cells[i + 1] + MARGIN_DEG &&
                point.lon >= cells[i + 2] - MARGIN_DEG && point.lon <= cells[i + 3] + MARGIN_DEG
            ) {
                return true
            }
            i += 4
        }
        return false
    }

    override fun equals(other: Any?): Boolean =
        other is RouteBounds && minLat == other.minLat && maxLat == other.maxLat &&
            minLon == other.minLon && maxLon == other.maxLon && cells.contentEquals(other.cells)

    override fun hashCode(): Int =
        (((minLat.hashCode() * 31 + maxLat.hashCode()) * 31 + minLon.hashCode()) * 31 +
            maxLon.hashCode()) * 31 + cells.contentHashCode()

    private companion object {
        /** Slightly beyond the engine's 250 m snap limit, in degrees. */
        const val MARGIN_DEG = 0.004
    }
}

data class Place(
    val id: String,
    val name: String,
    val address: String,
    val locality: String,
    val category: String,
    val type: String,
    val lat: Double,
    val lon: Double,
    val distanceMeters: Double?
) {
    val point: LatLon get() = LatLon(lat, lon)
}

data class SearchOutcome(val places: List<Place>, val total: Long)

data class Suggestion(
    val text: String,
    val mainText: String,
    val secondaryText: String,
    val kind: String,
    /** Canonical query text for this prediction (may differ from [text]). */
    val selectionQuery: String,
    /** Shard that owns the predicted place; turns search into a direct lookup. */
    val selectionShards: List<String>
)

data class RouteStep(
    val name: String,
    val meters: Double,
    val seconds: Double,
    val at: Int,
    /** Posted limit in km/h, 0 when the way carries no maxspeed tag. */
    val speedLimitKmh: Int,
    /**
     * Movements allowed from each lane of the approach to this step, left to
     * right, as a bit set per lane. Empty when the road carried no lane tags;
     * a zero entry is a lane whose movements are unknown.
     */
    val lanes: List<Int> = emptyList(),
    /**
     * The kind of road this step runs on — "motorway", "motorway_link" and
     * so on. A slip road is unnamed far more often than not, so its class is
     * the only thing that says it is a ramp at all.
     */
    val roadClass: String = "",
    /**
     * The road's own number, as written on the sign: "40", "A 13". On a
     * motorway this is the only label a driver can act on — the name is
     * never posted, and "Autoroute Félix-Leclerc" describes a road nobody
     * can find a sign for.
     */
    val ref: String = "",
    /** The exit number off the green panel: "32", "89-N". */
    val exitRef: String = "",
    /**
     * What a slip road leads to, numbered and with its cardinal: "20 Est",
     * "25 Nord;40". This is where a direction actually comes from — OSM
     * tags direction on only a handful of route relations, but it is on
     * thousands of ramps, because that is what the sign says.
     */
    val destinationRef: String = "",
    /** The places named on the sign: "Montréal;Québec". */
    val destination: String = "",
    /** Whether this step runs inside a roundabout. */
    val roundabout: Boolean = false,
    /** Which exit of that roundabout the route leaves by; 0 when unknown. */
    val roundaboutExit: Int = 0
) {
    /** A crossing by boat rather than by road. */
    val isFerry: Boolean get() = roadClass == "ferry"

    /**
     * The label to guide by: the number if the road has one, its name
     * otherwise. Semicolon lists are cut to their first entry — a banner has
     * room for one answer, and the first is the one the sign leads with.
     */
    val signLabel: String
        get() = firstOf(ref).ifBlank { name }

    /** Where a slip road leads, as the sign puts it: "20 Est". */
    val towardLabel: String
        get() = firstOf(destinationRef).ifBlank { firstOf(destination) }
}

private fun firstOf(list: String): String =
    list.substringBefore(';').trim()

/** Lane movement bits, matching the route index's own encoding. */
object LaneTurn {
    const val REVERSE = 1
    const val SHARP_LEFT = 2
    const val LEFT = 4
    const val SLIGHT_LEFT = 8
    const val THROUGH = 16
    const val SLIGHT_RIGHT = 32
    const val RIGHT = 64
    const val SHARP_RIGHT = 128
}

/** 1 signals, 2 stop, 3 give way, 4 level crossing, 5 crossing. */
data class RouteJunction(
    val kind: Int,
    val lat: Double,
    val lon: Double,
    val atMeters: Double
)

/**
 * A posted limit and the distance along the route where it takes effect.
 *
 * Limits belong to distance rather than to steps because a street is not a
 * limit: an autoroute drops through an interchange and climbs back without
 * ever changing its name, and one number for the whole street is the wrong
 * number for however much of it disagrees.
 */
data class SpeedLimitChange(
    val atMeters: Double,
    val limitKmh: Int,
    /** A lower limit that applies only at certain times, or null. */
    val conditional: ConditionalLimit? = null
)

/**
 * A limit in force only during a window — a school zone, in practice.
 *
 * The index cannot resolve this: it is static, and which limit applies
 * depends on the clock. So the window travels to the client and is answered
 * against the device's own local time, which is also the only clock that
 * agrees with the sign the driver is looking at.
 *
 * [days] is a 7-bit mask from Monday. The month range is inclusive and may
 * wrap, because a school year runs September to June.
 */
data class ConditionalLimit(
    val limitKmh: Int,
    val days: Int,
    val startMinute: Int,
    val endMinute: Int,
    val monthStart: Int,
    val monthEnd: Int
) {
    fun appliesAt(time: java.time.LocalDateTime): Boolean {
        val month = time.monthValue
        val inMonths = if (monthStart <= monthEnd) month in monthStart..monthEnd
        // September to June is one range that crosses the new year, not two.
        else month >= monthStart || month <= monthEnd
        if (!inMonths) return false
        // DayOfWeek is 1=Monday, and the mask counts from Monday too.
        if ((days shr (time.dayOfWeek.value - 1)) and 1 == 0) return false
        val minute = time.hour * 60 + time.minute
        return minute in startMinute until endMinute
    }
}

data class Route(
    val seconds: Double,
    val distanceMeters: Double,
    val geometry: List<LatLon>,
    val steps: List<RouteStep>,
    val junctions: List<RouteJunction>,
    val speedLimits: List<SpeedLimitChange>,
    val httpRequests: Int,
    val bytesFetched: Long
)

/**
 * The limit in force [meters] into the trip, 0 where none is known.
 *
 * [now] decides conditional limits. A school zone posted 50 that drops to 30
 * on school mornings is 30 at 08:15 on a Tuesday in October and 50 at the
 * same hour in July, and the sign has to say which.
 */
fun Route.speedLimitAt(
    meters: Double,
    now: java.time.LocalDateTime = java.time.LocalDateTime.now()
): Int {
    // The last change at or before this point. Routes carry a handful of
    // these, so a scan is honest and a binary search would be decoration.
    var answer: SpeedLimitChange? = null
    for (change in speedLimits) {
        if (change.atMeters > meters) break
        answer = change
    }
    val change = answer ?: return 0
    val conditional = change.conditional
    return if (conditional != null && conditional.appliesAt(now)) conditional.limitKmh
    else change.limitKmh
}

data class RouteBundle(val primary: Route, val alternatives: List<Route>)

data class SnapPoint(
    val lat: Double,
    val lon: Double,
    val distMeters: Double,
    val segment: String
) {
    val point: LatLon get() = LatLon(lat, lon)
}

class RangefindException(message: String) : Exception(message)
