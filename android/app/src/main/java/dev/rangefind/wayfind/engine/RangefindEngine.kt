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
    suspend fun route(from: LatLon, to: LatLon, alternatives: Int = 2): RouteBundle
    suspend fun snap(point: LatLon): SnapPoint?
}

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
    val speedLimitKmh: Int
)

/** 1 signals, 2 stop, 3 give way, 4 level crossing, 5 crossing. */
data class RouteJunction(
    val kind: Int,
    val lat: Double,
    val lon: Double,
    val atMeters: Double
)

data class Route(
    val seconds: Double,
    val distanceMeters: Double,
    val geometry: List<LatLon>,
    val steps: List<RouteStep>,
    val junctions: List<RouteJunction>,
    val httpRequests: Int,
    val bytesFetched: Long
)

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
