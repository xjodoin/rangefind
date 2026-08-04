package dev.rangefind.maps.engine

/**
 * The app's whole dependency on rangefind. The JS host behind it is an
 * implementation detail: today a headless WebView, tomorrow an embedded V8 or
 * Node runtime, without touching anything above this line.
 */
interface RangefindEngine {
    suspend fun init(searchBase: String, routeBase: String): EngineInfo
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

data class EngineInfo(
    val attribution: String,
    val license: String,
    val total: Long,
    val routing: Boolean,
    val routingError: String?,
    val profile: String
)

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
    val at: Int
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
