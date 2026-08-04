package dev.rangefind.wayfind.car

import android.annotation.SuppressLint
import android.content.Context
import android.location.Location
import dev.rangefind.wayfind.BuildConfig
import dev.rangefind.wayfind.SEARCH_BASE
import dev.rangefind.wayfind.WayfindRuntime
import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.engine.Place
import dev.rangefind.wayfind.engine.Route
import dev.rangefind.wayfind.engine.RouteBundle
import dev.rangefind.wayfind.nav.RouteTracker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlin.math.max

data class CarState(
    val ready: Boolean = false,
    val location: LatLon? = null,
    val results: List<Place> = emptyList(),
    val searching: Boolean = false,
    val destination: Place? = null,
    val routes: RouteBundle? = null,
    val routing: Boolean = false,
    val error: String? = null,
    val navigating: Boolean = false,
    val stepName: String = "",
    val nextStepName: String = "",
    val metersToManeuver: Double = 0.0,
    val remainingMeters: Double = 0.0,
    val remainingSeconds: Double = 0.0,
    val traveled: List<LatLon> = emptyList(),
    val ahead: List<LatLon> = emptyList(),
    val position: LatLon? = null,
    val bearing: Double = 0.0,
    val speedLimitKmh: Int = 0,
    val turnDelta: Double = 0.0
) {
    val route: Route? get() = routes?.primary
}

/**
 * Car-side session state.
 *
 * Deliberately its own small state machine rather than a second copy of the
 * phone view model: the head unit shows a much narrower flow (find, preview,
 * drive), and the parts worth sharing — the engine and the route matching in
 * [RouteTracker] — are shared already.
 */
class CarNavigator(private val context: Context) {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private val _state = MutableStateFlow(CarState())
    val state: StateFlow<CarState> = _state.asStateFlow()

    private var tracker: RouteTracker? = null
    private var lastAlong = 0.0
    private var locationJob: Job? = null
    private var searchJob: Job? = null

    fun start() {
        WayfindRuntime.ensureStarted(context)
        scope.launch {
            WayfindRuntime.engine(context, BuildConfig.ROUTE_BASE_URL)
            _state.update { it.copy(ready = true) }
        }
        startLocation()
    }

    fun stop() {
        locationJob?.cancel()
        scope.cancel()
    }

    @SuppressLint("MissingPermission")
    private fun startLocation() {
        if (locationJob != null) return
        locationJob = scope.launch {
            runCatching {
                WayfindRuntime.locationProvider.updates().collectLatest { onLocation(it) }
            }
        }
    }

    fun search(query: String) {
        val text = query.trim()
        if (text.isEmpty()) return
        searchJob?.cancel()
        _state.update { it.copy(searching = true, error = null) }
        searchJob = scope.launch {
            val engine = WayfindRuntime.engineOrNull()
                ?: WayfindRuntime.engine(context, BuildConfig.ROUTE_BASE_URL)
            runCatching { engine.search(text, _state.value.location, size = 12) }
                .onSuccess { outcome ->
                    _state.update {
                        it.copy(
                            searching = false,
                            results = outcome.places,
                            error = if (outcome.places.isEmpty()) "No matches" else null
                        )
                    }
                }
                .onFailure { error ->
                    _state.update { it.copy(searching = false, error = error.message ?: "Search failed") }
                }
        }
    }

    fun chooseDestination(place: Place, onRouted: () -> Unit) {
        val origin = _state.value.location
        _state.update { it.copy(destination = place, routing = true, routes = null, error = null) }
        scope.launch {
            val engine = WayfindRuntime.engineOrNull() ?: return@launch
            if (origin == null) {
                _state.update { it.copy(routing = false, error = "Waiting for location") }
                return@launch
            }
            runCatching { engine.route(origin, place.point, alternatives = 2) }
                .onSuccess { bundle ->
                    _state.update { it.copy(routing = false, routes = bundle) }
                    onRouted()
                }
                .onFailure { error ->
                    _state.update { it.copy(routing = false, error = error.message ?: "No route") }
                }
        }
    }

    fun startNavigation() {
        val route = _state.value.route ?: return
        tracker = RouteTracker(route)
        lastAlong = 0.0
        _state.update { it.copy(navigating = true) }
    }

    fun stopNavigation() {
        tracker = null
        _state.update {
            it.copy(navigating = false, traveled = emptyList(), ahead = emptyList())
        }
    }

    private fun onLocation(location: Location) {
        val point = LatLon(location.latitude, location.longitude)
        _state.update { it.copy(location = point) }

        val active = tracker ?: return
        val route = active.route
        val match = active.match(point, lastAlong) ?: return
        lastAlong = max(lastAlong, match.distanceAlong)

        val (traveled, ahead) = active.split(lastAlong)
        val geometric = max(0.0, active.totalMeters - lastAlong)
        val fraction = if (active.totalMeters <= 0) 0.0
        else (geometric / active.totalMeters).coerceIn(0.0, 1.0)
        val stepIndex = active.stepIndexAt(lastAlong)
        val step = route.steps.getOrNull(stepIndex)
        val next = route.steps.getOrNull(stepIndex + 1)

        _state.update {
            it.copy(
                stepName = step?.name.orEmpty(),
                nextStepName = next?.name.orEmpty(),
                metersToManeuver = active.metersToNextManeuver(lastAlong) ?: geometric,
                remainingMeters = route.distanceMeters * fraction,
                remainingSeconds = route.seconds * fraction,
                traveled = traveled,
                ahead = ahead,
                position = match.snapped,
                bearing = if (location.hasBearing() && location.speed > 1.5f) location.bearing.toDouble()
                else match.bearing,
                speedLimitKmh = step?.speedLimitKmh ?: 0,
                turnDelta = turnDeltaAt(route, next?.at)
            )
        }
    }

    /** Signed turn angle at a step boundary, for the car's maneuver glyph. */
    private fun turnDeltaAt(route: Route, at: Int?): Double {
        val index = at ?: return 0.0
        val points = route.geometry
        if (index <= 0 || index >= points.size - 1) return 0.0
        val incoming = dev.rangefind.wayfind.nav.bearingDegrees(points[index - 1], points[index])
        val outgoing = dev.rangefind.wayfind.nav.bearingDegrees(points[index], points[index + 1])
        return dev.rangefind.wayfind.nav.bearingDelta(incoming, outgoing)
    }
}
