package dev.rangefind.wayfind.car

import android.annotation.SuppressLint
import android.content.Context
import android.location.Location
import dev.rangefind.wayfind.BuildConfig
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.SEARCH_BASE
import dev.rangefind.wayfind.WayfindRuntime
import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.engine.Place
import dev.rangefind.wayfind.engine.Route
import dev.rangefind.wayfind.engine.RouteBundle
import dev.rangefind.wayfind.nav.GuidanceSpeaker
import dev.rangefind.wayfind.nav.Maneuver
import dev.rangefind.wayfind.nav.NavAlternative
import dev.rangefind.wayfind.nav.NavigationCore
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
    val speedMps: Double = 0.0,
    val speedLimitKmh: Int = 0,
    val turnDelta: Double = 0.0,
    /**
     * What the next instruction actually is, rather than a turn angle. A
     * roundabout's angles describe the curve of the circle and a slip road's
     * describe a gentle bend whichever way it goes, so neither can be read
     * off the geometry the head unit was reading it off.
     */
    val maneuver: Maneuver = Maneuver.Continue,
    /** The road the maneuver leads onto, written the way the signs are. */
    val maneuverTarget: String = "",
    /** The exit number on the panel, when this one has one. */
    val exitRef: String = "",
    /** Which exit to take at a roundabout; 0 when unknown. */
    val roundaboutExit: Int = 0,
    val alternatives: List<NavAlternative> = emptyList(),
    val rerouting: Boolean = false
) {
    val route: Route? get() = routes?.primary
}

/**
 * Car-side session state.
 *
 * The head unit shows a much narrower flow than the phone — find, preview,
 * drive — but the driving itself is identical, so the rules about off-route,
 * announcements and reachable alternates come from the shared
 * [NavigationCore] rather than a second copy of them.
 */
class CarNavigator(private val context: Context) {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private val _state = MutableStateFlow(CarState())
    val state: StateFlow<CarState> = _state.asStateFlow()

    private val core = NavigationCore(context)
    private var locationJob: Job? = null
    private var searchJob: Job? = null

    // Driving is exactly when the driver should not be reading the screen, so
    // the head unit speaks the same guidance the phone does — tagged as
    // navigation audio so the car plays it and ducks the music.
    private val speaker = GuidanceSpeaker(context)

    private fun say(phrase: String) = speaker.say(phrase)

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
        speaker.shutdown()
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
                            error = if (outcome.places.isEmpty()) {
                                context.getString(R.string.car_no_matches)
                            } else null
                        )
                    }
                }
                .onFailure { error ->
                    _state.update {
                        it.copy(
                            searching = false,
                            error = error.message ?: context.getString(R.string.search_failed)
                        )
                    }
                }
        }
    }

    fun chooseDestination(place: Place, onRouted: () -> Unit) {
        val origin = _state.value.location
        _state.update { it.copy(destination = place, routing = true, routes = null, error = null) }
        scope.launch {
            val engine = WayfindRuntime.engineOrNull() ?: return@launch
            if (origin == null) {
                _state.update {
                    it.copy(
                        routing = false,
                        error = context.getString(R.string.car_waiting_for_location)
                    )
                }
                return@launch
            }
            runCatching { engine.route(origin, place.point, alternatives = 2) }
                .onSuccess { bundle ->
                    _state.update { it.copy(routing = false, routes = bundle) }
                    onRouted()
                }
                .onFailure { error ->
                    _state.update {
                        it.copy(
                            routing = false,
                            error = error.message ?: context.getString(R.string.car_no_route)
                        )
                    }
                }
        }
    }

    fun startNavigation() {
        val routes = _state.value.routes ?: return
        val greeting = core.start(listOf(routes.primary) + routes.alternatives, 0)
        _state.update { it.copy(navigating = true) }
        greeting?.let { say(it) }
    }

    fun stopNavigation() {
        core.stop()
        _state.update {
            it.copy(navigating = false, traveled = emptyList(), ahead = emptyList())
        }
    }

    /** Take one of the alternates without leaving the drive. */
    fun selectRoute(index: Int) {
        if (!core.selectRoute(index)) return
        say(context.getString(R.string.nav_switching_route))
    }

    /**
     * The vehicle's own heading, or null when it cannot be trusted.
     *
     * Taken from the fix rather than NavUpdate.bearing on purpose: that one
     * falls back to the matched road's direction so the arrow stays steady,
     * and on an off-route event the matched road is the one the driver has
     * just left — precisely the direction a reroute must not assume.
     */
    private fun travelHeading(location: Location): Double? =
        if (location.hasBearing() && location.speed >= NavigationCore.MIN_HEADING_SPEED_MPS) {
            location.bearing.toDouble()
        } else null

    private fun reroute(from: LatLon, heading: Double?, accuracyMeters: Double = 0.0) {
        val destination = _state.value.destination ?: return
        if (_state.value.rerouting) return
        _state.update { it.copy(rerouting = true) }
        say(context.getString(R.string.nav_rerouting))
        scope.launch {
            val engine = WayfindRuntime.engineOrNull() ?: return@launch
            runCatching {
                engine.route(
                    from, destination.point,
                    alternatives = 0,
                    fromHeading = heading,
                    accuracyMeters = accuracyMeters
                )
            }
                .onSuccess { bundle ->
                    core.replaceRoutes(listOf(bundle.primary))
                    _state.update { it.copy(routes = bundle, rerouting = false) }
                }
                .onFailure { _state.update { it.copy(rerouting = false) } }
        }
    }

    private fun onLocation(location: Location) {
        val point = LatLon(location.latitude, location.longitude)
        _state.update { it.copy(location = point) }

        val update = core.onLocation(
            point = point,
            speedMps = if (location.hasSpeed()) location.speed.toDouble() else 0.0,
            gpsBearing = if (location.hasBearing()) location.bearing.toDouble() else null,
            hasBearing = location.hasBearing(),
            accuracyMeters = if (location.hasAccuracy()) location.accuracy.toDouble() else 0.0
        ) ?: return

        update.voice?.let { say(it) }
        _state.update {
            it.copy(
                stepName = update.stepName,
                nextStepName = update.nextStepName,
                metersToManeuver = update.metersToManeuver,
                remainingMeters = update.remainingMeters,
                remainingSeconds = update.remainingSeconds,
                traveled = update.traveled,
                ahead = update.ahead,
                position = update.position,
                bearing = update.bearing,
                speedMps = update.speedMps,
                speedLimitKmh = update.speedLimitKmh,
                turnDelta = update.turnDelta,
                maneuver = update.maneuver,
                maneuverTarget = update.maneuverTarget,
                exitRef = update.exitRef,
                roundaboutExit = update.roundaboutExit,
                alternatives = update.alternatives
            )
        }

        if (update.arrived) {
            core.stop()
            _state.update { it.copy(navigating = false) }
            return
        }
        if (update.offRoute) {
            reroute(
                point,
                travelHeading(location),
                if (location.hasAccuracy()) location.accuracy.toDouble() else 0.0
            )
        }
    }

}
