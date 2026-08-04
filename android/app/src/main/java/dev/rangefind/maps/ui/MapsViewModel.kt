package dev.rangefind.maps.ui

import android.location.Location
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import dev.rangefind.maps.engine.EngineInfo
import dev.rangefind.maps.engine.LatLon
import dev.rangefind.maps.engine.Place
import dev.rangefind.maps.engine.RangefindEngine
import dev.rangefind.maps.engine.Route
import dev.rangefind.maps.engine.RouteBundle
import dev.rangefind.maps.engine.Suggestion
import dev.rangefind.maps.location.LocationProvider
import dev.rangefind.maps.nav.RouteTracker
import dev.rangefind.maps.nav.absBearingDelta
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlin.math.max

enum class SheetMode { Search, Place, Directions, Navigating }

data class NavProgress(
    val stepIndex: Int,
    val stepName: String,
    val nextStepName: String,
    val metersToManeuver: Double,
    val remainingMeters: Double,
    val remainingSeconds: Double,
    val traveled: List<LatLon>,
    val ahead: List<LatLon>,
    val position: LatLon,
    val bearing: Double,
    val speedMps: Double,
    val offRoute: Boolean,
    val arrived: Boolean
)

data class UiState(
    val loading: Boolean = true,
    val info: EngineInfo? = null,
    val fatalError: String? = null,
    val query: String = "",
    val suggestions: List<Suggestion> = emptyList(),
    val results: List<Place> = emptyList(),
    val searching: Boolean = false,
    val searchError: String? = null,
    val selected: Place? = null,
    val sheet: SheetMode = SheetMode.Search,
    val routing: Boolean = false,
    val routeError: String? = null,
    val routes: RouteBundle? = null,
    val activeRouteIndex: Int = 0,
    val userLocation: LatLon? = null,
    val nav: NavProgress? = null,
    val rerouting: Boolean = false,
    /** Bumped to ask the map to fly back to the user; state-driven so the
     *  map layer never needs a handle on the view model. */
    val recenterTick: Int = 0
) {
    val activeRoute: Route?
        get() = routes?.let { bundle ->
            if (activeRouteIndex == 0) bundle.primary else bundle.alternatives.getOrNull(activeRouteIndex - 1)
        }

    val allRoutes: List<Route>
        get() = routes?.let { listOf(it.primary) + it.alternatives } ?: emptyList()
}

@OptIn(FlowPreview::class)
class MapsViewModel(
    private val engine: RangefindEngine,
    private val locationProvider: LocationProvider,
    private val searchBase: String,
    private val routeBase: String
) : ViewModel() {

    private val _state = MutableStateFlow(UiState())
    val state: StateFlow<UiState> = _state.asStateFlow()

    private val _voice = MutableSharedFlow<String>(extraBufferCapacity = 4)
    val voice: SharedFlow<String> = _voice.asSharedFlow()

    /** Where the map is looking; used to bias search when there is no fix. */
    var mapCenter: LatLon? = null

    private val queryFlow = MutableStateFlow("")
    private var searchJob: Job? = null
    private var routeJob: Job? = null
    private var locationJob: Job? = null

    private var tracker: RouteTracker? = null
    private var lastAlong = 0.0
    private var announcedStep = -1
    private var announcedThreshold = Int.MAX_VALUE
    private var offRouteStrikes = 0
    private var suppressSuggestFor: String? = null

    init {
        viewModelScope.launch {
            runCatching { engine.init(searchBase, routeBase) }
                .onSuccess { info -> _state.update { it.copy(loading = false, info = info) } }
                .onFailure { error ->
                    _state.update { it.copy(loading = false, fatalError = error.message ?: "Engine failed to start") }
                }
        }

        viewModelScope.launch {
            queryFlow
                .debounce(180)
                .distinctUntilChanged()
                .collectLatest { text ->
                    // A just-picked suggestion already resolved to a search;
                    // re-opening the dropdown over its results is noise.
                    if (text == suppressSuggestFor) {
                        suppressSuggestFor = null
                        return@collectLatest
                    }
                    if (text.trim().length < 2) {
                        _state.update { it.copy(suggestions = emptyList()) }
                        return@collectLatest
                    }
                    runCatching { engine.suggest(text, anchor()) }
                        .onSuccess { list -> _state.update { it.copy(suggestions = list) } }
                        .onFailure { _state.update { it.copy(suggestions = emptyList()) } }
                }
        }
    }

    private fun anchor(): LatLon? = _state.value.userLocation ?: mapCenter

    // ---- search -------------------------------------------------------

    fun onQueryChange(text: String) {
        _state.update { it.copy(query = text, searchError = null) }
        queryFlow.value = text
    }

    fun clearQuery() {
        searchJob?.cancel()
        queryFlow.value = ""
        _state.update {
            it.copy(
                query = "",
                suggestions = emptyList(),
                results = emptyList(),
                selected = null,
                searchError = null,
                sheet = SheetMode.Search
            )
        }
    }

    /**
     * Picking a prediction searches by its canonical query and home shard, so
     * "Gare Centrale" resolves to the station itself instead of a text match
     * that its display name may not even contain.
     */
    fun pickSuggestion(suggestion: Suggestion) {
        val query = suggestion.selectionQuery.ifBlank { suggestion.text }
        suppressSuggestFor = query
        queryFlow.value = query
        submitSearch(query, suggestion.selectionShards)
    }

    fun submitSearch(text: String = _state.value.query, shards: List<String> = emptyList()) {
        val query = text.trim()
        if (query.isEmpty()) return
        searchJob?.cancel()
        _state.update { it.copy(query = query, searching = true, suggestions = emptyList(), searchError = null) }
        searchJob = viewModelScope.launch {
            runCatching { engine.search(query, anchor(), shards = shards) }
                .onSuccess { outcome ->
                    _state.update {
                        it.copy(
                            searching = false,
                            results = outcome.places,
                            sheet = SheetMode.Search,
                            searchError = if (outcome.places.isEmpty()) "No matches for “$query”" else null
                        )
                    }
                }
                .onFailure { error ->
                    _state.update { it.copy(searching = false, searchError = error.message ?: "Search failed") }
                }
        }
    }

    fun selectPlace(place: Place) {
        _state.update { it.copy(selected = place, sheet = SheetMode.Place, suggestions = emptyList()) }
    }

    fun dismissPlace() {
        _state.update {
            it.copy(
                selected = null,
                sheet = SheetMode.Search,
                routes = null,
                routeError = null,
                activeRouteIndex = 0
            )
        }
    }

    fun dropPin(point: LatLon) {
        viewModelScope.launch {
            val resolved = runCatching { engine.reverse(point) }.getOrNull()
            val place = resolved ?: Place(
                id = "pin",
                name = "Dropped pin",
                address = String.format("%.5f, %.5f", point.lat, point.lon),
                locality = "",
                category = "",
                type = "",
                lat = point.lat,
                lon = point.lon,
                distanceMeters = null
            )
            selectPlace(place)
        }
    }

    // ---- directions ---------------------------------------------------

    fun requestDirections(target: Place? = null) {
        val destination = target ?: _state.value.selected ?: return
        val origin = _state.value.userLocation
        if (origin == null) {
            _state.update { it.copy(routeError = "Waiting for your location", sheet = SheetMode.Directions, selected = destination) }
            return
        }
        routeJob?.cancel()
        _state.update {
            it.copy(
                selected = destination,
                sheet = SheetMode.Directions,
                routing = true,
                routeError = null,
                routes = null,
                activeRouteIndex = 0
            )
        }
        routeJob = viewModelScope.launch {
            runCatching { engine.route(origin, destination.point, alternatives = 2) }
                .onSuccess { bundle -> _state.update { it.copy(routing = false, routes = bundle) } }
                .onFailure { error ->
                    _state.update { it.copy(routing = false, routeError = error.message ?: "No route found") }
                }
        }
    }

    fun selectRoute(index: Int) {
        _state.update { it.copy(activeRouteIndex = index) }
    }

    fun exitDirections() {
        routeJob?.cancel()
        _state.update {
            it.copy(sheet = if (it.selected != null) SheetMode.Place else SheetMode.Search, routes = null, routeError = null)
        }
    }

    // ---- navigation ---------------------------------------------------

    fun startNavigation() {
        val route = _state.value.activeRoute ?: return
        tracker = RouteTracker(route)
        lastAlong = 0.0
        announcedStep = -1
        announcedThreshold = Int.MAX_VALUE
        offRouteStrikes = 0
        _state.update { it.copy(sheet = SheetMode.Navigating) }
        route.steps.firstOrNull()?.let { first ->
            _voice.tryEmit("Starting navigation. Head onto ${first.name.ifBlank { "the route" }}.")
        }
    }

    fun stopNavigation() {
        tracker = null
        _state.update { it.copy(sheet = SheetMode.Directions, nav = null, rerouting = false) }
    }

    fun recenter() {
        _state.update { it.copy(recenterTick = it.recenterTick + 1) }
    }

    fun startLocationUpdates() {
        if (locationJob != null) return
        locationJob = viewModelScope.launch {
            locationProvider.updates().collectLatest { onLocation(it) }
        }
    }

    fun stopLocationUpdates() {
        locationJob?.cancel()
        locationJob = null
    }

    private fun onLocation(location: Location) {
        val point = LatLon(location.latitude, location.longitude)
        _state.update { it.copy(userLocation = point) }

        val activeTracker = tracker ?: return
        if (_state.value.sheet != SheetMode.Navigating) return

        val match = activeTracker.match(point, lastAlong) ?: return
        val route = activeTracker.route

        // GPS bearing is unreliable at low speed; fall back to the road's.
        val speed = if (location.hasSpeed()) location.speed.toDouble() else 0.0
        val heading = if (speed > 1.5 && location.hasBearing()) location.bearing.toDouble() else match.bearing
        val headingWrong = speed > 2.0 && absBearingDelta(match.bearing, heading) > 110.0

        val off = match.crossTrackMeters > OFF_ROUTE_METERS || headingWrong
        offRouteStrikes = if (off) offRouteStrikes + 1 else 0

        lastAlong = max(lastAlong, match.distanceAlong)
        val (traveled, ahead) = activeTracker.split(lastAlong)

        // Arrival is judged on real geometry, but the numbers shown are scaled
        // from the route's own totals: summing haversine over a simplified
        // polyline drifts a few percent from the index's road distances, and
        // a footer that disagrees with the summary card reads as a bug.
        val geometricRemaining = max(0.0, activeTracker.totalMeters - lastAlong)
        val fraction = if (activeTracker.totalMeters <= 0) 0.0
        else (geometricRemaining / activeTracker.totalMeters).coerceIn(0.0, 1.0)
        val remainingMeters = route.distanceMeters * fraction
        val stepIndex = activeTracker.stepIndexAt(lastAlong)
        val toManeuver = activeTracker.metersToNextManeuver(lastAlong)
        val arrived = geometricRemaining < ARRIVAL_METERS

        _state.update {
            it.copy(
                nav = NavProgress(
                    stepIndex = stepIndex,
                    stepName = route.steps.getOrNull(stepIndex)?.name.orEmpty(),
                    nextStepName = route.steps.getOrNull(stepIndex + 1)?.name.orEmpty(),
                    metersToManeuver = toManeuver ?: remainingMeters,
                    remainingMeters = remainingMeters,
                    remainingSeconds = route.seconds * fraction,
                    traveled = traveled,
                    ahead = ahead,
                    position = match.snapped,
                    bearing = heading,
                    speedMps = speed,
                    offRoute = offRouteStrikes >= OFF_ROUTE_STRIKES,
                    arrived = arrived
                )
            )
        }

        if (arrived) {
            _voice.tryEmit("You have arrived.")
            tracker = null
            return
        }

        if (offRouteStrikes >= OFF_ROUTE_STRIKES) {
            reroute(point)
            return
        }

        announce(stepIndex, toManeuver, route.steps.getOrNull(stepIndex + 1)?.name.orEmpty())
    }

    private fun announce(stepIndex: Int, metersToManeuver: Double?, nextName: String) {
        val meters = metersToManeuver ?: return
        if (nextName.isBlank()) return
        val threshold = ANNOUNCE_THRESHOLDS.firstOrNull { meters <= it } ?: return
        if (stepIndex == announcedStep && threshold >= announcedThreshold) return
        announcedStep = stepIndex
        announcedThreshold = threshold
        _voice.tryEmit(
            if (threshold <= 60) "Now, continue onto $nextName."
            else "In ${formatManeuverDistance(meters)}, continue onto $nextName."
        )
    }

    private fun reroute(from: LatLon) {
        val destination = _state.value.selected ?: return
        if (_state.value.rerouting) return
        _state.update { it.copy(rerouting = true) }
        _voice.tryEmit("Rerouting.")
        viewModelScope.launch {
            runCatching { engine.route(from, destination.point, alternatives = 0) }
                .onSuccess { bundle ->
                    tracker = RouteTracker(bundle.primary)
                    lastAlong = 0.0
                    offRouteStrikes = 0
                    announcedStep = -1
                    announcedThreshold = Int.MAX_VALUE
                    _state.update { it.copy(routes = bundle, activeRouteIndex = 0, rerouting = false) }
                }
                .onFailure { _state.update { it.copy(rerouting = false) } }
        }
    }

    companion object {
        private const val OFF_ROUTE_METERS = 45.0
        private const val OFF_ROUTE_STRIKES = 3
        private const val ARRIVAL_METERS = 25.0
        private val ANNOUNCE_THRESHOLDS = listOf(60, 200, 400)

        fun factory(
            engine: RangefindEngine,
            locationProvider: LocationProvider,
            searchBase: String,
            routeBase: String
        ) = object : ViewModelProvider.Factory {
            @Suppress("UNCHECKED_CAST")
            override fun <T : ViewModel> create(modelClass: Class<T>): T =
                MapsViewModel(engine, locationProvider, searchBase, routeBase) as T
        }
    }
}
