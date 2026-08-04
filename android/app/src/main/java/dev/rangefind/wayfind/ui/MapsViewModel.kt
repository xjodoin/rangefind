package dev.rangefind.wayfind.ui

import android.content.Context
import android.location.Location
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.engine.EngineInfo
import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.engine.Place
import dev.rangefind.wayfind.engine.RangefindEngine
import dev.rangefind.wayfind.engine.Route
import dev.rangefind.wayfind.engine.RouteBundle
import dev.rangefind.wayfind.engine.Suggestion
import dev.rangefind.wayfind.location.LocationProvider
import dev.rangefind.wayfind.region.REGION_CATALOG
import dev.rangefind.wayfind.region.RegionEntry
import dev.rangefind.wayfind.region.RegionPreferences
import dev.rangefind.wayfind.region.RegionServer
import dev.rangefind.wayfind.region.RegionStatus
import dev.rangefind.wayfind.region.RegionStore
import dev.rangefind.wayfind.nav.NavUpdate
import dev.rangefind.wayfind.nav.NavigationCore
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
    val nav: NavUpdate? = null,
    val rerouting: Boolean = false,
    /** Bumped to ask the map to fly back to the user; state-driven so the
     *  map layer never needs a handle on the view model. */
    val recenterTick: Int = 0,
    val regions: List<RegionEntry> = emptyList(),
    val regionHost: String = "",
    val showRegions: Boolean = false
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
    /** Application context: only ever used to resolve user-facing strings. */
    private val context: Context,
    private val engine: RangefindEngine,
    private val locationProvider: LocationProvider,
    private val regionStore: RegionStore,
    private val regionServer: RegionServer,
    private val regionPrefs: RegionPreferences,
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

    private val core = NavigationCore(context)
    private var suppressSuggestFor: String? = null

    private val downloads = mutableMapOf<String, Job>()

    init {
        refreshRegions()
        viewModelScope.launch {
            // A region kept on the device wins over the network base: the user
            // downloaded it precisely so routing would not depend on a server.
            val active = regionPrefs.activeRegion?.takeIf { regionStore.isReady(it) }
            val base = active?.let { regionServer.baseUrlFor(it) } ?: routeBase
            runCatching { engine.init(searchBase, base) }
                .onSuccess { info -> _state.update { it.copy(loading = false, info = info) } }
                .onFailure { error ->
                    _state.update {
                        it.copy(
                            loading = false,
                            fatalError = error.message
                                ?: context.getString(R.string.engine_failed_to_start)
                        )
                    }
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

    // ---- offline regions ----------------------------------------------

    fun showRegions(show: Boolean) {
        _state.update { it.copy(showRegions = show) }
    }

    fun setRegionHost(host: String) {
        regionPrefs.host = host
        refreshRegions()
    }

    private fun refreshRegions() {
        val active = regionPrefs.activeRegion
        _state.update { current ->
            current.copy(
                regionHost = regionPrefs.host,
                regions = REGION_CATALOG.map { spec ->
                    val existing = current.regions.firstOrNull { it.spec.id == spec.id }
                    if (downloads[spec.id]?.isActive == true && existing != null) {
                        existing.copy(active = spec.id == active)
                    } else if (regionStore.isReady(spec.id)) {
                        RegionEntry(
                            spec = spec,
                            status = RegionStatus.Ready,
                            bytes = regionStore.bytesOf(spec.id),
                            updatedAt = regionStore.updatedAt(spec.id),
                            active = spec.id == active
                        )
                    } else {
                        RegionEntry(spec = spec, status = RegionStatus.Absent, error = existing?.error)
                    }
                }
            )
        }
    }

    private fun updateRegion(id: String, transform: (RegionEntry) -> RegionEntry) {
        _state.update { current ->
            current.copy(regions = current.regions.map { if (it.spec.id == id) transform(it) else it })
        }
    }

    /** Download or re-download a region; refreshing is the same operation. */
    fun preloadRegion(id: String) {
        if (downloads[id]?.isActive == true) return
        val source = regionPrefs.sourceUrlOf(id)
        updateRegion(id) { it.copy(status = RegionStatus.Downloading, done = 0, total = 0, error = null) }
        downloads[id] = viewModelScope.launch {
            runCatching {
                val manifest = engine.regionFiles(source)
                updateRegion(id) { it.copy(total = manifest.files.size) }
                regionStore.install(id, source, manifest.files) { done, total, bytes ->
                    updateRegion(id) { it.copy(done = done, total = total, bytes = bytes) }
                }
            }.onSuccess {
                downloads.remove(id)
                refreshRegions()
                // A refresh of the region already in use must be picked up, or
                // routing keeps answering from the copy that was just replaced.
                if (regionPrefs.activeRegion == id) applyRouteBase(regionServer.baseUrlFor(id))
            }.onFailure { error ->
                downloads.remove(id)
                regionStore.delete(id)
                updateRegion(id) {
                    it.copy(
                        status = RegionStatus.Failed,
                        error = error.message
                            ?: context.getString(R.string.region_download_failed)
                    )
                }
            }
        }
    }

    fun deleteRegion(id: String) {
        downloads.remove(id)?.cancel()
        regionStore.delete(id)
        if (regionPrefs.activeRegion == id) {
            regionPrefs.activeRegion = null
            viewModelScope.launch { applyRouteBase(routeBase) }
        }
        refreshRegions()
    }

    /** Route from a stored region, or pass null to go back to the network. */
    fun activateRegion(id: String?) {
        if (id != null && !regionStore.isReady(id)) return
        regionPrefs.activeRegion = id
        refreshRegions()
        viewModelScope.launch {
            applyRouteBase(id?.let { regionServer.baseUrlFor(it) } ?: routeBase)
        }
    }

    private suspend fun applyRouteBase(base: String) {
        runCatching { engine.useRouteBase(base) }
            .onSuccess { info ->
                _state.update {
                    it.copy(
                        info = it.info?.copy(
                            routing = info.routing,
                            routingError = info.routingError,
                            profile = info.profile,
                            routeBounds = info.routeBounds
                        ),
                        routes = null,
                        routeError = null
                    )
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
                            searchError = if (outcome.places.isEmpty()) {
                                context.getString(R.string.search_no_matches, query)
                            } else null
                        )
                    }
                }
                .onFailure { error ->
                    _state.update {
                        it.copy(
                            searching = false,
                            searchError = error.message
                                ?: context.getString(R.string.search_failed)
                        )
                    }
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
                name = context.getString(R.string.place_dropped_pin),
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
        val bounds = _state.value.info?.routeBounds
        if (bounds != null && !bounds.contains(destination.point)) {
            _state.update {
                it.copy(
                    selected = destination,
                    sheet = SheetMode.Directions,
                    routes = null,
                    routing = false,
                    routeError = context.getString(R.string.route_outside_coverage)
                )
            }
            return
        }
        val origin = _state.value.userLocation
        if (origin == null) {
            _state.update {
                it.copy(
                    routeError = context.getString(R.string.route_waiting_for_location),
                    sheet = SheetMode.Directions,
                    selected = destination
                )
            }
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
                    _state.update { it.copy(routing = false, routeError = humanizeRouteError(error)) }
                }
        }
    }

    /**
     * Engine diagnostics name byte distances and raw coordinates. Useful in a
     * log, useless on a phone: a failed snap almost always means the place sits
     * outside the region this route graph was built for.
     */
    private fun humanizeRouteError(error: Throwable): String {
        val message = error.message.orEmpty()
        val bounds = _state.value.info?.routeBounds
        val outside = _state.value.selected?.point?.let { bounds != null && !bounds.contains(it) } ?: false
        return when {
            outside || message.contains("Nearest road", ignoreCase = true) ->
                context.getString(R.string.route_outside_coverage)
            message.contains("no route", ignoreCase = true) ->
                context.getString(R.string.route_no_driveable_route)
            message.isBlank() -> context.getString(R.string.route_unavailable)
            else -> message
        }
    }

    fun selectRoute(index: Int) {
        // Switching mid-drive re-tracks onto the new line from where the car
        // already is, rather than restarting the trip at its origin.
        if (_state.value.sheet == SheetMode.Navigating) {
            if (!core.selectRoute(index)) return
            _voice.tryEmit(context.getString(R.string.nav_switching_route))
        }
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
        val greeting = core.start(_state.value.allRoutes, _state.value.activeRouteIndex)
        _state.update { it.copy(sheet = SheetMode.Navigating) }
        greeting?.let { _voice.tryEmit(it) }
    }

    fun stopNavigation() {
        core.stop()
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
        if (_state.value.sheet != SheetMode.Navigating) return

        val update = core.onLocation(
            point = point,
            speedMps = if (location.hasSpeed()) location.speed.toDouble() else 0.0,
            gpsBearing = if (location.hasBearing()) location.bearing.toDouble() else null,
            hasBearing = location.hasBearing()
        ) ?: return

        update.voice?.let { _voice.tryEmit(it) }
        _state.update { it.copy(nav = update) }

        if (update.arrived) {
            core.stop()
            return
        }
        if (update.offRoute) reroute(point)
    }

    private fun reroute(from: LatLon) {
        val destination = _state.value.selected ?: return
        if (_state.value.rerouting) return
        _state.update { it.copy(rerouting = true) }
        _voice.tryEmit(context.getString(R.string.nav_rerouting))
        viewModelScope.launch {
            runCatching { engine.route(from, destination.point, alternatives = 0) }
                .onSuccess { bundle ->
                    core.replaceRoutes(listOf(bundle.primary))
                    _state.update { it.copy(routes = bundle, activeRouteIndex = 0, rerouting = false) }
                }
                .onFailure { _state.update { it.copy(rerouting = false) } }
        }
    }

    companion object {
        fun factory(
            context: Context,
            engine: RangefindEngine,
            locationProvider: LocationProvider,
            regionStore: RegionStore,
            regionServer: RegionServer,
            regionPrefs: RegionPreferences,
            searchBase: String,
            routeBase: String
        ) = object : ViewModelProvider.Factory {
            @Suppress("UNCHECKED_CAST")
            override fun <T : ViewModel> create(modelClass: Class<T>): T =
                MapsViewModel(
                    context.applicationContext, engine, locationProvider, regionStore,
                    regionServer, regionPrefs, searchBase, routeBase
                ) as T
        }
    }
}
