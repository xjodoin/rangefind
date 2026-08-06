package dev.rangefind.wayfind.ui

import android.content.Context
import android.location.Location
import android.os.SystemClock
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import dev.rangefind.wayfind.BuildConfig
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.engine.EngineInfo
import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.engine.Place
import dev.rangefind.wayfind.engine.RangefindEngine
import dev.rangefind.wayfind.engine.Route
import dev.rangefind.wayfind.engine.RouteBundle
import dev.rangefind.wayfind.engine.Suggestion
import dev.rangefind.wayfind.location.HeadingSensor
import dev.rangefind.wayfind.location.LocationProvider
import dev.rangefind.wayfind.nav.MotionModel
import dev.rangefind.wayfind.region.REGION_CATALOG
import dev.rangefind.wayfind.region.RegionEntry
import dev.rangefind.wayfind.region.RegionPreferences
import dev.rangefind.wayfind.region.RegionServer
import dev.rangefind.wayfind.region.RegionStatus
import dev.rangefind.wayfind.region.RegionStore
import dev.rangefind.wayfind.nav.NavUpdate
import dev.rangefind.wayfind.nav.TravelMode
import dev.rangefind.wayfind.ui.map.RenderCadence
import dev.rangefind.wayfind.nav.TripRecorder
import dev.rangefind.wayfind.nav.NavigationCore
import dev.rangefind.wayfind.nav.RerouteLimiter
import dev.rangefind.wayfind.nav.TravelHeading
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
    val showRegions: Boolean = false,
    val recordTrips: Boolean = false,
    /** How many faults the driver has flagged on this drive. */
    val issueMarks: Int = 0,
    /** Driving, cycling or walking. */
    val mode: TravelMode = TravelMode.Car,
    /** Modes this area actually has an index for. */
    val modesAvailable: Set<TravelMode> = setOf(TravelMode.Car)
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
    private val recorder = TripRecorder(context)
    // A fix a second is not a position; it is a sample of one. The model
    // carries the vehicle between them and decides how far each sample is
    // allowed to move the estimate.
    private val motion = MotionModel()
    private val rerouteLimiter = RerouteLimiter()
    private val travel = TravelHeading()
    private val heading = HeadingSensor(context)
    private var suppressSuggestFor: String? = null

    private val downloads = mutableMapOf<String, Job>()

    init {
        refreshRegions()
        viewModelScope.launch {
            // A region kept on the device wins over the network base: the user
            // downloaded it precisely so routing would not depend on a server.
            val active = resolveActiveRegion()
            val mode = regionPrefs.travelMode
            core.mode = mode
            _state.update { it.copy(mode = mode, modesAvailable = modesAvailable()) }
            // The remembered mode decides the index, region or not. Opening
            // the driving graph while the selector says Walk drew a driving
            // route under a walking label, which is worse than either.
            val base = routeBaseFor(mode)
            // Adopting a region changes which row reads as active.
            if (active != null) refreshRegions()
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
        // Opening the sheet is when the answer is worth having and when the
        // user is looking at the place it would be shown.
        if (show) checkForRegionUpdates()
    }

    fun setRegionHost(host: String) {
        regionPrefs.host = host
        refreshRegions()
        checkForRegionUpdates()
    }

    /**
     * Asks the server whether the regions kept here are still the ones it
     * publishes.
     *
     * Packs are content-addressed, so this is one small request per region and
     * an exact answer — not a heuristic about dates. Anything that fails is
     * left alone: a region that cannot be checked is not stale, it is
     * unchecked, and the difference matters most exactly when the phone is
     * offline and that region is all it has.
     */
    fun checkForRegionUpdates() {
        viewModelScope.launch {
            val stale = mutableSetOf<String>()
            for (spec in REGION_CATALOG) {
                if (!regionStore.isReady(spec.id)) continue
                val stored = regionStore.storedRoot(spec.id) ?: continue
                val published = regionStore.publishedRoot(regionPrefs.sourceUrlOf(spec.id)) ?: continue
                if (published != stored) stale += spec.id
            }
            _state.update { current ->
                current.copy(regions = current.regions.map { it.copy(stale = it.spec.id in stale) })
            }
        }
    }

    private fun refreshRegions() {
        val active = regionPrefs.activeRegion
        _state.update { current ->
            current.copy(
                regionHost = regionPrefs.host,
                recordTrips = regionPrefs.recordTrips,
                modesAvailable = modesAvailable(),
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
                            active = spec.id == active,
                            // Carried over rather than recomputed: the check is
                            // a network round trip, and this runs on every
                            // state change.
                            stale = existing?.stale == true
                        )
                    } else {
                        RegionEntry(spec = spec, status = RegionStatus.Absent, error = existing?.error)
                    }
                }
            )
        }
    }

    /**
     * The region to route from. A stored region wins over the network base:
     * the user downloaded it precisely so routing would not depend on a
     * server. When they have never chosen a source, a region sitting ready on
     * the device is the better default — the network base is not reachable
     * from every device, and an index they already waited for always is.
     */
    /**
     * Where this mode's routing comes from.
     *
     * A downloaded region wins, as always. Otherwise the same host the
     * regions are fetched from serves the network index, which keeps one
     * setting behind all three modes rather than three that can disagree.
     */
    private fun routeBaseFor(mode: TravelMode): String {
        val id = currentPlace(mode)?.let { mode.regionId(it) }
        if (id != null) {
            // Kept on the device wins. Otherwise the same host the regions are
            // fetched from serves it, which is a setting the user can reach
            // and correct — unlike the build's own default.
            if (regionStore.isReady(id)) return regionServer.baseUrlFor(id)
            return regionPrefs.sourceUrlOf(id)
        }
        return if (mode == TravelMode.Car) routeBase else routeBase.trimEnd('/') + mode.suffix + "/"
    }

    /**
     * The area routing is about.
     *
     * The active region names it. With no region chosen the network is the
     * source, and it still has to point somewhere: a place the device has
     * already downloaded something for is the one the user is demonstrably
     * interested in. Falling through to the build's own base instead sent a
     * physical phone at the emulator's loopback alias, which resolves nowhere
     * and times out as "no answer from the server".
     */
    private fun currentPlace(mode: TravelMode): String? {
        regionPrefs.activeRegion?.let { active ->
            REGION_CATALOG.firstOrNull { it.id == active }
                ?.let { spec -> return spec.id.removeSuffix(spec.mode.suffix) }
        }
        val places = REGION_CATALOG.map { it.id.removeSuffix(it.mode.suffix) }.distinct()
        // A place already downloaded for this very mode, and of those the one
        // downloaded most recently. Taking the first in catalogue order sent
        // a driver in Quebec at a Luxembourg index that happened to be listed
        // above it and still on the device, which then refused every
        // destination as outside its coverage.
        places.filter { regionStore.isReady(mode.regionId(it)) }
            .maxByOrNull { regionStore.updatedAt(mode.regionId(it)) }
            ?.let { return it }
        // Otherwise the one most recently downloaded: catalogue order is an
        // authoring accident, and picking by it asked the host for a
        // Luxembourg cycling index that was never published.
        return places
            .mapNotNull { place ->
                TravelMode.entries
                    .map { regionStore.updatedAt(it.regionId(place)) }
                    .maxOrNull()
                    ?.takeIf { it > 0 }
                    ?.let { place to it }
            }
            .maxByOrNull { it.second }
            ?.first
    }

    /** Which modes this area can actually route, downloaded or served. */
    private fun modesAvailable(): Set<TravelMode> {
        val place = currentPlace(_state.value.mode)
            // Without a region the network base is the only source, and it
            // publishes one index per profile alongside itself. Offering all
            // three is right there; a mode that turns out to be missing
            // reports it rather than being hidden before it is tried.
            ?: return TravelMode.entries.toSet()
        val downloaded = TravelMode.entries.filter { regionStore.isReady(it.regionId(place)) }
        // Once a region is kept on the device, driving offline and walking
        // only-when-online would be a trap. What is downloaded is what the
        // selector offers, and the rest are visibly unavailable.
        return if (downloaded.isEmpty()) TravelMode.entries.toSet() else downloaded.toSet()
    }

    /**
     * Switch how the trip is made. The route index changes with it, and any
     * route already on screen is recomputed: a walking line drawn from a
     * driving graph would send somebody the wrong way down a dual carriageway.
     */
    fun setTravelMode(mode: TravelMode) {
        if (mode == _state.value.mode) return
        regionPrefs.travelMode = mode
        core.mode = mode
        _state.update { it.copy(mode = mode) }
        viewModelScope.launch {
            applyRouteBase(routeBaseFor(mode))
            if (_state.value.selected != null && _state.value.sheet == SheetMode.Directions) {
                requestDirections(_state.value.selected)
            }
        }
    }

    private fun resolveActiveRegion(): String? {
        regionPrefs.activeRegion?.takeIf { regionStore.isReady(it) }?.let { return it }
        if (regionPrefs.hasChosenSource) return null
        return REGION_CATALOG.map { it.id }.firstOrNull { regionStore.isReady(it) }
            ?.also { regionPrefs.activeRegion = it }
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
                // Downloading a region is the user asking to route from it.
                // Nothing else adopts it, so leaving the base alone means
                // directions keep going to the network — which on a real
                // device is an address that never answers — with the region
                // they just waited for sitting unused. Only claim an unset
                // base: choosing the network is a real choice, and a refresh
                // must not drag routing away from another active region.
                if (regionPrefs.activeRegion == null && !regionPrefs.hasChosenSource) {
                    regionPrefs.activeRegion = id
                }
                refreshRegions()
                // A refresh of the region already in use must be picked up, or
                // routing keeps answering from the copy that was just replaced.
                if (regionPrefs.activeRegion == id) applyRouteBase(regionServer.baseUrlFor(id))
            }.onFailure { error ->
                downloads.remove(id)
                regionStore.delete(id)
                updateRegion(id) {
                    it.copy(status = RegionStatus.Failed, error = describeFailure(error, source))
                }
            }
        }
    }

    /**
     * A download fails because of the host far more often than because of the
     * region: the index server is stopped, the device is on another network,
     * or the configured address has gone stale. None of that is recoverable
     * from "Failed to fetch", which is all a blocked request inside the WebView
     * reports, so name the address that was actually tried.
     */
    private fun describeFailure(error: Throwable, source: String): String {
        val detail = error.message?.takeIf { it.isNotBlank() }
        val opaque = detail == null ||
            error is java.io.IOException ||
            detail.contains("Failed to fetch", ignoreCase = true) ||
            detail.contains("cleartext", ignoreCase = true)
        if (!opaque) return detail
        val host = runCatching {
            java.net.URL(source).let { "${it.protocol}://${it.authority}" }
        }.getOrNull() ?: return context.getString(R.string.region_download_failed)
        return context.getString(R.string.region_host_unreachable, host)
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
        // From here on the choice is theirs, including the choice of network.
        regionPrefs.hasChosenSource = true
        refreshRegions()
        viewModelScope.launch {
            // Adopting a region adopts its area, not its mode: the user picked
            // a place to keep offline, and the mode they are travelling in is
            // a separate choice they already made.
            applyRouteBase(if (id != null) routeBaseFor(_state.value.mode) else routeBase)
            _state.update { it.copy(modesAvailable = modesAvailable()) }
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
            .onFailure { error ->
                // Failing silently left the previous index's coverage in place
                // while the app claimed to be on the new one, so a destination
                // well inside the new map could be refused as outside it. A
                // switch that did not happen has to say so, and must not leave
                // a boundary behind that no longer describes anything.
                _state.update {
                    it.copy(
                        info = it.info?.copy(
                            routing = false,
                            routingError = error.message,
                            routeBounds = null
                        ),
                        routes = null,
                        routeError = error.message
                            ?: context.getString(R.string.route_unavailable)
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
        // A proposed itinerary belongs to the place that was searched for.
        // Clearing the search takes that place away, so the routes drawn for
        // it have to go with it — along with the request still in flight,
        // which would otherwise land afterwards and put them back.
        routeJob?.cancel()
        queryFlow.value = ""
        _state.update {
            it.copy(
                query = "",
                suggestions = emptyList(),
                results = emptyList(),
                selected = null,
                searchError = null,
                routes = null,
                routeError = null,
                activeRouteIndex = 0,
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
        heading.start()
        rerouteLimiter.reset()
        travel.reset()
        val greeting = core.start(_state.value.allRoutes, _state.value.activeRouteIndex)
        if (regionPrefs.recordTrips) {
            recorder.start(_state.value.activeRoute, System.currentTimeMillis(), environment())
        }
        _state.update { it.copy(sheet = SheetMode.Navigating, issueMarks = 0) }
        greeting?.let { _voice.tryEmit(it) }
    }

    /**
     * Files how the map actually performed, before the trace is closed. A
     * drive that felt choppy and one that did not now differ by a number
     * rather than by a recollection.
     */
    private fun recordRenderCadence() {
        val summary = RenderCadence.snapshot(SystemClock.elapsedRealtimeNanos()) ?: return
        recorder.renderSummary(summary, System.currentTimeMillis())
    }

    fun stopNavigation() {
        heading.stop()
        motion.reset()
        recorder.note("stopped-by-user", atMillis = System.currentTimeMillis())
        recordRenderCadence()
        recorder.stop()
        core.stop()
        _state.update { it.copy(sheet = SheetMode.Directions, nav = null, rerouting = false) }
    }

    // ---- diagnostics ---------------------------------------------------

    val recordTrips: Boolean get() = regionPrefs.recordTrips

    fun setRecordTrips(enabled: Boolean) {
        regionPrefs.recordTrips = enabled
        if (!enabled) recorder.stop()
        _state.update { it.copy(recordTrips = enabled) }
    }

    /**
     * Flag the current moment as wrong. Deliberately does nothing but write:
     * the driver is driving, and anything that needs a decision from them
     * here is a worse idea than a single tap.
     */
    fun markIssue(): java.io.File? {
        if (!recorder.isRecording) return null
        val ordinal = _state.value.issueMarks + 1
        val shot = recorder.mark(ordinal, _state.value.nav, System.currentTimeMillis())
        _state.update { it.copy(issueMarks = ordinal) }
        return shot
    }

    /**
     * What produced this trace: build, device, index and routing source. A
     * report that cannot say which version and which index it came from can
     * only be read as a guess about a build that may no longer exist.
     */
    private fun environment(): org.json.JSONObject {
        val active = regionPrefs.activeRegion
        return org.json.JSONObject()
            .put("app", BuildConfig.VERSION_NAME)
            .put("versionCode", BuildConfig.VERSION_CODE)
            .put("buildType", BuildConfig.BUILD_TYPE)
            .put("flavor", BuildConfig.FLAVOR)
            .put("device", "${android.os.Build.MANUFACTURER} ${android.os.Build.MODEL}")
            .put("android", android.os.Build.VERSION.RELEASE)
            .put("sdk", android.os.Build.VERSION.SDK_INT)
            .put("locale", java.util.Locale.getDefault().toLanguageTag())
            .put("routeSource", active ?: "network")
            .put("routeBytes", active?.let { regionStore.bytesOf(it) } ?: 0L)
            .put("regionHost", regionPrefs.host)
            .put("searchBase", searchBase)
            .put("routingProfile", _state.value.info?.profile ?: org.json.JSONObject.NULL)
            .put("routingAvailable", _state.value.info?.routing ?: false)
            .put("routingError", _state.value.info?.routingError ?: org.json.JSONObject.NULL)
    }

    /** The newest trace and its screenshots, zipped for handing over. */
    fun latestBundle(): java.io.File? = recorder.traces().firstOrNull()?.let { recorder.bundle(it) }

    /** The most recent finished trace, for sharing off the device. */
    fun latestTrace(): java.io.File? = recorder.traces().firstOrNull()

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
        travel.onFix(point, System.currentTimeMillis())
        _state.update { it.copy(userLocation = point) }
        if (_state.value.sheet != SheetMode.Navigating) return

        motion.onFix(
            location = location,
            roadBearing = _state.value.nav?.roadBearing,
            compassBearing = heading.bearing,
            mode = _state.value.mode,
            nowMs = System.currentTimeMillis()
        )
        val fused = motion.pose(System.currentTimeMillis())
        val update = core.onLocation(
            point = fused?.position ?: point,
            speedMps = fused?.speedMps ?: if (location.hasSpeed()) location.speed.toDouble() else 0.0,
            gpsBearing = fused?.bearing,
            hasBearing = fused != null || location.hasBearing()
        )
        // Trace the fix whether or not the core made anything of it: a fix it
        // declined to use is exactly the kind of gap worth seeing afterwards.
        recorder.log(location, update, System.currentTimeMillis())
        if (update == null) return

        update.voice?.let { _voice.tryEmit(it) }
        _state.update { it.copy(nav = update) }

        if (update.arrived) {
            recorder.note("arrived", atMillis = System.currentTimeMillis())
            recordRenderCadence()
            recorder.stop()
            core.stop()
            // Stopping the state machine is not finishing the drive: without
            // this the phone stayed on the navigation sheet at the doorstep,
            // with no guidance left to give and no way out but Back. The car
            // surface had always ended its own trip here.
            _state.update {
                it.copy(
                    sheet = if (it.selected != null) SheetMode.Place else SheetMode.Search,
                    nav = null,
                    routes = null,
                    activeRouteIndex = 0,
                    rerouting = false
                )
            }
            return
        }
        if (update.offRoute) {
            // Reroute from where the car is pointing, not just where it is.
            reroute(point, travelHeading(location))
        } else {
            // Back on the line: whatever was last computed worked, so the next
            // bit of trouble starts from a clean slate rather than a backoff
            // inherited from trouble that is over.
            rerouteLimiter.onRoute()
        }
    }

    /**
     * The vehicle's own heading, or null when nothing knows it.
     *
     * Taken from the fix rather than NavUpdate.bearing on purpose: that one
     * falls back to the matched road's direction so the arrow stays steady,
     * and on an off-route event the matched road is the one the driver has
     * just left — precisely the direction a reroute must not assume.
     *
     * Below the speed where a satellite course means anything, the answer used
     * to be null, and a router given no heading will happily start a route by
     * turning the car around. There is usually a reason a driver is not on the
     * route, and being told to go back the way they came is rarely it. So the
     * fallbacks: the course the vehicle has actually traced over the last few
     * seconds, then the phone's own idea of which way it points.
     */
    private fun travelHeading(location: Location): Double? = when {
        location.hasBearing() && location.speed >= NavigationCore.MIN_HEADING_SPEED_MPS ->
            location.bearing.toDouble()
        else -> travel.fromMovement() ?: heading.bearing
    }

    private fun reroute(from: LatLon, heading: Double?) {
        val destination = _state.value.selected ?: return
        if (_state.value.rerouting) return
        val now = System.currentTimeMillis()
        // Asking again on a three-second heartbeat is not persistence, it is a
        // loop: adopting a route clears the strike count, so a car the map
        // cannot place asks forever. Each attempt that does not get it back on
        // the line waits longer than the last.
        if (!rerouteLimiter.shouldReroute(now)) return
        _state.update { it.copy(rerouting = true) }
        recorder.note("reroute", detail = "heading=${heading ?: "none"}", atMillis = now)
        if (rerouteLimiter.shouldAnnounce(now)) _voice.tryEmit(context.getString(R.string.nav_rerouting))
        viewModelScope.launch {
            runCatching { engine.route(from, destination.point, alternatives = 0, fromHeading = heading) }
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
