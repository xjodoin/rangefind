package dev.rangefind.wayfind.ui

import android.content.Context
import android.location.Location
import android.os.SystemClock
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import dev.rangefind.wayfind.BuildConfig
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.engine.ActiveJob
import dev.rangefind.wayfind.engine.DeviceCard
import dev.rangefind.wayfind.engine.DeviceCardArtifact
import dev.rangefind.wayfind.engine.DeviceIdentity
import dev.rangefind.wayfind.engine.EngineInfo
import dev.rangefind.wayfind.engine.SealError
import dev.rangefind.wayfind.engine.JobFinalRecord
import dev.rangefind.wayfind.engine.JobStop
import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.engine.Place
import dev.rangefind.wayfind.engine.FollowedDrive
import dev.rangefind.wayfind.engine.FollowedEta
import dev.rangefind.wayfind.engine.JobOffer
import dev.rangefind.wayfind.engine.MeshIncident
import dev.rangefind.wayfind.engine.OfferSummary
import dev.rangefind.wayfind.engine.PulseMeshStatus
import dev.rangefind.wayfind.engine.QrMatrix
import dev.rangefind.wayfind.engine.RangefindEngine
import dev.rangefind.wayfind.engine.StopOutcome
import dev.rangefind.wayfind.engine.StopReason
import dev.rangefind.wayfind.engine.TrafficSegment
import dev.rangefind.wayfind.engine.Route
import dev.rangefind.wayfind.engine.RouteBundle
import dev.rangefind.wayfind.engine.Suggestion
import dev.rangefind.wayfind.engine.finalRecordFor
import dev.rangefind.wayfind.engine.shareStopsFor
import dev.rangefind.wayfind.engine.truncateNote
import dev.rangefind.wayfind.engine.unresolvedStops
import dev.rangefind.wayfind.location.HeadingSensor
import dev.rangefind.wayfind.location.LocationProvider
import dev.rangefind.wayfind.nav.MotionModel
import dev.rangefind.wayfind.region.AwardBid
import dev.rangefind.wayfind.region.AwardBidVerdict
import dev.rangefind.wayfind.region.EnrolledDevice
import dev.rangefind.wayfind.region.HeldOffer
import dev.rangefind.wayfind.region.awardVerdictFor
import dev.rangefind.wayfind.region.heldOfferOf
import dev.rangefind.wayfind.region.withOffer
import dev.rangefind.wayfind.region.withoutOffer
import dev.rangefind.wayfind.region.REGION_CATALOG
import dev.rangefind.wayfind.region.RegionEntry
import dev.rangefind.wayfind.region.cappedDeviceName
import dev.rangefind.wayfind.region.withDevice
import dev.rangefind.wayfind.region.withoutDevice
import dev.rangefind.wayfind.region.RegionPreferences
import dev.rangefind.wayfind.region.RegionServer
import dev.rangefind.wayfind.region.RegionStatus
import dev.rangefind.wayfind.region.RegionStore
import dev.rangefind.wayfind.region.ActiveTrip
import dev.rangefind.wayfind.nav.NavUpdate
import dev.rangefind.wayfind.nav.TravelMode
import dev.rangefind.wayfind.ui.map.RenderCadence
import dev.rangefind.wayfind.nav.TripRecorder
import dev.rangefind.wayfind.nav.NavigationCore
import dev.rangefind.wayfind.nav.NavigationService
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

/**
 * A ticket ready to leave the device: the file to attach, and the same
 * capability as text.
 *
 * Both, because the receiving end is not one thing — a chat app forwards
 * the text and drops the attachment, a mail client does the reverse — and
 * a handover that works only through the channel the dispatcher happens
 * not to use is not a handover.
 */
data class TicketHandover(val file: java.io.File, val link: String)

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
    /**
     * Whether a pan has left the drive's camera off the vehicle.
     *
     * A driver who drags the map to see what is up ahead has stopped
     * following, and nothing puts them back: the road keeps moving under a
     * map that stays where it was left. This is what raises the way back, so
     * the control appears only when there is something to return from.
     */
    val cameraDetached: Boolean = false,
    val regions: List<RegionEntry> = emptyList(),
    val regionHost: String = "",
    val showRegions: Boolean = false,
    val recordTrips: Boolean = false,
    /** How many faults the driver has flagged on this drive. */
    val issueMarks: Int = 0,
    /** Driving, cycling or walking. */
    val mode: TravelMode = TravelMode.Car,
    /** Modes this area actually has an index for. */
    val modesAvailable: Set<TravelMode> = setOf(TravelMode.Car),
    /**
     * Whether this phone publishes anonymous speed observations. Off by
     * default: reading live traffic costs the user nothing, contributing
     * publishes where they drove, and that is a choice they make rather
     * than one an app makes for them.
     */
    val contributingTraffic: Boolean = false,
    val pulseMesh: PulseMeshStatus? = null,
    /** What the mesh currently believes about the roads, ready to draw. */
    val traffic: List<TrafficSegment> = emptyList(),
    /** Scored incidents with positions. Hints are drawn as hints. */
    val incidents: List<MeshIncident> = emptyList(),
    /** A run this phone is following from a shared link. */
    val following: FollowedDrive? = null,
    /** When it reaches this device's destination, computed locally (§9). */
    val followingEta: FollowedEta? = null,
    /** §11: whether a shared link carries a live position or stops only. */
    val shareFine: Boolean = true,
    /**
     * A dispatched job this device has been handed but not yet taken
     * (threads §20). A ticket is a publish capability, so it is shown and
     * accepted rather than acted on the moment it arrives.
     */
    val jobOffer: JobOffer? = null,
    /** The ticket the offer came from, kept verbatim for accepting it. */
    val jobLink: String? = null,
    /**
     * How the awarded job on screen stands against the offers this
     * device bid on (§20.4), or null when there is nothing to compare.
     */
    val awardBid: AwardBid? = null,
    /**
     * A dispatcher's broadcast offer, waiting on a *bid* decision
     * (§20.4). A different artifact from [jobOffer] and a different
     * question: it grants nothing, names no address and may come to
     * nothing, and what it asks is whether to put this courier forward.
     */
    val offerOnScreen: OfferSummary? = null,
    /**
     * The offers this device has bid on, live ones only. Held because
     * the award arrives hours later and the commitment is what lets this
     * phone tell the advertised job from a swapped one.
     */
    val heldOffers: List<HeldOffer> = emptyList(),
    /**
     * The dispatch ticket this device is working through, and which stop
     * of it the run is on. Null when the drive is an ordinary one — a
     * single destination that ends when it is reached.
     */
    val activeJob: ActiveJob? = null,
    /**
     * The van has reached the stop the run is on and nobody has said what
     * happened there yet. Arriving is not delivering — nothing on this
     * device can tell a parcel handed over from a doorstep driven past —
     * so this raises the marking buttons and waits for the driver.
     */
    val arrivedAtJobStop: Boolean = false,
    /** The active ticket drawn for the next driver's camera (§20.5). */
    val handoverQr: QrMatrix? = null,
    /**
     * This device's own identity (threads §20.9) — the address a job can
     * be sealed to. Public parts only: the private key lives in the
     * engine and in preferences and never reaches a screen.
     */
    val deviceIdentity: DeviceIdentity? = null,
    /** Whether the stored key is wrapped by a Keystore key rather than plain. */
    val deviceKeyProtected: Boolean = false,
    /**
     * The devices this phone may hand a job to. Enrolment is the gate on
     * transfer: with this list empty there is nobody to seal a ticket to,
     * and the hand-over action says exactly that instead of drawing a QR
     * nobody can open.
     */
    val enrolledDevices: List<EnrolledDevice> = emptyList(),
    /** This phone's card, on screen because the driver asked for it. */
    val myDeviceCard: DeviceCardArtifact? = null,
    /**
     * Another phone's card, waiting on the driver to compare the
     * fingerprint against what that phone is showing. §20.9: a card is
     * bytes on a screen, and a screen in a depot can be showing anybody's.
     */
    val deviceOffer: DeviceCard? = null,
    /** Choosing which enrolled device this job is about to be sealed to. */
    val handoverPicking: Boolean = false,
    /** Whose device the ticket on screen was sealed to. Only they can open it. */
    val handoverTarget: EnrolledDevice? = null,
    /** The last thing the mesh declined, and why — shown once, then cleared. */
    val meshNotice: String? = null
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

    /**
     * The job sealed to the device the driver picked, held for as long as
     * the handover dialog is up.
     *
     * Not in [UiState] because nothing draws it: the QR is drawn from a
     * matrix and the file is written on demand. It is ciphertext either
     * way — the point of §20.9 — but the fewer places a publish capability
     * sits, the fewer there are to leak it.
     */
    private var handoverTicket: String? = null

    private val downloads = mutableMapOf<String, Job>()

    init {
        installCrashRecorder()
        refreshRegions()
        viewModelScope.launch {
            // A region kept on the device wins over the network base: the user
            // downloaded it precisely so routing would not depend on a server.
            val active = resolveActiveRegion()
            val mode = regionPrefs.travelMode
            core.mode = mode
            _state.update {
                it.copy(mode = mode, modesAvailable = modesAvailable(), shareFine = regionPrefs.shareFine)
            }
            // The remembered mode decides the index, region or not. Opening
            // the driving graph while the selector says Walk drew a driving
            // route under a walking label, which is worse than either.
            val base = routeBaseFor(mode)
            // Adopting a region changes which row reads as active.
            if (active != null) refreshRegions()
            runCatching { engine.init(searchBase, base) }
                .onSuccess { info ->
                    // Before `info` is published, and deliberately: the
                    // activity acts on whatever link opened the app the
                    // moment it sees `info`, and a sealed ticket arriving
                    // ahead of this device's own key would read as "sealed
                    // for another device" when it is sealed for this one.
                    loadDeviceIdentity()
                    // Before `info` too, and for the same reason: an
                    // award arriving on the link that opened the app has
                    // to be checked against the bids this device is
                    // holding, and a bid restored afterwards is a
                    // commitment that was not there when it mattered.
                    restoreHeldOffers()
                    _state.update { it.copy(loading = false, info = info) }
                    // A dispatch ticket wins over the single-destination
                    // resume, and the two must not both run: the trip file
                    // holds whichever stop the app happened to die on and
                    // would re-plan and *start* guidance to it, while the
                    // job knows which stop the run is actually up to. Two
                    // destinations, one screen, and the wrong one moving
                    // first is a driver sent back to a doorstep they have
                    // already been to.
                    if (!restoreActiveJob()) resumeInterruptedTrip()
                    // A driver who turned live traffic on meant it for their
                    // driving, not for one session. It comes back only after
                    // the index is open, because the mesh is bound to that
                    // exact build of it.
                    if (regionPrefs.liveTraffic && info.routing) setLiveTraffic(true)
                }
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
                // The copy on disk is the one the server just handed over, so
                // it is current by construction. [refreshRegions] carries the
                // previous staleness forward rather than recomputing it — a
                // network round trip has no business running on every state
                // change — which left a refreshed region still saying "a newer
                // index is published", with the freshly downloaded index
                // sitting under the message. The user's only move was to press
                // refresh again, which said the same thing.
                updateRegion(id) { it.copy(stale = false) }
                // And ask the server properly, in case it published something
                // new while this download was running. Cheap, and the answer
                // arrives while the sheet is still open.
                checkForRegionUpdates()
                // Re-point routing at whatever is now the best answer.
                //
                // This used to fire only when the finished region was the
                // active one, which meant a download made while "use the
                // network index" is selected changed nothing: routing kept the
                // base it started with. On a phone that base is the build's
                // own default — the emulator's loopback alias — so a driver
                // who had just waited for a 78 MB download still got "no
                // answer from the server" for every destination, with the
                // index they were waiting for sitting on the device unused.
                applyRouteBase(routeBaseFor(_state.value.mode))
            }.onFailure { error ->
                downloads.remove(id)
                // Whatever was already installed stays installed. `install`
                // downloads into a staging directory and swaps it in only once
                // every file has landed, precisely so a failed refresh cannot
                // cost the user the copy they had — and then deleting it here
                // threw that away anyway. A refresh that fails over a flaky
                // connection must not leave a driver with no offline region at
                // all, which is the state they are least able to recover from.
                updateRegion(id) {
                    it.copy(status = RegionStatus.Failed, error = describeFailure(error, source))
                }
                // The row's status is recomputed from what is on disk, so a
                // region that survived reports itself ready again.
                refreshRegions()
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
                // Coordinates are written the way coordinates are written,
                // not the way the device writes numbers: in French the
                // default locale renders this "45,64015, -73,79837", where
                // the same character separates the decimals and the pair.
                address = String.format(
                    java.util.Locale.ROOT, "%.5f, %.5f", point.lat, point.lon
                ),
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
        // Guidance holds location while the app is out of sight, and the
        // platform only allows that from a foreground service. Without one a
        // phone call quietly starved navigation of fixes and could take the
        // process with it, which is why a drive never came back afterwards.
        NavigationService.start(context, _state.value.selected?.name.orEmpty())
        // And on disk, in case the process goes anyway. The service makes
        // that unlikely, not impossible, and a driver who loses the app
        // mid-motorway should get their destination back rather than a
        // search box.
        _state.value.selected?.let { place ->
            regionPrefs.activeTrip = ActiveTrip(
                lat = place.lat,
                lon = place.lon,
                name = place.name,
                address = place.address,
                startedAtMillis = System.currentTimeMillis()
            )
        }
        _state.update { it.copy(sheet = SheetMode.Navigating, issueMarks = 0, cameraDetached = false) }
        greeting?.let { _voice.tryEmit(it) }
    }

    /**
     * Picks a drive back up after the app was taken away mid-trip.
     *
     * A phone call is the ordinary case: the activity goes, and with it the
     * view model and every trace of where the driver was going. What comes
     * back is a fresh app on a search screen, which is not a recovery — the
     * driver is still on the motorway and now has to type.
     *
     * Resuming re-plans from the car's position now rather than restoring
     * the old line: after several minutes on a call, the saved route starts
     * somewhere the car left long ago, and adopting it would open the drive
     * already off-route. A trip older than a few hours is not an interrupted
     * drive, it is yesterday's, and is simply discarded.
     */
    private suspend fun resumeInterruptedTrip() {
        val trip = regionPrefs.activeTrip ?: return
        val age = System.currentTimeMillis() - trip.startedAtMillis
        if (age !in 0..RESUME_WINDOW_MILLIS) {
            regionPrefs.activeTrip = null
            return
        }
        val here = _state.value.userLocation ?: locationProvider.lastKnown()
            ?.let { LatLon(it.latitude, it.longitude) }
        val destination = Place(
            id = "",
            name = trip.name,
            address = trip.address,
            locality = "",
            category = "",
            type = "",
            lat = trip.lat,
            lon = trip.lon,
            distanceMeters = null
        )
        // With no fix yet there is nothing to route from. Put the driver back
        // on the destination rather than on a blank search: one tap resumes,
        // and nothing has been silently invented on their behalf.
        if (here == null) {
            _state.update { it.copy(selected = destination, sheet = SheetMode.Place) }
            return
        }
        val bundle = runCatching {
            engine.route(here, destination.point, alternatives = 0)
        }.getOrNull()
        if (bundle == null) {
            _state.update { it.copy(selected = destination, sheet = SheetMode.Place) }
            return
        }
        _state.update {
            it.copy(selected = destination, routes = bundle, activeRouteIndex = 0)
        }
        startNavigation()
    }

    /**
     * Puts the stack of a fatal crash into the drive's own trace.
     *
     * "The app closes when I arrive at the destination" is a report about a
     * moment, and a moment is not something that can be fixed. The trace is
     * already what a driver sends, and it already knows exactly where the
     * drive was, so the crash belongs in it: the next such report arrives
     * with the exception that caused it instead of a recollection of when.
     *
     * The previous handler is always called. Replacing it rather than
     * chaining would swallow whatever reporting the platform does, and the
     * one thing worse than a crash with no stack is a crash with no crash.
     */
    private fun installCrashRecorder() {
        val previous = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { thread, error ->
            runCatching { recorder.noteCrash(error, System.currentTimeMillis()) }
            previous?.uncaughtException(thread, error)
        }
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
        rerouteLimiter.reset()
        NavigationService.stop(context)
        regionPrefs.activeTrip = null
        recorder.note("stopped-by-user", atMillis = System.currentTimeMillis())
        recordRenderCadence()
        recorder.stop()
        core.stop()
        _state.update {
            it.copy(sheet = SheetMode.Directions, nav = null, rerouting = false, cameraDetached = false)
        }
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
        _state.update { it.copy(recenterTick = it.recenterTick + 1, cameraDetached = false) }
    }

    /** The map was panned away from the vehicle; offer the way back. */
    fun cameraDetached() {
        _state.update { if (it.cameraDetached) it else it.copy(cameraDetached = true) }
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

    // ---- live traffic (PulseMesh) ---------------------------------------

    private var meshJob: Job? = null

    /**
     * Feeds one fix to the live-traffic mesh.
     *
     * Offered on every fix, not only when contributing: the protocol
     * decides whether anything is published, and even with contribution
     * off the fix keeps this device's snapped position fresh — which is
     * what makes an incident report possible at all (§10.4 files a report
     * only for a recently matched position). Failures are swallowed: live
     * traffic is best-effort and the router works without it.
     */
    private fun offerToPulseMesh(location: Location, point: LatLon) {
        if (_state.value.pulseMesh?.running != true) return
        viewModelScope.launch {
            val battery = batteryState()
            val status = engine.offerLocation(
                point = point,
                speedMps = if (location.hasSpeed()) location.speed.toDouble() else null,
                courseDeg = if (location.hasBearing()) location.bearing.toDouble() else null,
                batteryLevel = battery?.first,
                charging = battery?.second
            )
            _state.update { it.copy(pulseMesh = status) }
        }
    }

    /**
     * §10.1 rule 5's inputs. Read per fix rather than held: a drive is long
     * enough for a phone to be plugged in halfway through, and a cached
     * "empty battery" would keep contribution off for the rest of it.
     */
    private fun batteryState(): Pair<Double, Boolean>? {
        val manager = context.getSystemService(android.content.Context.BATTERY_SERVICE)
            as? android.os.BatteryManager ?: return null
        val level = manager.getIntProperty(android.os.BatteryManager.BATTERY_PROPERTY_CAPACITY)
        if (level < 0) return null
        val status = manager.getIntProperty(android.os.BatteryManager.BATTERY_PROPERTY_STATUS)
        val charging = status == android.os.BatteryManager.BATTERY_STATUS_CHARGING ||
            status == android.os.BatteryManager.BATTERY_STATUS_FULL
        return level / 100.0 to charging
    }

    /**
     * Turns live traffic on or off. With no keeper configured the mesh
     * comes up on the on-device demo transport — real records, real
     * validation, simulated drivers — and the UI says which it is rather
     * than implying this phone has joined anything.
     */
    fun setLiveTraffic(enabled: Boolean) {
        regionPrefs.liveTraffic = enabled
        viewModelScope.launch {
            val status = if (enabled) bringMeshUp() else engine.setMeshRunning(false)
            _state.update {
                it.copy(
                    pulseMesh = status,
                    contributingTraffic = status.contributing,
                    traffic = if (status.running) it.traffic else emptyList(),
                    incidents = if (status.running) it.incidents else emptyList()
                )
            }
            if (status.running) startMeshPolling() else stopMeshPolling()
        }
    }

    /**
     * Brings the mesh up carrying the driver's standing decision about
     * invented traffic. Every path that raises the mesh goes through here:
     * a driver who switched the demo vehicles off must not get them back
     * because the app happened to start the mesh for a followed link or an
     * accepted job rather than from the switch.
     */
    private suspend fun bringMeshUp(): PulseMeshStatus =
        engine.setMeshRunning(true, simulated = regionPrefs.simulatedTraffic)

    /**
     * The demo vehicles, on or off, with live traffic left running.
     *
     * They are the only invented thing on this map, and a driver is
     * entitled to a map with nothing invented on it — which on a keeperless
     * phone means a traffic layer that stays empty until real Wayfind
     * drivers are nearby. Records the vehicles already published stay in
     * the store: they were validated on arrival, and the source going away
     * is not grounds for unlearning what it said.
     */
    fun setSimulatedTraffic(enabled: Boolean) {
        regionPrefs.simulatedTraffic = enabled
        viewModelScope.launch {
            val status = engine.setSimulatedTraffic(enabled)
            // What the vehicles published is still true, but nothing renews
            // it now, so the layer is re-read from the mesh rather than left
            // as it was last drawn.
            val traffic = engine.meshTraffic()
            _state.update { it.copy(pulseMesh = status, traffic = traffic) }
        }
    }

    /** Turns contribution on or off. Off is the default and the safe state. */
    fun setContributingTraffic(enabled: Boolean) {
        viewModelScope.launch {
            val status = engine.setContributing(enabled)
            _state.update { it.copy(contributingTraffic = status.contributing, pulseMesh = status) }
        }
    }

    private fun startMeshPolling() {
        if (meshJob != null) return
        meshJob = viewModelScope.launch {
            while (true) {
                // The mesh's clock lives here, not in the page: see
                // RangefindEngine.tickMesh.
                engine.tickMesh()
                val status = engine.pulseMeshStatus()
                val traffic = engine.meshTraffic()
                val incidents = engine.meshIncidents()
                val following = engine.followedDrive()
                // Where *this* device is going is what a follower actually
                // wants the arrival for: the vehicle reaching the shared
                // destination, computed here rather than by the publisher.
                val destination = _state.value.selected?.point ?: _state.value.userLocation
                val eta = if (following?.live == true && destination != null) {
                    engine.followedEta(destination)
                } else {
                    null
                }
                _state.update {
                    it.copy(
                        pulseMesh = status,
                        contributingTraffic = status.contributing,
                        traffic = traffic,
                        incidents = incidents,
                        following = following,
                        followingEta = eta
                    )
                }
                kotlinx.coroutines.delay(MESH_POLL_MILLIS)
            }
        }
    }

    private fun stopMeshPolling() {
        meshJob?.cancel()
        meshJob = null
    }

    /**
     * Files an incident report. [acknowledgedPublic] is the app asserting
     * it has told the user, at the point of reporting, that a report is
     * public and locates them — the protocol will not mint one otherwise,
     * so the requirement cannot be satisfied by a settings page nobody read.
     */
    fun reportIncident(type: Int, acknowledgedPublic: Boolean) {
        viewModelScope.launch {
            val result = engine.reportIncident(type, acknowledgedPublic)
            _state.update {
                it.copy(
                    meshNotice = if (result.emitted) {
                        context.getString(R.string.mesh_report_sent)
                    } else {
                        context.getString(R.string.mesh_report_declined, meshReason(result.reason))
                    },
                    incidents = engine.meshIncidents()
                )
            }
        }
    }

    /** Confirms or refutes an incident this driver is passing. */
    fun answerIncident(key: String, stillThere: Boolean) {
        viewModelScope.launch {
            val result = engine.answerIncident(key, if (stillThere) 2 else 3, true)
            _state.update {
                it.copy(
                    meshNotice = if (result.emitted) {
                        context.getString(R.string.mesh_answer_sent)
                    } else {
                        context.getString(R.string.mesh_report_declined, meshReason(result.reason))
                    }
                )
            }
        }
    }

    /** The protocol's own refusal, in words a driver can act on. */
    private fun meshReason(reason: String?): String = when (reason) {
        "no-position" -> context.getString(R.string.mesh_reason_no_position)
        "rate" -> context.getString(R.string.mesh_reason_rate)
        "already-answered" -> context.getString(R.string.mesh_reason_answered)
        "suppressed-by-policy" -> context.getString(R.string.mesh_reason_policy)
        "no-mesh" -> context.getString(R.string.mesh_reason_no_mesh)
        "no-run" -> context.getString(R.string.mesh_reason_no_run)
        // §20.7's refusals. "Coarse" should be unreachable — the camera is
        // not offered on a coarse run — but a stale card on screen is not a
        // reason to show the driver a sentence about a Uint8Array.
        "photo-too-big" -> context.getString(R.string.mesh_reason_photo_too_big)
        "photo-coarse" -> context.getString(R.string.mesh_reason_photo_coarse)
        "photo-empty", "photo-unreadable" ->
            context.getString(R.string.mesh_reason_photo_unreadable)
        else -> reason ?: ""
    }

    /**
     * Starts publishing this drive and returns the link to share.
     *
     * A dispatch run publishes the whole day, not the doorstep on screen:
     * a plan of one stop tells every customer holding the link nothing
     * about the eleven addresses in front of theirs, and the ETA they are
     * shown is computed from the plan. Positions are the plan's own — see
     * [shareStopsFor] for why re-basing them would mis-attribute marks.
     */
    fun shareDrive(onLink: (String?) -> Unit) {
        viewModelScope.launch {
            val job = _state.value.activeJob
            val url = engine.shareDrive(
                stops = shareStopsFor(job, _state.value.selected?.point),
                baseUrl = SHARE_BASE_URL,
                // §11 stays with whoever decided it. On a dispatch run that
                // is the ticket's issuer, and re-publishing at fine because
                // this phone's own switch says fine would hand out a
                // position the operator deliberately withheld.
                fine = job?.fine ?: _state.value.shareFine,
                // The mode the driver is actually travelling in, so a
                // follower's arrival is computed on the matching graph
                // rather than on whatever this device happens to hold.
                travelMode = _state.value.mode.profile
            )
            _state.update { it.copy(pulseMesh = engine.pulseMeshStatus()) }
            onLink(url)
        }
    }

    /**
     * The tracking link for the run in progress, published first if it is
     * not already.
     *
     * Sharing has to work from the cab, mid-day: the link is in hand
     * whenever this device is publishing — a run taken from a dispatcher's
     * ticket included — and handing it on is one tap from the progress card.
     *
     * When the app holds a plan but no run, which is what a process death
     * leaves behind, the run is published again from the stored plan rather
     * than the button being greyed out with an explanation: the driver asked
     * for a link, and there is nothing they could do about a disabled
     * control anyway. The republished run is a new thread with a new seed,
     * so it is a fallback and not the ordinary path — anyone still holding
     * the dead link is watching a vehicle that has stopped moving.
     */
    fun shareJobLink(onLink: (String?) -> Unit) {
        _state.value.pulseMesh?.sharing?.url?.takeIf { it.isNotEmpty() }?.let { url ->
            onLink(url)
            return
        }
        viewModelScope.launch {
            // Publishing needs a channel, and a job card can outlive the
            // mesh that was up when the ticket was taken.
            if (_state.value.pulseMesh?.running != true) {
                val status = bringMeshUp()
                _state.update {
                    it.copy(pulseMesh = status, contributingTraffic = status.contributing)
                }
                if (status.running) startMeshPolling()
            }
            shareDrive { url ->
                if (url.isNullOrEmpty()) {
                    _state.update {
                        it.copy(meshNotice = context.getString(R.string.mesh_share_unavailable))
                    }
                } else {
                    onLink(url)
                }
            }
        }
    }

    fun followDrive(link: String) {
        viewModelScope.launch { follow(link) }
    }

    /**
     * Follows a drive somebody shared, bringing live traffic up first if
     * it is off.
     *
     * Being handed a link and then being told to go and enable something
     * is not a feature, it is a chore — and it costs the viewer nothing
     * to skip: threads carry no admission bond, are authenticated end to
     * end by the thread key, and publish nothing on the follower's
     * behalf. The ETA is then computed here, from the position they
     * broadcast, under this device's own traffic — so the publisher
     * never learns which destination anyone asked about.
     */
    fun followSharedLink(link: String) {
        viewModelScope.launch {
            if (_state.value.pulseMesh?.running != true) {
                val status = bringMeshUp()
                _state.update { it.copy(pulseMesh = status, contributingTraffic = status.contributing) }
                if (status.running) startMeshPolling()
            }
            follow(link)
        }
    }

    /**
     * A capability arrived — from a deep link, which is also what the
     * phone's own camera app fires when the driver scans a dispatcher's
     * QR. What happens next depends on what it *is*, never on how it came
     * in: a follow link is read-only and is acted on immediately, while a
     * dispatch ticket lets the holder move a vehicle and is shown first.
     */
    fun openSharedLink(link: String) {
        viewModelScope.launch {
            val artifact = engine.classifyMeshLink(link)
            when {
                // Known to be a ticket and unreadable by this build. That
                // is a different sentence from "this is not a capability":
                // the holder has a real job in their hand and needs a
                // fresh one, not a lecture about link byte lengths.
                artifact.isUnreadableTicket -> _state.update {
                    it.copy(meshNotice = context.getString(R.string.mesh_job_ticket_unreadable))
                }
                // Another phone offering to be enrolled (§20.9). It is the
                // *prerequisite* for a handover rather than a handover, so
                // it opens a confirmation with the fingerprint on it and
                // nothing happens until the driver has compared it.
                artifact.isDeviceCard -> {
                    val card = artifact.device ?: engine.readDeviceCard(link)
                    if (card == null) {
                        _state.update {
                            it.copy(meshNotice = context.getString(R.string.device_card_unreadable))
                        }
                    } else {
                        _state.update { it.copy(deviceOffer = card) }
                    }
                }
                // A dispatcher's broadcast offer (§20.4). It grants
                // nothing and publishes nothing — it is public and
                // unsealed by design — so it opens its own sheet, where
                // the decision is whether to bid rather than whether to
                // drive. Nothing about it is acted on: there is no
                // vehicle to move and no address to route to.
                artifact.isOffer -> {
                    val described = engine.describeOffer(link)
                    val offer = described.offer
                    if (offer == null) {
                        _state.update {
                            it.copy(meshNotice = context.getString(R.string.offer_unreadable))
                        }
                    } else {
                        _state.update { it.copy(offerOnScreen = offer) }
                    }
                }
                artifact.isTicket -> {
                    val held = _state.value.heldOffers
                    // Every held bid travels down, not the one whose job
                    // id matches: a swapped plan moves the job id too, so
                    // a lookup by job id finds nothing and waves through
                    // exactly the swap the commitment exists to catch.
                    val described = engine.describeJob(link, held.map { it.offerBase64 })
                    val offer = described.offer
                    if (offer == null) {
                        _state.update {
                            it.copy(meshNotice = context.getString(sealNoticeFor(described.code)))
                        }
                    } else {
                        val bid = awardVerdictFor(held, offer.jobIdHex, described.offerChecks)
                        _state.update {
                            it.copy(jobOffer = offer, jobLink = link, awardBid = bid)
                        }
                    }
                }
                // Anything that is not a ticket is tried as a drive to
                // watch: following costs the viewer nothing, and a link
                // that turns out to be nothing says so with its own reason.
                else -> followSharedLink(link)
            }
        }
    }

    /**
     * Takes the dispatched job on screen.
     *
     * Publishing needs a channel, so live traffic comes up first rather
     * than sending the driver to a settings page — the same courtesy
     * following a link already gets. Once the run is publishing, the
     * job's final stop becomes the destination and directions are
     * requested: the driver is then one tap from navigating, which is the
     * only reason they scanned anything.
     */
    fun acceptJob() {
        val link = _state.value.jobLink ?: return
        val bid = _state.value.awardBid
        // The refusal is enforced here as well as drawn on the sheet: an
        // award that fails the commitment of a bid with the same job id
        // is two artifacts claiming to be one job, which nothing honest
        // produces — `jobId` hashes the plan, so a real change of plan
        // changes the id with it.
        if (bid?.refuses == true) {
            _state.update {
                it.copy(
                    meshNotice = context.getString(
                        R.string.offer_award_refused,
                        meshReason(bid.reason)
                    )
                )
            }
            return
        }
        viewModelScope.launch {
            if (_state.value.pulseMesh?.running != true) {
                val status = bringMeshUp()
                _state.update { it.copy(pulseMesh = status, contributingTraffic = status.contributing) }
                if (status.running) startMeshPolling()
            }
            // The ticket's own travel mode wins; this is the fallback for a
            // plan whose dispatcher left it unspecified.
            val accepted = engine.acceptJob(link, SHARE_BASE_URL, _state.value.mode.profile)
            if (accepted?.action?.emitted != true) {
                // A job sealed to somebody else is not a failure to retry
                // (§20.9): the fix is enrolment plus a fresh seal, and a
                // generic "could not take the job" would send the driver
                // to press the button again for ever.
                val code = accepted?.action?.code
                _state.update {
                    it.copy(
                        meshNotice = if (code != null) context.getString(sealNoticeFor(code))
                        else context.getString(
                            R.string.mesh_job_failed,
                            meshReason(accepted?.action?.reason)
                        )
                    )
                }
                return@launch
            }
            // A plan arrives in visit order, so the driver is navigated to
            // delivery 1, not to wherever the run happens to finish.
            val stop = accepted.job?.firstStop
            // The whole plan is adopted, not just its head: the run is a
            // day's work, and the app has to know both what the rest of it
            // is and how far through it the driver has got.
            val active = accepted.job?.takeIf { it.stops.isNotEmpty() }?.let { job ->
                ActiveJob(
                    stops = job.stops,
                    // Stop 1 is the one being driven to now.
                    nextIndex = 1,
                    ticketBase64 = accepted.ticketBase64,
                    jobIdHex = job.jobIdHex,
                    notAfter = job.notAfter,
                    // §11, as the ticket's issuer decided it. Carried on the
                    // run because it governs what the driver is offered: a
                    // coarse run withholds the vehicle's position, and a
                    // doorstep photo would hand one over past that decision.
                    fine = job.fine
                )
            }
            regionPrefs.activeJob = active
            // The bid this award settles is done with: the commitment has
            // been checked and the job is being driven, so holding it any
            // longer would only put a stale row in front of the courier
            // and check a second award against a job already taken.
            val settled = bid?.bid?.jobIdHex
                ?.takeIf { bid.verdict == AwardBidVerdict.MatchesBid && it.isNotBlank() }
            val remaining = if (settled == null) _state.value.heldOffers
            else regionPrefs.heldOffers.withoutOffer(settled).also { regionPrefs.heldOffers = it }
            _state.update {
                it.copy(
                    jobOffer = null,
                    jobLink = null,
                    awardBid = null,
                    heldOffers = remaining,
                    activeJob = active,
                    meshNotice = context.getString(R.string.mesh_job_accepted)
                )
            }
            _state.update { it.copy(pulseMesh = engine.pulseMeshStatus()) }
            if (stop != null) {
                val destination = placeOf(stop)
                _state.update { it.copy(selected = destination, sheet = SheetMode.Place) }
                requestDirections(destination)
            }
        }
    }

    fun dismissJobOffer() {
        _state.update { it.copy(jobOffer = null, jobLink = null, awardBid = null) }
    }

    /**
     * Bids on the offer on screen, which in this protocol means sending
     * this device's card (§20.4).
     *
     * There is no claim record and no bid message in protocol v1, and
     * this deliberately does not invent one: what leaves the phone is the
     * same PMV1 card enrolment needs, over whatever channel the two
     * parties already use. If the dispatcher picks this courier, they
     * seal the job to the key on that card — which is the entire loop.
     *
     * The offer is held at the same moment, because the card and the
     * commitment are two halves of one act: without the held offer the
     * award that arrives tomorrow has nothing to be checked against, and
     * "3 stops downtown" becomes thirty across the province with no way
     * to say so.
     *
     * A phone with no identity yet cannot be sealed to, so it does not
     * bid: nothing is held, and the driver is told what is missing rather
     * than left holding a bid on a job that could never reach them.
     */
    fun bidOnOffer(onCard: (String) -> Unit) {
        val offer = _state.value.offerOnScreen ?: return
        viewModelScope.launch {
            val card = engine.myDeviceCard()
            if (card == null) {
                _state.update {
                    it.copy(
                        offerOnScreen = null,
                        meshNotice = context.getString(R.string.device_card_unavailable)
                    )
                }
                return@launch
            }
            val held = regionPrefs.heldOffers
                .withOffer(heldOfferOf(offer, System.currentTimeMillis()))
            regionPrefs.heldOffers = held
            _state.update {
                it.copy(
                    offerOnScreen = null,
                    heldOffers = held,
                    deviceIdentity = card.identity,
                    meshNotice = context.getString(R.string.offer_bid_sent)
                )
            }
            onCard(card.url)
        }
    }

    fun dismissOffer() {
        _state.update { it.copy(offerOnScreen = null) }
    }

    /**
     * Drops a bid. The commitment goes with it: an award for this job
     * arriving afterwards is checked against nothing, and reads as the
     * unrecognised job it now is.
     */
    fun forgetHeldOffer(jobIdHex: String) {
        val held = regionPrefs.heldOffers.withoutOffer(jobIdHex)
        regionPrefs.heldOffers = held
        _state.update { it.copy(heldOffers = held) }
    }

    /**
     * The open bids, with the dead ones dropped and the pruning written
     * back.
     *
     * Pruning on load rather than on a timer: an expired offer is not a
     * bid still outstanding, it is a job that can no longer be awarded to
     * anybody. Writing the pruned list back is what stops a season of
     * bidding accumulating on disk.
     */
    private fun restoreHeldOffers() {
        val held = regionPrefs.heldOffers
        regionPrefs.heldOffers = held
        _state.update { it.copy(heldOffers = held) }
    }

    /**
     * Puts the day's plan back after the process died mid-run.
     *
     * Deliberately restores the plan and nothing else — no route, no
     * guidance. The driver may have been parked at a doorstep for twenty
     * minutes with an armful of parcels, and an app that opens already
     * navigating has decided something for them. The progress card offers
     * Continue, and that is one tap.
     *
     * Returns whether a run was picked up, because the caller then has to
     * leave the single-destination resume alone.
     */
    private fun restoreActiveJob(): Boolean {
        val job = regionPrefs.activeJob ?: return false
        // An expired ticket is not a run in progress, it is yesterday's
        // paperwork: the publish capability behind it is dead, so carrying
        // its stops forward would show a day that cannot be driven.
        if (job.notAfter > 0 && job.notAfter * 1000L <= System.currentTimeMillis()) {
            regionPrefs.activeJob = null
            return false
        }
        _state.update { it.copy(activeJob = job) }
        return true
    }

    /**
     * What happened at the stop the run is on, said by the driver.
     *
     * This is what moves a run along: arriving somewhere is not delivering,
     * and the two cannot be told apart from here. It also happens to be the
     * only thing that works when the van is *already* at the doorstep —
     * navigating to a stop you are standing on produces a zero-metre route
     * on which arrival never fires, and a run that could only advance on
     * arrival could not advance at all.
     *
     * The mesh is told first so the customer hears it now, but a channel
     * that is down does not veto the day: the outcome is recorded locally
     * and the failure is said out loud rather than swallowed.
     */
    fun markCurrentStop(
        outcome: Int,
        reason: Int = StopReason.NONE,
        note: String? = null,
        /**
         * Proof of delivery as base64 JPEG, already downscaled and already
         * stripped of EXIF by whoever captured it (see `DeliveryPhoto.kt`).
         * Null is the ordinary case: most doorsteps need no photograph.
         */
        photoBase64: String? = null
    ) {
        val job = _state.value.activeJob ?: return
        // The stop's position in the plan, which is what the wire's outcome
        // map is keyed by — not the label's own number, which a dispatcher
        // is free to write as anything.
        val position = job.nextIndex
        viewModelScope.launch {
            val result = engine.markStop(position, outcome, reason, note, photoBase64)
            val marked = job.withOutcome(position, outcome)
            regionPrefs.activeJob = marked
            _state.update { it.copy(activeJob = marked, arrivedAtJobStop = false) }
            advanceJob(
                job = marked,
                outcome = outcome,
                // A refused photo is not a footnote to a published mark: the
                // library seals the picture before it records the outcome or
                // emits anything, so the refusal took the whole mark with
                // it. The driver is told which of the two happened, because
                // only one of them can be fixed by marking again.
                notice = when {
                    result.emitted -> null
                    result.photoRefused -> context.getString(
                        R.string.mesh_job_mark_photo_refused, position, meshReason(result.reason)
                    )
                    else -> context.getString(
                        R.string.mesh_job_mark_unsent, position, meshReason(result.reason)
                    )
                }
            )
        }
    }

    /**
     * One stop is done with, so the next one begins.
     *
     * A ticket is a day rather than a destination: stop k is the middle of
     * the run, and what the driver needs next is the route to stop k+1
     * ready to start — not guidance that pulls away from a kerb they have
     * not got out of yet. So the route is asked for and the Start button
     * stays theirs to press.
     *
     * [notice] replaces the progress line when there is something more
     * urgent to say than which doorstep is next.
     */
    private fun advanceJob(
        job: ActiveJob,
        outcome: Int = StopOutcome.DELIVERED,
        notice: String? = null
    ) {
        val done = job.nextIndex
        val next = job.followingStop
        if (next == null) {
            // The last doorstep. The run ends here and so does the
            // publishing behind it: §20's final record is what tells every
            // customer still holding the link that nothing more is coming.
            regionPrefs.activeJob = null
            _state.update {
                it.copy(
                    activeJob = null,
                    arrivedAtJobStop = false,
                    meshNotice = notice ?: context.getString(
                        R.string.mesh_job_run_complete, job.total
                    )
                )
            }
            // Closed on the run as marked, which is what makes this
            // COMPLETED: every stop has an outcome the driver asserted.
            endRun(job, reason = null)
            return
        }
        val advanced = job.copy(nextIndex = done + 1)
        // On disk before it is on screen: the stop that has just been
        // marked is the one thing a crash here would make the driver
        // repeat.
        regionPrefs.activeJob = advanced
        val destination = placeOf(next)
        _state.update {
            it.copy(
                activeJob = advanced,
                arrivedAtJobStop = false,
                selected = destination,
                sheet = SheetMode.Place,
                meshNotice = notice ?: context.getString(
                    when (outcome) {
                        StopOutcome.SKIPPED -> R.string.mesh_job_stop_skipped
                        StopOutcome.FAILED -> R.string.mesh_job_stop_failed
                        else -> R.string.mesh_job_stop_delivered
                    },
                    done, job.total, labelOf(next)
                )
            )
        }
        requestDirections(destination)
    }

    /**
     * Asks again for the route to the stop the run is on.
     *
     * This is what a delivery day looks like after a restart, or after the
     * driver has been out of the vehicle for a while: the plan is known and
     * the route is not, because whatever was computed started from a
     * position the van left long ago.
     */
    fun continueJob() {
        val stop = _state.value.activeJob?.currentStop ?: return
        val destination = placeOf(stop)
        _state.update { it.copy(selected = destination, sheet = SheetMode.Place) }
        requestDirections(destination)
    }

    /** A dispatch stop as a destination the rest of the app understands. */
    private fun placeOf(stop: JobStop) = Place(
        id = "job",
        name = labelOf(stop),
        // Coordinates are written the way coordinates are written, not the
        // way the device writes numbers: in French the default locale
        // renders this "45,64015, -73,79837".
        address = String.format(java.util.Locale.ROOT, "%.5f, %.5f", stop.lat, stop.lon),
        locality = "",
        category = "",
        type = "",
        lat = stop.lat,
        lon = stop.lon,
        distanceMeters = null
    )

    private fun labelOf(stop: JobStop): String =
        stop.label.ifBlank { context.getString(R.string.mesh_job_stop_default) }

    /**
     * The active ticket written out as a file, plus the link inside it.
     *
     * A QR is the fast handover and the one that needs no network at all,
     * but a plan large enough to need one is a plan a camera cannot read.
     * The same bytes as a file go by mail, chat or cable. One line, so
     * whatever mangles it in transit is visible rather than silent — and
     * the link travels beside the file because chat apps take text where
     * mail clients take attachments.
     */
    fun ticketHandover(): TicketHandover? {
        val job = _state.value.activeJob ?: return null
        // The ticket **sealed to the device the driver picked**, never the
        // copy this phone holds: that one is addressed to this device and
        // the next driver could not open it. §20.9 — the file and the QR
        // carry the same bytes, and both are ciphertext.
        val ticket = handoverTicket?.takeIf { it.isNotBlank() } ?: return null
        val link = "$TICKET_LINK_PREFIX$ticket"
        val name = "job-${job.jobIdHex.take(12).ifBlank { "ticket" }}.wayfindjob"
        val file = runCatching {
            val dir = java.io.File(context.cacheDir, "tickets")
            dir.mkdirs()
            java.io.File(dir, name).apply { writeText(link + "\n") }
        }.getOrNull() ?: return null
        return TicketHandover(file = file, link = link)
    }

    /** Something arrived that is not a ticket at all. */
    fun noteUnreadableJob() {
        _state.update { it.copy(meshNotice = context.getString(R.string.mesh_job_unreadable)) }
    }

    /**
     * Starts a handover, which since §20.9 begins with a question: **to
     * which enrolled device?**
     *
     * A ticket carries the run seed and the customers' phone numbers, so
     * it does not travel in the clear — what leaves this phone is
     * ciphertext addressed to one device's public key. That makes
     * enrolment the gate on transfer: with an empty roster there is
     * nothing to address, and the honest answer is to say what is missing
     * rather than to draw a QR nobody in the world can open.
     */
    fun handOverTicket() {
        val job = _state.value.activeJob
        if (job?.ticketBase64.isNullOrBlank()) {
            _state.update { it.copy(meshNotice = context.getString(R.string.mesh_handover_unavailable)) }
            return
        }
        if (_state.value.enrolledDevices.isEmpty()) {
            _state.update { it.copy(meshNotice = context.getString(R.string.mesh_handover_no_devices)) }
            return
        }
        _state.update { it.copy(handoverPicking = true) }
    }

    fun dismissHandoverPicker() {
        _state.update { it.copy(handoverPicking = false) }
    }

    /**
     * Seals this job to [device] and puts it on screen.
     *
     * The inner ticket is untouched — same signature, same seed, same
     * plan — and only its envelope changes: this phone opens its own copy
     * with its own key and re-seals the bytes to the key it enrolled from
     * that device's card. A photograph of what results is ciphertext for
     * everyone but them.
     *
     * §20.5 is still not enforceable by the protocol: once the other phone
     * opens this, both hold the same seed and the wire cannot tell them
     * apart. Stopping this one stays a human decision, and the dialog
     * offers the action that makes it true.
     */
    fun handOverTo(device: EnrolledDevice) {
        val ticket = _state.value.activeJob?.ticketBase64
        viewModelScope.launch {
            _state.update { it.copy(handoverPicking = false) }
            val sealed = engine.resealTicket(ticket, listOf(device.publicKey))
            val bytes = sealed.ticketBase64
            if (bytes == null) {
                handoverTicket = null
                _state.update {
                    it.copy(
                        handoverTarget = null,
                        meshNotice = context.getString(sealNoticeFor(sealed.code))
                    )
                }
                return@launch
            }
            handoverTicket = bytes
            val matrix = engine.ticketQr(bytes)
            // A plan too dense to photograph still has a handover — as a
            // file — so this opens the dialog rather than reporting there
            // is nothing to hand over.
            if (matrix == null || (matrix.isEmpty && !matrix.tooBig)) {
                _state.update {
                    it.copy(
                        handoverTarget = null,
                        meshNotice = context.getString(R.string.mesh_handover_unavailable)
                    )
                }
            } else {
                _state.update { it.copy(handoverQr = matrix, handoverTarget = device) }
            }
        }
    }

    fun clearHandoverQr() {
        handoverTicket = null
        _state.update { it.copy(handoverQr = null, handoverTarget = null, handoverPicking = false) }
    }

    /**
     * This device's identity, adopted from storage or minted here.
     *
     * The private key crosses the bridge on exactly one call in the life
     * of an install — the one that mints it — and goes straight into the
     * Keystore-wrapped preference. Nothing above this function ever holds
     * it: what reaches the UI is a public key, a fingerprint and a name.
     */
    private suspend fun loadDeviceIdentity() {
        // A first-run card that says "Unnamed device" is a card the other
        // driver cannot pick out of a list, so the handset's own model is
        // the opening guess. It is editable, and that is the point: what
        // a dispatcher is looking for is "Léa — Pixel 8", not a model
        // number two vans in the yard both answer to.
        val name = regionPrefs.deviceName.ifBlank { cappedDeviceName(android.os.Build.MODEL) }
        val loaded = engine.loadDeviceIdentity(regionPrefs.devicePrivateKey, name) ?: return
        loaded.mintedPrivateKey?.let { regionPrefs.devicePrivateKey = it }
        if (regionPrefs.deviceName.isBlank() && loaded.identity.name.isNotBlank()) {
            regionPrefs.deviceName = loaded.identity.name
        }
        _state.update {
            it.copy(
                deviceIdentity = loaded.identity,
                deviceKeyProtected = regionPrefs.deviceKeyHardwareBacked,
                enrolledDevices = regionPrefs.enrolledDevices
            )
        }
    }

    /** Renames this device on its card. Cut to the 32 bytes a card carries. */
    fun setDeviceName(name: String) {
        val capped = cappedDeviceName(name)
        regionPrefs.deviceName = capped
        viewModelScope.launch {
            val identity = engine.setDeviceName(capped) ?: return@launch
            _state.update {
                it.copy(
                    deviceIdentity = identity,
                    // The card on screen carries the old name in its bytes,
                    // so it is withdrawn rather than left to be scanned:
                    // enrolling from it would put the wrong name in the
                    // other driver's roster.
                    myDeviceCard = null
                )
            }
        }
    }

    /**
     * This phone's card, for the other driver's camera.
     *
     * There is no scanner here by design (§20.9 and the ticket path
     * before it): the other phone points its own camera at this, its
     * system camera fires ACTION_VIEW on `wayfind://device#…`, and Wayfind
     * opens with the confirmation sheet.
     */
    fun showMyDeviceCard() {
        viewModelScope.launch {
            val card = engine.myDeviceCard()
            if (card == null) {
                _state.update { it.copy(meshNotice = context.getString(R.string.device_card_unavailable)) }
            } else {
                _state.update { it.copy(myDeviceCard = card, deviceIdentity = card.identity) }
            }
        }
    }

    fun dismissMyDeviceCard() {
        _state.update { it.copy(myDeviceCard = null) }
    }

    /**
     * Adds a device to the roster, which is what makes a job transferable
     * to it at all.
     *
     * The driver has just been shown the fingerprint and told what it is
     * for. Nothing here re-checks it — four bytes is not a cryptographic
     * binding and §20.9 does not pretend otherwise; the comparison is the
     * human one, and this records that it was made.
     */
    fun enrolDevice(card: DeviceCard) {
        val device = EnrolledDevice(
            publicKey = card.publicKeyBase64,
            name = cappedDeviceName(card.name),
            fingerprint = card.fingerprint,
            addedAt = System.currentTimeMillis()
        )
        val roster = regionPrefs.enrolledDevices.withDevice(device)
        regionPrefs.enrolledDevices = roster
        _state.update {
            it.copy(
                enrolledDevices = roster,
                deviceOffer = null,
                meshNotice = context.getString(
                    R.string.device_enrolled_notice,
                    device.displayName(context.getString(R.string.device_unnamed))
                )
            )
        }
    }

    fun dismissDeviceOffer() {
        _state.update { it.copy(deviceOffer = null) }
    }

    /** Drops a device from the roster. Nothing can be sealed to it again. */
    fun forgetDevice(publicKey: String) {
        val roster = regionPrefs.enrolledDevices.withoutDevice(publicKey)
        regionPrefs.enrolledDevices = roster
        _state.update { it.copy(enrolledDevices = roster) }
    }

    /**
     * The sentence a refused seal is owed.
     *
     * Kept in one place because the distinction is the whole point of
     * §20.9's two error codes: "sealed for another device" has an action
     * attached and "unreadable" has a different one, and a single
     * catch-all would send the driver to the wrong screen either way.
     */
    private fun sealNoticeFor(code: String?): Int = when (code) {
        SealError.NOT_ENROLLED -> R.string.mesh_job_sealed_elsewhere
        SealError.NO_DEVICE_KEY -> R.string.mesh_job_no_device_key
        SealError.NO_RECIPIENT -> R.string.mesh_handover_no_devices
        SealError.UNSEALED -> R.string.mesh_job_unsealed
        else -> R.string.mesh_job_ticket_unreadable
    }

    private suspend fun follow(link: String) {
        val result = engine.followDrive(link)
        _state.update {
            it.copy(
                meshNotice = if (result.emitted) null
                else context.getString(R.string.mesh_follow_failed, result.reason.orEmpty()),
                following = engine.followedDrive()
            )
        }
    }

    /** Stops watching a followed drive. */
    fun stopFollowing() {
        viewModelScope.launch {
            engine.stopFollowing()
            _state.update { it.copy(following = null) }
        }
    }

    /**
     * Ends this device's published run: one final record, then it is dead.
     *
     * Which record is not this button's choice. COMPLETED says the day
     * arrived, and a plan with stops nobody has spoken for did not — so a
     * run stopped mid-day closes as CANCELED whatever route got here. See
     * [finalRecordFor]: the invariant is that no path can tell a customer
     * their parcel was delivered because the driver pressed stop.
     */
    fun stopSharingDrive() {
        endRun(_state.value.activeJob, reason = null)
    }

    /**
     * The driver cannot finish this day, and says so on the wire.
     *
     * CANCELED rather than a run that simply goes quiet: every customer
     * holding the link and the dispatcher watching are entitled to hear
     * that nobody is coming, now, instead of waiting out a heartbeat that
     * never arrives. [reason] is the driver's own words, carried as the
     * record's ≤64-byte note.
     */
    fun cancelJob(reason: String?) {
        endRun(_state.value.activeJob, reason = reason, canceled = true)
    }

    /**
     * Closes the run out with the one record it is entitled to.
     *
     * [job] is passed rather than read so the caller that has just marked
     * the last stop closes on the run it marked, not on whatever state has
     * been cleared by then — the all-delivered path is COMPLETED because
     * every stop is resolved, not because of the order two updates ran in.
     */
    private fun endRun(
        job: ActiveJob?,
        reason: String?,
        canceled: Boolean = false
    ) {
        val record = if (canceled) JobFinalRecord.CANCELED else finalRecordFor(job)
        val note = truncateNote(reason)
        // The day goes with the run, on disk as well as on screen. Leaving
        // the plan behind would put the progress card back up on the next
        // launch, offering to continue a job whose publish capability has
        // already been closed out — and every stop of it would fail.
        regionPrefs.activeJob = null
        handoverTicket = null
        viewModelScope.launch {
            engine.endSharedDrive(canceled = record == JobFinalRecord.CANCELED, note = note)
            // The run is dead, so the ticket behind it is no longer a
            // thing to hand anyone: a QR left on screen would offer a job
            // that has already been closed out.
            _state.update {
                it.copy(
                    pulseMesh = engine.pulseMeshStatus(),
                    handoverQr = null,
                    handoverTarget = null,
                    handoverPicking = false,
                    activeJob = null,
                    // Said out loud when the ending was not the ordinary
                    // one: "cancelled" reached people, and the driver is
                    // owed the same account of it that they got.
                    meshNotice = if (record == JobFinalRecord.CANCELED) {
                        context.getString(
                            R.string.mesh_job_canceled_notice,
                            unresolvedStops(job)
                        )
                    } else {
                        it.meshNotice
                    }
                )
            }
        }
    }

    /**
     * The other phone has the job: this one stops publishing and says
     * nothing on the way out.
     *
     * §20.5 — after a handover both devices hold the same seed and the
     * wire cannot tell them apart. A final record from here, COMPLETED or
     * CANCELED, would tell every follower the run had ended while the new
     * driver is still delivering it. So this is the one exit with no final
     * record, and it is reachable only from the handover dialog: an
     * ordinary stop must still say goodbye, or followers are left watching
     * a vehicle that never does.
     */
    fun handedOverJob() {
        regionPrefs.activeJob = null
        handoverTicket = null
        viewModelScope.launch {
            engine.endSharedDrive(silent = true)
            _state.update {
                it.copy(
                    pulseMesh = engine.pulseMeshStatus(),
                    handoverQr = null,
                    handoverTarget = null,
                    handoverPicking = false,
                    activeJob = null,
                    meshNotice = context.getString(R.string.mesh_handover_stopped_notice)
                )
            }
        }
    }

    /**
     * §11 is a harm decision rather than a bandwidth one, so it is the
     * user's: coarse publishes stop events with no position at all, and
     * a leaked coarse link is worth roughly what a printed timetable is.
     */
    fun setShareFine(fine: Boolean) {
        regionPrefs.shareFine = fine
        _state.update { it.copy(shareFine = fine) }
    }

    fun clearMeshNotice() {
        _state.update { it.copy(meshNotice = null) }
    }

    private fun onLocation(location: Location) {
        val point = LatLon(location.latitude, location.longitude)
        travel.onFix(point, System.currentTimeMillis())
        _state.update { it.copy(userLocation = point) }
        offerToPulseMesh(location, point)
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
            hasBearing = fused != null || location.hasBearing(),
            accuracyMeters = if (location.hasAccuracy()) location.accuracy.toDouble() else 0.0
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
            // Arriving ends the drive exactly as pressing stop does, and it
            // was not doing any of the rest of it: the compass stayed
            // registered, the motion model kept its last pose, and the
            // foreground service kept the notification up over a trip that
            // had finished.
            heading.stop()
            motion.reset()
            rerouteLimiter.reset()
            NavigationService.stop(context)
            regionPrefs.activeTrip = null
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
            // On a dispatch ticket this doorstep is one of many, but
            // arriving at it is not delivering: the driver may be about to
            // find nobody home. So the run does not advance here — the
            // progress card raises its marking buttons, and the driver's
            // own tap is what says what happened and moves the day on.
            // With no ticket the arrival above is the whole of it, exactly
            // as it always was.
            if (_state.value.activeJob != null) {
                _state.update { it.copy(arrivedAtJobStop = true) }
            }
            return
        }
        if (update.offRoute) {
            // Reroute from where the car is pointing, not just where it is.
            reroute(
                from = point,
                heading = travelHeading(location),
                accuracyMeters = if (location.hasAccuracy()) location.accuracy.toDouble() else 0.0
            )
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

    private fun reroute(from: LatLon, heading: Double?, accuracyMeters: Double = 0.0) {
        val destination = _state.value.selected ?: return
        if (_state.value.rerouting) return
        val now = System.currentTimeMillis()
        // Asking again on a three-second heartbeat is not persistence, it is a
        // loop: adopting a route clears the strike count, so a car the map
        // cannot place asks forever. Each attempt that does not get it back on
        // the line waits longer than the last.
        if (!rerouteLimiter.shouldReroute(now)) return
        _state.update { it.copy(rerouting = true) }
        recorder.note(
            "reroute",
            detail = "heading=${heading ?: "none"} accuracy=${accuracyMeters}",
            atMillis = now
        )
        if (rerouteLimiter.shouldAnnounce(now)) _voice.tryEmit(context.getString(R.string.nav_rerouting))
        viewModelScope.launch {
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
                    // The replacement goes into the trace as well as into the
                    // state machine. Without it a report of "it rerouted me
                    // onto the wrong road" cannot be checked at all: the file
                    // held the route the drive started with and nothing about
                    // the eleven that replaced it.
                    recorder.noteRoute(bundle.primary, System.currentTimeMillis())
                    _state.update { it.copy(routes = bundle, activeRouteIndex = 0, rerouting = false) }
                }
                .onFailure { _state.update { it.copy(rerouting = false) } }
        }
    }

    companion object {
        /**
         * How long an interrupted drive stays resumable. Long enough to
         * cover a call, a fuel stop or a coffee; short enough that opening
         * the app the next morning does not restart yesterday's trip.
         */
        private const val RESUME_WINDOW_MILLIS = 3 * 60 * 60 * 1000L

        /**
         * How often the map re-reads the mesh. Aggregation buckets are 15 s
         * and records live 90 s, so a picture refreshed every three seconds
         * is already ahead of the data; polling faster would only re-walk
         * the store.
         */
        private const val MESH_POLL_MILLIS = 3000L

        /**
         * Where a shared drive's link points. The capability is in the
         * fragment, which browsers never transmit, so this host serves inert
         * code and never receives a tracking key.
         */
        private const val SHARE_BASE_URL = "https://rangefind.dev/t"

        /**
         * How a ticket is written when it travels as a file. The
         * domainless form on purpose: a handover between two vans in a
         * yard should not depend on anybody's DNS.
         */
        private const val TICKET_LINK_PREFIX = "wayfind://ticket#"

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
