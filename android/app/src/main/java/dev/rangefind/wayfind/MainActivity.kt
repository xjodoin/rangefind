package dev.rangefind.wayfind

import android.Manifest
import android.content.pm.PackageManager
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.windowsizeclass.ExperimentalMaterial3WindowSizeClassApi
import androidx.compose.material3.windowsizeclass.WindowWidthSizeClass
import androidx.compose.material3.windowsizeclass.calculateWindowSizeClass
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalView
import androidx.core.content.ContextCompat
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import dev.rangefind.wayfind.engine.WebViewRangefindEngine
import dev.rangefind.wayfind.location.LocationProvider
import dev.rangefind.wayfind.nav.GuidanceSpeaker
import dev.rangefind.wayfind.region.RegionPreferences
import dev.rangefind.wayfind.region.RegionServer
import dev.rangefind.wayfind.region.RegionStore
import dev.rangefind.wayfind.ui.MapScreen
import android.content.Intent
import android.graphics.Bitmap
import android.net.Uri
import androidx.core.content.FileProvider
import androidx.core.content.IntentCompat
import java.io.File
import dev.rangefind.wayfind.ui.map.MapSnapshotter
import dev.rangefind.wayfind.ui.MapsViewModel
import dev.rangefind.wayfind.ui.SheetMode
import dev.rangefind.wayfind.ui.TicketHandover
import dev.rangefind.wayfind.ui.theme.WayfindTheme

@OptIn(ExperimentalMaterial3WindowSizeClassApi::class)
class MainActivity : ComponentActivity() {

    private lateinit var engine: WebViewRangefindEngine
    private lateinit var regionServer: RegionServer
    /** The link this activity was opened with, until a view model exists. */
    private var pendingThreadLink: String? = null
    private var viewModelRef: MapsViewModel? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()

        // Built before setContent so the headless host is already parked in the
        // view tree and loading its module graph while the UI composes.
        val regionStore = RegionStore(this)
        regionServer = RegionServer(regionStore).apply { start() }
        val regionPrefs = RegionPreferences(this)
        engine = WebViewRangefindEngine(this)
        val locationProvider = LocationProvider(this)

        setContent {
            val darkTheme = isSystemInDarkTheme()
            WayfindTheme(darkTheme = darkTheme) {
                val viewModel: MapsViewModel = viewModel(
                    factory = MapsViewModel.factory(
                        context = applicationContext,
                        engine = engine,
                        locationProvider = locationProvider,
                        regionStore = regionStore,
                        regionServer = regionServer,
                        regionPrefs = regionPrefs,
                        searchBase = SEARCH_BASE,
                        routeBase = BuildConfig.ROUTE_BASE_URL
                    )
                )
                val state by viewModel.state.collectAsStateWithLifecycle()
                val context = LocalContext.current
                viewModelRef = viewModel

                // A link that opened the app is acted on once the engine is
                // up. What happens then depends on what the link *is*: a
                // follow link is read-only, needs no admission bond and
                // publishes nothing on the viewer's behalf, so it is
                // followed immediately — while a dispatch ticket lets its
                // holder move a vehicle and is shown before it is taken.
                LaunchedEffect(state.info != null) {
                    if (state.info == null) return@LaunchedEffect
                    val source = intent
                    val link = pendingThreadLink ?: threadLinkOf(source)
                    pendingThreadLink = null
                    when {
                        link != null -> viewModel.openSharedLink(link)
                        // A file was shared in and nothing in it was a
                        // capability. Saying so beats a maps app that
                        // visibly did nothing at all with what it was
                        // handed.
                        carriesTicketFile(source) -> viewModel.noteUnreadableJob()
                    }
                }

                val permissionLauncher = rememberLauncherForActivityResult(
                    ActivityResultContracts.RequestMultiplePermissions()
                ) { granted ->
                    if (granted.values.any { it }) viewModel.startLocationUpdates()
                }

                LaunchedEffect(Unit) {
                    val fine = ContextCompat.checkSelfPermission(
                        context, Manifest.permission.ACCESS_FINE_LOCATION
                    ) == PackageManager.PERMISSION_GRANTED
                    if (fine) {
                        viewModel.startLocationUpdates()
                    } else {
                        permissionLauncher.launch(
                            arrayOf(
                                Manifest.permission.ACCESS_FINE_LOCATION,
                                Manifest.permission.ACCESS_COARSE_LOCATION
                            )
                        )
                    }
                }

                // Voice guidance: the view model emits phrases, the UI speaks
                // them, so navigation logic stays free of Android media APIs.
                val speaker = remember { GuidanceSpeaker(context) }
                DisposableEffect(Unit) { onDispose { speaker.shutdown() } }
                LaunchedEffect(Unit) {
                    viewModel.voice.collect { phrase -> speaker.say(phrase) }
                }

                // Phones get a sheet, tablets and unfolded devices a panel.
                val widthClass = calculateWindowSizeClass(this).widthSizeClass
                // Guidance is only useful while it is visible: the driver is
                // following the screen, not touching it, so nothing would stop
                // the display timing out mid-route.
                val view = LocalView.current
                val navigating = state.sheet == SheetMode.Navigating
                DisposableEffect(navigating) {
                    view.keepScreenOn = navigating
                    onDispose { view.keepScreenOn = false }
                }

                MapScreen(
                    state = state,
                    darkTheme = darkTheme,
                    wideLayout = widthClass != WindowWidthSizeClass.Compact,
                    onQueryChange = viewModel::onQueryChange,
                    onSubmit = { viewModel.submitSearch() },
                    onClear = viewModel::clearQuery,
                    onSuggestion = viewModel::pickSuggestion,
                    onSelectResult = { index ->
                        state.results.getOrNull(index)?.let(viewModel::selectPlace)
                    },
                    onDismissPlace = viewModel::dismissPlace,
                    onDirections = { viewModel.requestDirections() },
                    onSelectRoute = viewModel::selectRoute,
                    onSelectMode = viewModel::setTravelMode,
                    onStartNavigation = viewModel::startNavigation,
                    onStopNavigation = viewModel::stopNavigation,
                    onExitDirections = viewModel::exitDirections,
                    onRecenter = viewModel::recenter,
                    onCameraDetached = viewModel::cameraDetached,
                    onShowRegions = viewModel::showRegions,
                    onRegionHostChange = viewModel::setRegionHost,
                    onPreloadRegion = viewModel::preloadRegion,
                    onDeleteRegion = viewModel::deleteRegion,
                    onActivateRegion = viewModel::activateRegion,
                    onRecordTripsChange = viewModel::setRecordTrips,
                    onMarkIssue = { captureMark(viewModel.markIssue()) },
                    hasTrace = viewModel.latestTrace() != null,
                    onShareTrace = { shareTrace(viewModel.latestBundle()) },
                    onLiveTraffic = viewModel::setLiveTraffic,
                    onContributeTraffic = viewModel::setContributingTraffic,
                    onSimulatedTraffic = viewModel::setSimulatedTraffic,
                    onShareDrive = {
                        if (state.pulseMesh?.sharing != null) viewModel.stopSharingDrive()
                        else viewModel.shareDrive { url -> url?.let(::shareDriveLink) }
                    },
                    // Never a stop toggle: the driver on a delivery day is
                    // asking for the link to send, and ending the run from
                    // the progress card would close out every customer's.
                    onShareJobLink = { viewModel.shareJobLink { url -> url?.let(::shareDriveLink) } },
                    onShareMode = viewModel::setShareFine,
                    onHandOverJob = viewModel::handOverTicket,
                    onAcceptJob = viewModel::acceptJob,
                    onDeclineJob = viewModel::dismissJobOffer,
                    // Bidding on a broadcast offer is sending the device
                    // card (§20.4): the protocol carries no claim record,
                    // and the card is the same one enrolment needs. It
                    // leaves by the ordinary share sheet, because it
                    // grants nothing and opens nothing.
                    onSendCardForOffer = { viewModel.bidOnOffer(::shareDeviceCard) },
                    onDismissOffer = viewModel::dismissOffer,
                    onForgetHeldOffer = viewModel::forgetHeldOffer,
                    onContinueJob = viewModel::continueJob,
                    onMarkStop = { outcome, reason, note, photo ->
                        viewModel.markCurrentStop(outcome, reason, note, photo)
                    },
                    // CANCELED, with the driver's own words as the note: the
                    // dialog behind this has already said who hears it.
                    onCantFinishJob = viewModel::cancelJob,
                    onCloseHandover = viewModel::clearHandoverQr,
                    onShareTicket = { shareTicket(viewModel.ticketHandover()) },
                    // §20.9: the job is re-sealed to this device's key
                    // before anything is drawn or written. Enrolment is
                    // the gate on transfer, and this is where it bites.
                    onHandOverTo = viewModel::handOverTo,
                    onDismissHandoverPicker = viewModel::dismissHandoverPicker,
                    onDeviceNameChange = viewModel::setDeviceName,
                    onShowDeviceCard = viewModel::showMyDeviceCard,
                    onDismissDeviceCard = viewModel::dismissMyDeviceCard,
                    onShareDeviceCard = ::shareDeviceCard,
                    onEnrolDevice = viewModel::enrolDevice,
                    onDismissDeviceOffer = viewModel::dismissDeviceOffer,
                    onForgetDevice = viewModel::forgetDevice,
                    // The one exit that publishes nothing (§20.5): the other
                    // phone holds the same seed and is driving this vehicle now.
                    onHandedOverJob = viewModel::handedOverJob,
                    onStopFollowing = viewModel::stopFollowing,
                    // The disclosure the protocol requires is on the sheet the
                    // user just tapped through, so this asserts it was shown.
                    onReportIncident = { type -> viewModel.reportIncident(type, true) },
                    onAnswerIncident = viewModel::answerIncident,
                    onClearMeshNotice = viewModel::clearMeshNotice,
                    onLongPress = viewModel::dropPin,
                    onCenterChanged = { viewModel.mapCenter = it }
                )
            }
        }
    }

    /**
     * A capability arriving as a link — a shared drive, or a dispatch
     * ticket the driver just pointed the phone's own camera at.
     *
     * That last part is why there is no scanner in this app: an Android
     * camera fires ACTION_VIEW on the URL it decodes, and this filter is
     * what catches it. The capability itself is in the fragment — which
     * is precisely the part a browser never transmits — so nothing about
     * it reaches rangefind.dev or anywhere else on either path.
     * `singleTop` means a second link lands here rather than in a new task.
     */
    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        val link = threadLinkOf(intent)
        pendingThreadLink = link
        // The view model exists whenever the activity is on screen, which
        // is the only way onNewIntent runs; the pending link is kept anyway
        // so a cold start with the same intent is covered.
        viewModelRef?.let { model ->
            pendingThreadLink = null
            when {
                link != null -> model.openSharedLink(link)
                carriesTicketFile(intent) -> model.noteUnreadableJob()
            }
        }
    }

    /**
     * The capability this intent carries, however it arrived.
     *
     * Three ways in, one answer: a URL the camera decoded, a file shared
     * into the app from mail or chat, or a downloaded `.wayfindjob` tapped
     * in a file manager. The app has always branched on the artifact rather
     * than on the path — this only widens what counts as a path.
     */
    private fun threadLinkOf(intent: Intent?): String? = when (intent?.action) {
        Intent.ACTION_VIEW -> viewLinkOf(intent)
        // A plan too large for a camera travels as a file, and a file
        // reaches an app by being shared into it. Mail clients put the body
        // in EXTRA_TEXT and the attachment in EXTRA_STREAM, chat apps often
        // only one of the two, so both are tried in that order.
        Intent.ACTION_SEND -> linkInText(intent.getStringExtra(Intent.EXTRA_TEXT))
            ?: linkInText(readSharedText(streamOf(intent)))
        else -> null
    }

    private fun viewLinkOf(intent: Intent): String? {
        val data = intent.data ?: return null
        // A ticket saved to Downloads and tapped there arrives as a
        // content:// URI with no fragment at all: the capability is inside
        // the file, so the file is what gets read.
        if (data.scheme == "content") return linkInText(readSharedText(data))
        // Otherwise only the fragment matters; a path or query would be
        // somebody else's link, and following it would be following nothing.
        return data.fragment?.takeIf { it.length >= 40 }?.let { data.toString() }
    }

    /** Whether this intent was an attempt to hand the app a ticket file. */
    private fun carriesTicketFile(intent: Intent?): Boolean = when (intent?.action) {
        Intent.ACTION_SEND -> true
        Intent.ACTION_VIEW -> intent.data?.scheme == "content"
        else -> false
    }

    private fun streamOf(intent: Intent): Uri? =
        IntentCompat.getParcelableExtra(intent, Intent.EXTRA_STREAM, Uri::class.java)

    /**
     * The text of a file shared in, up to a cap.
     *
     * A ticket is a couple of kilobytes at the very most, so 64 KB is
     * already generous — and a cap is what stops somebody sharing a 2 GB
     * video into a maps app and taking it down. Anything unreadable is
     * simply not a ticket, and the caller says so rather than throwing.
     */
    private fun readSharedText(uri: Uri?): String? {
        if (uri == null) return null
        return runCatching {
            contentResolver.openInputStream(uri)?.use { stream ->
                val buffer = ByteArray(MAX_TICKET_BYTES)
                var filled = 0
                while (filled < buffer.size) {
                    val read = stream.read(buffer, filled, buffer.size - filled)
                    if (read <= 0) break
                    filled += read
                }
                String(buffer, 0, filled, Charsets.UTF_8)
            }
        }.getOrNull()
    }

    /**
     * A capability found in whatever text arrived.
     *
     * The fragment has *all* of its whitespace removed before it is used: a
     * mail client wraps a long line wherever it pleases, and a break inside
     * a several-hundred-character token is transport damage rather than
     * content. What comes out either verifies as a ticket downstream or is
     * reported unreadable — this is here to find a candidate, never to
     * vouch for one, which is also why prose after the link on the same
     * line is left for the verifier to reject.
     */
    private fun linkInText(text: String?): String? {
        if (text.isNullOrBlank()) return null
        val start = SCHEMES.mapNotNull { scheme -> text.indexOf(scheme).takeIf { it >= 0 } }
            .minOrNull() ?: return null
        val tail = text.substring(start)
        val hash = tail.indexOf('#')
        if (hash < 0) return null
        val head = tail.substring(0, hash)
        // Whitespace before the '#' is the end of the link, not damage
        // inside it: a wrap lands in the fragment, which is the long part.
        if (head.any { it.isWhitespace() }) return null
        val fragment = tail.substring(hash + 1).filterNot { it.isWhitespace() }
        if (fragment.length < 40) return null
        return "$head#$fragment"
    }

    override fun onDestroy() {
        super.onDestroy()
        if (::engine.isInitialized) engine.destroy()
        if (::regionServer.isInitialized) regionServer.stop()
    }

    /**
     * Hands a trip trace to whatever the user wants to send it with. Shared
     * through a FileProvider scoped to the trace directory alone, so the grant
     * cannot reach the region indexes or anything else in private storage.
     */
    /**
     * Hands the drive's capability link to whatever the user picks.
     *
     * The link is the whole credential — 45 bytes in a URL fragment — so
     * it goes out through the ordinary share sheet and nowhere else: there
     * is no endpoint, here or at rangefind.dev, that ever receives it.
     */
    private fun shareDriveLink(url: String) {
        val send = Intent(Intent.ACTION_SEND).apply {
            type = "text/plain"
            putExtra(Intent.EXTRA_TEXT, url)
            putExtra(Intent.EXTRA_SUBJECT, getString(R.string.mesh_share_title))
        }
        runCatching {
            startActivity(Intent.createChooser(send, getString(R.string.mesh_share_title)))
        }
    }

    /**
     * Hands the whole plan on as a file.
     *
     * The link goes in EXTRA_TEXT *and* the file in EXTRA_STREAM, carrying
     * the same capability twice: a chat app takes the text and drops the
     * attachment, a mail client does the reverse, and the receiving phone
     * reads whichever one survived. Scoped to its own FileProvider
     * authority so the grant reaches the ticket and nothing else the app
     * keeps — the trace provider deliberately exposes only traces.
     */
    private fun shareTicket(handover: TicketHandover?) {
        val file = handover?.file?.takeIf { it.isFile } ?: return
        val uri = runCatching {
            FileProvider.getUriForFile(this, "$packageName.tickets", file)
        }.getOrNull() ?: return
        val send = Intent(Intent.ACTION_SEND).apply {
            type = "text/plain"
            putExtra(Intent.EXTRA_STREAM, uri)
            putExtra(Intent.EXTRA_TEXT, handover.link)
            putExtra(Intent.EXTRA_SUBJECT, file.name)
            addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
        }
        runCatching {
            startActivity(Intent.createChooser(send, getString(R.string.mesh_handover_share_chooser)))
        }
    }

    /**
     * Sends this device's card, for a driver who is not standing here.
     *
     * A card is a public key and a name — it grants nothing and opens
     * nothing, which is exactly why it can go out over an ordinary share
     * sheet. What it does is let the *other* phone seal a job to this one,
     * and the fingerprint they compare is what says the card is ours.
     */
    private fun shareDeviceCard(url: String) {
        val send = Intent(Intent.ACTION_SEND).apply {
            type = "text/plain"
            putExtra(Intent.EXTRA_TEXT, url)
            putExtra(Intent.EXTRA_SUBJECT, getString(R.string.device_card_title))
        }
        runCatching {
            startActivity(Intent.createChooser(send, getString(R.string.device_card_share_chooser)))
        }
    }

    private fun shareTrace(trace: File?) {
        if (trace == null || !trace.isFile) return
        val uri = runCatching {
            FileProvider.getUriForFile(this, "$packageName.traces", trace)
        }.getOrNull() ?: return
        val send = Intent(Intent.ACTION_SEND).apply {
            type = "application/zip"
            putExtra(Intent.EXTRA_STREAM, uri)
            putExtra(Intent.EXTRA_SUBJECT, trace.name)
            addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
        }
        runCatching {
            startActivity(Intent.createChooser(send, getString(R.string.diag_share_chooser)))
        }
    }


    /**
     * Captures the map the driver was looking at when they flagged a fault.
     *
     * Only the map: everything drawn over it is already in the trace as
     * state, whereas the rendered roads, route line and junction icons exist
     * nowhere else. Failure is silent — the mark's own line is already
     * written, and a report without its picture beats no report.
     */
    private fun captureMark(target: File?) {
        if (target == null) return
        MapSnapshotter.snapshot { bitmap ->
            if (bitmap == null) return@snapshot
            // Off the main thread: a driver tapped this, and encoding a
            // full-screen PNG is not something to do between frames.
            Thread {
                runCatching {
                    target.outputStream().use { out ->
                        bitmap.compress(Bitmap.CompressFormat.PNG, 90, out)
                    }
                }
            }.start()
        }
    }

    private companion object {
        /**
         * The most of a shared file that is read looking for a ticket. A
         * plan is a couple of kilobytes at the outside, so this is already
         * generous; it exists so that sharing something enormous into the
         * app costs a bounded read rather than the process.
         */
        const val MAX_TICKET_BYTES = 64 * 1024

        /**
         * How a capability can be written. The domainless form first: it is
         * what a handover between two vans in a yard uses, and it depends
         * on nobody's DNS.
         */
        val SCHEMES = listOf("wayfind://", "https://")
    }
}
