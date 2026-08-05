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
import androidx.core.content.FileProvider
import java.io.File
import dev.rangefind.wayfind.ui.map.MapSnapshotter
import dev.rangefind.wayfind.ui.MapsViewModel
import dev.rangefind.wayfind.ui.SheetMode
import dev.rangefind.wayfind.ui.theme.WayfindTheme

@OptIn(ExperimentalMaterial3WindowSizeClassApi::class)
class MainActivity : ComponentActivity() {

    private lateinit var engine: WebViewRangefindEngine
    private lateinit var regionServer: RegionServer

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
                    onStartNavigation = viewModel::startNavigation,
                    onStopNavigation = viewModel::stopNavigation,
                    onExitDirections = viewModel::exitDirections,
                    onRecenter = viewModel::recenter,
                    onShowRegions = viewModel::showRegions,
                    onRegionHostChange = viewModel::setRegionHost,
                    onPreloadRegion = viewModel::preloadRegion,
                    onDeleteRegion = viewModel::deleteRegion,
                    onActivateRegion = viewModel::activateRegion,
                    onRecordTripsChange = viewModel::setRecordTrips,
                    onMarkIssue = { captureMark(viewModel.markIssue()) },
                    hasTrace = viewModel.latestTrace() != null,
                    onShareTrace = { shareTrace(viewModel.latestBundle()) },
                    onLongPress = viewModel::dropPin,
                    onCenterChanged = { viewModel.mapCenter = it }
                )
            }
        }
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

}
