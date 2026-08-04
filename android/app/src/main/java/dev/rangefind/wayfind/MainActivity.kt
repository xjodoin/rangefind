package dev.rangefind.wayfind

import android.Manifest
import android.content.pm.PackageManager
import android.os.Bundle
import android.speech.tts.TextToSpeech
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
import androidx.core.content.ContextCompat
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import dev.rangefind.wayfind.engine.WebViewRangefindEngine
import dev.rangefind.wayfind.location.LocationProvider
import dev.rangefind.wayfind.region.RegionPreferences
import dev.rangefind.wayfind.region.RegionServer
import dev.rangefind.wayfind.region.RegionStore
import dev.rangefind.wayfind.ui.MapScreen
import dev.rangefind.wayfind.ui.MapsViewModel
import dev.rangefind.wayfind.ui.theme.WayfindTheme
import java.util.Locale

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
                val speaker = remember { Speaker(context) }
                DisposableEffect(Unit) { onDispose { speaker.shutdown() } }
                LaunchedEffect(Unit) {
                    viewModel.voice.collect { phrase -> speaker.say(phrase) }
                }

                // Phones get a sheet, tablets and unfolded devices a panel.
                val widthClass = calculateWindowSizeClass(this).widthSizeClass
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
}

/** Minimal TTS wrapper that queues phrases until the engine is ready. */
private class Speaker(context: android.content.Context) {
    private var ready = false
    private var pending: String? = null
    private val tts = TextToSpeech(context.applicationContext) { status ->
        ready = status == TextToSpeech.SUCCESS
        if (ready) pending?.let { say(it) }
        pending = null
    }.apply { }

    fun say(phrase: String) {
        if (!ready) {
            pending = phrase
            return
        }
        runCatching {
            tts.language = Locale.getDefault()
            tts.speak(phrase, TextToSpeech.QUEUE_FLUSH, null, "rf-nav")
        }
    }

    fun shutdown() {
        runCatching {
            tts.stop()
            tts.shutdown()
        }
    }
}
