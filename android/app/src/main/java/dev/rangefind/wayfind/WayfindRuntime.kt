package dev.rangefind.wayfind

import android.content.Context
import dev.rangefind.wayfind.engine.EngineInfo
import dev.rangefind.wayfind.engine.RangefindEngine
import dev.rangefind.wayfind.engine.WebViewRangefindEngine
import dev.rangefind.wayfind.location.LocationProvider
import dev.rangefind.wayfind.region.RegionPreferences
import dev.rangefind.wayfind.region.RegionServer
import dev.rangefind.wayfind.region.RegionStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

const val SEARCH_BASE = "https://osm.rangefind.dev/"

/**
 * One engine per process, shared by the phone UI and the car app.
 *
 * The car head unit and the handset are two views of the same session — the
 * driver expects a destination picked on the phone to be there when they plug
 * in. Two engines would mean two indexes warmed, two sets of caches, and two
 * answers to "where am I going".
 */
object WayfindRuntime {

    private val lock = Mutex()
    private var engineInstance: WebViewRangefindEngine? = null

    lateinit var regionStore: RegionStore
        private set
    lateinit var regionServer: RegionServer
        private set
    lateinit var regionPrefs: RegionPreferences
        private set
    lateinit var locationProvider: LocationProvider
        private set

    private val _info = MutableStateFlow<EngineInfo?>(null)
    val info: StateFlow<EngineInfo?> = _info.asStateFlow()

    val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)

    private var started = false

    @Synchronized
    fun ensureStarted(context: Context) {
        if (started) return
        started = true
        val app = context.applicationContext
        regionStore = RegionStore(app)
        regionServer = RegionServer(regionStore).apply { start() }
        regionPrefs = RegionPreferences(app)
        locationProvider = LocationProvider(app)
    }

    fun routeBaseUrl(fallback: String): String {
        val active = regionPrefs.activeRegion?.takeIf { regionStore.isReady(it) }
        return active?.let { regionServer.baseUrlFor(it) } ?: fallback
    }

    /**
     * The WebView host needs a Context that can create one; the first caller
     * wins and everyone else shares it.
     */
    suspend fun engine(context: Context, routeBase: String): RangefindEngine = lock.withLock {
        engineInstance?.let { return it }
        ensureStarted(context)
        val created = WebViewRangefindEngine(context.applicationContext)
        engineInstance = created
        runCatching { created.init(SEARCH_BASE, routeBaseUrl(routeBase)) }
            .onSuccess { _info.value = it }
        created
    }

    fun engineOrNull(): RangefindEngine? = engineInstance

    fun updateInfo(info: EngineInfo) {
        _info.value = info
    }

    fun shutdown() {
        engineInstance?.destroy()
        engineInstance = null
        if (started) regionServer.stop()
        started = false
    }

    fun launch(block: suspend CoroutineScope.() -> Unit) {
        scope.launch(block = block)
    }
}
