package dev.rangefind.wayfind.engine

import android.app.Activity
import android.content.Context
import android.view.ViewGroup
import android.webkit.JavascriptInterface
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebSettings
import android.webkit.WebView
import androidx.webkit.WebViewAssetLoader
import androidx.webkit.WebViewClientCompat
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Runs the standard rangefind browser runtime in a headless WebView.
 *
 * The page is served from WebViewAssetLoader's virtual https origin, which
 * makes it a secure context: DecompressionStream and crypto.subtle are
 * present, so gzip inflation and SHA-256 pack verification are native and
 * stay enabled. Index bytes are fetched by the runtime itself and never cross
 * the JS bridge — only small JSON results do.
 */
class WebViewRangefindEngine(context: Context) : RangefindEngine {

    private val ready = CompletableDeferred<Unit>()
    private val pending = ConcurrentHashMap<Long, CompletableDeferred<JSONObject>>()
    private val nextId = AtomicLong(1)
    private val webView: WebView

    private inner class Bridge {
        @JavascriptInterface
        fun onReady() {
            ready.complete(Unit)
        }

        @JavascriptInterface
        fun onResult(id: String, json: String) {
            // A cancelled caller removes its slot first, so a late answer is
            // simply dropped instead of resuming a dead coroutine.
            val slot = pending.remove(id.toLongOrNull() ?: return) ?: return
            slot.complete(JSONObject(json))
        }
    }

    init {
        val loader = WebViewAssetLoader.Builder()
            .addPathHandler("/assets/", WebViewAssetLoader.AssetsPathHandler(context))
            .build()

        webView = WebView(context).apply {
            settings.javaScriptEnabled = true
            settings.domStorageEnabled = true
            settings.cacheMode = WebSettings.LOAD_DEFAULT
            // Development route indexes are served over cleartext from the
            // host loopback (10.0.2.2). Production https bases never hit this.
            settings.mixedContentMode = WebSettings.MIXED_CONTENT_ALWAYS_ALLOW
            addJavascriptInterface(Bridge(), "AndroidBridge")
            webViewClient = object : WebViewClientCompat() {
                override fun shouldInterceptRequest(
                    view: WebView,
                    request: WebResourceRequest
                ): WebResourceResponse? = loader.shouldInterceptRequest(request.url)
            }
        }

        // A detached WebView can have its timers throttled. When an Activity
        // hosts us, park it in the view tree at 1x1 so it stays scheduled; the
        // car app has no Activity and runs it detached, which is fine because
        // the work is fetch-driven rather than timer-driven.
        (context as? Activity)
            ?.findViewById<ViewGroup>(android.R.id.content)
            ?.addView(webView, ViewGroup.LayoutParams(1, 1))
        webView.loadUrl("https://appassets.androidplatform.net/assets/rangefind/engine.html")
    }

    fun destroy() {
        (webView.parent as? ViewGroup)?.removeView(webView)
        webView.destroy()
    }

    private suspend fun call(method: String, args: JSONObject): JSONObject {
        ready.await()
        val id = nextId.getAndIncrement()
        val slot = CompletableDeferred<JSONObject>()
        pending[id] = slot

        withContext(Dispatchers.Main.immediate) {
            val script = "__rfCall($id, ${JSONObject.quote(method)}, ${JSONObject.quote(args.toString())})"
            webView.evaluateJavascript(script, null)
        }

        try {
            val envelope = suspendCancellableCoroutine { cont ->
                cont.invokeOnCancellation { pending.remove(id) }
                slot.invokeOnCompletion { error ->
                    if (error != null) cont.resumeWithException(error)
                    else cont.resume(slot.getCompleted())
                }
            }
            if (!envelope.optBoolean("ok")) {
                throw RangefindException(envelope.optString("error", "Query failed"))
            }
            return envelope.optJSONObject("payload") ?: JSONObject()
        } finally {
            pending.remove(id)
        }
    }

    override suspend fun init(searchBase: String, routeBase: String): EngineInfo {
        val payload = call(
            "init",
            JSONObject().put("searchBase", searchBase).put("routeBase", routeBase)
        )
        val routing = payload.toRoutingInfo()
        return EngineInfo(
            attribution = payload.optString("attribution"),
            license = payload.optString("license"),
            total = payload.optLong("total"),
            routing = routing.routing,
            routingError = routing.routingError,
            profile = routing.profile,
            routeBounds = routing.routeBounds
        )
    }

    override suspend fun useRouteBase(routeBase: String): RoutingInfo {
        val payload = call("useRouteBase", JSONObject().put("routeBase", routeBase))
        return payload.toRoutingInfo()
    }

    override suspend fun regionFiles(baseUrl: String): RegionManifest {
        val payload = call("regionFiles", JSONObject().put("baseUrl", baseUrl))
        val array = payload.optJSONArray("files")
        return RegionManifest(
            files = (0 until (array?.length() ?: 0)).mapNotNull { array?.optString(it) }
                .filter { it.isNotEmpty() },
            profile = payload.optString("profile"),
            nodes = payload.optLong("nodes"),
            leaves = payload.optInt("leaves")
        )
    }

    override suspend fun search(
        query: String,
        anchor: LatLon?,
        size: Int,
        shards: List<String>
    ): SearchOutcome {
        val payload = call(
            "search",
            JSONObject()
                .put("q", query)
                .put("size", size)
                .put("shards", JSONArray(shards))
                .putAnchor(anchor)
        )
        return SearchOutcome(
            places = payload.optJSONArray("places").map { it.toPlace() },
            total = payload.optLong("total")
        )
    }

    override suspend fun suggest(query: String, anchor: LatLon?): List<Suggestion> {
        val payload = call("suggest", JSONObject().put("q", query).putAnchor(anchor))
        return payload.optJSONArray("suggestions").map {
            Suggestion(
                text = it.optString("text"),
                mainText = it.optString("mainText"),
                secondaryText = it.optString("secondaryText"),
                kind = it.optString("kind"),
                selectionQuery = it.optString("selectionQuery"),
                selectionShards = it.optJSONArray("selectionShards").strings()
            )
        }
    }

    override suspend fun reverse(point: LatLon): Place? {
        val payload = call("reverse", JSONObject().put("lat", point.lat).put("lon", point.lon))
        return payload.optJSONObject("place")?.toPlace()
    }

    override suspend fun route(
        from: LatLon,
        to: LatLon,
        alternatives: Int,
        fromHeading: Double?,
        accuracyMeters: Double?
    ): RouteBundle {
        val payload = call(
            "route",
            JSONObject()
                .put("from", JSONObject().put("lat", from.lat).put("lon", from.lon))
                .put("to", JSONObject().put("lat", to.lat).put("lon", to.lon))
                .put("alternatives", alternatives)
                .apply { fromHeading?.let { put("fromHeading", it) } }
                .apply { accuracyMeters?.takeIf { it > 0 }?.let { put("accuracyMeters", it) } }
        )
        val primary = payload.optJSONObject("primary")?.toRoute()
            ?: throw RangefindException("No route found")
        return RouteBundle(primary, payload.optJSONArray("alternatives").map { it.toRoute() })
    }

    override suspend fun snap(point: LatLon): SnapPoint? {
        val payload = call("snap", JSONObject().put("lat", point.lat).put("lon", point.lon))
        if (!payload.has("lat")) return null
        return SnapPoint(
            lat = payload.optDouble("lat"),
            lon = payload.optDouble("lon"),
            distMeters = payload.optDouble("distMeters"),
            segment = payload.optString("segment")
        )
    }

    override suspend fun pulseMeshStatus(): PulseMeshStatus =
        callPulseMesh(JSONObject().put("action", "status"))

    override suspend fun setContributing(enabled: Boolean): PulseMeshStatus =
        callPulseMesh(JSONObject().put("action", "enable").put("enabled", enabled))

    override suspend fun offerLocation(
        point: LatLon,
        speedMps: Double?,
        courseDeg: Double?
    ): PulseMeshStatus = callPulseMesh(
        JSONObject()
            .put("action", "location")
            .put("lat", point.lat)
            .put("lon", point.lon)
            .apply {
                speedMps?.let { put("speedMps", it) }
                courseDeg?.let { put("courseDeg", it) }
            }
    )

    /**
     * Live traffic must never take the app down with it: a mesh that is
     * unavailable is the ordinary case (no route index yet, an unsupported
     * host), and the router keeps working on the static metric regardless.
     */
    private suspend fun callPulseMesh(args: JSONObject): PulseMeshStatus = runCatching {
        val payload = call("pulseMesh", args)
        val stats = payload.optJSONObject("stats")
        PulseMeshStatus(
            available = payload.optBoolean("available"),
            contributing = payload.optBoolean("contributing"),
            epoch = payload.optString("epoch"),
            fixes = stats?.optInt("fixes") ?: 0,
            emitted = stats?.optInt("emitted") ?: 0,
            suppressed = stats?.optInt("suppressed") ?: 0,
            lastReason = stats?.optString("lastReason")?.takeIf { it.isNotEmpty() && it != "null" },
            error = payload.optString("error").takeIf { it.isNotEmpty() }
        )
    }.getOrElse { error ->
        PulseMeshStatus(available = false, error = error.message)
    }

}

private fun JSONObject.toRoutingInfo() = RoutingInfo(
    routing = optBoolean("routing"),
    routingError = optString("routingError").takeIf { it.isNotEmpty() && it != "null" },
    profile = optString("profile"),
    routeBounds = optJSONObject("routeBounds")?.let {
        val cells = it.optJSONArray("cells")
        RouteBounds(
            minLat = it.optDouble("minLat"),
            maxLat = it.optDouble("maxLat"),
            minLon = it.optDouble("minLon"),
            maxLon = it.optDouble("maxLon"),
            cells = DoubleArray(cells?.length() ?: 0) { index -> cells!!.optDouble(index) }
        )
    }
)

private fun JSONObject.putAnchor(anchor: LatLon?): JSONObject = apply {
    if (anchor != null) {
        put("anchor", JSONObject().put("lat", anchor.lat).put("lon", anchor.lon))
    }
}

private fun JSONArray?.strings(): List<String> {
    if (this == null) return emptyList()
    return (0 until length()).mapNotNull { optString(it).takeIf { s -> s.isNotEmpty() } }
}

private inline fun <T> JSONArray?.map(transform: (JSONObject) -> T): List<T> {
    if (this == null) return emptyList()
    return (0 until length()).mapNotNull { optJSONObject(it) }.map(transform)
}

private fun JSONObject.toPlace() = Place(
    id = optString("id"),
    name = optString("name"),
    address = optString("address"),
    locality = optString("locality"),
    category = optString("category"),
    type = optString("type"),
    lat = optDouble("lat"),
    lon = optDouble("lon"),
    distanceMeters = if (isNull("distanceMeters")) null else optDouble("distanceMeters")
)

private fun JSONObject.toRoute(): Route {
    val geometry = optJSONArray("geometry")?.let { array ->
        (0 until array.length()).mapNotNull { index ->
            array.optJSONArray(index)?.let { LatLon(it.optDouble(0), it.optDouble(1)) }
        }
    } ?: emptyList()

    return Route(
        seconds = optDouble("seconds"),
        distanceMeters = optDouble("distanceMeters", 0.0),
        geometry = geometry,
        steps = optJSONArray("steps").map {
            RouteStep(
                name = it.optString("name"),
                meters = it.optDouble("meters"),
                seconds = it.optDouble("seconds"),
                at = it.optInt("at"),
                roadClass = it.optString("roadClass", ""),
                lanes = it.optJSONArray("lanes")?.let { array ->
                    List(array.length()) { index -> array.optInt(index) }
                } ?: emptyList(),
                speedLimitKmh = it.optInt("speedLimitKmh"),
                ref = it.optString("ref", ""),
                exitRef = it.optString("exitRef", ""),
                destinationRef = it.optString("destinationRef", ""),
                destination = it.optString("destination", ""),
                roundabout = it.optBoolean("roundabout", false),
                roundaboutExit = it.optInt("roundaboutExit", 0)
            )
        },
        junctions = optJSONArray("junctions").map {
            RouteJunction(
                kind = it.optInt("kind"),
                lat = it.optDouble("lat"),
                lon = it.optDouble("lon"),
                atMeters = it.optDouble("atMeters")
            )
        },
        speedLimits = optJSONArray("speedLimits").map {
            SpeedLimitChange(
                atMeters = it.optDouble("atMeters"),
                limitKmh = it.optInt("limitKmh"),
                conditional = it.optJSONObject("conditional")?.let { c ->
                    ConditionalLimit(
                        limitKmh = c.optInt("limitKmh"),
                        days = c.optInt("days"),
                        startMinute = c.optInt("startMinute"),
                        endMinute = c.optInt("endMinute"),
                        monthStart = c.optInt("monthStart"),
                        monthEnd = c.optInt("monthEnd")
                    )
                }
            )
        },
        httpRequests = optJSONObject("stats")?.optInt("httpRequests") ?: 0,
        bytesFetched = optJSONObject("stats")?.optLong("bytesFetched") ?: 0L
    )
}
