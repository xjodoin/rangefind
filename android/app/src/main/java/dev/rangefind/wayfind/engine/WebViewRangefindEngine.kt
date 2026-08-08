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
            // The engine is headless, so anything it says has nowhere to go
            // by default — an exception inside the bridge simply vanishes and
            // the app shows an empty result with no explanation. Debug builds
            // forward the console to logcat, which is the difference between
            // "live traffic did not start" and knowing why.
            if (dev.rangefind.wayfind.BuildConfig.DEBUG) {
                webChromeClient = object : android.webkit.WebChromeClient() {
                    override fun onConsoleMessage(message: android.webkit.ConsoleMessage): Boolean {
                        android.util.Log.d(
                            "RangefindEngine",
                            "${message.messageLevel()} ${message.message()} " +
                                "(${message.sourceId()}:${message.lineNumber()})"
                        )
                        return true
                    }
                }
            }
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
            JSONObject()
                .put("searchBase", searchBase)
                .put("routeBase", routeBase)
                // A keeper multiaddr, when the deployment has one. Empty
                // means the only mesh available is the on-device demo
                // transport, which the UI labels as such.
                .put("pulseMeshBootstrap", dev.rangefind.wayfind.BuildConfig.PULSEMESH_BOOTSTRAP)
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

    override suspend fun setMeshRunning(
        running: Boolean,
        mode: String,
        simulated: Boolean
    ): PulseMeshStatus =
        callPulseMesh(
            JSONObject()
                .put("action", if (running) "start" else "stop")
                .put("simulated", simulated)
                .apply { if (mode.isNotEmpty()) put("mode", mode) }
        )

    override suspend fun setSimulatedTraffic(enabled: Boolean): PulseMeshStatus =
        callPulseMesh(JSONObject().put("action", "simulate").put("enabled", enabled))

    override suspend fun setContributing(enabled: Boolean): PulseMeshStatus =
        callPulseMesh(JSONObject().put("action", "contribute").put("enabled", enabled))

    override suspend fun offerLocation(
        point: LatLon,
        speedMps: Double?,
        courseDeg: Double?,
        batteryLevel: Double?,
        charging: Boolean?
    ): PulseMeshStatus = callPulseMesh(
        JSONObject()
            .put("action", "location")
            .put("lat", point.lat)
            .put("lon", point.lon)
            .apply {
                speedMps?.let { put("speedMps", it) }
                courseDeg?.let { put("courseDeg", it) }
                batteryLevel?.let { put("batteryLevel", it) }
                charging?.let { put("charging", it) }
            }
    )

    override suspend fun tickMesh() {
        runCatching { call("pulseMesh", JSONObject().put("action", "tick")) }
    }

    override suspend fun meshTraffic(): List<TrafficSegment> = runCatching {
        call("pulseMesh", JSONObject().put("action", "traffic"))
            .optJSONArray("segments").map { entry ->
                TrafficSegment(
                    segment = entry.optString("segment"),
                    points = entry.optJSONArray("points")?.let { array ->
                        (0 until array.length()).mapNotNull { index ->
                            array.optJSONArray(index)?.let { LatLon(it.optDouble(0), it.optDouble(1)) }
                        }
                    } ?: emptyList(),
                    speedKmh = entry.optDouble("speedKmh", 0.0),
                    freeflowKmh = if (entry.isNull("freeflowKmh")) null else entry.optDouble("freeflowKmh"),
                    ratio = if (entry.isNull("ratio")) null else entry.optDouble("ratio"),
                    level = entry.optString("level", "unknown"),
                    reports = entry.optInt("reports"),
                    confidence = entry.optDouble("confidence", 0.0),
                    ageSeconds = entry.optInt("ageSeconds")
                )
            }.filter { it.points.size >= 2 }
    }.getOrDefault(emptyList())

    override suspend fun meshIncidents(): List<MeshIncident> = runCatching {
        call("pulseMesh", JSONObject().put("action", "incidents"))
            .optJSONArray("incidents").map { entry ->
                MeshIncident(
                    key = entry.optString("key"),
                    type = entry.optInt("type"),
                    typeName = entry.optString("typeName"),
                    lat = entry.optDouble("lat"),
                    lon = entry.optDouble("lon"),
                    score = entry.optDouble("score", 0.0),
                    sources = entry.optInt("sources"),
                    tier = entry.optString("tier", "hint"),
                    informational = entry.optBoolean("informational"),
                    ageSeconds = entry.optInt("ageSeconds")
                )
            }
    }.getOrDefault(emptyList())

    override suspend fun reportIncident(type: Int, acknowledgedPublic: Boolean): MeshAction =
        meshAction(
            JSONObject()
                .put("action", "report")
                .put("type", type)
                .put("acknowledgedPublic", acknowledgedPublic)
        )

    override suspend fun answerIncident(
        key: String,
        polarity: Int,
        acknowledgedPublic: Boolean
    ): MeshAction = meshAction(
        JSONObject()
            .put("action", "answer")
            .put("key", key)
            .put("polarity", polarity)
            .put("acknowledgedPublic", acknowledgedPublic)
    )

    override suspend fun shareDrive(
        stops: List<LatLon>,
        baseUrl: String,
        fine: Boolean,
        travelMode: String
    ): String? = runCatching {
        val payload = call(
            "pulseMesh",
            JSONObject()
                .put("action", "shareDrive")
                .put("baseUrl", baseUrl)
                .put("fine", fine)
                .put("travelMode", travelMode)
                .put("stops", JSONArray().apply {
                    for (stop in stops) put(JSONObject().put("lat", stop.lat).put("lon", stop.lon))
                })
        )
        payload.optJSONObject("thread")?.optString("url")?.takeIf { it.isNotEmpty() }
    }.getOrNull()

    override suspend fun endSharedDrive(canceled: Boolean, note: String?, silent: Boolean) {
        runCatching {
            call(
                "pulseMesh",
                JSONObject()
                    .put("action", "endDrive")
                    .put("canceled", canceled)
                    // No final record at all, for the one caller entitled to
                    // that: the device that just handed the job on (§20.5).
                    .put("silent", silent)
                    .apply { note?.takeIf { it.isNotBlank() }?.let { put("note", it) } }
            )
        }
    }

    override suspend fun followDrive(link: String): MeshAction =
        meshAction(JSONObject().put("action", "followDrive").put("link", link))

    override suspend fun stopFollowing() {
        runCatching { call("pulseMesh", JSONObject().put("action", "stopFollowing")) }
    }

    override suspend fun followedEta(to: LatLon): FollowedEta? = runCatching {
        val eta = call(
            "pulseMesh",
            JSONObject()
                .put("action", "followedEta")
                .put("lat", to.lat)
                .put("lon", to.lon)
        ).optJSONObject("eta") ?: return@runCatching null
        FollowedEta(
            // Null rather than zero on a marked stop: the protocol goes out
            // of its way to leave no shape a caller can render a time out
            // of, and a 0.0 here would put "arriving now" on a doorstep
            // nobody is driving to.
            secondsFromNow = if (eta.isNull("secondsFromNow")) null
            else eta.optDouble("secondsFromNow"),
            basis = eta.optString("basis"),
            positionBasis = eta.optString("positionBasis"),
            stale = eta.optBoolean("stale"),
            outcome = eta.optInt("outcome", StopOutcome.PENDING),
            reasonCode = if (eta.isNull("reasonCode")) null else eta.optInt("reasonCode"),
            profile = eta.optString("profile").takeIf { it.isNotEmpty() && it != "null" },
            profileBasis = eta.optString("profileBasis", "unstated"),
            travelModeMismatch = eta.optBoolean("travelModeMismatch"),
            travelModeName = eta.optString("travelModeName")
                .takeIf { it.isNotEmpty() && it != "null" }
        )
    }.getOrNull()

    override suspend fun followedDrive(): FollowedDrive? = runCatching {
        val following = call("pulseMesh", JSONObject().put("action", "followed"))
            .optJSONObject("following") ?: return@runCatching null
        FollowedDrive(
            live = following.optBoolean("live"),
            hasPosition = following.optBoolean("hasPosition"),
            claim = following.optString("claim"),
            ageSeconds = if (following.isNull("ageSeconds")) null else following.optInt("ageSeconds"),
            lat = if (following.isNull("lat")) null else following.optDouble("lat"),
            lon = if (following.isNull("lon")) null else following.optDouble("lon"),
            travelModeName = following.optString("travelModeName")
                .takeIf { it.isNotEmpty() && it != "null" },
            outcomes = following.optJSONArray("outcomes").ints(),
            lastOutcome = following.optJSONObject("lastOutcome")?.let {
                StopMark(
                    stopIndex = it.optInt("stopIndex"),
                    outcome = it.optInt("outcome"),
                    reasonCode = it.optInt("reasonCode")
                )
            }
        )
    }.getOrNull()

    override suspend fun markStop(
        index: Int,
        outcome: Int,
        reason: Int,
        note: String?,
        photoBase64: String?
    ): MeshAction = meshAction(
        JSONObject()
            .put("action", "markStop")
            .put("index", index)
            .put("outcome", outcome)
            .put("reason", reason)
            .apply { note?.takeIf { it.isNotBlank() }?.let { put("note", it) } }
            .apply { photoBase64?.takeIf { it.isNotEmpty() }?.let { put("photoBase64", it) } }
    )

    override suspend fun classifyMeshLink(link: String): MeshArtifact = runCatching {
        val payload = call("pulseMesh", JSONObject().put("action", "classify").put("link", link))
        MeshArtifact(
            kind = payload.optString("kind").takeIf { it.isNotEmpty() && it != "null" },
            reason = payload.optString("reason").takeIf { it.isNotEmpty() && it != "null" },
            sealed = payload.optBoolean("sealed"),
            unreadable = payload.optBoolean("unreadable"),
            device = payload.optJSONObject("device")?.toDeviceCard()
        )
    }.getOrElse { MeshArtifact() }

    override suspend fun describeJob(
        link: String,
        heldOffers: List<String>
    ): JobDescription = runCatching {
        val payload = call(
            "pulseMesh",
            JSONObject()
                .put("action", "describeJob")
                .put("link", link)
                // Every held bid, not the one whose jobId looks right: a
                // swapped plan moves the jobId as well, so a lookup by
                // jobId finds nothing and lets the swap through (§20.4).
                .put("offers", JSONArray(heldOffers))
        )
        JobDescription(
            offer = payload.optJSONObject("job")?.toJobOffer(),
            code = payload.optString("code").takeIf { it.isNotEmpty() && it != "null" },
            sealed = payload.optBoolean("sealed"),
            offerChecks = payload.optJSONArray("offerChecks").map {
                AwardCheck(
                    index = it.optInt("index", -1),
                    ok = it.optBoolean("ok"),
                    reason = it.optString("reason").takeIf { text ->
                        text.isNotEmpty() && text != "null"
                    }
                )
            }
        )
    }.getOrElse { JobDescription(code = SealError.UNREADABLE) }

    override suspend fun describeOffer(link: String): OfferDescription = runCatching {
        val payload = call("pulseMesh", JSONObject().put("action", "describeOffer").put("link", link))
        OfferDescription(
            offer = payload.optJSONObject("offer")?.toOfferSummary(),
            kind = payload.optString("kind").takeIf { it.isNotEmpty() && it != "null" },
            error = payload.optString("error").takeIf { it.isNotEmpty() && it != "null" }
        )
    }.getOrElse { OfferDescription(error = it.message) }

    override suspend fun loadDeviceIdentity(
        privateKeyBase64: String?,
        name: String
    ): DeviceKeyLoad? = runCatching {
        val payload = call(
            "device",
            JSONObject()
                .put("action", "load")
                .put("name", name)
                .apply { privateKeyBase64?.takeIf { it.isNotBlank() }?.let { put("privateKey", it) } }
        )
        val identity = payload.toDeviceIdentity() ?: return@runCatching null
        DeviceKeyLoad(
            identity = identity,
            // Present on exactly one call in the life of an install. Every
            // later start hands the stored key down and gets null back.
            mintedPrivateKey = payload.optString("privateKey")
                .takeIf { it.isNotEmpty() && it != "null" }
        )
    }.getOrNull()

    override suspend fun setDeviceName(name: String): DeviceIdentity? = runCatching {
        call("device", JSONObject().put("action", "name").put("name", name)).toDeviceIdentity()
    }.getOrNull()

    override suspend fun myDeviceCard(): DeviceCardArtifact? = runCatching {
        val payload = call("device", JSONObject().put("action", "card"))
        val identity = payload.toDeviceIdentity() ?: return@runCatching null
        val url = payload.optString("url").takeIf { it.isNotEmpty() } ?: return@runCatching null
        val size = payload.optInt("size")
        DeviceCardArtifact(
            identity = identity,
            url = url,
            // A card is a few dozen bytes, so this always fits; the null
            // path is here because a link that cannot be drawn can still
            // be sent, and the sheet should show the one that works.
            qr = if (size > 0) QrMatrix(size, payload.optJSONArray("rows").strings()) else null
        )
    }.getOrNull()

    override suspend fun readDeviceCard(link: String): DeviceCard? = runCatching {
        call("device", JSONObject().put("action", "read").put("link", link))
            .optJSONObject("device")?.toDeviceCard()
    }.getOrNull()

    override suspend fun resealTicket(
        ticketBase64: String?,
        recipientPublicKeys: List<String>
    ): SealedTicket = runCatching {
        val payload = call(
            "device",
            JSONObject()
                .put("action", "reseal")
                .put("recipients", JSONArray(recipientPublicKeys))
                .apply { ticketBase64?.takeIf { it.isNotBlank() }?.let { put("ticket", it) } }
        )
        SealedTicket(
            ticketBase64 = payload.optString("ticket").takeIf { it.isNotEmpty() && it != "null" },
            reason = payload.optString("error").takeIf { it.isNotEmpty() && it != "null" },
            code = payload.optString("code").takeIf { it.isNotEmpty() && it != "null" }
        )
    }.getOrElse { SealedTicket(reason = it.message) }

    override suspend fun acceptJob(
        link: String,
        baseUrl: String,
        travelMode: String
    ): AcceptedJob? = runCatching {
        val payload = call(
            "pulseMesh",
            JSONObject()
                .put("action", "acceptJob")
                .put("link", link)
                .put("baseUrl", baseUrl)
                // Only a fallback: a ticket whose plan names a mode keeps it.
                .put("travelMode", travelMode)
        )
        val last = payload.optJSONObject("lastAction")
        AcceptedJob(
            action = MeshAction(
                emitted = last?.optBoolean("emitted") ?: false,
                reason = last?.optString("reason")?.takeIf { it.isNotEmpty() && it != "null" },
                // §20.9: a job sealed for another device is not a failure
                // to retry, and the app owes the driver that sentence.
                code = last?.optString("code")?.takeIf { it.isNotEmpty() && it != "null" }
            ),
            job = payload.optJSONObject("job")?.toJobOffer(),
            customerUrl = payload.optString("customerUrl").takeIf { it.isNotEmpty() && it != "null" },
            ticketBase64 = payload.optString("ticket").takeIf { it.isNotEmpty() && it != "null" }
        )
    }.getOrElse { AcceptedJob(MeshAction(emitted = false, reason = it.message)) }

    override suspend fun ticketQr(ticket: String?): QrMatrix? = runCatching {
        val payload = call(
            "pulseMesh",
            JSONObject()
                .put("action", "ticketQr")
                // The app's persisted copy, which outlives this WebView and
                // the run it published.
                .apply { ticket?.takeIf { it.isNotBlank() }?.let { put("ticket", it) } }
        )
        val size = payload.optInt("size")
        // A plan too large for a scannable symbol is an answer, not a
        // failure: there is no matrix, and saying so is what lets the app
        // offer the file instead of reporting nothing to hand over.
        if (size <= 0) {
            return@runCatching if (payload.optBoolean("tooBig")) {
                QrMatrix(size = 0, rows = emptyList(), tooBig = true)
            } else {
                null
            }
        }
        QrMatrix(size = size, rows = payload.optJSONArray("rows").strings())
    }.getOrNull()

    private suspend fun meshAction(args: JSONObject): MeshAction = runCatching {
        val payload = call("pulseMesh", args)
        val last = payload.optJSONObject("lastAction")
        MeshAction(
            emitted = last?.optBoolean("emitted") ?: false,
            reason = last?.optString("reason")?.takeIf { it.isNotEmpty() && it != "null" },
            photoRefused = last?.optBoolean("photoRefused") ?: false
        )
    }.getOrElse { MeshAction(emitted = false, reason = it.message) }

    /**
     * Live traffic must never take the app down with it: a mesh that is
     * unavailable is the ordinary case (no route index yet, no keeper
     * configured, an unsupported host), and the router keeps working on the
     * static metric regardless.
     */
    private suspend fun callPulseMesh(args: JSONObject): PulseMeshStatus = runCatching {
        val payload = call("pulseMesh", args)
        PulseMeshStatus(
            available = payload.optBoolean("available"),
            mode = payload.optString("mode", "off"),
            simulated = payload.optBoolean("simulated"),
            contributing = payload.optBoolean("contributing"),
            readOnly = payload.optBoolean("readOnly"),
            epoch = payload.optString("epoch"),
            peers = payload.optInt("peers"),
            records = payload.optInt("records"),
            segments = payload.optInt("segments"),
            zones = payload.optInt("zones"),
            fixes = payload.optInt("fixes"),
            emitted = payload.optInt("emitted"),
            suppressed = payload.optInt("suppressed"),
            incidents = payload.optInt("incidents"),
            lastReason = payload.optString("lastReason").takeIf { it.isNotEmpty() && it != "null" },
            threadsAvailable = payload.optBoolean("threadsAvailable"),
            incidentTypes = payload.optJSONArray("types").map {
                IncidentType(
                    type = it.optInt("type"),
                    name = it.optString("name"),
                    informational = it.optBoolean("informational")
                )
            },
            sharing = payload.optJSONObject("thread")?.let {
                SharedDrive(
                    url = it.optString("url"),
                    seq = it.optInt("seq"),
                    jobIdHex = it.optString("jobIdHex").takeIf { hex ->
                        hex.isNotEmpty() && hex != "null"
                    },
                    fromTicket = it.optBoolean("fromTicket")
                )
            },
            error = payload.optString("error").takeIf { it.isNotEmpty() && it != "null" }
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

private fun JSONArray?.ints(): List<Int> {
    if (this == null) return emptyList()
    return (0 until length()).map { optInt(it) }
}

private fun JSONArray?.strings(): List<String> {
    if (this == null) return emptyList()
    return (0 until length()).mapNotNull { optString(it).takeIf { s -> s.isNotEmpty() } }
}

private inline fun <T> JSONArray?.map(transform: (JSONObject) -> T): List<T> {
    if (this == null) return emptyList()
    return (0 until length()).mapNotNull { optJSONObject(it) }.map(transform)
}

/** §20.2: mode 1 is coarse, 2 is fine — and it is the issuer's call. */
private const val THREAD_MODE_FINE = 2

/**
 * A per-stop text field the dispatcher may not have stated (§20.8).
 *
 * `optString` answers `""` for a JSON null, and `""` here would be a
 * *stated* empty order reference — a claim nobody made, and one the UI
 * would then have to draw an empty row for. Unstated has exactly one
 * form on this side of the bridge and it is null.
 */
internal fun JSONObject.stopText(key: String): String? =
    if (has(key) && !isNull(key)) optString(key).takeIf { it.isNotBlank() } else null

/**
 * A per-stop count the dispatcher may not have stated.
 *
 * The same rule, and the case it exists for: `optInt` answers 0 for an
 * absent key, which is the one value that must never be invented here.
 * A stated 0 means the van is carrying nothing for this door.
 */
internal fun JSONObject.stopInt(key: String): Int? =
    if (has(key) && !isNull(key)) optInt(key) else null

/**
 * This device's public identity. There is no private-key field here on
 * purpose: the key lives in the engine and in the host's storage, and
 * nothing that draws a screen ever holds it.
 */
private fun JSONObject.toDeviceIdentity(): DeviceIdentity? {
    val publicKey = optString("publicKey").takeIf { it.isNotEmpty() && it != "null" } ?: return null
    return DeviceIdentity(
        publicKeyBase64 = publicKey,
        fingerprint = optString("fingerprint"),
        name = if (isNull("name")) "" else optString("name")
    )
}

private fun JSONObject.toDeviceCard(): DeviceCard? {
    val publicKey = optString("publicKey").takeIf { it.isNotEmpty() && it != "null" } ?: return null
    return DeviceCard(
        publicKeyBase64 = publicKey,
        // A device its owner never named is ordinary; "null" on a
        // confirmation sheet is not.
        name = if (isNull("name")) "" else optString("name"),
        fingerprint = optString("fingerprint")
    )
}

private fun JSONObject.toJobOffer() = JobOffer(
    jobIdHex = optString("jobIdHex"),
    stops = optJSONArray("stops").map {
        JobStop(
            index = it.optInt("index"),
            lat = it.optDouble("lat"),
            lon = it.optDouble("lon"),
            label = it.optString("label"),
            orderRef = it.stopText("orderRef"),
            parcels = it.stopInt("parcels"),
            instructions = it.stopText("instructions"),
            contact = it.stopText("contact")
        )
    },
    fine = optInt("mode") == THREAD_MODE_FINE,
    // Null when the plan left the field at 0: "unspecified" is not a mode
    // and a line naming one would invent it.
    travelModeName = optString("travelModeName").takeIf { it.isNotEmpty() && it != "null" },
    notAfter = optLong("notAfter"),
    issuerHex = optString("issuerHex"),
    ok = optBoolean("ok"),
    reason = optString("reason").takeIf { it.isNotEmpty() && it != "null" }
)

/**
 * A broadcast offer as it crossed the bridge (§20.4).
 *
 * Three fields are read with `isNull` rather than with a falsy test,
 * because the difference is the dispatcher's: a pay of `0` is a job
 * offered for nothing and null is a dispatcher who did not say, and a
 * courier owed the first must not be shown the second.
 */
private fun JSONObject.toOfferSummary() = OfferSummary(
    offerBase64 = optString("offer"),
    jobIdHex = optString("jobIdHex"),
    planRefHex = optString("planRefHex"),
    stopCount = optInt("stopCount"),
    travelModeName = optString("travelModeName").takeIf { it.isNotEmpty() && it != "null" },
    fine = optInt("mode") == THREAD_MODE_FINE,
    centroidLat = optDouble("centroidLat"),
    centroidLon = optDouble("centroidLon"),
    gridDegrees = optDouble("gridDegrees"),
    spread = optInt("spread"),
    // Null in the top bucket, where the true statement is "further than
    // the last edge" and any number would be one nobody made.
    spreadMaxMeters = if (isNull("spreadMaxMeters")) null else optInt("spreadMaxMeters"),
    totalMeters = optLong("totalMeters"),
    payMinor = if (isNull("payMinor")) null else optLong("payMinor"),
    currency = if (isNull("currency")) null else optString("currency").takeIf { it.isNotEmpty() },
    label = if (isNull("label")) null else optString("label").takeIf { it.isNotEmpty() },
    notAfter = optLong("notAfter"),
    issuerHex = optString("issuerHex"),
    ok = optBoolean("ok"),
    reason = optString("reason").takeIf { it.isNotEmpty() && it != "null" }
)

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

    val staticSeconds = optDouble("seconds")
    // The router already searched under the live metric; `adjustedSeconds`
    // is that path's total. Showing the static number next to a jam drawn
    // on the map would be the app contradicting itself, so the live total
    // *is* the duration and the static one is kept beside it.
    val adjusted = if (isNull("adjustedSeconds")) staticSeconds else optDouble("adjustedSeconds", staticSeconds)

    return Route(
        seconds = adjusted,
        staticSeconds = staticSeconds,
        liveSegments = optInt("liveApplied"),
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
