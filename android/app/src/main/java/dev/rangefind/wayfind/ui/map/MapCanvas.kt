package dev.rangefind.wayfind.ui.map

import android.content.Context
import android.graphics.RectF
import android.Manifest
import android.annotation.SuppressLint
import android.content.pm.PackageManager
import android.location.Location
import androidx.core.content.ContextCompat
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberUpdatedState
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.toArgb
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.viewinterop.AndroidView
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.compose.LocalLifecycleOwner
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.engine.RouteJunction
import dev.rangefind.wayfind.ui.SheetMode
import dev.rangefind.wayfind.ui.formatDuration
import dev.rangefind.wayfind.ui.formatEtaDelta
import dev.rangefind.wayfind.ui.UiState
import dev.rangefind.wayfind.ui.theme.MapPalette
import org.maplibre.android.MapLibre
import org.maplibre.android.camera.CameraPosition
import org.maplibre.android.camera.CameraUpdateFactory
import org.maplibre.android.geometry.LatLng
import org.maplibre.android.geometry.LatLngBounds
import org.maplibre.android.location.LocationComponentActivationOptions
import org.maplibre.android.location.LocationComponentOptions
import org.maplibre.android.location.modes.CameraMode
import org.maplibre.android.location.modes.RenderMode
import org.maplibre.android.maps.MapLibreMap
import org.maplibre.android.maps.MapView
import org.maplibre.android.maps.Style
import org.maplibre.android.style.layers.CircleLayer
import org.maplibre.android.style.layers.LineLayer
import org.maplibre.android.style.layers.Property
import org.maplibre.android.style.layers.PropertyFactory
import org.maplibre.android.style.layers.SymbolLayer
import org.maplibre.android.style.sources.GeoJsonSource
import org.maplibre.geojson.Feature
import org.maplibre.geojson.FeatureCollection
import org.maplibre.geojson.LineString
import org.maplibre.geojson.Point

const val STYLE_DAY = "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
const val STYLE_NIGHT = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"

private const val SRC_ALT = "rf-src-alt"
private const val SRC_ROUTE = "rf-src-route"
private const val SRC_TRAVELED = "rf-src-traveled"
private const val SRC_RESULTS = "rf-src-results"
private const val SRC_DESTINATION = "rf-src-destination"
private const val SRC_SIGNAL = "rf-src-signal"
private const val SRC_STOP = "rf-src-stop"
private const val SRC_CROSSING = "rf-src-crossing"
private const val SRC_PUCK = "rf-src-puck"
private const val SRC_ORIGIN = "rf-src-origin"

private const val LYR_RESULTS = "rf-lyr-results"
private const val LYR_ALT = "rf-lyr-alt"

/** Up to three candidates: the fastest plus two alternates. */
private val SRC_LABEL = listOf("rf-src-label-0", "rf-src-label-1", "rf-src-label-2")
private val LYR_LABEL = listOf("rf-lyr-label-0", "rf-lyr-label-1", "rf-lyr-label-2")
private val IMG_LABEL = listOf("rf-label-0", "rf-label-1", "rf-label-2")

/** Rangefind ink — the glyph punched out of the amber destination marker. */
private const val INK = 0xFF14161D.toInt()

/**
 * How much of the viewport sits above the vehicle while following, as a
 * fraction of its height. Driving is a forward-looking activity: what is
 * behind the car has already been dealt with.
 *
 * Camera padding centres the target in what is left, so this puts the vehicle
 * two thirds of the way down — low enough that the view is mostly road ahead,
 * high enough to stay clear of the speed readout and the road-name chip, which
 * sit above the sheet at the bottom of the screen.
 */
private const val FOLLOW_TOP_PADDING = 0.32

@Composable
fun MapCanvas(
    state: UiState,
    darkTheme: Boolean,
    palette: MapPalette,
    bottomInsetPx: Int,
    startInsetPx: Int = 0,
    onCenterChanged: (LatLon) -> Unit,
    onResultTapped: (Int) -> Unit,
    onRouteTapped: (Int) -> Unit,
    onLongPress: (LatLon) -> Unit,
    modifier: Modifier = Modifier
) {
    val density = LocalDensity.current.density
    // Route bubbles are drawn into bitmaps rather than composed, so the map
    // layer needs a context of its own to resolve their labels.
    val appContext = LocalContext.current.applicationContext
    val holder = remember { MapHolder() }

    // The click listener is bound once; without this it would compare taps
    // against whatever the selection was at bind time.
    val currentState by rememberUpdatedState(state)
    val currentOnCenter by rememberUpdatedState(onCenterChanged)
    val currentOnResult by rememberUpdatedState(onResultTapped)
    val currentOnRoute by rememberUpdatedState(onRouteTapped)
    val currentOnLongPress by rememberUpdatedState(onLongPress)

    val mapView = rememberMapViewWithLifecycle()

    AndroidView(
        modifier = modifier,
        factory = { mapView },
        update = { }
    )

    // Style (re)load: day/night swap rebuilds every source and layer.
    LaunchedEffect(mapView, darkTheme) {
        mapView.getMapAsync { map ->
            MapSnapshotter.attach(map)
            holder.map = map
            map.uiSettings.isAttributionEnabled = false
            map.uiSettings.isLogoEnabled = false
            map.uiSettings.isCompassEnabled = false
            map.uiSettings.isRotateGesturesEnabled = true
            map.uiSettings.isTiltGesturesEnabled = true

            map.setStyle(if (darkTheme) STYLE_NIGHT else STYLE_DAY) { style ->
                holder.style = style
                installLayers(style, palette, density)
                // The vehicle belongs to MapLibre once the style is up; it
                // draws and animates it, and needs the style to attach to.
                holder.attachVehicle(appContext, palette, darkTheme)
                holder.ready = true
                // Replay whatever the app knows now: a fix or a route that
                // arrived while the style was still loading would otherwise
                // never be drawn, since its state key has already settled.
                holder.flush()
            }

            if (!holder.listenersBound) {
                holder.listenersBound = true
                map.addOnCameraIdleListener {
                    val target = map.cameraPosition.target ?: return@addOnCameraIdleListener
                    currentOnCenter(LatLon(target.latitude, target.longitude))
                }
                map.addOnMapClickListener { point ->
                    val screen = map.projection.toScreenLocation(point)
                    fun boxOf(dp: Float) = RectF(
                        screen.x - dp * density, screen.y - dp * density,
                        screen.x + dp * density, screen.y + dp * density
                    )
                    fun routeOf(feature: org.maplibre.geojson.Feature) =
                        runCatching { feature.getNumberProperty("route")?.toInt() }.getOrNull()

                    // Duration bubbles are already finger-sized, so they get a
                    // tight box: a generous one spans all three and the first
                    // hit is always the active route, which reads as a dead tap.
                    val labelHits = map.queryRenderedFeatures(boxOf(6f), *LYR_LABEL.toTypedArray())
                        .mapNotNull(::routeOf)
                    // A route line is only a few pixels wide and deserves the
                    // slop a finger actually has.
                    val lineHits = map.queryRenderedFeatures(boxOf(22f), LYR_ALT)
                        .mapNotNull(::routeOf)

                    val picked = (labelHits + lineHits).let { hits ->
                        hits.firstOrNull { it != currentState.activeRouteIndex } ?: hits.firstOrNull()
                    }
                    if (picked != null) {
                        currentOnRoute(picked)
                        return@addOnMapClickListener true
                    }

                    val index = map.queryRenderedFeatures(boxOf(18f), LYR_RESULTS)
                        .firstOrNull()?.getNumberProperty("index")?.toInt()
                    if (index != null) {
                        currentOnResult(index)
                        true
                    } else {
                        false
                    }
                }
                map.addOnMapLongClickListener { point ->
                    currentOnLongPress(LatLon(point.latitude, point.longitude))
                    true
                }
            }
        }
    }

    // Data pushes.
    LaunchedEffect(
        state.results,
        state.selected,
        state.routes,
        state.activeRouteIndex,
        state.nav,
        state.userLocation,
        state.sheet
    ) {
        holder.latest = state
        holder.palette = palette
        holder.density = density
        holder.context = appContext
        holder.flush()
    }

    // Camera: frame every candidate, so the overview shows the whole choice.
    // Not keyed on the active index — switching alternates must not re-frame,
    // or comparing them turns into the map lurching under your thumb. It *is*
    // keyed on the sheet height, because routes arrive before the directions
    // sheet has laid out, and framing against the old height buries the line
    // under it.
    LaunchedEffect(state.routes, bottomInsetPx) {
        if (state.routes == null) return@LaunchedEffect
        if (state.sheet == SheetMode.Navigating) return@LaunchedEffect
        val map = holder.map ?: return@LaunchedEffect
        val points = state.allRoutes.flatMap { it.geometry }
        if (points.size < 2) return@LaunchedEffect
        val bounds = LatLngBounds.Builder()
            .includes(points.map { LatLng(it.lat, it.lon) })
            .build()
        val side = (48 * density).toInt()
        val top = (110 * density).toInt()
        // On wide layouts the panel floats over the map's start edge, so the
        // route has to be framed clear of it rather than centred behind it.
        val start = side + startInsetPx
        // Keep the whole line clear of the sheet, but never let padding exceed
        // the viewport or MapLibre rejects the camera update outright.
        val bottom = (bottomInsetPx + (24 * density).toInt())
            .coerceIn(1, (mapView.height - top - 1).coerceAtLeast(1))
        runCatching {
            // Fitting a very short route would otherwise slam the camera to
            // max zoom, where the map reads as an abstract stripe. Clamp it so
            // a 40 m hop still shows its neighborhood.
            val fitted = map.getCameraForLatLngBounds(bounds, intArrayOf(start, top, side, bottom))
            val update = if (fitted != null) {
                CameraUpdateFactory.newCameraPosition(
                    CameraPosition.Builder(fitted).zoom(minOf(fitted.zoom, 17.0)).tilt(0.0).build()
                )
            } else {
                CameraUpdateFactory.newLatLngBounds(bounds, start, top, side, bottom)
            }
            map.animateCamera(update, 700)
        }
    }

    // Follow camera and puck, driven by the frame clock rather than by the
    // fix rate. Location lands about once a second: easing the camera over
    // 900 ms while the puck's map position is set discretely made the map
    // glide and the arrow teleport. Both now advance together every frame.
    // Following is handed to MapLibre's own location component rather than
    // driven from here. A per-frame moveCamera from the Compose frame clock
    // pushes updates at whatever rate the composition runs, independent of
    // the map's render loop, which is the shape of stutter the renderer's own
    // docs warn about — frames queued faster than they can be presented. The
    // component animates the vehicle and the camera on its own schedule, so
    // the app's job shrinks to saying where the vehicle is.
    LaunchedEffect(state.sheet == SheetMode.Navigating) {
        holder.setFollowing(state.sheet == SheetMode.Navigating, mapView.height)
    }

    // One update per fix. MapLibre interpolates between them, which is what
    // the frame loop was trying to do by hand.
    LaunchedEffect(state.nav?.position, state.nav?.bearing) {
        val nav = state.nav ?: return@LaunchedEffect
        holder.pushVehicle(nav.position, nav.bearing, nav.speedMps)
    }

    // Camera: center a selected place when there is no route on screen.
    LaunchedEffect(state.selected, state.routes) {
        val place = state.selected ?: return@LaunchedEffect
        if (state.routes != null || state.sheet == SheetMode.Navigating) return@LaunchedEffect
        val map = holder.map ?: return@LaunchedEffect
        map.animateCamera(
            CameraUpdateFactory.newCameraPosition(
                CameraPosition.Builder()
                    .target(LatLng(place.lat, place.lon))
                    .zoom(maxOf(map.cameraPosition.zoom, 15.5))
                    .tilt(0.0)
                    .build()
            ),
            600
        )
    }

    // Camera: explicit recenter request.
    LaunchedEffect(state.recenterTick) {
        if (state.recenterTick == 0) return@LaunchedEffect
        val here = state.userLocation ?: return@LaunchedEffect
        val map = holder.map ?: return@LaunchedEffect
        map.animateCamera(
            CameraUpdateFactory.newCameraPosition(
                CameraPosition.Builder()
                    .target(LatLng(here.lat, here.lon))
                    .zoom(maxOf(map.cameraPosition.zoom, 16.0))
                    .tilt(0.0)
                    .bearing(0.0)
                    .build()
            ),
            600
        )
    }

    // Camera: first fix drops the user into their own neighborhood.
    LaunchedEffect(state.userLocation != null) {
        val here = state.userLocation ?: return@LaunchedEffect
        val map = holder.map ?: return@LaunchedEffect
        if (state.selected != null || state.routes != null) return@LaunchedEffect
        map.animateCamera(
            CameraUpdateFactory.newCameraPosition(
                CameraPosition.Builder()
                    .target(LatLng(here.lat, here.lon))
                    .zoom(14.5)
                    .build()
            ),
            800
        )
    }
}

/**
 * The live map, for anything outside the composition that needs to see what
 * the driver saw. Only the rendered map goes through here: everything drawn
 * over it — the maneuver banner, the speed, the lanes — is already written
 * into the trace as state, and a picture of it would add nothing.
 */
object MapSnapshotter {
    @Volatile
    private var map: MapLibreMap? = null

    internal fun attach(instance: MapLibreMap?) {
        map = instance
    }

    /**
     * Renders the map to a bitmap. MapLibre draws through a GL surface that
     * neither a view-tree capture nor PixelCopy over the window reliably
     * reaches — both come back with a blank hole where the roads were, which
     * is the one part worth having.
     */
    fun snapshot(onReady: (android.graphics.Bitmap?) -> Unit) {
        val instance = map
        if (instance == null) {
            onReady(null)
            return
        }
        runCatching { instance.snapshot { bitmap -> onReady(bitmap) } }
            .onFailure { onReady(null) }
    }
}

private class MapHolder {
    var map: MapLibreMap? = null
    var style: Style? = null
    var ready = false
    var listenersBound = false

    private var vehicleReady = false
    private var following = false
    private var viewportHeightPx = 0

    fun resetFollow() {
        runCatching { map?.locationComponent?.cameraMode = CameraMode.NONE }
    }

    /**
     * Hands the vehicle to MapLibre's location component.
     *
     * Activated with no location engine of its own: the positions come from
     * the app's motion model, which knows about the route, the mode and which
     * fixes to believe. What the component contributes is the drawing and the
     * camera — animated natively, in step with the renderer.
     *
     * Called again on every style load, because a new style is a new set of
     * layers and the component's own layers went with the old one. Whatever
     * the drive was doing is reapplied afterwards rather than reset.
     */
    @SuppressLint("MissingPermission")
    fun attachVehicle(context: Context, palette: MapPalette, night: Boolean) {
        val map = map ?: return
        val style = style ?: return
        val granted = ContextCompat.checkSelfPermission(
            context,
            Manifest.permission.ACCESS_FINE_LOCATION
        ) == PackageManager.PERMISSION_GRANTED
        if (!granted) return
        runCatching {
            // No tint: the puck is a chevron with a white outline over a
            // shadow, and a tint colour would flatten all three into one.
            val options = LocationComponentOptions.builder(context)
                .gpsDrawable(
                    if (night) R.drawable.wayfind_nav_puck_night
                    else R.drawable.wayfind_nav_puck
                )
                .accuracyColor(palette.puckHalo.toArgb())
                .accuracyAlpha(0.28f)
                // A pan mid-drive moves the camera's focus rather than
                // dropping the follow, so a glance up the road doesn't end
                // with the map stranded behind the car.
                .trackingGesturesManagement(true)
                .build()
            map.locationComponent.activateLocationComponent(
                LocationComponentActivationOptions.builder(context, style)
                    .locationComponentOptions(options)
                    .useDefaultLocationEngine(false)
                    .build()
            )
            vehicleReady = true
            applyFollow()
        }
    }

    /** Where the vehicle is, once per fix; the component animates between. */
    @SuppressLint("MissingPermission")
    fun pushVehicle(position: LatLon, bearing: Double, speedMps: Double) {
        if (!vehicleReady) return
        val map = map ?: return
        runCatching {
            map.locationComponent.forceLocationUpdate(
                Location("wayfind").apply {
                    latitude = position.lat
                    longitude = position.lon
                    this.bearing = bearing.toFloat()
                    speed = speedMps.toFloat()
                    time = System.currentTimeMillis()
                }
            )
            // Zoom tightens as speed drops, so the view holds roughly the same
            // stretch of road ahead whatever the pace.
            val speedKmh = speedMps * 3.6
            map.locationComponent.zoomWhileTracking(
                when {
                    speedKmh < 25 -> 17.6
                    speedKmh < 65 -> 16.9
                    else -> 16.2
                }
            )
        }
    }

    /** Turns following on for a drive and off again afterwards. */
    fun setFollowing(following: Boolean, viewportHeightPx: Int) {
        this.following = following
        if (viewportHeightPx > 0) this.viewportHeightPx = viewportHeightPx
        applyFollow()
    }

    @SuppressLint("MissingPermission")
    private fun applyFollow() {
        if (!vehicleReady) return
        val map = map ?: return
        runCatching {
            map.locationComponent.isLocationComponentEnabled = following
            if (following) {
                // Heading-up, tilted, with the vehicle low in frame so the
                // view is mostly the road ahead rather than the road behind.
                map.locationComponent.renderMode = RenderMode.GPS
                map.locationComponent.cameraMode = CameraMode.TRACKING_GPS
                map.locationComponent.tiltWhileTracking(58.0)
                // Padding shrinks the region the camera centres in, so a tall
                // top padding is what puts the car near the bottom edge.
                if (viewportHeightPx > 0) {
                    map.locationComponent.paddingWhileTracking(
                        doubleArrayOf(0.0, viewportHeightPx * FOLLOW_TOP_PADDING, 0.0, 0.0)
                    )
                }
            } else {
                map.locationComponent.cameraMode = CameraMode.NONE
                map.locationComponent.cancelPaddingWhileTrackingAnimation()
                runCatching { map.setPadding(0, 0, 0, 0) }
            }
        }
    }

    /** Most recent app state, replayed whenever a new style finishes loading. */
    var latest: UiState? = null
    var palette: MapPalette? = null
    var density: Float = 1f
    var context: Context? = null

    fun flush() {
        val state = latest ?: return
        val palette = palette ?: return
        pushAll(state, palette)
    }

    fun pushAll(state: UiState, palette: MapPalette) {
        val style = style ?: return
        val context = context ?: return
        if (!ready || !style.isFullyLoaded) return

        val navigating = state.sheet == SheetMode.Navigating
        val active = state.activeRoute

        // Alternatives sit under the active line so the choice reads clearly,
        // and each carries its index so tapping one selects it.
        // While driving, only the alternates the car could still take stay on
        // screen; the rest have been committed away from.
        val shown: Set<Int> = when {
            navigating -> state.nav?.alternatives?.map { it.index }?.toSet() ?: emptySet()
            state.routes != null -> state.allRoutes.indices.filter { it != state.activeRouteIndex }.toSet()
            else -> emptySet()
        }
        val altFeatures = state.allRoutes.mapIndexedNotNull { index, candidate ->
            if (index !in shown || candidate.geometry.size < 2) null
            else Feature.fromGeometry(
                LineString.fromLngLats(candidate.geometry.map { Point.fromLngLat(it.lon, it.lat) })
            ).apply { addNumberProperty("route", index) }
        }
        style.setSource(SRC_ALT, FeatureCollection.fromFeatures(altFeatures))

        if (navigating && state.nav != null) {
            style.setLine(SRC_TRAVELED, listOf(state.nav.traveled))
            style.setLine(SRC_ROUTE, listOf(state.nav.ahead))
        } else {
            style.setLine(SRC_TRAVELED, emptyList())
            style.setLine(SRC_ROUTE, active?.let { listOf(it.geometry) } ?: emptyList())
        }

        // Result dots disappear once a route is on screen: the map's job
        // changes from "choose a place" to "follow this line".
        val results = if (state.routes != null || navigating) emptyList() else state.results
        style.setSource(
            SRC_RESULTS,
            FeatureCollection.fromFeatures(
                results.mapIndexed { index, place ->
                    Feature.fromGeometry(Point.fromLngLat(place.lon, place.lat)).apply {
                        addNumberProperty("index", index)
                    }
                }
            )
        )

        style.setSource(
            SRC_DESTINATION,
            FeatureCollection.fromFeatures(
                listOfNotNull(
                    state.selected?.let {
                        Feature.fromGeometry(Point.fromLngLat(it.lon, it.lat))
                    }
                )
            )
        )

        // Road signs only during navigation, and only ahead of the driver.
        val junctions = if (navigating && active != null) {
            val passed = state.nav?.let { active.distanceMeters - it.remainingMeters } ?: 0.0
            active.junctions.filter { it.atMeters >= passed - 20 }
        } else {
            emptyList()
        }
        style.setSource(SRC_SIGNAL, junctions.filter { it.kind == 1 }.toPoints())
        style.setSource(SRC_STOP, junctions.filter { it.kind == 2 }.toPoints())
        style.setSource(SRC_CROSSING, junctions.filter { it.kind == 3 || it.kind == 4 || it.kind == 5 }.toPoints())

        // Trip start marker, and a duration bubble on each candidate — the
        // overview should answer "which one, and how long" without the sheet.
        val originPoint = if (navigating) null else active?.geometry?.firstOrNull()
        style.setSource(
            SRC_ORIGIN,
            FeatureCollection.fromFeatures(
                listOfNotNull(originPoint?.let { Feature.fromGeometry(Point.fromLngLat(it.lon, it.lat)) })
            )
        )

        val deltas = state.nav?.alternatives?.associateBy { it.index } ?: emptyMap()
        SRC_LABEL.indices.forEach { i ->
            val candidate = state.allRoutes.getOrNull(i)
            // Driving: label the alternates only — the active route's ETA is
            // already the largest number on screen, in the footer.
            val visible = if (navigating) i in shown else state.routes != null
            if (candidate == null || !visible || candidate.geometry.size < 2) {
                style.setSource(SRC_LABEL[i], FeatureCollection.fromFeatures(emptyList()))
                return@forEach
            }
            val isActive = !navigating && i == state.activeRouteIndex
            style.addImage(
                IMG_LABEL[i],
                MapIcons.durationLabel(
                    text = if (navigating) {
                        formatEtaDelta(context, deltas[i]?.deltaSeconds ?: 0.0)
                    } else {
                        formatDuration(context, candidate.seconds)
                    },
                    fill = if (isActive) palette.routeLine.toArgb() else 0xFFFFFFFF.toInt(),
                    textColor = if (isActive) 0xFFFAF9F6.toInt() else INK,
                    outline = if (isActive) palette.routeCasing.toArgb() else 0x1F000000,
                    density = density
                )
            )
            // Parked: stagger the anchors so three bubbles on a shared corridor
            // do not stack. Driving: put the bubble up the road ahead of the
            // car, where the driver is looking.
            val fraction = deltas[i]?.let { alt ->
                val total = candidate.distanceMeters
                if (total <= 0) 0.5 else ((alt.alongMeters + 350.0) / total).coerceIn(0.05, 0.95)
            } ?: (0.32 + 0.17 * i)
            val at = ((candidate.geometry.size - 1) * fraction).toInt()
                .coerceIn(0, candidate.geometry.size - 1)
            val anchor = candidate.geometry[at]
            style.setSource(
                SRC_LABEL[i],
                FeatureCollection.fromFeatures(
                    listOf(
                        Feature.fromGeometry(Point.fromLngLat(anchor.lon, anchor.lat))
                            .apply { addNumberProperty("route", i) }
                    )
                )
            )
        }

        // Parked: a plain dot from this layer. Driving: the vehicle belongs to
        // MapLibre's location component, so this source empties and gets out
        // of its way.
        val browsing = if (navigating) null else state.userLocation
        style.setSource(
            SRC_PUCK,
            FeatureCollection.fromFeatures(
                listOfNotNull(browsing?.let { Feature.fromGeometry(Point.fromLngLat(it.lon, it.lat)) })
            )
        )
    }
}

private fun List<RouteJunction>.toPoints() =
    FeatureCollection.fromFeatures(map { Feature.fromGeometry(Point.fromLngLat(it.lon, it.lat)) })

private fun Style.setSource(id: String, collection: FeatureCollection) {
    (getSourceAs<GeoJsonSource>(id))?.setGeoJson(collection)
}

private fun Style.setLine(id: String, lines: List<List<LatLon>>) {
    val features = lines
        .filter { it.size >= 2 }
        .map { line -> Feature.fromGeometry(LineString.fromLngLats(line.map { Point.fromLngLat(it.lon, it.lat) })) }
    setSource(id, FeatureCollection.fromFeatures(features))
}

private fun installLayers(style: Style, palette: MapPalette, density: Float) {
    (listOf(
        SRC_ALT, SRC_ROUTE, SRC_TRAVELED, SRC_RESULTS, SRC_DESTINATION,
        SRC_SIGNAL, SRC_STOP, SRC_CROSSING, SRC_PUCK, SRC_ORIGIN
    ) + SRC_LABEL).forEach { id ->
        if (style.getSource(id) == null) {
            style.addSource(GeoJsonSource(id, FeatureCollection.fromFeatures(emptyList())))
        }
    }

    style.addImage(MapIcons.DESTINATION, MapIcons.pin(palette.destination.toArgb(), INK, density))
    style.addImage(MapIcons.SIGNAL, MapIcons.signal(density))
    style.addImage(MapIcons.STOP, MapIcons.stop(density))
    style.addImage(MapIcons.CROSSING, MapIcons.crossing(density))

    fun line(id: String, source: String, color: Int, width: Float, opacity: Float = 1f) =
        LineLayer(id, source).withProperties(
            PropertyFactory.lineColor(color),
            PropertyFactory.lineWidth(width),
            PropertyFactory.lineOpacity(opacity),
            PropertyFactory.lineCap(Property.LINE_CAP_ROUND),
            PropertyFactory.lineJoin(Property.LINE_JOIN_ROUND)
        )

    style.addLayer(line("rf-lyr-alt-casing", SRC_ALT, palette.routeAlternateCasing.toArgb(), 11f, 0.9f))
    style.addLayer(line(LYR_ALT, SRC_ALT, palette.routeAlternate.toArgb(), 6.5f, 0.9f))
    style.addLayer(line("rf-lyr-traveled", SRC_TRAVELED, palette.routeTraveled.toArgb(), 9f, 0.85f))
    style.addLayer(line("rf-lyr-casing", SRC_ROUTE, palette.routeCasing.toArgb(), 14f))
    style.addLayer(line("rf-lyr-route", SRC_ROUTE, palette.routeLine.toArgb(), 8.5f))

    style.addLayer(
        CircleLayer(LYR_RESULTS, SRC_RESULTS).withProperties(
            PropertyFactory.circleRadius(7.5f),
            PropertyFactory.circleColor(palette.routeLine.toArgb()),
            PropertyFactory.circleStrokeWidth(3f),
            PropertyFactory.circleStrokeColor(0xFFFFFFFF.toInt()),
            PropertyFactory.circleOpacity(0.95f)
        )
    )

    listOf(
        Triple("rf-lyr-signal", SRC_SIGNAL, MapIcons.SIGNAL),
        Triple("rf-lyr-stop", SRC_STOP, MapIcons.STOP),
        Triple("rf-lyr-crossing", SRC_CROSSING, MapIcons.CROSSING)
    ).forEach { (layerId, sourceId, icon) ->
        style.addLayer(
            SymbolLayer(layerId, sourceId).withProperties(
                PropertyFactory.iconImage(icon),
                PropertyFactory.iconAllowOverlap(true),
                PropertyFactory.iconIgnorePlacement(true),
                PropertyFactory.iconSize(0.85f)
            )
        )
    }

    style.addLayer(
        SymbolLayer("rf-lyr-destination", SRC_DESTINATION).withProperties(
            PropertyFactory.iconImage(MapIcons.DESTINATION),
            PropertyFactory.iconAllowOverlap(true),
            PropertyFactory.iconIgnorePlacement(true),
            PropertyFactory.iconAnchor(Property.ICON_ANCHOR_BOTTOM)
        )
    )

    // Trip start, so an overview reads as a journey rather than a stray line.
    style.addLayer(
        CircleLayer("rf-lyr-origin", SRC_ORIGIN).withProperties(
            PropertyFactory.circleRadius(7f),
            PropertyFactory.circleColor(0xFFFAF9F6.toInt()),
            PropertyFactory.circleStrokeWidth(4f),
            PropertyFactory.circleStrokeColor(palette.routeCasing.toArgb())
        )
    )

    SRC_LABEL.indices.forEach { i ->
        style.addLayer(
            SymbolLayer(LYR_LABEL[i], SRC_LABEL[i]).withProperties(
                PropertyFactory.iconImage(IMG_LABEL[i]),
                PropertyFactory.iconAllowOverlap(true),
                PropertyFactory.iconIgnorePlacement(true)
            )
        )
    }

    style.addLayer(
        CircleLayer("rf-lyr-puck-halo", SRC_PUCK).withProperties(
            PropertyFactory.circleRadius(22f),
            PropertyFactory.circleColor(palette.puck.toArgb()),
            PropertyFactory.circleOpacity(0.18f)
        )
    )
    style.addLayer(
        CircleLayer("rf-lyr-puck", SRC_PUCK).withProperties(
            PropertyFactory.circleRadius(8f),
            PropertyFactory.circleColor(palette.puck.toArgb()),
            PropertyFactory.circleStrokeWidth(3.5f),
            PropertyFactory.circleStrokeColor(0xFFFFFFFF.toInt())
        )
    )
}

@Composable
private fun rememberMapViewWithLifecycle(): MapView {
    val context = LocalContext.current
    val mapView = remember {
        MapLibre.getInstance(context)
        MapView(context).apply { onCreate(null) }
    }

    val lifecycleOwner = LocalLifecycleOwner.current
    DisposableEffect(lifecycleOwner, mapView) {
        val observer = LifecycleEventObserver { _, event ->
            when (event) {
                Lifecycle.Event.ON_START -> mapView.onStart()
                Lifecycle.Event.ON_RESUME -> mapView.onResume()
                Lifecycle.Event.ON_PAUSE -> mapView.onPause()
                Lifecycle.Event.ON_STOP -> mapView.onStop()
                Lifecycle.Event.ON_DESTROY -> mapView.onDestroy()
                else -> Unit
            }
        }
        lifecycleOwner.lifecycle.addObserver(observer)
        onDispose { lifecycleOwner.lifecycle.removeObserver(observer) }
    }
    return mapView
}
