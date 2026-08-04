package dev.rangefind.wayfind.ui.map

import android.graphics.RectF
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
import kotlin.math.cos
import kotlin.math.sin

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
private const val SRC_NAV = "rf-src-nav"

private const val LYR_RESULTS = "rf-lyr-results"
private const val LYR_ALT = "rf-lyr-alt"
private const val LYR_NAV = "rf-lyr-nav"
private const val IMG_NAV = "rf-nav-arrow"

/** Up to three candidates: the fastest plus two alternates. */
private val SRC_LABEL = listOf("rf-src-label-0", "rf-src-label-1", "rf-src-label-2")
private val LYR_LABEL = listOf("rf-lyr-label-0", "rf-lyr-label-1", "rf-lyr-label-2")
private val IMG_LABEL = listOf("rf-label-0", "rf-label-1", "rf-label-2")

/** Rangefind ink — the glyph punched out of the amber destination marker. */
private const val INK = 0xFF14161D.toInt()

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
            holder.map = map
            map.uiSettings.isAttributionEnabled = false
            map.uiSettings.isLogoEnabled = false
            map.uiSettings.isCompassEnabled = false
            map.uiSettings.isRotateGesturesEnabled = true
            map.uiSettings.isTiltGesturesEnabled = true

            map.setStyle(if (darkTheme) STYLE_NIGHT else STYLE_DAY) { style ->
                holder.style = style
                installLayers(style, palette, density)
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

    // Camera: follow the driver.
    LaunchedEffect(state.nav?.position, state.nav?.bearing, state.sheet) {
        if (state.sheet != SheetMode.Navigating) return@LaunchedEffect
        val nav = state.nav ?: return@LaunchedEffect
        val map = holder.map ?: return@LaunchedEffect

        // Google-style framing: zoom tightens as speed drops, and the camera
        // targets a point ahead of the puck so the driver sits low on screen
        // with the road ahead filling the view.
        val speedKmh = nav.speedMps * 3.6
        val zoom = when {
            speedKmh < 25 -> 18.1
            speedKmh < 65 -> 17.3
            else -> 16.5
        }
        // Targeting a point ahead of the puck pushes it toward the lower third
        // of the screen, so the road being driven into fills the view.
        val leadMeters = when {
            speedKmh < 25 -> 85.0
            speedKmh < 65 -> 150.0
            else -> 240.0
        }
        val target = advance(nav.position, nav.bearing, leadMeters)

        map.easeCamera(
            CameraUpdateFactory.newCameraPosition(
                CameraPosition.Builder()
                    .target(LatLng(target.lat, target.lon))
                    .zoom(zoom)
                    .bearing(nav.bearing)
                    .tilt(58.0)
                    .build()
            ),
            900
        )
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

/** Moves a point [meters] along [bearing] — used to bias the follow camera. */
private fun advance(from: LatLon, bearing: Double, meters: Double): LatLon {
    val radians = Math.toRadians(bearing)
    val dLat = (meters * cos(radians)) / 111_320.0
    val dLon = (meters * sin(radians)) / (111_320.0 * cos(Math.toRadians(from.lat)).coerceAtLeast(0.01))
    return LatLon(from.lat + dLat, from.lon + dLon)
}

private class MapHolder {
    var map: MapLibreMap? = null
    var style: Style? = null
    var ready = false
    var listenersBound = false

    /** Most recent app state, replayed whenever a new style finishes loading. */
    var latest: UiState? = null
    var palette: MapPalette? = null
    var density: Float = 1f

    fun flush() {
        val state = latest ?: return
        val palette = palette ?: return
        pushAll(state, palette)
    }

    fun pushAll(state: UiState, palette: MapPalette) {
        val style = style ?: return
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
                    text = if (navigating) formatEtaDelta(deltas[i]?.deltaSeconds ?: 0.0)
                    else formatDuration(candidate.seconds),
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

        // Parked: a plain dot. Driving: a chevron aimed down the road.
        val browsing = if (navigating) null else state.userLocation
        style.setSource(
            SRC_PUCK,
            FeatureCollection.fromFeatures(
                listOfNotNull(browsing?.let { Feature.fromGeometry(Point.fromLngLat(it.lon, it.lat)) })
            )
        )
        val driving = if (navigating) state.nav else null
        style.setSource(
            SRC_NAV,
            FeatureCollection.fromFeatures(
                listOfNotNull(
                    driving?.let { Feature.fromGeometry(Point.fromLngLat(it.position.lon, it.position.lat)) }
                )
            )
        )
        if (driving != null) {
            style.getLayerAs<SymbolLayer>(LYR_NAV)
                ?.setProperties(PropertyFactory.iconRotate(driving.bearing.toFloat()))
        }
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
        SRC_SIGNAL, SRC_STOP, SRC_CROSSING, SRC_PUCK, SRC_ORIGIN, SRC_NAV
    ) + SRC_LABEL).forEach { id ->
        if (style.getSource(id) == null) {
            style.addSource(GeoJsonSource(id, FeatureCollection.fromFeatures(emptyList())))
        }
    }

    style.addImage(MapIcons.DESTINATION, MapIcons.pin(palette.destination.toArgb(), INK, density))
    style.addImage(IMG_NAV, MapIcons.navArrow(palette.puck.toArgb(), 0xFFFFFFFF.toInt(), density))
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

    // While driving the puck becomes a chevron pointing where the car is
    // heading. Rotation aligns to the map, so it stays truthful even if the
    // driver spins the view away from the direction of travel.
    style.addLayer(
        SymbolLayer(LYR_NAV, SRC_NAV).withProperties(
            PropertyFactory.iconImage(IMG_NAV),
            PropertyFactory.iconAllowOverlap(true),
            PropertyFactory.iconIgnorePlacement(true),
            PropertyFactory.iconRotationAlignment(Property.ICON_ROTATION_ALIGNMENT_MAP),
            PropertyFactory.iconPitchAlignment(Property.ICON_PITCH_ALIGNMENT_MAP)
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
