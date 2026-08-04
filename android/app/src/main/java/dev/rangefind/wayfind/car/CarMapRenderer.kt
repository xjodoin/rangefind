package dev.rangefind.wayfind.car

import android.content.Context
import android.graphics.Bitmap
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.Path
import android.graphics.Rect
import android.graphics.RectF
import android.os.Handler
import android.os.HandlerThread
import androidx.car.app.SurfaceCallback
import androidx.car.app.SurfaceContainer
import dev.rangefind.wayfind.engine.LatLon
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.cos
import kotlin.math.floor
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.roundToInt
import kotlin.math.sin

/**
 * A real map on the car surface.
 *
 * MapLibre has no renderer that targets an arbitrary Surface, so this composes
 * the view directly: raster basemap tiles underneath, then the route, the
 * junctions along it, the destination and the car. The camera follows the
 * driver, rotates to their heading and tightens with speed, exactly like the
 * phone's follow camera.
 *
 * Head units vary hugely in size, aspect and how much of the display the host
 * covers with its own chrome, so every dimension is derived from the reported
 * visible area rather than the raw surface.
 */
class CarMapRenderer(
    context: Context,
    private val dark: Boolean
) : SurfaceCallback {

    private var container: SurfaceContainer? = null
    private var visible = Rect()

    @Volatile private var target: CarState = CarState()
    private val scheduled = AtomicBoolean(false)

    // One render thread owns the Surface. Tiles land on a pool thread and the
    // car state arrives on main, and two threads inside lockCanvas at once is
    // a crash, not a glitch.
    private val thread = HandlerThread("wayfind-car-map").apply { start() }
    private val handler = Handler(thread.looper)

    // The camera eases toward the fix rather than snapping to it: location
    // arrives about once a second, and a map that jumps once a second reads as
    // broken however correct the geometry is.
    private var camLat = Double.NaN
    private var camLon = Double.NaN
    private var camBearing = 0.0

    private val tiles = CarTileSource(context.cacheDir, dark) { schedule() }

    private val ink = if (dark) Color.parseColor("#14161D") else Color.parseColor("#FAF9F6")
    private val routeAhead = if (dark) Color.parseColor("#35C2AC") else Color.parseColor("#0E6F63")
    private val routeCasing = if (dark) Color.parseColor("#0B4A43") else Color.parseColor("#06413A")
    private val routeBehind = if (dark) Color.parseColor("#5A6A6C") else Color.parseColor("#A8B2AF")
    private val puckColor = if (dark) Color.parseColor("#54A8FF") else Color.parseColor("#1573D6")
    private val marker = Color.parseColor("#FFC940")
    private val chrome = if (dark) Color.parseColor("#21242E") else Color.WHITE
    private val onChrome = if (dark) Color.parseColor("#ECEADA") else Color.parseColor("#14161D")

    override fun onSurfaceAvailable(surfaceContainer: SurfaceContainer) {
        container = surfaceContainer
        if (visible.isEmpty) visible = Rect(0, 0, surfaceContainer.width, surfaceContainer.height)
        schedule()
    }

    override fun onVisibleAreaChanged(visibleArea: Rect) {
        if (!visibleArea.isEmpty) visible = Rect(visibleArea)
        schedule()
    }

    override fun onSurfaceDestroyed(surfaceContainer: SurfaceContainer) {
        container = null
        handler.removeCallbacksAndMessages(null)
    }

    fun shutdown() {
        handler.removeCallbacksAndMessages(null)
        thread.quitSafely()
        tiles.shutdown()
    }

    fun render(next: CarState) {
        target = next
        schedule()
    }

    private fun schedule() {
        if (scheduled.compareAndSet(false, true)) handler.post(frame)
    }

    private val frame = Runnable {
        scheduled.set(false)
        val state = target
        val moving = advanceCamera(state)
        drawFrame(state)
        // Keep animating only while the camera is still catching up, so a
        // parked car costs nothing.
        if (moving) handler.postDelayed({ schedule() }, FRAME_MS)
    }

    /** Eases the camera toward the latest fix; true while still moving. */
    private fun advanceCamera(state: CarState): Boolean {
        val here = state.position ?: state.location ?: return false
        if (camLat.isNaN()) {
            camLat = here.lat
            camLon = here.lon
            camBearing = state.bearing
            return false
        }
        val dLat = here.lat - camLat
        val dLon = here.lon - camLon
        var dBearing = (state.bearing - camBearing + 540.0) % 360.0 - 180.0
        camLat += dLat * EASE
        camLon += dLon * EASE
        camBearing = (camBearing + dBearing * EASE + 360.0) % 360.0
        return kotlin.math.abs(dLat) > 1e-7 ||
            kotlin.math.abs(dLon) > 1e-7 ||
            kotlin.math.abs(dBearing) > 0.4
    }

    private fun drawFrame(state: CarState) {
        val surface = container?.surface ?: return
        if (!surface.isValid) return
        val canvas = runCatching { surface.lockCanvas(null) }.getOrNull() ?: return
        try {
            draw(canvas, state)
        } catch (error: Throwable) {
            // Never let a paint error take the head unit down mid-drive.
        } finally {
            runCatching { surface.unlockCanvasAndPost(canvas) }
        }
    }

    private fun draw(canvas: Canvas, state: CarState) {
        canvas.drawColor(ink)
        val area = if (visible.isEmpty) {
            RectF(0f, 0f, canvas.width.toFloat(), canvas.height.toFloat())
        } else {
            RectF(visible)
        }
        val unit = min(area.width(), area.height()) / 100f
        val paint = Paint(Paint.ANTI_ALIAS_FLAG)

        val fix = state.position ?: state.location
        val center = if (!camLat.isNaN()) LatLon(camLat, camLon) else fix
        if (center == null) {
            drawWaiting(canvas, area, unit, paint)
            drawAttribution(canvas, area, unit, paint)
            return
        }

        // Follow camera: tighter when slow, and the driver sits low on screen
        // with the road ahead filling the view.
        // Zoom bands with speed, like the phone's follow camera: tight in
        // town where turns come fast, wider at road speed where the driver
        // needs to see further ahead.
        val speedKmh = state.speedMps * 3.6
        val zoom = when {
            !state.navigating -> 15.2
            speedKmh < 25 -> 17.0
            speedKmh < 65 -> 16.4
            else -> 15.6
        }
        val bearing = if (state.navigating) camBearing else 0.0
        val anchorY = if (state.navigating) area.top + area.height() * 0.68f else area.centerY()
        val anchorX = area.centerX()

        val originX = mercatorX(center.lon, zoom)
        val originY = mercatorY(center.lat, zoom)
        val project: (LatLon) -> FloatArray = { point ->
            floatArrayOf(
                (mercatorX(point.lon, zoom) - originX).toFloat(),
                (mercatorY(point.lat, zoom) - originY).toFloat()
            )
        }

        canvas.save()
        canvas.clipRect(area)
        canvas.translate(anchorX, anchorY)
        if (bearing != 0.0) canvas.rotate(-bearing.toFloat())

        drawTiles(canvas, area, zoom, originX, originY, bearing)
        drawRoute(canvas, state, unit, paint, project)
        drawMarkers(canvas, state, unit, paint, project)

        canvas.restore()

        drawPuck(canvas, anchorX, anchorY, unit, paint, state)
        drawOverlay(canvas, area, unit, paint, state)
        drawAttribution(canvas, area, unit, paint)
    }

    private fun drawTiles(
        canvas: Canvas,
        area: RectF,
        zoom: Double,
        originX: Double,
        originY: Double,
        bearing: Double
    ) {
        val tileZoom = floor(zoom).toInt().coerceIn(0, 19)
        val scale = (2.0.pow(zoom - tileZoom)).toFloat()
        val tilePx = (TILE_SIZE * scale).toFloat()
        val worldTiles = 1 shl tileZoom

        // A rotated viewport needs a wider net than its own bounds.
        val reach = if (bearing != 0.0) {
            (max(area.width(), area.height()) * 0.75f)
        } else {
            max(area.width(), area.height()) * 0.6f
        }

        val zoomOriginX = originX / 2.0.pow(zoom - tileZoom)
        val zoomOriginY = originY / 2.0.pow(zoom - tileZoom)
        val minTileX = floor((zoomOriginX - reach / scale) / TILE_SIZE).toInt()
        val maxTileX = floor((zoomOriginX + reach / scale) / TILE_SIZE).toInt()
        val minTileY = floor((zoomOriginY - reach / scale) / TILE_SIZE).toInt()
        val maxTileY = floor((zoomOriginY + reach / scale) / TILE_SIZE).toInt()

        val paint = Paint(Paint.FILTER_BITMAP_FLAG or Paint.ANTI_ALIAS_FLAG)
        for (tx in minTileX..maxTileX) {
            for (ty in minTileY..maxTileY) {
                if (ty < 0 || ty >= worldTiles) continue
                val wrapped = ((tx % worldTiles) + worldTiles) % worldTiles
                val bitmap: Bitmap = tiles.bitmap(tileZoom, wrapped, ty) ?: continue
                val left = ((tx * TILE_SIZE - zoomOriginX) * scale).toFloat()
                val top = ((ty * TILE_SIZE - zoomOriginY) * scale).toFloat()
                canvas.drawBitmap(
                    bitmap,
                    null,
                    RectF(left, top, left + tilePx, top + tilePx),
                    paint
                )
            }
        }
    }

    private fun drawRoute(
        canvas: Canvas,
        state: CarState,
        unit: Float,
        paint: Paint,
        project: (LatLon) -> FloatArray
    ) {
        paint.style = Paint.Style.STROKE
        paint.strokeCap = Paint.Cap.ROUND
        paint.strokeJoin = Paint.Join.ROUND

        val ahead = if (state.ahead.size >= 2) state.ahead else state.route?.geometry.orEmpty()
        if (state.traveled.size >= 2) {
            paint.color = routeBehind
            paint.strokeWidth = 2.4f * unit
            canvas.drawPath(pathOf(state.traveled, project), paint)
        }
        if (ahead.size < 2) return

        val line = pathOf(ahead, project)
        paint.color = routeCasing
        paint.strokeWidth = 4.6f * unit
        canvas.drawPath(line, paint)
        paint.color = routeAhead
        paint.strokeWidth = 3.0f * unit
        canvas.drawPath(line, paint)
    }

    private fun drawMarkers(
        canvas: Canvas,
        state: CarState,
        unit: Float,
        paint: Paint,
        project: (LatLon) -> FloatArray
    ) {
        paint.style = Paint.Style.FILL
        val route = state.route ?: return

        // Signals and stops ahead of the car, the same road signs the phone
        // draws along the line.
        val passed = route.distanceMeters - state.remainingMeters
        route.junctions.asSequence()
            .filter { it.atMeters >= passed - 20 }
            .take(40)
            .forEach { junction ->
                val point = project(LatLon(junction.lat, junction.lon))
                paint.color = Color.WHITE
                canvas.drawCircle(point[0], point[1], 1.5f * unit, paint)
                paint.color = when (junction.kind) {
                    1 -> Color.parseColor("#E8912A")
                    2 -> Color.parseColor("#D7382C")
                    else -> Color.parseColor("#7A8794")
                }
                canvas.drawCircle(point[0], point[1], 1.0f * unit, paint)
            }

        (state.ahead.lastOrNull() ?: route.geometry.lastOrNull())?.let { end ->
            val point = project(end)
            paint.color = Color.WHITE
            canvas.drawCircle(point[0], point[1], 3.0f * unit, paint)
            paint.color = marker
            canvas.drawCircle(point[0], point[1], 2.4f * unit, paint)
            paint.color = ink
            canvas.drawCircle(point[0], point[1], 0.9f * unit, paint)
        }
    }

    /** The car sits at a fixed anchor, so it is drawn unrotated on top. */
    private fun drawPuck(
        canvas: Canvas,
        x: Float,
        y: Float,
        unit: Float,
        paint: Paint,
        state: CarState
    ) {
        paint.style = Paint.Style.FILL
        paint.color = puckColor
        paint.alpha = 55
        canvas.drawCircle(x, y, 5.2f * unit, paint)
        paint.alpha = 255

        if (state.navigating) {
            val r = 2.9f * unit
            val arrow = Path().apply {
                moveTo(x, y - r)
                lineTo(x + r * 0.78f, y + r * 0.82f)
                lineTo(x, y + r * 0.36f)
                lineTo(x - r * 0.78f, y + r * 0.82f)
                close()
            }
            paint.color = Color.WHITE
            paint.style = Paint.Style.STROKE
            paint.strokeWidth = 0.9f * unit
            paint.strokeJoin = Paint.Join.ROUND
            canvas.drawPath(arrow, paint)
            paint.style = Paint.Style.FILL
            paint.color = puckColor
            canvas.drawPath(arrow, paint)
        } else {
            paint.color = Color.WHITE
            canvas.drawCircle(x, y, 2.6f * unit, paint)
            paint.color = puckColor
            canvas.drawCircle(x, y, 1.9f * unit, paint)
        }
    }

    /** Speed limit and current road, sized off the visible area. */
    private fun drawOverlay(
        canvas: Canvas,
        area: RectF,
        unit: Float,
        paint: Paint,
        state: CarState
    ) {
        if (!state.navigating) return
        var left = area.left + 3f * unit
        val bottom = area.bottom - 3f * unit

        if (state.speedLimitKmh > 0) {
            val radius = 5.5f * unit
            val cx = left + radius
            val cy = bottom - radius
            paint.style = Paint.Style.FILL
            paint.color = Color.WHITE
            canvas.drawCircle(cx, cy, radius, paint)
            paint.style = Paint.Style.STROKE
            paint.strokeWidth = 1.5f * unit
            paint.color = Color.parseColor("#D7382C")
            canvas.drawCircle(cx, cy, radius - 0.7f * unit, paint)
            paint.style = Paint.Style.FILL
            paint.color = Color.parseColor("#14161D")
            paint.textAlign = Paint.Align.CENTER
            paint.textSize = 4.4f * unit
            paint.typeface = android.graphics.Typeface.DEFAULT_BOLD
            canvas.drawText(
                state.speedLimitKmh.toString(),
                cx,
                cy + paint.textSize * 0.36f,
                paint
            )
            left = cx + radius + 2.5f * unit
        }

        val road = state.stepName
        if (road.isNotBlank()) {
            paint.textAlign = Paint.Align.LEFT
            paint.textSize = 4.0f * unit
            paint.typeface = android.graphics.Typeface.DEFAULT_BOLD
            val width = paint.measureText(road) + 5f * unit
            val height = 8.5f * unit
            val box = RectF(left, bottom - height, left + width, bottom)
            paint.style = Paint.Style.FILL
            paint.color = chrome
            canvas.drawRoundRect(box, height / 2, height / 2, paint)
            paint.color = onChrome
            canvas.drawText(road, box.left + 2.5f * unit, box.centerY() + paint.textSize * 0.36f, paint)
        }
    }

    private fun drawWaiting(canvas: Canvas, area: RectF, unit: Float, paint: Paint) {
        paint.color = onChrome
        paint.alpha = 150
        paint.textAlign = Paint.Align.CENTER
        paint.textSize = 5f * unit
        paint.typeface = android.graphics.Typeface.DEFAULT_BOLD
        canvas.drawText("Waiting for location", area.centerX(), area.centerY(), paint)
        paint.alpha = 255
    }

    /** OSM and CARTO both require this wherever their data is shown. */
    private fun drawAttribution(canvas: Canvas, area: RectF, unit: Float, paint: Paint) {
        val text = "© OpenStreetMap · © CARTO"
        paint.style = Paint.Style.FILL
        paint.textAlign = Paint.Align.RIGHT
        paint.textSize = 2.6f * unit
        paint.typeface = android.graphics.Typeface.DEFAULT
        val width = paint.measureText(text)
        val box = RectF(
            area.right - width - 3f * unit,
            area.bottom - 5f * unit,
            area.right - 0.5f * unit,
            area.bottom - 0.6f * unit
        )
        paint.color = chrome
        paint.alpha = 210
        canvas.drawRoundRect(box, 1f * unit, 1f * unit, paint)
        paint.alpha = 255
        paint.color = onChrome
        canvas.drawText(text, box.right - 1.2f * unit, box.centerY() + paint.textSize * 0.36f, paint)
    }

    private companion object {
        const val FRAME_MS = 33L
        /** Per-frame fraction of the remaining camera distance. */
        const val EASE = 0.16
    }

    private fun pathOf(points: List<LatLon>, project: (LatLon) -> FloatArray): Path {
        val path = Path()
        points.forEachIndexed { index, point ->
            val xy = project(point)
            if (index == 0) path.moveTo(xy[0], xy[1]) else path.lineTo(xy[0], xy[1])
        }
        return path
    }
}
