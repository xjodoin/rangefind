package dev.rangefind.wayfind.car

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.util.LruCache
import java.io.File
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.math.PI
import kotlin.math.asinh
import kotlin.math.ln
import kotlin.math.tan

const val TILE_SIZE = 256.0

/** Web-Mercator pixel coordinates at a given zoom. */
fun mercatorX(lon: Double, zoom: Double): Double =
    (lon + 180.0) / 360.0 * TILE_SIZE * Math.pow(2.0, zoom)

fun mercatorY(lat: Double, zoom: Double): Double {
    val clamped = lat.coerceIn(-85.05112878, 85.05112878)
    val y = (1.0 - asinh(tan(Math.toRadians(clamped))) / PI) / 2.0
    return y * TILE_SIZE * Math.pow(2.0, zoom)
}

/**
 * Raster basemap tiles for the car surface.
 *
 * The head unit gets a real map — roads, water, labels — rather than a
 * schematic, so tiles are fetched, cached in memory and on disk, and decoded
 * off the render path. A miss draws nothing rather than blocking: the next
 * frame paints it once it lands.
 */
class CarTileSource(cacheDir: File, private val dark: Boolean, private val onTile: () -> Unit) {

    private val memory = object : LruCache<String, Bitmap>(24 * 1024 * 1024) {
        override fun sizeOf(key: String, value: Bitmap) = value.byteCount
    }
    private val disk = File(cacheDir, "car-tiles").apply { mkdirs() }
    private val inFlight = ConcurrentHashMap.newKeySet<String>()
    private val pool = Executors.newFixedThreadPool(4)

    fun bitmap(zoom: Int, x: Int, y: Int): Bitmap? {
        val key = "$zoom/$x/$y"
        memory.get(key)?.let { return it }
        if (inFlight.add(key)) pool.execute { load(key, zoom, x, y) }
        return null
    }

    fun shutdown() {
        pool.shutdownNow()
        memory.evictAll()
    }

    private fun load(key: String, zoom: Int, x: Int, y: Int) {
        try {
            val cached = File(disk, "${zoom}_${x}_${y}_${if (dark) "d" else "l"}.png")
            val bytes = if (cached.isFile) cached.readBytes() else download(zoom, x, y, cached)
            if (bytes != null) {
                val options = BitmapFactory.Options().apply { inPreferredConfig = Bitmap.Config.RGB_565 }
                BitmapFactory.decodeByteArray(bytes, 0, bytes.size, options)?.let { bitmap ->
                    memory.put(key, bitmap)
                    onTile()
                }
            }
        } catch (error: Throwable) {
            // A missing tile is a cosmetic gap, never a navigation failure.
        } finally {
            inFlight.remove(key)
        }
    }

    private fun download(zoom: Int, x: Int, y: Int, cacheFile: File): ByteArray? {
        val style = if (dark) "dark_all" else "voyager"
        val url = URL("https://basemaps.cartocdn.com/rastertiles/$style/$zoom/$x/$y@2x.png")
        val connection = (url.openConnection() as HttpURLConnection).apply {
            connectTimeout = 8_000
            readTimeout = 15_000
            setRequestProperty("User-Agent", "Wayfind/0.1 (rangefind.dev)")
        }
        try {
            if (connection.responseCode !in 200..299) return null
            val bytes = connection.inputStream.use { it.readBytes() }
            runCatching { cacheFile.writeBytes(bytes) }
            return bytes
        } finally {
            connection.disconnect()
        }
    }
}
