package dev.rangefind.maps.ui.map

import android.graphics.Bitmap
import android.graphics.Canvas
import android.graphics.Paint
import android.graphics.Path
import android.graphics.RectF

/**
 * Map symbols drawn straight to bitmaps. Keeping them procedural means one
 * source of truth for their color with the Compose theme, and no density
 * bucket assets to keep in sync.
 */
object MapIcons {

    const val DESTINATION = "rf-pin-destination"
    const val ORIGIN = "rf-pin-origin"
    const val SIGNAL = "rf-junction-signal"
    const val STOP = "rf-junction-stop"
    const val CROSSING = "rf-junction-crossing"

    fun pin(color: Int, density: Float, glyph: Boolean = true): Bitmap {
        val width = (34 * density).toInt().coerceAtLeast(24)
        val height = (46 * density).toInt().coerceAtLeast(32)
        val bitmap = Bitmap.createBitmap(width, height, Bitmap.Config.ARGB_8888)
        val canvas = Canvas(bitmap)
        val paint = Paint(Paint.ANTI_ALIAS_FLAG)

        val cx = width / 2f
        val radius = width * 0.36f
        val cy = radius + width * 0.08f

        // Soft contact shadow so the pin reads on both light and dark styles.
        paint.color = 0x33000000
        canvas.drawOval(
            RectF(cx - radius * 0.5f, height - radius * 0.42f, cx + radius * 0.5f, height - radius * 0.06f),
            paint
        )

        val body = Path().apply {
            addCircle(cx, cy, radius, Path.Direction.CW)
            moveTo(cx - radius * 0.62f, cy + radius * 0.78f)
            lineTo(cx, height - radius * 0.30f)
            lineTo(cx + radius * 0.62f, cy + radius * 0.78f)
            close()
        }

        paint.color = 0xFFFFFFFF.toInt()
        canvas.drawPath(body, paint)

        paint.color = color
        val inset = width * 0.06f
        val inner = Path().apply {
            addCircle(cx, cy, radius - inset, Path.Direction.CW)
            moveTo(cx - (radius - inset) * 0.62f, cy + (radius - inset) * 0.80f)
            lineTo(cx, height - radius * 0.30f - inset)
            lineTo(cx + (radius - inset) * 0.62f, cy + (radius - inset) * 0.80f)
            close()
        }
        canvas.drawPath(inner, paint)

        if (glyph) {
            paint.color = 0xFFFFFFFF.toInt()
            canvas.drawCircle(cx, cy, radius * 0.30f, paint)
        }
        return bitmap
    }

    /** Traffic-signal head: three stacked lamps on a dark housing. */
    fun signal(density: Float): Bitmap {
        val size = (22 * density).toInt().coerceAtLeast(16)
        val bitmap = Bitmap.createBitmap(size, size, Bitmap.Config.ARGB_8888)
        val canvas = Canvas(bitmap)
        val paint = Paint(Paint.ANTI_ALIAS_FLAG)

        paint.color = 0xFFFFFFFF.toInt()
        canvas.drawCircle(size / 2f, size / 2f, size / 2f - density * 0.5f, paint)
        paint.color = 0xFF23282E.toInt()
        canvas.drawCircle(size / 2f, size / 2f, size / 2f - density * 2f, paint)

        val lampRadius = size * 0.10f
        val cx = size / 2f
        val colors = intArrayOf(0xFFFF5A52.toInt(), 0xFFFFC24B.toInt(), 0xFF4ADE80.toInt())
        for (i in colors.indices) {
            paint.color = colors[i]
            canvas.drawCircle(cx, size * (0.30f + 0.20f * i), lampRadius, paint)
        }
        return bitmap
    }

    /** Octagonal STOP marker. */
    fun stop(density: Float): Bitmap {
        val size = (22 * density).toInt().coerceAtLeast(16)
        val bitmap = Bitmap.createBitmap(size, size, Bitmap.Config.ARGB_8888)
        val canvas = Canvas(bitmap)
        val paint = Paint(Paint.ANTI_ALIAS_FLAG)

        paint.color = 0xFFFFFFFF.toInt()
        canvas.drawCircle(size / 2f, size / 2f, size / 2f - density * 0.5f, paint)

        paint.color = 0xFFD7382C.toInt()
        val radius = size / 2f - density * 2f
        val octagon = Path()
        for (i in 0 until 8) {
            val angle = Math.PI / 8 + i * Math.PI / 4
            val x = (size / 2f + radius * Math.cos(angle)).toFloat()
            val y = (size / 2f + radius * Math.sin(angle)).toFloat()
            if (i == 0) octagon.moveTo(x, y) else octagon.lineTo(x, y)
        }
        octagon.close()
        canvas.drawPath(octagon, paint)
        return bitmap
    }

    /** Give-way / crossing: a hollow amber diamond. */
    fun crossing(density: Float): Bitmap {
        val size = (20 * density).toInt().coerceAtLeast(14)
        val bitmap = Bitmap.createBitmap(size, size, Bitmap.Config.ARGB_8888)
        val canvas = Canvas(bitmap)
        val paint = Paint(Paint.ANTI_ALIAS_FLAG)

        paint.color = 0xFFFFFFFF.toInt()
        canvas.drawCircle(size / 2f, size / 2f, size / 2f - density * 0.5f, paint)

        paint.color = 0xFFE8912A.toInt()
        val radius = size / 2f - density * 2.5f
        val diamond = Path().apply {
            moveTo(size / 2f, size / 2f - radius)
            lineTo(size / 2f + radius, size / 2f)
            lineTo(size / 2f, size / 2f + radius)
            lineTo(size / 2f - radius, size / 2f)
            close()
        }
        canvas.drawPath(diamond, paint)
        return bitmap
    }
}
