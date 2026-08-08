package dev.rangefind.wayfind.ui.components

import android.graphics.Bitmap
import android.util.Base64
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.rememberUpdatedState
import java.io.ByteArrayOutputStream

/**
 * The doorstep photo, from the camera to bytes the mesh will accept.
 *
 * PulseMesh threads §20.7 takes a photo as opaque already-compressed bytes
 * and says so: the library has no image decoder and no business having one.
 * That makes two things this app's obligation rather than the protocol's.
 *
 * The first is size. A sealed photo is capped at 128 KB, and it is carried
 * by request over the mesh to whoever is entitled to open it — a 4 MB
 * camera original would be refused, and would deserve to be.
 *
 * The second matters more. **A phone camera writes EXIF, and EXIF on a
 * delivery photo contains the GPS fix of the doorstep.** A run publishing
 * coarse is deliberately withholding the vehicle's position; a JPEG with
 * coordinates in its header hands that position over out of band, past
 * every granularity control the protocol has. Re-encoding a decoded
 * [Bitmap] is what prevents it: [Bitmap.compress] writes pixels and a JPEG
 * header, and there is no path by which the original file's metadata
 * reaches the output — the metadata was dropped at decode, before this code
 * ever sees the image. This is the whole reason the app never forwards
 * camera *file* bytes.
 */

/** The longest edge a proof-of-delivery photo is reduced to. */
const val PHOTO_MAX_EDGE = 1024

/**
 * Quality enough to read a door number, a parcel and a porch, and not
 * enough to blow the 128 KB seal cap on a busy scene.
 */
const val PHOTO_JPEG_QUALITY = 60

/**
 * The size [width]x[height] becomes with its longest edge at [maxEdge] and
 * its aspect ratio kept.
 *
 * Pure, and separate from the bitmap work, because this is the part that
 * can be wrong quietly: an off-by-one in the rounding is a squashed photo,
 * not a crash. Never scales up — a small camera preview stays its own size
 * rather than being interpolated into a bigger file of the same detail —
 * and never rounds an edge down to zero, which [Bitmap] rejects.
 */
fun photoTargetSize(width: Int, height: Int, maxEdge: Int = PHOTO_MAX_EDGE): Pair<Int, Int> {
    if (width <= 0 || height <= 0) return 0 to 0
    val longest = maxOf(width, height)
    if (maxEdge <= 0 || longest <= maxEdge) return width to height
    val scale = maxEdge.toDouble() / longest
    return maxOf(1, Math.round(width * scale).toInt()) to
        maxOf(1, Math.round(height * scale).toInt())
}

/**
 * The bitmap as base64 JPEG, downscaled and stripped of everything that
 * was not a pixel.
 *
 * Base64 because the bytes cross a JSON bridge into a WebView; the same
 * image as an array of numbers would be three times the text for the same
 * picture.
 */
fun scaledJpegBase64(
    bitmap: Bitmap,
    maxEdge: Int = PHOTO_MAX_EDGE,
    quality: Int = PHOTO_JPEG_QUALITY
): String? {
    val (width, height) = photoTargetSize(bitmap.width, bitmap.height, maxEdge)
    if (width <= 0 || height <= 0) return null
    val scaled =
        if (width == bitmap.width && height == bitmap.height) bitmap
        else Bitmap.createScaledBitmap(bitmap, width, height, true)
    val bytes = ByteArrayOutputStream()
    val encoded = scaled.compress(Bitmap.CompressFormat.JPEG, quality, bytes)
    if (scaled !== bitmap) scaled.recycle()
    if (!encoded || bytes.size() == 0) return null
    // NO_WRAP: the JS side hands the string straight to `atob`, which does
    // not want newlines in it.
    return Base64.encodeToString(bytes.toByteArray(), Base64.NO_WRAP)
}

/**
 * A one-tap camera capture that yields compressed base64 or nothing.
 *
 * `TakePicturePreview` on purpose: it needs no camera permission, no file
 * provider and no storage write, and it returns a preview-resolution
 * bitmap — which is not a compromise here, because the target is 1024 px
 * anyway. A driver holding an armful of parcels is one tap from a photo
 * and one back-press from having taken none.
 *
 * [onPhoto] is called with null when the driver backs out of the camera,
 * and callers treat that as "nothing happened" rather than as an empty
 * photo: the question they were asked is still on screen.
 */
@Composable
fun rememberPhotoCapture(onPhoto: (String?) -> Unit): () -> Unit {
    val deliver by rememberUpdatedState(onPhoto)
    val launcher = rememberLauncherForActivityResult(
        ActivityResultContracts.TakePicturePreview()
    ) { bitmap ->
        deliver(bitmap?.let { scaledJpegBase64(it) })
    }
    return { launcher.launch(null) }
}
