package dev.rangefind.wayfind.ui.components

import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * The size a doorstep photo is reduced to.
 *
 * Only the arithmetic is checked here, and deliberately: the encoding
 * around it is `Bitmap.compress`, which is a native Android method with no
 * JVM implementation, so a unit test of it would be a test of a Robolectric
 * shadow's JPEG writer rather than of this app. The arithmetic is the part
 * that can be wrong quietly — a squashed photo is still a photo, and
 * nothing downstream would complain about one.
 */
class DeliveryPhotoTest {

    @Test
    fun `a camera original is reduced with its shape kept`() {
        // 4:3 landscape, the ordinary phone camera frame.
        assertEquals(1024 to 768, photoTargetSize(4032, 3024))
        // And portrait: the longest edge is the one that gets the budget,
        // whichever edge that happens to be.
        assertEquals(768 to 1024, photoTargetSize(3024, 4032))
    }

    @Test
    fun `a photo already small enough is left exactly as it is`() {
        // Never scaled up: interpolating a small preview into a bigger file
        // spends bytes on detail that is not there, against a 128 KB cap.
        assertEquals(640 to 480, photoTargetSize(640, 480))
        assertEquals(1024 to 500, photoTargetSize(1024, 500))
    }

    @Test
    fun `an extreme shape keeps at least one row of pixels`() {
        // A panorama's short edge rounds towards zero, and a zero-height
        // bitmap is one Bitmap refuses to make.
        val (width, height) = photoTargetSize(20000, 9)
        assertEquals(1024, width)
        assertEquals(1, height)
    }

    @Test
    fun `nothing sensible in gives nothing out`() {
        assertEquals(0 to 0, photoTargetSize(0, 1200))
        assertEquals(0 to 0, photoTargetSize(-4, -3))
    }

    @Test
    fun `a smaller budget is honoured`() {
        assertEquals(256 to 192, photoTargetSize(4032, 3024, maxEdge = 256))
    }
}
