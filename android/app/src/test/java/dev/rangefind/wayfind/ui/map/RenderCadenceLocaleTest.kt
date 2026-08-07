package dev.rangefind.wayfind.ui.map

import android.os.SystemClock
import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.Locale

/**
 * The crash on arrival.
 *
 * `RenderCadence.snapshot()` rounded its figures by formatting them to a
 * string and parsing the string back. `String.format` writes in the device's
 * locale; `String.toDouble` reads only the machine one. On a phone set to
 * French the round trip is `464.1` → `"464,1"` → NumberFormatException — and
 * it is thrown from the last thing a drive does before it ends, so the app
 * closed as the driver pulled up. Six of these are in the device's crash
 * buffer, one per completed drive.
 *
 * It never reproduced in English, which is the whole reason it survived: the
 * bug is invisible in the locale the app was developed in. So this test runs
 * the summary under a comma-decimal locale on purpose.
 */
class RenderCadenceLocaleTest {

    private val original: Locale = Locale.getDefault()

    @After
    fun restore() {
        Locale.setDefault(original)
    }

    @Test
    fun `a drive summary survives a locale that writes a comma in a number`() {
        Locale.setDefault(Locale.CANADA_FRENCH)
        // Sanity: this locale really does format the way that broke it, or
        // the test proves nothing.
        assertTrue(
            "the fixture locale must use a decimal comma",
            String.format("%.1f", 464.1).contains(',')
        )

        val start = SystemClock.elapsedRealtimeNanos()
        RenderCadence.start(start)
        // A drive: fixes arriving, frames rendering, at figures that do not
        // land on whole numbers — which is when rounding has anything to do.
        var now = start
        repeat(120) { i ->
            now += 16_700_000L
            RenderCadence.onVehicleUpdate(11.7, now)
            RenderCadence.onFrame(now)
            RenderCadence.onStylePush(0.1 * i)
        }
        RenderCadence.stop()

        // The failure was an exception, not a wrong number, so reaching the
        // next line at all is the assertion.
        val summary = RenderCadence.snapshot(now)
        assertTrue("a drive that rendered frames has a summary", summary != null)
        requireNotNull(summary)

        for (key in listOf("movingSeconds", "driveSeconds", "fps", "droppedPercent", "settledDroppedPercent")) {
            assertTrue("$key should be present", summary.has(key))
            // And still a number in JSON, not a string that happens to parse.
            assertTrue("$key should be numeric", summary.get(key) is Number)
        }
    }
}
