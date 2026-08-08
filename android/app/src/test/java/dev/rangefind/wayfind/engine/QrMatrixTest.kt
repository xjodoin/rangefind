package dev.rangefind.wayfind.engine

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * The QR matrix crosses the JS bridge as packed hex rows rather than as
 * an array of thirty thousand booleans, which means both sides have to
 * agree on the packing to the bit: one hex digit per four modules, most
 * significant bit leftmost, each row padded out to a nibble boundary.
 *
 * A disagreement here does not fail loudly — it produces a QR code that
 * renders perfectly and that no camera can read.
 */
class QrMatrixTest {

    /** The packing the bridge writes, restated from the spec rather than shared. */
    private fun pack(size: Int, dark: (Int, Int) -> Boolean): QrMatrix {
        val rows = (0 until size).map { y ->
            val builder = StringBuilder()
            var acc = 0
            var bits = 0
            for (x in 0 until size) {
                acc = (acc shl 1) or (if (dark(x, y)) 1 else 0)
                bits++
                if (bits == 4) {
                    builder.append(acc.toString(16))
                    acc = 0
                    bits = 0
                }
            }
            // A row is `size` bits wide, and a QR size is never a multiple
            // of four, so the tail nibble is left-aligned like the rest.
            if (bits > 0) builder.append((acc shl (4 - bits)).toString(16))
            builder.toString()
        }
        return QrMatrix(size = size, rows = rows)
    }

    @Test
    fun `packed rows decode back to the modules they came from`() {
        // 21 is the real version-1 size: not a multiple of four, so the
        // padded tail nibble is exercised on every row.
        val size = 21
        // A pattern that is asymmetric in both axes: a matrix decoded
        // transposed, mirrored or off by one still fails this.
        val dark = { x: Int, y: Int -> (x * 3 + y * 7) % 5 == 0 }
        val matrix = pack(size, dark)

        assertEquals(size, matrix.size)
        assertEquals(size, matrix.rows.size)
        // ceil(21 / 4) hex digits per row.
        assertEquals(6, matrix.rows[0].length)
        for (y in 0 until size) {
            for (x in 0 until size) {
                assertEquals("module($x, $y)", dark(x, y), matrix.module(x, y))
            }
        }
    }

    @Test
    fun `the tail nibble carries only real modules`() {
        // 5 modules wide: one full nibble plus a single bit, so the second
        // hex digit is 8 when that last module is dark and 0 when it is not.
        val onlyLast = pack(5) { x, _ -> x == 4 }
        assertEquals("08", onlyLast.rows[0])
        assertTrue(onlyLast.module(4, 0))
        // The three padding bits are not modules and must never read as dark.
        assertFalse(onlyLast.module(5, 0))
        assertFalse(onlyLast.module(6, 0))
        assertFalse(onlyLast.module(7, 0))
    }

    @Test
    fun `a known row packs most significant bit first`() {
        // Dark at x = 0 alone: the leftmost module is the high bit of the
        // first digit. Packing least-significant-first would give "10".
        val leftmost = pack(8) { x, _ -> x == 0 }
        assertEquals("80", leftmost.rows[0])
    }

    @Test
    fun `outside the symbol is light and an empty matrix is empty`() {
        val matrix = pack(6) { _, _ -> true }
        assertFalse(matrix.module(-1, 0))
        assertFalse(matrix.module(0, -1))
        assertFalse(matrix.module(6, 0))
        assertFalse(matrix.module(0, 6))
        assertFalse(matrix.isEmpty)
        assertTrue(QrMatrix(size = 0, rows = emptyList()).isEmpty)
    }
}
