package dev.rangefind.wayfind.engine

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

/**
 * What the call button on a delivery stop actually dials.
 *
 * The number is prose: a dispatcher types it the way people write them,
 * with brackets, spaces and an extension after it, and `tel:` wants none
 * of that. The failure that matters is not a dial that does nothing — the
 * driver sees that — but a dial that reaches the wrong subscriber because
 * an extension's digits were swept onto the end of the number.
 */
class DialableNumberTest {

    @Test
    fun `the separators people write phone numbers with are dropped`() {
        assertEquals("+15145550142", dialableNumber("+1 (514) 555-0142"))
        assertEquals("5145550142", dialableNumber("514.555.0142"))
        assertEquals("+352621123456", dialableNumber("  +352 621 123 456  "))
        // A French keyboard's non-breaking spaces, which are not
        // Char.isWhitespace and are invisible in a bug report.
        assertEquals("0142555555", dialableNumber("01\u00A042\u00A055\u202F55\u00A055"))
        // A typographic dash is still a dash.
        assertEquals("5145550142", dialableNumber("514\u2013555\u20110142"))
    }

    @Test
    fun `an extension is left behind rather than dialled as part of the number`() {
        // The bug this function exists for: "poste 12" filtered rather than
        // stopped at gives +15145550142**12**, which is a real number and
        // somebody else's.
        assertEquals("+15145550142", dialableNumber("+1 (514) 555-0142 poste 12"))
        assertEquals("+15145550142", dialableNumber("+1 514 555 0142 ext. 12"))
        assertEquals("5145550142", dialableNumber("514-555-0142 / 514-555-0143"))
        assertEquals("5145550142", dialableNumber("514-555-0142 ask for Marie"))
    }

    @Test
    fun `a plus is a country prefix only at the front`() {
        assertEquals("+3526211234", dialableNumber("+352 621 1234"))
        // Two numbers on one line: the first is the answer, and the second
        // must not be concatenated onto it.
        assertEquals("5145550142", dialableNumber("514 555 0142 +1 514 555 0143"))
    }

    @Test
    fun `a contact with no number in it does not dial`() {
        // A name or a desk is a perfectly ordinary contact, and it is shown
        // to the driver — it simply has nothing to dial.
        assertNull(dialableNumber("Ask at reception"))
        assertNull(dialableNumber(""))
        assertNull(dialableNumber("   "))
        assertNull(dialableNumber(null))
        // Too short to be a number anyone can be reached on. A URI the
        // dialler rejects is worse than a line with no call on it.
        assertNull(dialableNumber("12"))
        assertNull(dialableNumber("+"))
    }

    @Test
    fun `only ASCII digits reach the URI`() {
        // Char.isDigit answers true for Arabic-Indic digits, which a tel:
        // URI has no idea what to do with.
        assertNull(dialableNumber("٠١٢٣"))
    }
}
