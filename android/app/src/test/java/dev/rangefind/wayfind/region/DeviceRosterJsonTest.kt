package dev.rangefind.wayfind.region

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * The roster on disk, and the two pure decisions around it.
 *
 * Enrolment is the gate on transfer (threads §20.9): with an empty roster
 * there is no key to seal a job to, so this list is what decides whether a
 * day can be handed on at all. It is written by one build and read by the
 * next, and it has to survive a phone call, a low-memory kill and an
 * upgrade — because the replacement courier was enrolled hours before the
 * first one's bike broke, and an enrolment that does not outlive the
 * process is not an enrolment.
 */
class DeviceRosterJsonTest {

    private fun device(
        publicKey: String,
        name: String = "Léa — Pixel 8",
        fingerprint: String = "0f1e2d3c",
        addedAt: Long = 1_800_000_000_000L
    ) = EnrolledDevice(
        publicKey = publicKey,
        name = name,
        fingerprint = fingerprint,
        addedAt = addedAt
    )

    @Test
    fun `a roster survives the round trip field for field`() {
        val original = listOf(
            device("cHVibGljS2V5QQ", name = "Léa — Pixel 8", fingerprint = "0f1e2d3c"),
            device("cHVibGljS2V5Qg", name = "Van 4", fingerprint = "a1b2c3d4", addedAt = 1L)
        )

        val restored = enrolledDevicesFromJson(enrolledDevicesToJson(original))

        assertEquals(original, restored)
        // The public key is the address a ticket is sealed to; a byte of
        // rounding here is a job nobody can open.
        assertEquals("cHVibGljS2V5QQ", restored[0].publicKey)
        assertEquals("a1b2c3d4", restored[1].fingerprint)
    }

    @Test
    fun `nothing usable reads as an empty roster rather than a crash`() {
        assertTrue(enrolledDevicesFromJson(null).isEmpty())
        assertTrue(enrolledDevicesFromJson("").isEmpty())
        assertTrue(enrolledDevicesFromJson("   ").isEmpty())
        assertTrue(enrolledDevicesFromJson("not json").isEmpty())
        assertTrue(enrolledDevicesFromJson("""{"devices":[]}""").isEmpty())
    }

    @Test
    fun `an entry with no public key is not a device anything can be sealed to`() {
        // A row with no key would sit on screen looking enrollable and
        // fail at the one moment it is needed — with a driver waiting.
        val partial = """
            {"devices":[{"name":"ghost","fingerprint":"0f1e2d3c","addedAt":1},
                        {"publicKey":"cHVibGljS2V5QQ","name":"real","fingerprint":"a1b2c3d4"}]}
        """.trimIndent()
        val restored = enrolledDevicesFromJson(partial)
        assertEquals(1, restored.size)
        assertEquals("real", restored[0].name)
        // No addedAt in the file: zero, and the row simply shows no date
        // rather than inventing one.
        assertEquals(0L, restored[0].addedAt)
    }

    @Test
    fun `a device with no name comes back unnamed rather than the text null`() {
        val explicitNull = """
            {"devices":[{"publicKey":"cHVibGljS2V5QQ","name":null,"fingerprint":null,"addedAt":7}]}
        """.trimIndent()
        val restored = enrolledDevicesFromJson(explicitNull)
        assertEquals("", restored[0].name)
        assertEquals("", restored[0].fingerprint)
        assertEquals("Unnamed", restored[0].displayName("Unnamed"))
    }

    @Test
    fun `re-enrolling the same phone replaces its row instead of doubling it`() {
        // Keyed on the public key, not the name: a driver who renames
        // their phone and shows the card again must not leave two rows
        // that seal to one device — and two different phones both called
        // "Pixel 8" must not collapse into one.
        val first = device("cHVibGljS2V5QQ", name = "Pixel 8")
        val renamed = device("cHVibGljS2V5QQ", name = "Léa — Pixel 8")
        val other = device("cHVibGljS2V5Qg", name = "Pixel 8")

        val roster = listOf(first).withDevice(other).withDevice(renamed)

        assertEquals(2, roster.size)
        assertEquals(listOf("cHVibGljS2V5Qg", "cHVibGljS2V5QQ"), roster.map { it.publicKey })
        assertEquals("Léa — Pixel 8", roster.last().name)

        val pruned = roster.withoutDevice("cHVibGljS2V5Qg")
        assertEquals(1, pruned.size)
        assertEquals("cHVibGljS2V5QQ", pruned[0].publicKey)
    }

    @Test
    fun `a device name is cut to what a card can carry, on a character boundary`() {
        // §20.9: a PMV1 card carries at most 32 bytes of name, and the
        // encoder throws above it — so a long name would otherwise take
        // the whole enrolment with it. Cutting mid-character would reach
        // the other phone as a replacement glyph, and the name exists
        // precisely so a human recognises it.
        assertEquals("Léa — Pixel 8", cappedDeviceName("  Léa — Pixel 8  "))
        assertEquals("", cappedDeviceName(null))
        assertEquals("", cappedDeviceName("   "))

        // Twelve accented characters are 24 bytes; twenty are 40.
        val long = "é".repeat(20)
        val capped = cappedDeviceName(long)
        assertEquals(16, capped.length)
        assertTrue(capped.toByteArray(Charsets.UTF_8).size <= MAX_DEVICE_NAME_BYTES)
        assertTrue(capped.all { it == 'é' })

        // An emoji is four bytes and must not be split into two halves of
        // a surrogate pair.
        val emoji = "a".repeat(30) + "🚚"
        val cut = cappedDeviceName(emoji)
        assertEquals("a".repeat(30), cut)
        assertTrue(cut.toByteArray(Charsets.UTF_8).size <= MAX_DEVICE_NAME_BYTES)
    }

    @Test
    fun `a fingerprint is grouped so two people can read it to each other`() {
        assertEquals("0F1E 2D3C", formatFingerprint("0f1e2d3c"))
        assertEquals("A1B2 C3D4", formatFingerprint("A1B2C3D4"))
        // Anything that is not the expected eight hex characters comes
        // back untouched rather than mangled into a shape it is not: a
        // fingerprint this build does not recognise must still be
        // comparable against what the other phone is showing.
        assertEquals("", formatFingerprint(null))
        assertEquals("abc", formatFingerprint("abc"))
        assertEquals("zzzzzzzz", formatFingerprint("zzzzzzzz"))
        assertEquals("0f1e2d3c4b", formatFingerprint("0f1e2d3c4b"))
    }
}
