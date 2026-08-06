package dev.rangefind.wayfind.region

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class RegionStoreTest {

    /**
     * Packs are content-addressed, so the root's filename is the version. This
     * is the whole basis of noticing that a kept region has gone out of date,
     * which nothing did before: `isReady` asked only whether the manifest file
     * existed, so a routing fix could ship to the server and never reach a
     * phone holding the region offline.
     */
    @Test
    fun `the root pack names the version`() {
        val manifest = """
            {"format":"rfroutegraph-v1","root":"root.66d2fe17b52937629cf98aec.bin.gz",
             "profile":"car","nodes":1725939,"leaves":4096}
        """.trimIndent()
        assertEquals("root.66d2fe17b52937629cf98aec.bin.gz", RegionStore.rootOf(manifest))
    }

    @Test
    fun `two builds of the same area are told apart`() {
        val before = """{"root":"root.c1084f28e0d9187b84271da1.bin.gz","profile":"car"}"""
        val after = """{"root":"root.66d2fe17b52937629cf98aec.bin.gz","profile":"car"}"""
        // Same region, same node and edge counts, different costs — only the
        // root distinguishes them, which is why the date stamp could not.
        assert(RegionStore.rootOf(before) != RegionStore.rootOf(after))
    }

    /**
     * Anything unreadable has to answer "unknown", never "stale": a phone with
     * no signal must not be told its offline region is out of date, which is
     * the one moment that region is all it has.
     */
    @Test
    fun `an unusable manifest is unknown, not stale`() {
        assertNull(RegionStore.rootOf(""))
        assertNull(RegionStore.rootOf("not json at all"))
        assertNull(RegionStore.rootOf("""{"profile":"car"}"""))
        assertNull(RegionStore.rootOf("""{"root":""}"""))
        assertNull(RegionStore.rootOf("<html>404</html>"))
    }
}
