package dev.rangefind.wayfind.region

import dev.rangefind.wayfind.engine.ActiveJob
import dev.rangefind.wayfind.engine.JobStop
import dev.rangefind.wayfind.engine.StopOutcome
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * The delivery day on disk.
 *
 * This string is the only thing standing between a driver and retyping
 * twelve addresses after a phone call kills the app, and it is written by
 * one build and read by the next. So the round trip is checked to the
 * field, and every way the stored copy can be wrong — truncated, from an
 * older schema, or simply corrupt — has to come back as "no run" rather
 * than as a crash on the way to the depot.
 */
class ActiveJobJsonTest {

    private fun job(
        stops: List<JobStop>,
        nextIndex: Int = 1,
        ticket: String? = "AAAABBBBCCCC",
        fine: Boolean = true
    ) = ActiveJob(
        stops = stops,
        nextIndex = nextIndex,
        ticketBase64 = ticket,
        jobIdHex = "0f1e2d3c4b5a6978",
        notAfter = 1_800_000_000L,
        fine = fine
    )

    @Test
    fun `a plan survives the round trip field for field`() {
        val original = job(
            stops = listOf(
                JobStop(index = 0, lat = 45.50884, lon = -73.55399, label = "Dépôt"),
                JobStop(index = 1, lat = 45.64015, lon = -73.79837, label = "12 rue Saint-Louis"),
                JobStop(index = 2, lat = 46.81228, lon = -71.21454, label = "Québec")
            ),
            nextIndex = 2
        )

        val restored = activeJobFromJson(original.toJson())

        assertEquals(original, restored)
        // Coordinates are the whole point of a stop: a metre of rounding
        // here is a van at the wrong side of a divided road.
        assertEquals(-73.79837, restored!!.stops[1].lon, 0.0)
        assertEquals("12 rue Saint-Louis", restored.currentStop?.label)
        assertEquals(3, restored.total)
    }

    @Test
    fun `a stop with no name comes back unnamed rather than missing`() {
        // A dispatcher numbers what it does not name, so a blank label is
        // ordinary — and a JSON null for it must not become the text
        // "null", which is what optString alone would do.
        val original = job(
            stops = listOf(
                JobStop(index = 0, lat = 45.5, lon = -73.5, label = ""),
                JobStop(index = 1, lat = 45.6, lon = -73.6, label = "")
            )
        )
        val restored = activeJobFromJson(original.toJson())
        assertEquals("", restored!!.stops[0].label)

        val explicitNull = """
            {"jobIdHex":"ab","notAfter":1,"nextIndex":1,"ticket":null,
             "stops":[{"index":0,"lat":45.5,"lon":-73.5,"label":null}]}
        """.trimIndent()
        val fromNull = activeJobFromJson(explicitNull)
        assertEquals("", fromNull!!.stops[0].label)
        assertNull(fromNull.ticketBase64)
    }

    @Test
    fun `a stored index is clamped into the run it belongs to`() {
        val stops = listOf(
            JobStop(index = 0, lat = 45.5, lon = -73.5, label = "one"),
            JobStop(index = 1, lat = 45.6, lon = -73.6, label = "two")
        )
        // Past the end: the day would index off the end of its own plan and
        // the progress card would read "Stop 9 of 2".
        val past = activeJobFromJson(job(stops, nextIndex = 9).toJson())
        assertEquals(2, past!!.nextIndex)
        assertEquals("two", past.currentStop?.label)
        assertNull(past.followingStop)

        // Below the start: zero would make currentStop null and strand a
        // run that is plainly on its first delivery.
        val before = activeJobFromJson(job(stops, nextIndex = 0).toJson())
        assertEquals(1, before!!.nextIndex)
        assertEquals("one", before.currentStop?.label)
        assertEquals("two", before.followingStop?.label)
    }

    @Test
    fun `nothing usable reads as no run at all`() {
        assertNull(activeJobFromJson(null))
        assertNull(activeJobFromJson(""))
        assertNull(activeJobFromJson("   "))
        assertNull(activeJobFromJson("not json"))
        // A truncated write: the file exists and parses, and there is still
        // no run in it.
        assertNull(activeJobFromJson("""{"jobIdHex":"ab","stops":[]}"""))
        // A stop with no coordinates is not an address, and a plan of
        // nothing but those is not a plan.
        assertNull(activeJobFromJson("""{"stops":[{"index":0,"label":"nowhere"}]}"""))
    }

    @Test
    fun `what the driver said about each stop survives the round trip`() {
        val original = job(
            stops = listOf(
                JobStop(index = 1, lat = 45.5, lon = -73.5, label = "one"),
                JobStop(index = 2, lat = 45.6, lon = -73.6, label = "two"),
                JobStop(index = 3, lat = 45.7, lon = -73.7, label = "three")
            ),
            nextIndex = 3
        ).withOutcome(1, StopOutcome.DELIVERED)
            .withOutcome(2, StopOutcome.SKIPPED)

        val restored = activeJobFromJson(original.toJson())

        assertEquals(original, restored)
        assertEquals(StopOutcome.DELIVERED, restored!!.outcomeOf(1))
        assertEquals(StopOutcome.SKIPPED, restored.outcomeOf(2))
        // Nobody has said anything about the stop the van is driving to,
        // and pending is the absence of an assertion rather than a claim.
        assertEquals(StopOutcome.PENDING, restored.outcomeOf(3))
        assertEquals(1, restored.deliveredCount)
        assertEquals(1, restored.missedCount)
    }

    @Test
    fun `a file written before outcomes existed reads as a run of pending stops`() {
        val old = """
            {"jobIdHex":"ab","notAfter":1,"nextIndex":2,"ticket":"T",
             "stops":[{"index":1,"lat":45.5,"lon":-73.5,"label":"one"},
                      {"index":2,"lat":45.6,"lon":-73.6,"label":"two"}]}
        """.trimIndent()
        val restored = activeJobFromJson(old)
        assertEquals(listOf(StopOutcome.PENDING, StopOutcome.PENDING), restored!!.outcomes)
        assertEquals(0, restored.deliveredCount)

        // And a value from a schema this build does not know is pending
        // too: an outcome nobody can read is not an outcome.
        val strange = """
            {"jobIdHex":"ab","notAfter":1,"nextIndex":1,"ticket":null,
             "stops":[{"index":1,"lat":45.5,"lon":-73.5,"label":"one","outcome":9}]}
        """.trimIndent()
        assertEquals(StopOutcome.PENDING, activeJobFromJson(strange)!!.outcomeOf(1))
    }

    @Test
    fun `an outcome stays with its own stop when another is dropped`() {
        // The middle stop has no coordinates and does not survive. An
        // outcome list stored beside the stops would then be off by one and
        // mark the wrong doorstep delivered.
        val mixed = """
            {"jobIdHex":"ab","notAfter":0,"nextIndex":1,"ticket":"T",
             "stops":[{"index":1,"lat":45.5,"lon":-73.5,"label":"first","outcome":1},
                      {"index":2,"label":"no position","outcome":1},
                      {"index":3,"lat":45.7,"lon":-73.7,"label":"third","outcome":2}]}
        """.trimIndent()
        val restored = activeJobFromJson(mixed)!!
        assertEquals(2, restored.total)
        assertEquals(StopOutcome.DELIVERED, restored.outcomeOf(1))
        assertEquals("third", restored.stops[1].label)
        assertEquals(StopOutcome.SKIPPED, restored.outcomeOf(2))
    }

    @Test
    fun `what the run publishes survives the round trip`() {
        // §11 outlives the process that took the ticket, and it decides
        // whether the driver is even offered a doorstep photo: a coarse run
        // that came back as fine after a restart would put a camera in front
        // of them for a run that withholds the vehicle's position.
        val stops = listOf(JobStop(index = 1, lat = 45.5, lon = -73.5, label = "one"))
        val coarse = activeJobFromJson(job(stops, fine = false).toJson())!!
        assertFalse(coarse.fine)
        val fine = activeJobFromJson(job(stops, fine = true).toJson())!!
        assertTrue(fine.fine)
    }

    @Test
    fun `a file written before granularity was stored reads as fine`() {
        // Fine is what a ticket defaults to, and guessing wrong this way is
        // the bounded direction: an offered photo on a run that is really
        // coarse is refused by the library before anything is sealed or
        // published, whereas defaulting to coarse would silently withdraw
        // proof of delivery from every run in progress across an upgrade.
        val old = """
            {"jobIdHex":"ab","notAfter":1,"nextIndex":1,"ticket":"T",
             "stops":[{"index":1,"lat":45.5,"lon":-73.5,"label":"one"}]}
        """.trimIndent()
        assertTrue(activeJobFromJson(old)!!.fine)
    }

    @Test
    fun `marking a stop leaves every other stop alone`() {
        val original = job(
            stops = listOf(
                JobStop(index = 1, lat = 45.5, lon = -73.5, label = "one"),
                JobStop(index = 2, lat = 45.6, lon = -73.6, label = "two")
            )
        )
        val marked = original.withOutcome(2, StopOutcome.FAILED)
        assertEquals(StopOutcome.PENDING, marked.outcomeOf(1))
        assertEquals(StopOutcome.FAILED, marked.outcomeOf(2))
        // A position off the end of the plan is not a stop, so nothing is
        // written rather than the list growing a phantom entry.
        assertEquals(original, original.withOutcome(7, StopOutcome.DELIVERED))
        assertEquals(2, marked.outcomes.size)
    }

    @Test
    fun `what the dispatcher said about each door survives the round trip`() {
        // §20.8. This is why it is stored at all: the ticket is decoded
        // once, inside a WebView that dies with the process, and a driver
        // who restarts the app still has to know which parcel this door is
        // and who to ring.
        val original = job(
            stops = listOf(
                JobStop(
                    index = 1, lat = 45.5, lon = -73.5, label = "12 rue Saint-Louis",
                    orderRef = "#4471-B",
                    parcels = 2,
                    instructions = "Laisser chez le concierge",
                    contact = "+1 514 555-0142"
                ),
                // A door the dispatcher said nothing about, which is most
                // doors on most days.
                JobStop(index = 2, lat = 45.6, lon = -73.6, label = "two")
            )
        )

        val restored = activeJobFromJson(original.toJson())!!

        assertEquals(original, restored)
        assertEquals("#4471-B", restored.stops[0].orderRef)
        assertEquals(2, restored.stops[0].parcels)
        assertEquals("Laisser chez le concierge", restored.stops[0].instructions)
        assertEquals("+1 514 555-0142", restored.stops[0].contact)
        assertNull(restored.stops[1].orderRef)
        assertNull(restored.stops[1].parcels)
        assertNull(restored.stops[1].instructions)
        assertNull(restored.stops[1].contact)
    }

    @Test
    fun `a stated zero parcels is not the same fact as no parcel count`() {
        // The whole reason the wire flags these fields rather than always
        // sending them. A dispatcher who wrote 0 has told the driver
        // something — nothing to carry, so this is a collection, a survey
        // or a retry — and one who left it empty has told them nothing. A
        // card that draws "unspecified" as "0 parcels", or hides a stated
        // 0, invents or drops a claim.
        val stated = activeJobFromJson(
            job(listOf(JobStop(index = 1, lat = 45.5, lon = -73.5, label = "one", parcels = 0)))
                .toJson()
        )!!
        assertEquals(0, stated.stops[0].parcels)

        val unstated = activeJobFromJson(
            job(listOf(JobStop(index = 1, lat = 45.5, lon = -73.5, label = "one"))).toJson()
        )!!
        assertNull(unstated.stops[0].parcels)

        // And the same distinction read straight off a file, where an
        // absent key must not arrive as optInt's zero.
        val fromDisk = activeJobFromJson(
            """
            {"jobIdHex":"ab","notAfter":1,"nextIndex":1,"ticket":"T",
             "stops":[{"index":1,"lat":45.5,"lon":-73.5,"label":"stated","parcels":0},
                      {"index":2,"lat":45.6,"lon":-73.6,"label":"unstated"}]}
            """.trimIndent()
        )!!
        assertEquals(0, fromDisk.stops[0].parcels)
        assertNull(fromDisk.stops[1].parcels)
    }

    @Test
    fun `a file written before per-stop metadata existed states nothing`() {
        // A build older than §20.8 wrote none of these keys, and a JSON
        // null is the same answer as an absent one. What must not happen is
        // optString's "" arriving as a stated empty order reference: an
        // empty row on the card for a fact nobody asserted.
        val old = """
            {"jobIdHex":"ab","notAfter":1,"nextIndex":1,"ticket":"T",
             "stops":[{"index":1,"lat":45.5,"lon":-73.5,"label":"one"},
                      {"index":2,"lat":45.6,"lon":-73.6,"label":"two",
                       "orderRef":null,"parcels":null,"instructions":null,"contact":null}]}
        """.trimIndent()
        val restored = activeJobFromJson(old)!!
        for (stop in restored.stops) {
            assertNull(stop.orderRef)
            assertNull(stop.parcels)
            assertNull(stop.instructions)
            assertNull(stop.contact)
        }
    }

    @Test
    fun `a stop without coordinates is dropped and the rest of the run stands`() {
        val mixed = """
            {"jobIdHex":"ab","notAfter":0,"nextIndex":1,"ticket":"T",
             "stops":[{"index":0,"lat":45.5,"lon":-73.5,"label":"real"},
                      {"index":1,"label":"no position"}]}
        """.trimIndent()
        val restored = activeJobFromJson(mixed)
        assertEquals(1, restored!!.total)
        assertEquals("real", restored.stops[0].label)
        // The clamp uses what survived, not what was written.
        assertEquals(1, restored.nextIndex)
    }
}
