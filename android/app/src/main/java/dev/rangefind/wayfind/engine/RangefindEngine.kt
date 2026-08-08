package dev.rangefind.wayfind.engine

/**
 * The app's whole dependency on rangefind. The JS host behind it is an
 * implementation detail: today a headless WebView, tomorrow an embedded V8 or
 * Node runtime, without touching anything above this line.
 */
interface RangefindEngine {
    suspend fun init(searchBase: String, routeBase: String): EngineInfo
    /** Repoint routing at another index (a preloaded region, or the network). */
    suspend fun useRouteBase(routeBase: String): RoutingInfo
    /** Every file that makes up the index at [baseUrl], for offline preload. */
    suspend fun regionFiles(baseUrl: String): RegionManifest
    suspend fun search(
        query: String,
        anchor: LatLon?,
        size: Int = 20,
        shards: List<String> = emptyList()
    ): SearchOutcome
    suspend fun suggest(query: String, anchor: LatLon?): List<Suggestion>
    suspend fun reverse(point: LatLon): Place?
    /**
     * [fromHeading] is the direction of travel in degrees, and belongs only to
     * a vehicle that is actually moving: it stops a reroute from snapping to
     * the opposite side of the road and routing the driver back the way they
     * came. Null means no opinion, so both directions stay equally good.
     */
    suspend fun route(
        from: LatLon,
        to: LatLon,
        alternatives: Int = 2,
        fromHeading: Double? = null,
        /**
         * The error bar on the fix [from] came from. The snap widens its
         * candidate band to match: a band narrower than the error discards
         * the road the car is on before anything can weigh it, which is how
         * a car on a service road beside a motorway gets routed onto the
         * motorway.
         */
        accuracyMeters: Double? = null
    ): RouteBundle
    suspend fun snap(point: LatLon): SnapPoint?

    /**
     * Live traffic (PulseMesh). The phone is the contributor the design
     * counts on — background location is what a browser tab cannot offer —
     * but publishing where you drive is a decision, so it stays off until
     * [setContributing] turns it on.
     */
    suspend fun pulseMeshStatus(): PulseMeshStatus
    /**
     * Brings the mesh up or down. [mode] is "mesh", "demo", or empty to pick.
     *
     * [simulated] is only ever about demo mode: with no keeper configured
     * the mesh has no peers, and the demo vehicles are what puts anything
     * on the traffic layer at all. Everything about them but their
     * positions and speeds is real, and they are still refusable.
     */
    suspend fun setMeshRunning(
        running: Boolean,
        mode: String = "",
        simulated: Boolean = true
    ): PulseMeshStatus
    /**
     * Starts or stops the demo vehicles with the mesh left up. Records they
     * already published stay: they were validated on arrival, and the
     * source going away is not a reason to unlearn what it said.
     */
    suspend fun setSimulatedTraffic(enabled: Boolean): PulseMeshStatus
    suspend fun setContributing(enabled: Boolean): PulseMeshStatus

    /**
     * One GPS fix through the contributor pipeline, with the battery state
     * §10.1 rule 5 needs: contribution pauses below 20% unless charging,
     * because a traffic layer that flattens a phone loses the contributor
     * permanently.
     */
    suspend fun offerLocation(
        point: LatLon,
        speedMps: Double?,
        courseDeg: Double?,
        batteryLevel: Double? = null,
        charging: Boolean? = null
    ): PulseMeshStatus

    /**
     * One maintenance round. The host drives it: a headless WebView is a
     * hidden page, and Chromium clamps a hidden page's timers — a mesh
     * that scheduled its own anti-entropy simply stopped after the first
     * round, with nothing to show for it but a store that never grew.
     */
    suspend fun tickMesh()

    /** What the mesh believes about the roads around us, ready to draw. */
    suspend fun meshTraffic(): List<TrafficSegment>
    /** Scored incidents with a position, ready to pin. */
    suspend fun meshIncidents(): List<MeshIncident>

    /**
     * Files a report for this device's own snapped position (§10.4). The
     * protocol refuses to mint one unless [acknowledgedPublic] — the app
     * has told the user, at the point of reporting, that a report is public
     * and locates them.
     */
    suspend fun reportIncident(type: Int, acknowledgedPublic: Boolean): MeshAction
    /** Confirms (2) or refutes (3) an incident this device is driving past. */
    suspend fun answerIncident(key: String, polarity: Int, acknowledgedPublic: Boolean): MeshAction

    /**
     * Starts publishing this drive; returns the capability link to share.
     *
     * [fine] is threads §11, and it is a harm decision rather than a
     * bandwidth one: coarse publishes stop events and a heartbeat with
     * **no position at all**, so a leaked coarse link is worth roughly
     * what a printed timetable is. Fine publishes a live locator.
     */
    suspend fun shareDrive(
        stops: List<LatLon>,
        baseUrl: String,
        fine: Boolean = true,
        /**
         * How the vehicle moves, as a routing profile name ("car", "bike",
         * "foot"). A different question from [fine]: one is what a leaked
         * link reveals, the other is which graph a follower's arrival is
         * computed on — and a cyclist quoted off a car graph is wrong by a
         * factor rather than by a minute.
         */
        travelMode: String = "car"
    ): String?
    /**
     * Ends the published run: one final record, after which it is dead.
     *
     * The default record is COMPLETED, and that is a claim — every customer
     * holding the link is told the run arrived. [canceled] emits §20's
     * CANCELED instead, which is the only honest ending for a day the
     * driver abandoned, and [note] carries their reason (at most 64 bytes
     * on the wire; see [truncateNote]).
     *
     * [silent] stops publishing here with **no** final record, and exists
     * for exactly one caller: the device that has just handed the job to
     * another phone. §20.5 — both then hold the same seed and the wire
     * cannot tell them apart, so any goodbye from this one would tell every
     * follower the run had ended while the new driver is still delivering.
     */
    suspend fun endSharedDrive(
        canceled: Boolean = false,
        note: String? = null,
        silent: Boolean = false
    )
    /** Follows someone else's drive from their link. */
    suspend fun followDrive(link: String): MeshAction
    /** Stops watching. */
    suspend fun stopFollowing()
    /** Where the followed vehicle is, and what claim the UI may make. */
    suspend fun followedDrive(): FollowedDrive?
    /**
     * When the followed vehicle reaches [to], routed locally from the
     * position it broadcast under this device's own live-traffic metric.
     *
     * This is why the channel lives in a maps engine at all: the
     * publisher broadcasts one position and never learns which
     * destination anyone asked about — unlike every server-side ETA,
     * which by construction knows each recipient's address.
     */
    suspend fun followedEta(to: LatLon): FollowedEta?

    /**
     * What an incoming capability is: a ticket, a link, or neither.
     *
     * A dispatch ticket and a follow link arrive the same way — in a URL
     * fragment, which is never transmitted — and they are worth wildly
     * different things: one lets you *watch* a run, the other lets you
     * *move* the vehicle. The app branches on the artifact rather than on
     * how it arrived.
     */
    suspend fun classifyMeshLink(link: String): MeshArtifact

    /**
     * What a dispatch ticket says, without taking it (threads §20).
     *
     * Works with live traffic off: decoding and verifying are local, and
     * only publishing the run needs a mesh. Being shown where, until when
     * and by whom before pressing accept is the whole difference between
     * an offer and an instruction.
     *
     * [heldOffers] are the offers this device has bid on, as the bytes
     * they arrived in. Each one is checked against the award with
     * `awardMatchesOffer` and answered in [JobDescription.offerChecks] —
     * every one of them, in order, rather than the one whose `jobId`
     * looks right: a swapped plan moves the `jobId` too, so a lookup by
     * `jobId` finds nothing and waves exactly the swap through that the
     * commitment exists to catch.
     */
    suspend fun describeJob(
        link: String,
        heldOffers: List<String> = emptyList()
    ): JobDescription

    /**
     * What a dispatcher's broadcast *offer* says (threads §20.4).
     *
     * A different artifact from a ticket, and the only one here that is
     * public and unsealed by design: it goes to couriers the dispatcher
     * has never met, so it carries a commitment to a plan plus coarse
     * descriptors and **no stop at all**. Nothing downstream may invent
     * one.
     */
    suspend fun describeOffer(link: String): OfferDescription

    /**
     * Takes the job: publishes the run this ticket names, resuming above
     * whatever sequence the audience has already seen (§20.5), so the
     * customer's link keeps working across a handover.
     */
    suspend fun acceptJob(link: String, baseUrl: String, travelMode: String = "car"): AcceptedJob?

    /**
     * What actually happened at a stop, asserted by the driver (§5.2).
     *
     * Nothing infers this and nothing may: arriving somewhere is not
     * delivering, and a run that inferred "delivered" from a doorstep the
     * van drove past would be telling a customer something no one said.
     * [index] is the stop's 1-based position in the plan, [outcome] one of
     * [StopOutcome], and [reason] one of [StopReason]. Published
     * immediately — a customer whose parcel went to a neighbour should not
     * wait out a heartbeat to hear it.
     *
     * [photoBase64] is an optional proof of delivery: standard base64 of
     * **already-compressed** image bytes. Compressing it and stripping its
     * EXIF is this app's job, not the library's — a phone camera writes the
     * GPS fix into the file, and a run that publishes coarse would hand
     * over out of band exactly the position it is withholding. The photo
     * never rides gossip; only a 32-byte commitment to it does.
     */
    suspend fun markStop(
        index: Int,
        outcome: Int,
        reason: Int,
        note: String?,
        photoBase64: String? = null
    ): MeshAction

    /**
     * The active ticket as a QR matrix, for the next driver's camera.
     *
     * This is the handover path and it needs no camera code here: the
     * other phone's own camera app fires a VIEW intent on the URL, which
     * opens Wayfind.
     *
     * [ticket] is the app's own persisted copy of the capability and wins
     * when it is given. The engine's copy dies with its host while a
     * delivery day outlives the process several times over, and a handover
     * that stops working after a process death is one no driver can rely on.
     */
    suspend fun ticketQr(ticket: String? = null): QrMatrix?

    /**
     * Adopts this device's stored X25519 key, or mints one on first run
     * (threads §20.9).
     *
     * The key is the address a job is sealed *to*. It is none of the other
     * keys in this protocol — not the run seed, which travels with a
     * ticket, and not an issuer seed, which signs — and it never signs
     * anything.
     *
     * [DeviceKeyLoad.mintedPrivateKey] is set on exactly one call in the
     * life of an install: the one that minted it. That is the only moment
     * the private key leaves the engine, and it leaves so the host can
     * keep it — a device that loses this key cannot open any job already
     * sealed to it and has to be enrolled again.
     */
    suspend fun loadDeviceIdentity(privateKeyBase64: String?, name: String): DeviceKeyLoad?

    /** Renames this device on its card. At most 32 bytes; longer is cut. */
    suspend fun setDeviceName(name: String): DeviceIdentity?

    /**
     * The card this phone shows another one: the `wayfind://device#…`
     * link, and the same link as a QR for the other phone's own camera.
     *
     * There is no scanner in this app by design — the system camera fires
     * ACTION_VIEW on the URL it decodes, and that is what lands here.
     */
    suspend fun myDeviceCard(): DeviceCardArtifact?

    /** A card that arrived, decoded so the driver can check its fingerprint. */
    suspend fun readDeviceCard(link: String): DeviceCard?

    /**
     * Re-seals a held ticket to enrolled devices — handover (§20.5) as
     * §20.9 requires it to happen.
     *
     * The inner job is untouched: same signature, same run seed, same
     * plan. What changes is who can open it, which is the entire content
     * of "a device must register with the sending device first". There is
     * no path in this app that hands a ticket on in the clear.
     */
    suspend fun resealTicket(
        ticketBase64: String?,
        recipientPublicKeys: List<String>
    ): SealedTicket
}

/**
 * This device's public identity. Deliberately has no private-key field:
 * the key lives in the engine and in the host's storage, and nothing that
 * draws a screen has any business holding it.
 *
 * [fingerprint] is eight hex characters — the comparison two people
 * actually make in a doorway. §20.9 is explicit that four bytes is not a
 * cryptographic binding and does not claim to be one.
 */
data class DeviceIdentity(
    val publicKeyBase64: String,
    val fingerprint: String,
    val name: String
)

/**
 * The identity, plus the freshly minted private key on the one call that
 * minted it. Null on every other call, including every later start.
 */
data class DeviceKeyLoad(
    val identity: DeviceIdentity,
    val mintedPrivateKey: String? = null
)

/** Another device's PMV1 card, as it arrived. */
data class DeviceCard(
    val publicKeyBase64: String,
    val name: String,
    val fingerprint: String
)

/** This phone's own card, ready to be shown and to be sent. */
data class DeviceCardArtifact(
    val identity: DeviceIdentity,
    val url: String,
    val qr: QrMatrix?
)

/**
 * The outcome of sealing a ticket to somebody. [ticketBase64] is
 * ciphertext — the only form a ticket travels in since §20.9 — and
 * [code] says which refusal this was when there is none.
 */
data class SealedTicket(
    val ticketBase64: String? = null,
    val reason: String? = null,
    val code: String? = null
)

/**
 * Why an artifact could not be opened, as the library codes it.
 *
 * The distinction is the point: [NOT_ENROLLED] is a perfectly good job
 * for somebody else and has an action attached — get enrolled, ask for a
 * fresh seal — while [UNREADABLE] is bytes this build cannot parse, which
 * no amount of re-sending fixes. A driver told the wrong one goes and
 * does the wrong thing.
 */
object SealError {
    const val NOT_ENROLLED = "sealed-for-another-device"
    const val UNREADABLE = "unreadable"
    /** This phone has no identity yet, so nothing sealed can be opened here. */
    const val NO_DEVICE_KEY = "no-device-key"
    /** Nothing to seal *to*: the roster is empty, or the target is not on it. */
    const val NO_RECIPIENT = "not-enrolled"
    /**
     * A job that arrived in the clear. Refused rather than opened: its
     * plan travelled readable by anyone who handled it, and so did the
     * seed that publishes the vehicle.
     */
    const val UNSEALED = "unsealed"
}

/**
 * What a ticket says, or why it could not be read.
 *
 * The code travels beside the offer because "this job was sealed for
 * another device" and "these bytes are not a job" send the driver to
 * different places, and a single null offer cannot tell them apart.
 */
data class JobDescription(
    val offer: JobOffer? = null,
    val code: String? = null,
    /** Whether the artifact arrived sealed, which since §20.9 it should. */
    val sealed: Boolean = false,
    /**
     * How this award stands against each offer the device bid on, one
     * entry per held bid and in the same order (§20.4). Empty when
     * nothing was bid on — a direct dispatch, which is the ordinary case
     * and says nothing about the award either way.
     */
    val offerChecks: List<AwardCheck> = emptyList()
)

/**
 * `awardMatchesOffer`'s answer for one held bid.
 *
 * [index] is the bid's position in the list that was handed down, which
 * is how the answer finds its way back to the offer it is about: a bid
 * this build can no longer decode still occupies its place, where
 * dropping it would shift every later verdict onto the wrong job.
 *
 * [reason] names the check that failed — the core orders them so a
 * swapped plan reads as `planRef`, because a courier told "planRef"
 * knows the plan was changed where "this does not match" tells them
 * nothing they can argue with.
 */
data class AwardCheck(
    val index: Int,
    val ok: Boolean,
    val reason: String? = null
)

/** What a broadcast offer says, or why it could not be read. */
data class OfferDescription(
    val offer: OfferSummary? = null,
    val kind: String? = null,
    val error: String? = null
)

/**
 * A dispatcher's public offer (threads §20.4), exactly as coarse as it
 * arrived.
 *
 * There is no stop in this type and there must never be one: an offer is
 * broadcast to a pool of couriers, most of whom will not win, so
 * everything in it is disclosed to strangers for free. What it carries is
 * a commitment ([planRefHex]) plus the descriptors a courier prices
 * against.
 *
 * [centroidLat]/[centroidLon] are **not a position**. They are the corner
 * of a [gridDegrees]-wide cell the whole round sits somewhere inside — a
 * point usually with nothing at it, since it is the mean of the stops —
 * and [spread] says how far the round ranges from it in one of five
 * buckets rather than as a radius, because an exact centre plus an exact
 * radius is a disc that two or three offers trilaterate.
 *
 * [offerBase64] is the artifact itself, kept because a bid recorded now
 * has to be re-checkable against an award that arrives hours later, and
 * that check compares bytes rather than summaries.
 */
data class OfferSummary(
    val offerBase64: String,
    val jobIdHex: String,
    /** The commitment: the first 8 bytes of SHA-256 over the plan. */
    val planRefHex: String,
    val stopCount: Int,
    val travelModeName: String? = null,
    /** §11, as the issuer decided it: a live locator, or stop events only. */
    val fine: Boolean = true,
    val centroidLat: Double,
    val centroidLon: Double,
    /** The grid the centroid was snapped to, in degrees. 0.01° ≈ 1.1 km. */
    val gridDegrees: Double,
    /** Bucket 0–4: ≤1 km, ≤3 km, ≤10 km, ≤30 km, beyond. */
    val spread: Int,
    /** The bucket's upper edge, or null in the top bucket. */
    val spreadMaxMeters: Int? = null,
    /** Exact, because the size of the work is not a location. */
    val totalMeters: Long,
    /**
     * Pay in minor units, or null when the dispatcher did not state it.
     * Zero is a statement — a job offered for nothing — and null is
     * silence, so the two never collapse into each other.
     */
    val payMinor: Long? = null,
    /** ISO 4217, and only ever present with [payMinor]. */
    val currency: String? = null,
    /** A short public line the dispatcher wrote, or null. Not an address. */
    val label: String? = null,
    val notAfter: Long,
    val issuerHex: String,
    /**
     * The protocol's verdict: signature and expiry. False means these
     * bytes are not an offer anybody stands behind, and [reason] says
     * which check failed.
     */
    val ok: Boolean = false,
    val reason: String? = null
)

/**
 * What arrived, and whether this build can read it.
 *
 * [kind] is decided by the artifact's magic, never by whether it decodes:
 * a ticket issued before a wire change is an unreadable *ticket*, and its
 * holder is owed "ask the dispatcher for a fresh one" rather than the
 * link decoder's complaint about a byte length. [reason] is null when the
 * artifact reads cleanly, and set when [kind] is known but unusable.
 */
data class MeshArtifact(
    val kind: String? = null,
    val reason: String? = null,
    /**
     * Whether a ticket arrived as PME1 ciphertext (§20.9), which is the
     * only form one should travel in. A sealed ticket carries a *reason*
     * — "this is a job, sealed to a device" — and that is information, not
     * a fault: [unreadable] is what says the bytes could not be parsed.
     */
    val sealed: Boolean = false,
    val unreadable: Boolean = false,
    /** The card, when what arrived was another phone's identity. */
    val device: DeviceCard? = null
) {
    val isTicket: Boolean get() = kind == "ticket"

    /**
     * A dispatcher's broadcast offer (§20.4), which is worth a different
     * screen from a ticket: it grants nothing, publishes nothing and
     * names no address, and the decision in front of it is whether to
     * bid rather than whether to drive.
     */
    val isOffer: Boolean get() = kind == "offer"

    /** Another device offering to be enrolled (§20.9). */
    val isDeviceCard: Boolean get() = kind == "device"
    /** Known to be a ticket, and unreadable — a different notice from "not a capability". */
    val isUnreadableTicket: Boolean get() = isTicket && unreadable
}

/**
 * A dispatched job, as its ticket describes it.
 *
 * [ok] is the protocol's verdict, not a well-formedness check: a ticket
 * bound to last week's graph epoch parses perfectly and cannot be
 * published, and a ticket sealed for another phone is intact and
 * unopenable here. [reason] says which.
 *
 * This is the awarded job, never the broadcast one. A dispatcher's
 * *offer* (PMJ1, §20.4) is a different artifact carrying no stop at
 * all — a commitment plus coarse descriptors — so it cannot be
 * described by this type and does not try to be.
 */
data class JobOffer(
    val jobIdHex: String,
    val stops: List<JobStop>,
    /** §11, the issuer's decision: a live locator, or stop events only. */
    val fine: Boolean,
    /**
     * How the run moves, as a routing profile name ("car", "bike",
     * "foot"), or null when the plan did not say. A day of deliveries by
     * bike is a different day from the same addresses by van, and the
     * driver is entitled to see which one they are being offered.
     */
    val travelModeName: String? = null,
    /** Absolute expiry in unix seconds. What bounds a leaked ticket. */
    val notAfter: Long,
    val issuerHex: String,
    val ok: Boolean,
    val reason: String? = null
) {
    /**
     * Where the job begins. A plan is minted in optimized visit order, so
     * the head of the list is delivery 1 — the address the driver leaves
     * for now.
     */
    val firstStop: JobStop? get() = stops.firstOrNull()
}

/**
 * One doorstep, and what the dispatcher said about it (threads §20.8).
 *
 * All four optional fields are null when the dispatcher did not state
 * them, and null is the *only* form of unstated: `""` would be a stated
 * empty reference and [parcels] `0` is a stated zero — a collection, a
 * survey, a retry with nothing in the van for this door. The difference
 * is the reason the wire flags these fields instead of always sending
 * them, so nothing above this line may collapse it.
 *
 * None of it is publishable. It lives in the plan, the plan reaches this
 * device inside the ticket, and the wire carries an 8-byte `planRef`
 * hash of the plan and no field that could hold an order reference, an
 * instruction or a customer's phone number.
 */
data class JobStop(
    val index: Int,
    val lat: Double,
    val lon: Double,
    val label: String,
    /** The dispatcher's reference for this drop — read aloud, typed elsewhere. */
    val orderRef: String? = null,
    /** How many parcels are for this door. Zero is a statement, not a blank. */
    val parcels: Int? = null,
    /** What to do at the door, in the dispatcher's own words. */
    val instructions: String? = null,
    /** Who to reach at this door. Personal data: shown, dialled, never sent. */
    val contact: String? = null
) {
    val point: LatLon get() = LatLon(lat, lon)
}

/**
 * A contact as a `tel:` URI can dial it, or null when there is no number
 * in it at all.
 *
 * A dispatcher writes phone numbers the way people write them — "+1 (514)
 * 555-0142 poste 12" — and `tel:` with spaces, brackets and prose in it
 * reaches the dialler as something nobody can call. Digits and a leading
 * `+` survive; the usual separators are dropped.
 *
 * Scanning **stops** at the first character that is neither a digit nor a
 * separator, rather than filtering the whole string: an extension's digits
 * pulled onto the end of the number would dial a different subscriber,
 * which is worse than not dialling. What was typed is what stays on
 * screen — this only decides what the dial button sends.
 *
 * Fewer than three digits is not a number anyone can be called on, so it
 * gets no call action rather than a URI the dialler will reject.
 */
fun dialableNumber(contact: String?): String? {
    val text = contact?.trim().orEmpty()
    if (text.isEmpty()) return null
    val out = StringBuilder()
    var digits = 0
    for ((position, ch) in text.withIndex()) {
        when {
            // A `+` is the country prefix only at the front; anywhere else
            // it separates two numbers, and the first one is the answer.
            ch == '+' && position == 0 -> out.append(ch)
            ch in '0'..'9' -> {
                // ASCII only: a `tel:` URI is not a place for Arabic-Indic
                // digits, which `Char.isDigit` would happily accept.
                out.append(ch)
                digits++
            }
            ch.isWhitespace() || ch in PHONE_SEPARATORS -> Unit
            else -> break
        }
    }
    return if (digits >= 3) out.toString() else null
}

/**
 * Written into phone numbers by people, meaningless to the network.
 *
 * The two non-breaking spaces are escaped rather than typed: a French
 * dispatcher's keyboard puts them between groups, they are
 * indistinguishable from an ordinary space in this file, and
 * `Char.isWhitespace` answers false for both.
 */
private val PHONE_SEPARATORS = setOf(
    '-', '.', '(', ')', '\u00A0', '\u202F', '\u2010', '\u2011', '\u2012', '\u2013', '\u2014'
)

/**
 * A ticket this device has taken and is working through.
 *
 * A dispatch ticket is a *day*, not a destination: it names N addresses in
 * visit order, and the driver reaches each in turn. What is held here is
 * the plan plus how far along it the run has got — [nextIndex] is the
 * 1-based number of the stop being driven to now, which makes it also the
 * 0-based index of the one after it.
 *
 * [ticketBase64] is the capability itself. It is kept because handing the
 * run to another driver means handing on exactly those bytes, and because
 * the engine's own copy dies with its host while a delivery day outlives
 * the app process several times over.
 */
data class ActiveJob(
    val stops: List<JobStop>,
    val nextIndex: Int,
    val ticketBase64: String?,
    val jobIdHex: String,
    val notAfter: Long,
    /**
     * What the driver has said about each stop, one entry per stop, in the
     * plan's own order. Pending is the absence of an assertion rather than
     * a claim that nothing happened, which is why a fresh run is all
     * pending and why nothing but an explicit mark ever changes an entry.
     */
    val outcomes: List<Int> = List(stops.size) { StopOutcome.PENDING },
    /**
     * §11, carried over from the ticket that started the run: true when
     * this run publishes a live position, false when it publishes stop
     * events only.
     *
     * Kept on the run rather than asked of the engine because it decides
     * what the driver is *offered*: on a coarse run a doorstep photo would
     * hand over, out of band, the position the operator decided this
     * audience is not entitled to — so the camera is not put in front of
     * them at all. The library refuses it too; this is so the refusal never
     * has to happen.
     */
    val fine: Boolean = true
) {
    val total: Int get() = stops.size

    /** The address the van is headed for now. */
    val currentStop: JobStop? get() = stops.getOrNull(nextIndex - 1)

    /** The one after it, or null when this is the last doorstep. */
    val followingStop: JobStop? get() = stops.getOrNull(nextIndex)

    /** What was said about the stop at [position], 1-based. */
    fun outcomeOf(position: Int): Int =
        outcomes.getOrElse(position - 1) { StopOutcome.PENDING }

    /** The same run with [position] marked, leaving every other stop alone. */
    fun withOutcome(position: Int, outcome: Int): ActiveJob {
        if (position < 1 || position > stops.size) return this
        val updated = List(stops.size) { i ->
            if (i == position - 1) outcome else outcomes.getOrElse(i) { StopOutcome.PENDING }
        }
        return copy(outcomes = updated)
    }

    val deliveredCount: Int get() = outcomes.count { it == StopOutcome.DELIVERED }

    /** Skipped and failed together: both mean nobody is coming back today. */
    val missedCount: Int
        get() = outcomes.count { it == StopOutcome.SKIPPED || it == StopOutcome.FAILED }
}

/**
 * What a published run's plan is, given what this device is doing.
 *
 * A dispatch run wins over the pinned destination: the day is the plan, and
 * a run published with one stop tells every customer holding the link
 * nothing about the eleven doorsteps in front of theirs.
 *
 * The whole plan goes out, in plan order, **including the stops already
 * dealt with** — not the remainder. The wire keys its outcome map by
 * position in this list (`outcomes[index - 1]`, validated against
 * `1..stops.length`), and the app marks a stop with its absolute position
 * in [ActiveJob]. Publishing stops k..N would renumber every one of them,
 * so marking stop 7 of a twelve-stop day would land on whatever the list's
 * seventh entry had become and mis-attribute a delivery to the wrong
 * doorstep, silently.
 *
 * Sending the whole plan discloses nothing either way: a plan is local to
 * the publisher — dwell detection and the outcome map are all it feeds —
 * and the wire carries an 8-byte `planRef` hash, never a coordinate. There
 * is no address here to withhold from a follower.
 */
fun shareStopsFor(job: ActiveJob?, selected: LatLon?): List<LatLon> = when {
    job != null && job.stops.isNotEmpty() -> job.stops.map { it.point }
    selected != null -> listOf(selected)
    // No plan at all, which the channel accepts: a run with no stops is a
    // live position and nothing else.
    else -> emptyList()
}

/**
 * Which final record closing this run out is entitled to emit.
 *
 * [COMPLETED] is a claim about the world — every customer holding the link
 * and every dispatcher watching is told the run arrived — so it is owed
 * only when every stop in the plan has an outcome the driver asserted. A
 * day with stops still pending was abandoned, whichever button ended it,
 * and [CANCELED] is the only ending that does not lie about it.
 */
enum class JobFinalRecord { COMPLETED, CANCELED }

/**
 * The final record a run ending now may publish.
 *
 * Pure and separate from the view model because it is the one place the
 * invariant lives: no path may publish COMPLETED for a day that was
 * abandoned. A null [job] is a plain shared drive with no plan behind it —
 * there is no unresolved stop to be dishonest about, and the driver
 * pressing stop is the end of it.
 */
fun finalRecordFor(job: ActiveJob?): JobFinalRecord = when {
    job == null || job.stops.isEmpty() -> JobFinalRecord.COMPLETED
    job.stops.indices.any { job.outcomes.getOrElse(it) { StopOutcome.PENDING } == StopOutcome.PENDING } ->
        JobFinalRecord.CANCELED
    else -> JobFinalRecord.COMPLETED
}

/** How many stops nobody has said anything about yet. */
fun unresolvedStops(job: ActiveJob?): Int {
    if (job == null) return 0
    return job.stops.indices.count {
        job.outcomes.getOrElse(it) { StopOutcome.PENDING } == StopOutcome.PENDING
    }
}

/** What a PMTP record can carry of a driver's note (threads §5.3). */
const val MAX_NOTE_BYTES = 64

/**
 * A driver's note trimmed to what a record can carry.
 *
 * Cut on a code-point boundary, never mid-character: a note truncated
 * through the middle of an accented letter or an emoji reaches the customer
 * as a replacement glyph. Blank is null — an empty note is the absence of
 * one, not a zero-length remark. The dialog caps typing at the same bound;
 * this is what makes the bound true regardless of caller, because a note
 * the codec refuses would otherwise take the whole final record with it.
 */
fun truncateNote(note: String?, maxBytes: Int = MAX_NOTE_BYTES): String? {
    val trimmed = note?.trim()?.takeIf { it.isNotEmpty() } ?: return null
    if (trimmed.toByteArray(Charsets.UTF_8).size <= maxBytes) return trimmed
    val kept = StringBuilder()
    var bytes = 0
    var index = 0
    while (index < trimmed.length) {
        val point = trimmed.codePointAt(index)
        val chars = Character.charCount(point)
        val piece = trimmed.substring(index, index + chars)
        val cost = piece.toByteArray(Charsets.UTF_8).size
        if (bytes + cost > maxBytes) break
        kept.append(piece)
        bytes += cost
        index += chars
    }
    return kept.toString().trim().takeIf { it.isNotEmpty() }
}

/**
 * §5.2 stop outcomes, as the wire numbers them.
 *
 * [PENDING] is the absence of an assertion, never the claim that a van has
 * not arrived — a UI that drew the two the same way would be saying
 * something the protocol deliberately refuses to.
 */
object StopOutcome {
    const val PENDING = 0
    const val DELIVERED = 1
    const val SKIPPED = 2
    const val FAILED = 3
}

/** Why a stop was not delivered. The wire carries the code, the app the words. */
object StopReason {
    const val NONE = 0
    const val CUSTOMER_ABSENT = 1
    const val REFUSED = 2
    const val INACCESSIBLE = 3
    const val PARCEL_MISSING = 4
    const val OTHER = 5
}

/** The most recent mark a followed run published, with its reason. */
data class StopMark(val stopIndex: Int, val outcome: Int, val reasonCode: Int)

/**
 * The outcome of accepting. [customerUrl] is the read-only follow link
 * derived from the ticket itself, which is why it is the same link the
 * dispatcher could print before any driver existed. [ticketBase64] is the
 * publish capability the run was taken with, kept so the day can survive a
 * restart and be handed on afterwards.
 */
data class AcceptedJob(
    val action: MeshAction,
    val job: JobOffer? = null,
    val customerUrl: String? = null,
    val ticketBase64: String? = null
)

/**
 * A QR symbol as rows of packed modules: one hex digit per four modules,
 * most significant bit leftmost, each row padded to a nibble boundary.
 *
 * A version-2 symbol is 625 modules and a large one over 30 000; shipping
 * that across the JS bridge as an array of booleans, for something the
 * screen redraws, is not worth doing.
 *
 * [tooBig] is a plan that will not fit in a symbol a hand-held camera can
 * resolve. It is deliberately an answer rather than an error: there *is* a
 * way to hand that job over, it just is not a picture, and a dialog that
 * said "unavailable" would be hiding the way that works.
 */
data class QrMatrix(
    val size: Int,
    val rows: List<String>,
    val tooBig: Boolean = false
) {
    val isEmpty: Boolean get() = size <= 0 || rows.isEmpty()

    /** Whether the module at [x],[y] is dark. Outside the symbol is light. */
    fun module(x: Int, y: Int): Boolean {
        if (x < 0 || y < 0 || x >= size || y >= size) return false
        val row = rows.getOrNull(y) ?: return false
        val nibble = x / 4
        if (nibble >= row.length) return false
        val value = Character.digit(row[nibble], 16)
        if (value < 0) return false
        return (value shr (3 - (x % 4))) and 1 == 1
    }
}

/**
 * A locally computed arrival. [basis] says which metric produced it and
 * [positionBasis] whether it started from a reported position or, in
 * coarse mode, from the last stop the run reported serving — the
 * accuracy trade §11 describes, stated rather than hidden.
 *
 * [secondsFromNow] is null when there is no arrival to state: a stop the
 * driver marked skipped or failed is not going to be visited, and any
 * number at all would be a lie with a decimal point on it. [profileBasis]
 * is "matched", "mismatched" or "unstated" — a bike courier's arrival
 * computed on this device's car graph is worth showing and not worth
 * showing silently.
 */
data class FollowedEta(
    val secondsFromNow: Double?,
    val basis: String,
    val positionBasis: String,
    val stale: Boolean,
    val outcome: Int = StopOutcome.PENDING,
    val reasonCode: Int? = null,
    /** The routing profile the answer was actually computed on. */
    val profile: String? = null,
    val profileBasis: String = "unstated",
    val travelModeMismatch: Boolean = false,
    /** How the run says it travels, or null when it did not say. */
    val travelModeName: String? = null
) {
    /** The driver said this stop is not being made, so there is no time. */
    val marked: Boolean get() = basis == "marked"
}

/**
 * What the mesh is doing right now.
 *
 * [contributing] false means this phone only reads traffic. [suppressed]
 * counts fixes the protocol's own gates declined to publish, which is the
 * normal case under the reticent profile and not a fault. [mode] is
 * "mesh" (a real keeper), "demo" (the on-device transport, used when no
 * keeper is configured) or "off".
 */
data class PulseMeshStatus(
    val available: Boolean,
    val mode: String = "off",
    /**
     * Whether demo vehicles are producing what the map draws. A separate
     * question from [mode], and the one the map has to label: demo mode
     * with the vehicles switched off shows only what real peers said.
     */
    val simulated: Boolean = false,
    val contributing: Boolean = false,
    val readOnly: Boolean = false,
    val epoch: String = "",
    val peers: Int = 0,
    val records: Int = 0,
    val segments: Int = 0,
    val zones: Int = 0,
    val fixes: Int = 0,
    val emitted: Int = 0,
    val suppressed: Int = 0,
    val incidents: Int = 0,
    val lastReason: String? = null,
    val threadsAvailable: Boolean = false,
    /** The taxonomy a report sheet is built from, straight from the protocol. */
    val incidentTypes: List<IncidentType> = emptyList(),
    val sharing: SharedDrive? = null,
    val error: String? = null
) {
    val running: Boolean get() = available && mode != "off"
}

data class IncidentType(val type: Int, val name: String, val informational: Boolean)

/**
 * A run this device is publishing. [fromTicket] means the run belongs to a
 * dispatched job rather than to this phone — which is what makes handover
 * possible at all: the seed came in with the ticket, so it can go out with
 * it again (§20.5).
 */
data class SharedDrive(
    val url: String,
    val seq: Int,
    val jobIdHex: String? = null,
    val fromTicket: Boolean = false
)

/** The result of a user action the protocol may decline, with its reason. */
/**
 * Whether the protocol accepted what the app asked it to say, and why not.
 *
 * [photoRefused] singles out the one refusal the driver can do something
 * about on the spot: a proof-of-delivery photo is sealed before the outcome
 * is recorded, so a refused photo refuses the whole mark, and the honest
 * thing to offer is marking the stop again without the picture.
 */
data class MeshAction(
    val emitted: Boolean,
    val reason: String? = null,
    val photoRefused: Boolean = false,
    /**
     * Which refusal this was, when the protocol named one — see
     * [SealError]. A sealed job that belongs to another device is not a
     * failure the driver can retry their way out of, and the sentence in
     * front of them has to say so.
     */
    val code: String? = null
)

/**
 * One segment of live traffic. [ratio] is the observed speed over the
 * static free-flow speed of that road; [reports] is how many independent
 * observations are behind it, and below the protocol's hint threshold
 * there is no aggregate at all.
 */
data class TrafficSegment(
    val segment: String,
    val points: List<LatLon>,
    val speedKmh: Double,
    val freeflowKmh: Double?,
    val ratio: Double?,
    val level: String,
    val reports: Int,
    val confidence: Double,
    val ageSeconds: Int
)

/**
 * A scored incident. [tier] is "shown" when enough distinct peers have
 * corroborated it to display as fact, "hint" when they have not — and a UI
 * that renders the two identically is making a claim the mesh did not.
 */
data class MeshIncident(
    val key: String,
    val type: Int,
    val typeName: String,
    val lat: Double,
    val lon: Double,
    val score: Double,
    val sources: Int,
    val tier: String,
    val informational: Boolean,
    val ageSeconds: Int
)

/**
 * A followed run. [claim] is the sentence the UI is allowed to say, taken
 * from the subscriber rather than composed here: "bus expected 07:44" and
 * "bus is here, arriving 07:44" are different claims, and a stale position
 * must never be presented as the second.
 */
data class FollowedDrive(
    val live: Boolean,
    val hasPosition: Boolean,
    val claim: String,
    val ageSeconds: Int?,
    val lat: Double?,
    val lon: Double?,
    /**
     * How the run says it travels, as a profile name, or null when it did
     * not say. Not a §12 claim and deliberately outside the rows: it does
     * not change what this device knows about the position.
     */
    val travelModeName: String? = null,
    /**
     * What the driver has said about each stop, cumulative in every
     * record — so the newest update carries the whole day and a follower
     * who joined late sees no holes.
     */
    val outcomes: List<Int> = emptyList(),
    val lastOutcome: StopMark? = null
) {
    val deliveredCount: Int get() = outcomes.count { it == StopOutcome.DELIVERED }
    val missedCount: Int
        get() = outcomes.count { it == StopOutcome.SKIPPED || it == StopOutcome.FAILED }
    /** Whether anyone has said anything at all yet. */
    val hasOutcomes: Boolean get() = deliveredCount + missedCount > 0
}

data class LatLon(val lat: Double, val lon: Double)

data class RoutingInfo(
    val routing: Boolean,
    val routingError: String?,
    val profile: String,
    val routeBounds: RouteBounds?
)

data class RegionManifest(
    val files: List<String>,
    val profile: String,
    val nodes: Long,
    val leaves: Int
)

data class EngineInfo(
    val attribution: String,
    val license: String,
    val total: Long,
    val routing: Boolean,
    val routingError: String?,
    val profile: String,
    /** Extent of the route graph. Search and the basemap are worldwide; this is not. */
    val routeBounds: RouteBounds?
)

/**
 * Extent of the route graph: the overall envelope plus the leaf boxes that
 * tile it. The envelope alone spans neighbouring countries the extract never
 * covered, so containment is tested per leaf.
 */
data class RouteBounds(
    val minLat: Double,
    val maxLat: Double,
    val minLon: Double,
    val maxLon: Double,
    /** Flat [minLat, maxLat, minLon, maxLon] runs, one per leaf cell. */
    val cells: DoubleArray
) {
    fun contains(point: LatLon): Boolean {
        // Cheap envelope reject first.
        if (point.lat < minLat - MARGIN_DEG || point.lat > maxLat + MARGIN_DEG) return false
        if (point.lon < minLon - MARGIN_DEG || point.lon > maxLon + MARGIN_DEG) return false
        if (cells.isEmpty()) return true

        var i = 0
        while (i + 3 < cells.size) {
            if (point.lat >= cells[i] - MARGIN_DEG && point.lat <= cells[i + 1] + MARGIN_DEG &&
                point.lon >= cells[i + 2] - MARGIN_DEG && point.lon <= cells[i + 3] + MARGIN_DEG
            ) {
                return true
            }
            i += 4
        }
        return false
    }

    override fun equals(other: Any?): Boolean =
        other is RouteBounds && minLat == other.minLat && maxLat == other.maxLat &&
            minLon == other.minLon && maxLon == other.maxLon && cells.contentEquals(other.cells)

    override fun hashCode(): Int =
        (((minLat.hashCode() * 31 + maxLat.hashCode()) * 31 + minLon.hashCode()) * 31 +
            maxLon.hashCode()) * 31 + cells.contentHashCode()

    private companion object {
        /** Slightly beyond the engine's 250 m snap limit, in degrees. */
        const val MARGIN_DEG = 0.004
    }
}

data class Place(
    val id: String,
    val name: String,
    val address: String,
    val locality: String,
    val category: String,
    val type: String,
    val lat: Double,
    val lon: Double,
    val distanceMeters: Double?
) {
    val point: LatLon get() = LatLon(lat, lon)
}

data class SearchOutcome(val places: List<Place>, val total: Long)

data class Suggestion(
    val text: String,
    val mainText: String,
    val secondaryText: String,
    val kind: String,
    /** Canonical query text for this prediction (may differ from [text]). */
    val selectionQuery: String,
    /** Shard that owns the predicted place; turns search into a direct lookup. */
    val selectionShards: List<String>
)

data class RouteStep(
    val name: String,
    val meters: Double,
    val seconds: Double,
    val at: Int,
    /** Posted limit in km/h, 0 when the way carries no maxspeed tag. */
    val speedLimitKmh: Int,
    /**
     * Movements allowed from each lane of the approach to this step, left to
     * right, as a bit set per lane. Empty when the road carried no lane tags;
     * a zero entry is a lane whose movements are unknown.
     */
    val lanes: List<Int> = emptyList(),
    /**
     * The kind of road this step runs on — "motorway", "motorway_link" and
     * so on. A slip road is unnamed far more often than not, so its class is
     * the only thing that says it is a ramp at all.
     */
    val roadClass: String = "",
    /**
     * The road's own number, as written on the sign: "40", "A 13". On a
     * motorway this is the only label a driver can act on — the name is
     * never posted, and "Autoroute Félix-Leclerc" describes a road nobody
     * can find a sign for.
     */
    val ref: String = "",
    /** The exit number off the green panel: "32", "89-N". */
    val exitRef: String = "",
    /**
     * What a slip road leads to, numbered and with its cardinal: "20 Est",
     * "25 Nord;40". This is where a direction actually comes from — OSM
     * tags direction on only a handful of route relations, but it is on
     * thousands of ramps, because that is what the sign says.
     */
    val destinationRef: String = "",
    /** The places named on the sign: "Montréal;Québec". */
    val destination: String = "",
    /** Whether this step runs inside a roundabout. */
    val roundabout: Boolean = false,
    /** Which exit of that roundabout the route leaves by; 0 when unknown. */
    val roundaboutExit: Int = 0
) {
    /** A crossing by boat rather than by road. */
    val isFerry: Boolean get() = roadClass == "ferry"

    /**
     * The label to guide by: the number if the road has one, its name
     * otherwise. Semicolon lists are cut to their first entry — a banner has
     * room for one answer, and the first is the one the sign leads with.
     */
    val signLabel: String
        get() = firstOf(ref).ifBlank { name }

    /** Where a slip road leads, as the sign puts it: "20 Est". */
    val towardLabel: String
        get() = firstOf(destinationRef).ifBlank { firstOf(destination) }
}

private fun firstOf(list: String): String =
    list.substringBefore(';').trim()

/** Lane movement bits, matching the route index's own encoding. */
object LaneTurn {
    const val REVERSE = 1
    const val SHARP_LEFT = 2
    const val LEFT = 4
    const val SLIGHT_LEFT = 8
    const val THROUGH = 16
    const val SLIGHT_RIGHT = 32
    const val RIGHT = 64
    const val SHARP_RIGHT = 128
}

/** 1 signals, 2 stop, 3 give way, 4 level crossing, 5 crossing. */
data class RouteJunction(
    val kind: Int,
    val lat: Double,
    val lon: Double,
    val atMeters: Double
)

/**
 * A posted limit and the distance along the route where it takes effect.
 *
 * Limits belong to distance rather than to steps because a street is not a
 * limit: an autoroute drops through an interchange and climbs back without
 * ever changing its name, and one number for the whole street is the wrong
 * number for however much of it disagrees.
 */
data class SpeedLimitChange(
    val atMeters: Double,
    val limitKmh: Int,
    /** A lower limit that applies only at certain times, or null. */
    val conditional: ConditionalLimit? = null
)

/**
 * A limit in force only during a window — a school zone, in practice.
 *
 * The index cannot resolve this: it is static, and which limit applies
 * depends on the clock. So the window travels to the client and is answered
 * against the device's own local time, which is also the only clock that
 * agrees with the sign the driver is looking at.
 *
 * [days] is a 7-bit mask from Monday. The month range is inclusive and may
 * wrap, because a school year runs September to June.
 */
data class ConditionalLimit(
    val limitKmh: Int,
    val days: Int,
    val startMinute: Int,
    val endMinute: Int,
    val monthStart: Int,
    val monthEnd: Int
) {
    fun appliesAt(time: java.time.LocalDateTime): Boolean {
        val month = time.monthValue
        val inMonths = if (monthStart <= monthEnd) month in monthStart..monthEnd
        // September to June is one range that crosses the new year, not two.
        else month >= monthStart || month <= monthEnd
        if (!inMonths) return false
        // DayOfWeek is 1=Monday, and the mask counts from Monday too.
        if ((days shr (time.dayOfWeek.value - 1)) and 1 == 0) return false
        val minute = time.hour * 60 + time.minute
        return minute in startMinute until endMinute
    }
}

data class Route(
    /**
     * How long the drive takes, live traffic included when the mesh has
     * any. Deliberately the *one* duration in this model: an ETA that
     * quietly stayed on the static metric while a jam was drawn on the map
     * would be the app disagreeing with itself.
     */
    val seconds: Double,
    val distanceMeters: Double,
    val geometry: List<LatLon>,
    val steps: List<RouteStep>,
    val junctions: List<RouteJunction>,
    val speedLimits: List<SpeedLimitChange>,
    val httpRequests: Int,
    val bytesFetched: Long,
    /** The same drive under the static metric alone. */
    val staticSeconds: Double = seconds,
    /** Segments the mesh had an opinion on that this route passes through. */
    val liveSegments: Int = 0
) {
    /** What live traffic added, in seconds. Negative when it cleared. */
    val liveDelaySeconds: Double get() = seconds - staticSeconds
    val hasLive: Boolean get() = liveSegments > 0
}

/**
 * The limit in force [meters] into the trip, 0 where none is known.
 *
 * [now] decides conditional limits. A school zone posted 50 that drops to 30
 * on school mornings is 30 at 08:15 on a Tuesday in October and 50 at the
 * same hour in July, and the sign has to say which.
 */
fun Route.speedLimitAt(
    meters: Double,
    now: java.time.LocalDateTime = java.time.LocalDateTime.now()
): Int {
    // The last change at or before this point. Routes carry a handful of
    // these, so a scan is honest and a binary search would be decoration.
    var answer: SpeedLimitChange? = null
    for (change in speedLimits) {
        if (change.atMeters > meters) break
        answer = change
    }
    val change = answer ?: return 0
    val conditional = change.conditional
    return if (conditional != null && conditional.appliesAt(now)) conditional.limitKmh
    else change.limitKmh
}

data class RouteBundle(val primary: Route, val alternatives: List<Route>)

data class SnapPoint(
    val lat: Double,
    val lon: Double,
    val distMeters: Double,
    val segment: String
) {
    val point: LatLon get() = LatLon(lat, lon)
}

class RangefindException(message: String) : Exception(message)
