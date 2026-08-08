package dev.rangefind.wayfind.ui.components

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.RoundedCornerShape
import android.content.Intent
import android.net.Uri
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Call
import androidx.compose.material.icons.filled.CheckCircle
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.LocalShipping
import androidx.compose.material.icons.filled.MoreVert
import androidx.compose.material.icons.filled.PhotoCamera
import androidx.compose.material.icons.filled.Place
import androidx.compose.material.icons.filled.RadioButtonChecked
import androidx.compose.material.icons.filled.RadioButtonUnchecked
import androidx.compose.material.icons.filled.ReportProblem
import androidx.compose.material.icons.filled.Share
import androidx.compose.ui.draw.clip
import androidx.compose.ui.platform.LocalContext
import dev.rangefind.wayfind.engine.FollowedDrive
import dev.rangefind.wayfind.engine.FollowedEta
import dev.rangefind.wayfind.ui.formatArrivalClock
import dev.rangefind.wayfind.ui.formatDuration
import androidx.compose.material3.Button
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.FilledTonalIconButton
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.res.pluralStringResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.engine.ActiveJob
import dev.rangefind.wayfind.engine.JobOffer
import dev.rangefind.wayfind.engine.JobStop
import dev.rangefind.wayfind.engine.dialableNumber
import dev.rangefind.wayfind.engine.MAX_NOTE_BYTES
import dev.rangefind.wayfind.engine.MeshIncident
import dev.rangefind.wayfind.engine.PulseMeshStatus
import dev.rangefind.wayfind.engine.QrMatrix
import dev.rangefind.wayfind.engine.StopMark
import dev.rangefind.wayfind.engine.StopOutcome
import dev.rangefind.wayfind.engine.StopReason
import dev.rangefind.wayfind.region.AwardBid
import dev.rangefind.wayfind.region.AwardBidVerdict

/**
 * Live traffic, in the settings sheet.
 *
 * Three separate consents live here and none of them is implied by
 * another: reading traffic, publishing your own speeds, and publishing a
 * link that lets someone watch this drive. The card also says, plainly,
 * which mesh it is on — a phone running simulated drivers because no
 * keeper is configured must not look like a phone on a real mesh.
 */
@Composable
fun LiveTrafficSection(
    status: PulseMeshStatus?,
    shareFine: Boolean,
    onEnabled: (Boolean) -> Unit,
    onContribute: (Boolean) -> Unit,
    /** The demo vehicles, with live traffic left running. Demo mode only. */
    onSimulated: (Boolean) -> Unit,
    onShareMode: (Boolean) -> Unit,
    onShareDrive: () -> Unit,
    onHandOver: () -> Unit = {},
    /**
     * Whether there is a ticket to hand on. Deliberately not "is a
     * ticket-born run live": the ticket is on disk, so a process death that
     * took the run with it left the capability intact — and a driver whose
     * phone restarted mid-day still has a job to pass to the next van.
     */
    canHandOver: Boolean = false
) {
    val running = status?.running == true

    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .fillMaxWidth()
            .clickable { onEnabled(!running) }
            .padding(start = 20.dp, end = 12.dp, top = 6.dp, bottom = 6.dp)
    ) {
        Column(Modifier.weight(1f)) {
            Text(
                stringResource(R.string.mesh_title),
                style = MaterialTheme.typography.bodyLarge,
                color = MaterialTheme.colorScheme.onSurface
            )
            Text(
                stringResource(R.string.mesh_subtitle),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        Switch(checked = running, onCheckedChange = onEnabled)
    }

    if (!running) {
        status?.error?.let { error ->
            Text(
                error,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.error,
                modifier = Modifier.padding(horizontal = 20.dp, vertical = 4.dp)
            )
        }
        return
    }

    val mesh = status ?: return
    Column(Modifier.padding(horizontal = 20.dp)) {
        Text(
            stringResource(
                when (mesh.mode) {
                    "mesh" -> R.string.mesh_mode_mesh
                    "demo" -> R.string.mesh_mode_demo
                    else -> R.string.mesh_mode_off
                }
            ),
            style = MaterialTheme.typography.labelLarge,
            color = MaterialTheme.colorScheme.primary
        )
        Text(
            stringResource(R.string.mesh_stats, mesh.peers, mesh.records, mesh.segments),
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
        if (mesh.mode == "demo") {
            Spacer(Modifier.height(4.dp))
            Text(
                stringResource(
                    // What is actually on the map. With the vehicles off,
                    // the note about them being simulated describes nothing
                    // the driver is looking at.
                    if (mesh.simulated) R.string.mesh_demo_note else R.string.mesh_demo_note_off
                ),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        // How many fixes the gates declined is the number that tells a user
        // the reticent profile is doing its job, so it is shown rather than
        // hidden as an implementation detail.
        if (mesh.contributing && mesh.fixes > 0) {
            Spacer(Modifier.height(4.dp))
            Text(
                stringResource(
                    R.string.mesh_suppressed,
                    mesh.emitted,
                    mesh.fixes,
                    mesh.lastReason.orEmpty()
                ),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }

    // Only in demo mode: on a real mesh there is nothing simulated to
    // switch, and a switch for it would imply the traffic might be.
    if (mesh.mode == "demo") {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier
                .fillMaxWidth()
                .clickable { onSimulated(!mesh.simulated) }
                .padding(start = 20.dp, end = 12.dp, top = 6.dp, bottom = 6.dp)
        ) {
            Column(Modifier.weight(1f)) {
                Text(
                    stringResource(R.string.mesh_simulated),
                    style = MaterialTheme.typography.bodyLarge,
                    color = MaterialTheme.colorScheme.onSurface
                )
                Text(
                    stringResource(R.string.mesh_simulated_note),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            Switch(checked = mesh.simulated, onCheckedChange = onSimulated)
        }
    }

    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .fillMaxWidth()
            .clickable(enabled = !mesh.readOnly) { onContribute(!mesh.contributing) }
            .padding(start = 20.dp, end = 12.dp, top = 6.dp, bottom = 6.dp)
    ) {
        Column(Modifier.weight(1f)) {
            Text(
                stringResource(R.string.mesh_contribute),
                style = MaterialTheme.typography.bodyLarge,
                color = MaterialTheme.colorScheme.onSurface
            )
            Text(
                stringResource(R.string.mesh_contribute_note),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        Switch(
            checked = mesh.contributing,
            enabled = !mesh.readOnly,
            onCheckedChange = onContribute
        )
    }

    if (mesh.threadsAvailable) {
        // §11 is a harm decision, not a bandwidth one, so the two options
        // are shown with what each costs if the link leaks — not hidden
        // behind a word like "precision".
        if (mesh.sharing == null) {
            Column(Modifier.padding(horizontal = 20.dp, vertical = 4.dp)) {
                ShareModeRow(
                    title = stringResource(R.string.mesh_share_mode_fine),
                    note = stringResource(R.string.mesh_share_mode_fine_note),
                    selected = shareFine,
                    onClick = { onShareMode(true) }
                )
                Spacer(Modifier.height(6.dp))
                ShareModeRow(
                    title = stringResource(R.string.mesh_share_mode_coarse),
                    note = stringResource(R.string.mesh_share_mode_coarse_note),
                    selected = !shareFine,
                    onClick = { onShareMode(false) }
                )
            }
        }
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier
                .fillMaxWidth()
                .clickable(onClick = onShareDrive)
                .padding(horizontal = 20.dp, vertical = 10.dp)
        ) {
            Column(Modifier.weight(1f)) {
                Text(
                    stringResource(
                        if (mesh.sharing != null) R.string.mesh_share_stop else R.string.mesh_share_button
                    ),
                    style = MaterialTheme.typography.bodyLarge,
                    color = if (mesh.sharing != null) MaterialTheme.colorScheme.error
                    else MaterialTheme.colorScheme.primary
                )
                Text(
                    stringResource(R.string.mesh_share_note),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        // A dispatched run belongs to the job rather than to this phone,
        // so it can be passed on: the ticket goes back on screen as a QR
        // and the next driver's own camera picks it up (§20.5). A drive
        // this device started owns its own seed and cannot be handed over
        // at all, which is why this row only exists for the other case.
        if (canHandOver || mesh.sharing?.fromTicket == true) {
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .fillMaxWidth()
                    .clickable(onClick = onHandOver)
                    .padding(horizontal = 20.dp, vertical = 10.dp)
            ) {
                Column(Modifier.weight(1f)) {
                    Text(
                        stringResource(R.string.mesh_handover_button),
                        style = MaterialTheme.typography.bodyLarge,
                        color = MaterialTheme.colorScheme.primary
                    )
                    Text(
                        stringResource(R.string.mesh_handover_note),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
        }
    }
}

/**
 * The map saying that what it is drawing was invented here.
 *
 * One label, deliberately, and not a second colour scheme: the demo
 * records went through the same validation and the same aggregation as any
 * other, so drawing them differently would be a claim about the protocol
 * rather than about where the vehicles came from. Styled as the
 * attribution chip is, because it is the same kind of statement — who is
 * responsible for what is on screen.
 */
@Composable
fun SimulatedTrafficChip(modifier: Modifier = Modifier) {
    Surface(
        shape = RoundedCornerShape(6.dp),
        color = MaterialTheme.colorScheme.surface.copy(alpha = 0.82f),
        modifier = modifier
    ) {
        Text(
            stringResource(R.string.mesh_demo_badge),
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(horizontal = 7.dp, vertical = 3.dp)
        )
    }
}

/**
 * What the dispatcher said this drop is: its reference and how many
 * parcels are for the door (§20.8).
 *
 * The reference is monospaced because of what happens to it next — it is
 * read aloud down a phone and typed into somebody else's system, where a
 * 0 that looks like an O is a parcel nobody can find.
 *
 * A field the dispatcher left unstated draws **nothing**: no dash, no
 * empty row, because there is no fact to report. A stated `0` draws
 * "0 parcels", which is a different thing entirely — the van is carrying
 * nothing for this door, and that is why the driver is going (a
 * collection, a survey, a retry). Collapsing either into the other
 * invents a claim.
 */
@Composable
private fun StopRefAndParcels(stop: JobStop, style: TextStyle, modifier: Modifier = Modifier) {
    val orderRef = stop.orderRef
    val parcels = stop.parcels
    if (orderRef == null && parcels == null) return
    Row(verticalAlignment = Alignment.CenterVertically, modifier = modifier) {
        if (orderRef != null) {
            Text(orderRef, style = style.copy(fontFamily = FontFamily.Monospace))
        }
        if (orderRef != null && parcels != null) {
            Text(
                stringResource(R.string.mesh_job_stop_meta_sep),
                style = style,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        if (parcels != null) {
            Text(
                pluralStringResource(R.plurals.mesh_job_stop_parcels, parcels, parcels),
                style = style
            )
        }
    }
}

/**
 * Everything the dispatcher said about the door the van is at now.
 *
 * The offer sheet deliberately shows less than this: a driver deciding
 * whether to take the day needs the references, not twelve sets of
 * instructions. Here there is exactly one stop and it is the one being
 * worked, so the instruction and the contact are worth the height.
 *
 * The contact is a dial, never a call: `ACTION_DIAL` needs no permission
 * and leaves the last press to the driver, which is the right place for
 * it when the number belongs to a customer. What is dialled is
 * [dialableNumber]'s answer; what is displayed is what the dispatcher
 * typed, because that is the form a person reads back. A contact with no
 * number in it — a name, a desk — is shown and simply does not dial.
 */
@Composable
private fun CurrentStopDetail(stop: JobStop) {
    StopRefAndParcels(
        stop,
        MaterialTheme.typography.titleSmall,
        Modifier.padding(top = 6.dp)
    )
    stop.instructions?.let { instructions ->
        Text(
            instructions,
            style = MaterialTheme.typography.bodyMedium,
            modifier = Modifier.padding(top = 4.dp)
        )
    }
    val contact = stop.contact ?: return
    val dialable = dialableNumber(contact)
    val context = LocalContext.current
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .padding(top = 6.dp)
            .then(
                if (dialable == null) Modifier else Modifier.clickable {
                    // A dialler the phone does not have is a phone that
                    // cannot ring anybody; it is not a reason to take the
                    // app down mid-delivery.
                    runCatching {
                        context.startActivity(
                            Intent(Intent.ACTION_DIAL, Uri.parse("tel:$dialable"))
                        )
                    }
                }
            )
    ) {
        Icon(
            Icons.Filled.Call,
            contentDescription = if (dialable == null) null
            else stringResource(R.string.mesh_job_stop_call),
            tint = if (dialable == null) MaterialTheme.colorScheme.onSurfaceVariant
            else MaterialTheme.colorScheme.primary,
            modifier = Modifier.size(18.dp)
        )
        Spacer(Modifier.size(8.dp))
        Text(
            contact,
            style = MaterialTheme.typography.bodyMedium,
            color = if (dialable == null) MaterialTheme.colorScheme.onSurface
            else MaterialTheme.colorScheme.primary
        )
    }
}

/**
 * A dispatched job, before it is taken (threads §20).
 *
 * A ticket is a *publish* capability: whoever holds it can move the
 * vehicle every customer holding the follow link is watching. So the
 * stops, the granularity the issuer chose, the expiry that bounds a leak
 * and the issuer's own key are all on screen before accept exists — and
 * a ticket the protocol will not publish shows why instead of a button
 * that fails.
 */
@Composable
fun JobOfferSheet(
    offer: JobOffer,
    bottomInset: Dp,
    /**
     * How this award stands against the offers this device bid on
     * (§20.4), or null when there is nothing to say. It is drawn first,
     * above the stops: whether this is the job that was advertised
     * decides whether the rest of the sheet is worth reading, and a
     * refusal shown under twelve addresses is a refusal nobody sees.
     */
    bid: AwardBid? = null,
    onAccept: () -> Unit,
    onDecline: () -> Unit
) {
    Box(
        Modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.scrim.copy(alpha = 0.32f))
            .clickable(onClick = onDecline)
    )
    Box(Modifier.fillMaxSize(), contentAlignment = Alignment.BottomCenter) {
        SheetSurface(bottomInset = bottomInset) {
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 20.dp)
            ) {
                Text(
                    stringResource(R.string.mesh_job_title),
                    style = MaterialTheme.typography.titleMedium,
                    modifier = Modifier.weight(1f)
                )
                Icon(
                    Icons.Filled.Close,
                    contentDescription = null,
                    modifier = Modifier
                        .size(28.dp)
                        .clickable(onClick = onDecline)
                )
            }

            if (bid != null) AwardBidLine(bid)

            // A multi-stop ticket is a sequence, not a set: the run is
            // already ordered, and the numbers are how the driver reads it.
            if (offer.stops.size > 1) {
                Text(
                    stringResource(R.string.mesh_job_stop_count, offer.stops.size),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.padding(horizontal = 20.dp, vertical = 4.dp)
                )
            }

            for ((position, stop) in offer.stops.withIndex()) {
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(horizontal = 20.dp, vertical = 8.dp)
                ) {
                    Icon(
                        Icons.Filled.Place,
                        contentDescription = null,
                        tint = MaterialTheme.colorScheme.primary,
                        modifier = Modifier.size(20.dp)
                    )
                    Spacer(Modifier.size(12.dp))
                    Column(Modifier.weight(1f)) {
                        Text(
                            stringResource(
                                R.string.mesh_job_stop_line,
                                position + 1,
                                stop.label.ifBlank {
                                    stringResource(R.string.mesh_job_stop_numbered, stop.index)
                                }
                            ),
                            style = MaterialTheme.typography.bodyLarge
                        )
                        // The reference and the parcel count, and
                        // deliberately not the instructions or the phone
                        // number: this list is how a driver checks the day
                        // is the one dispatch described, and twelve sets of
                        // door instructions is a list nobody reads.
                        StopRefAndParcels(stop, MaterialTheme.typography.bodySmall)
                        Text(
                            // Coordinates are written the way coordinates
                            // are written, not the way the device writes
                            // numbers: a French locale would otherwise
                            // separate the decimals and the pair with the
                            // same character.
                            String.format(
                                java.util.Locale.ROOT, "%.5f, %.5f", stop.lat, stop.lon
                            ),
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
            }

            Column(Modifier.padding(horizontal = 20.dp, vertical = 6.dp)) {
                // §11 is the issuer's decision here, not the driver's: the
                // party that chooses who gets the customer link chooses
                // what that link is worth. Beside it, how the run moves —
                // stated only when the plan stated it, because a day of
                // deliveries by bike is a different day from the same
                // addresses by van and the driver is agreeing to one.
                val shareMode = stringResource(
                    if (offer.fine) R.string.mesh_share_mode_fine
                    else R.string.mesh_share_mode_coarse
                )
                val travel = travelModeLabel(offer.travelModeName)
                Text(
                    if (travel == null) shareMode
                    else stringResource(R.string.mesh_job_mode_line, shareMode, travel),
                    style = MaterialTheme.typography.bodyMedium
                )
                Text(
                    stringResource(
                        R.string.mesh_job_expiry,
                        formatArrivalClock(
                            LocalContext.current,
                            (offer.notAfter - System.currentTimeMillis() / 1000).toDouble()
                        )
                    ),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                Text(
                    stringResource(R.string.mesh_job_issuer, offer.issuerHex.take(12)),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }

            Text(
                stringResource(R.string.mesh_job_warning),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(horizontal = 20.dp, vertical = 8.dp)
            )

            if (!offer.ok) {
                Text(
                    stringResource(R.string.mesh_job_unusable, offer.reason.orEmpty()),
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.error,
                    modifier = Modifier.padding(horizontal = 20.dp, vertical = 4.dp)
                )
            }

            Row(
                horizontalArrangement = Arrangement.End,
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 12.dp)
            ) {
                TextButton(onClick = onDecline) {
                    Text(stringResource(R.string.mesh_job_decline))
                }
                Spacer(Modifier.size(6.dp))
                // A commitment this award fails is not a warning to click
                // through: the offer and the ticket claim to be one job
                // and are not, which nothing legitimate produces.
                TextButton(onClick = onAccept, enabled = offer.ok && bid?.refuses != true) {
                    Text(stringResource(R.string.mesh_job_accept))
                }
            }
            Spacer(Modifier.height(8.dp))
        }
    }
}

/**
 * What the courier's own device makes of this award, given what they bid
 * on (§20.4).
 *
 * Three sentences and three colours, because the three cases are not one
 * decision. A commitment that *passed* is said out loud rather than left
 * silent — it is the only evidence the courier has that the day in front
 * of them is the day they priced. A commitment that failed on the same
 * job id is a refusal. An award matching none of the open bids is the
 * ambiguous case the core warns about: a swapped plan moves the job id,
 * so a swap arrives looking exactly like a second unrelated dispatch, and
 * only the driver knows which they were expecting.
 */
@Composable
private fun AwardBidLine(bid: AwardBid) {
    // A direct dispatch behaves as it always has: this device bid on
    // nothing, so there is no comparison to report and silence is the
    // whole of the honest answer.
    if (bid.verdict == AwardBidVerdict.NoBids) return

    // The ambiguous case gets the weight, because it is the one the
    // driver has to decide rather than read: a filled panel above the
    // stops, not a grey line under them.
    if (bid.verdict == AwardBidVerdict.UnrecognizedJob) {
        Surface(
            shape = RoundedCornerShape(14.dp),
            color = MaterialTheme.colorScheme.errorContainer,
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 20.dp, vertical = 8.dp)
        ) {
            Text(
                pluralStringResource(
                    R.plurals.offer_award_unknown, bid.heldCount, bid.heldCount
                ),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onErrorContainer,
                modifier = Modifier.padding(horizontal = 14.dp, vertical = 10.dp)
            )
        }
        return
    }

    Text(
        when (bid.verdict) {
            AwardBidVerdict.MatchesBid -> stringResource(R.string.offer_award_matches)
            else -> bid.reason
                ?.let { stringResource(R.string.offer_award_mismatch, it) }
                ?: stringResource(R.string.offer_award_mismatch_unnamed)
        },
        style = MaterialTheme.typography.bodyMedium,
        color = when (bid.verdict) {
            AwardBidVerdict.MismatchedBid -> MaterialTheme.colorScheme.error
            else -> MaterialTheme.colorScheme.primary
        },
        modifier = Modifier.padding(horizontal = 20.dp, vertical = 6.dp)
    )
}

/**
 * The ticket, back on screen, for the next driver's camera.
 *
 * White tile with its quiet zone drawn in: the margin *is* part of the
 * code, and a symbol laid straight onto a dark app is one no scanner will
 * find. The caption states the half of §20.5 the protocol cannot enforce
 * — two holders of one seed are indistinguishable on the wire, so the
 * previous device stopping is a human decision.
 */
@Composable
fun HandoverDialog(
    matrix: QrMatrix,
    /**
     * The device this ticket was sealed to (§20.9). What is on screen is
     * ciphertext addressed to that one phone, so the warning names it:
     * a photograph of this by anyone else is worth nothing, and whoever
     * *does* open it becomes the publisher.
     */
    targetName: String,
    onClose: () -> Unit,
    onShare: () -> Unit,
    /**
     * The other phone has it, so this one goes quiet — with no final record
     * at all (§20.5). The only exit in the app that emits nothing: both
     * devices now hold the same seed, and a goodbye from this one would tell
     * every customer the run had ended while the new driver is still out.
     */
    onHandedOver: () -> Unit
) {
    Dialog(onDismissRequest = onClose) {
        Surface(
            shape = RoundedCornerShape(22.dp),
            color = MaterialTheme.colorScheme.surface,
            modifier = Modifier.fillMaxWidth()
        ) {
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                modifier = Modifier.padding(20.dp)
            ) {
                Text(
                    stringResource(R.string.mesh_handover_title),
                    style = MaterialTheme.typography.titleMedium
                )
                Spacer(Modifier.height(14.dp))
                // A twenty-stop plan encodes into a symbol whose modules are
                // finer than a hand-held camera resolves. Drawing it anyway
                // would look like a working handover and be one nobody can
                // scan, so the reason is on screen instead — and the file,
                // which is the path that does work, is a button below.
                if (matrix.tooBig) {
                    Text(
                        stringResource(R.string.mesh_handover_too_big),
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurface
                    )
                } else {
                    Box(
                        Modifier
                            .fillMaxWidth()
                            .clip(RoundedCornerShape(16.dp))
                            .background(Color.White)
                            .padding(12.dp)
                    ) {
                        QrCanvas(matrix, Modifier.fillMaxWidth().aspectRatio(1f))
                    }
                }
                Spacer(Modifier.height(14.dp))
                Text(
                    stringResource(R.string.mesh_handover_warning, targetName),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                Spacer(Modifier.height(10.dp))
                // The half of §20.5 the protocol cannot enforce, as an
                // action rather than as advice. It publishes nothing —
                // deliberately — and the line above it says why, because
                // "stopped without saying anything" is otherwise exactly
                // what a broken app looks like.
                Text(
                    stringResource(R.string.mesh_handover_stop_note),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.End) {
                    TextButton(onClick = onHandedOver) {
                        Text(
                            stringResource(R.string.mesh_handover_stop_here),
                            color = MaterialTheme.colorScheme.error
                        )
                    }
                }
                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.End) {
                    // Offered in both cases: even a scannable ticket often
                    // has to reach a driver who is not standing here.
                    TextButton(onClick = onShare) {
                        Text(stringResource(R.string.mesh_handover_share))
                    }
                    Spacer(Modifier.size(6.dp))
                    TextButton(onClick = onClose) {
                        Text(stringResource(R.string.mesh_handover_close))
                    }
                }
            }
        }
    }
}

/**
 * How far through the day this van is.
 *
 * Shown only when a ticket is active and the phone is not already
 * navigating: mid-drive the guidance overlay is the whole screen and the
 * count belongs nowhere near it. Continue re-asks for the route to the
 * stop the run is on, which is what turns a restarted app — or a driver
 * back in the cab after twenty minutes at a door — into one tap rather
 * than an address to retype.
 */
@Composable
fun ActiveJobCard(
    job: ActiveJob,
    arrived: Boolean,
    onContinue: () -> Unit,
    onDelivered: () -> Unit,
    /**
     * The same claim with a doorstep photo attached (§20.7). Absent on a
     * coarse run, where the camera is never offered at all: see
     * [ActiveJob.fine].
     */
    onDeliveredWithPhoto: () -> Unit,
    onSkip: () -> Unit,
    /**
     * Hands the run's tracking link to whoever the driver picks. Here rather
     * than only in the settings sheet because this is where the driver is
     * while the day is running, and because the link exists already whenever
     * the phone is publishing — a run taken from a ticket included.
     */
    onShareLink: () -> Unit,
    /**
     * Passes the job itself to another driver's phone (§20.5). Behind the
     * overflow menu, and gated on there being a ticket to pass — a drive
     * this device started owns its own seed and cannot be handed over.
     */
    onHandOver: () -> Unit,
    /** Whether this run came from a ticket, which is the only kind that moves. */
    canHandOver: Boolean,
    /**
     * The driver cannot finish the day. Rare and consequential — the wire
     * says CANCELED and every customer holding a link hears it — so it is
     * behind the same menu rather than beside Delivered, where a bump in the
     * road could reach it.
     */
    onCantFinish: () -> Unit,
    /**
     * Whether the run is on the wire right now. False after a process death,
     * which took the run with it and left the plan on disk: the link is then
     * published from the stored plan on the way to the share sheet, and the
     * card says so instead of offering a link that does not exist yet.
     */
    publishing: Boolean,
    modifier: Modifier = Modifier
) {
    val stop = job.currentStop
    // The overflow belongs to the card: nothing outside it has an opinion
    // about whether the menu is open.
    var menuOpen by remember { mutableStateOf(false) }
    Box(
        modifier
            .background(
                MaterialTheme.colorScheme.surface.copy(alpha = 0.96f),
                RoundedCornerShape(16.dp)
            )
            .padding(horizontal = 14.dp, vertical = 10.dp)
    ) {
        Column {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Icon(
                    Icons.Filled.LocalShipping,
                    contentDescription = null,
                    tint = MaterialTheme.colorScheme.primary,
                    modifier = Modifier.size(22.dp)
                )
                Spacer(Modifier.size(10.dp))
                Column(Modifier.weight(1f)) {
                    Text(
                        stringResource(R.string.mesh_job_progress, job.nextIndex, job.total),
                        style = MaterialTheme.typography.bodyLarge
                    )
                    Text(
                        stop?.label?.takeIf { it.isNotBlank() }
                            ?: stringResource(R.string.mesh_job_stop_default),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                TextButton(onClick = onContinue) {
                    Text(stringResource(R.string.mesh_job_continue))
                }
                FilledTonalIconButton(
                    onClick = onShareLink,
                    modifier = Modifier.size(40.dp)
                ) {
                    Icon(
                        Icons.Filled.Share,
                        contentDescription = stringResource(R.string.mesh_job_share),
                        modifier = Modifier.size(20.dp)
                    )
                }
                // Handing the job on and giving it up are both rare, both
                // consequential, and neither is a thing to put in the row a
                // driver taps at every doorstep. A menu is one extra tap and
                // one fewer accident.
                Box {
                    IconButton(
                        onClick = { menuOpen = true },
                        modifier = Modifier.size(40.dp)
                    ) {
                        Icon(
                            Icons.Filled.MoreVert,
                            contentDescription = stringResource(R.string.mesh_job_more),
                            tint = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.size(20.dp)
                        )
                    }
                    DropdownMenu(expanded = menuOpen, onDismissRequest = { menuOpen = false }) {
                        if (canHandOver) {
                            DropdownMenuItem(
                                text = { Text(stringResource(R.string.mesh_handover_button)) },
                                onClick = {
                                    menuOpen = false
                                    onHandOver()
                                }
                            )
                        }
                        DropdownMenuItem(
                            text = {
                                Text(
                                    stringResource(R.string.mesh_job_cant_finish),
                                    color = MaterialTheme.colorScheme.error
                                )
                            },
                            onClick = {
                                menuOpen = false
                                onCantFinish()
                            }
                        )
                    }
                }
            }
            // What dispatch said about this door (§20.8). Under the address
            // because it is about that address, and above everything about
            // the run as a whole: the driver reading this card is standing
            // at one door, not managing a day.
            if (stop != null) CurrentStopDetail(stop)
            // A run the app holds without publishing it is what a process
            // death leaves behind. Sharing still works — the plan is on disk
            // — but it starts a new run under a new link, and the driver is
            // told that before they send anything to a customer.
            if (!publishing) {
                Text(
                    stringResource(R.string.mesh_job_share_republish),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.padding(top = 6.dp)
                )
            }
            // Arriving is not delivering, so the arrival says only that the
            // van is here and leaves the claim to the driver.
            if (arrived) {
                Text(
                    stringResource(R.string.mesh_job_arrived),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.primary,
                    modifier = Modifier.padding(top = 6.dp)
                )
            }
            // Always available for the stop the run is on, before, at and
            // after arrival: a driver already standing at the door gets a
            // zero-metre route that never fires an arrival, and marking is
            // what moves the day on in that case as in every other.
            Row(
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(8.dp),
                modifier = Modifier.padding(top = 8.dp)
            ) {
                Button(onClick = onDelivered) {
                    Text(stringResource(R.string.mesh_job_mark_delivered))
                }
                // Proof of delivery, one tap away and never in the way of
                // the plain answer — most deliveries need no photograph.
                // Hidden entirely on a coarse run: the operator decided
                // this run's audience does not get the vehicle's position,
                // and a doorstep photo carries one.
                if (job.fine) {
                    FilledTonalIconButton(
                        onClick = onDeliveredWithPhoto,
                        modifier = Modifier.size(40.dp)
                    ) {
                        Icon(
                            Icons.Filled.PhotoCamera,
                            contentDescription = stringResource(
                                R.string.mesh_job_mark_delivered_photo
                            ),
                            modifier = Modifier.size(20.dp)
                        )
                    }
                }
                TextButton(onClick = onSkip) {
                    Text(stringResource(R.string.mesh_job_mark_skip))
                }
            }
        }
    }
}

/**
 * Why a stop was not delivered, and which of the two things happened.
 *
 * Skipped and failed are deliberately separate confirmations rather than
 * one button with a toggle: "I did not go" and "I went and could not hand
 * it over" are different facts about the same doorstep, and the customer
 * reading the outcome is entitled to the difference. The reason is a code
 * on the wire; the free-text note is capped at what a PMTP record can
 * carry, so it is capped here rather than rejected after the tap.
 */
@Composable
fun SkipStopDialog(
    stopNumber: Int,
    /**
     * Whether this run may carry a photo at all (§11 fine). A coarse run
     * gets no camera row, and the picture is offered only for "not
     * delivered" regardless: a blocked gate is evidence, and a stop the van
     * never went to has nothing to photograph.
     */
    photoAllowed: Boolean,
    onDismiss: () -> Unit,
    onMark: (outcome: Int, reason: Int, note: String?, photoBase64: String?) -> Unit
) {
    var reason by remember { mutableStateOf(StopReason.CUSTOMER_ABSENT) }
    var note by remember { mutableStateOf("") }
    var photo by remember { mutableStateOf<String?>(null) }
    // Backing out of the camera leaves the dialog exactly as it was, with
    // whatever photo was already attached still attached: cancelling a
    // second capture is not a request to discard the first.
    val capture = rememberPhotoCapture { taken -> if (taken != null) photo = taken }
    val reasons = listOf(
        StopReason.CUSTOMER_ABSENT to R.string.mesh_job_reason_absent,
        StopReason.REFUSED to R.string.mesh_job_reason_refused,
        StopReason.INACCESSIBLE to R.string.mesh_job_reason_inaccessible,
        StopReason.PARCEL_MISSING to R.string.mesh_job_reason_parcel,
        StopReason.OTHER to R.string.mesh_job_reason_other
    )
    val noted = { note.trim().takeIf { it.isNotEmpty() } }

    Dialog(onDismissRequest = onDismiss) {
        Surface(
            shape = RoundedCornerShape(22.dp),
            color = MaterialTheme.colorScheme.surface,
            modifier = Modifier.fillMaxWidth()
        ) {
            Column(Modifier.padding(20.dp)) {
                Text(
                    stringResource(R.string.mesh_job_skip_title, stopNumber),
                    style = MaterialTheme.typography.titleMedium
                )
                Spacer(Modifier.height(10.dp))
                for ((code, label) in reasons) {
                    Row(
                        verticalAlignment = Alignment.CenterVertically,
                        modifier = Modifier
                            .fillMaxWidth()
                            .clip(RoundedCornerShape(12.dp))
                            .clickable { reason = code }
                            .padding(vertical = 8.dp, horizontal = 6.dp)
                    ) {
                        Icon(
                            if (reason == code) Icons.Filled.RadioButtonChecked
                            else Icons.Filled.RadioButtonUnchecked,
                            contentDescription = null,
                            tint = if (reason == code) MaterialTheme.colorScheme.primary
                            else MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.size(18.dp)
                        )
                        Spacer(Modifier.size(10.dp))
                        Text(stringResource(label), style = MaterialTheme.typography.bodyMedium)
                    }
                }
                if (reason == StopReason.OTHER) {
                    OutlinedTextField(
                        value = note,
                        // A record carries at most 64 bytes of note, so the
                        // field stops there rather than letting the driver
                        // type a sentence the wire will refuse.
                        onValueChange = { text ->
                            if (text.toByteArray(Charsets.UTF_8).size <= MAX_NOTE_BYTES) note = text
                        },
                        label = { Text(stringResource(R.string.mesh_job_note_hint)) },
                        singleLine = true,
                        modifier = Modifier.fillMaxWidth().padding(top = 4.dp)
                    )
                }
                // A photograph of the gate that would not open goes with
                // "not delivered" and with nothing else, so it sits above
                // those buttons and stays optional — a driver who takes
                // none has still answered the question.
                if (photoAllowed) {
                    Row(
                        verticalAlignment = Alignment.CenterVertically,
                        modifier = Modifier
                            .fillMaxWidth()
                            .clip(RoundedCornerShape(12.dp))
                            .clickable { capture() }
                            .padding(vertical = 8.dp, horizontal = 6.dp)
                    ) {
                        Icon(
                            if (photo == null) Icons.Filled.PhotoCamera
                            else Icons.Filled.CheckCircle,
                            contentDescription = null,
                            tint = if (photo == null) MaterialTheme.colorScheme.onSurfaceVariant
                            else MaterialTheme.colorScheme.primary,
                            modifier = Modifier.size(18.dp)
                        )
                        Spacer(Modifier.size(10.dp))
                        Text(
                            stringResource(
                                if (photo == null) R.string.mesh_job_photo_add
                                else R.string.mesh_job_photo_attached
                            ),
                            style = MaterialTheme.typography.bodyMedium,
                            color = if (photo == null) MaterialTheme.colorScheme.onSurfaceVariant
                            else MaterialTheme.colorScheme.primary
                        )
                    }
                }
                Spacer(Modifier.height(10.dp))
                Text(
                    stringResource(R.string.mesh_job_skip_explainer),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                Spacer(Modifier.height(8.dp))
                Row(
                    horizontalArrangement = Arrangement.End,
                    modifier = Modifier.fillMaxWidth()
                ) {
                    TextButton(onClick = onDismiss) {
                        Text(stringResource(R.string.mesh_job_mark_cancel))
                    }
                    Spacer(Modifier.size(4.dp))
                    // Skipped means the van never went, so whatever is in
                    // the camera row is not evidence of this stop and is
                    // deliberately dropped rather than published.
                    TextButton(onClick = { onMark(StopOutcome.SKIPPED, reason, noted(), null) }) {
                        Text(stringResource(R.string.mesh_job_mark_skipped_action))
                    }
                    Spacer(Modifier.size(4.dp))
                    TextButton(onClick = { onMark(StopOutcome.FAILED, reason, noted(), photo) }) {
                        Text(stringResource(R.string.mesh_job_mark_failed_action))
                    }
                }
            }
        }
    }
}

/**
 * The driver cannot finish the day, and confirms what saying so does.
 *
 * Deliberately not a toast with an undo: cancelling publishes §20's CANCELED
 * to everyone holding a link, and there is no unsending it. So the dialog
 * states who hears it before the tap rather than after, and offers the reason
 * as free text — a customer told "cancelled" and nothing else has been given
 * a fact without an explanation.
 *
 * The two buttons never share the word "cancel". "Cancel" on a dialog about
 * cancelling means both "back out" and "do it", and a driver reading fast at
 * a kerb should not have to work out which.
 */
@Composable
fun CancelJobDialog(
    /** Stops nobody has spoken for yet — what is actually being given up. */
    unresolved: Int,
    total: Int,
    onDismiss: () -> Unit,
    onCancelJob: (String?) -> Unit
) {
    var reason by remember { mutableStateOf("") }

    Dialog(onDismissRequest = onDismiss) {
        Surface(
            shape = RoundedCornerShape(22.dp),
            color = MaterialTheme.colorScheme.surface,
            modifier = Modifier.fillMaxWidth()
        ) {
            Column(Modifier.padding(20.dp)) {
                Text(
                    stringResource(R.string.mesh_job_cancel_title),
                    style = MaterialTheme.typography.titleMedium
                )
                Spacer(Modifier.height(10.dp))
                Text(
                    stringResource(R.string.mesh_job_cancel_body, unresolved, total),
                    style = MaterialTheme.typography.bodyMedium
                )
                Spacer(Modifier.height(8.dp))
                Text(
                    stringResource(R.string.mesh_job_cancel_explainer),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                OutlinedTextField(
                    value = reason,
                    // A record carries at most 64 bytes of note, so the field
                    // stops there rather than letting the driver type a
                    // sentence the wire will refuse — the same bound the skip
                    // note is held to.
                    onValueChange = { text ->
                        if (text.toByteArray(Charsets.UTF_8).size <= MAX_NOTE_BYTES) reason = text
                    },
                    label = { Text(stringResource(R.string.mesh_job_cancel_reason_hint)) },
                    singleLine = true,
                    modifier = Modifier.fillMaxWidth().padding(top = 10.dp)
                )
                Spacer(Modifier.height(12.dp))
                Row(
                    horizontalArrangement = Arrangement.End,
                    modifier = Modifier.fillMaxWidth()
                ) {
                    TextButton(onClick = onDismiss) {
                        Text(stringResource(R.string.mesh_job_cancel_keep))
                    }
                    Spacer(Modifier.size(6.dp))
                    TextButton(
                        onClick = { onCancelJob(reason.trim().takeIf { it.isNotEmpty() }) }
                    ) {
                        Text(
                            stringResource(R.string.mesh_job_cancel_confirm),
                            color = MaterialTheme.colorScheme.error
                        )
                    }
                }
            }
        }
    }
}

/**
 * The symbol itself: black squares on white, quiet zone included.
 *
 * Modules are snapped to whole device pixels — a QR drawn on fractional
 * boundaries picks up seams between cells that a camera reads as noise.
 */
@Composable
internal fun QrCanvas(matrix: QrMatrix, modifier: Modifier = Modifier) {
    val margin = 4
    val extent = matrix.size + margin * 2
    Canvas(modifier) {
        if (extent <= 0) return@Canvas
        val cell = kotlin.math.floor(minOf(size.width, size.height) / extent)
        if (cell <= 0f) return@Canvas
        val originX = (size.width - cell * extent) / 2f
        val originY = (size.height - cell * extent) / 2f
        drawRect(
            color = Color.White,
            topLeft = Offset(originX, originY),
            size = Size(cell * extent, cell * extent)
        )
        for (y in 0 until matrix.size) {
            for (x in 0 until matrix.size) {
                if (!matrix.module(x, y)) continue
                drawRect(
                    color = Color.Black,
                    topLeft = Offset(
                        originX + (x + margin) * cell,
                        originY + (y + margin) * cell
                    ),
                    size = Size(cell, cell)
                )
            }
        }
    }
}

@Composable
private fun ShareModeRow(title: String, note: String, selected: Boolean, onClick: () -> Unit) {
    Row(
        verticalAlignment = Alignment.Top,
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(12.dp))
            .background(
                if (selected) MaterialTheme.colorScheme.primaryContainer.copy(alpha = 0.35f)
                else Color.Transparent
            )
            .clickable(onClick = onClick)
            .padding(10.dp)
    ) {
        Icon(
            if (selected) Icons.Filled.RadioButtonChecked else Icons.Filled.RadioButtonUnchecked,
            contentDescription = null,
            tint = if (selected) MaterialTheme.colorScheme.primary
            else MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(18.dp)
        )
        Spacer(Modifier.size(10.dp))
        Column {
            Text(title, style = MaterialTheme.typography.bodyMedium)
            Text(
                note,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

/**
 * A drive somebody shared with this device.
 *
 * Every claim here comes from the subscriber's own status, not from this
 * code: §12 is explicit that "the bus is expected at 07:44" and "the bus
 * is here, arriving 07:44" are different claims, and a stale position
 * must never be presented as the second. The arrival is computed on this
 * device from the position they broadcast — so the publisher never
 * learns where the follower was waiting.
 */
@Composable
fun FollowedDriveCard(
    drive: FollowedDrive,
    eta: FollowedEta?,
    onStop: () -> Unit,
    modifier: Modifier = Modifier
) {
    Box(
        modifier
            .background(
                MaterialTheme.colorScheme.surface.copy(alpha = 0.96f),
                RoundedCornerShape(16.dp)
            )
            .padding(horizontal = 14.dp, vertical = 10.dp)
    ) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Icon(
                Icons.Filled.Share,
                contentDescription = null,
                tint = if (drive.live) Color(0xFF2F6DF6) else MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.size(22.dp)
            )
            Spacer(Modifier.size(10.dp))
            Column(Modifier.weight(1f)) {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text(
                        drive.claim,
                        style = MaterialTheme.typography.bodyLarge,
                        modifier = Modifier.weight(1f, fill = false)
                    )
                    // Stated only when the run stated it. "Unspecified" is
                    // not a mode and a chip for it would invent one.
                    travelModeLabel(drive.travelModeName)?.let { mode ->
                        Spacer(Modifier.size(8.dp))
                        Text(
                            mode,
                            style = MaterialTheme.typography.labelMedium,
                            color = MaterialTheme.colorScheme.primary
                        )
                    }
                }
                // A stop the driver marked skipped or failed is not going to
                // be visited, so there is no arrival to render and none is
                // rendered: a number here would be a lie with a decimal
                // point on it.
                val detail = when {
                    eta != null && eta.marked -> markedStopSentence(eta)
                    eta?.secondsFromNow != null -> stringResource(
                        R.string.mesh_following_eta,
                        formatDuration(LocalContext.current, eta.secondsFromNow),
                        stringResource(
                            if (eta.basis == "live-traffic") R.string.mesh_basis_live
                            else R.string.mesh_basis_static
                        )
                    )
                    !drive.hasPosition && drive.live -> stringResource(R.string.mesh_following_coarse)
                    drive.ageSeconds != null -> stringResource(R.string.mesh_following_age, drive.ageSeconds)
                    else -> stringResource(R.string.mesh_following_waiting)
                }
                Text(
                    detail,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                // §9 honesty: this device holds one graph. Quoting a bike
                // courier's arrival off a driving graph understates it
                // badly, and the follower is owed that sentence rather than
                // a confident number.
                if (eta?.travelModeMismatch == true && eta.secondsFromNow != null) {
                    Text(
                        stringResource(
                            R.string.mesh_following_mismatch,
                            stringResource(profileMapLabel(eta.profile)),
                            travelModeInline(eta.travelModeName ?: drive.travelModeName)
                        ),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.error
                    )
                }
                // What the driver has said about the day so far, from the
                // run's own cumulative map. A pending stop draws nothing:
                // "nobody has said what happened here" and "the van has not
                // arrived" are different statements.
                if (drive.hasOutcomes) {
                    Text(
                        if (drive.missedCount > 0) stringResource(
                            R.string.mesh_following_progress_missed,
                            drive.deliveredCount, drive.outcomes.size, drive.missedCount
                        ) else stringResource(
                            R.string.mesh_following_progress,
                            drive.deliveredCount, drive.outcomes.size
                        ),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                lastOutcomeSentence(drive.lastOutcome)?.let { sentence ->
                    Text(
                        sentence,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
            TextButton(onClick = onStop) {
                Text(stringResource(R.string.mesh_following_stop))
            }
        }
    }
}

/**
 * How the run says it travels, or null when it did not say. Mode 0 is the
 * absence of a claim, and a chip reading "unspecified" is noise.
 */
@Composable
private fun travelModeLabel(name: String?): String? = when (name) {
    "car" -> stringResource(R.string.mesh_travel_car)
    "bike" -> stringResource(R.string.mesh_travel_bike)
    "foot" -> stringResource(R.string.mesh_travel_foot)
    else -> null
}

/** The same, as it reads inside a sentence. */
@Composable
private fun travelModeInline(name: String?): String = when (name) {
    "bike" -> stringResource(R.string.mesh_travel_inline_bike)
    "foot" -> stringResource(R.string.mesh_travel_inline_foot)
    else -> stringResource(R.string.mesh_travel_inline_car)
}

/** The graph the arrival was actually computed on, named for a reader. */
private fun profileMapLabel(profile: String?): Int = when (profile) {
    "bike" -> R.string.mesh_profile_map_bike
    "foot" -> R.string.mesh_profile_map_foot
    else -> R.string.mesh_profile_map_car
}

/**
 * The reason clause a follower reads. Code 5 is deliberately "another
 * reason" rather than "no reason given": the driver may well have typed
 * one, and this card does not carry the note.
 */
@Composable
private fun stopReasonWord(reasonCode: Int?): String? = when (reasonCode) {
    StopReason.CUSTOMER_ABSENT -> stringResource(R.string.mesh_stop_reason_absent)
    StopReason.REFUSED -> stringResource(R.string.mesh_stop_reason_refused)
    StopReason.INACCESSIBLE -> stringResource(R.string.mesh_stop_reason_inaccessible)
    StopReason.PARCEL_MISSING -> stringResource(R.string.mesh_stop_reason_parcel)
    StopReason.OTHER -> stringResource(R.string.mesh_stop_reason_other)
    else -> null
}

/** The follower's own stop, marked. There is no time to show, so none is. */
@Composable
private fun markedStopSentence(eta: FollowedEta): String {
    val reason = stopReasonWord(eta.reasonCode)
    return when {
        eta.outcome == StopOutcome.FAILED && reason != null ->
            stringResource(R.string.mesh_following_your_stop_failed_because, reason)
        eta.outcome == StopOutcome.FAILED ->
            stringResource(R.string.mesh_following_your_stop_failed)
        reason != null ->
            stringResource(R.string.mesh_following_your_stop_skipped_because, reason)
        else -> stringResource(R.string.mesh_following_your_stop_skipped)
    }
}

/** The newest mark in words — the reason is what a customer can act on. */
@Composable
private fun lastOutcomeSentence(mark: StopMark?): String? {
    if (mark == null || mark.outcome == StopOutcome.PENDING) return null
    val reason = stopReasonWord(mark.reasonCode)
    return when {
        mark.outcome == StopOutcome.DELIVERED ->
            stringResource(R.string.mesh_following_last_delivered, mark.stopIndex)
        mark.outcome == StopOutcome.FAILED && reason != null ->
            stringResource(R.string.mesh_following_last_failed_because, mark.stopIndex, reason)
        mark.outcome == StopOutcome.FAILED ->
            stringResource(R.string.mesh_following_last_failed, mark.stopIndex)
        reason != null ->
            stringResource(R.string.mesh_following_last_skipped_because, mark.stopIndex, reason)
        else -> stringResource(R.string.mesh_following_last_skipped, mark.stopIndex)
    }
}

/**
 * The report sheet.
 *
 * §10.4 requires the user to be told, *at the point of reporting*, that a
 * report is public and locates them — which is why the disclosure is on
 * this sheet rather than in a settings page nobody read, and why the
 * protocol refuses to mint a report unless the caller asserts it showed
 * this.
 */
@Composable
fun IncidentReportSheet(
    status: PulseMeshStatus,
    bottomInset: Dp,
    onReport: (Int) -> Unit,
    onClose: () -> Unit
) {
    Box(
        Modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.scrim.copy(alpha = 0.32f))
            .clickable(onClick = onClose)
    )
    Box(Modifier.fillMaxSize(), contentAlignment = Alignment.BottomCenter) {
        SheetSurface(bottomInset = bottomInset) {
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 20.dp)
            ) {
                Text(
                    stringResource(R.string.mesh_report_title),
                    style = MaterialTheme.typography.titleMedium,
                    modifier = Modifier.weight(1f)
                )
                Icon(
                    Icons.Filled.Close,
                    contentDescription = null,
                    modifier = Modifier
                        .size(28.dp)
                        .clickable(onClick = onClose)
                )
            }
            Text(
                stringResource(R.string.mesh_report_disclosure),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(horizontal = 20.dp, vertical = 8.dp)
            )
            // The taxonomy is the protocol's, not the app's: a client that
            // invented a type would be minting records nobody else scores.
            for (type in status.incidentTypes) {
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    modifier = Modifier
                        .fillMaxWidth()
                        .clickable { onReport(type.type) }
                        .padding(horizontal = 20.dp, vertical = 12.dp)
                ) {
                    Icon(
                        Icons.Filled.ReportProblem,
                        contentDescription = null,
                        tint = if (type.informational) MaterialTheme.colorScheme.onSurfaceVariant
                        else MaterialTheme.colorScheme.error,
                        modifier = Modifier.size(20.dp)
                    )
                    Spacer(Modifier.size(12.dp))
                    Text(
                        type.name.replaceFirstChar { it.uppercase() },
                        style = MaterialTheme.typography.bodyLarge
                    )
                }
            }
            Spacer(Modifier.height(8.dp))
        }
    }
}

/**
 * The Waze loop, while driving: the incident ahead, and the two answers
 * that are worth anything — is it still there, or is it gone.
 *
 * A hint and a corroborated incident are drawn differently on purpose.
 * One report is a claim; §8.5 only turns it into something the router
 * acts on once several distinct peers agree.
 */
@Composable
fun IncidentPrompt(
    incident: MeshIncident,
    onAnswer: (Boolean) -> Unit,
    modifier: Modifier = Modifier
) {
    Box(
        modifier
            .background(
                MaterialTheme.colorScheme.surface.copy(alpha = 0.96f),
                RoundedCornerShape(16.dp)
            )
            .padding(horizontal = 14.dp, vertical = 10.dp)
    ) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Icon(
                Icons.Filled.ReportProblem,
                contentDescription = null,
                tint = if (incident.tier == "shown") MaterialTheme.colorScheme.error
                else Color(0xFFE5B93C),
                modifier = Modifier.size(22.dp)
            )
            Spacer(Modifier.size(10.dp))
            Column(Modifier.weight(1f)) {
                Text(
                    incident.typeName.replaceFirstChar { it.uppercase() },
                    style = MaterialTheme.typography.bodyLarge
                )
                Text(
                    if (incident.tier == "shown") {
                        stringResource(R.string.mesh_incident_shown, incident.sources, incident.ageSeconds)
                    } else {
                        stringResource(
                            if (incident.sources == 1) R.string.mesh_incident_hint
                            else R.string.mesh_incident_hints,
                            incident.sources
                        )
                    },
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            Row(horizontalArrangement = Arrangement.spacedBy(4.dp)) {
                TextButton(onClick = { onAnswer(true) }) {
                    Text(stringResource(R.string.mesh_answer_still))
                }
                TextButton(onClick = { onAnswer(false) }) {
                    Text(stringResource(R.string.mesh_answer_gone))
                }
            }
        }
    }
}
