package dev.rangefind.wayfind.ui.components

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.pluralStringResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.engine.OfferSummary
import dev.rangefind.wayfind.region.HeldOffer
import dev.rangefind.wayfind.ui.formatDistance
import java.text.DateFormat
import java.text.NumberFormat
import java.util.Currency
import java.util.Date
import java.util.Locale

/**
 * A dispatcher's offer, and the one decision it supports: whether to bid
 * (threads §20.4).
 *
 * Deliberately not [JobOfferSheet]. That sheet is about an *award* — a
 * publish capability with every doorstep in it, and the question is
 * whether to drive. This one is about a broadcast that grants nothing,
 * names no address and may never come to anything, and the question is
 * whether to put your name forward. Leading with a stop list would be
 * impossible here anyway: there is no stop in an offer.
 *
 * What leads instead is what a courier prices against — how many drops,
 * how far, how, for how much, and until when — and the locality is stated
 * as exactly the grid cell it is. Nothing here draws a pin.
 */
@Composable
fun JobBidSheet(
    offer: OfferSummary,
    bottomInset: Dp,
    /** Sends this device's card, which is how a courier says "consider me". */
    onSendCard: () -> Unit,
    onDismiss: () -> Unit
) {
    Box(
        Modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.scrim.copy(alpha = 0.32f))
            .clickable(onClick = onDismiss)
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
                    stringResource(R.string.offer_title),
                    style = MaterialTheme.typography.titleMedium,
                    modifier = Modifier.weight(1f)
                )
                Icon(
                    Icons.Filled.Close,
                    contentDescription = null,
                    modifier = Modifier
                        .size(28.dp)
                        .clickable(onClick = onDismiss)
                )
            }

            Column(Modifier.padding(horizontal = 20.dp, vertical = 8.dp)) {
                // The price of the work, first, because it is the first
                // thing a courier decides on. Silence is stated as
                // silence: a dispatcher who named no figure has not
                // offered zero.
                val pay = formatOfferPay(offer.payMinor, offer.currency)
                Text(
                    if (pay == null) stringResource(R.string.offer_pay_unstated)
                    else stringResource(R.string.offer_pay, pay),
                    style = MaterialTheme.typography.titleLarge,
                    color = if (pay == null) MaterialTheme.colorScheme.onSurfaceVariant
                    else MaterialTheme.colorScheme.onSurface
                )
                Spacer(Modifier.height(6.dp))
                Text(
                    pluralStringResource(R.plurals.offer_stops, offer.stopCount, offer.stopCount),
                    style = MaterialTheme.typography.bodyLarge
                )
                Text(
                    stringResource(
                        R.string.offer_distance,
                        formatDistance(LocalContext.current, offer.totalMeters.toDouble())
                    ),
                    style = MaterialTheme.typography.bodyMedium
                )
                // §11 is the issuer's decision and travels with the offer:
                // a courier is being told, before bidding, what the run
                // they would publish is worth to whoever holds its link.
                val shareMode = stringResource(
                    if (offer.fine) R.string.mesh_share_mode_fine
                    else R.string.mesh_share_mode_coarse
                )
                val travel = offerTravelLabel(offer.travelModeName)
                Text(
                    if (travel == null) shareMode
                    else stringResource(R.string.mesh_job_mode_line, shareMode, travel),
                    style = MaterialTheme.typography.bodyMedium
                )
                // The dispatcher's own public line, in their words and in
                // quotation marks, because it is a claim they made and not
                // one this app is making.
                offer.label?.let { label ->
                    Spacer(Modifier.height(6.dp))
                    Text(
                        stringResource(R.string.offer_label, label),
                        style = MaterialTheme.typography.bodyMedium
                    )
                }
            }

            // Where the work is, as coarsely as the offer states it. The
            // coordinates are the grid cell's own — two decimals is
            // exactly the 0.01° resolution, so there is no false precision
            // to read off them — and the spread is the bucket in words,
            // never a radius, because an exact centre plus an exact radius
            // is a disc two or three offers trilaterate.
            Surface(
                shape = RoundedCornerShape(14.dp),
                color = MaterialTheme.colorScheme.surfaceVariant,
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 20.dp, vertical = 4.dp)
            ) {
                Column(Modifier.padding(horizontal = 14.dp, vertical = 10.dp)) {
                    Text(
                        stringResource(
                            R.string.offer_locality,
                            String.format(
                                Locale.ROOT, "%.2f, %.2f", offer.centroidLat, offer.centroidLon
                            )
                        ),
                        style = MaterialTheme.typography.bodyLarge
                    )
                    Text(
                        stringResource(R.string.offer_locality_grid, gridKilometres(offer.gridDegrees)),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    Spacer(Modifier.height(4.dp))
                    Text(
                        stringResource(spreadWords(offer.spread)),
                        style = MaterialTheme.typography.bodyMedium
                    )
                }
            }

            // The line that keeps the sheet honest. A courier reading a
            // grid square and a stop count will assume the addresses are
            // somewhere behind them; they are not in these bytes at all,
            // and saying so is the difference between a coarse offer and
            // an offer that looks like it is hiding something.
            Text(
                stringResource(R.string.offer_no_addresses),
                style = MaterialTheme.typography.bodyMedium,
                modifier = Modifier.padding(horizontal = 20.dp, vertical = 8.dp)
            )

            Column(Modifier.padding(horizontal = 20.dp, vertical = 2.dp)) {
                Text(
                    stringResource(R.string.offer_expires, formatOfferMoment(offer.notAfter)),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                Text(
                    stringResource(R.string.offer_issuer, offer.issuerHex.take(12)),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }

            if (!offer.ok) {
                Text(
                    stringResource(R.string.offer_unusable, offer.reason.orEmpty()),
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.error,
                    modifier = Modifier.padding(horizontal = 20.dp, vertical = 4.dp)
                )
            }

            // Bidding is out of scope in protocol v1 and this says so
            // rather than implying a channel that does not exist: what
            // leaves this phone is the same PMV1 card enrolment needs, over
            // whatever the two parties already use.
            Text(
                stringResource(R.string.offer_send_card_note),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(horizontal = 20.dp, vertical = 8.dp)
            )

            Row(
                horizontalArrangement = Arrangement.End,
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 12.dp)
            ) {
                TextButton(onClick = onDismiss) {
                    Text(stringResource(R.string.offer_dismiss))
                }
                Spacer(Modifier.size(6.dp))
                // Refused on an offer that does not verify: an unsigned or
                // expired offer is not one anybody stands behind, and a
                // bid on it is a card sent to nobody.
                TextButton(onClick = onSendCard, enabled = offer.ok) {
                    Text(stringResource(R.string.offer_send_card))
                }
            }
            Spacer(Modifier.height(8.dp))
        }
    }
}

/**
 * The offers this device has bid on, in settings.
 *
 * Beside enrolment rather than beside a drive, and for the same reason:
 * a bid is a prior act whose whole worth is that it outlives the moment
 * it was made. What the list is *for* is the check — when an award
 * arrives, it is compared against these — so the rows say what was
 * advertised, and removing one is the courier withdrawing a bid they no
 * longer want checked.
 */
@Composable
fun HeldOffersSection(
    offers: List<HeldOffer>,
    onForget: (String) -> Unit
) {
    Column {
        Text(
            stringResource(R.string.offer_held_title),
            style = MaterialTheme.typography.titleSmall,
            modifier = Modifier.padding(horizontal = 20.dp)
        )
        Spacer(Modifier.height(6.dp))
        if (offers.isEmpty()) {
            Text(
                stringResource(R.string.offer_held_empty),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(horizontal = 20.dp)
            )
            return@Column
        }
        for (offer in offers) {
            HeldOfferRow(offer = offer, onForget = { onForget(offer.jobIdHex) })
        }
    }
}

@Composable
private fun HeldOfferRow(offer: HeldOffer, onForget: () -> Unit) {
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 20.dp, vertical = 8.dp)
    ) {
        Column(Modifier.weight(1f)) {
            val pay = formatOfferPay(offer.payMinor, offer.currency)
            Text(
                offer.label ?: pay ?: pluralStringResource(
                    R.plurals.offer_stops, offer.stopCount, offer.stopCount
                ),
                style = MaterialTheme.typography.bodyLarge
            )
            Text(
                stringResource(
                    R.string.mesh_job_mode_line,
                    pluralStringResource(R.plurals.offer_stops, offer.stopCount, offer.stopCount),
                    formatDistance(LocalContext.current, offer.totalMeters.toDouble())
                ),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            Text(
                stringResource(R.string.offer_held_expires, formatOfferMoment(offer.notAfter)),
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            if (offer.bidAt > 0) {
                Text(
                    stringResource(
                        R.string.offer_held_bid_at,
                        DateFormat.getDateInstance(DateFormat.MEDIUM).format(Date(offer.bidAt))
                    ),
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        TextButton(onClick = onForget) {
            Text(stringResource(R.string.offer_forget), color = MaterialTheme.colorScheme.error)
        }
    }
}

/** The spread bucket in words. Five buckets, and no sixth reading. */
private fun spreadWords(spread: Int): Int = when (spread) {
    0 -> R.string.offer_spread_1km
    1 -> R.string.offer_spread_3km
    2 -> R.string.offer_spread_10km
    3 -> R.string.offer_spread_30km
    else -> R.string.offer_spread_beyond
}

/**
 * The grid's north-south width in kilometres, from the degrees the offer
 * states rather than from a constant here — the codec owns that number
 * and this only reads it.
 *
 * Formatted in the device's locale because it is prose: a French reader
 * writes 1,1 km. The coordinates above it are the opposite case and stay
 * on [Locale.ROOT].
 */
private fun gridKilometres(gridDegrees: Double): String =
    String.format(Locale.getDefault(), "%.1f", gridDegrees * 111.32)

/**
 * The pay, in the offer's own currency.
 *
 * The currency is the dispatcher's and the grouping is the reader's:
 * "1 234,50 $ US" and "$1,234.50" are the same offer to two couriers, and
 * neither should be shown the other's. Minor units are scaled by the
 * currency's own fraction digits — yen have none, dinars have three — so
 * the figure is never off by a factor of a hundred.
 *
 * A code this device does not know is not a reason to hide the number:
 * the amount and the code are shown as they arrived. Null is only ever
 * "the dispatcher did not say"; a stated zero is formatted like any other
 * amount, because a job offered for nothing is a statement worth reading.
 */
internal fun formatOfferPay(payMinor: Long?, currency: String?): String? {
    val minor = payMinor ?: return null
    val code = currency?.trim()?.uppercase(Locale.ROOT)?.takeIf { it.length == 3 } ?: return null
    return runCatching {
        val unit = Currency.getInstance(code)
        val digits = unit.defaultFractionDigits.coerceAtLeast(0)
        val format = NumberFormat.getCurrencyInstance(Locale.getDefault())
        format.currency = unit
        format.minimumFractionDigits = digits
        format.maximumFractionDigits = digits
        var scale = 1.0
        repeat(digits) { scale *= 10.0 }
        format.format(minor / scale)
    }.getOrElse { "$minor $code" }
}

/** An absolute expiry, as a date and time a person reads. */
internal fun formatOfferMoment(unixSeconds: Long): String =
    DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT)
        .format(Date(unixSeconds * 1000L))

/** How the round travels, or null when the plan did not say. */
@Composable
private fun offerTravelLabel(name: String?): String? = when (name) {
    "car" -> stringResource(R.string.mesh_travel_car)
    "bike" -> stringResource(R.string.mesh_travel_bike)
    "foot" -> stringResource(R.string.mesh_travel_foot)
    else -> null
}
