package dev.rangefind.wayfind.ui.components

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.itemsIndexed
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.AltRoute
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.Navigation
import androidx.compose.material.icons.filled.Flag
import androidx.compose.material.icons.filled.Straight
import androidx.compose.material.icons.filled.TurnLeft
import androidx.compose.material.icons.filled.TurnRight
import androidx.compose.material.icons.filled.TurnSlightLeft
import androidx.compose.material.icons.filled.TurnSlightRight
import androidx.compose.material.icons.filled.UTurnLeft
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.Icon
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.shadow
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.pluralStringResource
import androidx.compose.ui.hapticfeedback.HapticFeedbackType
import androidx.compose.ui.platform.LocalHapticFeedback
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import dev.rangefind.wayfind.R
import androidx.compose.ui.draw.clip
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.material.icons.filled.DirectionsBike
import androidx.compose.material.icons.filled.DirectionsCar
import androidx.compose.material.icons.filled.DirectionsWalk
import dev.rangefind.wayfind.engine.LaneTurn
import androidx.compose.material.icons.filled.CallSplit
import androidx.compose.material.icons.filled.MergeType
import dev.rangefind.wayfind.nav.ItineraryLine
import dev.rangefind.wayfind.nav.Maneuver
import dev.rangefind.wayfind.nav.TravelMode
import dev.rangefind.wayfind.nav.itineraryOf
import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.nav.postsRectangularSpeedLimits
import dev.rangefind.wayfind.engine.Route
import dev.rangefind.wayfind.nav.bearingDegrees
import dev.rangefind.wayfind.nav.bearingDelta
import dev.rangefind.wayfind.ui.UiState
import dev.rangefind.wayfind.ui.formatArrivalClock
import dev.rangefind.wayfind.ui.formatBytes
import dev.rangefind.wayfind.ui.formatDistance
import dev.rangefind.wayfind.ui.formatDuration
import dev.rangefind.wayfind.ui.formatEtaDelta
import dev.rangefind.wayfind.ui.formatManeuverDistance
import dev.rangefind.wayfind.ui.formatSpeed
import dev.rangefind.wayfind.ui.theme.LocalMapPalette
import kotlin.math.abs

@Composable
fun DirectionsSheet(
    state: UiState,
    bottomInset: Dp,
    onSelectRoute: (Int) -> Unit,
    onSelectMode: (TravelMode) -> Unit,
    onStart: () -> Unit,
    onClose: () -> Unit
) {
    val context = LocalContext.current
    val travelModeLabel = stringResource(R.string.mode_switch)
    SheetSurface(bottomInset = bottomInset) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 20.dp)
        ) {
            Column(Modifier.weight(1f)) {
                Text(
                    stringResource(
                        R.string.directions_to,
                        state.selected?.name
                            ?: stringResource(R.string.directions_destination_fallback)
                    ),
                    style = MaterialTheme.typography.labelMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
            }
            Icon(
                Icons.Filled.Close,
                contentDescription = stringResource(R.string.action_close),
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier
                    .size(22.dp)
                    .clickable { onClose() }
            )
        }

        // How the trip is made, above everything the trip says about itself.
        // The choice changes the route rather than annotating it, so it reads
        // first — and it sits at the top of the sheet, in the third of the
        // screen a thumb reaches without shifting grip.
        Spacer(Modifier.height(14.dp))
        Row(
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 20.dp)
                .semantics { contentDescription = travelModeLabel }
        ) {
            for (mode in TravelMode.entries) {
                val chosen = mode == state.mode
                val usable = mode in state.modesAvailable
                Surface(
                    onClick = { onSelectMode(mode) },
                    enabled = usable,
                    shape = RoundedCornerShape(14.dp),
                    color = if (chosen) MaterialTheme.colorScheme.primary
                    else MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.6f),
                    modifier = Modifier
                        .weight(1f)
                        // Comfortably over the 48dp minimum: this is tapped
                        // one-handed, often in motion.
                        .height(54.dp)
                ) {
                    Row(
                        horizontalArrangement = Arrangement.Center,
                        verticalAlignment = Alignment.CenterVertically,
                        modifier = Modifier.fillMaxSize()
                    ) {
                        val tint = when {
                            chosen -> MaterialTheme.colorScheme.onPrimary
                            usable -> MaterialTheme.colorScheme.onSurface
                            else -> MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.38f)
                        }
                        Icon(
                            travelModeIcon(mode),
                            contentDescription = null,
                            tint = tint,
                            modifier = Modifier.size(21.dp)
                        )
                        Spacer(Modifier.width(7.dp))
                        Text(
                            stringResource(mode.labelRes),
                            style = MaterialTheme.typography.labelLarge,
                            color = tint,
                            maxLines = 1
                        )
                    }
                }
            }
        }

        when {
            state.routing -> {
                Spacer(Modifier.height(18.dp))
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    modifier = Modifier.padding(horizontal = 20.dp)
                ) {
                    CircularProgressIndicator(strokeWidth = 2.5.dp, modifier = Modifier.size(20.dp))
                    Spacer(Modifier.width(14.dp))
                    Text(
                        stringResource(R.string.directions_fetching),
                        style = MaterialTheme.typography.bodyLarge
                    )
                }
                Spacer(Modifier.height(18.dp))
            }

            state.routeError != null -> {
                Spacer(Modifier.height(14.dp))
                Text(
                    state.routeError,
                    style = MaterialTheme.typography.bodyLarge,
                    color = MaterialTheme.colorScheme.error,
                    modifier = Modifier.padding(horizontal = 20.dp)
                )
                Spacer(Modifier.height(14.dp))
            }

            else -> {
                val route = state.activeRoute
                if (route != null) {
                    Spacer(Modifier.height(4.dp))
                    Row(
                        verticalAlignment = Alignment.Bottom,
                        modifier = Modifier.padding(horizontal = 20.dp)
                    ) {
                        Text(
                            formatDuration(context, route.seconds),
                            style = MaterialTheme.typography.displaySmall,
                            color = MaterialTheme.colorScheme.primary
                        )
                        Spacer(Modifier.width(12.dp))
                        Text(
                            stringResource(
                                R.string.directions_summary,
                                formatDistance(context, route.distanceMeters),
                                formatArrivalClock(context, route.seconds)
                            ),
                            style = MaterialTheme.typography.bodyMedium,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.padding(bottom = 6.dp)
                        )
                    }

                    if (state.allRoutes.size > 1) {
                        Spacer(Modifier.height(12.dp))
                        Row(
                            horizontalArrangement = Arrangement.spacedBy(8.dp),
                            modifier = Modifier.padding(horizontal = 20.dp)
                        ) {
                            state.allRoutes.forEachIndexed { index, candidate ->
                                RouteChip(
                                    label = if (index == 0) {
                                        stringResource(R.string.route_option_fastest)
                                    } else {
                                        stringResource(R.string.route_option_alternative, index)
                                    },
                                    detail = formatDuration(context, candidate.seconds),
                                    selected = index == state.activeRouteIndex,
                                    onClick = { onSelectRoute(index) }
                                )
                            }
                        }
                    }

                    Spacer(Modifier.height(14.dp))
                    Button(
                        onClick = onStart,
                        shape = RoundedCornerShape(16.dp),
                        colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.primary),
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(horizontal = 20.dp)
                            .height(54.dp)
                    ) {
                        Icon(Icons.Filled.Navigation, contentDescription = null, modifier = Modifier.size(20.dp))
                        Spacer(Modifier.width(10.dp))
                        Text(
                            stringResource(R.string.directions_start),
                            style = MaterialTheme.typography.labelLarge
                        )
                    }

                    Spacer(Modifier.height(12.dp))
                    StepsPreview(route)

                    Spacer(Modifier.height(10.dp))
                    Text(
                        pluralStringResource(
                            R.plurals.directions_range_requests,
                            route.httpRequests,
                            route.httpRequests,
                            formatBytes(context, route.bytesFetched)
                        ),
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.outline,
                        modifier = Modifier.padding(horizontal = 20.dp)
                    )
                }
            }
        }
    }
}

@Composable
private fun StepsPreview(route: Route) {
    val context = LocalContext.current
    LazyColumn(Modifier.heightIn(max = 132.dp)) {
        itemsIndexed(itineraryOf(route) { i -> turnDeltaAtStep(route, i) }.take(12)) { _, line ->
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 20.dp, vertical = 7.dp)
            ) {
                Icon(
                    itineraryIcon(route, line),
                    contentDescription = null,
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(18.dp)
                )
                Spacer(Modifier.width(14.dp))
                Text(
                    line.name.ifBlank {
                        stringResource(
                            when (line.maneuver) {
                                Maneuver.Exit -> R.string.nav_exit_label
                                Maneuver.Merge -> R.string.nav_merge_label
                                else -> R.string.directions_unnamed_road
                            }
                        )
                    },
                    style = MaterialTheme.typography.bodyMedium,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                    modifier = Modifier.weight(1f)
                )
                Text(
                    formatDistance(context, line.meters),
                    style = MaterialTheme.typography.labelMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
    }
}

@Composable
private fun RouteChip(label: String, detail: String, selected: Boolean, onClick: () -> Unit) {
    Surface(
        shape = RoundedCornerShape(12.dp),
        color = if (selected) MaterialTheme.colorScheme.primaryContainer
        else MaterialTheme.colorScheme.surfaceVariant,
        modifier = Modifier.clickable { onClick() }
    ) {
        Column(Modifier.padding(horizontal = 14.dp, vertical = 8.dp)) {
            Text(
                label,
                style = MaterialTheme.typography.labelSmall,
                color = if (selected) MaterialTheme.colorScheme.onPrimaryContainer
                else MaterialTheme.colorScheme.onSurfaceVariant
            )
            Text(
                detail,
                style = MaterialTheme.typography.labelLarge,
                color = if (selected) MaterialTheme.colorScheme.onPrimaryContainer
                else MaterialTheme.colorScheme.onSurface
            )
        }
    }
}

@Composable
fun NavigationOverlay(
    state: UiState,
    topInset: Dp,
    bottomInset: Dp,
    onSelectRoute: (Int) -> Unit,
    onMarkIssue: () -> Unit,
    onStop: () -> Unit
) {
    val context = LocalContext.current
    val nav = state.nav
    val route = state.activeRoute
    val palette = LocalMapPalette.current
    val offers = nav?.alternatives.orEmpty()
    val placeholder = stringResource(R.string.format_placeholder)
    val continueLabel = stringResource(R.string.nav_continue)

    Box(Modifier.fillMaxSize()) {

        // Maneuver banner.
        Surface(
            shape = RoundedCornerShape(bottomStart = 24.dp, bottomEnd = 24.dp),
            color = MaterialTheme.colorScheme.primary,
            shadowElevation = 12.dp,
            modifier = Modifier
                .align(Alignment.TopCenter)
                .fillMaxWidth()
        ) {
            Column(
                Modifier
                    .fillMaxWidth()
                    .padding(top = topInset + 12.dp, bottom = 16.dp)
                    .padding(horizontal = 20.dp)
            ) {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Icon(
                        when {
                            nav?.maneuver == Maneuver.Exit -> Icons.Filled.CallSplit
                            nav?.maneuver == Maneuver.Merge -> Icons.Filled.MergeType
                            route != null && nav != null -> maneuverIcon(route, nav.stepIndex + 1)
                            else -> Icons.Filled.Straight
                        },
                        contentDescription = null,
                        tint = MaterialTheme.colorScheme.onPrimary,
                        modifier = Modifier.size(42.dp)
                    )
                    Spacer(Modifier.width(16.dp))
                    Column(Modifier.weight(1f)) {
                        Text(
                            nav?.let { formatManeuverDistance(context, it.metersToManeuver) }
                                ?: placeholder,
                            style = MaterialTheme.typography.headlineMedium,
                            color = MaterialTheme.colorScheme.onPrimary
                        )
                        Text(
                            nav?.nextStepName?.ifBlank { nav.stepName }?.ifBlank { continueLabel }
                                ?: continueLabel,
                            style = MaterialTheme.typography.titleMedium,
                            color = MaterialTheme.colorScheme.onPrimary.copy(alpha = 0.86f),
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis
                        )
                    }
                }

                if (state.rerouting) {
                    Spacer(Modifier.height(10.dp))
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        CircularProgressIndicator(
                            strokeWidth = 2.dp,
                            color = MaterialTheme.colorScheme.onPrimary,
                            modifier = Modifier.size(14.dp)
                        )
                        Spacer(Modifier.width(10.dp))
                        Text(
                            stringResource(R.string.nav_rerouting_label),
                            style = MaterialTheme.typography.labelMedium,
                            color = MaterialTheme.colorScheme.onPrimary
                        )
                    }
                }
            }
        }

        // Lane guidance. Sits directly under the maneuver banner because that
        // is already where the eye goes for the next instruction, and the two
        // answer the same question one after the other: what happens next,
        // and which lane it happens from.
        val laneGuidanceLabel = stringResource(R.string.nav_lane_guidance)
        nav?.takeIf { it.lanes.isNotEmpty() }?.let { current ->
            Row(
                horizontalArrangement = Arrangement.spacedBy(6.dp),
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .align(Alignment.TopCenter)
                    .padding(top = topInset + 108.dp)
                    .clip(RoundedCornerShape(14.dp))
                    .background(MaterialTheme.colorScheme.surface.copy(alpha = 0.96f))
                    .padding(horizontal = 10.dp, vertical = 8.dp)
                    .semantics { contentDescription = laneGuidanceLabel }
            ) {
                current.lanes.forEachIndexed { index, mask ->
                    val usable = current.laneUsable.getOrElse(index) { false }
                    LaneChip(mask = mask, usable = usable, fallback = current.turnDelta)
                }
            }
        }

        // Current road + speed.
        nav?.let {
            Row(
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(10.dp),
                modifier = Modifier
                    .align(Alignment.BottomStart)
                    .padding(
                        start = 16.dp,
                        bottom = bottomInset + 122.dp + if (offers.isNotEmpty()) 58.dp else 0.dp
                    )
            ) {
                // Over the posted limit by more than GPS noise, the readout
                // itself turns: a driver checking their speed should not have
                // to compare two numbers to know the answer.
                val speeding = it.speedLimitKmh > 0 &&
                    it.speedMps * 3.6 > it.speedLimitKmh + SPEEDING_TOLERANCE_KMH
                Surface(
                    shape = CircleShape,
                    color = if (speeding) MaterialTheme.colorScheme.errorContainer
                    else MaterialTheme.colorScheme.surface,
                    shadowElevation = 4.dp
                ) {
                    Column(
                        horizontalAlignment = Alignment.CenterHorizontally,
                        modifier = Modifier.padding(horizontal = 14.dp, vertical = 8.dp)
                    ) {
                        Text(
                            formatSpeed(it.speedMps),
                            style = MaterialTheme.typography.titleLarge,
                            color = if (speeding) MaterialTheme.colorScheme.onErrorContainer
                            else MaterialTheme.colorScheme.onSurface
                        )
                        Text(
                            stringResource(R.string.nav_speed_unit),
                            style = MaterialTheme.typography.labelSmall,
                            color = if (speeding) MaterialTheme.colorScheme.onErrorContainer
                            else MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
                if (it.speedLimitKmh > 0) SpeedLimitSign(it.speedLimitKmh, it.position)
                if (it.stepName.isNotBlank()) {
                    Surface(
                        shape = RoundedCornerShape(14.dp),
                        color = MaterialTheme.colorScheme.surface.copy(alpha = 0.94f),
                        shadowElevation = 4.dp
                    ) {
                        Text(
                            it.stepName,
                            style = MaterialTheme.typography.labelLarge,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis,
                            modifier = Modifier.padding(horizontal = 14.dp, vertical = 9.dp)
                        )
                    }
                }
            }
        }

        // Flagging a fault has to survive being done at speed, by someone who
        // must not look away from the road: one thumb-sized target on the side
        // the hand already rests, no confirmation, no dialog. The running count
        // is the only feedback that matters — it says the tap landed — and the
        // haptic carries it when even a glance is too much.
        if (state.recordTrips) {
            val haptics = LocalHapticFeedback.current
            Surface(
                onClick = {
                    haptics.performHapticFeedback(HapticFeedbackType.LongPress)
                    onMarkIssue()
                },
                shape = CircleShape,
                color = MaterialTheme.colorScheme.errorContainer,
                shadowElevation = 8.dp,
                modifier = Modifier
                    .align(Alignment.BottomEnd)
                    .padding(
                        end = 16.dp,
                        bottom = bottomInset + 122.dp + if (offers.isNotEmpty()) 58.dp else 0.dp
                    )
                    .size(64.dp)
            ) {
                Column(
                    horizontalAlignment = Alignment.CenterHorizontally,
                    verticalArrangement = Arrangement.Center,
                    modifier = Modifier.fillMaxSize()
                ) {
                    Icon(
                        Icons.Filled.Flag,
                        contentDescription = stringResource(R.string.diag_mark),
                        tint = MaterialTheme.colorScheme.onErrorContainer
                    )
                    if (state.issueMarks > 0) {
                        Text(
                            stringResource(R.string.diag_mark_count, state.issueMarks),
                            style = MaterialTheme.typography.labelSmall,
                            color = MaterialTheme.colorScheme.onErrorContainer
                        )
                    }
                }
            }
        }

        // Trip footer.
        Surface(
            shape = RoundedCornerShape(topStart = 24.dp, topEnd = 24.dp),
            color = MaterialTheme.colorScheme.surface,
            shadowElevation = 18.dp,
            modifier = Modifier
                .align(Alignment.BottomCenter)
                .fillMaxWidth()
        ) {
            Column {
                LinearProgressIndicator(
                    progress = {
                        val total = route?.distanceMeters ?: 0.0
                        if (total <= 0.0 || nav == null) 0f
                        else ((total - nav.remainingMeters) / total).toFloat().coerceIn(0f, 1f)
                    },
                    color = palette.routeLine,
                    trackColor = MaterialTheme.colorScheme.surfaceVariant,
                    modifier = Modifier.fillMaxWidth().height(3.dp)
                )
                // Alternates the car could still take. At driving zoom their
                // map bubbles are usually off-screen, so the offer belongs
                // here — one tap re-tracks onto the other line.
                if (offers.isNotEmpty()) {
                    Row(
                        horizontalArrangement = Arrangement.spacedBy(8.dp),
                        modifier = Modifier.padding(start = 22.dp, end = 16.dp, top = 12.dp)
                    ) {
                        offers.forEach { offer ->
                            Surface(
                                onClick = { onSelectRoute(offer.index) },
                                shape = RoundedCornerShape(12.dp),
                                color = if (offer.deltaSeconds < -30)
                                    MaterialTheme.colorScheme.secondaryContainer
                                else MaterialTheme.colorScheme.surfaceVariant
                            ) {
                                Row(
                                    verticalAlignment = Alignment.CenterVertically,
                                    modifier = Modifier.padding(horizontal = 12.dp, vertical = 8.dp)
                                ) {
                                    Icon(
                                        Icons.Filled.AltRoute,
                                        contentDescription = null,
                                        tint = if (offer.deltaSeconds < -30)
                                            MaterialTheme.colorScheme.onSecondaryContainer
                                        else MaterialTheme.colorScheme.onSurfaceVariant,
                                        modifier = Modifier.size(16.dp)
                                    )
                                    Spacer(Modifier.width(8.dp))
                                    Text(
                                        formatEtaDelta(context, offer.deltaSeconds),
                                        style = MaterialTheme.typography.labelLarge,
                                        color = if (offer.deltaSeconds < -30)
                                            MaterialTheme.colorScheme.onSecondaryContainer
                                        else MaterialTheme.colorScheme.onSurface
                                    )
                                }
                            }
                        }
                    }
                }

                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(start = 22.dp, end = 16.dp, top = 14.dp)
                        .padding(bottom = bottomInset + 14.dp)
                ) {
                    Column(Modifier.weight(1f)) {
                        Text(
                            nav?.let { formatDuration(context, it.remainingSeconds) }
                                ?: placeholder,
                            style = MaterialTheme.typography.headlineSmall,
                            color = MaterialTheme.colorScheme.primary
                        )
                        Text(
                            nav?.let {
                                context.getString(
                                    R.string.nav_trip_summary,
                                    formatDistance(context, it.remainingMeters),
                                    formatArrivalClock(context, it.remainingSeconds)
                                )
                            } ?: "",
                            style = MaterialTheme.typography.bodyMedium,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                    Surface(
                        onClick = onStop,
                        shape = CircleShape,
                        color = MaterialTheme.colorScheme.errorContainer,
                        modifier = Modifier.size(52.dp)
                    ) {
                        Box(contentAlignment = Alignment.Center) {
                            Icon(
                                Icons.Filled.Close,
                                contentDescription = stringResource(R.string.action_end_navigation),
                                tint = MaterialTheme.colorScheme.onErrorContainer
                            )
                        }
                    }
                }
            }
        }
    }
}

/** Allowance for GPS speed noise before the readout calls it speeding. */
private const val SPEEDING_TOLERANCE_KMH = 5

/**
 * Posted speed limit, drawn as the circular sign used across most of the
 * world (and in the region this index covers). Deliberately not restyled to
 * the app palette: a speed limit is a road sign, and drivers read it by shape
 * and color before they read the number.
 */
/**
 * One lane of the approach, drawn with the movements it allows.
 *
 * A lane the route can use is drawn solid; the rest are dimmed rather than
 * hidden, because "there are four lanes and yours is the second" is the part
 * a driver actually needs. A lane the map describes only by existing —
 * no movement tagged — borrows the maneuver's own arrow, which is the
 * honest best guess and is what the banner above already says.
 */
@Composable
private fun LaneChip(mask: Int, usable: Boolean, fallback: Double) {
    val tint = if (usable) MaterialTheme.colorScheme.onSurface
    else MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.38f)
    Column(
        horizontalAlignment = Alignment.CenterHorizontally,
        modifier = Modifier
            .width(34.dp)
            .clip(RoundedCornerShape(8.dp))
            .background(
                if (usable) MaterialTheme.colorScheme.primary.copy(alpha = 0.14f)
                else Color.Transparent
            )
            .padding(vertical = 5.dp)
    ) {
        Row(horizontalArrangement = Arrangement.spacedBy(1.dp)) {
            // Two glyphs is all that fits at a glance; a lane allowing more
            // movements than that is vanishingly rare and reads as clutter.
            for (icon in laneIcons(mask, fallback).take(2)) {
                Icon(icon, contentDescription = null, tint = tint, modifier = Modifier.size(19.dp))
            }
        }
    }
}

/** Arrows for a lane's movement bits, most-left movement first. */
private fun laneIcons(mask: Int, fallback: Double): List<ImageVector> {
    if (mask == 0) return listOf(maneuverIconForDelta(fallback))
    val icons = mutableListOf<ImageVector>()
    if (mask and LaneTurn.REVERSE != 0) icons += Icons.Filled.UTurnLeft
    if (mask and (LaneTurn.SHARP_LEFT or LaneTurn.LEFT) != 0) icons += Icons.Filled.TurnLeft
    if (mask and LaneTurn.SLIGHT_LEFT != 0) icons += Icons.Filled.TurnSlightLeft
    if (mask and LaneTurn.THROUGH != 0) icons += Icons.Filled.Straight
    if (mask and LaneTurn.SLIGHT_RIGHT != 0) icons += Icons.Filled.TurnSlightRight
    if (mask and (LaneTurn.SHARP_RIGHT or LaneTurn.RIGHT) != 0) icons += Icons.Filled.TurnRight
    return icons.ifEmpty { listOf(Icons.Filled.Straight) }
}

private fun maneuverIconForDelta(delta: Double): ImageVector = when {
    abs(delta) > 150 -> Icons.Filled.UTurnLeft
    delta <= -60 -> Icons.Filled.TurnLeft
    delta <= -22 -> Icons.Filled.TurnSlightLeft
    delta >= 60 -> Icons.Filled.TurnRight
    delta >= 22 -> Icons.Filled.TurnSlightRight
    else -> Icons.Filled.Straight
}

@Composable
private fun SpeedLimitSign(limitKmh: Int, at: LatLon?) {
    // The sign a driver actually sees out of the windscreen: a white plate in
    // the US and Canada, a red-ringed disc most other places.
    if (at != null && postsRectangularSpeedLimits(at.lat, at.lon)) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.Center,
            modifier = Modifier
                // Wide enough for three digits: a 100 km/h limit was being
                // clipped by the plate meant to display it.
                .size(width = 56.dp, height = 56.dp)
                .shadow(4.dp, SIGN_PLATE)
                .background(Color.White, SIGN_PLATE)
                .border(2.5.dp, Color(0xFF14161D), SIGN_PLATE)
                .padding(horizontal = 2.dp)
        ) {
            Text(
                stringResource(R.string.nav_speed_limit_label),
                style = MaterialTheme.typography.labelSmall,
                fontSize = 8.5.sp,
                lineHeight = 9.sp,
                letterSpacing = (-0.2).sp,
                fontWeight = FontWeight.Bold,
                maxLines = 1,
                softWrap = false,
                color = Color(0xFF14161D)
            )
            Text(
                limitKmh.toString(),
                style = MaterialTheme.typography.headlineSmall,
                fontSize = if (limitKmh >= 100) 23.sp else 27.sp,
                lineHeight = 29.sp,
                maxLines = 1,
                softWrap = false,
                fontWeight = FontWeight.Black,
                color = Color(0xFF14161D)
            )
        }
        return
    }
    Box(
        contentAlignment = Alignment.Center,
        modifier = Modifier
            .size(52.dp)
            .shadow(4.dp, CircleShape)
            .background(Color.White, CircleShape)
            .border(6.dp, Color(0xFFD7382C), CircleShape)
    ) {
        Text(
            limitKmh.toString(),
            style = MaterialTheme.typography.titleMedium,
            fontWeight = FontWeight.Black,
            color = Color(0xFF14161D)
        )
    }
}

/** The signed turn angle onto a step, the same way the maneuver glyph reads it. */
private fun turnDeltaAtStep(route: Route, index: Int): Double {
    val at = route.steps.getOrNull(index)?.at ?: return 0.0
    val geometry = route.geometry
    if (at <= 0 || at >= geometry.size - 1) return 0.0
    return bearingDelta(
        bearingDegrees(geometry[at - 1], geometry[at]),
        bearingDegrees(geometry[at], geometry[at + 1])
    )
}

private fun itineraryIcon(route: Route, line: ItineraryLine): ImageVector = when (line.maneuver) {
    // A slip road curves gently whichever way it goes, so the arrow has to
    // come from what the ramp does rather than from its geometry — otherwise
    // leaving a motorway and joining one draw the same picture.
    Maneuver.Exit -> Icons.Filled.CallSplit
    Maneuver.Merge -> Icons.Filled.MergeType
    else -> maneuverIcon(route, line.index)
}

private fun travelModeIcon(mode: TravelMode): ImageVector = when (mode) {
    TravelMode.Car -> Icons.Filled.DirectionsCar
    TravelMode.Bike -> Icons.Filled.DirectionsBike
    TravelMode.Walk -> Icons.Filled.DirectionsWalk
}

/** Highway plates have barely-rounded corners, not pill ends. */
private val SIGN_PLATE = RoundedCornerShape(5.dp)

/**
 * Maneuver glyph inferred from the turn angle between the segment entering a
 * step and the one leaving it — the route index carries geometry and street
 * names, so the arrow is derived rather than transmitted.
 */
private fun maneuverIcon(route: Route, stepIndex: Int): ImageVector {
    val step = route.steps.getOrNull(stepIndex) ?: return Icons.Filled.Straight
    val at = step.at
    val geometry = route.geometry
    if (at <= 0 || at >= geometry.size - 1) return Icons.Filled.Straight

    val incoming = bearingDegrees(geometry[(at - 1).coerceAtLeast(0)], geometry[at])
    val outgoing = bearingDegrees(geometry[at], geometry[(at + 1).coerceAtMost(geometry.size - 1)])
    val delta = bearingDelta(incoming, outgoing)

    return when {
        abs(delta) > 150 -> Icons.Filled.UTurnLeft
        delta <= -60 -> Icons.Filled.TurnLeft
        delta <= -22 -> Icons.Filled.TurnSlightLeft
        delta >= 60 -> Icons.Filled.TurnRight
        delta >= 22 -> Icons.Filled.TurnSlightRight
        else -> Icons.Filled.Straight
    }
}
