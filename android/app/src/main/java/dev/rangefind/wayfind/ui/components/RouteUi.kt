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
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.itemsIndexed
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.Navigation
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
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import dev.rangefind.wayfind.engine.Route
import dev.rangefind.wayfind.nav.bearingDegrees
import dev.rangefind.wayfind.nav.bearingDelta
import dev.rangefind.wayfind.ui.UiState
import dev.rangefind.wayfind.ui.formatArrivalClock
import dev.rangefind.wayfind.ui.formatBytes
import dev.rangefind.wayfind.ui.formatDistance
import dev.rangefind.wayfind.ui.formatDuration
import dev.rangefind.wayfind.ui.formatManeuverDistance
import dev.rangefind.wayfind.ui.formatSpeed
import dev.rangefind.wayfind.ui.theme.LocalMapPalette
import kotlin.math.abs

@Composable
fun DirectionsSheet(
    state: UiState,
    bottomInset: Dp,
    onSelectRoute: (Int) -> Unit,
    onStart: () -> Unit,
    onClose: () -> Unit
) {
    SheetSurface(bottomInset = bottomInset) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 20.dp)
        ) {
            Column(Modifier.weight(1f)) {
                Text(
                    "To ${state.selected?.name ?: "destination"}",
                    style = MaterialTheme.typography.labelMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
            }
            Icon(
                Icons.Filled.Close,
                contentDescription = "Close",
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier
                    .size(22.dp)
                    .clickable { onClose() }
            )
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
                    Text("Fetching byte ranges…", style = MaterialTheme.typography.bodyLarge)
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
                            formatDuration(route.seconds),
                            style = MaterialTheme.typography.displaySmall,
                            color = MaterialTheme.colorScheme.primary
                        )
                        Spacer(Modifier.width(12.dp))
                        Text(
                            "${formatDistance(route.distanceMeters)} · arrive ${formatArrivalClock(route.seconds)}",
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
                                    label = if (index == 0) "Fastest" else "Alt ${index}",
                                    detail = formatDuration(candidate.seconds),
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
                        Text("Start", style = MaterialTheme.typography.labelLarge)
                    }

                    Spacer(Modifier.height(12.dp))
                    StepsPreview(route)

                    Spacer(Modifier.height(10.dp))
                    Text(
                        "${route.httpRequests} range requests · ${formatBytes(route.bytesFetched)} fetched",
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
    LazyColumn(Modifier.heightIn(max = 132.dp)) {
        itemsIndexed(route.steps.take(12)) { index, step ->
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 20.dp, vertical = 7.dp)
            ) {
                Icon(
                    maneuverIcon(route, index),
                    contentDescription = null,
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(18.dp)
                )
                Spacer(Modifier.width(14.dp))
                Text(
                    step.name.ifBlank { "Unnamed road" },
                    style = MaterialTheme.typography.bodyMedium,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                    modifier = Modifier.weight(1f)
                )
                Text(
                    formatDistance(step.meters),
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
    onStop: () -> Unit
) {
    val nav = state.nav
    val route = state.activeRoute
    val palette = LocalMapPalette.current

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
                        if (route != null && nav != null) maneuverIcon(route, nav.stepIndex + 1)
                        else Icons.Filled.Straight,
                        contentDescription = null,
                        tint = MaterialTheme.colorScheme.onPrimary,
                        modifier = Modifier.size(42.dp)
                    )
                    Spacer(Modifier.width(16.dp))
                    Column(Modifier.weight(1f)) {
                        Text(
                            nav?.let { formatManeuverDistance(it.metersToManeuver) } ?: "—",
                            style = MaterialTheme.typography.headlineMedium,
                            color = MaterialTheme.colorScheme.onPrimary
                        )
                        Text(
                            nav?.nextStepName?.ifBlank { nav.stepName }?.ifBlank { "Continue" } ?: "Continue",
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
                            "Rerouting",
                            style = MaterialTheme.typography.labelMedium,
                            color = MaterialTheme.colorScheme.onPrimary
                        )
                    }
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
                    .padding(start = 16.dp, bottom = bottomInset + 122.dp)
            ) {
                Surface(shape = CircleShape, color = MaterialTheme.colorScheme.surface, shadowElevation = 4.dp) {
                    Column(
                        horizontalAlignment = Alignment.CenterHorizontally,
                        modifier = Modifier.padding(horizontal = 14.dp, vertical = 8.dp)
                    ) {
                        Text(
                            formatSpeed(it.speedMps),
                            style = MaterialTheme.typography.titleLarge,
                            color = MaterialTheme.colorScheme.onSurface
                        )
                        Text(
                            "km/h",
                            style = MaterialTheme.typography.labelSmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
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
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(start = 22.dp, end = 16.dp, top = 14.dp)
                        .padding(bottom = bottomInset + 14.dp)
                ) {
                    Column(Modifier.weight(1f)) {
                        Text(
                            nav?.let { formatDuration(it.remainingSeconds) } ?: "—",
                            style = MaterialTheme.typography.headlineSmall,
                            color = MaterialTheme.colorScheme.primary
                        )
                        Text(
                            nav?.let {
                                "${formatDistance(it.remainingMeters)} · ${formatArrivalClock(it.remainingSeconds)}"
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
                                contentDescription = "End navigation",
                                tint = MaterialTheme.colorScheme.onErrorContainer
                            )
                        }
                    }
                }
            }
        }
    }
}

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
