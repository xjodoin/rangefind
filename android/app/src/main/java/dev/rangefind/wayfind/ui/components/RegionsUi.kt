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
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Check
import androidx.compose.material.icons.filled.CloudDownload
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.Delete
import androidx.compose.material.icons.filled.Refresh
import androidx.compose.material3.Icon
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import android.content.Context
import dev.rangefind.wayfind.R
import androidx.compose.material.icons.filled.DirectionsBike
import androidx.compose.material.icons.filled.DirectionsCar
import androidx.compose.material.icons.filled.DirectionsWalk
import androidx.compose.material3.Switch
import dev.rangefind.wayfind.nav.TravelMode
import dev.rangefind.wayfind.region.RegionEntry
import dev.rangefind.wayfind.region.RegionStatus
import dev.rangefind.wayfind.ui.formatBytes
import java.text.DateFormat
import java.util.Date

/**
 * Offline region management. An index is a deliberate multi-megabyte download,
 * so every row states its size and the user drives every transition: preload,
 * refresh, delete, and which one routing actually uses.
 */
@Composable
fun RegionsSheet(
    regions: List<RegionEntry>,
    host: String,
    bottomInset: Dp,
    onHostChange: (String) -> Unit,
    onPreload: (String) -> Unit,
    onDelete: (String) -> Unit,
    onActivate: (String?) -> Unit,
    recordTrips: Boolean,
    hasTrace: Boolean,
    onRecordTripsChange: (Boolean) -> Unit,
    onShareTrace: () -> Unit,
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
                Column(Modifier.weight(1f)) {
                    Text(
                        stringResource(R.string.region_offline_title),
                        style = MaterialTheme.typography.titleLarge
                    )
                    Text(
                        stringResource(R.string.region_offline_subtitle),
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                Icon(
                    Icons.Filled.Close,
                    contentDescription = stringResource(R.string.action_close),
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier
                        .size(24.dp)
                        .clickable { onClose() }
                )
            }

            Spacer(Modifier.height(14.dp))

            // A phone on Wi-Fi cannot reach the emulator's loopback alias, so
            // the source host has to be editable rather than compiled in.
            Surface(
                shape = RoundedCornerShape(14.dp),
                color = MaterialTheme.colorScheme.surfaceVariant,
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 20.dp)
            ) {
                Column(Modifier.padding(horizontal = 14.dp, vertical = 10.dp)) {
                    Text(
                        stringResource(R.string.region_source_host),
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    Spacer(Modifier.height(4.dp))
                    BasicTextField(
                        value = host,
                        onValueChange = onHostChange,
                        singleLine = true,
                        textStyle = MaterialTheme.typography.bodyMedium.copy(
                            color = MaterialTheme.colorScheme.onSurface
                        ),
                        cursorBrush = SolidColor(MaterialTheme.colorScheme.primary),
                        modifier = Modifier.fillMaxWidth()
                    )
                }
            }

            Spacer(Modifier.height(6.dp))

            regions.forEach { entry ->
                RegionRow(
                    entry = entry,
                    onPreload = { onPreload(entry.spec.id) },
                    onDelete = { onDelete(entry.spec.id) },
                    onActivate = { onActivate(entry.spec.id) }
                )
            }

            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .fillMaxWidth()
                    .clickable { onActivate(null) }
                    .padding(horizontal = 20.dp, vertical = 12.dp)
            ) {
                Icon(
                    if (regions.none { it.active }) Icons.Filled.Check else Icons.Filled.CloudDownload,
                    contentDescription = null,
                    tint = if (regions.none { it.active }) MaterialTheme.colorScheme.primary
                    else MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(20.dp)
                )
                Spacer(Modifier.width(14.dp))
                Text(
                    stringResource(R.string.region_use_network_index),
                    style = MaterialTheme.typography.bodyLarge,
                    color = MaterialTheme.colorScheme.onSurface
                )
            }

            // Diagnostics. A trace is a precise record of where someone drove,
            // so it is opt-in, capped to the last few drives, and never leaves
            // the device unless they share it themselves.
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .fillMaxWidth()
                    .clickable { onRecordTripsChange(!recordTrips) }
                    .padding(start = 20.dp, end = 12.dp, top = 6.dp, bottom = 6.dp)
            ) {
                Column(Modifier.weight(1f)) {
                    Text(
                        stringResource(R.string.diag_title),
                        style = MaterialTheme.typography.bodyLarge,
                        color = MaterialTheme.colorScheme.onSurface
                    )
                    Text(
                        stringResource(R.string.diag_subtitle),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                Switch(checked = recordTrips, onCheckedChange = onRecordTripsChange)
            }

            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .fillMaxWidth()
                    .clickable(enabled = hasTrace, onClick = onShareTrace)
                    .padding(horizontal = 20.dp, vertical = 12.dp)
            ) {
                Text(
                    stringResource(if (hasTrace) R.string.diag_share else R.string.diag_none),
                    style = MaterialTheme.typography.bodyLarge,
                    color = if (hasTrace) MaterialTheme.colorScheme.primary
                    else MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
    }
}

@Composable
private fun RegionRow(
    entry: RegionEntry,
    onPreload: () -> Unit,
    onDelete: () -> Unit,
    onActivate: () -> Unit
) {
    val context = LocalContext.current
    Column(Modifier.fillMaxWidth().padding(horizontal = 20.dp, vertical = 10.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Column(Modifier.weight(1f)) {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Text(entry.spec.label, style = MaterialTheme.typography.titleMedium)
                        // Three indexes cover Québec — one per way of
                        // travelling — so the place name alone cannot tell
                        // them apart on the row the user is about to download.
                        Spacer(Modifier.width(8.dp))
                        Icon(
                            when (entry.spec.mode) {
                                TravelMode.Car -> Icons.Filled.DirectionsCar
                                TravelMode.Bike -> Icons.Filled.DirectionsBike
                                TravelMode.Walk -> Icons.Filled.DirectionsWalk
                            },
                            contentDescription = stringResource(entry.spec.mode.labelRes),
                            tint = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.size(17.dp)
                        )
                    }
                    if (entry.active) {
                        Spacer(Modifier.width(8.dp))
                        Surface(
                            shape = RoundedCornerShape(6.dp),
                            color = MaterialTheme.colorScheme.primaryContainer
                        ) {
                            Text(
                                stringResource(R.string.region_in_use),
                                style = MaterialTheme.typography.labelSmall,
                                color = MaterialTheme.colorScheme.onPrimaryContainer,
                                modifier = Modifier.padding(horizontal = 7.dp, vertical = 2.dp)
                            )
                        }
                    }
                }
                Text(
                    entry.statusLine(context),
                    style = MaterialTheme.typography.bodyMedium,
                    color = if (entry.status == RegionStatus.Failed) MaterialTheme.colorScheme.error
                    else MaterialTheme.colorScheme.onSurfaceVariant,
                    maxLines = 2,
                    overflow = TextOverflow.Ellipsis
                )
            }

            when (entry.status) {
                RegionStatus.Ready -> {
                    if (!entry.active) {
                        RegionAction(
                            Icons.Filled.Check,
                            stringResource(R.string.region_action_use),
                            onActivate
                        )
                        Spacer(Modifier.width(6.dp))
                    }
                    RegionAction(
                        Icons.Filled.Refresh,
                        stringResource(R.string.region_action_refresh),
                        onPreload
                    )
                    Spacer(Modifier.width(6.dp))
                    RegionAction(
                        Icons.Filled.Delete,
                        stringResource(R.string.region_action_delete),
                        onDelete
                    )
                }
                RegionStatus.Downloading -> Unit
                else -> RegionAction(
                    Icons.Filled.CloudDownload,
                    stringResource(R.string.region_action_preload),
                    onPreload
                )
            }
        }

        if (entry.status == RegionStatus.Downloading) {
            Spacer(Modifier.height(8.dp))
            if (entry.total > 0) {
                LinearProgressIndicator(
                    progress = { entry.progress },
                    modifier = Modifier.fillMaxWidth().height(4.dp)
                )
            } else {
                LinearProgressIndicator(modifier = Modifier.fillMaxWidth().height(4.dp))
            }
        }
    }
}

@Composable
private fun RegionAction(
    icon: androidx.compose.ui.graphics.vector.ImageVector,
    label: String,
    onClick: () -> Unit
) {
    Surface(
        onClick = onClick,
        shape = RoundedCornerShape(12.dp),
        color = MaterialTheme.colorScheme.surfaceVariant
    ) {
        Box(Modifier.size(width = 44.dp, height = 40.dp), contentAlignment = Alignment.Center) {
            Icon(
                icon,
                contentDescription = label,
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.size(20.dp)
            )
        }
    }
}

private fun RegionEntry.statusLine(context: Context): String = when (status) {
    RegionStatus.Absent -> context.getString(spec.noteRes)
    RegionStatus.Downloading ->
        if (total > 0) context.resources.getQuantityString(
            R.plurals.region_downloading, total, done, total, formatBytes(context, bytes)
        )
        else context.getString(R.string.region_reading_manifest)
    RegionStatus.Ready -> {
        val stamp = DateFormat.getDateInstance(DateFormat.MEDIUM).format(Date(updatedAt))
        context.getString(R.string.region_ready, formatBytes(context, bytes), stamp)
    }
    RegionStatus.Failed -> error ?: context.getString(R.string.region_download_failed)
}
