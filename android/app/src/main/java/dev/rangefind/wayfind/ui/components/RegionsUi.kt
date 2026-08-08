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
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
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
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.platform.LocalConfiguration
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
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.selection.selectableGroup
import androidx.compose.material.icons.filled.RadioButtonChecked
import androidx.compose.material.icons.filled.RadioButtonUnchecked
import androidx.compose.material3.Switch
import androidx.compose.ui.semantics.Role
import dev.rangefind.wayfind.nav.TravelMode
import dev.rangefind.wayfind.region.RegionEntry
import dev.rangefind.wayfind.region.RegionStatus
import dev.rangefind.wayfind.ui.formatBytes
import java.text.DateFormat
import java.util.Date

/**
 * The three things this sheet settles, kept apart.
 *
 * They arrived one at a time and were simply stacked: where indexes come from,
 * whether the phone joins the traffic mesh and what it publishes, and whether
 * drives are recorded. Three unrelated decisions in one column, and the column
 * outgrew the screen.
 */
private enum class SettingsTab { Maps, Traffic, Diagnostics }

/**
 * Settings: offline regions, live traffic, and diagnostics.
 *
 * An index is a deliberate multi-megabyte download, so every region row states
 * its size and the user drives every transition: preload, refresh, delete, and
 * which one routing actually uses.
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
    pulseMesh: dev.rangefind.wayfind.engine.PulseMeshStatus?,
    shareFine: Boolean,
    onLiveTraffic: (Boolean) -> Unit,
    onContributeTraffic: (Boolean) -> Unit,
    onSimulatedTraffic: (Boolean) -> Unit,
    onShareMode: (Boolean) -> Unit,
    onShareDrive: () -> Unit,
    onHandOver: () -> Unit,
    /** Whether a ticket is held at all — on disk counts, a live run is not required. */
    canHandOver: Boolean,
    /**
     * This device's identity and the devices it can hand a job to
     * (threads §20.9). It lives here rather than beside a drive because
     * enrolment is a prior act, not a per-trip one: a replacement courier
     * has to have been enrolled before the first one's bike breaks.
     */
    deviceIdentity: dev.rangefind.wayfind.engine.DeviceIdentity?,
    deviceKeyProtected: Boolean,
    enrolledDevices: List<dev.rangefind.wayfind.region.EnrolledDevice>,
    onDeviceNameChange: (String) -> Unit,
    onShowDeviceCard: () -> Unit,
    onForgetDevice: (String) -> Unit,
    /**
     * The offers this device has bid on (§20.4). Here rather than beside
     * a drive for the same reason enrolment is: a bid is a prior act
     * whose whole worth is that it outlives the moment it was made.
     */
    heldOffers: List<dev.rangefind.wayfind.region.HeldOffer>,
    onForgetHeldOffer: (String) -> Unit,
    onClose: () -> Unit
) {
    Box(
        Modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.scrim.copy(alpha = 0.32f))
            .clickable(onClick = onClose)
    )

    // Which group is showing. Local to the sheet: nothing outside it has an
    // opinion, and it starts again at Maps each time the sheet is opened.
    var tab by remember { mutableStateOf(SettingsTab.Maps) }

    Box(Modifier.fillMaxSize(), contentAlignment = Alignment.BottomCenter) {
        SheetSurface(bottomInset = bottomInset, resizable = true) {
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 20.dp)
            ) {
                Text(
                    stringResource(R.string.settings_title),
                    style = MaterialTheme.typography.titleLarge,
                    modifier = Modifier.weight(1f)
                )
                Icon(
                    Icons.Filled.Close,
                    contentDescription = stringResource(R.string.action_close),
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier
                        .size(24.dp)
                        .clickable { onClose() }
                )
            }

            Spacer(Modifier.height(12.dp))
            SettingsTabs(selected = tab, onSelect = { tab = it })
            Spacer(Modifier.height(8.dp))

            // The sheet grew with its content and never scrolled, so with live
            // traffic switched on everything below it — trip recording, sharing
            // a trace — sat off the bottom edge with no way to reach it. The
            // content area is capped at whatever the handle has been dragged to
            // and scrolls; the header and the tabs stay put, so the way out and
            // the way between sections are always on screen.
            Column(
                Modifier
                    .heightIn(max = LocalSheetContentMaxHeight.current)
                    .verticalScroll(rememberScrollState())
            ) {
                when (tab) {
                    SettingsTab.Maps -> MapsSettings(
                        regions = regions,
                        host = host,
                        onHostChange = onHostChange,
                        onPreload = onPreload,
                        onDelete = onDelete,
                        onActivate = onActivate
                    )

                    SettingsTab.Traffic -> Column {
                        LiveTrafficSection(
                            status = pulseMesh,
                            shareFine = shareFine,
                            onEnabled = onLiveTraffic,
                            onContribute = onContributeTraffic,
                            onSimulated = onSimulatedTraffic,
                            onShareMode = onShareMode,
                            onShareDrive = onShareDrive,
                            onHandOver = onHandOver,
                            canHandOver = canHandOver
                        )
                        // Directly under hand-over, because it is the thing
                        // hand-over now depends on: a job is sealed to an
                        // enrolled device or it does not move at all.
                        Spacer(Modifier.height(20.dp))
                        DeviceIdentitySection(
                            identity = deviceIdentity,
                            keyProtected = deviceKeyProtected,
                            enrolled = enrolledDevices,
                            onNameChange = onDeviceNameChange,
                            onShowCard = onShowDeviceCard,
                            onForget = onForgetDevice
                        )
                        // Under the card, because bidding *is* sending
                        // that card: what is listed here is every offer
                        // this device sent it for, and the commitment
                        // each award will be checked against.
                        Spacer(Modifier.height(20.dp))
                        HeldOffersSection(
                            offers = heldOffers,
                            onForget = onForgetHeldOffer
                        )
                    }

                    SettingsTab.Diagnostics -> DiagnosticsSettings(
                        recordTrips = recordTrips,
                        hasTrace = hasTrace,
                        onRecordTripsChange = onRecordTripsChange,
                        onShareTrace = onShareTrace
                    )
                }
            }
        }
    }
}

/** The section picker, drawn like the travel-mode switch it sits above. */
@Composable
private fun SettingsTabs(selected: SettingsTab, onSelect: (SettingsTab) -> Unit) {
    Row(
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 20.dp)
            .selectableGroup()
    ) {
        for (tab in SettingsTab.entries) {
            val chosen = tab == selected
            Surface(
                onClick = { onSelect(tab) },
                shape = RoundedCornerShape(12.dp),
                color = if (chosen) MaterialTheme.colorScheme.primary
                else MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.6f),
                modifier = Modifier
                    .weight(1f)
                    .height(44.dp)
            ) {
                Row(
                    horizontalArrangement = Arrangement.Center,
                    verticalAlignment = Alignment.CenterVertically,
                    modifier = Modifier.fillMaxSize()
                ) {
                    Text(
                        stringResource(
                            when (tab) {
                                SettingsTab.Maps -> R.string.settings_tab_maps
                                SettingsTab.Traffic -> R.string.settings_tab_traffic
                                SettingsTab.Diagnostics -> R.string.settings_tab_diagnostics
                            }
                        ),
                        style = MaterialTheme.typography.labelLarge,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                        color = if (chosen) MaterialTheme.colorScheme.onPrimary
                        else MaterialTheme.colorScheme.onSurface
                    )
                }
            }
        }
    }
}

@Composable
private fun MapsSettings(
    regions: List<RegionEntry>,
    host: String,
    onHostChange: (String) -> Unit,
    onPreload: (String) -> Unit,
    onDelete: (String) -> Unit,
    onActivate: (String?) -> Unit
) {
    Column {
        Text(
            stringResource(R.string.region_offline_subtitle),
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(horizontal = 20.dp)
        )

        Spacer(Modifier.height(10.dp))

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

        // Routing comes from exactly one place, and the network is one of
        // the candidates rather than a switch over them. Drawn as a tick
        // it read as a checkbox that would not clear: tapping the chosen
        // option can do nothing, because something has to be chosen. A
        // radio says that, and says it before the tap.
        val onNetwork = regions.none { it.active }
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier
                .fillMaxWidth()
                .selectable(
                    selected = onNetwork,
                    role = Role.RadioButton,
                    onClick = { onActivate(null) }
                )
                .padding(horizontal = 20.dp, vertical = 14.dp)
        ) {
            Icon(
                if (onNetwork) Icons.Filled.RadioButtonChecked
                else Icons.Filled.RadioButtonUnchecked,
                contentDescription = null,
                tint = if (onNetwork) MaterialTheme.colorScheme.primary
                else MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.size(20.dp)
            )
            Spacer(Modifier.width(14.dp))
            Text(
                stringResource(R.string.region_use_network_index),
                style = MaterialTheme.typography.bodyLarge,
                color = MaterialTheme.colorScheme.onSurface,
                modifier = Modifier.weight(1f)
            )
            if (onNetwork) InUseChip()
        }
    }
}

/**
 * Diagnostics. A trace is a precise record of where someone drove, so it is
 * opt-in, capped to the last few drives, and never leaves the device unless
 * they share it themselves.
 */
@Composable
private fun DiagnosticsSettings(
    recordTrips: Boolean,
    hasTrace: Boolean,
    onRecordTripsChange: (Boolean) -> Unit,
    onShareTrace: () -> Unit
) {
    Column {
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

@Composable
private fun InUseChip() {
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

@Composable
private fun RegionRow(
    entry: RegionEntry,
    onPreload: () -> Unit,
    onDelete: () -> Unit,
    onActivate: () -> Unit
) {
    val context = LocalContext.current
    val selectable = entry.status == RegionStatus.Ready
    Column(
        Modifier
            .fillMaxWidth()
            // A downloaded region is chosen by tapping it, the same gesture
            // and the same target size as the network row above. The small
            // tick button it replaces was the only way to leave the network,
            // and nothing about the row said so.
            .then(
                if (selectable) Modifier.selectable(
                    selected = entry.active,
                    role = Role.RadioButton,
                    onClick = onActivate
                ) else Modifier
            )
            .padding(horizontal = 20.dp, vertical = 10.dp)
    ) {
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
                        InUseChip()
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
                    RegionAction(
                        Icons.Filled.Refresh,
                        stringResource(R.string.region_action_refresh),
                        onPreload,
                        // The one control that resolves a stale region, so it
                        // stops being just another grey icon when it matters.
                        tint = if (entry.stale) MaterialTheme.colorScheme.primary
                        else MaterialTheme.colorScheme.onSurfaceVariant
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
    onClick: () -> Unit,
    tint: androidx.compose.ui.graphics.Color? = null
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
                tint = tint ?: MaterialTheme.colorScheme.onSurfaceVariant,
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
        // A stored region that no longer matches what the server publishes is
        // the more useful thing to say than the date it was fetched — the date
        // reads as reassurance while the index underneath it is out of date.
        if (stale) context.getString(R.string.region_stale, formatBytes(context, bytes))
        else {
            val stamp = DateFormat.getDateInstance(DateFormat.MEDIUM).format(Date(updatedAt))
            context.getString(R.string.region_ready, formatBytes(context, bytes), stamp)
        }
    }
    RegionStatus.Failed -> error ?: context.getString(R.string.region_download_failed)
}
