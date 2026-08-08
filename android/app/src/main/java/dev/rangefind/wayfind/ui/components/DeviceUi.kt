package dev.rangefind.wayfind.ui.components

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.PhoneAndroid
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.engine.DeviceCard
import dev.rangefind.wayfind.engine.DeviceCardArtifact
import dev.rangefind.wayfind.engine.DeviceIdentity
import dev.rangefind.wayfind.region.EnrolledDevice
import dev.rangefind.wayfind.region.MAX_DEVICE_NAME_BYTES
import dev.rangefind.wayfind.region.cappedDeviceName
import dev.rangefind.wayfind.region.formatFingerprint
import java.text.DateFormat
import java.util.Date

/**
 * This device's identity and the devices it can hand a job to (threads
 * §20.9).
 *
 * It sits in settings rather than beside a drive on purpose: a device
 * card is not a per-trip action. Enrolment is a *prior* act by
 * construction — the replacement courier has to have been enrolled before
 * the first one's bike breaks — so the place for it is the durable one.
 */
@Composable
fun DeviceIdentitySection(
    identity: DeviceIdentity?,
    /** Whether the private key is wrapped by a key the app cannot export. */
    keyProtected: Boolean,
    enrolled: List<EnrolledDevice>,
    onNameChange: (String) -> Unit,
    onShowCard: () -> Unit,
    onForget: (String) -> Unit
) {
    Column {
        Text(
            stringResource(R.string.device_section_title),
            style = MaterialTheme.typography.titleSmall,
            modifier = Modifier.padding(horizontal = 20.dp)
        )
        Spacer(Modifier.height(6.dp))
        Text(
            stringResource(R.string.device_section_note),
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(horizontal = 20.dp)
        )
        Spacer(Modifier.height(12.dp))

        if (identity == null) {
            Text(
                stringResource(R.string.device_card_unavailable),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(horizontal = 20.dp)
            )
            return@Column
        }

        // The name lives in a field rather than behind a dialog: it is on
        // the card the other driver reads, and it is the only part of the
        // identity a person chooses.
        var name by remember(identity.name) { mutableStateOf(identity.name) }
        Surface(
            shape = RoundedCornerShape(14.dp),
            color = MaterialTheme.colorScheme.surfaceVariant,
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 20.dp)
        ) {
            Column(Modifier.padding(horizontal = 14.dp, vertical = 10.dp)) {
                Text(
                    stringResource(R.string.device_name_label),
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                Spacer(Modifier.height(4.dp))
                BasicTextField(
                    value = name,
                    // Capped as it is typed, at the same bound the card
                    // encoder enforces: a name the encoder refuses would
                    // otherwise take the whole card with it.
                    onValueChange = {
                        name = cappedDeviceName(it.replace("\n", ""), MAX_DEVICE_NAME_BYTES)
                        onNameChange(name)
                    },
                    singleLine = true,
                    textStyle = MaterialTheme.typography.bodyMedium.copy(
                        color = MaterialTheme.colorScheme.onSurface
                    ),
                    cursorBrush = SolidColor(MaterialTheme.colorScheme.primary),
                    decorationBox = { inner ->
                        if (name.isEmpty()) {
                            Text(
                                stringResource(R.string.device_name_hint),
                                style = MaterialTheme.typography.bodyMedium,
                                color = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        }
                        inner()
                    },
                    modifier = Modifier.fillMaxWidth()
                )
            }
        }

        Spacer(Modifier.height(10.dp))
        FingerprintBlock(
            fingerprint = identity.fingerprint,
            modifier = Modifier.padding(horizontal = 20.dp)
        )

        Spacer(Modifier.height(4.dp))
        Text(
            stringResource(R.string.device_fingerprint_note),
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(horizontal = 20.dp)
        )

        Spacer(Modifier.height(10.dp))
        Row(Modifier.fillMaxWidth().padding(horizontal = 14.dp)) {
            TextButton(onClick = onShowCard) {
                Text(stringResource(R.string.device_show_card))
            }
        }

        // The one loss this design cannot recover from, said where the key
        // is, before it happens rather than after.
        Text(
            stringResource(R.string.device_key_loss_note),
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(horizontal = 20.dp)
        )
        if (!keyProtected) {
            Spacer(Modifier.height(4.dp))
            Text(
                stringResource(R.string.device_key_plain),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.error,
                modifier = Modifier.padding(horizontal = 20.dp)
            )
        }

        Spacer(Modifier.height(18.dp))
        Text(
            stringResource(R.string.device_roster_title),
            style = MaterialTheme.typography.titleSmall,
            modifier = Modifier.padding(horizontal = 20.dp)
        )
        Spacer(Modifier.height(6.dp))
        if (enrolled.isEmpty()) {
            Text(
                stringResource(R.string.device_roster_empty),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(horizontal = 20.dp)
            )
        } else {
            for (device in enrolled) {
                EnrolledDeviceRow(device = device, onForget = { onForget(device.publicKey) })
            }
        }
    }
}

@Composable
private fun EnrolledDeviceRow(device: EnrolledDevice, onForget: () -> Unit) {
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 20.dp, vertical = 8.dp)
    ) {
        Icon(
            Icons.Filled.PhoneAndroid,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(20.dp)
        )
        Spacer(Modifier.size(12.dp))
        Column(Modifier.weight(1f)) {
            Text(
                device.displayName(stringResource(R.string.device_unnamed)),
                style = MaterialTheme.typography.bodyLarge
            )
            Text(
                formatFingerprint(device.fingerprint),
                style = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            if (device.addedAt > 0) {
                Text(
                    stringResource(
                        R.string.device_added_at,
                        DateFormat.getDateInstance(DateFormat.MEDIUM).format(Date(device.addedAt))
                    ),
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        TextButton(onClick = onForget) {
            Text(stringResource(R.string.device_forget), color = MaterialTheme.colorScheme.error)
        }
    }
}

/**
 * The fingerprint, big enough to read across a counter.
 *
 * Monospaced and spaced into two groups because that is the shape of the
 * act it supports: eight characters in one run is a string nobody can say
 * aloud without losing their place.
 */
@Composable
private fun FingerprintBlock(fingerprint: String, modifier: Modifier = Modifier) {
    Surface(
        shape = RoundedCornerShape(14.dp),
        color = MaterialTheme.colorScheme.surfaceVariant,
        modifier = modifier.fillMaxWidth()
    ) {
        Column(Modifier.padding(horizontal = 14.dp, vertical = 10.dp)) {
            Text(
                stringResource(R.string.device_fingerprint_label),
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            Spacer(Modifier.height(2.dp))
            Text(
                formatFingerprint(fingerprint),
                style = MaterialTheme.typography.headlineSmall.copy(
                    fontFamily = FontFamily.Monospace
                ),
                color = MaterialTheme.colorScheme.onSurface
            )
        }
    }
}

/**
 * This phone's card, for the other phone's camera.
 *
 * There is no scanner in this app by design: the other device's own
 * system camera fires ACTION_VIEW on the `wayfind://device#…` it decodes,
 * and Wayfind opens with the confirmation sheet. Sending the link is the
 * same capability for a driver who is not standing here.
 */
@Composable
fun MyDeviceCardDialog(
    card: DeviceCardArtifact,
    onShare: () -> Unit,
    onClose: () -> Unit
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
                    stringResource(R.string.device_card_title),
                    style = MaterialTheme.typography.titleMedium
                )
                Spacer(Modifier.height(12.dp))
                // White behind the symbol whatever the app theme is: a QR
                // is a contrast code, and one laid straight onto a dark
                // surface is one no scanner will find.
                card.qr?.takeIf { !it.isEmpty }?.let { matrix ->
                    Box(
                        Modifier
                            .fillMaxWidth()
                            .clip(RoundedCornerShape(16.dp))
                            .background(Color.White)
                            .padding(12.dp)
                    ) {
                        QrCanvas(matrix, Modifier.fillMaxWidth().aspectRatio(1f))
                    }
                    Spacer(Modifier.height(12.dp))
                }
                Text(
                    card.identity.name.ifBlank { stringResource(R.string.device_unnamed) },
                    style = MaterialTheme.typography.bodyLarge
                )
                Spacer(Modifier.height(6.dp))
                FingerprintBlock(card.identity.fingerprint)
                Spacer(Modifier.height(6.dp))
                Text(
                    stringResource(R.string.device_fingerprint_note),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.End) {
                    TextButton(onClick = onShare) {
                        Text(stringResource(R.string.device_card_share))
                    }
                    Spacer(Modifier.size(6.dp))
                    TextButton(onClick = onClose) {
                        Text(stringResource(R.string.device_card_close))
                    }
                }
            }
        }
    }
}

/**
 * A card that arrived, before it is trusted.
 *
 * The fingerprint is the whole content of this screen. §20.9 is explicit
 * that four bytes is not a cryptographic binding and does not claim to
 * be: it is eight characters two people can read to each other in three
 * seconds, and that comparison is the only thing standing between
 * enrolment and a screen in a depot showing somebody else's card.
 */
@Composable
fun EnrolDeviceDialog(
    card: DeviceCard,
    onEnrol: () -> Unit,
    onDismiss: () -> Unit
) {
    Dialog(onDismissRequest = onDismiss) {
        Surface(
            shape = RoundedCornerShape(22.dp),
            color = MaterialTheme.colorScheme.surface,
            modifier = Modifier.fillMaxWidth()
        ) {
            Column(Modifier.padding(20.dp)) {
                Text(
                    stringResource(R.string.device_enrol_title),
                    style = MaterialTheme.typography.titleMedium
                )
                Spacer(Modifier.height(12.dp))
                Text(
                    card.name.ifBlank { stringResource(R.string.device_unnamed) },
                    style = MaterialTheme.typography.bodyLarge
                )
                Spacer(Modifier.height(8.dp))
                FingerprintBlock(card.fingerprint)
                Spacer(Modifier.height(8.dp))
                Text(
                    stringResource(R.string.device_enrol_note),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                Spacer(Modifier.height(12.dp))
                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.End) {
                    TextButton(onClick = onDismiss) {
                        Text(stringResource(R.string.device_enrol_cancel))
                    }
                    Spacer(Modifier.size(6.dp))
                    TextButton(onClick = onEnrol) {
                        Text(stringResource(R.string.device_enrol_confirm))
                    }
                }
            }
        }
    }
}

/**
 * Which enrolled device this job is about to be sealed to.
 *
 * Asked explicitly even when there is only one candidate: the answer
 * decides which single phone in the world can open the ticket, and a
 * depot with two vans in it is exactly where that matters.
 */
@Composable
fun HandoverTargetDialog(
    devices: List<EnrolledDevice>,
    onPick: (EnrolledDevice) -> Unit,
    onDismiss: () -> Unit
) {
    Dialog(onDismissRequest = onDismiss) {
        Surface(
            shape = RoundedCornerShape(22.dp),
            color = MaterialTheme.colorScheme.surface,
            modifier = Modifier.fillMaxWidth()
        ) {
            Column(Modifier.padding(20.dp)) {
                Text(
                    stringResource(R.string.mesh_handover_pick_title),
                    style = MaterialTheme.typography.titleMedium
                )
                Spacer(Modifier.height(6.dp))
                Text(
                    stringResource(R.string.mesh_handover_pick_note),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                Spacer(Modifier.height(10.dp))
                Column(
                    Modifier
                        .heightIn(max = 320.dp)
                        .verticalScroll(rememberScrollState())
                ) {
                    for (device in devices) {
                        Surface(
                            onClick = { onPick(device) },
                            shape = RoundedCornerShape(14.dp),
                            color = MaterialTheme.colorScheme.surfaceVariant,
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(vertical = 4.dp)
                        ) {
                            Column(Modifier.padding(horizontal = 14.dp, vertical = 10.dp)) {
                                Text(
                                    device.displayName(stringResource(R.string.device_unnamed)),
                                    style = MaterialTheme.typography.bodyLarge
                                )
                                Text(
                                    formatFingerprint(device.fingerprint),
                                    style = MaterialTheme.typography.bodySmall.copy(
                                        fontFamily = FontFamily.Monospace
                                    ),
                                    color = MaterialTheme.colorScheme.onSurfaceVariant
                                )
                            }
                        }
                    }
                }
                Spacer(Modifier.height(10.dp))
                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.End) {
                    TextButton(onClick = onDismiss) {
                        Text(stringResource(R.string.mesh_handover_pick_cancel))
                    }
                }
            }
        }
    }
}
