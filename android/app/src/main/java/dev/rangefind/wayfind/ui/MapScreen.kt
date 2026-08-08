package dev.rangefind.wayfind.ui

import androidx.activity.compose.BackHandler
import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInVertically
import androidx.compose.animation.slideOutVertically
import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.asPaddingValues
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawing
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material.icons.filled.Flag
import androidx.compose.material.icons.filled.MyLocation
import androidx.compose.material.icons.filled.ReportProblem
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.hapticfeedback.HapticFeedbackType
import androidx.compose.ui.platform.LocalHapticFeedback
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.platform.LocalFocusManager
import androidx.compose.ui.unit.dp
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.engine.LatLon
import dev.rangefind.wayfind.engine.StopOutcome
import dev.rangefind.wayfind.engine.StopReason
import dev.rangefind.wayfind.engine.Suggestion
import dev.rangefind.wayfind.engine.unresolvedStops
import dev.rangefind.wayfind.ui.components.AttributionChip
import dev.rangefind.wayfind.ui.components.DirectionsSheet
import dev.rangefind.wayfind.ui.components.NavigationOverlay
import dev.rangefind.wayfind.ui.components.PlaceSheet
import dev.rangefind.wayfind.nav.TravelMode
import dev.rangefind.wayfind.engine.DeviceCard
import dev.rangefind.wayfind.region.EnrolledDevice
import dev.rangefind.wayfind.ui.components.ActiveJobCard
import dev.rangefind.wayfind.ui.components.CancelJobDialog
import dev.rangefind.wayfind.ui.components.EnrolDeviceDialog
import dev.rangefind.wayfind.ui.components.FollowedDriveCard
import dev.rangefind.wayfind.ui.components.HandoverTargetDialog
import dev.rangefind.wayfind.ui.components.MyDeviceCardDialog
import dev.rangefind.wayfind.ui.components.IncidentPrompt
import dev.rangefind.wayfind.ui.components.HandoverDialog
import dev.rangefind.wayfind.ui.components.IncidentReportSheet
import dev.rangefind.wayfind.ui.components.JobBidSheet
import dev.rangefind.wayfind.ui.components.JobOfferSheet
import dev.rangefind.wayfind.ui.components.RegionsSheet
import dev.rangefind.wayfind.ui.components.ResultsSheet
import dev.rangefind.wayfind.ui.components.SimulatedTrafficChip
import dev.rangefind.wayfind.ui.components.SkipStopDialog
import dev.rangefind.wayfind.ui.components.rememberPhotoCapture
import dev.rangefind.wayfind.ui.components.SearchField
import dev.rangefind.wayfind.ui.components.SuggestionList
import dev.rangefind.wayfind.ui.map.MapCanvas
import dev.rangefind.wayfind.ui.theme.LocalMapPalette

@Composable
fun MapScreen(
    state: UiState,
    darkTheme: Boolean,
    wideLayout: Boolean,
    onQueryChange: (String) -> Unit,
    onSubmit: () -> Unit,
    onClear: () -> Unit,
    onSuggestion: (Suggestion) -> Unit,
    onSelectResult: (Int) -> Unit,
    onDismissPlace: () -> Unit,
    onDirections: () -> Unit,
    onSelectRoute: (Int) -> Unit,
    onSelectMode: (TravelMode) -> Unit,
    onStartNavigation: () -> Unit,
    onStopNavigation: () -> Unit,
    onExitDirections: () -> Unit,
    onRecenter: () -> Unit,
    onCameraDetached: () -> Unit,
    onShowRegions: (Boolean) -> Unit,
    onRegionHostChange: (String) -> Unit,
    onPreloadRegion: (String) -> Unit,
    onDeleteRegion: (String) -> Unit,
    onActivateRegion: (String?) -> Unit,
    onRecordTripsChange: (Boolean) -> Unit,
    onMarkIssue: () -> Unit,
    onShareTrace: () -> Unit,
    onLiveTraffic: (Boolean) -> Unit,
    onContributeTraffic: (Boolean) -> Unit,
    /** The demo vehicles, with live traffic left running. */
    onSimulatedTraffic: (Boolean) -> Unit,
    onShareDrive: () -> Unit,
    /** Sends the run's tracking link, publishing the stored plan if needed. */
    onShareJobLink: () -> Unit,
    onShareMode: (Boolean) -> Unit,
    onHandOverJob: () -> Unit,
    onAcceptJob: () -> Unit,
    onDeclineJob: () -> Unit,
    /**
     * Bids on the offer on screen by sending this device's card (§20.4).
     * There is no bidding channel in the protocol, and this does not
     * invent one — the card goes over whatever the two parties use.
     */
    onSendCardForOffer: () -> Unit,
    onDismissOffer: () -> Unit,
    /** Withdraws a held bid, and the commitment an award is checked against. */
    onForgetHeldOffer: (String) -> Unit,
    onContinueJob: () -> Unit,
    /**
     * What happened at the stop the run is on: outcome, reason, note, and
     * an optional proof-of-delivery photo as base64 JPEG.
     */
    onMarkStop: (Int, Int, String?, String?) -> Unit,
    /**
     * The driver cannot finish the day: CANCELED on the wire, with their own
     * words as the note. The only path to it is the confirmation dialog.
     */
    onCantFinishJob: (String?) -> Unit,
    onCloseHandover: () -> Unit,
    onShareTicket: () -> Unit,
    /**
     * Which enrolled device the job is sealed to (threads §20.9).
     * Enrolment is the gate on transfer: a job goes to one device's key
     * or it does not go.
     */
    onHandOverTo: (EnrolledDevice) -> Unit,
    onDismissHandoverPicker: () -> Unit,
    onDeviceNameChange: (String) -> Unit,
    onShowDeviceCard: () -> Unit,
    onDismissDeviceCard: () -> Unit,
    onShareDeviceCard: (String) -> Unit,
    onEnrolDevice: (DeviceCard) -> Unit,
    onDismissDeviceOffer: () -> Unit,
    onForgetDevice: (String) -> Unit,
    /**
     * The next driver has the ticket, so this device stops publishing and
     * emits nothing on the way out (§20.5).
     */
    onHandedOverJob: () -> Unit,
    onStopFollowing: () -> Unit,
    onReportIncident: (Int) -> Unit,
    onAnswerIncident: (String, Boolean) -> Unit,
    onClearMeshNotice: () -> Unit,
    hasTrace: Boolean,
    onLongPress: (LatLon) -> Unit,
    onCenterChanged: (LatLon) -> Unit
) {
    val palette = LocalMapPalette.current
    val insets = WindowInsets.safeDrawing.asPaddingValues()
    val density = LocalDensity.current
    var focused by remember { mutableStateOf(false) }
    var reporting by remember { mutableStateOf(false) }
    // The skip sheet belongs to the card that opened it, so it lives here
    // rather than in the view model: nothing outside this screen has an
    // opinion about whether it is open.
    var skipping by remember { mutableStateOf(false) }
    // Giving the day up, confirmed. Local for the same reason the skip sheet
    // is: nothing outside this screen has an opinion about whether it is open.
    var cancelling by remember { mutableStateOf(false) }
    // Delivered, with the doorstep photographed. The camera is what runs
    // first and the claim follows it: a driver who backs out of the camera
    // has marked nothing, and the card is still asking.
    val deliverWithPhoto = rememberPhotoCapture { photo ->
        if (photo != null) onMarkStop(StopOutcome.DELIVERED, StopReason.NONE, null, photo)
    }

    // Floating controls and the map's camera padding follow the sheet's real
    // measured height — guessing it leaves the recenter button stranded behind
    // whichever sheet happens to be taller than the estimate.
    var sheetHeightPx by remember { mutableIntStateOf(0) }
    val sheetHeight = with(density) { sheetHeightPx.toDp() }
    // Tablets, foldables and landscape phones get a panel beside the map
    // instead of a sheet over it: a sheet on a 10" screen wastes the width and
    // covers the very thing the panel is describing.
    val panelWidth = 380.dp
    val panelWidthPx = with(density) { panelWidth.roundToPx() }

    val navigating = state.sheet == SheetMode.Navigating

    // Committing to a place or a route ends the typing session. Without this
    // the field keeps focus and the keyboard stays up over the map, hiding the
    // very suggestion or result the user just tapped — the text field only
    // gave focus back on the IME's own Search action or the clear button.
    val focusManager = LocalFocusManager.current
    val pickSuggestion: (Suggestion) -> Unit = { focusManager.clearFocus(); onSuggestion(it) }
    val selectResult: (Int) -> Unit = { focusManager.clearFocus(); onSelectResult(it) }
    val longPressMap: (LatLon) -> Unit = { focusManager.clearFocus(); onLongPress(it) }
    val startDirections: () -> Unit = { focusManager.clearFocus(); onDirections() }

    BackHandler(enabled = state.sheet != SheetMode.Search || state.query.isNotEmpty()) {
        when (state.sheet) {
            SheetMode.Navigating -> onStopNavigation()
            SheetMode.Directions -> onExitDirections()
            SheetMode.Place -> onDismissPlace()
            SheetMode.Search -> onClear()
        }
    }

    Box(Modifier.fillMaxSize().background(MaterialTheme.colorScheme.background)) {

        MapCanvas(
            state = state,
            darkTheme = darkTheme,
            palette = palette,
            bottomInsetPx = if (wideLayout) 0 else sheetHeightPx,
            startInsetPx = if (wideLayout && !navigating) panelWidthPx else 0,
            onCenterChanged = onCenterChanged,
            onResultTapped = selectResult,
            onRouteTapped = onSelectRoute,
            onLongPress = longPressMap,
            onFollowDismissed = onCameraDetached,
            modifier = Modifier.fillMaxSize()
        )

        if (!wideLayout) {
            // Search chrome hides entirely while navigating: the driver needs the
            // road, not a text field.
            AnimatedVisibility(
                visible = !navigating,
                enter = fadeIn() + slideInVertically { -it },
                exit = fadeOut() + slideOutVertically { -it },
                modifier = Modifier.align(Alignment.TopCenter)
            ) {
                Column(
                    Modifier
                        .fillMaxWidth()
                        .padding(top = insets.calculateTopPadding() + 10.dp)
                        .padding(horizontal = 14.dp)
                ) {
                    SearchField(
                        query = state.query,
                        searching = state.searching,
                        onQueryChange = onQueryChange,
                        onSubmit = onSubmit,
                        onClear = onClear,
                        onFocusChanged = { focused = it }
                    )
                    AnimatedVisibility(visible = state.suggestions.isNotEmpty() && focused) {
                        SuggestionList(
                            suggestions = state.suggestions,
                            onPick = pickSuggestion,
                            modifier = Modifier.padding(top = 8.dp)
                        )
                    }
                }
            }
        }

        // Recenter control sits above whichever sheet is showing. Mid-drive
        // everything else on this stack is hidden — the driver needs the road,
        // not a download button — but the way back to the vehicle appears the
        // moment a pan takes the camera off it. Without it a driver who looked
        // up the road had no way to resume the follow short of ending the trip.
        AnimatedVisibility(
            visible = !navigating,
            enter = fadeIn(),
            exit = fadeOut(),
            modifier = Modifier
                .align(Alignment.BottomEnd)
                .padding(end = 16.dp, bottom = if (wideLayout) 28.dp else sheetHeight + 16.dp)
        ) {
            Column(horizontalAlignment = Alignment.CenterHorizontally) {
                if (!navigating) {
                    Surface(
                        onClick = { onShowRegions(true) },
                        shape = CircleShape,
                        color = MaterialTheme.colorScheme.surface,
                        shadowElevation = 6.dp,
                        modifier = Modifier.size(48.dp)
                    ) {
                        Box(contentAlignment = Alignment.Center) {
                            // A cloud with an arrow said "download something",
                            // which stopped being true once live traffic and
                            // diagnostics moved in beside the regions. It opens
                            // settings, so it is the settings icon — still
                            // tinted while an offline region is what routes.
                            Icon(
                                Icons.Filled.Settings,
                                contentDescription = stringResource(R.string.settings_title),
                                tint = if (state.regions.any { it.active })
                                    MaterialTheme.colorScheme.primary
                                else MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        }
                    }
                    if (state.pulseMesh?.running == true) {
                        Spacer(Modifier.height(10.dp))
                        Surface(
                            onClick = { reporting = true },
                            shape = CircleShape,
                            color = MaterialTheme.colorScheme.surface,
                            shadowElevation = 6.dp,
                            modifier = Modifier.size(48.dp)
                        ) {
                            Box(contentAlignment = Alignment.Center) {
                                Icon(
                                    Icons.Filled.ReportProblem,
                                    contentDescription = stringResource(R.string.mesh_report_title),
                                    tint = MaterialTheme.colorScheme.error
                                )
                            }
                        }
                    }
                    Spacer(Modifier.height(10.dp))
                }
                Surface(
                    onClick = onRecenter,
                    shape = CircleShape,
                    // Mid-drive this is the only control on the map and it is
                    // read at a glance from a driving position, so it is the
                    // accent colour rather than one more grey circle.
                    color = if (navigating) MaterialTheme.colorScheme.primaryContainer
                    else MaterialTheme.colorScheme.surface,
                    shadowElevation = 6.dp,
                    modifier = Modifier.size(if (navigating) 56.dp else 48.dp)
                ) {
                    Box(contentAlignment = Alignment.Center) {
                        Icon(
                            Icons.Filled.MyLocation,
                            contentDescription = stringResource(
                                if (navigating) R.string.action_resume_follow else R.string.action_recenter
                            ),
                            tint = when {
                                navigating -> MaterialTheme.colorScheme.onPrimaryContainer
                                state.userLocation != null -> palette.puck
                                else -> MaterialTheme.colorScheme.onSurfaceVariant
                            }
                        )
                    }
                }
            }
        }

        // What the protocol just declined, and why. Shown briefly and in
        // the driver's own words: "not sent" with no reason is the kind of
        // silence that makes a feature feel broken.
        state.meshNotice?.let { notice ->
            LaunchedEffect(notice) {
                kotlinx.coroutines.delay(4000)
                onClearMeshNotice()
            }
            Surface(
                shape = RoundedCornerShape(14.dp),
                color = MaterialTheme.colorScheme.inverseSurface,
                shadowElevation = 6.dp,
                modifier = Modifier
                    .align(Alignment.BottomCenter)
                    .padding(bottom = sheetHeight + 24.dp, start = 24.dp, end = 24.dp)
            ) {
                Text(
                    notice,
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.inverseOnSurface,
                    modifier = Modifier.padding(horizontal = 16.dp, vertical = 10.dp)
                )
            }
        }

        // A drive somebody shared with this device. It sits above the
        // incident prompt because it is why the app was opened at all when
        // the way in was a link.
        state.following?.let { drive ->
            FollowedDriveCard(
                drive = drive,
                eta = state.followingEta,
                onStop = onStopFollowing,
                modifier = Modifier
                    .align(Alignment.TopCenter)
                    // Clear of the search field: the card is an answer to
                    // something the user already did, not a thing competing
                    // with the box they type into.
                    .padding(top = 150.dp, start = 14.dp, end = 14.dp)
            )
        }

        // How far through the delivery day this van is. Hidden while
        // navigating — the guidance overlay owns the screen then, and the
        // count belongs nowhere near it.
        val job = state.activeJob
        if (job != null && !navigating) {
            ActiveJobCard(
                job = job,
                arrived = state.arrivedAtJobStop,
                onContinue = onContinueJob,
                onDelivered = { onMarkStop(StopOutcome.DELIVERED, StopReason.NONE, null, null) },
                onDeliveredWithPhoto = deliverWithPhoto,
                onSkip = { skipping = true },
                onShareLink = onShareJobLink,
                onHandOver = onHandOverJob,
                // The ticket, not the run: a process death took the run and
                // left the capability on disk, and the driver still has a job
                // to pass to the next van.
                canHandOver = job.ticketBase64 != null ||
                    state.pulseMesh?.sharing?.fromTicket == true,
                onCantFinish = { cancelling = true },
                publishing = state.pulseMesh?.sharing != null,
                modifier = Modifier
                    .align(Alignment.TopCenter)
                    // Below a followed drive when both are up: that card is
                    // why the app was opened at all in that case.
                    .padding(
                        top = if (state.following != null) 244.dp else 150.dp,
                        start = 14.dp,
                        end = 14.dp
                    )
            )
        }

        // The nearest live incident, while driving. Confirming or refuting
        // one is how §8.5 gets the distinct-peer corroboration that turns a
        // claim into something the router is allowed to act on.
        val ahead = if (navigating) state.incidents.firstOrNull() else null
        val incidentTop = if (state.following != null) 244.dp else 140.dp
        if (ahead != null) {
            IncidentPrompt(
                incident = ahead,
                onAnswer = { still -> onAnswerIncident(ahead.key, still) },
                modifier = Modifier
                    .align(Alignment.TopCenter)
                    .padding(top = incidentTop, start = 14.dp, end = 14.dp)
            )
        }

        // Who is responsible for what is on screen. ODbL and CARTO both
        // require visible attribution, so it rides just above the sheet
        // rather than being hidden behind it — and the same corner says when
        // the traffic being drawn was invented on this phone, which is a
        // claim about the map that belongs on the map. The label stays up
        // mid-drive, where the attribution goes away: that is exactly when a
        // driver is deciding whether to believe a jam.
        Column(
            modifier = Modifier
                .align(Alignment.BottomStart)
                .padding(
                    start = if (wideLayout && !navigating) panelWidth + 26.dp else 14.dp,
                    bottom = if (wideLayout) 14.dp else sheetHeight + 10.dp
                )
        ) {
            if (state.pulseMesh?.simulated == true) {
                SimulatedTrafficChip()
                Spacer(Modifier.height(6.dp))
            }
            if (!navigating) AttributionChip(info = state.info)
        }

        if (!wideLayout) {
            Column(
                Modifier
                    .align(Alignment.BottomCenter)
                    .fillMaxWidth()
                    .onSizeChanged { sheetHeightPx = it.height }
            ) {
                when (state.sheet) {
                    SheetMode.Search -> ResultsSheet(
                        state = state,
                        bottomInset = insets.calculateBottomPadding(),
                        onSelect = selectResult
                    )

                    SheetMode.Place -> PlaceSheet(
                        state = state,
                        bottomInset = insets.calculateBottomPadding(),
                        onDismiss = onDismissPlace,
                        onDirections = startDirections
                    )

                    SheetMode.Directions -> DirectionsSheet(
                        state = state,
                        bottomInset = insets.calculateBottomPadding(),
                        onSelectRoute = onSelectRoute,
                        onSelectMode = onSelectMode,
                        onStart = onStartNavigation,
                        onClose = onExitDirections
                    )

                    SheetMode.Navigating -> Unit
                }
            }
        } else {
            AnimatedVisibility(
                visible = !navigating,
                enter = fadeIn(),
                exit = fadeOut(),
                modifier = Modifier.align(Alignment.TopStart)
            ) {
                Surface(
                    shape = MaterialTheme.shapes.large,
                    color = MaterialTheme.colorScheme.surface,
                    shadowElevation = 14.dp,
                    modifier = Modifier
                        .padding(
                            start = 14.dp,
                            top = insets.calculateTopPadding() + 12.dp,
                            bottom = insets.calculateBottomPadding() + 12.dp
                        )
                        .width(panelWidth)
                        // Wrap the content: an empty full-height slab beside a
                        // map reads as a loading failure, not a panel.
                        .heightIn(max = 640.dp)
                ) {
                    Column(Modifier.padding(vertical = 12.dp)) {
                        Box(Modifier.padding(horizontal = 12.dp)) {
                            SearchField(
                                query = state.query,
                                searching = state.searching,
                                onQueryChange = onQueryChange,
                                onSubmit = onSubmit,
                                onClear = onClear,
                                onFocusChanged = { focused = it }
                            )
                        }
                        AnimatedVisibility(visible = state.suggestions.isNotEmpty() && focused) {
                            SuggestionList(
                                suggestions = state.suggestions,
                                onPick = pickSuggestion,
                                modifier = Modifier.padding(horizontal = 12.dp, vertical = 8.dp)
                            )
                        }
                        Spacer(Modifier.height(4.dp))
                        when (state.sheet) {
                            SheetMode.Search -> ResultsSheet(
                                state = state,
                                bottomInset = 0.dp,
                                onSelect = selectResult
                            )

                            SheetMode.Place -> PlaceSheet(
                                state = state,
                                bottomInset = 0.dp,
                                onDismiss = onDismissPlace,
                                onDirections = startDirections
                            )

                            SheetMode.Directions -> DirectionsSheet(
                                state = state,
                                bottomInset = 0.dp,
                                onSelectRoute = onSelectRoute,
                        onSelectMode = onSelectMode,
                                onStart = onStartNavigation,
                                onClose = onExitDirections
                            )

                            SheetMode.Navigating -> Unit
                        }
                    }
                }
            }
        }

        if (navigating) {
            NavigationOverlay(
                state = state,
                topInset = insets.calculateTopPadding(),
                bottomInset = insets.calculateBottomPadding(),
                onSelectRoute = onSelectRoute,
                onMarkIssue = onMarkIssue,
                onRecenter = onRecenter,
                onStop = onStopNavigation
            )
        }

        if (state.showRegions) {
            RegionsSheet(
                regions = state.regions,
                host = state.regionHost,
                bottomInset = insets.calculateBottomPadding(),
                onHostChange = onRegionHostChange,
                onPreload = onPreloadRegion,
                onDelete = onDeleteRegion,
                onActivate = onActivateRegion,
                recordTrips = state.recordTrips,
                hasTrace = hasTrace,
                onRecordTripsChange = onRecordTripsChange,
                onShareTrace = onShareTrace,
                pulseMesh = state.pulseMesh,
                shareFine = state.shareFine,
                onLiveTraffic = onLiveTraffic,
                onContributeTraffic = onContributeTraffic,
                onSimulatedTraffic = onSimulatedTraffic,
                onShareMode = onShareMode,
                onShareDrive = onShareDrive,
                onHandOver = onHandOverJob,
                canHandOver = state.activeJob?.ticketBase64 != null ||
                    state.pulseMesh?.sharing?.fromTicket == true,
                deviceIdentity = state.deviceIdentity,
                deviceKeyProtected = state.deviceKeyProtected,
                enrolledDevices = state.enrolledDevices,
                heldOffers = state.heldOffers,
                onForgetHeldOffer = onForgetHeldOffer,
                onDeviceNameChange = onDeviceNameChange,
                onShowDeviceCard = onShowDeviceCard,
                onForgetDevice = onForgetDevice,
                onClose = { onShowRegions(false) }
            )
        }

        // This phone's card, for the other driver's camera. There is no
        // scanner here by design: their system camera fires ACTION_VIEW on
        // the `wayfind://device#…` it decodes, and Wayfind opens.
        state.myDeviceCard?.let { card ->
            MyDeviceCardDialog(
                card = card,
                onShare = { onShareDeviceCard(card.url) },
                onClose = onDismissDeviceCard
            )
        }

        // A card that arrived, before it is trusted. The fingerprint is the
        // whole point of the screen — enrolment over a QR is only as good
        // as the enroller's ability to tell they scanned the right one.
        state.deviceOffer?.let { card ->
            EnrolDeviceDialog(
                card = card,
                onEnrol = { onEnrolDevice(card) },
                onDismiss = onDismissDeviceOffer
            )
        }

        // Which single device in the world will be able to open this job.
        if (state.handoverPicking) {
            HandoverTargetDialog(
                devices = state.enrolledDevices,
                onPick = onHandOverTo,
                onDismiss = onDismissHandoverPicker
            )
        }

        // A dispatched job, before it is taken. A ticket is a publish
        // capability — whoever holds it can move the vehicle every
        // customer is watching — so it is shown and accepted, never acted
        // on the moment it arrives.
        state.jobOffer?.let { offer ->
            JobOfferSheet(
                offer = offer,
                bottomInset = insets.calculateBottomPadding(),
                // Whether this is the job that was bid on (§20.4). Silent
                // on a direct dispatch, which is what a phone holding no
                // bids has always seen.
                bid = state.awardBid,
                onAccept = onAcceptJob,
                onDecline = onDeclineJob
            )
        }

        // A dispatcher's broadcast offer. Its own sheet, because the
        // decision is a bid rather than an accept: it grants nothing,
        // moves no vehicle and carries no address at all.
        state.offerOnScreen?.let { offer ->
            JobBidSheet(
                offer = offer,
                bottomInset = insets.calculateBottomPadding(),
                onSendCard = onSendCardForOffer,
                onDismiss = onDismissOffer
            )
        }

        // Why a stop was not delivered, and which of the two things
        // happened — skipped is "I did not go", failed is "I went and
        // could not hand it over", and the customer is owed the difference.
        if (skipping && job != null) {
            SkipStopDialog(
                stopNumber = job.nextIndex,
                photoAllowed = job.fine,
                onDismiss = { skipping = false },
                onMark = { outcome, reason, note, photo ->
                    skipping = false
                    onMarkStop(outcome, reason, note, photo)
                }
            )
        }

        // Giving the day up. Confirmed rather than done, because CANCELED
        // reaches every customer holding a link and there is no unsending it.
        if (cancelling && job != null) {
            CancelJobDialog(
                unresolved = unresolvedStops(job),
                total = job.total,
                onDismiss = { cancelling = false },
                onCancelJob = { reason ->
                    cancelling = false
                    onCantFinishJob(reason)
                }
            )
        }

        // The same ticket back on screen, for the next driver's camera.
        state.handoverQr?.let { matrix ->
            HandoverDialog(
                matrix = matrix,
                // Named, because the ticket is ciphertext addressed to
                // that device: "only they can open this" is now the true
                // sentence, where "anyone who photographs this" was.
                targetName = state.handoverTarget
                    ?.displayName(stringResource(R.string.device_unnamed))
                    ?: stringResource(R.string.device_unnamed),
                onClose = onCloseHandover,
                onShare = onShareTicket,
                onHandedOver = onHandedOverJob
            )
        }

        // Reporting lives behind one tap while driving, and the sheet it
        // opens carries the disclosure the protocol insists on.
        if (reporting && state.pulseMesh != null) {
            IncidentReportSheet(
                status = state.pulseMesh,
                bottomInset = insets.calculateBottomPadding(),
                onReport = {
                    reporting = false
                    onReportIncident(it)
                },
                onClose = { reporting = false }
            )
        }

        if (state.loading) {
            Box(
                Modifier
                    .fillMaxSize()
                    .background(MaterialTheme.colorScheme.background),
                contentAlignment = Alignment.Center
            ) {
                Column(horizontalAlignment = Alignment.CenterHorizontally) {
                    Image(
                        painter = painterResource(R.drawable.ic_wayfind_mark),
                        contentDescription = null,
                        modifier = Modifier.size(84.dp)
                    )
                    Spacer(Modifier.height(22.dp))
                    Text(
                        stringResource(R.string.app_name),
                        style = MaterialTheme.typography.displaySmall
                    )
                    Spacer(Modifier.height(6.dp))
                    Text(
                        stringResource(R.string.app_tagline),
                        style = MaterialTheme.typography.labelLarge,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    Spacer(Modifier.height(30.dp))
                    LinearProgressIndicator(
                        color = MaterialTheme.colorScheme.primary,
                        trackColor = MaterialTheme.colorScheme.surfaceVariant,
                        modifier = Modifier.width(132.dp).height(3.dp)
                    )
                }
            }
        }

        state.fatalError?.let { message ->
            Box(
                Modifier
                    .fillMaxSize()
                    .background(MaterialTheme.colorScheme.background),
                contentAlignment = Alignment.Center
            ) {
                Column(
                    horizontalAlignment = Alignment.CenterHorizontally,
                    modifier = Modifier.padding(32.dp)
                ) {
                    Text(
                        stringResource(R.string.startup_failed_title),
                        style = MaterialTheme.typography.headlineSmall
                    )
                    Spacer(Modifier.height(8.dp))
                    Text(
                        message,
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
        }
    }
}

