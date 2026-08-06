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
import androidx.compose.material.icons.filled.CloudDownload
import androidx.compose.material.icons.filled.Flag
import androidx.compose.material.icons.filled.MyLocation
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
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
import dev.rangefind.wayfind.engine.Suggestion
import dev.rangefind.wayfind.ui.components.AttributionChip
import dev.rangefind.wayfind.ui.components.DirectionsSheet
import dev.rangefind.wayfind.ui.components.NavigationOverlay
import dev.rangefind.wayfind.ui.components.PlaceSheet
import dev.rangefind.wayfind.nav.TravelMode
import dev.rangefind.wayfind.ui.components.RegionsSheet
import dev.rangefind.wayfind.ui.components.ResultsSheet
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
    onShowRegions: (Boolean) -> Unit,
    onRegionHostChange: (String) -> Unit,
    onPreloadRegion: (String) -> Unit,
    onDeleteRegion: (String) -> Unit,
    onActivateRegion: (String?) -> Unit,
    onRecordTripsChange: (Boolean) -> Unit,
    onMarkIssue: () -> Unit,
    onShareTrace: () -> Unit,
    hasTrace: Boolean,
    onLongPress: (LatLon) -> Unit,
    onCenterChanged: (LatLon) -> Unit
) {
    val palette = LocalMapPalette.current
    val insets = WindowInsets.safeDrawing.asPaddingValues()
    val density = LocalDensity.current
    var focused by remember { mutableStateOf(false) }

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

        // Recenter control sits above whichever sheet is showing.
        AnimatedVisibility(
            visible = !navigating,
            enter = fadeIn(),
            exit = fadeOut(),
            modifier = Modifier
                .align(Alignment.BottomEnd)
                .padding(end = 16.dp, bottom = if (wideLayout) 28.dp else sheetHeight + 16.dp)
        ) {
            Column(horizontalAlignment = Alignment.CenterHorizontally) {
                Surface(
                    onClick = { onShowRegions(true) },
                    shape = CircleShape,
                    color = MaterialTheme.colorScheme.surface,
                    shadowElevation = 6.dp,
                    modifier = Modifier.size(48.dp)
                ) {
                    Box(contentAlignment = Alignment.Center) {
                        Icon(
                            Icons.Filled.CloudDownload,
                            contentDescription = stringResource(R.string.region_offline_title),
                            tint = if (state.regions.any { it.active })
                                MaterialTheme.colorScheme.primary
                            else MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
                Spacer(Modifier.height(10.dp))
                Surface(
                    onClick = onRecenter,
                    shape = CircleShape,
                    color = MaterialTheme.colorScheme.surface,
                    shadowElevation = 6.dp,
                    modifier = Modifier.size(48.dp)
                ) {
                    Box(contentAlignment = Alignment.Center) {
                        Icon(
                            Icons.Filled.MyLocation,
                            contentDescription = stringResource(R.string.action_recenter),
                            tint = if (state.userLocation != null) palette.puck
                            else MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
            }
        }

        // ODbL and CARTO both require visible attribution, so it rides just
        // above the sheet rather than being hidden behind it.
        if (!navigating) {
            AttributionChip(
                info = state.info,
                modifier = Modifier
                    .align(Alignment.BottomStart)
                    .padding(
                        start = if (wideLayout) panelWidth + 26.dp else 14.dp,
                        bottom = if (wideLayout) 14.dp else sheetHeight + 10.dp
                    )
            )
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
                onClose = { onShowRegions(false) }
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

