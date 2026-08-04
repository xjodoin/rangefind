package dev.rangefind.maps.ui

import androidx.activity.compose.BackHandler
import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInVertically
import androidx.compose.animation.slideOutVertically
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
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawing
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.MyLocation
import androidx.compose.material3.CircularProgressIndicator
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
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.dp
import dev.rangefind.maps.engine.LatLon
import dev.rangefind.maps.engine.Suggestion
import dev.rangefind.maps.ui.components.AttributionChip
import dev.rangefind.maps.ui.components.DirectionsSheet
import dev.rangefind.maps.ui.components.NavigationOverlay
import dev.rangefind.maps.ui.components.PlaceSheet
import dev.rangefind.maps.ui.components.ResultsSheet
import dev.rangefind.maps.ui.components.SearchField
import dev.rangefind.maps.ui.components.SuggestionList
import dev.rangefind.maps.ui.map.MapCanvas
import dev.rangefind.maps.ui.theme.LocalMapPalette

@Composable
fun MapScreen(
    state: UiState,
    darkTheme: Boolean,
    onQueryChange: (String) -> Unit,
    onSubmit: () -> Unit,
    onClear: () -> Unit,
    onSuggestion: (Suggestion) -> Unit,
    onSelectResult: (Int) -> Unit,
    onDismissPlace: () -> Unit,
    onDirections: () -> Unit,
    onSelectRoute: (Int) -> Unit,
    onStartNavigation: () -> Unit,
    onStopNavigation: () -> Unit,
    onExitDirections: () -> Unit,
    onRecenter: () -> Unit,
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

    val navigating = state.sheet == SheetMode.Navigating

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
            bottomInsetPx = sheetHeightPx,
            onCenterChanged = onCenterChanged,
            onResultTapped = onSelectResult,
            onLongPress = onLongPress,
            modifier = Modifier.fillMaxSize()
        )

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
                        onPick = onSuggestion,
                        modifier = Modifier.padding(top = 8.dp)
                    )
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
                .padding(end = 16.dp, bottom = sheetHeight + 16.dp)
        ) {
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
                        contentDescription = "Recenter",
                        tint = if (state.userLocation != null) palette.puck
                        else MaterialTheme.colorScheme.onSurfaceVariant
                    )
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
                    .padding(start = 14.dp, bottom = sheetHeight + 10.dp)
            )
        }

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
                    onSelect = onSelectResult
                )

                SheetMode.Place -> PlaceSheet(
                    state = state,
                    bottomInset = insets.calculateBottomPadding(),
                    onDismiss = onDismissPlace,
                    onDirections = onDirections
                )

                SheetMode.Directions -> DirectionsSheet(
                    state = state,
                    bottomInset = insets.calculateBottomPadding(),
                    onSelectRoute = onSelectRoute,
                    onStart = onStartNavigation,
                    onClose = onExitDirections
                )

                SheetMode.Navigating -> Unit
            }
        }

        if (navigating) {
            NavigationOverlay(
                state = state,
                topInset = insets.calculateTopPadding(),
                bottomInset = insets.calculateBottomPadding(),
                onStop = onStopNavigation
            )
        }

        if (state.loading) {
            Box(
                Modifier
                    .fillMaxSize()
                    .background(MaterialTheme.colorScheme.background.copy(alpha = 0.92f)),
                contentAlignment = Alignment.Center
            ) {
                Column(horizontalAlignment = Alignment.CenterHorizontally) {
                    CircularProgressIndicator(strokeWidth = 3.dp)
                    Spacer(Modifier.height(18.dp))
                    Text("Opening the static index", style = MaterialTheme.typography.titleMedium)
                    Spacer(Modifier.height(4.dp))
                    Text(
                        "Byte ranges only — no search server",
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
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
                    Text("Couldn't start", style = MaterialTheme.typography.headlineSmall)
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

