package dev.rangefind.wayfind.ui.components

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.gestures.Orientation
import androidx.compose.foundation.gestures.draggable
import androidx.compose.foundation.gestures.rememberDraggableState
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.itemsIndexed
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.Directions
import androidx.compose.material.icons.filled.LocalGasStation
import androidx.compose.material.icons.filled.LocalHospital
import androidx.compose.material.icons.filled.LocalParking
import androidx.compose.material.icons.filled.LocalPharmacy
import androidx.compose.material.icons.filled.NorthEast
import androidx.compose.material.icons.filled.Place
import androidx.compose.material.icons.filled.Restaurant
import androidx.compose.material.icons.filled.School
import androidx.compose.material.icons.filled.Search
import androidx.compose.material.icons.filled.ShoppingCart
import androidx.compose.material.icons.filled.Hotel
import androidx.compose.material.icons.filled.DirectionsBus
import androidx.compose.material.icons.filled.AccountBalance
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.compositionLocalOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.focus.onFocusChanged
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.LocalConfiguration
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalFocusManager
import androidx.compose.ui.res.pluralStringResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.SpanStyle
import androidx.compose.ui.text.buildAnnotatedString
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.text.withStyle
import androidx.compose.ui.unit.dp
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.engine.EngineInfo
import dev.rangefind.wayfind.engine.Place
import dev.rangefind.wayfind.engine.Suggestion
import dev.rangefind.wayfind.ui.SheetMode
import dev.rangefind.wayfind.ui.UiState
import dev.rangefind.wayfind.ui.formatDistance
import dev.rangefind.wayfind.ui.humanizeType

@Composable
fun SearchField(
    query: String,
    searching: Boolean,
    onQueryChange: (String) -> Unit,
    onSubmit: () -> Unit,
    onClear: () -> Unit,
    onFocusChanged: (Boolean) -> Unit
) {
    val focusManager = LocalFocusManager.current

    Surface(
        shape = RoundedCornerShape(28.dp),
        color = MaterialTheme.colorScheme.surface,
        shadowElevation = 8.dp,
        modifier = Modifier.fillMaxWidth()
    ) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier
                .fillMaxWidth()
                .height(54.dp)
                .padding(horizontal = 16.dp)
        ) {
            Icon(
                Icons.Filled.Search,
                contentDescription = null,
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.size(22.dp)
            )
            Spacer(Modifier.width(12.dp))

            Box(Modifier.weight(1f)) {
                if (query.isEmpty()) {
                    Text(
                        stringResource(R.string.search_hint),
                        style = MaterialTheme.typography.bodyLarge,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                BasicTextField(
                    value = query,
                    onValueChange = onQueryChange,
                    singleLine = true,
                    textStyle = MaterialTheme.typography.bodyLarge.copy(
                        color = MaterialTheme.colorScheme.onSurface
                    ),
                    cursorBrush = androidx.compose.ui.graphics.SolidColor(MaterialTheme.colorScheme.primary),
                    keyboardOptions = KeyboardOptions(imeAction = ImeAction.Search),
                    keyboardActions = KeyboardActions(onSearch = {
                        focusManager.clearFocus()
                        onSubmit()
                    }),
                    modifier = Modifier
                        .fillMaxWidth()
                        .onFocusChanged { onFocusChanged(it.isFocused) }
                )
            }

            if (searching) {
                CircularProgressIndicator(strokeWidth = 2.dp, modifier = Modifier.size(20.dp))
            } else if (query.isNotEmpty()) {
                Icon(
                    Icons.Filled.Close,
                    contentDescription = stringResource(R.string.action_clear),
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier
                        .size(22.dp)
                        .clickable {
                            focusManager.clearFocus()
                            onClear()
                        }
                )
            }
        }
    }
}

@Composable
fun SuggestionList(
    suggestions: List<Suggestion>,
    onPick: (Suggestion) -> Unit,
    modifier: Modifier = Modifier
) {
    Surface(
        shape = RoundedCornerShape(20.dp),
        color = MaterialTheme.colorScheme.surface,
        shadowElevation = 8.dp,
        modifier = modifier.fillMaxWidth()
    ) {
        Column(Modifier.padding(vertical = 6.dp)) {
            suggestions.take(6).forEach { suggestion ->
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    modifier = Modifier
                        .fillMaxWidth()
                        .clickable { onPick(suggestion) }
                        .padding(horizontal = 16.dp, vertical = 11.dp)
                ) {
                    Icon(
                        Icons.Filled.Search,
                        contentDescription = null,
                        tint = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.size(18.dp)
                    )
                    Spacer(Modifier.width(14.dp))
                    Column(Modifier.weight(1f)) {
                        Text(
                            suggestion.mainText.ifBlank { suggestion.text },
                            style = MaterialTheme.typography.bodyLarge,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis
                        )
                        if (suggestion.secondaryText.isNotBlank()) {
                            Text(
                                suggestion.secondaryText,
                                style = MaterialTheme.typography.bodyMedium,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                                maxLines = 1,
                                overflow = TextOverflow.Ellipsis
                            )
                        }
                    }
                    Icon(
                        Icons.Filled.NorthEast,
                        contentDescription = null,
                        tint = MaterialTheme.colorScheme.outline,
                        modifier = Modifier.size(16.dp)
                    )
                }
            }
        }
    }
}

@Composable
fun ResultsSheet(
    state: UiState,
    bottomInset: androidx.compose.ui.unit.Dp,
    onSelect: (Int) -> Unit
) {
    if (state.results.isEmpty() && state.searchError == null) return

    SheetSurface(bottomInset = bottomInset) {
        state.searchError?.let { message ->
            Text(
                message,
                style = MaterialTheme.typography.bodyLarge,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(horizontal = 20.dp, vertical = 10.dp)
            )
        }

        if (state.results.isNotEmpty()) {
            Text(
                pluralStringResource(
                    R.plurals.search_result_count,
                    state.results.size,
                    state.results.size
                ),
                style = MaterialTheme.typography.labelMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(start = 20.dp, bottom = 6.dp)
            )
            LazyColumn(Modifier.heightIn(max = 300.dp)) {
                itemsIndexed(state.results) { index, place ->
                    PlaceRow(place = place, onClick = { onSelect(index) })
                }
            }
        }
    }
}

@Composable
fun PlaceSheet(
    state: UiState,
    bottomInset: androidx.compose.ui.unit.Dp,
    onDismiss: () -> Unit,
    onDirections: () -> Unit
) {
    val place = state.selected ?: return
    val context = LocalContext.current
    val bounds = state.info?.routeBounds
    val outsideCoverage = bounds != null && !bounds.contains(place.point)
    val routingReady = state.info?.routing == true && !outsideCoverage

    SheetSurface(bottomInset = bottomInset) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 20.dp)
        ) {
            Column(Modifier.weight(1f)) {
                Text(
                    place.name,
                    style = MaterialTheme.typography.headlineSmall,
                    maxLines = 2,
                    overflow = TextOverflow.Ellipsis
                )
                Spacer(Modifier.height(6.dp))
                Row(verticalAlignment = Alignment.CenterVertically) {
                    if (place.type.isNotBlank()) {
                        CategoryChip(place.type)
                        Spacer(Modifier.width(8.dp))
                    }
                    place.distanceMeters?.let {
                        Text(
                            formatDistance(context, it),
                            style = MaterialTheme.typography.labelLarge,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
                if (place.address.isNotBlank()) {
                    Spacer(Modifier.height(8.dp))
                    Text(
                        place.address,
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
            Icon(
                Icons.Filled.Close,
                contentDescription = stringResource(R.string.action_close),
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier
                    .size(24.dp)
                    .clickable { onDismiss() }
            )
        }

        Spacer(Modifier.height(16.dp))

        Button(
            onClick = onDirections,
            enabled = routingReady,
            shape = RoundedCornerShape(16.dp),
            colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.primary),
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 20.dp)
                .height(52.dp)
        ) {
            Icon(Icons.Filled.Directions, contentDescription = null, modifier = Modifier.size(20.dp))
            Spacer(Modifier.width(10.dp))
            Text(
                stringResource(
                    when {
                        routingReady -> R.string.place_directions
                        outsideCoverage -> R.string.place_outside_coverage
                        else -> R.string.place_routing_unavailable
                    }
                ),
                style = MaterialTheme.typography.labelLarge
            )
        }

        // Search and the basemap are worldwide; the route graph is not. Say so
        // here rather than letting the user press a button that cannot work.
        // A region built by an older version reports itself with a marker
        // rather than a sentence, so the reason reaches the driver in their
        // own language and says what to do about it.
        val outdatedRegion = stringResource(R.string.region_outdated_note)
        val note = when {
            routingReady -> null
            outsideCoverage -> stringResource(R.string.place_outside_coverage_note)
            state.info?.routingError == "region-outdated" -> outdatedRegion
            else -> state.info?.routingError
        }
        if (note != null) {
            Spacer(Modifier.height(8.dp))
            Text(
                note,
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(horizontal = 20.dp)
            )
        }
    }
}

@Composable
fun PlaceRow(place: Place, onClick: () -> Unit) {
    val context = LocalContext.current
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .fillMaxWidth()
            .clickable { onClick() }
            .padding(horizontal = 20.dp, vertical = 11.dp)
    ) {
        Surface(
            shape = CircleShape,
            color = MaterialTheme.colorScheme.surfaceVariant,
            modifier = Modifier.size(40.dp)
        ) {
            Box(contentAlignment = Alignment.Center) {
                Icon(
                    iconForType(place.type, place.category),
                    contentDescription = null,
                    tint = MaterialTheme.colorScheme.primary,
                    modifier = Modifier.size(20.dp)
                )
            }
        }
        Spacer(Modifier.width(14.dp))
        Column(Modifier.weight(1f)) {
            Text(
                place.name,
                style = MaterialTheme.typography.bodyLarge,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
            val secondary = listOfNotNull(
                place.type.takeIf { it.isNotBlank() }?.let { humanizeType(it) },
                place.address.takeIf { it.isNotBlank() } ?: place.locality.takeIf { it.isNotBlank() }
            ).joinToString(" · ")
            if (secondary.isNotBlank()) {
                Text(
                    secondary,
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
            }
        }
        place.distanceMeters?.let {
            Spacer(Modifier.width(10.dp))
            Text(
                formatDistance(context, it),
                style = MaterialTheme.typography.labelMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

@Composable
fun CategoryChip(type: String) {
    Surface(
        shape = RoundedCornerShape(8.dp),
        color = MaterialTheme.colorScheme.primaryContainer
    ) {
        Text(
            humanizeType(type),
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onPrimaryContainer,
            modifier = Modifier.padding(horizontal = 8.dp, vertical = 4.dp)
        )
    }
}

@Composable
fun AttributionChip(info: EngineInfo?, modifier: Modifier = Modifier) {
    val source = info?.attribution ?: stringResource(R.string.attribution_openstreetmap)
    val carto = stringResource(R.string.attribution_carto)
    Surface(
        shape = RoundedCornerShape(6.dp),
        color = MaterialTheme.colorScheme.surface.copy(alpha = 0.82f),
        modifier = modifier
    ) {
        Text(
            buildAnnotatedString {
                withStyle(SpanStyle(fontWeight = FontWeight.Medium)) {
                    append(source)
                }
                append(carto)
            },
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(horizontal = 7.dp, vertical = 3.dp)
        )
    }
}

/**
 * How much room the sheet is giving its content right now.
 *
 * A sheet that simply grew with its content ran off the bottom of the screen
 * and took the end of itself with it — the last rows of a settings section, the
 * fetch receipt under a route. Content that can be long reads this and caps
 * itself, so what does not fit scrolls instead of disappearing. The value moves
 * when the handle is dragged, which is what makes the drag do anything.
 */
val LocalSheetContentMaxHeight = compositionLocalOf { 320.dp }

/**
 * The band a dragged sheet stays inside, as a fraction of the screen.
 *
 * The floor is not "as small as it will go": a route sheet keeps its mode
 * switch, its ETA and its Start button outside the scrolling part, and dragged
 * below about this the button is what falls off the bottom. The ceiling leaves
 * a strip of map, because a sheet that covers the route is not showing you the
 * route.
 */
private const val SHEET_MIN_FRACTION = 0.38f
private const val SHEET_MAX_FRACTION = 0.90f

/** Shared bottom-sheet chrome: rounded top, elevation, drag affordance. */
@Composable
fun SheetSurface(
    bottomInset: androidx.compose.ui.unit.Dp,
    /**
     * Whether the handle resizes the sheet. Only worth setting on sheets whose
     * content reads [LocalSheetContentMaxHeight] — anywhere else the drag would
     * be a gesture that visibly does nothing.
     */
    resizable: Boolean = false,
    initialFraction: Float = 0.52f,
    content: @Composable androidx.compose.foundation.layout.ColumnScope.() -> Unit
) {
    val screenHeight = LocalConfiguration.current.screenHeightDp.dp
    val density = LocalDensity.current
    // Survives rotation: a sheet the user pulled up should still be up
    // afterwards, and landscape is exactly where they had to pull it.
    var fraction by rememberSaveable { mutableFloatStateOf(initialFraction) }
    Surface(
        shape = RoundedCornerShape(topStart = 26.dp, topEnd = 26.dp),
        color = MaterialTheme.colorScheme.surface,
        shadowElevation = 16.dp,
        modifier = Modifier.fillMaxWidth()
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(top = 10.dp, bottom = bottomInset + 16.dp)
        ) {
            // The pill is four pixels tall and nobody can hit four pixels in a
            // moving car. The grab area is the full width of the sheet and the
            // height of a fingertip; the pill just says where it is.
            Box(
                contentAlignment = Alignment.Center,
                modifier = Modifier
                    .fillMaxWidth()
                    .height(26.dp)
                    .then(
                        if (!resizable) Modifier else Modifier.draggable(
                            orientation = Orientation.Vertical,
                            state = rememberDraggableState { delta ->
                                // Up is negative and means a taller sheet.
                                val step = with(density) { delta.toDp() } / screenHeight
                                fraction = (fraction - step)
                                    .coerceIn(SHEET_MIN_FRACTION, SHEET_MAX_FRACTION)
                            }
                        )
                    )
            ) {
                Box(
                    Modifier
                        .size(width = 36.dp, height = 4.dp)
                        .background(MaterialTheme.colorScheme.outlineVariant, CircleShape)
                )
            }
            CompositionLocalProvider(
                LocalSheetContentMaxHeight provides screenHeight * fraction
            ) {
                content()
            }
        }
    }
}

fun iconForType(type: String, category: String): ImageVector {
    val token = "$type $category".lowercase()
    return when {
        token.contains("restaurant") || token.contains("food") || token.contains("cafe") ||
            token.contains("bar") || token.contains("pub") -> Icons.Filled.Restaurant
        token.contains("pharmacy") -> Icons.Filled.LocalPharmacy
        token.contains("hospital") || token.contains("clinic") || token.contains("doctor") -> Icons.Filled.LocalHospital
        token.contains("fuel") || token.contains("charging") -> Icons.Filled.LocalGasStation
        token.contains("parking") -> Icons.Filled.LocalParking
        token.contains("hotel") || token.contains("hostel") || token.contains("motel") -> Icons.Filled.Hotel
        token.contains("bank") || token.contains("atm") -> Icons.Filled.AccountBalance
        token.contains("school") || token.contains("university") || token.contains("college") -> Icons.Filled.School
        token.contains("supermarket") || token.contains("shop") || token.contains("store") ||
            token.contains("mall") -> Icons.Filled.ShoppingCart
        token.contains("bus") || token.contains("station") || token.contains("tram") ||
            token.contains("subway") -> Icons.Filled.DirectionsBus
        else -> Icons.Filled.Place
    }
}
