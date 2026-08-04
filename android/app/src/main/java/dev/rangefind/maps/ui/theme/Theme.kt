package dev.rangefind.maps.ui.theme

import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Shapes
import androidx.compose.material3.Typography
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp

// A calm teal/graphite identity rather than stock Material purple: the map is
// the loudest surface in the app, so the chrome stays quiet and lets route and
// selection colors carry meaning.
private val LightColors = lightColorScheme(
    primary = Color(0xFF0E6E76),
    onPrimary = Color.White,
    primaryContainer = Color(0xFFB6EFE9),
    onPrimaryContainer = Color(0xFF00201F),
    secondary = Color(0xFFC4643A),
    onSecondary = Color.White,
    secondaryContainer = Color(0xFFFFDBCB),
    onSecondaryContainer = Color(0xFF3A1600),
    background = Color(0xFFF7F5F1),
    onBackground = Color(0xFF16191C),
    surface = Color(0xFFFFFFFF),
    onSurface = Color(0xFF16191C),
    surfaceVariant = Color(0xFFEDEAE4),
    onSurfaceVariant = Color(0xFF4A4F55),
    outline = Color(0xFFBFBBB3),
    outlineVariant = Color(0xFFDBD7D0),
    error = Color(0xFFB3261E),
    onError = Color.White
)

private val DarkColors = darkColorScheme(
    primary = Color(0xFF5BE3D3),
    onPrimary = Color(0xFF00332F),
    primaryContainer = Color(0xFF00504B),
    onPrimaryContainer = Color(0xFFB6EFE9),
    secondary = Color(0xFFFFB693),
    onSecondary = Color(0xFF5A1B00),
    secondaryContainer = Color(0xFF7A2E0B),
    onSecondaryContainer = Color(0xFFFFDBCB),
    background = Color(0xFF0D1116),
    onBackground = Color(0xFFE7ECF0),
    surface = Color(0xFF141A20),
    onSurface = Color(0xFFE7ECF0),
    surfaceVariant = Color(0xFF1E262E),
    onSurfaceVariant = Color(0xFFB4BEC7),
    outline = Color(0xFF3E4852),
    outlineVariant = Color(0xFF2A333B),
    error = Color(0xFFFFB4AB),
    onError = Color(0xFF690005)
)

/** Colors the map draws with, which Material's roles don't cover. */
data class MapPalette(
    val routeLine: Color,
    val routeCasing: Color,
    val routeTraveled: Color,
    val routeAlternate: Color,
    val routeAlternateCasing: Color,
    val puck: Color,
    val puckHalo: Color,
    val destination: Color,
    val scrim: Color
)

private val LightMap = MapPalette(
    routeLine = Color(0xFF13A5A0),
    routeCasing = Color(0xFF0A5A57),
    routeTraveled = Color(0xFFA9BDBC),
    routeAlternate = Color(0xFF97A2AD),
    routeAlternateCasing = Color(0xFF6B7580),
    puck = Color(0xFF1573D6),
    puckHalo = Color(0x331573D6),
    destination = Color(0xFFC4643A),
    scrim = Color(0x14000000)
)

private val DarkMap = MapPalette(
    routeLine = Color(0xFF4FE0CF),
    routeCasing = Color(0xFF0C4F4A),
    routeTraveled = Color(0xFF4A5A5C),
    routeAlternate = Color(0xFF6C7883),
    routeAlternateCasing = Color(0xFF39424C),
    puck = Color(0xFF54A8FF),
    puckHalo = Color(0x3354A8FF),
    destination = Color(0xFFFFB693),
    scrim = Color(0x33000000)
)

val LocalMapPalette = staticCompositionLocalOf { LightMap }

private val AppShapes = Shapes(
    extraSmall = RoundedCornerShape(8.dp),
    small = RoundedCornerShape(12.dp),
    medium = RoundedCornerShape(18.dp),
    large = RoundedCornerShape(26.dp),
    extraLarge = RoundedCornerShape(34.dp)
)

// Slightly tighter tracking and heavier display weights than the Material
// defaults: numerals (ETA, distance) need to read at a glance while driving.
private val AppTypography = Typography(
    displaySmall = TextStyle(fontSize = 34.sp, lineHeight = 40.sp, fontWeight = FontWeight.SemiBold, letterSpacing = (-0.6).sp),
    headlineMedium = TextStyle(fontSize = 27.sp, lineHeight = 34.sp, fontWeight = FontWeight.SemiBold, letterSpacing = (-0.4).sp),
    headlineSmall = TextStyle(fontSize = 23.sp, lineHeight = 30.sp, fontWeight = FontWeight.SemiBold, letterSpacing = (-0.3).sp),
    titleLarge = TextStyle(fontSize = 20.sp, lineHeight = 26.sp, fontWeight = FontWeight.SemiBold, letterSpacing = (-0.2).sp),
    titleMedium = TextStyle(fontSize = 16.sp, lineHeight = 22.sp, fontWeight = FontWeight.SemiBold),
    bodyLarge = TextStyle(fontSize = 16.sp, lineHeight = 23.sp),
    bodyMedium = TextStyle(fontSize = 14.sp, lineHeight = 20.sp),
    labelLarge = TextStyle(fontSize = 14.sp, lineHeight = 18.sp, fontWeight = FontWeight.SemiBold, letterSpacing = 0.1.sp),
    labelMedium = TextStyle(fontSize = 12.sp, lineHeight = 16.sp, fontWeight = FontWeight.Medium, letterSpacing = 0.3.sp),
    labelSmall = TextStyle(fontSize = 11.sp, lineHeight = 14.sp, fontWeight = FontWeight.Medium, letterSpacing = 0.4.sp)
)

@Composable
fun RangefindTheme(
    darkTheme: Boolean = isSystemInDarkTheme(),
    content: @Composable () -> Unit
) {
    CompositionLocalProvider(LocalMapPalette provides if (darkTheme) DarkMap else LightMap) {
        MaterialTheme(
            colorScheme = if (darkTheme) DarkColors else LightColors,
            typography = AppTypography,
            shapes = AppShapes,
            content = content
        )
    }
}
