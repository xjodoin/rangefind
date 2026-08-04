package dev.rangefind.wayfind.ui.theme

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

// Rangefind's own tokens: ink and paper for the field, pine for structure,
// and the amber marker — the color of the highlighted range in the rangefind
// mark — reserved for the thing you are travelling toward.
private val LightColors = lightColorScheme(
    primary = Color(0xFF0E6F63),
    onPrimary = Color(0xFFFAF9F6),
    primaryContainer = Color(0xFFB9EDE4),
    onPrimaryContainer = Color(0xFF00201C),
    secondary = Color(0xFFFFC940),
    onSecondary = Color(0xFF7A5200),
    secondaryContainer = Color(0xFFFFE9AD),
    onSecondaryContainer = Color(0xFF7A5200),
    background = Color(0xFFFAF9F6),
    onBackground = Color(0xFF14161D),
    surface = Color(0xFFFFFFFF),
    onSurface = Color(0xFF14161D),
    surfaceVariant = Color(0xFFEFEDE6),
    onSurfaceVariant = Color(0xFF5B6370),
    outline = Color(0xFFC6C2B8),
    outlineVariant = Color(0xFFE2DED4),
    error = Color(0xFFB3261E),
    onError = Color(0xFFFAF9F6)
)

private val DarkColors = darkColorScheme(
    primary = Color(0xFF35C2AC),
    onPrimary = Color(0xFF00312B),
    primaryContainer = Color(0xFF0E6F63),
    onPrimaryContainer = Color(0xFFB9EDE4),
    secondary = Color(0xFFFFC940),
    onSecondary = Color(0xFF3D2A00),
    secondaryContainer = Color(0xFF5C4200),
    onSecondaryContainer = Color(0xFFFFE9AD),
    background = Color(0xFF14161D),
    onBackground = Color(0xFFECEADA),
    surface = Color(0xFF21242E),
    onSurface = Color(0xFFECEADA),
    surfaceVariant = Color(0xFF2C3140),
    onSurfaceVariant = Color(0xFF9AA1AD),
    outline = Color(0xFF39404F),
    outlineVariant = Color(0xFF2C3140),
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
    routeLine = Color(0xFF0E6F63),
    routeCasing = Color(0xFF06413A),
    routeTraveled = Color(0xFFA8B2AF),
    routeAlternate = Color(0xFF9AA1AD),
    routeAlternateCasing = Color(0xFF5B6370),
    // The puck stays a conventional blue: "you are here" is a functional
    // signal drivers read at a glance, not a place to spend brand equity.
    puck = Color(0xFF1573D6),
    puckHalo = Color(0x331573D6),
    destination = Color(0xFFFFC940),
    scrim = Color(0x14000000)
)

private val DarkMap = MapPalette(
    routeLine = Color(0xFF35C2AC),
    routeCasing = Color(0xFF0B4A43),
    routeTraveled = Color(0xFF48565C),
    routeAlternate = Color(0xFF6C7883),
    routeAlternateCasing = Color(0xFF39404F),
    puck = Color(0xFF54A8FF),
    puckHalo = Color(0x3354A8FF),
    destination = Color(0xFFFFC940),
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
fun WayfindTheme(
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
