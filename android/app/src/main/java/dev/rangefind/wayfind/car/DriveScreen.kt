package dev.rangefind.wayfind.car

import androidx.car.app.CarContext
import androidx.car.app.Screen
import androidx.car.app.model.Action
import androidx.car.app.model.ActionStrip
import androidx.car.app.model.CarColor
import androidx.car.app.model.CarIcon
import androidx.car.app.model.Distance
import androidx.car.app.model.Template
import androidx.car.app.navigation.NavigationManager
import androidx.car.app.navigation.model.Maneuver
import androidx.car.app.navigation.model.NavigationTemplate
import androidx.car.app.navigation.model.RoutingInfo
import androidx.car.app.navigation.model.Destination
import androidx.car.app.navigation.model.Step
import androidx.car.app.navigation.model.Trip
import androidx.car.app.navigation.model.TravelEstimate
import androidx.car.app.model.DateTimeWithZone
import androidx.core.graphics.drawable.IconCompat
import androidx.lifecycle.lifecycleScope
import dev.rangefind.wayfind.R
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.launch
import java.util.Calendar
import java.util.TimeZone
import kotlin.math.abs
import kotlin.math.roundToInt

/**
 * The car's home surface: the route drawn on the head unit's display, with the
 * host's own maneuver card and travel estimate on top once driving.
 */
class DriveScreen(
    carContext: CarContext,
    private val navigator: CarNavigator
) : Screen(carContext) {

    private val renderer = CarMapRenderer(carContext, carContext.isDarkMode)
    private var navigationActive = false

    init {
        carContext.getCarService(androidx.car.app.AppManager::class.java)
            .setSurfaceCallback(renderer)

        lifecycleScope.launch {
            navigator.state.collectLatest { state ->
                renderer.render(state)
                syncNavigationManager(state)
                invalidate()
            }
        }
    }

    private fun syncNavigationManager(state: CarState) {
        val manager = carContext.getCarService(NavigationManager::class.java)
        if (state.navigating && !navigationActive) {
            navigationActive = true
            runCatching { manager.navigationStarted() }
        } else if (!state.navigating && navigationActive) {
            navigationActive = false
            runCatching { manager.navigationEnded() }
            return
        }

        // The host needs the trip, not just our template: this is what feeds
        // the instrument cluster and the car's own notification surface.
        if (state.navigating && state.route != null) {
            runCatching { manager.updateTrip(trip(state)) }
        }
    }

    private fun trip(state: CarState): Trip {
        val estimate = travelEstimate(state)
        return Trip.Builder()
            .addStep(currentStep(state), DriveScreen.distanceOf(state.metersToManeuver).let {
                TravelEstimate.Builder(it, DateTimeWithZone.create(
                    Calendar.getInstance().apply {
                        add(Calendar.SECOND, state.remainingSeconds.roundToInt())
                    }.timeInMillis,
                    TimeZone.getDefault()
                )).setRemainingTimeSeconds(state.remainingSeconds.toLong().coerceAtLeast(0)).build()
            })
            .addDestination(
                Destination.Builder()
                    .setName(state.destination?.name ?: "Destination")
                    .build(),
                estimate
            )
            .apply { if (state.stepName.isNotBlank()) setCurrentRoad(state.stepName) }
            .build()
    }

    private fun currentStep(state: CarState): Step = Step.Builder()
        .setCue(state.nextStepName.ifBlank { state.stepName }.ifBlank { "Continue" })
        .setManeuver(Maneuver.Builder(maneuverType(state.turnDelta)).build())
        .build()

    override fun onGetTemplate(): Template {
        val state = navigator.state.value
        val builder = NavigationTemplate.Builder()
            .setBackgroundColor(CarColor.PRIMARY)
            .setActionStrip(actionStrip(state))

        if (state.navigating && state.route != null) {
            builder.setNavigationInfo(routingInfo(state))
            builder.setDestinationTravelEstimate(travelEstimate(state))
        }
        return builder.build()
    }

    private fun actionStrip(state: CarState): ActionStrip {
        val strip = ActionStrip.Builder()
        strip.addAction(
            Action.Builder()
                .setTitle(if (state.navigating) "End" else "Search")
                .setIcon(
                    CarIcon.Builder(
                        IconCompat.createWithResource(carContext, R.drawable.ic_car_action)
                    ).build()
                )
                .setOnClickListener {
                    if (state.navigating) {
                        navigator.stopNavigation()
                    } else {
                        screenManager.push(SearchCarScreen(carContext, navigator))
                    }
                }
                .build()
        )
        return strip.build()
    }

    private fun routingInfo(state: CarState): RoutingInfo = RoutingInfo.Builder()
        .setCurrentStep(currentStep(state), distanceOf(state.metersToManeuver))
        .apply { if (state.rerouting) setLoading(true) }
        .build()

    private fun travelEstimate(state: CarState): TravelEstimate {
        val arrival = Calendar.getInstance().apply {
            add(Calendar.SECOND, state.remainingSeconds.roundToInt())
        }
        return TravelEstimate.Builder(
            distanceOf(state.remainingMeters),
            DateTimeWithZone.create(arrival.timeInMillis, TimeZone.getDefault())
        )
            .setRemainingTimeSeconds(state.remainingSeconds.toLong().coerceAtLeast(0))
            .setRemainingTimeColor(CarColor.GREEN)
            .build()
    }

    private fun maneuverType(delta: Double) = when {
        abs(delta) > 150 -> Maneuver.TYPE_U_TURN_LEFT
        delta <= -60 -> Maneuver.TYPE_TURN_NORMAL_LEFT
        delta <= -22 -> Maneuver.TYPE_TURN_SLIGHT_LEFT
        delta >= 60 -> Maneuver.TYPE_TURN_NORMAL_RIGHT
        delta >= 22 -> Maneuver.TYPE_TURN_SLIGHT_RIGHT
        else -> Maneuver.TYPE_STRAIGHT
    }

    companion object {
        /** Car UI wants distances rounded the way a driver hears them. */
        fun distanceOf(meters: Double): Distance = when {
            meters < 1000 -> Distance.create(
                (meters / 10).roundToInt() * 10.0,
                Distance.UNIT_METERS
            )
            else -> Distance.create(
                (meters / 100).roundToInt() / 10.0,
                Distance.UNIT_KILOMETERS
            )
        }
    }
}
