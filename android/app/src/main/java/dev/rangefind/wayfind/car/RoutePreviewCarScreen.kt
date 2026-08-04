package dev.rangefind.wayfind.car

import androidx.car.app.CarContext
import androidx.car.app.Screen
import androidx.car.app.model.Action
import androidx.car.app.model.ItemList
import androidx.car.app.model.Row
import androidx.car.app.model.Template
import androidx.car.app.navigation.model.RoutePreviewNavigationTemplate
import androidx.lifecycle.lifecycleScope
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.ui.formatDistance
import dev.rangefind.wayfind.ui.formatDuration
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.launch

/** Route options for the chosen destination, before committing to drive. */
class RoutePreviewCarScreen(
    carContext: CarContext,
    private val navigator: CarNavigator
) : Screen(carContext) {

    private var selected = 0

    init {
        lifecycleScope.launch {
            navigator.state.collectLatest { invalidate() }
        }
    }

    override fun onGetTemplate(): Template {
        val state = navigator.state.value
        val builder = RoutePreviewNavigationTemplate.Builder()
            .setTitle(state.destination?.name ?: carContext.getString(R.string.car_route_title))
            .setHeaderAction(Action.BACK)

        if (state.routing || state.routes == null) {
            return builder.setLoading(true).build()
        }

        val candidates = listOf(state.routes.primary) + state.routes.alternatives
        val list = ItemList.Builder()
            .setOnSelectedListener { index -> selected = index }
        candidates.take(3).forEachIndexed { index, route ->
            list.addItem(
                Row.Builder()
                    .setTitle(formatDuration(carContext, route.seconds))
                    .addText(
                        carContext.getString(
                            R.string.car_route_detail,
                            formatDistance(carContext, route.distanceMeters),
                            carContext.getString(
                                if (index == 0) R.string.car_route_fastest
                                else R.string.car_route_alternative
                            )
                        )
                    )
                    .build()
            )
        }

        return builder
            .setItemList(list.build())
            .setNavigateAction(
                Action.Builder()
                    .setTitle(carContext.getString(R.string.car_go))
                    .setOnClickListener {
                        navigator.startNavigation()
                        screenManager.popToRoot()
                    }
                    .build()
            )
            .build()
    }
}
