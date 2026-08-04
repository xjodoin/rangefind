package dev.rangefind.wayfind.car

import androidx.car.app.CarContext
import androidx.car.app.Screen
import androidx.car.app.model.Action
import androidx.car.app.model.ItemList
import androidx.car.app.model.Row
import androidx.car.app.model.SearchTemplate
import androidx.car.app.model.Template
import androidx.lifecycle.lifecycleScope
import dev.rangefind.wayfind.R
import dev.rangefind.wayfind.ui.formatDistance
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.launch

/** Voice- or keyboard-driven destination search on the head unit. */
class SearchCarScreen(
    carContext: CarContext,
    private val navigator: CarNavigator
) : Screen(carContext) {

    init {
        lifecycleScope.launch {
            navigator.state.collectLatest { invalidate() }
        }
    }

    override fun onGetTemplate(): Template {
        val state = navigator.state.value
        val builder = SearchTemplate.Builder(object : SearchTemplate.SearchCallback {
            override fun onSearchSubmitted(searchText: String) = navigator.search(searchText)
            override fun onSearchTextChanged(searchText: String) = Unit
        })
            .setHeaderAction(Action.BACK)
            .setSearchHint(carContext.getString(R.string.car_search_hint))
            .setShowKeyboardByDefault(false)

        when {
            state.searching -> builder.setLoading(true)
            state.results.isNotEmpty() -> builder.setItemList(results(state))
            state.error != null -> builder.setItemList(
                ItemList.Builder().setNoItemsMessage(state.error).build()
            )
        }
        return builder.build()
    }

    private fun results(state: CarState): ItemList {
        val list = ItemList.Builder()
        // Car UIs cap list length; the driver only needs the nearest handful.
        state.results.take(6).forEach { place ->
            val detail = listOfNotNull(
                place.distanceMeters?.let { formatDistance(carContext, it) },
                place.address.takeIf { it.isNotBlank() } ?: place.locality.takeIf { it.isNotBlank() }
            ).joinToString(" · ")
            list.addItem(
                Row.Builder()
                    .setTitle(place.name)
                    .apply { if (detail.isNotBlank()) addText(detail) }
                    .setOnClickListener {
                        navigator.chooseDestination(place) {
                            screenManager.push(RoutePreviewCarScreen(carContext, navigator))
                        }
                    }
                    .build()
            )
        }
        return list.build()
    }
}
