package dev.rangefind.wayfind.car

import android.content.Intent
import android.content.pm.ApplicationInfo
import androidx.car.app.CarAppService
import androidx.car.app.Screen
import androidx.car.app.Session
import androidx.car.app.validation.HostValidator
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.LifecycleOwner

/** Entry point the car host binds to. */
class WayfindCarAppService : CarAppService() {

    override fun createHostValidator(): HostValidator =
        if ((applicationInfo.flags and ApplicationInfo.FLAG_DEBUGGABLE) != 0) {
            // Debug builds accept the desktop head unit and any dev host.
            HostValidator.ALLOW_ALL_HOSTS_VALIDATOR
        } else {
            HostValidator.Builder(applicationContext)
                .addAllowedHosts(androidx.car.app.R.array.hosts_allowlist_sample)
                .build()
        }

    override fun onCreateSession(): Session = WayfindSession()
}

class WayfindSession : Session() {

    private lateinit var navigator: CarNavigator

    override fun onCreateScreen(intent: Intent): Screen {
        navigator = CarNavigator(carContext).also { it.start() }
        lifecycle.addObserver(object : DefaultLifecycleObserver {
            override fun onDestroy(owner: LifecycleOwner) {
                navigator.stop()
            }
        })
        return DriveScreen(carContext, navigator)
    }
}
