package dev.rangefind.wayfind.nav

import android.app.Service
import android.content.Intent
import android.os.IBinder

/** Keeps the process alive and location flowing while navigating. */
class NavigationService : Service() {
    override fun onBind(intent: Intent?): IBinder? = null
}
