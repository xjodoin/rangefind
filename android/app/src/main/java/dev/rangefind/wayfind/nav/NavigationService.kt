package dev.rangefind.wayfind.nav

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat
import dev.rangefind.wayfind.MainActivity
import dev.rangefind.wayfind.R

/**
 * Keeps the drive alive while the phone is doing something else.
 *
 * This class existed as an empty stub, declared in the manifest with
 * `foregroundServiceType="location"` and never started. The consequence is
 * the one a driver notices: from Android 10 on, an app the user is not
 * looking at gets background location, which means a handful of fixes an
 * hour, and from Android 12 on it can be frozen or killed outright to make
 * room. So a phone call — which is exactly "the user is looking at something
 * else" — starved navigation of the only input it has. The drive did not
 * resume when the call ended; it had stopped receiving positions minutes
 * earlier, and if the process had been reclaimed there was no drive left to
 * resume at all.
 *
 * A foreground service is the platform's answer to that, and the notification
 * is the price: the system requires the user to be able to see that an app is
 * holding location while out of sight. Started when guidance starts, stopped
 * when it ends — including on arrival, which was ending the drive without
 * ending anything else.
 */
class NavigationService : Service() {

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val destination = intent?.getStringExtra(EXTRA_DESTINATION).orEmpty()
        startInForeground(destination)
        // The drive is the app's state, not the service's: a restart with a
        // null intent has nothing to guide and should simply not come back.
        return START_NOT_STICKY
    }

    private fun startInForeground(destination: String) {
        ensureChannel(this)
        val notification = buildNotification(this, destination)
        // The typed overload is mandatory from Android 14; passing the type
        // is what tells the platform this service is the reason location
        // keeps flowing, and omitting it is a crash rather than a warning.
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            startForeground(
                NOTIFICATION_ID,
                notification,
                ServiceInfo.FOREGROUND_SERVICE_TYPE_LOCATION
            )
        } else {
            startForeground(NOTIFICATION_ID, notification)
        }
    }

    companion object {
        private const val CHANNEL_ID = "wayfind.navigation"
        private const val NOTIFICATION_ID = 1
        private const val EXTRA_DESTINATION = "destination"

        /**
         * Starts guidance's foreground service.
         *
         * Failures are swallowed on purpose. A service start can be refused —
         * the app was backgrounded in the same instant, the user revoked
         * notifications — and none of that is a reason to abandon a drive
         * that is otherwise working. Navigation degrades to what it was
         * before: fine in the foreground, starved behind a call.
         */
        fun start(context: Context, destination: String) {
            val intent = Intent(context, NavigationService::class.java)
                .putExtra(EXTRA_DESTINATION, destination)
            runCatching { context.startForegroundService(intent) }
        }

        fun stop(context: Context) {
            runCatching { context.stopService(Intent(context, NavigationService::class.java)) }
        }

        private fun ensureChannel(context: Context) {
            val manager = context.getSystemService(NotificationManager::class.java) ?: return
            if (manager.getNotificationChannel(CHANNEL_ID) != null) return
            manager.createNotificationChannel(
                NotificationChannel(
                    CHANNEL_ID,
                    context.getString(R.string.nav_channel_name),
                    // Low: this notification is a disclosure, not an alert.
                    // It must be visible and must never make a sound at a
                    // moment when the driver's attention is on the road.
                    NotificationManager.IMPORTANCE_LOW
                ).apply {
                    description = context.getString(R.string.nav_channel_description)
                    setShowBadge(false)
                }
            )
        }

        private fun buildNotification(context: Context, destination: String): Notification {
            val open = PendingIntent.getActivity(
                context,
                0,
                Intent(context, MainActivity::class.java).apply {
                    flags = Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP
                },
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
            )
            return NotificationCompat.Builder(context, CHANNEL_ID)
                .setSmallIcon(R.drawable.wayfind_nav_notification)
                .setContentTitle(context.getString(R.string.nav_notification_title))
                .setContentText(
                    if (destination.isBlank()) context.getString(R.string.nav_notification_text)
                    else context.getString(R.string.nav_notification_to, destination)
                )
                .setContentIntent(open)
                .setOngoing(true)
                .setSilent(true)
                .setCategory(NotificationCompat.CATEGORY_NAVIGATION)
                .setPriority(NotificationCompat.PRIORITY_LOW)
                .build()
        }
    }
}
