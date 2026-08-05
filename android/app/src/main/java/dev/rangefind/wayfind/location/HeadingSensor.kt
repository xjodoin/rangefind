package dev.rangefind.wayfind.location

import android.content.Context
import android.hardware.Sensor
import android.hardware.SensorEvent
import android.hardware.SensorEventListener
import android.hardware.SensorManager
import android.view.Surface
import android.view.WindowManager

/**
 * Which way the device is pointing, from the fused rotation vector.
 *
 * A satellite fix says which way you are *moving*, which is not the same
 * thing and is not available at all when you are stopped. The rotation vector
 * is the platform's own fusion of gyroscope, accelerometer and magnetometer,
 * so it answers while stationary and settles far faster than a bare compass.
 *
 * It is only ever a fallback here. Once a vehicle is moving, where it is
 * going beats where the phone happens to be held — a handset on a passenger
 * seat, or turned to read, says nothing about the car.
 */
class HeadingSensor(context: Context) : SensorEventListener {

    private val manager =
        context.applicationContext.getSystemService(Context.SENSOR_SERVICE) as? SensorManager
    private val windows =
        context.applicationContext.getSystemService(Context.WINDOW_SERVICE) as? WindowManager
    private val sensor: Sensor? =
        manager?.getDefaultSensor(Sensor.TYPE_ROTATION_VECTOR)

    private val rotation = FloatArray(9)
    private val adjusted = FloatArray(9)
    private val orientation = FloatArray(3)

    @Volatile
    private var azimuth: Double? = null

    /** Degrees clockwise from true north, or null until the sensor reports. */
    val bearing: Double? get() = azimuth

    fun start() {
        val sensor = sensor ?: return
        runCatching {
            manager?.registerListener(this, sensor, SensorManager.SENSOR_DELAY_GAME)
        }
    }

    fun stop() {
        runCatching { manager?.unregisterListener(this) }
        azimuth = null
    }

    override fun onSensorChanged(event: SensorEvent?) {
        val values = event?.values ?: return
        if (event.sensor?.type != Sensor.TYPE_ROTATION_VECTOR) return
        runCatching {
            SensorManager.getRotationMatrixFromVector(rotation, values)
            // The sensor frame is fixed to the device, not the screen, so a
            // phone in landscape reports a heading ninety degrees out unless
            // the display's own rotation is taken back out of it.
            val (x, y) = when (windows?.defaultDisplay?.rotation) {
                Surface.ROTATION_90 -> SensorManager.AXIS_Y to SensorManager.AXIS_MINUS_X
                Surface.ROTATION_180 -> SensorManager.AXIS_MINUS_X to SensorManager.AXIS_MINUS_Y
                Surface.ROTATION_270 -> SensorManager.AXIS_MINUS_Y to SensorManager.AXIS_X
                else -> SensorManager.AXIS_X to SensorManager.AXIS_Y
            }
            SensorManager.remapCoordinateSystem(rotation, x, y, adjusted)
            SensorManager.getOrientation(adjusted, orientation)
            azimuth = ((Math.toDegrees(orientation[0].toDouble()) % 360.0) + 360.0) % 360.0
        }
    }

    override fun onAccuracyChanged(sensor: Sensor?, accuracy: Int) = Unit
}
