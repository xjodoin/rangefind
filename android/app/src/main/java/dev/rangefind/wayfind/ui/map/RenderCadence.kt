package dev.rangefind.wayfind.ui.map

import org.json.JSONObject

/**
 * How often the map actually put a frame on screen during a drive.
 *
 * "The arrow is choppy" is not a diagnosis, and on a phone in a car it cannot
 * be turned into one by looking harder. There are two very different faults
 * behind it: the map is presenting frames late, or the map is presenting
 * frames perfectly well and the position it is drawing only changes once a
 * second. The fix for the first is in the renderer; the fix for the second is
 * in the motion model. Guessing between them is how the last two attempts at
 * this went.
 *
 * So a drive now measures itself. Every presented frame is timed, the gaps
 * between them are kept as a histogram, and the number of vehicle updates
 * pushed over the same window is counted alongside. A trace comes back saying
 * "3,400 frames in 62 s, p95 gap 18 ms, 61 updates" — smooth renderer, coarse
 * input — or "900 frames, p95 gap 140 ms" — the renderer is the problem. Both
 * are actionable; neither needs the driver to describe what they saw.
 *
 * A histogram rather than every timestamp: a twenty-minute drive is ~72,000
 * frames, and the shape of the distribution is the whole question. Counts are
 * cheap enough to take on the frame callback itself.
 *
 * Only moving time is counted. MapLibre draws on demand, so a map that is not
 * changing correctly presents nothing, and a car stopped at a light produces
 * second-long gaps that are the renderer behaving exactly as it should.
 * Counting those would make every drive through a town look like a drive that
 * stuttered — a measurement that misleads is worse than none.
 */
object RenderCadence {

    /** One bucket per millisecond, then everything slower lands in the last. */
    private const val BUCKETS = 250

    private val lock = Any()
    private val gaps = IntArray(BUCKETS)
    private var frames = 0L
    private var updates = 0L
    private var lastFrameNs = 0L
    private var worstGapMs = 0.0
    private var startedNs = 0L
    private var running = false
    private var moving = false
    private var movingNs = 0L
    private var movingSinceNs = 0L

    /** Begins a window. Called when the drive starts. */
    fun start(nowNs: Long) {
        synchronized(lock) {
            gaps.fill(0)
            frames = 0
            updates = 0
            lastFrameNs = 0
            worstGapMs = 0.0
            startedNs = nowNs
            running = true
            moving = false
            movingNs = 0L
            movingSinceNs = 0L
        }
    }

    fun stop() {
        synchronized(lock) {
            settle(System.nanoTime())
            running = false
        }
    }

    /**
     * Whether the vehicle is under way. Stopping closes the measured window;
     * starting again opens a new one, and the gap across the stop is dropped
     * rather than charged to the renderer.
     */
    private fun setMoving(nowNs: Long, value: Boolean) {
        if (value == moving) return
        settle(nowNs)
        moving = value
        if (value) {
            movingSinceNs = nowNs
            lastFrameNs = 0L
        }
    }

    /** Banks the moving stretch that just ended. */
    private fun settle(nowNs: Long) {
        if (moving && movingSinceNs != 0L) movingNs += nowNs - movingSinceNs
        movingSinceNs = 0L
    }

    /**
     * A frame reached the screen. The first one only establishes a baseline —
     * the gap before it spans whatever the map was doing beforehand, which is
     * not a drive.
     */
    fun onFrame(nowNs: Long) {
        synchronized(lock) {
            if (!running || !moving) return
            frames++
            if (lastFrameNs != 0L) {
                val gapMs = (nowNs - lastFrameNs) / 1_000_000.0
                if (gapMs > worstGapMs) worstGapMs = gapMs
                val bucket = gapMs.toInt().coerceIn(0, BUCKETS - 1)
                gaps[bucket]++
            }
            lastFrameNs = nowNs
        }
    }

    /**
     * A position was handed to the map. The denominator for the frame count,
     * and where the vehicle's speed decides whether this stretch is measured.
     */
    fun onVehicleUpdate(speedMps: Double, nowNs: Long) {
        synchronized(lock) {
            if (!running) return
            setMoving(nowNs, speedMps >= MOVING_MPS)
            if (moving) updates++
        }
    }

    /**
     * The window so far, or null if nothing was drawn. Percentiles come from
     * the histogram, so they are accurate to the millisecond, which is finer
     * than any distinction that matters here.
     */
    fun snapshot(nowNs: Long): JSONObject? = synchronized(lock) {
        if (frames < 2) return null
        val counted = gaps.sum()
        if (counted == 0) return null
        val elapsed = (nowNs - startedNs) / 1_000_000_000.0
        val banked = movingNs + if (moving && movingSinceNs != 0L) nowNs - movingSinceNs else 0L
        val seconds = banked / 1_000_000_000.0
        if (seconds <= 0) return null

        fun percentile(fraction: Double): Int {
            val target = (counted * fraction).toInt().coerceAtLeast(1)
            var seen = 0
            gaps.forEachIndexed { ms, count ->
                seen += count
                if (seen >= target) return ms
            }
            return BUCKETS - 1
        }

        // A 60 Hz frame is 16.7 ms. Past 33 ms a frame was missed, past 66 ms
        // three were, which is where a moving map stops reading as motion.
        val over33 = (33 until BUCKETS).sumOf { gaps[it] }
        val over66 = (66 until BUCKETS).sumOf { gaps[it] }

        JSONObject()
            .put("frames", frames)
            .put("movingSeconds", String.format("%.1f", seconds).toDouble())
            .put("driveSeconds", String.format("%.1f", elapsed).toDouble())
            .put("fps", if (seconds > 0) String.format("%.1f", frames / seconds).toDouble() else 0.0)
            .put("vehicleUpdates", updates)
            .put("gapP50Ms", percentile(0.50))
            .put("gapP95Ms", percentile(0.95))
            .put("gapP99Ms", percentile(0.99))
            .put("gapWorstMs", worstGapMs.toInt())
            .put("framesOver33Ms", over33)
            .put("framesOver66Ms", over66)
            .put("droppedPercent", String.format("%.1f", 100.0 * over33 / counted).toDouble())
    }

    /** Below this the map is not really moving, so neither is the measurement. */
    private const val MOVING_MPS = 2.0
}
