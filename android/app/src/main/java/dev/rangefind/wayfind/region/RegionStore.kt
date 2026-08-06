package dev.rangefind.wayfind.region

import android.content.Context
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.File
import java.net.HttpURLConnection
import java.net.URL
import kotlin.coroutines.coroutineContext

/**
 * Offline route indexes on local storage.
 *
 * Chromium's HTTP cache would keep some of an index around, but it offers no
 * guarantee about what survives, and Range responses are the first thing it
 * declines to store. A region the user asked to keep has to be bytes the app
 * owns — downloaded deliberately, served back over a loopback socket by
 * [RegionServer], and deleted when they say so.
 */
class RegionStore(context: Context) {

    private val root = File(context.filesDir, "regions")

    fun directoryOf(id: String) = File(root, id)

    fun isReady(id: String) = File(directoryOf(id), "manifest.json").isFile

    fun bytesOf(id: String): Long =
        directoryOf(id).walkTopDown().filter { it.isFile }.sumOf { it.length() }

    fun updatedAt(id: String): Long = File(directoryOf(id), "manifest.json").lastModified()

    /**
     * The index a stored region actually holds.
     *
     * Packs are content-addressed, so the root's filename *is* the version:
     * rebuild the index from corrected data and the name changes. Until this
     * existed, [isReady] answering "the manifest file is present" was the whole
     * of the freshness check, and a region downloaded once was used forever.
     * A fix to the routing costs shipped to the server and simply never
     * reached a phone that had the region kept offline — silently, since
     * nothing compared the two.
     */
    fun storedRoot(id: String): String? {
        val manifest = File(directoryOf(id), "manifest.json")
        if (!manifest.isFile) return null
        return runCatching { rootOf(manifest.readText()) }.getOrNull()
    }

    /**
     * The index the server is publishing now, or null when it cannot be asked.
     *
     * Null means unknown, never stale: a phone out of signal must not be told
     * its offline region is out of date, which is the one moment that region
     * is the only thing it has.
     */
    suspend fun publishedRoot(baseUrl: String): String? = withContext(Dispatchers.IO) {
        runCatching {
            val url = URL(URL(baseUrl), "manifest.json")
            val connection = (url.openConnection() as HttpURLConnection).apply {
                connectTimeout = 8_000
                readTimeout = 8_000
                requestMethod = "GET"
            }
            try {
                if (connection.responseCode !in 200..299) return@runCatching null
                rootOf(connection.inputStream.bufferedReader().readText())
            } finally {
                connection.disconnect()
            }
        }.getOrNull()
    }

    fun delete(id: String) {
        directoryOf(id).deleteRecursively()
        // A download that failed or was cancelled leaves its staging directory
        // behind, holding up to the full size of the region. Nothing reports
        // those bytes — [bytesOf] only walks the installed directory — so they
        // would sit there as storage the user cannot see or reclaim.
        stagingOf(id).deleteRecursively()
    }

    private fun stagingOf(id: String) = File(root, "$id.staging")

    /**
     * Downloads [files] into a staging directory and swaps it in only once
     * every file has landed. A half-downloaded index that answered queries
     * would fail deep inside a route with a checksum error instead of simply
     * being absent.
     */
    suspend fun install(
        id: String,
        baseUrl: String,
        files: List<String>,
        onProgress: (done: Int, total: Int, bytes: Long) -> Unit
    ) = withContext(Dispatchers.IO) {
        val staging = stagingOf(id)
        staging.deleteRecursively()
        staging.mkdirs()

        try {
            var bytes = 0L
            files.forEachIndexed { index, relative ->
                coroutineContext.ensureActive()
                val target = File(staging, relative)
                target.parentFile?.mkdirs()
                bytes += downloadWithRetry(URL(URL(baseUrl), relative), target)
                onProgress(index + 1, files.size, bytes)
            }
        } catch (failure: Throwable) {
            staging.deleteRecursively()
            throw failure
        }

        val destination = directoryOf(id)
        destination.deleteRecursively()
        staging.renameTo(destination)
    }

    /**
     * A region is tens of megabytes spread over dozens of files, and any one
     * of them failing throws the whole download away. Over Wi-Fi a single
     * dropped connection is ordinary, so give each file a few attempts before
     * giving up on the region.
     */
    private suspend fun downloadWithRetry(url: URL, target: File): Long {
        var attempt = 0
        while (true) {
            coroutineContext.ensureActive()
            try {
                return download(url, target)
            } catch (failure: java.io.IOException) {
                if (++attempt >= DOWNLOAD_ATTEMPTS) throw failure
                kotlinx.coroutines.delay(RETRY_BACKOFF_MS * attempt)
            }
        }
    }

    private fun download(url: URL, target: File): Long {
        val connection = (url.openConnection() as HttpURLConnection).apply {
            connectTimeout = 15_000
            readTimeout = 60_000
            requestMethod = "GET"
        }
        try {
            if (connection.responseCode !in 200..299) {
                throw IllegalStateException("${url.path.substringAfterLast('/')}: HTTP ${connection.responseCode}")
            }
            target.outputStream().use { out -> connection.inputStream.use { it.copyTo(out) } }
            return target.length()
        } finally {
            connection.disconnect()
        }
    }

    companion object {
        private const val DOWNLOAD_ATTEMPTS = 3
        private const val RETRY_BACKOFF_MS = 400L

        /** The root pack named by a route-graph manifest, or null if it names none. */
        fun rootOf(manifestJson: String): String? =
            runCatching { JSONObject(manifestJson).optString("root").ifBlank { null } }.getOrNull()
    }
}
