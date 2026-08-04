package dev.rangefind.wayfind.region

import java.io.BufferedOutputStream
import java.io.File
import java.io.RandomAccessFile
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.URLDecoder
import java.util.concurrent.Executors
import kotlin.concurrent.thread

/**
 * A loopback HTTP server for preloaded regions.
 *
 * WebView's `shouldInterceptRequest` cannot answer a Range request honestly:
 * Chromium applies its own range handling to whatever the interceptor returns,
 * so a 206 is rejected outright and a 200 gets sliced twice — once by the
 * browser and again by the runtime — which surfaces as a checksum mismatch
 * deep inside a route. A real socket sidesteps all of it: the runtime issues
 * ordinary HTTP with ordinary Range headers, and gets ordinary 206s back.
 *
 * Bound to 127.0.0.1 on an ephemeral port, so nothing outside the device can
 * reach it.
 */
class RegionServer(private val store: RegionStore) {

    private var socket: ServerSocket? = null
    private val workers = Executors.newCachedThreadPool()

    val port: Int get() = socket?.localPort ?: 0

    fun baseUrlFor(regionId: String) = "http://127.0.0.1:$port/$regionId/"

    @Synchronized
    fun start() {
        if (socket != null) return
        val server = ServerSocket(0, 32, InetAddress.getByName("127.0.0.1"))
        socket = server
        thread(isDaemon = true, name = "region-server") {
            while (!server.isClosed) {
                val client = runCatching { server.accept() }.getOrNull() ?: break
                workers.execute { runCatching { handle(client) } }
            }
        }
    }

    @Synchronized
    fun stop() {
        runCatching { socket?.close() }
        socket = null
        workers.shutdownNow()
    }

    private fun handle(client: Socket) = client.use { connection ->
        val input = connection.getInputStream().bufferedReader()
        val requestLine = input.readLine() ?: return@use
        val parts = requestLine.split(' ')
        if (parts.size < 2) return@use

        var range: String? = null
        while (true) {
            val line = input.readLine() ?: break
            if (line.isEmpty()) break
            val separator = line.indexOf(':')
            if (separator > 0 && line.substring(0, separator).equals("Range", ignoreCase = true)) {
                range = line.substring(separator + 1).trim()
            }
        }

        val output = BufferedOutputStream(connection.getOutputStream())
        val path = URLDecoder.decode(parts[1].substringBefore('?'), "UTF-8").trimStart('/')
        val regionId = path.substringBefore('/')
        val relative = path.substringAfter('/', "")
        val file = resolve(regionId, relative)

        if (parts[0] != "GET" && parts[0] != "HEAD") {
            respondEmpty(output, 405, "Method Not Allowed")
            return@use
        }
        if (file == null) {
            respondEmpty(output, 404, "Not Found")
            return@use
        }

        val size = file.length()
        val span = parseRange(range, size)
        RandomAccessFile(file, "r").use { handle ->
            if (span == null) {
                writeHeaders(output, 200, "OK", size, null, size)
                if (parts[0] == "HEAD") { output.flush(); return@use }
                copy(handle, output, 0, size)
            } else {
                val length = span.last - span.first + 1
                writeHeaders(output, 206, "Partial Content", length, span, size)
                if (parts[0] == "HEAD") { output.flush(); return@use }
                copy(handle, output, span.first, length)
            }
        }
        output.flush()
    }

    private fun resolve(regionId: String, relative: String): File? {
        if (regionId.isEmpty() || relative.isEmpty()) return null
        val directory = store.directoryOf(regionId)
        val file = File(directory, relative)
        if (!file.isFile) return null
        // Never serve outside the region directory, whatever the path contains.
        if (!file.canonicalPath.startsWith(directory.canonicalPath)) return null
        return file
    }

    private fun writeHeaders(
        output: BufferedOutputStream,
        status: Int,
        reason: String,
        length: Long,
        span: LongRange?,
        total: Long
    ) {
        val builder = StringBuilder()
            .append("HTTP/1.1 ").append(status).append(' ').append(reason).append("\r\n")
            .append("Content-Type: application/octet-stream\r\n")
            .append("Content-Length: ").append(length).append("\r\n")
            .append("Accept-Ranges: bytes\r\n")
            .append("Access-Control-Allow-Origin: *\r\n")
            .append("Cache-Control: no-store\r\n")
        if (span != null) {
            builder.append("Content-Range: bytes ")
                .append(span.first).append('-').append(span.last).append('/').append(total).append("\r\n")
        }
        builder.append("Connection: close\r\n\r\n")
        output.write(builder.toString().toByteArray(Charsets.US_ASCII))
    }

    private fun respondEmpty(output: BufferedOutputStream, status: Int, reason: String) {
        writeHeaders(output, status, reason, 0, null, 0)
        output.flush()
    }

    private fun copy(handle: RandomAccessFile, output: BufferedOutputStream, offset: Long, length: Long) {
        handle.seek(offset)
        val buffer = ByteArray(64 * 1024)
        var remaining = length
        while (remaining > 0) {
            val read = handle.read(buffer, 0, minOf(buffer.size.toLong(), remaining).toInt())
            if (read <= 0) break
            output.write(buffer, 0, read)
            remaining -= read
        }
    }

    private fun parseRange(header: String?, size: Long): LongRange? {
        val spec = header?.removePrefix("bytes=")?.trim() ?: return null
        // Multi-range is legal HTTP but the runtime falls back happily to a
        // full body, which is cheap from local disk.
        if (spec.contains(',')) return null
        val parts = spec.split('-', limit = 2)
        if (parts.size != 2) return null
        val start = parts[0].trim()
        val end = parts[1].trim()
        return when {
            start.isEmpty() -> {
                val tail = end.toLongOrNull() ?: return null
                if (tail <= 0) null else LongRange(maxOf(0, size - tail), size - 1)
            }
            else -> {
                val from = start.toLongOrNull() ?: return null
                if (from >= size) return null
                val to = end.toLongOrNull()?.coerceAtMost(size - 1) ?: (size - 1)
                if (to < from) null else LongRange(from, to)
            }
        }
    }
}
