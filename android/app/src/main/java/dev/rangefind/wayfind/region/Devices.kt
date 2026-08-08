package dev.rangefind.wayfind.region

import android.content.SharedPreferences
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import org.json.JSONArray
import org.json.JSONObject
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

/**
 * A device this phone has enrolled, and may therefore hand a job to
 * (threads §20.9).
 *
 * Enrolment is the gate on transfer: a ticket is sealed to a public key,
 * so a driver who is not in this list cannot be handed the day — not
 * because a check refuses it, but because there is no key to address the
 * ciphertext to. That is a real operational cost and it is the whole
 * point; it replaces "whoever holds these bytes" with "one device this
 * phone named".
 *
 * [fingerprint] is stored rather than recomputed because it is what the
 * two people compared aloud when they enrolled, and the row that says
 * "you checked these eight characters" should show the same eight
 * characters afterwards.
 */
data class EnrolledDevice(
    /** X25519 public key, base64url — the address a ticket is sealed to. */
    val publicKey: String,
    val name: String,
    val fingerprint: String,
    val addedAt: Long
) {
    /** What to call this device when it has no name of its own. */
    fun displayName(fallback: String): String = name.ifBlank { fallback }
}

/** §20.9: a PMV1 card carries at most 32 bytes of name. */
const val MAX_DEVICE_NAME_BYTES = 32

/**
 * A device name cut to what a card can carry.
 *
 * Cut on a code-point boundary, never mid-character — the same rule as a
 * driver's note, and for the same reason: "Léa — Pixel 8" truncated
 * through the middle of an em dash reaches the other phone as a
 * replacement glyph, and the name exists precisely so a human recognises
 * it. The card encoder throws above 32 bytes, so capping here is what
 * keeps a long name from taking the whole enrolment with it.
 */
fun cappedDeviceName(value: String?, maxBytes: Int = MAX_DEVICE_NAME_BYTES): String {
    val text = value?.trim().orEmpty()
    if (text.isEmpty()) return ""
    if (text.toByteArray(Charsets.UTF_8).size <= maxBytes) return text
    val kept = StringBuilder()
    var bytes = 0
    var index = 0
    while (index < text.length) {
        val point = text.codePointAt(index)
        val chars = Character.charCount(point)
        val piece = text.substring(index, index + chars)
        val cost = piece.toByteArray(Charsets.UTF_8).size
        if (bytes + cost > maxBytes) break
        kept.append(piece)
        bytes += cost
        index += chars
    }
    return kept.toString().trim()
}

/**
 * The fingerprint as two people actually read it to each other.
 *
 * Eight hex characters in one run is a string nobody can say aloud
 * without losing their place; in two groups of four it is two words. The
 * comparison this supports is the only thing standing between enrolment
 * and a screen in a depot showing somebody else's card — §20.9 is explicit
 * that four bytes is not a cryptographic binding and is not claimed to be.
 *
 * Anything that is not the expected eight hex characters is returned
 * unchanged rather than mangled into a shape it is not: a fingerprint
 * this build does not recognise must still be comparable against the
 * other phone's screen.
 */
fun formatFingerprint(hex: String?): String {
    val text = hex?.trim().orEmpty()
    if (text.length != 8 || !text.all { it.isHexDigit() }) return text
    val upper = text.uppercase()
    return "${upper.substring(0, 4)} ${upper.substring(4)}"
}

private fun Char.isHexDigit(): Boolean =
    this in '0'..'9' || this in 'a'..'f' || this in 'A'..'F'

/**
 * The roster as one preference string.
 *
 * JSON for the same reason the day's plan is JSON: the list is variable
 * length, so a key per device is a schema that cannot be replaced
 * atomically — and a roster half-written is a phone that will seal a job
 * to a driver who has left.
 */
fun enrolledDevicesToJson(devices: List<EnrolledDevice>): String {
    val list = JSONArray()
    for (device in devices) {
        list.put(
            JSONObject()
                .put(KEY_DEVICE_PUBLIC_KEY, device.publicKey)
                .put(KEY_DEVICE_NAME, device.name)
                .put(KEY_DEVICE_FINGERPRINT, device.fingerprint)
                .put(KEY_DEVICE_ADDED_AT, device.addedAt)
        )
    }
    return JSONObject().put(KEY_DEVICES, list).toString()
}

/**
 * The roster back out again.
 *
 * Defensive throughout, like every other stored artifact here: this
 * string was written by an older build as often as by this one, and a
 * driver whose phone cannot parse yesterday's roster should get an empty
 * one — and a plain sentence about enrolling again — rather than a crash
 * on the way to the depot.
 *
 * An entry with no public key is dropped: it is not an address anything
 * can be sealed to, and keeping it would put a row on screen that
 * silently fails at the moment of handover.
 */
fun enrolledDevicesFromJson(text: String?): List<EnrolledDevice> {
    if (text.isNullOrBlank()) return emptyList()
    return runCatching {
        val list = JSONObject(text).optJSONArray(KEY_DEVICES) ?: JSONArray()
        (0 until list.length()).mapNotNull { position ->
            val item = list.optJSONObject(position) ?: return@mapNotNull null
            val key = item.optString(KEY_DEVICE_PUBLIC_KEY).takeIf { it.isNotBlank() }
                ?: return@mapNotNull null
            EnrolledDevice(
                publicKey = key,
                // A device the driver never named is ordinary, and a JSON
                // null must not become the text "null" on the card.
                name = if (item.isNull(KEY_DEVICE_NAME)) "" else item.optString(KEY_DEVICE_NAME),
                fingerprint = if (item.isNull(KEY_DEVICE_FINGERPRINT)) ""
                else item.optString(KEY_DEVICE_FINGERPRINT),
                addedAt = item.optLong(KEY_DEVICE_ADDED_AT)
            )
        }
    }.getOrDefault(emptyList())
}

/**
 * The roster with [device] in it, replacing any earlier entry for the
 * same key.
 *
 * Keyed on the public key rather than on the name: two phones in a depot
 * are routinely both called "Pixel 8", and enrolling the second must not
 * silently overwrite the first — while re-scanning the *same* phone after
 * it was renamed must not leave two rows that seal to one device.
 */
fun List<EnrolledDevice>.withDevice(device: EnrolledDevice): List<EnrolledDevice> =
    filterNot { it.publicKey == device.publicKey } + device

fun List<EnrolledDevice>.withoutDevice(publicKey: String): List<EnrolledDevice> =
    filterNot { it.publicKey == publicKey }

private const val KEY_DEVICES = "devices"
private const val KEY_DEVICE_PUBLIC_KEY = "publicKey"
private const val KEY_DEVICE_NAME = "name"
private const val KEY_DEVICE_FINGERPRINT = "fingerprint"
private const val KEY_DEVICE_ADDED_AT = "addedAt"

/**
 * This device's private key on disk, wrapped by a key the app cannot
 * export.
 *
 * §20.9 asks a host to put this in the Keystore, and it is right to: the
 * key is what opens every job sealed to this phone, and it is the one
 * secret here that is *long-lived* — a run seed dies with its day, this
 * does not. So what SharedPreferences holds is AES-256-GCM ciphertext
 * under a key generated inside `AndroidKeyStore`, which on most handsets
 * lives in hardware and cannot leave it. A backup of the preferences
 * file, or a read of it from another app's exploit, is then ciphertext
 * with no key beside it.
 *
 * Deliberately **not** `androidx.security:security-crypto`: that library
 * is an alpha, is deprecated upstream, and pulls Tink in for what is
 * forty lines of platform API here.
 *
 * The fallback is stated rather than silent. A device whose Keystore
 * refuses (an emulator image without one, a provider failure) stores the
 * key as plain text and says so through [hardwareBacked], because the
 * alternative — refusing to have an identity at all — would mean a phone
 * that cannot be handed a job because its keystore had an opinion.
 */
class DeviceKeyVault(private val prefs: SharedPreferences) {

    /** Whether the stored key is wrapped by a Keystore key rather than plain. */
    var hardwareBacked: Boolean = false
        private set

    /** The key as the bridge writes it (base64url), or null on first run. */
    fun read(): String? {
        prefs.getString(KEY_SEALED, null)?.let { sealed ->
            unseal(sealed)?.let {
                hardwareBacked = true
                return it
            }
            // Wrapped, and the wrapping key is gone — a Keystore reset, a
            // restored backup, a device migration. There is nothing to
            // recover: this reads as a device with no identity, which is
            // exactly what it is now.
            prefs.edit().remove(KEY_SEALED).apply()
            return null
        }
        return prefs.getString(KEY_PLAIN, null)
    }

    fun write(privateKeyBase64: String) {
        val sealed = seal(privateKeyBase64)
        hardwareBacked = sealed != null
        prefs.edit().apply {
            if (sealed != null) {
                putString(KEY_SEALED, sealed)
                remove(KEY_PLAIN)
            } else {
                putString(KEY_PLAIN, privateKeyBase64)
                remove(KEY_SEALED)
            }
        }.apply()
    }

    private fun seal(value: String): String? = runCatching {
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(Cipher.ENCRYPT_MODE, wrappingKey())
        val iv = cipher.iv
        val body = cipher.doFinal(value.toByteArray(Charsets.UTF_8))
        Base64.encodeToString(iv + body, Base64.NO_WRAP)
    }.getOrNull()

    private fun unseal(value: String): String? = runCatching {
        val blob = Base64.decode(value, Base64.NO_WRAP)
        if (blob.size <= IV_BYTES) return null
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(
            Cipher.DECRYPT_MODE,
            wrappingKey(),
            GCMParameterSpec(TAG_BITS, blob, 0, IV_BYTES)
        )
        String(cipher.doFinal(blob, IV_BYTES, blob.size - IV_BYTES), Charsets.UTF_8)
    }.getOrNull()

    private fun wrappingKey(): SecretKey {
        val store = KeyStore.getInstance(PROVIDER).apply { load(null) }
        (store.getEntry(ALIAS, null) as? KeyStore.SecretKeyEntry)?.let { return it.secretKey }
        val generator = KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, PROVIDER)
        generator.init(
            KeyGenParameterSpec.Builder(
                ALIAS,
                KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT
            )
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                // Deliberately no user authentication requirement: a
                // courier opens a job at a loading bay with gloves on, and
                // a ticket that needs a fingerprint to decode is a ticket
                // that does not open.
                .build()
        )
        return generator.generateKey()
    }

    private companion object {
        const val PROVIDER = "AndroidKeyStore"
        const val ALIAS = "wayfind.device.identity"
        const val TRANSFORMATION = "AES/GCM/NoPadding"
        const val IV_BYTES = 12
        const val TAG_BITS = 128
        const val KEY_SEALED = "deviceKeySealed"
        const val KEY_PLAIN = "deviceKeyPlain"
    }
}
