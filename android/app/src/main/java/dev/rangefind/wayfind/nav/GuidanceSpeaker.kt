package dev.rangefind.wayfind.nav

import android.content.Context
import android.media.AudioAttributes
import android.media.AudioFocusRequest
import android.media.AudioManager
import android.speech.tts.TextToSpeech
import android.speech.tts.UtteranceProgressListener
import android.speech.tts.Voice
import java.util.Locale

/**
 * Spoken guidance, routed the way a car expects it.
 *
 * A bare `TextToSpeech.speak()` is media audio: a head unit may play it on the
 * handset instead of the car speakers, and it talks over whatever is playing
 * rather than ducking it. Tagging the stream as navigation guidance and taking
 * transient focus for the length of each phrase is what makes it behave — in
 * the car and, just as usefully, over music on the phone.
 */
class GuidanceSpeaker(context: Context) {

    private val audio = context.getSystemService(Context.AUDIO_SERVICE) as? AudioManager

    private val attributes = AudioAttributes.Builder()
        .setUsage(AudioAttributes.USAGE_ASSISTANCE_NAVIGATION_GUIDANCE)
        .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
        .build()

    private val focus = AudioFocusRequest.Builder(AudioManager.AUDIOFOCUS_GAIN_TRANSIENT_MAY_DUCK)
        .setAudioAttributes(attributes)
        .setWillPauseWhenDucked(false)
        .build()

    private var ready = false
    private var pending: String? = null

    private val tts = TextToSpeech(context.applicationContext) { status ->
        ready = status == TextToSpeech.SUCCESS
        if (!ready) return@TextToSpeech
        runCatching {
            engine.setAudioAttributes(attributes)
            selectVoice(engine)
            engine.setOnUtteranceProgressListener(object : UtteranceProgressListener() {
                override fun onStart(utteranceId: String?) = Unit
                override fun onDone(utteranceId: String?) = release()
                @Deprecated("Deprecated in Java")
                override fun onError(utteranceId: String?) = release()
                override fun onError(utteranceId: String?, errorCode: Int) = release()
            })
        }
        // A phrase that arrived before the engine was ready still matters:
        // the first one is usually "starting navigation".
        pending?.let { say(it) }
        pending = null
    }

    private val engine: TextToSpeech get() = tts

    /**
     * Picks a voice that still speaks with no connectivity.
     *
     * Guidance matters most when the network is gone — that is precisely when
     * a downloaded region is carrying the drive — but Android is free to
     * resolve the default voice to network synthesis, which simply goes silent
     * offline. Prefer an installed voice in the user's language, matching their
     * country first so a fr-CA device is not read to in fr-FR, and fall back to
     * the platform default rather than forcing a language it cannot speak.
     */
    private fun selectVoice(tts: TextToSpeech) {
        val wanted = Locale.getDefault()
        if (tts.setLanguage(wanted) == TextToSpeech.LANG_NOT_SUPPORTED) {
            tts.setLanguage(Locale.ENGLISH)
        }
        val language = tts.voice?.locale?.language ?: wanted.language
        val installed = runCatching {
            tts.voices.orEmpty().filter { voice ->
                !voice.isNetworkConnectionRequired &&
                    !voice.features.orEmpty()
                        .contains(TextToSpeech.Engine.KEY_FEATURE_NOT_INSTALLED) &&
                    voice.locale.language == language
            }
        }.getOrDefault(emptyList())
        val best = installed.sortedWith(
            compareByDescending<Voice> { it.locale.country == wanted.country }
                .thenByDescending { it.quality }
        ).firstOrNull()
        if (best != null) runCatching { tts.voice = best }
    }

    fun say(phrase: String) {
        if (!ready) {
            pending = phrase
            return
        }
        runCatching {
            audio?.requestAudioFocus(focus)
            tts.speak(phrase, TextToSpeech.QUEUE_FLUSH, null, UTTERANCE_ID)
        }
    }

    private fun release() {
        runCatching { audio?.abandonAudioFocusRequest(focus) }
    }

    fun shutdown() {
        runCatching {
            tts.stop()
            tts.shutdown()
        }
        release()
    }

    private companion object {
        const val UTTERANCE_ID = "wayfind-guidance"
    }
}
