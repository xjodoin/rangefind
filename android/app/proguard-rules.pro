# The JS bridge is reached reflectively from the WebView.
-keepclassmembers class dev.rangefind.maps.engine.** {
    @android.webkit.JavascriptInterface <methods>;
}
