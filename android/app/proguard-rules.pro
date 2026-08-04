# The JS bridge is reached reflectively from the WebView.
-keepclassmembers class dev.rangefind.wayfind.engine.** {
    @android.webkit.JavascriptInterface <methods>;
}
