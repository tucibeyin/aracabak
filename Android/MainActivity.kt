package com.example.aracabak

import androidx.appcompat.app.AppCompatActivity
import android.os.Bundle
import android.util.Log
import android.webkit.ConsoleMessage
import android.webkit.CookieManager
import android.webkit.WebChromeClient
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.activity.OnBackPressedCallback

class MainActivity : AppCompatActivity() {

    private lateinit var webView: WebView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        WebView.setWebContentsDebuggingEnabled(true)

        webView = findViewById(R.id.webView)

        // --- EN KAPSAMLI WEBVIEW AYARLARI ---
        val webSettings = webView.settings
        webSettings.javaScriptEnabled = true
        webSettings.domStorageEnabled = true
        webSettings.databaseEnabled = true

        // --- Güvenlik ve Uyumluluk Ayarları (EN ÖNEMLİ KISIM) ---
        webSettings.mixedContentMode = WebSettings.MIXED_CONTENT_ALWAYS_ALLOW
        webSettings.javaScriptCanOpenWindowsAutomatically = true

        // Üçüncü parti çerezlere izin ver (Google SSO için ÇOK ÖNEMLİ)
        CookieManager.getInstance().setAcceptThirdPartyCookies(webView, true)

        // --- YENİ GÜNCELLEME: WebView'i mobil Chrome olarak gizle ---
        // Bu, Google gibi servislerin WebView'i engellemesini önler.
        val newUserAgent = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36"
        webSettings.userAgentString = newUserAgent
        // --- GÜNCELLEME SONU ---


        webView.webViewClient = WebViewClient()

        webView.webChromeClient = object : WebChromeClient() {
            override fun onConsoleMessage(consoleMessage: ConsoleMessage): Boolean {
                Log.d("WebViewConsole", "${consoleMessage.message()} -- From line ${consoleMessage.lineNumber()} of ${consoleMessage.sourceId()}")
                return true
            }
        }

        webView.loadUrl("https://aracabak.com?client_type=android")

        // --- Düzeltilmiş ve Daha Güvenli Geri Tuşu Mantığı ---
        onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
            override fun handleOnBackPressed() {
                if (webView.canGoBack()) {
                    // Eğer web'de geri gidilecek sayfa varsa, sadece geri git.
                    webView.goBack()
                } else {
                    // Yoksa, callback'i devre dışı bırak ve uygulamayı normal şekilde kapatmasını sağla.
                    isEnabled = false
                    onBackPressedDispatcher.onBackPressed()
                }
            }
        })
    }
}
