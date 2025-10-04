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
import android.content.Intent
import android.net.Uri
import android.webkit.WebResourceRequest

class MainActivity : AppCompatActivity() {

    private lateinit var webView: WebView
    private val MAIN_DOMAIN = "aracabak.com" // Uygulamanın yüklendiği ana domain

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

        // --- GÜNCELLEME BAŞLANGICI: Özel WebViewClient (tel: ve harici linkler için) ---
        webView.webViewClient = object : WebViewClient() {

            // Yeni API'lar için (Android 5.0 ve sonrası)
            override fun shouldOverrideUrlLoading(view: WebView?, request: WebResourceRequest?): Boolean {
                val url = request?.url?.toString()
                return handleUri(view, url)
            }

            // Eski API'lar için (Geriye dönük uyumluluk)
            @Suppress("DEPRECATION")
            override fun shouldOverrideUrlLoading(view: WebView?, url: String?): Boolean {
                return handleUri(view, url)
            }

            private fun handleUri(view: WebView?, url: String?): Boolean {
                if (url == null) return false

                // TELEFON, E-POSTA ve HARİTA şemalarını yerel uygulamaya yönlendir
                if (url.startsWith("tel:") || url.startsWith("mailto:") || url.startsWith("geo:")) {
                    try {
                        val intent = Intent(Intent.ACTION_VIEW, Uri.parse(url))
                        startActivity(intent)
                        return true // WebView'ın bu URL'i yüklemesini engelle
                    } catch (e: Exception) {
                        Log.e("WebViewClient", "Yerel Intent başlatılamadı: $e")
                        return true
                    }
                }

                // Harici linkleri (Google Maps yol tarifi gibi) harici tarayıcıda aç
                if (url.startsWith("http") && !url.contains(MAIN_DOMAIN)) {
                    try {
                        // Harici bağlantıyı varsayılan tarayıcıda aç
                        val intent = Intent(Intent.ACTION_VIEW, Uri.parse(url))
                        startActivity(intent)
                        return true
                    } catch (e: Exception) {
                        Log.e("WebViewClient", "Harici URL açılamadı: $e")
                        return true
                    }
                }

                // Ana domaine ait veya özel bir şema değilse, WebView'ın yüklemesine izin ver
                return false
            }
        }
        // --- GÜNCELLEME SONU: Özel WebViewClient ---

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
