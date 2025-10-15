package com.example.aracabak

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Bundle
import android.util.Log
import android.webkit.ConsoleMessage
import android.webkit.CookieManager
import android.webkit.GeolocationPermissions
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.activity.OnBackPressedCallback
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat

class MainActivity : AppCompatActivity() {

    private lateinit var webView: WebView
    private val MAIN_DOMAIN = "aracabak.com" // Uygulamanın yüklendiği ana domain

    // --- Konum İzinleri için Gerekli Değişkenler ---
    private val LOCATION_PERMISSION_REQUEST_CODE = 1
    private var geolocationOrigin: String? = null
    private var geolocationCallback: GeolocationPermissions.Callback? = null

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
        
        // --- KONUM AYARI EKLENDİ ---
        webSettings.setGeolocationEnabled(true)


        // --- Güvenlik ve Uyumluluk Ayarları (EN ÖNEMLİ KISIM) ---
        webSettings.mixedContentMode = WebSettings.MIXED_CONTENT_ALWAYS_ALLOW
        webSettings.javaScriptCanOpenWindowsAutomatically = true

        // Üçüncü parti çerezlere izin ver (Google SSO için ÇOK ÖNEMLİ)
        CookieManager.getInstance().setAcceptThirdPartyCookies(webView, true)

        // --- WebView'i mobil Chrome olarak gizle ---
        // Bu, Google gibi servislerin WebView'i engellemesini önler.
        val newUserAgent = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36"
        webSettings.userAgentString = newUserAgent

        // --- WebViewClient ---
        webView.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(view: WebView?, request: WebResourceRequest?): Boolean {
                val url = request?.url?.toString()
                return handleUri(view, url)
            }

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

                if (url.startsWith("http") && !url.contains(MAIN_DOMAIN) && !url.contains("accounts.google.com")) {
                    try {
                        val intent = Intent(Intent.ACTION_VIEW, Uri.parse(url))
                        startActivity(intent)
                        return true
                    } catch (e: Exception) {
                        Log.e("WebViewClient", "Harici URL açılamadı: $e")
                        return true
                    }
                }
                return false
            }
        }

        // --- GÜNCELLENMİŞ WebChromeClient (Konum İzini İçin) ---
        webView.webChromeClient = object : WebChromeClient() {
            override fun onConsoleMessage(consoleMessage: ConsoleMessage): Boolean {
                Log.d("WebViewConsole", "${consoleMessage.message()} -- From line ${consoleMessage.lineNumber()} of ${consoleMessage.sourceId()}")
                return true
            }

            override fun onGeolocationPermissionsShowPrompt(origin: String, callback: GeolocationPermissions.Callback) {
                if (ContextCompat.checkSelfPermission(this@MainActivity, Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
                    // İzin daha önce verilmemişse, isteği ve callback'i sakla
                    geolocationOrigin = origin
                    geolocationCallback = callback
                    ActivityCompat.requestPermissions(this@MainActivity, arrayOf(Manifest.permission.ACCESS_FINE_LOCATION), LOCATION_PERMISSION_REQUEST_CODE)
                } else {
                    // İzin zaten verilmişse, doğrudan onayla
                    callback.invoke(origin, true, false)
                }
            }
        }

        webView.loadUrl("https://aracabak.com?client_type=android")

        // Geri Tuşu Mantığı
        onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
            override fun handleOnBackPressed() {
                if (webView.canGoBack()) {
                    webView.goBack()
                } else {
                    isEnabled = false
                    onBackPressedDispatcher.onBackPressed()
                }
            }
        })
    }

    // --- KONUM İZNİ SONUCUNU İŞLEYEN FONKSİYON ---
    override fun onRequestPermissionsResult(requestCode: Int, permissions: Array<out String>, grantResults: IntArray) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        when (requestCode) {
            LOCATION_PERMISSION_REQUEST_CODE -> {
                if (grantResults.isNotEmpty() && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                    // Kullanıcı izni verdiyse, saklanan callback'i onayla
                    geolocationCallback?.invoke(geolocationOrigin, true, false)
                } else {
                    // Kullanıcı izni reddettiyse, saklanan callback'i reddet
                    geolocationCallback?.invoke(geolocationOrigin, false, false)
                    Log.w("MainActivity", "Konum izni kullanıcı tarafından reddedildi.")
                }
                // Saklanan değerleri temizle
                geolocationOrigin = null
                geolocationCallback = null
            }
        }
    }
}

