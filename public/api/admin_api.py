import os
import logging
import traceback
from functools import wraps
from flask import Blueprint, jsonify, request, session
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from werkzeug.security import check_password_hash
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import requests as http_requests

# .env dosyasının yolunu belirtip, değişkenleri yüklüyoruz
dotenv_path = '/var/www/aracabak.com/private/secrets/.env'
load_dotenv(dotenv_path=dotenv_path)

# Blueprint'i oluşturalım
admin_bp = Blueprint('admin_api', __name__)

# Google Client ID'yi ortam değişkeninden alalım
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')

# --- VERİTABANI BAĞLANTISI (DÜZELTİLMİŞ) ---
# Bu fonksiyon artık main_api.py'deki gibi doğru bilgileri okuyor.
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        return conn
    except Exception as e:
        logging.error(f"Admin API veritabanı bağlantı hatası: {e}")
        raise # Hatayı yukarı taşıyarak işlemin durmasını sağlıyoruz

# --- YETKİ KONTROL DECORATOR'I ---
def admin_only(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return jsonify({"description": "Bu kaynağa erişim yetkiniz yok."}), 403
        return f(*args, **kwargs)
    return decorated_function

# --- ADMIN API ENDPOINT'LERİ ---

@admin_bp.route('/api/admin/login', methods=['POST'])
def admin_login():
    data = request.json
    google_token, password = data.get('token'), data.get('password')
    if not google_token or not password:
        return jsonify({"description": "Token ve şifre gerekli."}), 400

    conn = None # conn değişkenini başlangıçta tanımlıyoruz
    try:
        idinfo = id_token.verify_oauth2_token(google_token, google_requests.Request(), GOOGLE_CLIENT_ID)
        admin_email = idinfo['email']
        
        conn = get_db_connection() # Düzeltilmiş fonksiyonu çağırıyoruz
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cursor.execute('SELECT * FROM Admins WHERE email = %s', (admin_email,))
        admin_user = cursor.fetchone()
        
        cursor.close()

        if not admin_user:
            logging.warning(f"Yetkisiz admin giriş denemesi: {admin_email}")
            return jsonify({"description": "Bu hesap admin yetkisine sahip değil."}), 403

        if check_password_hash(admin_user['password_hash'], password):
            session.clear()
            session.update(admin_logged_in=True, admin_email=admin_user['email'], name=idinfo.get('name', 'Admin'))
            logging.info(f"Admin {admin_user['email']} başarıyla giriş yaptı.")
            return jsonify({"status": "admin_login_success"})
        else:
            logging.warning(f"Başarısız admin giriş denemesi (yanlış şifre): {admin_email}")
            return jsonify({"description": "Geçersiz admin şifresi."}), 401

    except ValueError:
        logging.error(f"Geçersiz Google token ile admin girişi denendi.")
        return jsonify({"description": "Geçersiz Google kimliği."}), 401
    except Exception as e:
        logging.error(f"Admin giriş hatası: {e}")
        logging.error(traceback.format_exc())
        return jsonify({"description": "Sunucu tarafında bir hata oluştu."}), 500
    finally:
        if conn:
            conn.close()

@admin_bp.route('/api/admin/status')
def admin_status():
    if session.get('admin_logged_in'):
        return jsonify({"loggedIn": True, "email": session.get('admin_email'), "name": session.get('name')})
    return jsonify({"loggedIn": False})

@admin_bp.route('/api/admin/dashboard_stats')
@admin_only
def dashboard_stats():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        stats = {}
        
        cursor.execute("SELECT COUNT(*) FROM PageViews WHERE timestamp >= NOW() - INTERVAL '1 day'")
        stats['daily_views'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM PageViews WHERE timestamp >= NOW() - INTERVAL '1 week'")
        stats['weekly_views'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM PageViews WHERE timestamp >= NOW() - INTERVAL '1 year'")
        stats['yearly_views'] = cursor.fetchone()[0]

        cursor.execute("SELECT country, city, COUNT(*) as count FROM PageViews WHERE country IS NOT NULL AND city IS NOT NULL GROUP BY country, city ORDER BY count DESC LIMIT 20")
        stats['locations'] = [dict(row) for row in cursor.fetchall()]

        cursor.execute("SELECT name, email, created_at, user_type FROM Users ORDER BY created_at DESC LIMIT 10")
        stats['latest_users'] = [dict(row) for row in cursor.fetchall()]
        
        cursor.close()
        return jsonify(stats)
    except Exception as e:
        logging.error(f"Dashboard istatistikleri alınırken hata: {e}")
        return jsonify({"description": "İstatistikler alınamadı."}), 500
    finally:
        if conn:
            conn.close()

@admin_bp.route('/api/admin/test')
def admin_test_route():
    return jsonify({"status": "success", "message": "Tebrikler, admin_api.py başarıyla çalışıyor ve bu mesajı gösteriyor!"})

