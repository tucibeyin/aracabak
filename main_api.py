import os
import psycopg2
import psycopg2.extras
from psycopg2 import errors as psycopg2_errors
import logging
import re
import json
import math
import traceback
from flask import Flask, jsonify, request, session
from flask_session import Session
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import redis
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from werkzeug.security import generate_password_hash, check_password_hash
import requests
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException

# --- Yapılandırma ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
dotenv_path = '/var/www/aracabak.com/private/secrets/.env'
load_dotenv(dotenv_path=dotenv_path)

# --- Değişkenler ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VEHICLE_DATA_PATH = os.path.join(BASE_DIR, '..', '..', 'database', 'tum_data.json')
DIZEL_MAINTENANCE_PATH = os.path.join(BASE_DIR, '..', '..', 'database', 'dizel_bakim_parcalari.json')
BENZIN_MAINTENANCE_PATH = os.path.join(BASE_DIR, '..', '..', 'database', 'benzin_bakim_parcalari.json')
CITIES_DATA_PATH = os.path.join(BASE_DIR, '..', '..', 'database', 'sehirler.json')
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY", "").strip()
all_vehicle_data = []

# --- Flask Uygulaması ve Oturum Yapılandırması ---
app = Flask(__name__)

# --- Sabit ve Güvenli SECRET_KEY Kullanımı ---
FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY")
if not FLASK_SECRET_KEY:
    raise ValueError("FLASK_SECRET_KEY ortam değişkeni ayarlanmamış! Lütfen .env dosyanıza güvenli bir anahtar ekleyin.")
app.config["SECRET_KEY"] = FLASK_SECRET_KEY

app.config["SESSION_TYPE"] = "redis"
app.config["SESSION_PERMANENT"] = True
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)
app.config["SESSION_USE_SIGNER"] = True
app.config["SESSION_REDIS"] = redis.from_url("redis://127.0.0.1:6379")
Session(app)

# --- Rate Limiter ---
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)
try:
    redis_client = redis.from_url("redis://127.0.0.1:6379")
    redis_client.ping()
    app.config["RATELIMIT_STORAGE_URI"] = "redis://127.0.0.1:6379"
    limiter = Limiter(get_remote_address, app=app, storage_uri="redis://127.0.0.1:6379")
    logging.info("Rate limiter Redis ile başarıyla yapılandırıldı.")
except (redis.exceptions.ConnectionError, Exception) as e:
    logging.warning(f"Redis'e bağlanılamadı, rate limiter bellek üzerinde çalışacak: {e}")

# --- Helper Fonksiyonlar ve Veritabanı ---
def get_db_connection():
    """PostgreSQL veritabanına yeni bir bağlantı oluşturur."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )
    return conn

def init_db():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Users (
                id SERIAL PRIMARY KEY,
                google_id TEXT UNIQUE,
                email TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                password_hash TEXT,
                user_type TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        add_column_if_not_exists(cursor, "users", "phone_number", "TEXT")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Vehicles (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES Users(id) ON DELETE CASCADE,
                plate_number TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                brand TEXT,
                series TEXT,
                year TEXT,
                fuel TEXT,
                model TEXT,
                last_inspection_date TEXT,
                tax_paid_jan INTEGER DEFAULT 0,
                tax_paid_jul INTEGER DEFAULT 0
            )
        ''')
        add_column_if_not_exists(cursor, "vehicles", "tax_paid_jan", "INTEGER DEFAULT 0")
        add_column_if_not_exists(cursor, "vehicles", "tax_paid_jul", "INTEGER DEFAULT 0")
        add_column_if_not_exists(cursor, "vehicles", "last_inspection_date", "TEXT")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Shops (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL UNIQUE REFERENCES Users(id) ON DELETE CASCADE,
                city TEXT,
                phone TEXT,
                google_place_id TEXT
            )
        ''')
        add_column_if_not_exists(cursor, "shops", "serviced_brands", "TEXT")
        add_column_if_not_exists(cursor, "shops", "google_place_name", "TEXT")
        add_column_if_not_exists(cursor, "shops", "google_place_phone", "TEXT")
        add_column_if_not_exists(cursor, "shops", "google_place_url", "TEXT")
        add_column_if_not_exists(cursor, "shops", "google_place_last_updated", "TIMESTAMP WITH TIME ZONE")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Requests (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES Users(id),
                shop_user_id INTEGER NOT NULL REFERENCES Users(id),
                vehicle_brand TEXT,
                vehicle_series TEXT,
                vehicle_year TEXT,
                vehicle_fuel TEXT,
                vehicle_model TEXT,
                vehicle_km INTEGER,
                city TEXT,
                maintenance_km INTEGER,
                selected_parts TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                shop_google_place_id TEXT
            )
        ''')
        add_column_if_not_exists(cursor, "requests", "status", "TEXT DEFAULT 'pending'")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Quotes (
                id SERIAL PRIMARY KEY,
                request_id INTEGER NOT NULL UNIQUE REFERENCES Requests(id),
                shop_user_id INTEGER NOT NULL REFERENCES Users(id),
                parts_cost REAL NOT NULL,
                labor_cost REAL NOT NULL,
                total_cost REAL NOT NULL,
                notes TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        add_column_if_not_exists(cursor, "quotes", "owner_proposed_cost", "REAL")
        add_column_if_not_exists(cursor, "quotes", "last_offer_by", "TEXT DEFAULT 'business'")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS FuelEntries (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES Users(id) ON DELETE CASCADE,
                vehicle_id INTEGER NOT NULL REFERENCES Vehicles(id) ON DELETE CASCADE,
                date TEXT NOT NULL,
                amount_tl REAL,
                amount_liter REAL,
                distance_km REAL NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Appointments (
                id SERIAL PRIMARY KEY,
                request_id INTEGER NOT NULL REFERENCES Requests(id) ON DELETE CASCADE,
                quote_id INTEGER NOT NULL REFERENCES Quotes(id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES Users(id) ON DELETE CASCADE,
                shop_user_id INTEGER NOT NULL REFERENCES Users(id) ON DELETE CASCADE,
                appointment_date TEXT,
                status TEXT DEFAULT 'scheduled',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS FuelPrices (
                id SERIAL PRIMARY KEY,
                city TEXT NOT NULL,
                vmax_kursunsuz_95 REAL,
                vmax_diesel REAL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL
            )
        ''')

        # --- GÜNCELLENMİŞ LİSANS TABLOSU ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Licenses (
                id SERIAL PRIMARY KEY,
                shop_id INTEGER NOT NULL UNIQUE REFERENCES Shops(id) ON DELETE CASCADE,
                license_key TEXT NOT NULL UNIQUE,
                is_active BOOLEAN NOT NULL DEFAULT true
            )
        ''')

        conn.commit()
        logging.info("Veritabanı başarıyla kontrol edildi.")
    except Exception as e:
        logging.error(f"Veritabanı başlatma hatası: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def add_column_if_not_exists(cursor, table_name, column_name, column_def):
    """PostgreSQL için bir tabloda sütun var mı diye kontrol eder ve yoksa ekler."""
    table_name = table_name.lower()
    column_name = column_name.lower()
    cursor.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
    """, (table_name, column_name))
    if not cursor.fetchone():
        cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_def}')
        logging.info(f"'{column_name}' sütunu '{table_name}' tablosuna eklendi.")

def load_vehicle_data():
    global all_vehicle_data
    if not all_vehicle_data:
        try:
            with open(VEHICLE_DATA_PATH, 'r', encoding='utf-8') as f:
                all_vehicle_data = json.load(f)
        except Exception as e:
            logging.error(f"{VEHICLE_DATA_PATH} okunurken hata: {e}")

def validate_plate_number(plate):
    cleaned_plate = re.sub(r'\s+', '', plate.upper())
    return re.fullmatch(r'^\d{2}[A-Z]{1,3}\d{2,4}$', cleaned_plate)

def format_plate_for_db(plate):
    cleaned = re.sub(r'\s+', '', plate.upper())
    match = re.match(r'^(\d{2})([A-Z]{1,3})(\d{2,4})$', cleaned)
    if match:
        return f"{match.group(1)} {match.group(2)} {match.group(3)}"
    return plate.upper()

def validate_phone_number(phone):
    return re.fullmatch(r'^0\d{10}$', phone) if phone else True

def send_welcome_email(user_name, user_email, user_type):
    if not BREVO_API_KEY:
        logging.error("Brevo API anahtarı bulunamadı. E-posta gönderilemiyor.")
        return

    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = BREVO_API_KEY
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))

    subject = ""
    html_content = ""
    
    base_template = """
    <!DOCTYPE html>
    <html lang="tr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{subject}</title>
        <style>
            body {{ margin: 0; padding: 0; background-color: #f4f4f4; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif; }}
            .container {{ width: 100%; max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }}
            .header {{ background-color: #111827; padding: 20px; text-align: center; }}
            .header img {{ max-width: 150px; }}
            .content {{ padding: 30px 40px; color: #333333; line-height: 1.6; }}
            .content h1 {{ color: #111827; font-size: 24px; }}
            .content ul {{ padding-left: 20px; }}
            .button-container {{ text-align: center; margin: 30px 0; }}
            .button {{ background-color: #2563eb; color: #ffffff; padding: 12px 25px; border-radius: 5px; text-decoration: none; font-weight: bold; display: inline-block; }}
            .footer {{ background-color: #f8f9fa; text-align: center; padding: 20px; font-size: 12px; color: #6c757d; border-top: 1px solid #dee2e6; }}
            .footer a {{ color: #2563eb; text-decoration: none; }}
        </style>
    </head>
    <body>
        <div style="padding: 20px;">
            <div class="container">
                <div class="header">
                    <img src="https://aracabak.com/logolar/aracabak_menu_logo.png" alt="aracabak Logo">
                </div>
                <div class="content">
                    <h1>Merhaba {user_name},</h1>
                    <p>{welcome_message}</p>
                    <p>{platform_intro}</p>
                    <ul>{features_list}</ul>
                    <div class="button-container">
                        <a href="https://aracabak.com/account.html" class="button">{button_text}</a>
                    </div>
                    <p>Herhangi bir sorunuz olursa, bize her zaman <a href="mailto:info.aracabak@gmail.com">info.aracabak@gmail.com</a> adresinden ulaşabilirsiniz.</p>
                    <p>İyi günler dileriz,<br><b>aracabak Ekibi</b></p>
                </div>
                <div class="footer">
                    <p>© 2025 aracabak. Tüm hakları saklıdır.<br>®vovDigital</p>
                    <p><a href="https://aracabak.com">Website</a> | <a href="mailto:info.aracabak@gmail.com">İletişim</a></p>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    if user_type == 'business':
        subject = f"İşletmeniz aracabak'ta, {user_name}!"
        email_vars = {
            "subject": subject,
            "user_name": user_name,
            "welcome_message": "İşletme hesabınız başarıyla oluşturuldu! Sizi aramızda görmekten ve bölgenizdeki araç sahipleriyle buluşturacak olmaktan heyecan duyuyoruz.",
            "platform_intro": "aracabak, hizmetlerinize ihtiyaç duyan potansiyel müşterilere kolayca ulaşmanız için tasarlandı. Platformumuzda işletmenizi bir adım öne taşıyabilirsiniz:",
            "features_list": """
                <li>Profesyonel bir işletme profili oluşturun.</li>
                <li>Bölgenizdeki araç sahiplerinden bakım talepleri alın.</li>
                <li>Hızlı ve kolay bir şekilde fiyat teklifleri sunun.</li>
                <li>Onaylanan teklifleri randevulara dönüştürüp yönetin.</li>
            """,
            "button_text": "İşletme Profilinize Gidin"
        }
    else: # owner
        subject = f"aracabak'a Hoş Geldin, {user_name}!"
        email_vars = {
            "subject": subject,
            "user_name": user_name,
            "welcome_message": "aracabak.com'a başarıyla kaydoldunuz! Sizi aramızda görmekten büyük mutluluk duyuyoruz.",
            "platform_intro": "aracabak, aracınızın tüm ihtiyaçlarını kolayca yönetmeniz için tasarlandı. Platformumuzda şunları yapabilirsiniz:",
            "features_list": """
                <li>Aracınızın periyodik bakım detaylarını anında öğrenin.</li>
                <li>Anlaşmalı servislerden hızlıca bakım teklifleri alın.</li>
                <li>MTV ve muayene tarihlerini sizin yerinize biz takip edelim.</li>
                <li>Yakıt harcamalarınızı kaydedip detaylı raporlar alın.</li>
            """,
            "button_text": "Hesabınıza Gidin"
        }
    
    html_content = base_template.format(**email_vars)
    
    sender = {"name": "aracabak", "email": "info.aracabak@gmail.com"}
    to = [{"email": user_email, "name": user_name}]
    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=to, html_content=html_content, sender=sender, subject=subject)
    
    try:
        api_instance.send_transac_email(send_smtp_email)
        logging.info(f"'{user_type}' tipi için hoş geldin e-postası başarıyla gönderildi: {user_email}")
    except ApiException as e:
        logging.error(f"Brevo API hatası: E-posta gönderilemedi ({user_email}). Hata Kodu: {e.status}, Hata Sebebi: {e.reason}")
        logging.error(f"Brevo API Hata Detayı: {e.body}")

with app.app_context():
    init_db()

# --- API Endpoint'leri ---
@app.route('/api/auth/status')
def auth_status():
    if 'user_id' in session:
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            user_id = session['user_id']
            user_type = session.get('user_type')
            
            response_data = {
                "loggedIn": True,
                "userName": session.get('name'),
                "userType": user_type,
                "email": session.get('email')
            }
            
            if user_type == 'owner':
                cursor.execute('SELECT id, plate_number, brand, series, year, model, fuel FROM Vehicles WHERE user_id = %s', (user_id,))
                vehicles = [dict(row) for row in cursor.fetchall()]
                response_data['vehicles'] = vehicles
            
            return jsonify(response_data)
        except Exception as e:
            logging.error(f"Oturum durumu kontrol edilirken veritabanı hatası: {e}")
            return jsonify({"loggedIn": False, "error": "Internal server error"}), 500
        finally:
            if conn:
                conn.close()
    
    return jsonify({"loggedIn": False})

@app.route('/api/auth/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({"status": "success"})

@app.route('/api/config')
def get_config():
    return jsonify({
        "googleClientId": GOOGLE_CLIENT_ID,
        "googleMapsApiKey": GOOGLE_MAPS_API_KEY
    })

@app.route('/api/fuel_prices')
def get_fuel_prices():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cursor.execute("SELECT MAX(updated_at) as last_update FROM FuelPrices")
        result = cursor.fetchone()
        last_update = result['last_update'] if result else None
        
        is_data_fresh = False
        if last_update:
            if (datetime.now(timezone.utc) - last_update) < timedelta(hours=1):
                is_data_fresh = True
        
        if is_data_fresh:
            logging.info("Yakıt fiyatları önbellekten (DB) sunuluyor.")
            cursor.execute("SELECT city, vmax_kursunsuz_95, vmax_diesel FROM FuelPrices")
            prices_from_db = cursor.fetchall()
            formatted_prices = [
                {"City": row['city'], "V/Max Kurşunsuz 95": row['vmax_kursunsuz_95'], "V/Max Diesel": row['vmax_diesel']}
                for row in prices_from_db
            ]
            return jsonify(formatted_prices)
            
        logging.info("Önbellek boş veya eski, harici API'den yakıt fiyatları çekiliyor.")
        try:
            response = requests.get("https://apisepeti.com/wp-json/petrol/v1/fiyatlar", timeout=10)
            response.raise_for_status()
            prices = response.json()
            
            now_ts = datetime.now(timezone.utc)
            cursor.execute("DELETE FROM FuelPrices")
            for city_data in prices:
                city = city_data.get("City")
                benzin = city_data.get("V/Max Kurşunsuz 95")
                motorin = city_data.get("V/Max Diesel")
                if city and benzin is not None and motorin is not None:
                    cursor.execute(
                        "INSERT INTO FuelPrices (city, vmax_kursunsuz_95, vmax_diesel, updated_at) VALUES (%s, %s, %s, %s)",
                        (city, benzin, motorin, now_ts)
                    )
            conn.commit()
            logging.info("Yakıt fiyatları veritabanına başarıyla kaydedildi.")
            return jsonify(prices)

        except requests.exceptions.RequestException as e:
            logging.error(f"Harici yakıt API'sine ulaşılamadı: {e}")
            if last_update:
                logging.warning("API hatası, eski yakıt verileri sunuluyor.")
                cursor.execute("SELECT city, vmax_kursunsuz_95, vmax_diesel FROM FuelPrices")
                prices_from_db = cursor.fetchall()
                formatted_prices = [
                    {"City": row['city'], "V/Max Kurşunsuz 95": row['vmax_kursunsuz_95'], "V/Max Diesel": row['vmax_diesel']}
                    for row in prices_from_db
                ]
                return jsonify(formatted_prices)
            return jsonify({"description": "Yakıt fiyatları servisine şu anda ulaşılamıyor."}), 503

    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Yakıt fiyatları alınırken beklenmedik bir hata oluştu: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/requests', methods=['GET'])
@limiter.limit("30 per minute")
def get_requests():
    if 'user_id' not in session:
        return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        user_id = session['user_id']
        user_type = session['user_type']

        if user_type == 'business':
            cursor.execute("SELECT id, google_place_id FROM Shops WHERE user_id = %s", (user_id,))
            shop_data = cursor.fetchone()
            if not shop_data or not shop_data['google_place_id']:
                logging.warning(f"İşletme (ID: {user_id}) lisans anahtarı girmediği için talepleri göremiyor.")
                return jsonify([])

            license_key = shop_data['google_place_id']
            shop_id = shop_data['id']
            cursor.execute("SELECT is_active FROM Licenses WHERE license_key = %s AND shop_id = %s", (license_key, shop_id))
            license_status = cursor.fetchone()

            if not license_status or not license_status['is_active']:
                logging.warning(f"İşletme (ID: {user_id}) geçersiz veya pasif lisans ('{license_key}') nedeniyle talepleri göremiyor.")
                return jsonify([])
            
            query = """
                SELECT r.*, u.name as customer_name, u.phone_number as customer_phone, 
                       q.total_cost, q.parts_cost, q.labor_cost, q.notes as quote_notes, q.id as quote_id,
                       q.owner_proposed_cost, q.last_offer_by
                FROM Requests r JOIN Users u ON r.user_id = u.id
                LEFT JOIN Quotes q ON r.id = q.request_id
                WHERE r.shop_user_id = %s ORDER BY r.created_at DESC
            """
            cursor.execute(query, (user_id,))
        elif user_type == 'owner':
            query = """
                SELECT r.*, 
                       COALESCE(s.google_place_name, u.name) as shop_name, 
                       COALESCE(s.google_place_phone, s.phone) as shop_phone, 
                       s.google_place_id, s.google_place_last_updated,
                       q.parts_cost, q.labor_cost, q.total_cost, q.notes as quote_notes, q.id as quote_id,
                       q.owner_proposed_cost, q.last_offer_by
                FROM Requests r JOIN Users u ON r.shop_user_id = u.id
                LEFT JOIN Shops s ON r.shop_user_id = s.user_id
                LEFT JOIN Quotes q ON r.id = q.request_id
                WHERE r.user_id = %s ORDER BY r.created_at DESC
            """
            cursor.execute(query, (user_id,))
        else:
            return jsonify([])

        requests_list = []
        for row in cursor.fetchall():
            req = dict(row)
            if req.get('selected_parts'):
                req['selected_parts'] = json.loads(req['selected_parts'])

            if req.get('total_cost') is not None:
                req['quote'] = { 
                    "id": req['quote_id'], "parts_cost": req['parts_cost'], "labor_cost": req['labor_cost'], 
                    "total_cost": req['total_cost'], "notes": req['quote_notes'],
                    "owner_proposed_cost": req['owner_proposed_cost'], "last_offer_by": req['last_offer_by']
                }
            
            for key in ['parts_cost', 'labor_cost', 'total_cost', 'quote_notes', 'quote_id', 'owner_proposed_cost', 'last_offer_by']:
                req.pop(key, None)
            
            if user_type == 'owner':
                place_id = req.get('google_place_id')
                last_updated = req.get('google_place_last_updated')
                is_cache_stale = not last_updated or (datetime.now(timezone.utc) - last_updated) > timedelta(days=7)

                if place_id and is_cache_stale:
                    logging.info(f"[get_requests] '{req['shop_name']}' için eski/boş önbellek. Google API'den güncel veri çekiliyor.")
                    try:
                        url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=name,formatted_phone_number,url&key={GOOGLE_PLACES_API_KEY}&language=tr"
                        response = requests.get(url, timeout=5)
                        place_data = response.json()
                        if place_data.get("status") == "OK" and "result" in place_data:
                            result = place_data['result']
                            new_name = result.get('name')
                            new_phone = result.get('formatted_phone_number')
                            new_url = result.get('url')
                            
                            req['shop_name'] = new_name or req['shop_name']
                            req['shop_phone'] = new_phone or req['shop_phone']
                            
                            with get_db_connection() as conn2:
                                with conn2.cursor() as update_cursor:
                                    update_cursor.execute(
                                        "UPDATE Shops SET google_place_name = %s, google_place_phone = %s, google_place_url = %s, google_place_last_updated = %s WHERE google_place_id = %s",
                                        (new_name, new_phone, new_url, datetime.now(timezone.utc), place_id)
                                    )
                                    conn2.commit()
                                    logging.info(f"[get_requests] '{new_name}' için önbellek güncellendi.")
                    except Exception as e:
                        logging.error(f"[get_requests] İhtiyaç anında önbellek güncelleme başarısız: {e}")

            requests_list.append(req)
        return jsonify(requests_list)
    except Exception as e:
        logging.error(f"Talep listeleme hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/requests', methods=['POST'])
@limiter.limit("30 per minute")
def create_request():
    if 'user_id' not in session:
        return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        user_id = session['user_id']
        data = request.get_json()
        if not all(field in data for field in ['shop_user_id', 'shop_google_place_id', 'vehicle', 'maintenance_km', 'selected_parts', 'city']):
            return jsonify({"description": "Eksik bilgi."}), 400
        vehicle = data['vehicle']
        selected_parts_json = json.dumps(data['selected_parts'])
        cursor.execute(
            """
            INSERT INTO Requests (user_id, shop_user_id, shop_google_place_id, vehicle_brand, vehicle_series,
                                  vehicle_year, vehicle_fuel, vehicle_model, vehicle_km, city,
                                  maintenance_km, selected_parts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (user_id, data['shop_user_id'], data['shop_google_place_id'], vehicle['brand'], vehicle['series'],
             vehicle['year'], vehicle['fuel'], vehicle['model'], vehicle['km'], data['city'],
             data['maintenance_km'], selected_parts_json)
        )
        conn.commit()
        return jsonify({"status": "success", "description": "Talep iletildi."}), 201
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Talep oluşturma hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/requests/<int:request_id>', methods=['DELETE'])
@limiter.limit("30 per minute")
def delete_request(request_id):
    if 'user_id' not in session:
        return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        user_id = session['user_id']
        user_type = session['user_type']
        req_to_delete = None
        if user_type == 'business':
            cursor.execute('SELECT id FROM Requests WHERE id = %s AND shop_user_id = %s', (request_id, user_id))
            req_to_delete = cursor.fetchone()
        elif user_type == 'owner':
            cursor.execute('SELECT id FROM Requests WHERE id = %s AND user_id = %s', (request_id, user_id))
            req_to_delete = cursor.fetchone()
        if not req_to_delete:
            return jsonify({"description": "Talep bulunamadı veya silme yetkiniz yok."}), 404
        cursor.execute('DELETE FROM Appointments WHERE request_id = %s', (request_id,))
        cursor.execute('DELETE FROM Quotes WHERE request_id = %s', (request_id,))
        cursor.execute('DELETE FROM Requests WHERE id = %s', (request_id,))
        conn.commit()
        return jsonify({"status": "success", "description": "Talep silindi."})
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Talep silme hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/requests/<int:request_id>/quote', methods=['POST', 'PUT'])
@limiter.limit("30 per minute")
def manage_quote(request_id):
    if 'user_id' not in session:
        return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        user_id = session['user_id']
        user_type = session['user_type']
        data = request.get_json()

        if user_type == 'business':
            parts_cost = data.get('parts_cost')
            labor_cost = data.get('labor_cost')
            notes = data.get('notes', '')
            if not isinstance(parts_cost, (int, float)) or not isinstance(labor_cost, (int, float)):
                return jsonify({"description": "Maliyetler sayı olmalıdır."}), 400
            total_cost = parts_cost + labor_cost
            
            cursor.execute("SELECT status FROM Requests WHERE id = %s AND shop_user_id = %s", (request_id, user_id))
            req = cursor.fetchone()
            if not req: return jsonify({"description": "Yetkisiz işlem."}), 404
            
            new_status = 'negotiating' if req['status'] != 'pending' else 'quoted'

            if request.method == 'POST':
                cursor.execute(
                    "INSERT INTO Quotes (request_id, shop_user_id, parts_cost, labor_cost, total_cost, notes, last_offer_by) VALUES (%s, %s, %s, %s, %s, %s, 'business')",
                    (request_id, user_id, parts_cost, labor_cost, total_cost, notes)
                )
            elif request.method == 'PUT':
                cursor.execute(
                    "UPDATE Quotes SET parts_cost = %s, labor_cost = %s, total_cost = %s, notes = %s, last_offer_by = 'business', owner_proposed_cost = NULL WHERE request_id = %s",
                    (parts_cost, labor_cost, total_cost, notes, request_id)
                )
            
            cursor.execute("UPDATE Requests SET status = %s WHERE id = %s", (new_status, request_id))
            conn.commit()
            return jsonify({"status": "success", "description": "Teklif gönderildi."}), 201
        
        elif user_type == 'owner':
            if request.method != 'PUT': return jsonify({"description": "Geçersiz metod."}), 405
            proposed_cost = data.get('owner_proposed_cost')
            if not isinstance(proposed_cost, (int, float)) or proposed_cost <= 0:
                return jsonify({"description": "Geçerli bir karşı teklif girin."}), 400

            cursor.execute("SELECT user_id FROM Requests WHERE id = %s", (request_id,))
            req_owner = cursor.fetchone()
            if not req_owner or req_owner['user_id'] != user_id: return jsonify({"description": "Yetkisiz işlem."}), 403

            cursor.execute("UPDATE Quotes SET owner_proposed_cost = %s, last_offer_by = 'owner' WHERE request_id = %s", (proposed_cost, request_id))
            cursor.execute("UPDATE Requests SET status = 'negotiating' WHERE id = %s", (request_id,))
            conn.commit()
            return jsonify({"status": "success", "description": "Karşı teklif iletildi."})

    except psycopg2_errors.UniqueViolation:
        if conn: conn.rollback()
        return jsonify({"description": "Bu talep için daha önce bir teklif oluşturulmuş."}), 409
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Teklif yönetimi hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()
        
@app.route('/api/quotes/<int:quote_id>/reject', methods=['POST'])
@limiter.limit("30 per minute")
def reject_quote(quote_id):
    if 'user_id' not in session: return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        user_id = session['user_id']
        user_type = session['user_type']

        cursor.execute("SELECT q.request_id, r.user_id, r.shop_user_id FROM Quotes q JOIN Requests r ON q.request_id = r.id WHERE q.id = %s", (quote_id,))
        data = cursor.fetchone()
        if not data: return jsonify({"description": "Teklif bulunamadı."}), 404

        if (user_type == 'owner' and user_id == data['user_id']) or (user_type == 'business' and user_id == data['shop_user_id']):
            cursor.execute("UPDATE Requests SET status = 'rejected' WHERE id = %s", (data['request_id'],))
            cursor.execute("UPDATE Quotes SET status = 'rejected' WHERE id = %s", (quote_id,))
            conn.commit()
            return jsonify({"status": "success", "description": "Teklif reddedildi."})
        else:
            return jsonify({"description": "Yetkisiz işlem."}), 403
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Teklif reddetme hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/quotes/<int:quote_id>/accept', methods=['POST'])
@limiter.limit("20 per hour")
def accept_quote(quote_id):
    if 'user_id' not in session: return jsonify({"description": "Yetkilendirme gerekli."}), 403
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        user_id = session['user_id']
        user_type = session['user_type']

        query = "SELECT q.*, r.user_id as owner_user_id FROM Quotes q JOIN Requests r ON q.request_id = r.id WHERE q.id = %s"
        cursor.execute(query, (quote_id,))
        quote = cursor.fetchone()
        if not quote: return jsonify({"description": "Teklif bulunamadı."}), 404

        final_cost = 0
        
        if user_type == 'owner' and user_id == quote['owner_user_id']:
            if quote['last_offer_by'] != 'business': return jsonify({"description": "İşletmenin teklifini bekleyin."}), 400
            final_cost = quote['total_cost']
        elif user_type == 'business' and user_id == quote['shop_user_id']:
            if quote['last_offer_by'] != 'owner' or not quote['owner_proposed_cost']: return jsonify({"description": "Müşterinin karşı teklifi bekleniyor."}), 400
            final_cost = quote['owner_proposed_cost']
            # İşletme müşterinin fiyatını kabul ettiği için ana maliyeti güncelle
            cursor.execute("UPDATE Quotes SET total_cost = %s WHERE id = %s", (final_cost, quote_id))
        else:
            return jsonify({"description": "Yetkisiz işlem."}), 403

        cursor.execute("UPDATE Quotes SET status = 'accepted' WHERE id = %s", (quote_id,))
        cursor.execute("UPDATE Requests SET status = 'accepted' WHERE id = %s", (quote['request_id'],))
        cursor.execute(
            "INSERT INTO Appointments (request_id, quote_id, user_id, shop_user_id, status) VALUES (%s, %s, %s, %s, 'tarih_bekleniyor')",
            (quote['request_id'], quote_id, quote['owner_user_id'], quote['shop_user_id'])
        )
        conn.commit()
        return jsonify({"status": "success", "description": "Teklif kabul edildi ve randevu oluşturuldu."}), 201
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Teklif kabul etme hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/appointments', methods=['GET'])
@limiter.limit("60 per minute")
def get_appointments():
    if 'user_id' not in session:
        return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        user_id = session['user_id']
        user_type = session['user_type']

        if user_type == 'owner':
            query = """
                SELECT a.*, r.vehicle_brand, r.vehicle_model, r.selected_parts, 
                       COALESCE(s.google_place_name, u.name) as shop_name, 
                       COALESCE(s.google_place_phone, s.phone) as shop_phone,
                       s.google_place_id, s.google_place_last_updated,
                       s.google_place_url
                FROM Appointments a
                JOIN Requests r ON a.request_id = r.id
                JOIN Users u ON a.shop_user_id = u.id
                JOIN Shops s ON a.shop_user_id = s.user_id
                WHERE a.user_id = %s ORDER BY a.created_at DESC
            """
            cursor.execute(query, (user_id,))
        elif user_type == 'business':
            query = """
                SELECT a.*, r.vehicle_brand, r.vehicle_model, r.vehicle_km, r.selected_parts, v.plate_number as vehicle_plate, u.name as customer_name, u.phone_number as customer_phone
                FROM Appointments a
                JOIN Requests r ON a.request_id = r.id
                JOIN Users u ON a.user_id = u.id
                LEFT JOIN Vehicles v ON r.user_id = v.user_id AND r.vehicle_brand = v.brand AND r.vehicle_model = v.model
                WHERE a.shop_user_id = %s ORDER BY a.created_at DESC
            """
            cursor.execute(query, (user_id,))
        else:
            return jsonify([])
        
        appointments = []
        for row in cursor.fetchall():
            app_data = dict(row)
            if app_data.get('selected_parts'):
                app_data['selected_parts'] = json.loads(app_data['selected_parts'])
            
            if user_type == 'owner':
                place_id = app_data.get('google_place_id')
                last_updated = app_data.get('google_place_last_updated')
                is_cache_stale = not last_updated or (datetime.now(timezone.utc) - last_updated) > timedelta(days=7)

                if place_id and is_cache_stale:
                    logging.info(f"[get_appointments] '{app_data['shop_name']}' için eski/boş önbellek. Google API'den güncel veri çekiliyor.")
                    try:
                        url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=name,formatted_phone_number,url&key={GOOGLE_PLACES_API_KEY}&language=tr"
                        response = requests.get(url, timeout=5)
                        place_data = response.json()
                        if place_data.get("status") == "OK" and "result" in place_data:
                            result = place_data['result']
                            new_name = result.get('name')
                            new_phone = result.get('formatted_phone_number')
                            new_url = result.get('url')
                            
                            app_data['shop_name'] = new_name or app_data['shop_name']
                            app_data['shop_phone'] = new_phone or app_data['shop_phone']
                            app_data['google_place_url'] = new_url or app_data.get('google_place_url')
                            
                            with get_db_connection() as conn2:
                                with conn2.cursor() as update_cursor:
                                    update_cursor.execute(
                                        "UPDATE Shops SET google_place_name = %s, google_place_phone = %s, google_place_url = %s, google_place_last_updated = %s WHERE google_place_id = %s",
                                        (new_name, new_phone, new_url, datetime.now(timezone.utc), place_id)
                                    )
                                    conn2.commit()
                                    logging.info(f"[get_appointments] '{new_name}' için önbellek güncellendi.")
                    except Exception as e:
                        logging.error(f"[get_appointments] İhtiyaç anında önbellek güncelleme başarısız: {e}")

            appointments.append(app_data)
        
        return jsonify(appointments)

    except Exception as e:
        logging.error(f"Randevu listeleme hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/appointments/<int:appointment_id>', methods=['PUT', 'DELETE'])
@limiter.limit("30 per minute")
def manage_appointment(appointment_id):
    if 'user_id' not in session or session['user_type'] != 'business':
        return jsonify({"description": "Yetkisiz işlem."}), 403
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if request.method == 'PUT':
            data = request.get_json()
            appointment_date = data.get('appointment_date')
            if not appointment_date:
                return jsonify({"description": "Randevu tarihi gereklidir."}), 400
            
            cursor.execute("UPDATE Appointments SET appointment_date = %s, status = 'tarih_belirlendi' WHERE id = %s AND shop_user_id = %s",
                           (appointment_date, appointment_id, session['user_id']))
            conn.commit()
            if cursor.rowcount == 0:
                return jsonify({"description": "Randevu bulunamadı veya yetkiniz yok."}), 404
            return jsonify({"status": "success", "description": "Randevu tarihi güncellendi."})

        elif request.method == 'DELETE':
            cursor.execute("SELECT status FROM Appointments WHERE id = %s AND shop_user_id = %s", (appointment_id, session['user_id']))
            appointment = cursor.fetchone()
            if not appointment:
                return jsonify({"description": "Randevu bulunamadı veya yetkiniz yok."}), 404
            if appointment[0] != 'tamamlandi':
                return jsonify({"description": "Sadece tamamlanmış randevular silinebilir."}), 403

            cursor.execute("DELETE FROM Appointments WHERE id = %s", (appointment_id,))
            conn.commit()
            return jsonify({"status": "success", "description": "Randevu başarıyla silindi."})

    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Randevu yönetimi hatası: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/appointments/<int:appointment_id>/complete', methods=['POST'])
@limiter.limit("30 per minute")
def complete_appointment(appointment_id):
    if 'user_id' not in session or session['user_type'] != 'business':
        return jsonify({"description": "Yetkisiz işlem."}), 403
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE Appointments SET status = 'tamamlandi' WHERE id = %s AND shop_user_id = %s",
                       (appointment_id, session['user_id']))
        conn.commit()
        if cursor.rowcount == 0:
            return jsonify({"description": "Randevu bulunamadı veya yetkiniz yok."}), 404
        return jsonify({"status": "success", "description": "Randevu tamamlandı olarak işaretlendi."})
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Randevu tamamlama hatası: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/vehicles/<int:vehicle_id>/fuel_entries', methods=['GET', 'POST'])
@limiter.limit("60 per minute")
def manage_fuel_entries(vehicle_id):
    if 'user_id' not in session:
        return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute('SELECT id FROM Vehicles WHERE id = %s AND user_id = %s', (vehicle_id, session['user_id']))
        if not cursor.fetchone():
            return jsonify({"description": "Araç bulunamadı veya yetkiniz yok."}), 404
        
        if request.method == 'POST':
            data = request.get_json()
            if not all(data.get(field) for field in ['date', 'amount', 'unit', 'distance']):
                return jsonify({"description": "Tüm alanlar zorunludur."}), 400
            amount_tl = data.get('amount') if data.get('unit') == 'TL' else None
            amount_liter = data.get('amount') if data.get('unit') == 'Litre' else None
            cursor.execute(
                "INSERT INTO FuelEntries (user_id, vehicle_id, date, amount_tl, amount_liter, distance_km) VALUES (%s, %s, %s, %s, %s, %s)",
                (session['user_id'], vehicle_id, data['date'], amount_tl, amount_liter, data['distance'])
            )
            conn.commit()
            return jsonify({"status": "success", "description": "Yakıt verisi eklendi."}), 201
        
        if request.method == 'GET':
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            query = "SELECT * FROM FuelEntries WHERE vehicle_id = %s AND date BETWEEN %s AND %s ORDER BY date DESC"
            cursor.execute(query, (vehicle_id, start_date, end_date))
            entries = [dict(row) for row in cursor.fetchall()]
            total_tl = sum(e['amount_tl'] for e in entries if e['amount_tl'])
            total_liter = sum(e['amount_liter'] for e in entries if e['amount_liter'])
            total_km = sum(e['distance_km'] for e in entries if e['distance_km'])
            avg_consumption = (total_liter / total_km * 100) if total_liter > 0 and total_km > 0 else 0
            return jsonify({
                "entries": entries,
                "summary": {
                    "total_tl": total_tl,
                    "total_liter": total_liter,
                    "total_km": total_km,
                    "avg_consumption_liter_100km": avg_consumption
                }
            })
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Yakıt girişi yönetimi hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/find_shops')
@limiter.limit("60 per minute")
def find_shops():
    city = request.args.get('city')
    brand = request.args.get('brand')
    logging.info(f"[/api/find_shops] Arama başlatıldı. Şehir: {city}, Marka: {brand}")
    if not all([city, brand]):
        return jsonify({"description": "Şehir ve marka bilgisi gereklidir."}), 400
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = """
            SELECT u.id as shop_user_id, u.name as db_name, s.phone as db_phone, s.city, s.google_place_id,
                   s.google_place_name, s.google_place_phone, s.google_place_url, s.google_place_last_updated
            FROM Shops s JOIN Users u ON s.user_id = u.id
            WHERE s.city = %s AND s.serviced_brands LIKE %s
        """
        brand_search_term = f"%{brand}%"
        cursor.execute(query, (city, brand_search_term))
        shops = [dict(row) for row in cursor.fetchall()]
        logging.info(f"[/api/find_shops] Veritabanından {len(shops)} adet işletme bulundu.")
        
        for shop in shops:
            place_id = shop.get('google_place_id')
            last_updated = shop.get('google_place_last_updated')
            api_key = GOOGLE_PLACES_API_KEY
            is_cache_valid = False

            if last_updated:
                if (datetime.now(timezone.utc) - last_updated) < timedelta(days=7):
                    is_cache_valid = True

            if is_cache_valid:
                logging.info(f"[/api/find_shops] İşletme '{shop['db_name']}' için önbellekten veri kullanılıyor.")
                shop['name'] = shop['google_place_name'] or shop['db_name']
                shop['phone'] = shop['google_place_phone'] or shop['db_phone']
                shop['url'] = shop['google_place_url']
            
            elif place_id and api_key:
                logging.info(f"[/api/find_shops] İşletme '{shop['db_name']}' için Google Places API çağrısı yapılıyor (Önbellek geçersiz veya boş). Place ID: {place_id}")
                try:
                    url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=name,rating,user_ratings_total,reviews,formatted_phone_number,url&key={api_key}&language=tr"
                    response = requests.get(url, timeout=5)
                    place_data = response.json()
                    logging.info(f"[/api/find_shops] Google Places API Ham Yanıtı: {json.dumps(place_data, ensure_ascii=False)}")
                    
                    if place_data.get("status") == "OK" and "result" in place_data:
                        logging.info(f"[/api/find_shops] API yanıt durumu 'OK'. Veriler işleniyor.")
                        result = place_data['result']
                        
                        shop.update({
                            'name': result.get('name') or shop['db_name'],
                            'rating': result.get('rating', 0),
                            'user_ratings_total': result.get('user_ratings_total', 0),
                            'reviews': result.get('reviews', [])[:2],
                            'phone': result.get('formatted_phone_number') or shop['db_phone'],
                            'url': result.get('url')
                        })
                        
                        with get_db_connection() as conn2:
                            with conn2.cursor() as update_cursor:
                                update_cursor.execute(
                                    """
                                    UPDATE Shops SET google_place_name = %s, google_place_phone = %s, google_place_url = %s, google_place_last_updated = %s
                                    WHERE user_id = %s
                                    """,
                                    (result.get('name'), result.get('formatted_phone_number'), result.get('url'), datetime.now(timezone.utc), shop['shop_user_id'])
                                )
                                conn2.commit()
                        logging.info(f"[/api/find_shops] İşletme '{shop['name']}' için önbellek güncellendi.")
                    else:
                        logging.warning(f"[/api/find_shops] Google Places API'den beklenen yanıt alınamadı. Durum: {place_data.get('status')}, Hata Mesajı: {place_data.get('error_message')}. DB verileri kullanılacak.")
                        shop['name'] = shop['db_name']
                        shop['phone'] = shop['db_phone']

                except requests.exceptions.RequestException as e:
                    logging.error(f"[/api/find_shops] API hatası. İşletme '{shop['db_name']}' için DB/eski önbellek verisi kullanılıyor: {e}")
                    shop['name'] = shop['google_place_name'] or shop['db_name']
                    shop['phone'] = shop['google_place_phone'] or shop['db_phone']
            else:
                 logging.warning(f"[/api/find_shops] İşletme '{shop['db_name']}' için Google Place ID veya API Key eksik. Sadece DB verileri kullanılacak.")
                 shop['name'] = shop['db_name']
                 shop['phone'] = shop['db_phone']
                 
        return jsonify(shops)
    except Exception as e:
        logging.error(f"İşletme arama sırasında genel hata: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/shops', methods=['DELETE'])
@limiter.limit("10 per minute")
def delete_shop():
    if 'user_id' not in session:
        return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        if session['user_type'] != 'business':
            return jsonify({"description": "Yetkisiz işlem."}), 403
        cursor.execute('DELETE FROM Shops WHERE user_id = %s', (session['user_id'],))
        conn.commit()
        return jsonify({"status": "success", "description": "İşletme profili silindi."})
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Dükkan silinirken hata: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/vehicles', methods=['POST'])
@app.route('/api/vehicles/<int:vehicle_id>', methods=['PUT', 'DELETE'])
@limiter.limit("15 per minute")
def manage_vehicles(vehicle_id=None):
    if 'user_id' not in session:
        return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        user_id = session['user_id']
        if session['user_type'] != 'owner':
            return jsonify({"description": "Yetkisiz işlem."}), 403
        
        if request.method == 'POST':
            data = request.get_json()
            if not all(data.get(f) for f in ['plate_number', 'brand', 'series', 'year', 'fuel', 'model', 'last_inspection_date']):
                return jsonify({"description": "Tüm bilgiler zorunludur."}), 400
            if not validate_plate_number(data.get('plate_number')):
                return jsonify({"description": "Geçersiz plaka formatı."}), 400
            plate_number = format_plate_for_db(data.get('plate_number'))
            cursor.execute('SELECT id FROM Vehicles WHERE plate_number = %s', (plate_number,))
            if cursor.fetchone():
                return jsonify({"description": "Plaka zaten kayıtlı."}), 409
            cursor.execute(
                'INSERT INTO Vehicles (user_id, plate_number, brand, series, year, fuel, model, last_inspection_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                (user_id, plate_number, data['brand'], data['series'], data['year'], data['fuel'], data['model'], data['last_inspection_date'])
            )
            conn.commit()
            return jsonify({"status": "success", "description": "Araç eklendi."}), 201

        cursor.execute('SELECT id FROM Vehicles WHERE id = %s AND user_id = %s', (vehicle_id, user_id))
        if not cursor.fetchone():
            return jsonify({"description": "Araç bulunamadı."}), 404

        if request.method == 'PUT':
            data = request.get_json()
            if not validate_plate_number(data.get('plate_number')):
                return jsonify({"description": "Geçersiz plaka formatı."}), 400
            new_plate = format_plate_for_db(data.get('plate_number'))
            cursor.execute('SELECT id FROM Vehicles WHERE plate_number = %s AND id != %s', (new_plate, vehicle_id))
            if cursor.fetchone():
                return jsonify({"description": "Plaka başka araca ait."}), 409
            cursor.execute(
                'UPDATE Vehicles SET plate_number = %s, brand = %s, series = %s, year = %s, fuel = %s, model = %s, last_inspection_date = %s WHERE id = %s',
                (new_plate, data['brand'], data['series'], data['year'], data['fuel'], data['model'], data['last_inspection_date'], vehicle_id)
            )
            conn.commit()
            return jsonify({"status": "success", "description": "Araç güncellendi."})

        if request.method == 'DELETE':
            cursor.execute('DELETE FROM FuelEntries WHERE vehicle_id = %s', (vehicle_id,))
            cursor.execute('DELETE FROM Vehicles WHERE id = %s', (vehicle_id,))
            conn.commit()
            return jsonify({"status": "success", "description": "Araç silindi."})
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Araç yönetimi hatası: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/account', methods=['GET', 'POST'])
def account_details():
    if 'email' not in session:
        return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute('SELECT * FROM Users WHERE email = %s', (session['email'],))
        user = cursor.fetchone()
        if not user:
            return jsonify({"description": "Kullanıcı bulunamadı."}), 404
        user_data = dict(user)

        if request.method == 'GET':
            if user_data['user_type'] == 'owner':
                cursor.execute('SELECT id, plate_number, brand, series, year, model, fuel, tax_paid_jan, tax_paid_jul, last_inspection_date FROM Vehicles WHERE user_id = %s', (user['id'],))
                user_data['vehicles'] = [dict(row) for row in cursor.fetchall()]
            elif user_data['user_type'] == 'business':
                cursor.execute('SELECT city, phone as shop_phone, google_place_id, serviced_brands FROM Shops WHERE user_id = %s', (user['id'],))
                shop = cursor.fetchone()
                if shop:
                    user_data.update(dict(shop))
            for key in ['password_hash', 'google_id', 'id']:
                user_data.pop(key, None)
            return jsonify(user_data)

        if request.method == 'POST':
            data = request.get_json()
            
            if 'phone_number' in data and len(data) == 1:
                phone_number = data.get('phone_number')
                if phone_number and not validate_phone_number(phone_number):
                    return jsonify({"description": "Geçersiz kişisel telefon no."}), 400
                cursor.execute('UPDATE Users SET phone_number = %s WHERE id = %s', (phone_number, user['id']))
                conn.commit()
            
            elif user['user_type'] == 'business':
                serviced_brands_str = ",".join(data.get('serviced_brands', []))
                shop_phone = data.get('shop_phone')
                new_license_key = data.get('google_place_id')

                if shop_phone and not validate_phone_number(shop_phone):
                     return jsonify({"description": "Geçersiz işletme telefonu no."}), 400

                cursor.execute('SELECT id, google_place_id FROM Shops WHERE user_id = %s', (user['id'],))
                shop_data = cursor.fetchone()
                
                if shop_data: # İşletme var, güncelle
                    cursor.execute(
                        'UPDATE Shops SET city = %s, phone = %s, google_place_id = %s, serviced_brands = %s WHERE user_id = %s',
                        (data.get('city'), shop_phone, new_license_key, serviced_brands_str, user['id'])
                    )
                else: # İşletme yok, yeni oluştur
                    cursor.execute(
                        'INSERT INTO Shops (user_id, city, phone, google_place_id, serviced_brands) VALUES (%s, %s, %s, %s, %s)',
                        (user['id'], data.get('city'), shop_phone, new_license_key, serviced_brands_str)
                    )
                conn.commit() # Shops tablosunu kaydet
                
                # --- LİSANS TABLOSUNU GÜNCELLE ---
                if new_license_key:
                    cursor.execute("SELECT id FROM Shops WHERE user_id = %s", (user['id'],))
                    shop_id = cursor.fetchone()['id']
                    
                    try:
                        cursor.execute(
                            """
                            INSERT INTO Licenses (shop_id, license_key, is_active) 
                            VALUES (%s, %s, true)
                            ON CONFLICT (shop_id) DO UPDATE SET license_key = EXCLUDED.license_key;
                            """, (shop_id, new_license_key)
                        )
                        conn.commit()
                        logging.info(f"İşletme (Shop ID: {shop_id}) için lisans kaydı başarıyla oluşturuldu/güncellendi.")
                    except psycopg2_errors.UniqueViolation as e:
                        conn.rollback()
                        logging.error(f"Lisans anahtarı çakışması: {new_license_key} zaten kullanılıyor. {e}")
                        return jsonify({"description": f"Bu lisans anahtarı başka bir işletme tarafından kullanılıyor."}), 409

            return jsonify({"status": "success", "description": "Hesap güncellendi."})
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Hesap yönetimi hatası: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/vehicles/tax_status', methods=['POST'])
@limiter.limit("60 per minute")
def update_tax_status():
    if 'user_id' not in session:
        return jsonify({"description": "Yetkilendirme gerekli."}), 401
    data = request.get_json()
    vehicle_id, period, status = data.get('vehicle_id'), data.get('period'), data.get('status')
    if not all([vehicle_id, period, isinstance(status, bool)]) or period not in ['jan', 'jul']:
        return jsonify({"description": "Eksik veya geçersiz bilgi."}), 400
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute('SELECT id FROM Vehicles WHERE id = %s AND user_id = %s', (vehicle_id, session['user_id']))
        if not cursor.fetchone():
            return jsonify({"description": "Araç bulunamadı veya yetkiniz yok."}), 404
        column_to_update = f"tax_paid_{period}"
        cursor.execute(f'UPDATE Vehicles SET {column_to_update} = %s WHERE id = %s', (1 if status else 0, vehicle_id))
        conn.commit()
        return jsonify({"status": "success", "description": "Vergi durumu güncellendi."})
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Vergi durumu güncellenirken hata: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/cities')
def get_cities():
    try:
        with open(CITIES_DATA_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify(sorted([city['isim'] for city in data.get('sehirler', [])]))
    except Exception as e:
        logging.error(f"Şehir dosyası okunurken hata: {e}")
        return jsonify([]), 500

@app.route('/api/brands')
def get_brands():
    load_vehicle_data()
    return jsonify(sorted(list(set(item['marka'] for item in all_vehicle_data))))

@app.route('/api/series')
def get_series():
    load_vehicle_data()
    brand = request.args.get('brand')
    if not brand:
        return jsonify([])
    return jsonify(sorted(list(set(item['seri'] for item in all_vehicle_data if item['marka'] == brand))))

@app.route('/api/years')
def get_years():
    load_vehicle_data()
    brand, series = request.args.get('brand'), request.args.get('series')
    if not brand or not series:
        return jsonify([])
    return jsonify(sorted(list(set(item['yil'] for item in all_vehicle_data if item['marka'] == brand and item['seri'] == series))))

@app.route('/api/fuels')
def get_fuels():
    load_vehicle_data()
    brand, series, year = request.args.get('brand'), request.args.get('series'), request.args.get('year')
    if not all([brand, series, year]):
        return jsonify([])
    return jsonify(sorted(list(set(item['yakit'] for item in all_vehicle_data if item['marka'] == brand and item['seri'] == series and item['yil'] == year))))

@app.route('/api/models')
def get_models():
    load_vehicle_data()
    brand, series, year, fuel = request.args.get('brand'), request.args.get('series'), request.args.get('year'), request.args.get('fuel')
    if not all([brand, series, year, fuel]):
        return jsonify([])
    return jsonify(sorted(list(set(item['model'] for item in all_vehicle_data if item['marka'] == brand and item['seri'] == series and item['yil'] == year and item['yakit'] == fuel))))

@app.route('/api/maintenance_options')
@limiter.limit("60 per minute")
def get_maintenance_options():
    try:
        current_km = int(request.args.get('km'))
        fuel = request.args.get('fuel')
    except (ValueError, TypeError):
        return jsonify({"description": "Geçerli bir kilometre ve yakıt tipi gereklidir."}), 400
    if not fuel:
        return jsonify({"description": "Yakıt tipi gereklidir."}), 400
    file_path = DIZEL_MAINTENANCE_PATH if 'dizel' in fuel.lower() else BENZIN_MAINTENANCE_PATH
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            schedule_data = {int(k): v for k, v in json.load(f).items()}
        sorted_kms = sorted(schedule_data.keys())
        if not sorted_kms:
            return jsonify({"description": "Bakım verisi bulunamadı."}), 404
        cycle_km = sorted_kms[-1]
        base_km = math.floor(current_km / cycle_km) * cycle_km
        relative_km = current_km % cycle_km
        previous_km_point = 0
        for km_point in sorted_kms:
            if relative_km >= km_point:
                previous_km_point = km_point
            else:
                break
        
        question_km = base_km + previous_km_point
        
        if previous_km_point == 0:
            next_km_point_index = 0
        else:
            try:
                next_km_point_index = sorted_kms.index(previous_km_point) + 1
            except ValueError:
                next_km_point_index = 0
            
        if next_km_point_index < len(sorted_kms):
            next_service_km = base_km + sorted_kms[next_km_point_index]
            next_details = schedule_data.get(sorted_kms[next_km_point_index])
        else:
            next_service_km = base_km + cycle_km + sorted_kms[0]
            next_details = schedule_data.get(sorted_kms[0])
        return jsonify({
            "question_km": question_km if question_km > 0 else "ilk",
            "missed_service": {
                "km": question_km,
                "details": schedule_data.get(previous_km_point, None)
            },
            "next_service": {
                "km": next_service_km,
                "details": next_details
            }
        })
    except FileNotFoundError:
        return jsonify({"description": f"Bakım dosyası bulunamadı."}), 404
    except Exception as e:
        logging.error(f"{file_path} okunurken hata: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500

@app.route('/api/auth/google', methods=['POST'])
@limiter.limit("10 per minute")
def google_auth():
    token = request.json.get('token')
    conn = None
    try:
        idinfo = id_token.verify_oauth2_token(token, google_requests.Request(), GOOGLE_CLIENT_ID)
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute('SELECT * FROM Users WHERE email = %s', (idinfo['email'],))
        user = cursor.fetchone()
        if user:
            session.clear()
            session.update({'user_id': user['id'], 'email': user['email'], 'name': user['name'], 'user_type': user['user_type']})
            return jsonify({"status": "login_success", "userName": user['name'], "userType": user['user_type']})
        else:
            return jsonify({"status": "complete_profile", "email": idinfo['email'], "name": idinfo['name'], "google_id": idinfo['sub']})
    except Exception as e:
        logging.error(f"Google auth sırasında hata: {e}")
        return jsonify({"description": "Sunucu hatası veya geçersiz token."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/auth/register', methods=['POST'])
@limiter.limit("5 per minute")
def google_register_complete():
    data = request.get_json()
    email, name, google_id, user_type, phone_number = data.get('email'), data.get('name'), data.get('google_id'), data.get('user_type'), data.get('phone_number')
    if not all([email, name, google_id, user_type, phone_number]):
        return jsonify({"description": "Eksik bilgi."}), 400
    if not validate_phone_number(phone_number):
        return jsonify({"description": "Geçersiz telefon numarası formatı."}), 400
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(
            'INSERT INTO Users (google_id, email, name, user_type, phone_number) VALUES (%s, %s, %s, %s, %s) RETURNING id',
            (google_id, email, name, user_type, phone_number)
        )
        user_id = cursor.fetchone()['id']
        if user_type == 'business':
            cursor.execute('INSERT INTO Shops (user_id, phone) VALUES (%s, %s)', (user_id, phone_number))
        conn.commit()
        cursor.execute('SELECT * FROM Users WHERE id = %s', (user_id,))
        new_user = cursor.fetchone()
        session.clear()
        session.update({'user_id': new_user['id'], 'email': new_user['email'], 'name': new_user['name'], 'user_type': new_user['user_type']})
        try:
            send_welcome_email(new_user['name'], new_user['email'], new_user['user_type'])
        except Exception as email_error:
            logging.error(f"E-posta gönderme başarısız oldu, ancak kullanıcı kaydı başarılı: {email_error}")
        return jsonify({"status": "login_success", "userName": new_user['name'], "userType": new_user['user_type']}), 201
    except psycopg2_errors.UniqueViolation:
        if conn:
            conn.rollback()
        return jsonify({"description": "Bu e-posta adresi zaten kullanımda."}), 409
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Kayıt tamamlama sırasında kritik hata: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn:
            conn.close()

# --- Uygulama Başlangıcı ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False, load_dotenv=False)

