import os
import sqlite3
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
from datetime import datetime, timedelta
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
DATABASE_PATH = os.path.join(BASE_DIR, '..', '..', 'database', 'aracabak.db')
VEHICLE_DATA_PATH = os.path.join(BASE_DIR, '..', '..', 'database', 'tum_data.json')
DIZEL_MAINTENANCE_PATH = os.path.join(BASE_DIR, '..', '..', 'database', 'dizel_bakim_parcalari.json')
BENZIN_MAINTENANCE_PATH = os.path.join(BASE_DIR, '..', '..', 'database', 'benzin_bakim_parcalari.json')
CITIES_DATA_PATH = os.path.join(BASE_DIR, '..', '..', 'database', 'sehirler.json')
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY", "").strip()
all_vehicle_data = []

# --- Flask Uygulamasını Oluştur ve Yapılandır ---
app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", os.urandom(24))
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
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn
    
def init_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Users (
                id INTEGER PRIMARY KEY AUTOINCREMENT, google_id TEXT UNIQUE, email TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL, password_hash TEXT, user_type TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        add_column_if_not_exists(cursor, "Users", "phone_number", "TEXT")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Vehicles (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, plate_number TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, brand TEXT, series TEXT, year TEXT, fuel TEXT, model TEXT,
                last_inspection_date TEXT, tax_paid_jan INTEGER DEFAULT 0, tax_paid_jul INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES Users (id) ON DELETE CASCADE
            )
        ''')
        add_column_if_not_exists(cursor, "Vehicles", "tax_paid_jan", "INTEGER DEFAULT 0")
        add_column_if_not_exists(cursor, "Vehicles", "tax_paid_jul", "INTEGER DEFAULT 0")
        add_column_if_not_exists(cursor, "Vehicles", "last_inspection_date", "TEXT")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Shops (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL UNIQUE, city TEXT,
                phone TEXT, google_place_id TEXT,
                FOREIGN KEY (user_id) REFERENCES Users (id) ON DELETE CASCADE
            )
        ''')
        add_column_if_not_exists(cursor, "Shops", "serviced_brands", "TEXT")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                shop_user_id INTEGER NOT NULL,
                vehicle_brand TEXT, vehicle_series TEXT, vehicle_year TEXT,
                vehicle_fuel TEXT, vehicle_model TEXT, vehicle_km INTEGER,
                city TEXT, maintenance_km INTEGER, selected_parts TEXT,
                status TEXT DEFAULT 'pending', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                shop_google_place_id TEXT,
                FOREIGN KEY (user_id) REFERENCES Users (id),
                FOREIGN KEY (shop_user_id) REFERENCES Users (id)
            )
        ''')
        add_column_if_not_exists(cursor, "Requests", "status", "TEXT DEFAULT 'pending'")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL UNIQUE,
                shop_user_id INTEGER NOT NULL,
                parts_cost REAL NOT NULL,
                labor_cost REAL NOT NULL,
                total_cost REAL NOT NULL,
                notes TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (request_id) REFERENCES Requests (id),
                FOREIGN KEY (shop_user_id) REFERENCES Users (id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS FuelEntries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                vehicle_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                amount_tl REAL,
                amount_liter REAL,
                distance_km REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES Users (id) ON DELETE CASCADE,
                FOREIGN KEY (vehicle_id) REFERENCES Vehicles (id) ON DELETE CASCADE
            )
        ''')
        conn.commit()
        logging.info("Veritabanı başarıyla kontrol edildi.")
    except Exception as e:
        logging.error(f"Veritabanı başlatma hatası: {e}")
    finally:
        if 'conn' in locals() and conn: conn.close()

def add_column_if_not_exists(cursor, table_name, column_name, column_def):
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    if column_name not in columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
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

def send_welcome_email(user_name, user_email):
    if not BREVO_API_KEY:
        logging.error("Brevo API anahtarı bulunamadı. E-posta gönderilemiyor.")
        return
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = BREVO_API_KEY
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
    subject = f"Aramıza Hoş Geldin, {user_name}!"
    html_content = f"""
    <!DOCTYPE html>
    <html lang="tr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{ margin: 0; padding: 0; font-family: Arial, sans-serif; background-color: #f4f4f4; }}
            .container {{ max-width: 600px; margin: 20px auto; background-color: #ffffff; padding: 30px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
            h1 {{ color: #333; }}
            p {{ color: #555; line-height: 1.6; }}
            ul {{ color: #555; padding-left: 20px; }}
            .footer {{ margin-top: 20px; text-align: center; color: #999; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Merhaba {user_name},</h1>
            <p>aracabak.com'a başarıyla kaydoldunuz.</p>
            <p><b>aracabak</b>, tüm iyi araç sahiplerinin ve iyi bakım merkezlerinin buluşma noktasıdır.</p>
            <p>Bu sitede yapabilecekleriniz:</p>
            <ul>
                <li>Aracınızı kaydedebilir,</li>
                <li>Periyodik bakım detaylarını öğrenebilir,</li>
                <li>Anlaşmalı servislerden bakım teklifleri alabilirsiniz.</li>
            </ul>
            <p>Hoş geldiniz,<br><b>aracabak Ekibi</b></p>
        </div>
        <div class="footer">
            <p>&reg;vovDigital.</p>
        </div>
    </body>
    </html>
    """
    sender = {"name":"aracabak","email":"info@aracabak.com"}
    to = [{"email":user_email,"name":user_name}]
    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=to, html_content=html_content, sender=sender, subject=subject)
    try:
        api_instance.send_transac_email(send_smtp_email)
        logging.info(f"Hoş geldin e-postası başarıyla gönderildi: {user_email}")
    except ApiException as e:
        logging.error(f"Brevo API hatası: E-posta gönderilemedi ({user_email}). Hata Kodu: {e.status}, Hata Sebebi: {e.reason}")
        logging.error(f"Brevo API Hata Detayı: {e.body}")

with app.app_context():
    init_db()
    
# --- API Endpoint'leri ---
@app.route('/api/auth/status')
def auth_status():
    if 'user_id' in session:
        return jsonify({
            "loggedIn": True,
            "userName": session.get('name'),
            "userType": session.get('user_type'),
            "email": session.get('email')
        })
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
@limiter.limit("10 per hour")
def get_fuel_prices():
    try:
        url = "https://apisepeti.com/wp-json/petrol/v1/fiyatlar"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return jsonify(data)
    except requests.exceptions.RequestException as e:
        logging.error(f"Harici yakıt API'sine ulaşılamadı: {e}")
        return jsonify({"description": "Yakıt fiyatları servisine şu anda ulaşılamıyor."}), 503
    except Exception as e:
        logging.error(f"Yakıt fiyatları alınırken beklenmedik bir hata oluştu: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500

@app.route('/api/requests', methods=['GET', 'POST'])
@app.route('/api/requests/<int:request_id>', methods=['PUT', 'DELETE'])
@limiter.limit("30 per minute")
def manage_requests(request_id=None):
    if 'user_id' not in session: return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = get_db_connection()
    try:
        user_id = session['user_id']
        user_type = session['user_type']

        if request.method == 'GET':
            if user_type == 'business':
                query = "SELECT r.*, u.name as customer_name, u.phone_number as customer_phone FROM Requests r JOIN Users u ON r.user_id = u.id WHERE r.shop_user_id = ? ORDER BY r.created_at DESC"
                requests_cursor = conn.execute(query, (user_id,))
            elif user_type == 'owner':
                query = "SELECT r.*, u.name as shop_name, s.phone as shop_phone, r.shop_google_place_id FROM Requests r JOIN Users u ON r.shop_user_id = u.id LEFT JOIN Shops s ON r.shop_user_id = s.user_id WHERE r.user_id = ? ORDER BY r.created_at DESC"
                requests_cursor = conn.execute(query, (user_id,))
            else:
                return jsonify([])
            
            requests_list = [dict(row) for row in requests_cursor.fetchall()]
            for req in requests_list:
                if req.get('selected_parts'):
                    req['selected_parts'] = json.loads(req['selected_parts'])
                
                if user_type == 'owner' and req.get('shop_google_place_id') and GOOGLE_PLACES_API_KEY:
                    try:
                        place_id = req['shop_google_place_id']
                        fields = "name,formatted_phone_number"
                        url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields={fields}&key={GOOGLE_PLACES_API_KEY}&language=tr"
                        response = requests.get(url, timeout=5)
                        place_data = response.json()
                        if place_data.get("status") == "OK" and "result" in place_data:
                            result = place_data['result']
                            req['shop_name'] = result.get('name', req.get('shop_name'))
                            req['shop_phone'] = result.get('formatted_phone_number', req.get('shop_phone'))
                    except requests.exceptions.RequestException as e:
                        logging.error(f"Google Places API isteği (talep listesi için) başarısız oldu: {e}")

            return jsonify(requests_list)
        
        data = request.get_json()
        if request.method == 'POST':
            required_fields = ['shop_user_id', 'shop_google_place_id', 'vehicle', 'maintenance_km', 'selected_parts', 'city']
            if not all(field in data for field in required_fields):
                return jsonify({"description": "Eksik bilgi."}), 400
            vehicle = data['vehicle']
            selected_parts_json = json.dumps(data['selected_parts'])
            conn.execute(
                "INSERT INTO Requests (user_id, shop_user_id, shop_google_place_id, vehicle_brand, vehicle_series, vehicle_year, vehicle_fuel, vehicle_model, vehicle_km, city, maintenance_km, selected_parts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (user_id, data['shop_user_id'], data['shop_google_place_id'], vehicle['brand'], vehicle['series'], vehicle['year'], vehicle['fuel'], vehicle['model'], vehicle['km'], data['city'], data['maintenance_km'], selected_parts_json)
            )
            conn.commit()
            return jsonify({"status": "success", "description": "Talep iletildi."}), 201

        if request.method == 'PUT':
            if user_type != 'owner': return jsonify({"description": "Yetkisiz işlem."}), 403
            req_to_update = conn.execute('SELECT id FROM Requests WHERE id = ? AND user_id = ?', (request_id, user_id)).fetchone()
            if not req_to_update: return jsonify({"description": "Talep bulunamadı veya yetkiniz yok."}), 404
            conn.execute(
                "UPDATE Requests SET vehicle_km = ?, selected_parts = ? WHERE id = ?",
                (data.get('vehicle_km'), json.dumps(data.get('selected_parts')), request_id)
            )
            conn.commit()
            return jsonify({"status": "success", "description": "Talep güncellendi."})

        elif request.method == 'DELETE':
            if user_type == 'business':
                req_to_delete = conn.execute('SELECT id FROM Requests WHERE id = ? AND shop_user_id = ?', (request_id, user_id)).fetchone()
            elif user_type == 'owner':
                req_to_delete = conn.execute('SELECT id FROM Requests WHERE id = ? AND user_id = ?', (request_id, user_id)).fetchone()
            else:
                req_to_delete = None
            if not req_to_delete: return jsonify({"description": "Talep bulunamadı veya silme yetkiniz yok."}), 404
            conn.execute('DELETE FROM Requests WHERE id = ?', (request_id,))
            conn.commit()
            return jsonify({"status": "success", "description": "Talep silindi."})
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Talep yönetimi hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/requests/<int:request_id>/quote', methods=['POST'])
@limiter.limit("30 per minute")
def submit_quote(request_id):
    if 'user_id' not in session or session.get('user_type') != 'business':
        return jsonify({"description": "Yetkisiz işlem."}), 403

    data = request.get_json()
    parts_cost = data.get('parts_cost')
    labor_cost = data.get('labor_cost')
    notes = data.get('notes', '')

    if not isinstance(parts_cost, (int, float)) or not isinstance(labor_cost, (int, float)):
        return jsonify({"description": "Parça ve işçilik maliyetleri sayı olmalıdır."}), 400

    total_cost = parts_cost + labor_cost
    shop_user_id = session['user_id']

    conn = get_db_connection()
    try:
        request_to_quote = conn.execute(
            "SELECT id, status FROM Requests WHERE id = ? AND shop_user_id = ?",
            (request_id, shop_user_id)
        ).fetchone()

        if not request_to_quote:
            return jsonify({"description": "Talep bulunamadı veya bu talebe teklif verme yetkiniz yok."}), 404
        
        if request_to_quote['status'] != 'pending':
            return jsonify({"description": "Bu talebe zaten teklif verilmiş veya işlem yapılmış."}), 409
        
        conn.execute(
            "INSERT INTO Quotes (request_id, shop_user_id, parts_cost, labor_cost, total_cost, notes) VALUES (?, ?, ?, ?, ?, ?)",
            (request_id, shop_user_id, parts_cost, labor_cost, total_cost, notes)
        )
        
        conn.execute("UPDATE Requests SET status = 'quoted' WHERE id = ?", (request_id,))
        
        conn.commit()
        logging.info(f"İşletme {shop_user_id}, talep {request_id} için teklif gönderdi.")
        
        # TODO: Araç sahibine e-posta ile bildirim gönderilebilir.

        return jsonify({"status": "success", "description": "Teklif başarıyla gönderildi."}), 201

    except sqlite3.IntegrityError:
        if conn: conn.rollback()
        return jsonify({"description": "Bu talep için daha önce bir teklif oluşturulmuş."}), 409
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Teklif oluşturma hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()
        
@app.route('/api/vehicles/<int:vehicle_id>/fuel_entries', methods=['GET', 'POST'])
@limiter.limit("60 per minute")
def manage_fuel_entries(vehicle_id):
    if 'user_id' not in session:
        return jsonify({"description": "Yetkilendirme gerekli."}), 401
    
    conn = get_db_connection()
    try:
        vehicle = conn.execute('SELECT id FROM Vehicles WHERE id = ? AND user_id = ?', (vehicle_id, session['user_id'])).fetchone()
        if not vehicle:
            return jsonify({"description": "Araç bulunamadı veya yetkiniz yok."}), 404

        if request.method == 'POST':
            data = request.get_json()
            if not all(data.get(field) for field in ['date', 'amount', 'unit', 'distance']):
                return jsonify({"description": "Tüm alanlar zorunludur."}), 400
            
            amount_tl = data.get('amount') if data.get('unit') == 'TL' else None
            amount_liter = data.get('amount') if data.get('unit') == 'Litre' else None

            conn.execute(
                "INSERT INTO FuelEntries (user_id, vehicle_id, date, amount_tl, amount_liter, distance_km) VALUES (?, ?, ?, ?, ?, ?)",
                (session['user_id'], vehicle_id, data['date'], amount_tl, amount_liter, data['distance'])
            )
            conn.commit()
            return jsonify({"status": "success", "description": "Yakıt verisi eklendi."}), 201

        if request.method == 'GET':
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')

            query = "SELECT * FROM FuelEntries WHERE vehicle_id = ? AND date BETWEEN ? AND ? ORDER BY date DESC"
            entries_cursor = conn.execute(query, (vehicle_id, start_date, end_date))
            entries = [dict(row) for row in entries_cursor.fetchall()]
            
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
        if conn: conn.rollback()
        logging.error(f"Yakıt girişi yönetimi hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/find_shops')
@limiter.limit("60 per minute")
def find_shops():
    city = request.args.get('city')
    brand = request.args.get('brand')
    if not all([city, brand]):
        return jsonify({"description": "Şehir ve marka bilgisi gereklidir."}), 400
    conn = get_db_connection()
    try:
        query = "SELECT u.id as shop_user_id, u.name, s.phone, s.city, s.google_place_id FROM Shops s JOIN Users u ON s.user_id = u.id WHERE s.city = ? AND s.serviced_brands LIKE ?"
        brand_search_term = f"%{brand}%"
        shops_cursor = conn.execute(query, (city, brand_search_term))
        shops = [dict(row) for row in shops_cursor.fetchall()]
        for shop in shops:
            if shop.get('google_place_id') and GOOGLE_PLACES_API_KEY:
                try:
                    place_id = shop['google_place_id']
                    fields = "name,rating,user_ratings_total,reviews,formatted_phone_number,url"
                    url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields={fields}&key={GOOGLE_PLACES_API_KEY}&language=tr"
                    response = requests.get(url, timeout=5)
                    place_data = response.json()
                    if place_data.get("status") == "OK" and "result" in place_data:
                        result = place_data['result']
                        shop['name'] = result.get('name', shop.get('name'))
                        shop['rating'] = result.get('rating', 0)
                        shop['user_ratings_total'] = result.get('user_ratings_total', 0)
                        shop['reviews'] = result.get('reviews', [])[:2]
                        shop['formatted_phone_number'] = result.get('formatted_phone_number', shop.get('phone'))
                        shop['url'] = result.get('url')
                except requests.exceptions.RequestException as e:
                    logging.error(f"Google Places API isteği başarısız oldu (Place ID: {place_id}): {e}")
        return jsonify(shops)
    except Exception as e:
        logging.error(f"İşletme arama sırasında hata: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/shops', methods=['DELETE'])
@limiter.limit("10 per minute")
def delete_shop():
    if 'user_id' not in session: return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = get_db_connection()
    try:
        user_id = session['user_id']
        if session['user_type'] != 'business':
            return jsonify({"description": "Yetkisiz işlem."}), 403
        conn.execute('DELETE FROM Shops WHERE user_id = ?', (user_id,))
        conn.commit()
        return jsonify({"status": "success", "description": "İşletme profili silindi."})
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Dükkan silinirken hata: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/vehicles', methods=['POST'])
@app.route('/api/vehicles/<int:vehicle_id>', methods=['PUT', 'DELETE'])
@limiter.limit("15 per minute")
def manage_vehicles(vehicle_id=None):
    if 'user_id' not in session: return jsonify({"description": "Yetkilendirme gerekli."}), 401
    conn = get_db_connection()
    try:
        user_id = session['user_id']
        if session['user_type'] != 'owner':
            return jsonify({"description": "Yetkisiz işlem."}), 403
        data = request.get_json()
        if request.method == 'POST':
            if not all(data.get(field) for field in ['plate_number', 'brand', 'series', 'year', 'fuel', 'model', 'last_inspection_date']):
                return jsonify({"description": "Tüm bilgiler zorunludur."}), 400
            plate_number_raw = data.get('plate_number')
            if not validate_plate_number(plate_number_raw):
                return jsonify({"description": "Geçersiz plaka formatı."}), 400
            plate_number = format_plate_for_db(plate_number_raw)
            existing_plate = conn.execute('SELECT id FROM Vehicles WHERE plate_number = ?', (plate_number,)).fetchone()
            if existing_plate: return jsonify({"description": "Plaka zaten kayıtlı."}), 409
            conn.execute(
                'INSERT INTO Vehicles (user_id, plate_number, brand, series, year, fuel, model, last_inspection_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (user_id, plate_number, data['brand'], data['series'], data['year'], data['fuel'], data['model'], data['last_inspection_date'])
            )
            conn.commit()
            return jsonify({"status": "success", "description": "Araç eklendi."}), 201
        elif request.method == 'PUT':
            vehicle = conn.execute('SELECT id FROM Vehicles WHERE id = ? AND user_id = ?', (vehicle_id, user_id)).fetchone()
            if not vehicle: return jsonify({"description": "Araç bulunamadı."}), 404
            new_plate_raw = data.get('plate_number')
            if not validate_plate_number(new_plate_raw): return jsonify({"description": "Geçersiz plaka formatı."}), 400
            new_plate = format_plate_for_db(new_plate_raw)
            existing_plate = conn.execute('SELECT id FROM Vehicles WHERE plate_number = ? AND id != ?', (new_plate, vehicle_id)).fetchone()
            if existing_plate: return jsonify({"description": "Plaka başka araca ait."}), 409
            conn.execute(
                'UPDATE Vehicles SET plate_number = ?, brand = ?, series = ?, year = ?, fuel = ?, model = ?, last_inspection_date = ? WHERE id = ?',
                (new_plate, data['brand'], data['series'], data['year'], data['fuel'], data['model'], data['last_inspection_date'], vehicle_id)
            )
            conn.commit()
            return jsonify({"status": "success", "description": "Araç güncellendi."})
        elif request.method == 'DELETE':
            vehicle = conn.execute('SELECT id FROM Vehicles WHERE id = ? AND user_id = ?', (vehicle_id, user_id)).fetchone()
            if not vehicle: return jsonify({"description": "Araç bulunamadı."}), 404
            conn.execute('DELETE FROM Vehicles WHERE id = ?', (vehicle_id,))
            conn.commit()
            return jsonify({"status": "success", "description": "Araç silindi."})
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Araç yönetimi hatası: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/account', methods=['GET','POST'])
def account_details():
    if 'email' not in session: return jsonify({"description": "Yetkilendirme gerekli."}), 401
    email = session['email']
    conn = get_db_connection()
    try:
        user = conn.execute('SELECT * FROM Users WHERE email = ?', (email,)).fetchone()
        if not user: return jsonify({"description": "Kullanıcı bulunamadı."}), 404
        user_data = dict(user)
        if request.method == 'GET':
            if user_data['user_type'] == 'owner':
                vehicles_cursor = conn.execute('SELECT id, plate_number, brand, series, year, model, fuel, tax_paid_jan, tax_paid_jul, last_inspection_date FROM Vehicles WHERE user_id = ?', (user['id'],))
                user_data['vehicles'] = [dict(row) for row in vehicles_cursor.fetchall()]
            elif user_data['user_type'] == 'business':
                shop = conn.execute('SELECT city, phone, google_place_id, serviced_brands FROM Shops WHERE user_id = ?', (user['id'],)).fetchone()
                if shop:
                    user_data.update(dict(shop))
            for key in ['password_hash', 'google_id', 'id']: user_data.pop(key, None)
            return jsonify(user_data), 200
        
        if request.method == 'POST':
            data = request.get_json()
            phone_number = data.get('phone_number')
            if not phone_number or not validate_phone_number(phone_number): 
                return jsonify({"description": "Geçersiz telefon no."}), 400
            conn.execute('UPDATE Users SET phone_number = ? WHERE id = ?', (phone_number, user['id']))
            if user['user_type'] == 'business':
                serviced_brands_str = ",".join(data.get('serviced_brands', []))
                shop = conn.execute('SELECT id FROM Shops WHERE user_id = ?', (user['id'],)).fetchone()
                if shop:
                    conn.execute(
                        'UPDATE Shops SET city = ?, phone = ?, google_place_id = ?, serviced_brands = ? WHERE user_id = ?',
                        (data.get('city'), data.get('shop_phone'), data.get('google_place_id'), serviced_brands_str, user['id'])
                    )
                else:
                    conn.execute(
                        'INSERT INTO Shops (user_id, city, phone, google_place_id, serviced_brands) VALUES (?, ?, ?, ?, ?)',
                        (user['id'], data.get('city'), data.get('shop_phone'), data.get('google_place_id'), serviced_brands_str)
                    )
            conn.commit()
            return jsonify({"status": "success", "description": "Hesap güncellendi."}), 200
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Hesap yönetimi hatası: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/vehicles/tax_status', methods=['POST'])
@limiter.limit("60 per minute")
def update_tax_status():
    if 'user_id' not in session: return jsonify({"description": "Yetkilendirme gerekli."}), 401
    data = request.get_json()
    vehicle_id = data.get('vehicle_id')
    period = data.get('period')
    status = data.get('status')
    if not all([vehicle_id, period, isinstance(status, bool)]) or period not in ['jan', 'jul']:
        return jsonify({"description": "Eksik veya geçersiz bilgi."}), 400
    conn = get_db_connection()
    try:
        vehicle = conn.execute('SELECT id FROM Vehicles WHERE id = ? AND user_id = ?', (vehicle_id, session['user_id'])).fetchone()
        if not vehicle: return jsonify({"description": "Araç bulunamadı veya yetkiniz yok."}), 404
        column_to_update = f"tax_paid_{period}"
        status_int = 1 if status else 0
        conn.execute(f'UPDATE Vehicles SET {column_to_update} = ? WHERE id = ?', (status_int, vehicle_id))
        conn.commit()
        return jsonify({"status": "success", "description": "Vergi durumu güncellendi."})
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Vergi durumu güncellenirken hata: {e}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()
            
@app.route('/api/cities')
def get_cities():
    try:
        with open(CITIES_DATA_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        city_names = [city['isim'] for city in data.get('sehirler', [])]
        return jsonify(sorted(city_names))
    except Exception as e:
        logging.error(f"Şehir dosyası okunurken hata: {e}")
        return jsonify([]), 500

@app.route('/api/brands')
def get_brands():
    load_vehicle_data()
    brands = sorted(list(set(item['marka'] for item in all_vehicle_data)))
    return jsonify(brands)
    
@app.route('/api/series')
def get_series():
    load_vehicle_data()
    brand = request.args.get('brand')
    if not brand: return jsonify([])
    series = sorted(list(set(item['seri'] for item in all_vehicle_data if item['marka'] == brand)))
    return jsonify(series)

@app.route('/api/years')
def get_years():
    load_vehicle_data()
    brand = request.args.get('brand')
    series = request.args.get('series')
    if not brand or not series: return jsonify([])
    years = sorted(list(set(item['yil'] for item in all_vehicle_data if item['marka'] == brand and item['seri'] == series)))
    return jsonify(years)

@app.route('/api/fuels')
def get_fuels():
    load_vehicle_data()
    brand = request.args.get('brand')
    series = request.args.get('series')
    year = request.args.get('year')
    if not all([brand, series, year]): return jsonify([])
    fuels = sorted(list(set(item['yakit'] for item in all_vehicle_data if item['marka'] == brand and item['seri'] == series and item['yil'] == year)))
    return jsonify(fuels)
    
@app.route('/api/models')
def get_models():
    load_vehicle_data()
    brand = request.args.get('brand')
    series = request.args.get('series')
    year = request.args.get('year')
    fuel = request.args.get('fuel')
    if not all([brand, series, year, fuel]): return jsonify([])
    models = sorted(list(set(item['model'] for item in all_vehicle_data if item['marka'] == brand and item['seri'] == series and item['yil'] == year and item['yakit'] == fuel)))
    return jsonify(models)

@app.route('/api/maintenance_options')
@limiter.limit("60 per minute")
def get_maintenance_options():
    fuel = request.args.get('fuel')
    try:
        current_km = int(request.args.get('km'))
    except (ValueError, TypeError):
        return jsonify({"description": "Geçerli bir kilometre gereklidir."}), 400
    if not fuel:
        return jsonify({"description": "Yakıt tipi gereklidir."}), 400
    file_path = DIZEL_MAINTENANCE_PATH if 'dizel' in fuel.lower() else BENZIN_MAINTENANCE_PATH
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            schedule_data = {int(k): v for k, v in json.load(f).items()}
        sorted_kms = sorted(schedule_data.keys())
        cycle_km = sorted_kms[-1] if sorted_kms else 120000
        base_km = math.floor(current_km / cycle_km) * cycle_km
        relative_km = current_km % cycle_km
        previous_km_point = 0
        next_km_point = sorted_kms[0]
        for km_point in sorted_kms:
            if relative_km >= km_point:
                previous_km_point = km_point
            else:
                next_km_point = km_point
                break
        if relative_km == previous_km_point and previous_km_point != 0:
            current_index = sorted_kms.index(previous_km_point)
            if current_index > 0:
                 previous_km_point = sorted_kms[current_index - 1]
        question_km = base_km + previous_km_point
        missed_service_km = question_km
        next_service_km = base_km + next_km_point
        if relative_km >= next_km_point and next_km_point != sorted_kms[0]:
             next_service_km = base_km + cycle_km + sorted_kms[0]
        return jsonify({
            "question_km": question_km if question_km > 0 else "ilk",
            "missed_service": { "km": missed_service_km, "details": schedule_data.get(previous_km_point, None) },
            "next_service": { "km": next_service_km, "details": schedule_data.get(next_km_point, None) }
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
    try:
        idinfo = id_token.verify_oauth2_token(token, google_requests.Request(), GOOGLE_CLIENT_ID)
        conn = get_db_connection()
        user = conn.execute('SELECT * FROM Users WHERE email = ?', (idinfo['email'],)).fetchone()
        conn.close()
        if user:
            session.clear()
            session['user_id'] = user['id']
            session['email'] = user['email']
            session['name'] = user['name']
            session['user_type'] = user['user_type']
            return jsonify({"status": "login_success", "userName": user['name'], "userType": user['user_type']}), 200
        else:
            return jsonify({"status": "complete_profile", "email": idinfo['email'], "name": idinfo['name'], "google_id": idinfo['sub']}), 200
    except Exception as e:
        logging.error(f"Google auth sırasında hata: {e}")
        return jsonify({"description": "Sunucu hatası veya geçersiz token."}), 500

@app.route('/api/auth/register', methods=['POST'])
@limiter.limit("5 per minute")
def google_register_complete():
    data = request.get_json()
    email = data.get('email')
    name = data.get('name')
    google_id = data.get('google_id')
    user_type = data.get('user_type')
    phone_number = data.get('phone_number')
    if not all([email, name, google_id, user_type, phone_number]):
        return jsonify({"description": "Eksik bilgi."}), 400
    if not validate_phone_number(phone_number):
        return jsonify({"description": "Geçersiz telefon numarası formatı."}), 400
    conn = get_db_connection()
    try:
        existing_user = conn.execute('SELECT id FROM Users WHERE email = ?', (email,)).fetchone()
        if existing_user:
            return jsonify({"description": "Bu e-posta adresi zaten kullanımda."}), 409
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO Users (google_id, email, name, user_type, phone_number) VALUES (?, ?, ?, ?, ?)',
            (google_id, email, name, user_type, phone_number)
        )
        user_id = cursor.lastrowid
        if user_type == 'business':
            cursor.execute('INSERT INTO Shops (user_id, phone) VALUES (?, ?)',(user_id, phone_number))
        conn.commit()
        new_user = conn.execute('SELECT * FROM Users WHERE id = ?', (user_id,)).fetchone()
        session.clear()
        session['user_id'] = new_user['id']
        session['email'] = new_user['email']
        session['name'] = new_user['name']
        session['user_type'] = new_user['user_type']
        try:
            send_welcome_email(new_user['name'], new_user['email'])
        except Exception as email_error:
            logging.error(f"E-posta gönderme başarısız oldu, ancak kullanıcı kaydı başarılı: {email_error}")
        return jsonify({"status": "login_success", "userName": new_user['name'], "userType": new_user['user_type']}), 201
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Kayıt tamamlama sırasında kritik hata: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu hatası."}), 500
    finally:
        if conn: conn.close()
        
# --- Uygulama Başlangıcı ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False, load_dotenv=False)
