import os
import logging
import traceback
from functools import wraps
from flask import Blueprint, jsonify, request, session
from datetime import datetime, timedelta
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from werkzeug.security import check_password_hash
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

dotenv_path = '/var/www/aracabak.com/private/secrets/.env'
load_dotenv(dotenv_path=dotenv_path)

admin_bp = Blueprint('admin_api', __name__)

GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')
COST_PER_INPUT_TOKEN = 0.20 / 1_000_000
COST_PER_OUTPUT_TOKEN = 0.60 / 1_000_000

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
        raise

def admin_only(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return jsonify({"description": "Bu kaynağa erişim yetkiniz yok."}), 403
        return f(*args, **kwargs)
    return decorated_function

@admin_bp.route('/api/admin/login', methods=['POST'])
def admin_login():
    data = request.json
    google_token, password = data.get('token'), data.get('password')
    if not google_token or not password:
        return jsonify({"description": "Token ve şifre gerekli."}), 400
    conn = None
    try:
        idinfo = id_token.verify_oauth2_token(google_token, google_requests.Request(), GOOGLE_CLIENT_ID)
        admin_email = idinfo['email']
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute('SELECT * FROM Admins WHERE email = %s', (admin_email,))
        admin_user = cursor.fetchone()
        cursor.close()
        if not admin_user:
            return jsonify({"description": "Bu hesap admin yetkisine sahip değil."}), 403
        if check_password_hash(admin_user['password_hash'], password):
            session.clear()
            session.update(admin_logged_in=True, admin_email=admin_user['email'], name=idinfo.get('name', 'Admin'))
            return jsonify({"status": "admin_login_success"})
        else:
            return jsonify({"description": "Geçersiz admin şifresi."}), 401
    except Exception as e:
        logging.error(f"Admin giriş hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Sunucu tarafında bir hata oluştu."}), 500
    finally:
        if conn: conn.close()

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
        if conn: conn.close()

@admin_bp.route('/api/admin/ai_stats')
@admin_only
def get_ai_stats():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        where_clauses, params = [], []
        if start_date_str:
            where_clauses.append("created_at >= %s")
            params.append(start_date_str)
        if end_date_str:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1)
            where_clauses.append("created_at < %s")
            params.append(end_date.strftime('%Y-%m-%d'))
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        summary_query = f"SELECT COUNT(*) AS total_requests, COUNT(DISTINCT user_identifier) AS unique_users, COALESCE(SUM(prompt_tokens), 0) AS total_prompt_tokens, COALESCE(SUM(response_tokens), 0) AS total_response_tokens FROM YapayUstaUsage {where_sql};"
        cursor.execute(summary_query, tuple(params))
        summary_data = cursor.fetchone()
        total_prompt_tokens = summary_data['total_prompt_tokens']
        total_response_tokens = summary_data['total_response_tokens']
        estimated_cost = (total_prompt_tokens * COST_PER_INPUT_TOKEN) + (total_response_tokens * COST_PER_OUTPUT_TOKEN)
        summary = {"total_requests": summary_data['total_requests'], "unique_users": summary_data['unique_users'], "total_tokens": total_prompt_tokens + total_response_tokens, "estimated_cost": round(estimated_cost, 4)}
        time_series_query = f"SELECT DATE_TRUNC('day', created_at) AS period, COUNT(*) as count, COALESCE(SUM(prompt_tokens), 0) as prompt_tokens, COALESCE(SUM(response_tokens), 0) as response_tokens FROM YapayUstaUsage {where_sql} GROUP BY period ORDER BY period DESC;"
        cursor.execute(time_series_query, tuple(params))
        time_series_data = [{"period": row['period'].strftime('%Y-%m-%d'), "count": row['count'], "cost": round((row['prompt_tokens'] * COST_PER_INPUT_TOKEN) + (row['response_tokens'] * COST_PER_OUTPUT_TOKEN), 4)} for row in cursor.fetchall()]
        logs_query = f"SELECT id, created_at, user_identifier, prompt, prompt_tokens, response_tokens, is_from_cache FROM YapayUstaUsage {where_sql} ORDER BY created_at DESC LIMIT 100;"
        cursor.execute(logs_query, tuple(params))
        usage_logs = [{"id": row['id'], "timestamp": row['created_at'].strftime('%Y-%m-%d %H:%M:%S'), "user_identifier": row['user_identifier'], "prompt": row['prompt'], "total_tokens": row['prompt_tokens'] + row['response_tokens'], "cost": round((row['prompt_tokens'] * COST_PER_INPUT_TOKEN) + (row['response_tokens'] * COST_PER_OUTPUT_TOKEN), 6), "is_from_cache": row['is_from_cache']} for row in cursor.fetchall()]
        return jsonify({"summary": summary, "time_series": time_series_data, "usage_logs": usage_logs})
    except psycopg2.errors.UndefinedTable:
        return jsonify({"description": "'YapayUstaUsage' tablosu mevcut değil."}), 500
    except Exception as e:
        logging.error(f"AI istatistikleri alınırken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "AI istatistikleri alınamadı."}), 500
    finally:
        if conn: conn.close()

@admin_bp.route('/api/admin/pending_businesses', methods=['GET'])
@admin_only
def get_pending_businesses():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = "SELECT u.id as user_id, u.name, u.email, u.phone_number, u.created_at, COALESCE(s.google_place_name, u.name) as shop_name FROM Users u JOIN Shops s ON u.id = s.user_id WHERE u.user_type = 'business' AND (s.google_place_id IS NULL OR s.google_place_id = '') ORDER BY u.created_at ASC;"
        cursor.execute(query)
        pending = [dict(row) for row in cursor.fetchall()]
        return jsonify(pending)
    except Exception as e:
        logging.error(f"Bekleyen işletmeler alınırken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "Başvurular alınamadı."}), 500
    finally:
        if conn: conn.close()

@admin_bp.route('/api/admin/reject_business', methods=['POST'])
@admin_only
def reject_business():
    data = request.json
    user_id = data.get('user_id')
    if not user_id: return jsonify({"description": "Geçersiz istek."}), 400
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Users WHERE id = %s AND user_type = 'business'", (user_id,))
        conn.commit()
        if cursor.rowcount == 0:
            return jsonify({"description": "İşletme bulunamadı veya silme yetkiniz yok."}), 404
        logging.info(f"Yönetici tarafından reddedilen işletme (user_id: {user_id}) silindi.")
        return jsonify({"status": "success"})
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"İşletme reddedilirken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "İşlem sırasında bir hata oluştu."}), 500
    finally:
        if conn: conn.close()

@admin_bp.route('/api/admin/businesses', methods=['GET'])
@admin_only
def get_businesses():
    name_filter = request.args.get('name', '')
    city_filter = request.args.get('city', '')
    shop_type_filter = request.args.get('shop_type', '')
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        base_query = "SELECT u.id as user_id, u.name as owner_name, u.email, u.phone_number, u.created_at, s.id as shop_id, COALESCE(s.google_place_name, u.name) as shop_name, s.city, s.shop_type, l.is_active FROM Users u JOIN Shops s ON u.id = s.user_id LEFT JOIN Licenses l ON s.id = l.shop_id WHERE u.user_type = 'business' AND s.google_place_id IS NOT NULL AND s.google_place_id != ''"
        filters, params = [], []
        if name_filter:
            filters.append("(COALESCE(s.google_place_name, u.name) ILIKE %s OR u.name ILIKE %s)")
            params.extend([f"%{name_filter}%", f"%{name_filter}%"])
        if city_filter:
            filters.append("s.city ILIKE %s")
            params.append(f"%{city_filter}%")
        if shop_type_filter:
            filters.append("s.shop_type = %s")
            params.append(shop_type_filter)
        if filters:
            base_query += " AND " + " AND ".join(filters)
        base_query += " ORDER BY u.created_at DESC;"
        cursor.execute(base_query, tuple(params))
        businesses = [dict(row) for row in cursor.fetchall()]
        return jsonify(businesses)
    except Exception as e:
        logging.error(f"İşletmeler listelenirken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "İşletmeler alınamadı."}), 500
    finally:
        if conn: conn.close()

@admin_bp.route('/api/admin/update_business_status', methods=['POST'])
@admin_only
def update_business_status():
    data = request.json
    shop_id, is_active = data.get('shop_id'), data.get('is_active')
    if shop_id is None or is_active is None:
        return jsonify({"description": "Geçersiz istek."}), 400
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE Licenses SET is_active = %s WHERE shop_id = %s", (is_active, shop_id))
        if cursor.rowcount == 0:
            return jsonify({"status": "no_license_found", "description": "İşletmenin aktif bir lisansı bulunmadığı için durum güncellenemedi."})
        conn.commit()
        logging.info(f"İşletme (shop_id: {shop_id}) aktif durumu '{is_active}' olarak güncellendi.")
        return jsonify({"status": "success"})
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"İşletme durumu güncellenirken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"description": "İşletme durumu güncellenemedi."}), 500
    finally:
        if conn: conn.close()

@admin_bp.route('/api/admin/test')
def admin_test_route():
    return jsonify({"status": "success", "message": "Admin API çalışıyor."})
