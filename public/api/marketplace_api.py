import os
import logging
import traceback
import json
from flask import Blueprint, jsonify, request, session
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# --- Loglama ve .env Kurulumu ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')

dotenv_path = '/var/www/aracabak.com/private/secrets/.env'
if not os.path.exists(dotenv_path):
    dotenv_path_alt = os.path.join(os.path.dirname(__file__), '..', '..', 'private', 'secrets', '.env')
    if os.path.exists(dotenv_path_alt):
        dotenv_path = dotenv_path_alt
    else:
        logging.critical(f"marketplace_api: .env dosyası bulunamadı! Kontrol edilen yollar: '{dotenv_path}', '{dotenv_path_alt}'")
        raise FileNotFoundError(".env dosyası belirtilen yollarda bulunamadı.")
load_dotenv(dotenv_path=dotenv_path)

# --- Blueprint Oluşturma ---
marketplace_bp = Blueprint('marketplace_api', __name__, url_prefix='/api/marketplace')

# --- Veritabanı Bağlantısı ---
def get_db_connection():
    """Veritabanı bağlantısı kurar ve DictCursor kullanır."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        conn.cursor_factory = psycopg2.extras.DictCursor
        return conn
    except psycopg2.OperationalError as conn_err:
        logging.error(f"Marketplace API: Veritabanına bağlanılamadı! Hata: {conn_err}")
        raise
    except Exception as e:
        logging.error(f"Marketplace API: Veritabanı bağlantı hatası: {e}")
        raise

# --- API ENDPOINT'LERİ ---

@marketplace_bp.route('/requests', methods=['GET'])
def get_public_requests():
    """
    Pazar yerinde gösterilmek üzere tüm 'public' ve 'pending' talepleri listeler.
    Her talebin kaç teklif aldığını da sayar.
    Giriş yapmış olmak GEREKMEZ.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # PublicBids tablosundan her talep için teklif sayısını (bid_count) alır
        query = """
            SELECT 
                r.id, r.user_id, r.vehicle_brand, r.vehicle_model, r.vehicle_year, 
                r.vehicle_km, r.selected_parts, r.created_at, 
                u.name as owner_name, 
                COALESCE(b.bid_count, 0) as bid_count
            FROM Requests r 
            JOIN Users u ON r.user_id = u.id
            LEFT JOIN (
                SELECT request_id, COUNT(*) as bid_count 
                FROM PublicBids 
                GROUP BY request_id
            ) b ON r.id = b.request_id
            WHERE r.request_type = 'public' AND r.status = 'pending'
            ORDER BY r.created_at DESC;
        """
        cursor.execute(query)
        requests_list = [dict(row) for row in cursor.fetchall()]
        
        # selected_parts (JSON string) verisini dict'e çevir
        for req in requests_list:
            if req.get('selected_parts'):
                try:
                    req['selected_parts'] = json.loads(req['selected_parts'])
                except json.JSONDecodeError:
                    req['selected_parts'] = {}
        
        return jsonify(requests_list)
        
    except Exception as e:
        logging.error(f"Herkese açık talepler (public requests) alınırken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Talepler alınırken bir hata oluştu."}), 500
    finally:
        if conn:
            conn.close()

@marketplace_bp.route('/bids', methods=['POST'])
def create_bid():
    """
    Giriş yapmış bir kullanıcının, herkese açık bir talebe teklif vermesini sağlar.
    JSON Body: {"request_id": 123, "price": 500, "notes": "Bu parçaları temin edebilirim."}
    """
    if 'user_id' not in session:
        return jsonify({"error": "Teklif verebilmek için giriş yapmalısınız."}), 401
        
    data = request.get_json()
    if not data:
        return jsonify({"error": "Geçersiz istek."}), 400
        
    try:
        request_id = int(data['request_id'])
        price = float(data['price'])
        notes = data.get('notes', '').strip()
        bidder_user_id = session['user_id']
        
        if price <= 0:
            return jsonify({"error": "Fiyat 0'dan büyük olmalıdır."}), 400
            
    except (ValueError, KeyError, TypeError):
        return jsonify({"error": "Geçersiz format: 'request_id' (int) ve 'price' (float) gereklidir."}), 400

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Kullanıcının kendi talebine teklif vermesini engelle (isteğe bağlı ama mantıklı)
        cursor.execute("SELECT user_id FROM Requests WHERE id = %s", (request_id,))
        request_owner = cursor.fetchone()
        if not request_owner:
            return jsonify({"error": "Teklif verilmek istenen talep bulunamadı."}), 404
        if request_owner['user_id'] == bidder_user_id:
            return jsonify({"error": "Kendi talebinize teklif veremezsiniz."}), 403

        # Kullanıcının aynı talebe birden fazla aktif teklif vermesini engelle
        cursor.execute("SELECT id FROM PublicBids WHERE request_id = %s AND bidder_user_id = %s AND status = 'pending'", (request_id, bidder_user_id))
        if cursor.fetchone():
            return jsonify({"error": "Bu talep için zaten aktif bir teklifiniz bulunuyor."}), 409

        # Teklifi ekle
        cursor.execute(
            """
            INSERT INTO PublicBids (request_id, bidder_user_id, price, notes, status) 
            VALUES (%s, %s, %s, %s, 'pending')
            RETURNING id;
            """,
            (request_id, bidder_user_id, price, notes)
        )
        new_bid_id = cursor.fetchone()['id']
        conn.commit()
        
        logging.info(f"Kullanıcı {bidder_user_id}, talep {request_id} için {price} TL teklif verdi. Bid ID: {new_bid_id}")
        return jsonify({"status": "success", "message": "Teklifiniz başarıyla gönderildi.", "bid_id": new_bid_id}), 201
        
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Genel teklif (bid) oluşturulurken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Teklif gönderilirken bir sunucu hatası oluştu."}), 500
    finally:
        if conn:
            conn.close()

@marketplace_bp.route('/requests/<int:request_id>/bids', methods=['GET'])
def get_bids_for_request(request_id):
    """
    Bir talebin sahibinin, kendi talebine gelen tüm genel teklifleri görmesini sağlar.
    (account.html'de kullanılacak)
    """
    if 'user_id' not in session:
        return jsonify({"error": "Teklifleri görmek için giriş yapmalısınız."}), 401
        
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Bu talebin gerçekten oturum açmış kullanıcıya ait olduğunu doğrula
        cursor.execute("SELECT user_id FROM Requests WHERE id = %s", (request_id,))
        request_owner = cursor.fetchone()
        
        if not request_owner:
            return jsonify({"error": "Talep bulunamadı."}), 404
        if request_owner['user_id'] != session['user_id']:
            return jsonify({"error": "Bu talebe ait teklifleri görme yetkiniz yok."}), 403
            
        # 2. Talebe ait tüm teklifleri ve teklif verenlerin adlarını al (fiyata göre sıralı)
        query = """
            SELECT b.*, u.name as bidder_name 
            FROM PublicBids b 
            JOIN Users u ON b.bidder_user_id = u.id 
            WHERE b.request_id = %s 
            ORDER BY b.price ASC, b.created_at ASC;
        """
        cursor.execute(query, (request_id,))
        bids_list = [dict(row) for row in cursor.fetchall()]
        
        return jsonify(bids_list)

    except Exception as e:
        logging.error(f"Talep {request_id} için teklifler alınırken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Teklifler alınırken bir hata oluştu."}), 500
    finally:
        if conn:
            conn.close()

@marketplace_bp.route('/bids/<int:bid_id>/accept', methods=['POST'])
def accept_bid(bid_id):
    """
    Bir talep sahibinin, gelen genel tekliflerden birini kabul etmesini sağlar.
    Bu işlem, talebi 'closed' (kapandı) olarak,
    seçilen teklifi 'accepted' (kabul edildi) olarak,
    diğer tüm teklifleri 'rejected' (reddedildi) olarak işaretler.
    """
    if 'user_id' not in session:
        return jsonify({"error": "İşlem yapmak için giriş yapmalısınız."}), 401
        
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Teklifin varlığını ve ait olduğu talebin sahibini doğrula
        cursor.execute(
            """
            SELECT b.request_id, b.status as bid_status, r.user_id as owner_user_id, r.status as request_status
            FROM PublicBids b
            JOIN Requests r ON b.request_id = r.id
            WHERE b.id = %s
            """,
            (bid_id,)
        )
        data = cursor.fetchone()
        
        if not data:
            return jsonify({"error": "Teklif bulunamadı."}), 404
        if data['owner_user_id'] != session['user_id']:
            return jsonify({"error": "Bu işlemi yapma yetkiniz yok."}), 403
        if data['request_status'] != 'pending':
            return jsonify({"error": "Bu talep zaten kapanmış veya işlem görmüş."}), 409
        if data['bid_status'] != 'pending':
            return jsonify({"error": "Bu teklif zaten işlem görmüş."}), 409

        request_id = data['request_id']
        
        # 2. Atomik olarak güncelle (Transaction)
        # Talebi kapat
        cursor.execute("UPDATE Requests SET status = 'closed' WHERE id = %s", (request_id,))
        
        # Kazanan teklifi işaretle
        cursor.execute("UPDATE PublicBids SET status = 'accepted' WHERE id = %s", (bid_id,))
        
        # Kalan teklifleri reddet
        cursor.execute("UPDATE PublicBids SET status = 'rejected' WHERE request_id = %s AND id != %s", (request_id, bid_id))
        
        conn.commit()
        
        logging.info(f"Kullanıcı {session['user_id']}, talep {request_id} için teklif {bid_id}'yi kabul etti.")
        return jsonify({"status": "success", "message": "Teklif kabul edildi. Talep pazar yerinden kaldırıldı."})

    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Genel teklif (bid) kabul edilirken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "İşlem sırasında bir sunucu hatası oluştu."}), 500
    finally:
        if conn:
            conn.close()
