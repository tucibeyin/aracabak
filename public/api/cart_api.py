import os
import logging
import traceback
from flask import Blueprint, jsonify, request, session
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import decimal

# --- Loglama ve .env Kurulumu (parca_api.py ile aynı) ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')

# .env dosyasının yolunu bul
dotenv_path = '/var/www/aracabak.com/private/secrets/.env'
if not os.path.exists(dotenv_path):
    dotenv_path_alt = os.path.join(os.path.dirname(__file__), '..', '..', 'private', 'secrets', '.env')
    if os.path.exists(dotenv_path_alt):
        dotenv_path = dotenv_path_alt
    else:
        logging.critical(f"cart_api: .env dosyası bulunamadı! Kontrol edilen yollar: '{dotenv_path}', '{dotenv_path_alt}'")
        raise FileNotFoundError(".env dosyası belirtilen yollarda bulunamadı.")
load_dotenv(dotenv_path=dotenv_path)

# --- Blueprint Oluşturma ---
cart_bp = Blueprint('cart_api', __name__, url_prefix='/api/cart')

# --- VERİTABANI BAĞLANTISI (parca_api.py ile aynı) ---
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
        logging.error(f"Cart API: Veritabanına bağlanılamadı! Hata: {conn_err}")
        raise
    except Exception as e:
        logging.error(f"Cart API: Veritabanı bağlantı hatası: {e}")
        raise

# --- SEPET YARDIMCI FONKSİYONLARI ---

def get_cart_from_session():
    """
    Oturumdan (session) sepet verisini güvenli bir şekilde alır.
    Sepet yoksa veya formatı bozuksa, boş bir sepet oluşturur.
    """
    cart = session.get('cart', {})
    if not isinstance(cart, dict) or 'items' not in cart or not isinstance(cart.get('items'), dict):
        cart = {'items': {}}  # items = { 'product_id_str': quantity_int }
    
    # 'items' anahtarının varlığını garantile
    if 'items' not in cart:
        cart['items'] = {}
        
    session['cart'] = cart
    return cart

def save_cart_to_session(cart_data):
    """
    Sepeti oturuma (session) kaydeder ve Flask'a değişikliği bildirir.
    'session.modified = True' çok önemlidir, çünkü sözlük gibi
    değişken (mutable) nesnelerin değiştirildiğini Flask'a bildirir.
    """
    session['cart'] = cart_data
    session.modified = True
    logging.debug(f"Sepet session'a kaydedildi: {cart_data}")

def get_cart_details_internal():
    """
    Bu, tüm endpoint'lerin sonunda çağıracağı ana fonksiyondur.
    Oturumdaki sepet ID'lerini alır, veritabanından ürün verilerini çeker (hydrate)
    ve tam, detaylı bir sepet objesi (Python dict) döndürür.
    """
    
    cart = get_cart_from_session()
    product_id_map = cart.get('items', {}) # {'123': 2, '456': 1}
    product_ids = list(product_id_map.keys())

    # Döndürülecek varsayılan boş sepet yapısı
    response_cart = {
        "items": [],
        "total_price": 0.0,
        "item_count": 0
    }

    if not product_ids:
        return response_cart # Sepet boşsa hemen döndür

    conn = None
    try:
        # Veritabanından bu ID'lere ait tüm ürün detaylarını tek seferde çek
        int_product_ids = [int(pid) for pid in product_ids]
        
        conn = get_db_connection()
        cursor = conn.cursor() # Zaten DictCursor
        
        query = """
            SELECT id, name, price, image_url, stock_code, brand, url
            FROM Products 
            WHERE id = ANY(%s)
        """
        cursor.execute(query, (int_product_ids,))
        
        products_in_db = cursor.fetchall()
        
        # Hızlı erişim için ürünleri bir haritaya (map) dönüştür
        product_details_map = {str(row['id']): row for row in products_in_db}
        
        total_price = decimal.Decimal('0.0')
        total_items = 0
        hydrated_items = []
        needs_cart_update = False # DB'de bulunamayan bir ürün olursa
        
        # Sepetteki ID'ler ve DB'den gelen ürünler arasında döngü
        for pid_str, quantity in product_id_map.items():
            product_data = product_details_map.get(pid_str)
            
            if product_data:
                # Fiyatı 'decimal' olarak al, DB'den 'None' gelirse 0 kabul et
                price = product_data.get('price') or decimal.Decimal('0.0')
                if not isinstance(price, decimal.Decimal):
                    price = decimal.Decimal(str(price)) # String veya float ise Decimal'e çevir
                    
                item_total_price = price * quantity
                total_price += item_total_price
                total_items += quantity
                
                hydrated_items.append({
                    "id": product_data['id'],
                    "name": product_data['name'],
                    "price": float(price), # JSON için float'a geri çevir
                    "quantity": quantity,
                    "item_total_price": float(item_total_price), # JSON için float
                    "image_url": product_data.get('image_url'),
                    "stock_code": product_data.get('stock_code'),
                    "brand": product_data.get('brand'),
                    "url": product_data.get('url')
                })
            else:
                # DB'de bulunamayan bir ürün sepette kalmış (örn: ürün silinmiş)
                # Onu sepetten temizle (otomatik iyileştirme)
                logging.warning(f"Sepette bulunan ancak DB'de olmayan ürün (ID: {pid_str}) temizleniyor.")
                cart['items'].pop(pid_str, None)
                needs_cart_update = True

        if needs_cart_update:
            save_cart_to_session(cart) # Temizlenmiş sepeti kaydet

        response_cart['items'] = hydrated_items
        response_cart['total_price'] = float(total_price)
        response_cart['item_count'] = total_items
        
        return response_cart

    except (psycopg2.Error, Exception) as e:
        logging.error(f"Sepet detayları (DB) alınırken hata: {e}\n{traceback.format_exc()}")
        # DB hatası olursa, en azından ham sepeti (ID ve miktar) döndür
        response_cart['items'] = [{"id": pid, "quantity": qty, "error": "Ürün detayı alınamadı"} for pid, qty in product_id_map.items()]
        response_cart['item_count'] = sum(product_id_map.values())
        return response_cart
    finally:
        if conn:
            conn.close()

# --- 🛒 SEPET API ENDPOINT'LERİ ---

@cart_bp.route('/', methods=['GET'])
def get_cart():
    """
    Sepetin mevcut durumunu, tüm ürün detayları ve toplam fiyat ile birlikte
    JSON olarak döndürür.
    """
    try:
        detailed_cart = get_cart_details_internal()
        return jsonify(detailed_cart)
    except Exception as e:
        logging.error(f"GET /cart hatası: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Sepet getirilirken bir hata oluştu."}), 500

@cart_bp.route('/add', methods=['POST'])
def add_to_cart():
    """
    Sepete yeni bir ürün ekler veya mevcut ürünün miktarını artırır.
    JSON Body: {"product_id": "123", "quantity": 1}
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "Geçersiz istek."}), 400

    try:
        product_id = str(data['product_id'])
        quantity = int(data.get('quantity', 1))
        
        if quantity <= 0:
            return jsonify({"error": "Miktar pozitif bir sayı olmalıdır."}), 400
            
    except (ValueError, KeyError, TypeError):
        return jsonify({"error": "Geçersiz istek: 'product_id' (string) ve 'quantity' (int) gereklidir."}), 400

    try:
        cart = get_cart_from_session()
        
        current_quantity = cart['items'].get(product_id, 0)
        cart['items'][product_id] = current_quantity + quantity
        
        save_cart_to_session(cart)
        
        logging.info(f"Sepete eklendi: ProductID {product_id}, Eklenen Miktar {quantity}, Yeni Miktar {cart['items'][product_id]}")
        
        # İstemciye güncel, detaylı sepeti döndür
        detailed_cart = get_cart_details_internal()
        return jsonify(detailed_cart), 200
        
    except Exception as e:
        logging.error(f"Sepete eklerken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Sepete ürün eklenirken bir sunucu hatası oluştu."}), 500

@cart_bp.route('/update', methods=['PUT'])
def update_cart_item():
    """
    Sepetteki bir ürünün miktarını doğrudan belirler.
    Miktar 0 veya daha az ise ürünü sepetten kaldırır.
    JSON Body: {"product_id": "123", "quantity": 3}
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "Geçersiz istek."}), 400
        
    try:
        product_id = str(data['product_id'])
        quantity = int(data['quantity'])
            
    except (ValueError, KeyError, TypeError):
        return jsonify({"error": "Geçersiz istek: 'product_id' (string) ve 'quantity' (int) gereklidir."}), 400

    try:
        cart = get_cart_from_session()
        
        if product_id not in cart['items']:
            return jsonify({"error": "Ürün sepette bulunamadı."}), 404

        if quantity > 0:
            cart['items'][product_id] = quantity
            logging.info(f"Sepet güncellendi: ProductID {product_id}, Yeni Miktar {quantity}")
        else:
            # Miktar 0 veya daha az ise ürünü kaldır
            cart['items'].pop(product_id, None)
            logging.info(f"Sepetten kaldırıldı (miktar <= 0): ProductID {product_id}")
        
        save_cart_to_session(cart)
        
        detailed_cart = get_cart_details_internal()
        return jsonify(detailed_cart), 200
        
    except Exception as e:
        logging.error(f"Sepet güncellenirken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Sepet güncellenirken bir sunucu hatası oluştu."}), 500

@cart_bp.route('/remove', methods=['DELETE'])
def remove_from_cart():
    """
    Bir ürünü miktarından bağımsız olarak sepetten tamamen kaldırır.
    JSON Body: {"product_id": "123"}
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "Geçersiz istek."}), 400

    try:
        product_id = str(data['product_id'])
    except (ValueError, KeyError, TypeError):
        return jsonify({"error": "Geçersiz istek: 'product_id' (string) gereklidir."}), 400

    try:
        cart = get_cart_from_session()
        
        if cart['items'].pop(product_id, None) is not None:
            logging.info(f"Sepetten kaldırıldı: ProductID {product_id}")
            save_cart_to_session(cart)
        else:
            logging.warning(f"Sepetten kaldırılmak istenen ürün bulunamadı: ProductID {product_id}")

        detailed_cart = get_cart_details_internal()
        return jsonify(detailed_cart), 200
        
    except Exception as e:
        logging.error(f"Sepetten ürün kaldırılırken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Sepetten ürün kaldırılırken bir sunucu hatası oluştu."}), 500

@cart_bp.route('/clear', methods=['DELETE'])
def clear_cart():
    """
    Sepetteki tüm ürünleri kaldırır.
    """
    try:
        save_cart_to_session({'items': {}}) # Sepeti boşalt
        logging.info("Sepet temizlendi.")
        
        # Boş sepet yapısını döndür
        detailed_cart = get_cart_details_internal() 
        return jsonify(detailed_cart), 200
    except Exception as e:
        logging.error(f"Sepet temizlenirken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Sepet temizlenirken bir sunucu hatası oluştu."}), 500

# --- TEKLİF İSTEME (GİRİŞ KONTROLÜ) ENDPOINT'İ ---

@cart_bp.route('/request-quote', methods=['POST'])
def request_quote_auth_check():
    """
    Bu endpoint, "Teklif İste" butonunun arkasında çalışır.
    1. Sepetin boş olup olmadığını kontrol eder.
    2. Kullanıcının giriş yapıp yapmadığını kontrol eder.
    
    - Giriş yapmışsa: 200 OK döndürür, frontend bir sonraki adıma (sağlayıcı seçimi) geçer.
    - Giriş yapmamışsa: 401 Unauthorized döndürür, frontend login modal'ı açar.
    """
    try:
        cart = get_cart_from_session()
        
        if not cart.get('items'):
            return jsonify({
                "status": "empty_cart", 
                "message": "Teklif istemeden önce sepetinize ürün eklemelisiniz."
            }), 400 # 400 Bad Request
            
        if 'user_id' in session:
            # Kullanıcı giriş yapmış, devam edebilir
            logging.info(f"Kullanıcı {session['user_id']} teklif isteme (auth check) başarılı.")
            return jsonify({
                "status": "authenticated", 
                "message": "Lütfen teklif almak için bir parça sağlayıcı seçin."
            }), 200 # 200 OK
        else:
            # Kullanıcı giriş yapmamış
            logging.info("Misafir kullanıcı teklif istedi, giriş yapması gerekiyor.")
            return jsonify({
                "status": "unauthenticated", 
                "message": "Sepetinizdeki ürünler için teklif alabilmek lütfen giriş yapın veya kayıt olun."
            }), 401 # 401 Unauthorized

    except Exception as e:
        logging.error(f"Teklif isteme (auth check) sırasında hata: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Bir hata oluştu, lütfen tekrar deneyin."}), 500
