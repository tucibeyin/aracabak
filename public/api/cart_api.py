import os
import logging
import traceback
from flask import Blueprint, jsonify, request, session
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import decimal
import uuid # YENİ: Özel parçalar için benzersiz ID

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
    Oturumdan (session) 'aktif' sepet verisini güvenli bir şekilde alır.
    Gerekirse 'carts' ve 'active_cart_name' anahtarlarını oluşturur.
    YENİ: Sepet yapısını {'items': {'db': {}, 'custom': {}}} olarak günceller/doğrular.
    """
    carts = session.get('carts', {})
    active_cart_name = session.get('active_cart_name', 'default')

    # Veri bozulmasına karşı koruma
    if not isinstance(carts, dict):
        carts = {}

    # İlk defa çalışıyorsa veya sepetler tamamen boşsa, varsayılan sepeti oluştur
    if not carts:
        carts['default'] = {'items': {'db': {}, 'custom': {}}}
        active_cart_name = 'default'
    
    # Eğer aktif sepet adı, sepet listesinde yoksa (örn: silinmişse)
    # varsayılan sepete geri dön ve onu oluştur
    if active_cart_name not in carts:
        active_cart_name = 'default'
        if 'default' not in carts:
            carts['default'] = {'items': {'db': {}, 'custom': {}}}

    # --- YENİ YAPIYA GEÇİŞ KONTROLÜ ---
    # Aktif sepetin veri yapısını kontrol et
    active_cart_data = carts[active_cart_name]
    if 'items' not in active_cart_data or not isinstance(active_cart_data.get('items'), dict):
        # Sepet tamamen bozuksa, sıfırla
        active_cart_data = {'items': {'db': {}, 'custom': {}}}
    elif 'db' not in active_cart_data.get('items', {}) or 'custom' not in active_cart_data.get('items', {}):
        # Bu, 'items' anahtarı olan ama alt-yapısı olmayan eski bir sepet
        old_items = active_cart_data.get('items', {})
        # Eski verinin 'db' mi 'custom' mu olduğunu bilemeyiz, 'db' varsayalım
        active_cart_data['items'] = {'db': old_items, 'custom': {}}
        logging.info(f"Eski sepet yapısı '{active_cart_name}' için yeni yapıya geçirildi.")
    
    # Güncellenmiş/Doğrulanmış yapıyı ana sözlüğe geri koy
    carts[active_cart_name] = active_cart_data
    
    # Değişikliklerin (eğer yapıldıysa) bir sonraki istek için kaydedildiğinden emin ol
    session['carts'] = carts
    session['active_cart_name'] = active_cart_name
    
    # Sadece 'aktif' olan sepetin sözlüğünü (yeni yapıyla) döndür
    return active_cart_data

def save_cart_to_session(cart_data):
    """
    Verilen 'cart_data'yı (örn: {'items':{...}}) mevcut 'aktif' sepet olarak kaydeder.
    """
    # Aktif sepetin adını al (get_cart_from_session'dan geçtiği için var olmalı)
    active_cart_name = session.get('active_cart_name', 'default')
    carts = session.get('carts', {})
    
    # Aktif sepetin verisini 'carts' sözlüğü içinde güncelle
    carts[active_cart_name] = cart_data
    session['carts'] = carts # Ana sepet sözlüğünü session'a geri koy
    session.modified = True
    logging.debug(f"Sepet '{active_cart_name}' session'a kaydedildi.")

def get_cart_details_internal():
    """
    Bu, tüm endpoint'lerin sonunda çağıracağı ana fonksiyondur.
    Oturumdaki 'aktif' sepet ID'lerini ('db' ve 'custom') alır,
    veritabanından ürün verilerini çeker (hydrate) ve tam, detaylı bir sepet objesi döndürür.
    """
    
    cart = get_cart_from_session() # Bu artık {'items': {'db': ..., 'custom': ...}} döndürür
    
    # YENİ YAPI: 'db' ve 'custom' itemları ayır
    db_item_map = cart.get('items', {}).get('db', {})
    custom_item_map = cart.get('items', {}).get('custom', {})
    
    db_product_ids = list(db_item_map.keys())

    response_cart = {
        "items": [],
        "total_price": 0.0,
        "item_count": 0
    }

    total_price = decimal.Decimal('0.0')
    total_items = 0
    hydrated_items = []
    needs_cart_update = False # DB'de bulunamayan bir ürün olursa
    conn = None

    try:
        # --- 1. ADIM: Veritabanı (db) parçalarını işle ---
        if db_product_ids:
            # Sadece integer ID'leri al, 'custom_...' ID'leri hariç tut
            int_product_ids = [int(pid) for pid in db_product_ids if pid.isdigit()]
            
            if int_product_ids: # Eğer sepette geçerli DB ID'leri varsa
                conn = get_db_connection()
                cursor = conn.cursor() # Zaten DictCursor
                
                query = """
                    SELECT id, name, price, image_url, stock_code, brand, url
                    FROM Products 
                    WHERE id = ANY(%s)
                """
                cursor.execute(query, (int_product_ids,))
                products_in_db = cursor.fetchall()
                product_details_map = {str(row['id']): row for row in products_in_db}
                
                for pid_str, quantity in db_item_map.items():
                    if not pid_str.isdigit(): # Güvenlik önlemi, 'db' içinde custom ID varsa atla
                        needs_cart_update = True
                        cart['items']['db'].pop(pid_str, None)
                        continue

                    product_data = product_details_map.get(pid_str)
                    
                    if product_data:
                        price = product_data.get('price') or decimal.Decimal('0.0')
                        if not isinstance(price, decimal.Decimal):
                            price = decimal.Decimal(str(price))
                            
                        item_total_price = price * quantity
                        total_price += item_total_price
                        total_items += quantity
                        
                        hydrated_items.append({
                            "id": product_data['id'],
                            "name": product_data['name'],
                            "price": float(price),
                            "quantity": quantity,
                            "item_total_price": float(item_total_price),
                            "image_url": product_data.get('image_url'),
                            "stock_code": product_data.get('stock_code'),
                            "brand": product_data.get('brand'),
                            "url": product_data.get('url'),
                            "is_custom": False # YENİ: Bu bir DB parçası
                        })
                    else:
                        logging.warning(f"Sepette bulunan ancak DB'de olmayan ürün (ID: {pid_str}) temizleniyor.")
                        cart['items']['db'].pop(pid_str, None)
                        needs_cart_update = True

        # --- 2. ADIM: Özel (custom) parçaları işle ---
        for item_id, item_data in custom_item_map.items():
            try:
                # Özel parçalar fiyatı zaten float olarak saklıyor olmalı (add_custom_item'a göre)
                price = decimal.Decimal(str(item_data.get('price', 0)))
                quantity = int(item_data.get('quantity', 0))
                item_total_price = price * quantity
                
                total_price += item_total_price
                total_items += quantity
                
                hydrated_items.append({
                    "id": item_id, # Bu 'custom_uuid_...' gibi bir ID olacak
                    "name": item_data.get('name', 'Özel Parça'),
                    "price": float(price),
                    "quantity": quantity,
                    "item_total_price": float(item_total_price),
                    "image_url": '/logolar/placeholder.png', # Özel parçalar için varsayılan resim
                    "stock_code": "ÖZEL",
                    "brand": item_data.get('brand', 'Özel'),
                    "url": "#",
                    "is_custom": True # YENİ: Bu bir özel parça
                })
            except Exception as e:
                logging.warning(f"Özel parça işlenirken hata: {item_id}, Hata: {e}")
                cart['items']['custom'].pop(item_id, None) # Bozuk veriyi sil
                needs_cart_update = True

        # --- 3. ADIM: Sonuçları birleştir ---
        if needs_cart_update:
            save_cart_to_session(cart) # Temizlenmiş 'aktif' sepeti kaydet

        response_cart['items'] = hydrated_items
        response_cart['total_price'] = float(total_price)
        response_cart['item_count'] = total_items
        
        return response_cart

    except (psycopg2.Error, Exception) as e:
        logging.error(f"Sepet detayları (DB) alınırken hata: {e}\n{traceback.format_exc()}")
        # DB hatası olursa, en azından ham sepeti (ID ve miktar) döndür
        db_items_list = [{"id": pid, "quantity": qty, "error": "Ürün detayı alınamadı"} for pid, qty in db_item_map.items()]
        custom_items_list = [{"id": pid, "quantity": data.get('quantity', 0), "name": data.get('name', 'Özel parça')} for pid, data in custom_item_map.items()]
        
        response_cart['items'] = db_items_list + custom_items_list
        response_cart['item_count'] = sum(db_item_map.values()) + sum(d.get('quantity', 0) for d in custom_item_map.values())
        return response_cart
    finally:
        if conn:
            conn.close()

# --- 🛒 SEPET API ENDPOINT'LERİ ---

@cart_bp.route('/', methods=['GET'])
def get_cart():
    """
    'Aktif' sepetin mevcut durumunu, tüm ürün detayları ve toplam fiyat ile birlikte
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
    'Aktif' sepete yeni bir VERİTABANI ürünü ekler veya miktarını artırır.
    JSON Body: {"product_id": "123", "quantity": 1}
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "Geçersiz istek."}), 400

    try:
        product_id = str(data['product_id']) # Bu her zaman '123' gibi bir DB ID'sidir
        quantity = int(data.get('quantity', 1))
        
        if quantity <= 0:
            return jsonify({"error": "Miktar pozitif bir sayı olmalıdır."}), 400
            
    except (ValueError, KeyError, TypeError):
        return jsonify({"error": "Geçersiz istek: 'product_id' (string) ve 'quantity' (int) gereklidir."}), 400

    try:
        cart = get_cart_from_session()
        
        # YENİ YAPI: 'db' altına ekle
        cart.setdefault('items', {'db': {}, 'custom': {}})
        cart['items'].setdefault('db', {})
        
        current_quantity = cart['items']['db'].get(product_id, 0)
        cart['items']['db'][product_id] = current_quantity + quantity
        
        save_cart_to_session(cart)
        
        logging.info(f"DB Parçası Sepete eklendi: ProductID {product_id}, Eklenen Miktar {quantity}, Yeni Miktar {cart['items']['db'][product_id]}")
        
        detailed_cart = get_cart_details_internal()
        return jsonify(detailed_cart), 200
        
    except Exception as e:
        logging.error(f"Sepete eklerken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Sepete ürün eklenirken bir sunucu hatası oluştu."}), 500

# YENİ ENDPOINT: Özel Parça Ekleme
@cart_bp.route('/add-custom', methods=['POST'])
def add_custom_item():
    """
    'Aktif' sepete özel (veritabanında olmayan) bir parça ekler.
    JSON Body: {"name": "Silecek Suyu", "price": 150.0, "quantity": 1, "brand": "MarkaA"}
    """
    data = request.get_json()
    try:
        name = str(data['name']).strip()
        price = decimal.Decimal(str(data['price']))
        quantity = int(data['quantity'])
        brand = str(data.get('brand', 'Özel')).strip() # İsteğe bağlı
        
        if not name or price < 0 or quantity <= 0:
            return jsonify({"error": "Geçersiz özel parça verisi (isim, fiyat, miktar zorunludur)."}), 400
            
    except (ValueError, KeyError, TypeError, decimal.InvalidOperation):
        return jsonify({"error": "Geçersiz format: 'name' (str), 'price' (num) ve 'quantity' (int) gereklidir."}), 400

    try:
        cart = get_cart_from_session()
        cart.setdefault('items', {'db': {}, 'custom': {}})
        cart['items'].setdefault('custom', {})
        
        # Benzersiz bir ID oluştur
        item_id = f"custom_{uuid.uuid4().hex[:8]}"

        cart['items']['custom'][item_id] = {
            'name': name,
            'price': float(price), # Fiyatı JSON uyumlu sakla
            'quantity': quantity,
            'brand': brand
        }
        
        save_cart_to_session(cart)
        logging.info(f"Özel parça eklendi: ID {item_id}, İsim {name}")
        
        detailed_cart = get_cart_details_internal()
        return jsonify(detailed_cart), 200
        
    except Exception as e:
        logging.error(f"Özel parça eklerken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Özel parça eklenirken bir sunucu hatası oluştu."}), 500


@cart_bp.route('/update', methods=['PUT'])
def update_cart_item():
    """
    'Aktif' sepetteki bir ürünün (DB veya Özel) miktarını doğrudan belirler.
    Miktar 0 veya daha az ise ürünü sepetten kaldırır.
    JSON Body: {"product_id": "123" veya "custom_...", "quantity": 3}
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
        
        # YENİ YAPI: Parçanın 'db' mi 'custom' mu olduğunu ID'den anla
        item_type = 'custom' if product_id.startswith('custom_') else 'db'
        
        # 'items' anahtarının var olup olmadığını ve bir sözlük olduğunu kontrol et
        if 'items' not in cart or not isinstance(cart['items'], dict):
             cart['items'] = {'db': {}, 'custom': {}} # Bozuksa sıfırla

        # 'db' veya 'custom' anahtarının var olup olmadığını kontrol et
        if item_type not in cart['items'] or not isinstance(cart['items'][item_type], dict):
            cart['items'][item_type] = {} # Bozuksa sıfırla

        if product_id not in cart['items'][item_type]:
            return jsonify({"error": "Ürün sepette bulunamadı."}), 404

        if quantity > 0:
            if item_type == 'db':
                cart['items']['db'][product_id] = quantity
            else:
                # Özel parçanın tüm verisi saklandığı için miktarını güncelle
                cart['items']['custom'][product_id]['quantity'] = quantity
            logging.info(f"Sepet güncellendi: ProductID {product_id}, Yeni Miktar {quantity}")
        else:
            # Miktar 0 veya daha az ise ürünü kaldır
            cart['items'][item_type].pop(product_id, None)
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
    Bir ürünü (DB veya Özel) 'aktif' sepetten tamamen kaldırır.
    JSON Body: {"product_id": "123" veya "custom_..."}
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
        
        # YENİ YAPI: Parçanın 'db' mi 'custom' mu olduğunu ID'den anla
        item_type = 'custom' if product_id.startswith('custom_') else 'db'
        
        item_found = False
        # 'items' ve 'item_type' anahtarlarının varlığını güvenli bir şekilde kontrol et
        if 'items' in cart and isinstance(cart['items'], dict) and \
           item_type in cart['items'] and isinstance(cart['items'][item_type], dict):
            
            if cart['items'][item_type].pop(product_id, None) is not None:
                item_found = True
        
        if item_found:
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
    'Aktif' sepetteki tüm ürünleri ('db' ve 'custom') kaldırır.
    """
    try:
        # YENİ YAPI: 'db' ve 'custom' anahtarlarını koruyarak içlerini boşalt
        save_cart_to_session({'items': {'db': {}, 'custom': {}}}) 
        logging.info("Aktif sepet temizlendi.")
        
        detailed_cart = get_cart_details_internal() 
        return jsonify(detailed_cart), 200
    except Exception as e:
        logging.error(f"Sepet temizlenirken hata: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Sepet temizlenirken bir sunucu hatası oluştu."}), 500

# --- LİSTE YÖNETİM ENDPOINT'LERİ ---

def get_all_lists_summary_internal():
    """
    Kullanıcının sahip olduğu tüm sepet listelerinin bir özetini döndürür.
    (Yardımcı fonksiyon, JSON döndürmez)
    YENİ: 'db' ve 'custom' yapılarına göre sayım yapar.
    """
    # get_cart_from_session() çağrısı, session'daki tüm listelerin
    # 'default' dahil olmak üzere yeni yapıda ('db'/'custom') olmasını garantiler.
    get_cart_from_session() 
    
    carts = session.get('carts', {})
    active_cart_name = session.get('active_cart_name', 'default')
    
    summary = {
        'active_list_name': active_cart_name,
        'lists': []
    }
    
    for name, data in carts.items():
        # YENİ YAPIYA GÖRE item_count
        db_items = data.get('items', {}).get('db', {})
        custom_items = data.get('items', {}).get('custom', {})
        
        db_count = sum(db_items.values())
        custom_count = sum(item.get('quantity', 0) for item in custom_items.values())
        item_count = db_count + custom_count
        
        summary['lists'].append({
            'name': name,
            'item_count': item_count
        })
        
    return summary

@cart_bp.route('/lists', methods=['GET'])
def get_all_lists_summary():
    """
    Kullanıcının sahip olduğu tüm sepet listelerinin bir özetini JSON olarak döndürür.
    """
    summary = get_all_lists_summary_internal()
    return jsonify(summary)

def activate_or_create_list():
    """
    Frontend'den gelen 'list_name'e göre aktif sepeti değiştirir.
    Eğer o isimde bir sepet yoksa, onu (yeni yapıyla) oluşturur.
    JSON Body: {"list_name": "yeni_sepet_adim"}
    """
    data = request.get_json()
    list_name = data.get('list_name')
    
    if not list_name or list_name.strip() == "":
        return jsonify({"error": "Geçerli bir 'list_name' gereklidir."}), 400
        
    carts = session.get('carts', {})
    if not isinstance(carts, dict):
        carts = {}

    # Eğer istenen liste henüz mevcut değilse, (yeni yapıyla) boş olarak oluştur
    if list_name not in carts:
        carts[list_name] = {'items': {'db': {}, 'custom': {}}}
        logging.info(f"Yeni sepet listesi oluşturuldu: '{list_name}'")
    
    session['carts'] = carts
    session['active_cart_name'] = list_name
    session.modified = True
    logging.info(f"Aktif sepet değiştirildi: '{list_name}'")

    detailed_cart = get_cart_details_internal()
    
    response_data = {
        "new_active_list_name": list_name,
        "cart_details": detailed_cart
    }
    return jsonify(response_data)

@cart_bp.route('/set-active', methods=['POST'])
def set_active_list():
    return activate_or_create_list()

@cart_bp.route('/lists/create', methods=['POST'])
def create_list():
    return activate_or_create_list()

@cart_bp.route('/lists/delete', methods=['POST'])
def delete_list():
    """
    Belirtilen bir sepet listesini siler.
    'default' listesi silinemez.
    JSON Body: {"list_name": "silinecek_liste_adi"}
    """
    data = request.get_json()
    list_name = data.get('list_name')
    
    if not list_name:
        return jsonify({"error": "'list_name' gereklidir."}), 400
        
    if list_name == 'default':
        return jsonify({"error": "'default' (varsayılan) liste silinemez."}), 400

    carts = session.get('carts', {})
    if list_name not in carts:
        return jsonify({"error": "Silinecek liste bulunamadı."}), 404
        
    # Listeyi sil
    carts.pop(list_name, None)
    logging.info(f"Sepet listesi silindi: '{list_name}'")
    
    # Eğer silinen liste aktif listeyse, 'default' listesini aktif yap
    active_cart_name = session.get('active_cart_name', 'default')
    if active_cart_name == list_name:
        session['active_cart_name'] = 'default'
        logging.info("Aktif liste silindi, 'default' listesine geçildi.")
        
    session['carts'] = carts
    session.modified = True
    
    # İşlem sonrası güncel liste özetini döndür
    summary = get_all_lists_summary_internal()
    return jsonify(summary)

@cart_bp.route('/lists/rename', methods=['POST'])
def rename_list():
    """
    Bir sepet listesinin adını değiştirir.
    'default' listesi yeniden adlandırılamaz.
    JSON Body: {"old_name": "eski_ad", "new_name": "yeni_ad"}
    """
    data = request.get_json()
    old_name = data.get('old_name')
    new_name = data.get('new_name')
    
    if not old_name or not new_name:
        return jsonify({"error": "'old_name' ve 'new_name' gereklidir."}), 400
        
    if old_name == 'default':
        return jsonify({"error": "'default' (varsayılan) liste yeniden adlandırılamaz."}), 400
        
    if new_name.strip() == "":
        return jsonify({"error": "Yeni liste adı boş olamaz."}), 400

    carts = session.get('carts', {})
    if old_name not in carts:
        return jsonify({"error": "Yeniden adlandırılacak liste bulunamadı."}), 404
        
    if new_name in carts:
        return jsonify({"error": "Bu isimde bir liste zaten mevcut."}), 409
        
    # Listeyi yeniden adlandır (veriyi kopyala ve eskiyi sil)
    carts[new_name] = carts.pop(old_name)
    logging.info(f"Sepet listesi yeniden adlandırıldı: '{old_name}' -> '{new_name}'")

    # Eğer yeniden adlandırılan liste aktif listeyse, aktif adı da güncelle
    active_cart_name = session.get('active_cart_name', 'default')
    if active_cart_name == old_name:
        session['active_cart_name'] = new_name
        
    session['carts'] = carts
    session.modified = True
    
    # İşlem sonrası güncel liste özetini döndür
    summary = get_all_lists_summary_internal()
    return jsonify(summary)


# --- TEKLİF İSTEME (GİRİŞ KONTROLÜ) ENDPOINT'İ ---

@cart_bp.route('/request-quote', methods=['POST'])
def request_quote_auth_check():
    """
    Bu endpoint, "Teklif İste" butonunun arkasında çalışır.
    1. 'Aktif' sepetin boş olup olmadığını kontrol eder.
    2. Kullanıcının giriş yapıp yapmadığını kontrol eder.
    
    - Giriş yapmışsa: 200 OK döndürür, frontend bir sonraki adıma (sağlayıcı seçimi) geçer.
    - Giriş yapmamışsa: 401 Unauthorized döndürür, frontend login modal'ı açar.
    """
    try:
        cart = get_cart_from_session() # Bu artık 'aktif' sepeti alır
        
        # YENİ YAPI: Hem 'db' hem de 'custom' itemları kontrol et
        db_items = cart.get('items', {}).get('db', {})
        custom_items = cart.get('items', {}).get('custom', {})
        
        if not db_items and not custom_items:
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
