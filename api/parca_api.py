import os
import logging
import traceback
from flask import Blueprint, jsonify, request
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Loglama ayarları (main_api.py'deki gibi olabilir)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')

# .env dosyasının yolunu belirtip, değişkenleri yüklüyoruz
# Bu yolun sunucunuzdaki .env dosyasının doğru konumu olduğundan emin olun
dotenv_path = '/var/www/aracabak.com/private/secrets/.env'
if not os.path.exists(dotenv_path):
    # Alternatif yol (betik api klasöründeyse bir üst dizine çıkıp private/secrets'e bakar)
    dotenv_path_alt = os.path.join(os.path.dirname(__file__), '..', '..', 'private', 'secrets', '.env')
    if os.path.exists(dotenv_path_alt):
         dotenv_path = dotenv_path_alt
    else:
        logging.critical(f"parca_api: .env dosyası bulunamadı! Kontrol edilen yollar: '{dotenv_path}', '{dotenv_path_alt}'")
        # .env olmadan veritabanına bağlanılamaz, bu yüzden hata vermek mantıklı olabilir
        # veya varsayılan değerler denenebilir (güvenli değil)
        raise FileNotFoundError(".env dosyası belirtilen yollarda bulunamadı.")
load_dotenv(dotenv_path=dotenv_path)

# Blueprint'i oluşturalım
parca_bp = Blueprint('parca_api', __name__, url_prefix='/api/parca') # Prefix eklemek daha düzenli olabilir

# --- VERİTABANI BAĞLANTISI ---
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
        # Sonuçları sütun adlarıyla erişilebilir dict (sözlük) olarak almak için
        conn.cursor_factory = psycopg2.extras.DictCursor
        return conn
    except psycopg2.OperationalError as conn_err:
        logging.error(f"Parca API: Veritabanına bağlanılamadı! Bağlantı ayarlarını kontrol edin. Hata: {conn_err}")
        raise # Bu hatayı yukarı taşıyarak endpoint'in 500 dönmesini sağla
    except Exception as e:
        logging.error(f"Parca API: Veritabanı bağlantı hatası: {e}")
        raise

# --- PARÇA ARAMA ENDPOINT'İ ---
@parca_bp.route('/bul', methods=['GET'])
def find_parts():
    """
    URL parametreleri (marka, seri, yil, yakit, model) ile gelen araç bilgilerine göre
    veritabanından uyumlu parçaları bulur ve JSON olarak döndürür.
    """
    # URL'den parametreleri al (Flask'ın request.args'ı kullanılır)
    brand = request.args.get('marka')
    series = request.args.get('seri')
    year_str = request.args.get('yil')
    fuel = request.args.get('yakit')
    engine = request.args.get('model') # index.html'den 'model' olarak geliyor, DB'de 'engine' sütununa karşılık geliyor

    # Gerekli parametreler eksikse hata döndür
    required_params = {'marka': brand, 'seri': series, 'yil': year_str, 'yakit': fuel, 'model': engine}
    missing_params = [k for k, v in required_params.items() if not v]
    if missing_params:
        return jsonify({"error": f"Eksik parametreler: {', '.join(missing_params)} gereklidir."}), 400

    try:
        # Yıl parametresini güvenli bir şekilde sayıya çevir
        year = int(year_str)
    except ValueError:
        return jsonify({"error": "Geçersiz 'yil' parametresi. Sayı olmalıdır."}), 400

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor() # Zaten DictCursor olarak ayarlandı

        # SQL Sorgusu:
        # Compatibility tablosundan eşleşen kayıtları bul, sonra Products tablosundan detayları al.
        # Yıl kontrolü: Seçilen yıl, parçanın uyumlu olduğu yıl aralığında olmalı.
        # Eğer year_start veya year_end NULL ise, o sınır kontrol edilmez (yani o yönde sınırsız).
        # ILIKE: Büyük/küçük harf duyarsız arama yapar.
        # DISTINCT p.id: Aynı ürünün birden fazla uyumluluk nedeniyle tekrarlanmasını önler.
        query = """
            SELECT DISTINCT p.id, p.url, p.name, p.brand, p.stock_code, p.price, p.image_url
            FROM Products p
            JOIN Compatibility c ON p.id = c.product_id
            WHERE c.vehicle_brand ILIKE %s
              AND c.vehicle_series ILIKE %s
              AND (c.year_start IS NULL OR c.year_start <= %s)
              AND (c.year_end IS NULL OR c.year_end >= %s)
              AND c.fuel ILIKE %s
              AND c.engine ILIKE %s
            ORDER BY p.name;
        """

        # Sorgu parametreleri (ILIKE için % işaretleri eklenmez, psycopg2 bunu kendi halleder)
        params = (
            brand,      # vehicle_brand
            series,     # vehicle_series
            year,       # year_start <= ?
            year,       # year_end >= ?
            fuel,       # fuel
            engine      # engine
        )

        cursor.execute(query, params)

        # Sonuçları al (DictCursor sayesinde liste içinde sözlükler olarak gelir)
        parts_data = cursor.fetchall()

        # Sonuçları JSON uyumlu hale getir (DictRow'dan standart dict'e çevir)
        result_list = [dict(row) for row in parts_data]

        logging.info(f"Parça arama: {brand} {series} {year} {fuel} {engine} için {len(result_list)} adet parça bulundu.")
        
        # Bulunan parçaları 'parcalar' anahtarı altında JSON olarak döndür
        return jsonify({"parcalar": result_list})

    except psycopg2.Error as db_err:
        logging.error(f"Parça arama sırasında veritabanı hatası: {db_err}\n{traceback.format_exc()}")
        # Kullanıcıya daha genel bir hata mesajı gösterilebilir
        return jsonify({"error": "Parçalar aranırken bir veritabanı hatası oluştu."}), 500
    except Exception as e:
        logging.error(f"Parça arama sırasında beklenmedik bir hata oluştu: {e}\n{traceback.format_exc()}")
        return jsonify({"error": "Parçalar aranırken sunucu tarafında bir hata oluştu."}), 500
    finally:
        # Bağlantıyı her zaman kapat (hata olsa bile)
        if conn:
            conn.close()
            logging.debug("Parca API: Veritabanı bağlantısı kapatıldı.")

# Test endpoint'i (isteğe bağlı)
@parca_bp.route('/test')
def parca_test():
    """API'nin çalışıp çalışmadığını test etmek için basit bir endpoint."""
    return jsonify({"message": "Parca API başarıyla çalışıyor!"})
