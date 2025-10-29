import os
import sys
import google.generativeai as genai
from openai import OpenAI, APIConnectionError, RateLimitError
from dotenv import load_dotenv
import logging
from flask import Blueprint, request, jsonify, session
import redis
import hashlib
from datetime import timedelta
import psycopg2 
import psycopg2.extras

# --- YAPILANDIRMA ---
logger = logging.getLogger(__name__)
dotenv_path = '/var/www/aracabak.com/private/secrets/.env'
load_dotenv(dotenv_path=dotenv_path)

yapayusta_bp = Blueprint('yapayusta_api', __name__)

# --- VERİTABANI BAĞLANTISI VE TABLO YÖNETİMİ ---
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
        logger.error(f"Yapay Usta API: Veritabanı bağlantı hatası: {e}")
        return None

def add_column_if_not_exists(cursor, table_name, column_name, column_def):
    table_name = table_name.lower()
    column_name = column_name.lower()
    cursor.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
    """, (table_name, column_name))
    if not cursor.fetchone():
        cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_def}')
        logger.info(f"'{column_name}' sütunu '{table_name}' tablosuna eklendi.")

def initialize_database():
    conn = get_db_connection()
    if not conn:
        logger.critical("Yapay Usta API: Veritabanı bağlantısı kurulamadığı için tablo oluşturulamadı!")
        return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS YapayUstaUsage (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                user_identifier TEXT NOT NULL,
                prompt TEXT,
                response TEXT,
                prompt_tokens INTEGER DEFAULT 0,
                response_tokens INTEGER DEFAULT 0,
                is_from_cache BOOLEAN DEFAULT FALSE
            );
        """)
        # Yeni 'api_provider' sütununu ekle
        add_column_if_not_exists(cursor, 'YapayUstaUsage', 'api_provider', 'TEXT')
        conn.commit()
        logger.info("Yapay Usta API: 'YapayUstaUsage' tablosu başarıyla kontrol edildi/oluşturuldu.")
        cursor.close()
    except Exception as e:
        logger.error(f"Yapay Usta API: 'YapayUstaUsage' tablosu oluşturulurken hata: {e}")
    finally:
        conn.close()

# --- REDIS ÖNBELLEK BAĞLANTISI ---
try:
    redis_client = redis.from_url("redis://127.0.0.1:6379", decode_responses=True)
    redis_client.ping()
    logger.info("Yapay Usta API: Redis önbellek bağlantısı başarılı.")
except redis.exceptions.ConnectionError as e:
    logger.warning(f"Yapay Usta API: Redis'e bağlanılamadı, önbellekleme ve hız sınırlama devre dışı kalacak. Hata: {e}")
    redis_client = None

# --- API SAĞLAYICILARI YAPILANDIRMASI ---
API_PROVIDERS = []

# Google Gemini'yi yapılandır
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if GOOGLE_API_KEY:
    try:
        genai.configure(api_key=GOOGLE_API_KEY)
        gemini_model = genai.GenerativeModel('gemini-2.5-flash-lite')
        API_PROVIDERS.append({
            "name": "gemini",
            "client": gemini_model,
            "model_name": "gemini-2.5-flash-lite",
            "rpm_limit": 15,
            "rpd_limit": 1000,
        })
        logger.info(f"Yapay Usta API: Google Gemini API başarıyla yüklendi.")
    except Exception as e:
        logger.error(f"Yapay Usta API: Google Gemini API yapılandırılamadı! Hata: {e}")

# DeepSeek'i yapılandır
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
if DEEPSEEK_API_KEY:
    try:
        deepseek_client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com/v1")
        API_PROVIDERS.append({
            "name": "deepseek",
            "client": deepseek_client,
            "model_name": "deepseek-chat",
            "rpm_limit": 60,  # Örnek limit, gerekirse değiştirilebilir
            "rpd_limit": 5000, # Örnek limit
        })
        logger.info(f"Yapay Usta API: DeepSeek API başarıyla yüklendi.")
    except Exception as e:
        logger.error(f"Yapay Usta API: DeepSeek API yapılandırılamadı! Hata: {e}")

# --- YARDIMCI FONKSİYONLAR ---
def log_usage_to_db(provider, user_identifier, prompt, response, p_tokens, r_tokens, is_cache=False):
    conn = get_db_connection()
    if not conn: return
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO YapayUstaUsage (user_identifier, prompt, response, prompt_tokens, response_tokens, is_from_cache, api_provider)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (user_identifier, prompt, response, p_tokens, r_tokens, is_cache, provider)
        )
        conn.commit()
        cursor.close()
    except Exception as db_error:
        logger.error(f"Yapay Usta API: Veritabanına '{provider}' logu yazılırken hata: {db_error}")
    finally:
        conn.close()

def _call_gemini(client, prompt):
    response = client.generate_content(prompt)
    prompt_tokens = response.usage_metadata.prompt_token_count
    response_tokens = response.usage_metadata.candidates_token_count
    return response.text, prompt_tokens, response_tokens

def _call_deepseek(client, prompt):
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": "Sen Araca Bak web sitesinin 'Yapay Usta' isimli, araçlar ve bakımları konusunda uzman bir yapay zeka asistanısın. Kısa, net ve yardımsever cevaplar ver."},
            {"role": "user", "content": prompt.split("'")[-2]} # Sadece kullanıcı sorusunu al
        ],
        stream=False
    )
    prompt_tokens = response.usage.prompt_tokens
    response_tokens = response.usage.completion_tokens
    return response.choices[0].message.content, prompt_tokens, response_tokens

# --- API ENDPOINT ---
@yapayusta_bp.route('/api/yapayusta/generate', methods=['POST'])
def generate_chat_response():
    if not API_PROVIDERS:
        return jsonify({'error': 'Hiçbir yapay zeka sağlayıcısı yapılandırılamadığı için servis devre dışı.'}), 503

    data = request.get_json()
    if not data or 'prompt' not in data:
        return jsonify({'error': 'Lütfen bir "prompt" (soru) gönderin.'}), 400

    prompt = data['prompt'].strip()
    is_logged_in = 'user_id' in session
    user_identifier = str(session.get('user_id')) if is_logged_in else request.remote_addr

    # 1. ÖNBELLEK KONTROLÜ
    if redis_client:
        cache_key = "yapayusta:" + hashlib.sha256(prompt.lower().encode('utf-8')).hexdigest()
        cached_response = redis_client.get(cache_key)
        if cached_response:
            logger.info(f"Yapay Usta API: Yanıt önbellekten sunuldu. Kimlik: {user_identifier}")
            log_usage_to_db('cache', user_identifier, prompt, cached_response, 0, 0, True)
            return jsonify({'text': cached_response})

    # 2. MİSAFİR KULLANICI LİMİTİ
    if not is_logged_in and redis_client:
        try:
            guest_limit_key = f"yapayusta:limit:daily:guest:{user_identifier}"
            if int(redis_client.get(guest_limit_key) or 0) >= 2:
                logger.warning(f"Yapay Usta API: Misafir kullanıcı günlük limitini aştı. IP: {user_identifier}")
                return jsonify({'error': 'Ücretsiz deneme limitinize ulaştınız. Devam etmek için lütfen giriş yapın.'}), 429
        except Exception as e:
            logger.error(f"Yapay Usta API: Redis ile misafir limiti kontrolü sırasında hata: {e}")
    
    # 3. AKILLI YÖNLENDİRİCİ (SMART ROUTER)
    full_prompt = f"Sen Araca Bak web sitesinin 'Yapay Usta' isimli, araçlar ve bakımları konusunda uzman bir yapay zeka asistanısın. Kısa, net ve yardımsever cevaplar ver. Kullanıcının sorusu şu: '{prompt}'"

    for provider in API_PROVIDERS:
        provider_name = provider['name']
        
        # API'nin günlük ve dakikalık limitlerini kontrol et
        if redis_client:
            try:
                rpm_key = f"yapayusta:rpm:{provider_name}"
                rpd_key = f"yapayusta:rpd:{provider_name}"
                
                # Dakikalık limiti kontrol et
                if int(redis_client.get(rpm_key) or 0) >= provider['rpm_limit']:
                    logger.warning(f"'{provider_name}' için RPM limiti ({provider['rpm_limit']}) aşıldı. Sonraki sağlayıcı deneniyor.")
                    continue
                
                # Günlük limiti kontrol et
                if int(redis_client.get(rpd_key) or 0) >= provider['rpd_limit']:
                    logger.warning(f"'{provider_name}' için RPD limiti ({provider['rpd_limit']}) aşıldı. Sonraki sağlayıcı deneniyor.")
                    continue
            except Exception as e:
                logger.error(f"Redis ile '{provider_name}' limit kontrolü hatası: {e}")
                continue # Redis hatasında bir sonraki API'yi dene

        # API'yi çağırmayı dene
        try:
            logger.info(f"Yapay Usta API: '{provider_name}' deniyor. Kimlik: {user_identifier}")
            response_text, p_tokens, r_tokens = (None, 0, 0)

            if provider_name == 'gemini':
                response_text, p_tokens, r_tokens = _call_gemini(provider['client'], full_prompt)
            elif provider_name == 'deepseek':
                response_text, p_tokens, r_tokens = _call_deepseek(provider['client'], full_prompt)

            if response_text:
                # Başarılı, limitleri artır ve logla
                if redis_client:
                    # RPM sayacını 60 saniyeliğine artır
                    redis_client.incr(rpm_key)
                    if redis_client.ttl(rpm_key) == -1: redis_client.expire(rpm_key, 60)
                    # RPD sayacını 24 saatliğine artır
                    redis_client.incr(rpd_key)
                    if redis_client.ttl(rpd_key) == -1: redis_client.expire(rpd_key, 86400)
                
                if not is_logged_in and redis_client:
                    redis_client.incr(f"yapayusta:limit:daily:guest:{user_identifier}")
                    redis_client.expire(f"yapayusta:limit:daily:guest:{user_identifier}", 86400)

                # Yanıtı ana önbelleğe ve veritabanına kaydet
                redis_client.setex(cache_key, timedelta(hours=24), response_text)
                log_usage_to_db(provider_name, user_identifier, prompt, response_text, p_tokens, r_tokens)
                
                return jsonify({'text': response_text})

        except RateLimitError:
            logger.warning(f"'{provider_name}' API hız limitine takıldı. Sonraki sağlayıcı deneniyor.")
            continue # Limite takılınca bir sonrakini dene
        except APIConnectionError:
            logger.error(f"'{provider_name}' API'ye bağlanılamadı. Sonraki sağlayıcı deneniyor.")
            continue
        except Exception as e:
            logger.error(f"'{provider_name}' ile iletişimde kritik hata: {e}", exc_info=True)
            continue # Diğer hatalarda da bir sonrakini dene

    # Tüm sağlayıcılar denendi ve başarısız oldu
    logger.error("Tüm yapay zeka sağlayıcılarının limitleri dolu veya çevrimdışı.")
    return jsonify({'error': 'Yapay zeka asistanımız şu an aşırı yoğun. Lütfen birkaç dakika sonra tekrar deneyin.'}), 503

initialize_database()
