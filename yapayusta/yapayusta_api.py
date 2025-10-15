import os
import sys
import google.generativeai as genai
from dotenv import load_dotenv
import logging
from flask import Blueprint, request, jsonify, session
import redis
import hashlib
from datetime import timedelta

# --- YAPILANDIRMA ---
logger = logging.getLogger(__name__)
# Geliştirme ortamı için .env yolunu esnek hale getirebiliriz.
# Sunucu için mutlak yol kullanmak daha güvenilirdir.
dotenv_path = '/var/www/aracabak.com/private/secrets/.env'
if not os.path.exists(dotenv_path):
    dotenv_path = None # Yerelde .env kullanılmasına izin ver
load_dotenv(dotenv_path=dotenv_path)

yapayusta_bp = Blueprint('yapayusta_api', __name__)

# --- REDIS ÖNBELLEK BAĞLANTISI ---
try:
    redis_client = redis.from_url("redis://127.0.0.1:6379", decode_responses=True)
    redis_client.ping()
    logger.info("Yapay Usta API: Redis önbellek bağlantısı başarılı.")
except redis.exceptions.ConnectionError as e:
    logger.warning(f"Yapay Usta API: Redis'e bağlanılamadı, önbellekleme ve hız sınırlama devre dışı kalacak. Hata: {e}")
    redis_client = None

# --- API ANAHTARI VE MODEL YAPILANDIRMASI ---
API_KEY = os.getenv("GOOGLE_API_KEY")
MODEL = None

if not API_KEY:
    logger.critical("Yapay Usta API: GOOGLE_API_KEY ortam değişkeni .env dosyasında bulunamadı veya boş!")
else:
    try:
        genai.configure(api_key=API_KEY)
        # MODEL OPTİMİZASYONU: Model, uygulama başlangıcında bir kez yüklenir.
        MODEL = genai.GenerativeModel('gemini-2.5-flash-lite')
        logger.info(f"Yapay Usta API: Google API yapılandırıldı ve '{MODEL.model_name}' modeli yüklendi.")
    except Exception as e:
        logger.critical(f"Yapay Usta API: Google API veya model yapılandırılamadı! Hata: {e}", exc_info=True)


# --- API ENDPOINT ---
@yapayusta_bp.route('/api/yapayusta/generate', methods=['POST'])
def generate_chat_response():
    if not API_KEY or not MODEL:
        return jsonify({'error': 'API anahtarı veya AI modeli yapılandırılamadığı için servis devre dışı.'}), 503

    # YENİ - KORUMA 1: GENEL HIZ LİMİTİ (4000 RPM)
    # Bu limit, Google API anahtarını genel aşırı kullanımdan korur.
    if redis_client:
        try:
            global_minute_key = "yapayusta:limit:global:minute"
            current_global_requests = redis_client.incr(global_minute_key)
            if current_global_requests == 1:
                redis_client.expire(global_minute_key, 60)
            if current_global_requests > 4000:
                logger.critical(f"Yapay Usta API: GLOBAL HIZ LİMİTİ (4000 RPM) AŞILDI! Mevcut istek: {current_global_requests}")
                return jsonify({'error': 'Servis şu anda aşırı yoğun. Lütfen bir dakika sonra tekrar deneyin.'}), 503
        except Exception as e:
            logger.error(f"Yapay Usta API: Redis ile global limit kontrolü sırasında hata oluştu: {e}")

    data = request.get_json()
    if not data or 'prompt' not in data:
        return jsonify({'error': 'Lütfen bir "prompt" (soru) gönderin.'}), 400

    prompt = data['prompt'].strip().lower()
    ip_address = request.remote_addr
    is_logged_in = 'user_id' in session

    # KORUMA 2: ÖNCE ÖNBELLEĞİ KONTROL ET (Tüm kullanıcılar için ücretsiz)
    if redis_client:
        cache_key = "yapayusta:" + hashlib.sha256(prompt.encode('utf-8')).hexdigest()
        cached_response = redis_client.get(cache_key)
        if cached_response:
            logger.info(f"Yapay Usta API: Yanıt önbellekten (Redis) sunuldu. IP: {ip_address}")
            return jsonify({'text': cached_response})

    # KORUMA 3: KULLANICI TİPİNE GÖRE LİMİTLERİ UYGULA
    if redis_client:
        try:
            # Giriş yapmış kullanıcılar için daha cömert hız limiti
            if is_logged_in:
                user_id = session['user_id']
                minute_limit_key = f"yapayusta:limit:minute:user:{user_id}"
                current_requests = redis_client.incr(minute_limit_key)
                if current_requests == 1:
                    redis_client.expire(minute_limit_key, 60)
                # GÜNCELLENDİ: Limit 100'e yükseltildi.
                if current_requests > 100:
                    logger.warning(f"Yapay Usta API: Giriş yapmış kullanıcı için hız limiti (100) aşıldı. User ID: {user_id}")
                    return jsonify({'error': 'Çok fazla istek gönderdiniz. Lütfen biraz yavaşlayın.'}), 429
            # Misafir kullanıcılar için GÜNLÜK 2 SORU limiti
            else:
                daily_limit_key = f"yapayusta:limit:daily:guest:{ip_address}"
                current_daily_requests = redis_client.get(daily_limit_key)
                
                if current_daily_requests and int(current_daily_requests) >= 2:
                    logger.warning(f"Yapay Usta API: Misafir kullanıcı için günlük limit (2) aşıldı. IP: {ip_address}")
                    return jsonify({'error': 'Ücretsiz deneme limitinize ulaştınız. Daha fazla soru sormak için lütfen giriş yapın veya kayıt olun.'}), 429
        
        except Exception as e:
            logger.error(f"Yapay Usta API: Redis ile kullanıcı limiti kontrolü sırasında hata oluştu: {e}")


    # Eğer limitler aşılmadıysa, Google API'ye git
    try:
        logger.info(f"Yapay Usta API: '{MODEL.model_name}' modeli için istek alındı. IP: {ip_address}, Giriş Yapmış: {is_logged_in}")
        
        full_prompt = f"Sen Araca Bak web sitesinin 'Yapay Usta' isimli, araçlar ve bakımları konusunda uzman bir yapay zeka asistanısın. Kısa, net ve yardımsever cevaplar ver. Kullanıcının sorusu şu: '{prompt}'"

        response = MODEL.generate_content(full_prompt)
        
        # YANITI ÖNBELLEĞE VE LİMİT SAYACINI REDIS'E KAYDET
        if redis_client and hasattr(response, 'text'):
            # Yanıtı 24 saatliğine önbelleğe al
            redis_client.setex(cache_key, timedelta(hours=24), response.text)
            
            # Eğer misafir kullanıcı ise, günlük sayacını artır
            if not is_logged_in:
                daily_limit_key = f"yapayusta:limit:daily:guest:{ip_address}"
                daily_req_count = redis_client.incr(daily_limit_key)
                if daily_req_count == 1: # Eğer günün ilk isteğiyse, süreyi 24 saat yap
                    redis_client.expire(daily_limit_key, timedelta(days=1))
                logger.info(f"Yapay Usta API: Yanıt önbelleğe kaydedildi. Misafir IP: {ip_address}, Günlük İstek: {daily_req_count}/2")
            else:
                logger.info(f"Yapay Usta API: Yanıt önbelleğe kaydedildi. Giriş yapmış kullanıcı: {session['user_id']}")

        return jsonify({'text': response.text})

    except Exception as e:
        logger.error(f"Yapay Usta API: Google API ile iletişimde kritik hata oluştu: {e}", exc_info=True)
        return jsonify({'error': 'Yapay zeka ile iletişim kurarken bir sorun oluştu.'}), 500
