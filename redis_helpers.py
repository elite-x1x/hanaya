import redis, json, logging
from .config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

redis_client = None

def connect_redis():
    global redis_client
    for attempt in range(5):
        try:
            redis_client = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD,
                ssl=True, decode_responses=True, socket_connect_timeout=10, socket_timeout=10
            )
            redis_client.ping()
            logging.info("✅ Redis terhubung!")
            return
        except Exception as e:
            logging.error(f"❌ Redis gagal (percobaan {attempt+1}/5): {e}")
    logging.error("❌ Redis tidak bisa terhubung, fallback ke lokal")
    redis_client = None

def r_get(key): return redis_client.get(key) if redis_client else None
def r_set(key, value): 
    if redis_client: redis_client.set(key, value)
def r_sismember(key, value): return redis_client.sismember(key, value) if redis_client else False
def r_sadd_with_ttl(key, value, ttl):
    if redis_client:
        pipe = redis_client.pipeline()
        pipe.sadd(key, value); pipe.expire(key, ttl); pipe.execute()
def r_set_json(key, data): 
    if redis_client: redis_client.set(key, json.dumps(data))
def r_get_json(key):
    if redis_client:
        data = redis_client.get(key)
        if data: return json.loads(data)
    return None
