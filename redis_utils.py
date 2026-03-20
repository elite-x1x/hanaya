import redis.asyncio as redis
import logging
import asyncio
import json
from config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

redis_client: redis.Redis | None = None

# === KONEKSI REDIS ===
async def connect_redis_async() -> None:
    global redis_client
    if not REDIS_HOST:
        logging.warning("⚠️ REDIS_HOST kosong, skip koneksi Redis")
        redis_client = None
        return

    for attempt in range(5):
        try:
            client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                ssl=True,
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=10,
            )
            await client.ping()
            redis_client = client
            logging.info("✅ Redis terhubung!")
            return
        except Exception as e:
            logging.error(f"❌ Redis gagal (percobaan {attempt+1}/5): {e}")
            await asyncio.sleep(5)

    logging.error("❌ Redis tidak bisa terhubung, fallback ke lokal")
    redis_client = None

async def ensure_redis() -> None:
    global redis_client
    if redis_client is None:
        return
    try:
        await redis_client.ping()
    except Exception:
        logging.warning("⚠️ Redis terputus, mencoba reconnect...")
        await connect_redis_async()

# === HELPER FUNGSI ===
async def r_get(key: str) -> str | None:
    if redis_client is None:
        return None
    try:
        await ensure_redis()
        return await redis_client.get(key)
    except Exception as e:
        logging.error(f"Redis GET error [{key}]: {e}")
        return None

async def r_set(key: str, value: str) -> None:
    if redis_client is None:
        return
    try:
        await ensure_redis()
        await redis_client.set(key, value)
    except Exception as e:
        logging.error(f"Redis SET error [{key}]: {e}")

async def r_set_json(key: str, data: dict) -> None:
    if redis_client is None:
        return
    try:
        await ensure_redis()
        await redis_client.set(key, json.dumps(data))
    except Exception as e:
        logging.error(f"Redis SET JSON error [{key}]: {e}")

async def r_get_json(key: str) -> dict | None:
    if redis_client is None:
        return None
    try:
        await ensure_redis()
        raw = await redis_client.get(key)
        if raw:
            return json.loads(raw)
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Redis JSON decode error [{key}]: {e}")
        try:
            await redis_client.delete(key)
            logging.warning(f"🗑️ Key korup berhasil dihapus: {key}")
        except Exception as del_err:
            logging.error(f"❌ Gagal hapus key korup [{key}]: {del_err}")
    except Exception as e:
        logging.error(f"Redis GET JSON error [{key}]: {e}")
    return None

async def r_sadd_with_ttl(key: str, value: str, ttl: int) -> None:
    if redis_client is None:
        return
    try:
        await ensure_redis()
        async with redis_client.pipeline() as pipe:
            pipe.sadd(key, value)
            pipe.expire(key, ttl)
            await pipe.execute()
    except Exception as e:
        logging.error(f"Redis SADD+TTL error [{key}]: {e}")

async def r_sismember(key: str, value: str) -> bool:
    if redis_client is None:
        return False
    try:
        await ensure_redis()
        return bool(await redis_client.sismember(key, value))
    except Exception as e:
        logging.error(f"Redis SISMEMBER error [{key}]: {e}")
        return False
