import logging
import json
import os
import asyncio
import random
import hashlib
import re
import signal
import sys
from collections import OrderedDict
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

from telegram import Update, InputMediaVideo
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters, CommandHandler
from dotenv import load_dotenv
import redis.asyncio as aioredis

load_dotenv()

start_time = datetime.now(timezone.utc)
# ============================================================
# === KONFIGURASI UTAMA ===
# ============================================================
BOT_TOKEN      = os.getenv("BOT_F_LOKAL", "")
TARGET_CHAT_ID = int(os.getenv("CHAT_ID_BEDUL", 0))
ADMIN_CHAT_ID  = int(os.getenv("CHAT_ID_ADMIN", 0))

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN tidak diatur di .env")
if TARGET_CHAT_ID == 0:
    raise ValueError("❌ CHAT_ID_TARGET tidak diatur di .env")
if ADMIN_CHAT_ID == 0:
    logging.warning("⚠️ ADMIN_CHAT_ID tidak diatur — flood alert dinonaktifkan")

# ============================================================
# === ADMIN CONFIG ===
# ============================================================
def _parse_id_list(env_key: str) -> list[int]:
    raw = os.getenv(env_key, "")
    result = []
    for uid in raw.split(","):
        uid = uid.strip()
        if uid:
            try:
                result.append(int(uid))
            except ValueError:
                logging.warning(f"⚠️ ID tidak valid di {env_key}: '{uid}'")
    return result

ADMINS: dict[int, str] = {}
for _uid in _parse_id_list("SUPERADMINS"):
    ADMINS[_uid] = "superadmin"
for _uid in _parse_id_list("MODERATORS"):
    ADMINS[_uid] = "moderator"

if not ADMINS:
    logging.warning("⚠️ Tidak ada admin terdaftar — semua command admin akan ditolak")

# ============================================================
# === REDIS CONFIG ===
# ============================================================
REDIS_HOST     = os.getenv("REDIS_HOST", "")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

if not REDIS_HOST:
    logging.warning("⚠️ REDIS_HOST tidak diatur — fallback ke mode lokal")

# ============================================================
# === REDIS KEYS ===
# ============================================================
KEY_SENT        = "hanaya:sent"
KEY_PENDING     = "hanaya:pending"
KEY_DAILY_COUNT = "hanaya:daily_count"
KEY_DAILY_DATE  = "hanaya:daily_date"
KEY_FLOOD_CTRL  = "hanaya:flood_ctrl"
KEY_DAILY_LIMIT = "hanaya:daily_limit"
KEY_SEND_DELAY  = "hanaya:send_delay"

# ============================================================
# === TTL CONFIG ===
# ============================================================
SENT_TTL = 60 * 60 * 24 * 30  # 30 hari

# ============================================================
# === RATE LIMIT SETTINGS ===
# ============================================================
DELAY_BETWEEN_SEND      = 2.0
DELAY_RANDOM_MIN        = 0.5
DELAY_RANDOM_MAX        = 2.5
GROUP_SIZE              = 5
DELAY_BETWEEN_GROUP_MIN = 20
DELAY_BETWEEN_GROUP_MAX = 40
WAIT_TIME               = 20
BATCH_PAUSE_EVERY       = 30
BATCH_PAUSE_MIN         = 300
BATCH_PAUSE_MAX         = 600
DAILY_LIMIT             = 2500
MAX_RETRIES             = 3
MAX_QUEUE_SIZE          = 7000
AUTO_SAVE_INTERVAL      = 60

# ============================================================
# === SMART FLOOD CONTROL SETTINGS ===
# ============================================================
FLOOD_RANDOM_MIN     = 10
FLOOD_RANDOM_MAX     = 30
FLOOD_PENALTY_STEP   = 15
FLOOD_MAX_PENALTY    = 300
FLOOD_RESET_AFTER    = 600
FLOOD_WARN_THRESHOLD = 3

# ============================================================
# === ENHANCED LOGGING SYSTEM ===
# ============================================================
class CompactFormatter(logging.Formatter):
    """Formatter yang ringkas, hanya tampilkan error essentials tanpa full traceback"""
    
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'
    }
    
    def format(self, record):
        if self._is_network_error(record):
            return self._format_network_error(record)
        
    #    timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
        timestamp = datetime.fromtimestamp(record.created).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        level = record.levelname
        message = record.getMessage()
        
        color = self.COLORS.get(level, '')
        reset = self.COLORS['RESET']
        
        if record.exc_info and record.levelno >= logging.ERROR:
            exc_type, exc_value, _ = record.exc_info
            exc_name = exc_type.__name__ if exc_type else "Unknown"
            return f"{timestamp} [{color}{level}{reset}] {message}\n    └─ {exc_name}: {str(exc_value)[:150]}"
        
        return f"{timestamp} [{color}{level}{reset}] {message}"
    
    @staticmethod
    def _is_network_error(record):
        network_errors = [
            'httpx.ReadError',
            'httpx.ConnectError',
            'httpx.TimeoutException',
            'ConnectionError',
            'TimeoutError',
            'OSError',
            'socket.error'
        ]

        if record.exc_info:
            exc_type = record.exc_info if record.exc_info else None
            exc_name = exc_type.__name__ if exc_type else ""
            
            return any(err in exc_name for err in network_errors)
        
        return any(err in record.getMessage() for err in network_errors)
    
    @staticmethod
    def _format_network_error(record):
    #	timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
        timestamp = datetime.fromtimestamp(record.created).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        level = record.levelname
        message = record.getMessage()
        color = CompactFormatter.COLORS.get(level, '')
        reset = CompactFormatter.COLORS['RESET']
        
        if record.exc_info:
            exc_type, exc_value, _ = record.exc_info
            exc_name = exc_type.__name__ if exc_type else "Unknown"
            exc_msg = str(exc_value)[:100]
            return f"{timestamp} [{color}{level}{reset}] {exc_name}: {exc_msg}"
        
        return f"{timestamp} [{color}{level}{reset}] {message}"


class NetworkErrorFilter(logging.Filter):
    
    def __init__(self, max_same_errors=5):
        super().__init__()
        self.error_cache = {}
        self.max_same_errors = max_same_errors
    
    def filter(self, record):
        
        if record.levelno >= logging.ERROR:
            exc_info = record.exc_info
            if exc_info:
                exc_type = exc_info  # ✅ Ambil type
                exc_name = exc_type.__name__ if exc_type else "Unknown"
                
                if exc_name in self.error_cache:
                    count, last_time = self.error_cache[exc_name]
                    now = datetime.now(timezone.utc).timestamp()
                     

                    if now - last_time > 60:  # Reset setelah 60 detik
                        self.error_cache[exc_name] = (1, now)
                    else:
                        self.error_cache[exc_name] = (count + 1, now)
                        if count >= self.max_same_errors:
                            return False
                else:
                    self.error_cache[exc_name] = (1, datetime.now(timezone.utc).timestamp())
        
        return True

# ============================================================
# === SETUP LOGGING (REPLACEMENT) ===
# ============================================================
_log_formatter = CompactFormatter()
_file_handler = RotatingFileHandler(
    "bot.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
_file_handler.setFormatter(_log_formatter)
_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_log_formatter)

network_filter = NetworkErrorFilter(max_same_errors=3)
_stream_handler.addFilter(network_filter)
_file_handler.addFilter(network_filter)

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _stream_handler])
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ============================================================
# === GLOBAL STATE ===
# ============================================================
pending_queue: asyncio.Queue = None
is_sending:       bool        = False
daily_count:      int         = 0
daily_reset_date              = datetime.now(timezone.utc).date()
last_save_time                = datetime.now(timezone.utc)
sending_lock                  = None
is_paused:        bool        = False
local_sent:       OrderedDict = OrderedDict()
MAX_LOCAL_SENT                = 5000
flood_ctrl:       dict | None = None
config_lock                  = None

# ============================================================
# === REDIS CONNECTION ===
# ============================================================
redis_client: aioredis.Redis | None = None

async def connect_redis_async() -> None:
    global redis_client
    if not REDIS_HOST:
        logging.warning("⚠️ REDIS_HOST kosong, skip koneksi Redis")
        redis_client = None
        return

    for attempt in range(5):
        try:
            client = aioredis.Redis(
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

# ============================================================
# === REDIS HELPERS ===
# ============================================================
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

async def r_sismember(key: str, value: str) -> bool:
    if redis_client is None:
        return False
    try:
        await ensure_redis()
        return bool(await redis_client.sismember(key, value))
    except Exception as e:
        logging.error(f"Redis SISMEMBER error [{key}]: {e}")
        return False

async def r_sadd_with_ttl(key: str, value: str, ttl: int) -> None:
    if redis_client is None:
        return
    try:
        await ensure_redis()
        pipe = redis_client.pipeline()
        pipe.sadd(key, value)
        pipe.expire(key, ttl)
        await pipe.execute()
    except Exception as e:
        logging.error(f"Redis SADD+TTL error [{key}]: {e}")

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
        if raw and raw.strip():
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

# ============================================================
# === MAKE HASH ===
# ============================================================
def make_hash(fingerprint: dict) -> str:
    fp_copy = {k: v for k, v in fingerprint.items() if k not in ("file_id", "timestamp")}
    return hashlib.md5(
        json.dumps(fp_copy, sort_keys=True).encode()
    ).hexdigest()

# ============================================================
# === PARSE RETRY AFTER ===
# ============================================================
def parse_retry_after(err: str) -> int:
    match = re.search(r"[Rr]etry in (\d+)", err)
    if match:
        return int(match.group(1))
    match = re.search(r"retry_after=(\d+)", err)
    if match:
        return int(match.group(1))
    return 60

# ============================================================
# === SMART FLOOD CONTROLLER ===
# ============================================================
class SmartFloodController:

    def __init__(self):
        self.flood_count:     int             = 0
        self.total_flood:     int             = 0
        self.last_flood_time: datetime | None = None
        self.penalty:         float           = 0.0
        self.is_cooling:      bool            = False
        self.group_delay_min: float           = float(DELAY_BETWEEN_GROUP_MIN)
        self.group_delay_max: float           = float(DELAY_BETWEEN_GROUP_MAX)

    @staticmethod
    def _parse_last_flood_time(value) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except (ValueError, TypeError) as e:
                logging.warning(f"⚠️ Gagal parse last_flood_time '{value}': {e}")
                return None
        logging.warning(f"⚠️ Tipe last_flood_time tidak dikenal: {type(value)}")
        return None

    async def record_flood(self, suggested_wait: int) -> float:
        now = datetime.now(timezone.utc)

        if self.last_flood_time is not None:
            elapsed = (now - self.last_flood_time).total_seconds()
            if elapsed > FLOOD_RESET_AFTER:
                logging.info(
                    f"🔄 Flood counter direset "
                    f"(tidak ada flood selama {elapsed:.0f}s)"
                )
                self.flood_count = 0
                self.penalty     = 0.0

        self.flood_count     += 1
        self.total_flood     += 1
        self.last_flood_time  = now
        self.is_cooling       = True
        self.penalty          = min(
            float(FLOOD_MAX_PENALTY),
            float(self.flood_count * FLOOD_PENALTY_STEP)
        )

        random_add = random.uniform(FLOOD_RANDOM_MIN, FLOOD_RANDOM_MAX)
        total_wait = float(suggested_wait) + random_add + self.penalty

        self.group_delay_min = min(120.0, DELAY_BETWEEN_GROUP_MIN + (self.flood_count * 5))
        self.group_delay_max = min(180.0, DELAY_BETWEEN_GROUP_MAX + (self.flood_count * 10))

        logging.warning(
            f"🚨 FLOOD #{self.flood_count} terdeteksi!\n"
            f"   ├─ Saran Telegram   : {suggested_wait}s\n"
            f"   ├─ Random tambahan  : {random_add:.1f}s\n"
            f"   ├─ Penalti kumulatif: {self.penalty:.0f}s\n"
            f"   ├─ Total tunggu     : {total_wait:.1f}s\n"
            f"   └─ Delay group baru : {self.group_delay_min:.0f}s - {self.group_delay_max:.0f}s"
        )

        await self.save_state()
        return total_wait

    async def record_success(self) -> None:
        if self.penalty > 0:
            self.penalty = max(0.0, self.penalty - 5.0)
        if self.flood_count > 0:
            self.flood_count = max(0, self.flood_count - 1)
            if self.flood_count == 0:
                self.last_flood_time = None
        if self.group_delay_min > DELAY_BETWEEN_GROUP_MIN:
            self.group_delay_min = max(
                float(DELAY_BETWEEN_GROUP_MIN), self.group_delay_min - 2.0
            )
        if self.group_delay_max > DELAY_BETWEEN_GROUP_MAX:
            self.group_delay_max = max(
                float(DELAY_BETWEEN_GROUP_MAX), self.group_delay_max - 3.0
            )
        self.is_cooling = False
        await self.save_state()

    def get_group_delay(self) -> float:
        return random.uniform(self.group_delay_min, self.group_delay_max)

    def get_status(self) -> str:
        return (
            f"FloodCtrl | Count: {self.flood_count} | "
            f"Total: {self.total_flood} | "
            f"Penalty: {self.penalty:.0f}s | "
            f"Cooling: {self.is_cooling}"
        )

    def to_dict(self) -> dict:
        return {
            "flood_count":     self.flood_count,
            "total_flood":     self.total_flood,
            "last_flood_time": self.last_flood_time.isoformat() if self.last_flood_time else None,
            "penalty":         self.penalty,
            "is_cooling":      self.is_cooling,
            "group_delay_min": self.group_delay_min,
            "group_delay_max": self.group_delay_max,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SmartFloodController":
        obj = cls()
        obj.flood_count     = int(data.get("flood_count", 0))
        obj.total_flood     = int(data.get("total_flood", 0))
        obj.last_flood_time = cls._parse_last_flood_time(data.get("last_flood_time"))
        obj.penalty         = float(data.get("penalty", 0.0))
        obj.is_cooling      = bool(data.get("is_cooling", False))
        obj.group_delay_min = float(data.get("group_delay_min", DELAY_BETWEEN_GROUP_MIN))
        obj.group_delay_max = float(data.get("group_delay_max", DELAY_BETWEEN_GROUP_MAX))
        return obj

    async def save_state(self) -> None:
        try:
            await r_set_json(KEY_FLOOD_CTRL, self.to_dict())
        except Exception as e:
            logging.error(f"❌ Gagal save flood state: {e}")

# ============================================================
# === LOCAL SENT HELPERS ===
# ============================================================
def _local_sent_add(key: str) -> None:
    try:
        local_sent[key] = True
        while len(local_sent) > MAX_LOCAL_SENT:
            local_sent.popitem(last=False)
    except Exception as e:
        logging.error(f"❌ Gagal simpan ke local_sent: {e}")

def _local_sent_check(key: str) -> bool:
    return key in local_sent

def _local_sent_cleanup() -> None:
    """Bersihkan local_sent jika mendekati batas"""
    if len(local_sent) > MAX_LOCAL_SENT * 0.8:
        to_remove = int(len(local_sent) * 0.1)
        for _ in range(to_remove):
            if local_sent:
                local_sent.popitem(last=False)
        logging.info(f"🧹 local_sent dibersihkan: hapus {to_remove} entry lama")

# ============================================================
# === DUPLICATE CHECK ===
# ============================================================
async def is_duplicate(file_id: str, fp_hash: str) -> bool:
    # Layer 1: Redis
    if await r_sismember(KEY_SENT, file_id):
        return True
    if await r_sismember(KEY_SENT, fp_hash):
        return True

    # Layer 2: Local
    if _local_sent_check(file_id):
        return True
    if _local_sent_check(fp_hash):
        return True

    return False

async def mark_sent(file_id: str, fp_hash: str) -> None:
    # Redis
    try:
        await r_sadd_with_ttl(KEY_SENT, file_id, SENT_TTL)
        await r_sadd_with_ttl(KEY_SENT, fp_hash, SENT_TTL)
    except Exception as e:
        logging.warning(f"⚠️ Redis mark_sent error, fallback ke local: {e}")

    # Local (selalu simpan sebagai fallback)
    _local_sent_add(file_id)
    _local_sent_add(fp_hash)

# ============================================================
# === DAILY COUNTER ===
# ============================================================
async def check_daily_reset() -> None:
    global daily_count, daily_reset_date
    today = datetime.now(timezone.utc).date()
    if today != daily_reset_date:
        logging.info(f"🔄 Reset harian | Kemarin terkirim: {daily_count}")
        daily_count      = 0
        daily_reset_date = today
        await save_daily()
        await load_daily_limit()  # ✅ Perbarui limit setelah reset

async def save_daily() -> None:
    await r_set(KEY_DAILY_COUNT, str(daily_count))
    await r_set(KEY_DAILY_DATE, daily_reset_date.isoformat())

async def load_daily_limit() -> None:
    global DAILY_LIMIT, DELAY_BETWEEN_SEND
    try:
        limit_str = await r_get(KEY_DAILY_LIMIT)
        if limit_str:
            DAILY_LIMIT = int(limit_str)
            logging.info(f"📥 Daily limit dimuat: {DAILY_LIMIT}")
    except (ValueError, TypeError) as e:
        logging.warning(f"⚠️ Gagal load daily limit: {e}")

    try:
        delay_str = await r_get(KEY_SEND_DELAY)
        if delay_str:
            DELAY_BETWEEN_SEND = float(delay_str)
            logging.info(f"📥 Send delay dimuat: {DELAY_BETWEEN_SEND}s")
    except (ValueError, TypeError) as e:
        logging.warning(f"⚠️ Gagal load send delay: {e}")

# ============================================================
# === PERSIST: SAVE & LOAD ALL ===
# ============================================================
async def save_all() -> None:
    global last_save_time

    # Pending queue → list untuk disimpan
    pending_snapshot = list(pending_queue._queue) if pending_queue else []
    try:
        await r_set(KEY_PENDING, json.dumps(pending_snapshot))
    except Exception as e:
        logging.error(f"❌ Gagal save pending: {e}")

    await save_daily()

    if flood_ctrl:
        await flood_ctrl.save_state()

    last_save_time = datetime.now(timezone.utc)
    logging.debug(f"💾 Auto-save selesai | Pending: {len(pending_snapshot)}")

async def load_all() -> None:
    global daily_count, daily_reset_date

    # Load pending
    try:
        raw = await r_get(KEY_PENDING)
        if raw and raw.strip():
            loaded = json.loads(raw)
            if len(loaded) > MAX_QUEUE_SIZE:
                logging.warning(
                    f"⚠️ Pending terlalu besar ({len(loaded)}), dipotong ke {MAX_QUEUE_SIZE}"
                )
                loaded = loaded[-MAX_QUEUE_SIZE:]
            for item in loaded:
                try:
                    if not pending_queue.full():
                        await pending_queue.put(item)
                except Exception as e:
                    logging.warning(f"⚠️ Skip item pending korup: {e}")
            logging.info(f"📥 Pending dimuat: {pending_queue.qsize()} item")
        else:
            logging.info("📥 Tidak ada pending tersimpan")
    except Exception as e:
        logging.error(f"❌ Gagal load pending, reset: {e}")

    # Load daily count
    try:
        count_str = await r_get(KEY_DAILY_COUNT)
        if count_str:
            daily_count = int(count_str)
    except (ValueError, TypeError) as e:
        logging.warning(f"⚠️ Gagal load daily count: {e}")

    # Load daily date
    try:
        date_str = await r_get(KEY_DAILY_DATE)
        if date_str:
            daily_reset_date = datetime.fromisoformat(date_str).date()
    except (ValueError, TypeError) as e:
        logging.warning(f"⚠️ Gagal load daily date: {e}")

    # Load daily limit & delay
    await load_daily_limit()

    # Load flood ctrl
    try:
        flood_data = await r_get_json(KEY_FLOOD_CTRL)
        if flood_data:
            global flood_ctrl
            flood_ctrl = SmartFloodController.from_dict(flood_data)
            logging.info(f"📥 Flood ctrl dimuat: {flood_ctrl.get_status()}")
    except Exception as e:
        logging.warning(f"⚠️ Gagal load flood ctrl: {e}")

    logging.info(
        f"📊 State dimuat | Daily: {daily_count}/{DAILY_LIMIT} | "
        f"Pending: {pending_queue.qsize()}"
    )

# ============================================================
# === FINGERPRINT ===
# ============================================================
def get_fingerprint(msg) -> dict:
    fp = {
        "file_id":    None,
        "media_type": None,
        "file_size":  None,
        "duration":   None,
        "width":      None,
        "height":     None,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }
    if msg.video:
        v = msg.video
        fp.update({
            "file_id":    v.file_id,
            "media_type": "video",
            "file_size":  v.file_size,
            "duration":   v.duration,
            "width":      v.width,
            "height":     v.height,
        })
    elif msg.document:
        d = msg.document
        fp.update({
            "file_id":    d.file_id,
            "media_type": "document",
            "file_size":  d.file_size,
        })
    return fp

def _flatten_arg(arg) -> str | None:
    """Flatten nested list/tuple ke string"""
    while isinstance(arg, (list, tuple)):
        if len(arg) == 0:
            return None
        arg = arg  # ✅ Fix: ambil elemen pertama
    return str(arg).strip() if arg is not None else None

# ============================================================
# === ADMIN HELPERS ===
# ============================================================
def get_role(update: Update) -> str | None:
    uid = update.effective_user.id if update.effective_user else None
    return ADMINS.get(uid)

def is_superadmin(update: Update) -> bool:
    return get_role(update) == "superadmin"

def is_moderator(update: Update) -> bool:
    return get_role(update) in ("superadmin", "moderator")

# ============================================================
# === SEND MEDIA GROUP WITH RETRY ===
# ============================================================
async def send_media_group_with_retry(
    bot,
    chat_id: int,
    media_items: list[tuple[str, str]],
    max_retries: int = MAX_RETRIES,
) -> bool:
    media_group = [
        InputMediaVideo(media=fid)
        for fid, mtype in media_items
        if mtype == "video"
    ]
    if not media_group:
        return False

    attempt = 0
    while attempt < max_retries:
        try:
            await asyncio.wait_for(
                bot.send_media_group(chat_id=chat_id, media=media_group),
                timeout=60,
            )
            return True

        except asyncio.TimeoutError:
            logging.warning(f"⏱️ Timeout kirim grup (attempt {attempt+1}/{max_retries})")
            attempt += 1
            await asyncio.sleep(10)

        except Exception as e:
            err = str(e).lower()

            # Flood wait
            if "flood" in err or "too many requests" in err or "retry" in err:
                suggested = parse_retry_after(str(e))
                total_wait = await flood_ctrl.record_flood(suggested)
                logging.warning(f"⏳ Flood wait {total_wait:.1f}s sebelum retry...")
                await asyncio.sleep(total_wait)
                # ✅ Tidak increment attempt — retry attempt yang sama
                continue

            # Chat tidak ditemukan / migrasi
            elif "chat not found" in err or "forbidden" in err:
                migrated = re.search(r"migrated to chat (\-?\d+)", str(e))
                if migrated:
                    new_id = int(migrated.group(1))
                    logging.info(f"🔄 Chat dipindahkan ke {new_id}, update target...")
                    global TARGET_CHAT_ID
                    TARGET_CHAT_ID = new_id
                    await r_set("CHAT_ID_BEDUL", str(new_id))
                    attempt += 1
                    continue
                logging.error(f"❌ Chat tidak ditemukan / forbidden: {e}")
                return False

            # Error lainnya
            else:
                logging.error(f"❌ Gagal kirim (attempt {attempt+1}/{max_retries}): {e}")
                attempt += 1
                await asyncio.sleep(5 * attempt)

    logging.error(f"❌ Gagal kirim setelah {max_retries} percobaan")
    return False

# ============================================================
# === QUEUE WORKER ===
# ============================================================
async def queue_worker(bot) -> None:
    global daily_count, is_sending, is_paused
    global flood_ctrl

    if flood_ctrl is None:
        flood_ctrl = SmartFloodController()

    logging.info("🚀 Queue worker dimulai")
    sent_since_pause = 0
    iteration_count  = 0

    while True:
        try:
            # Cek pause
            if is_paused:
                await asyncio.sleep(5)
                continue

            # Cek daily limit
            await check_daily_reset()
            if daily_count >= DAILY_LIMIT:
                logging.info(f"🛑 Daily limit tercapai ({daily_count}/{DAILY_LIMIT})")
                await asyncio.sleep(60)
                continue

            # Kumpulkan batch dari queue
            batch: list = []
            try:
                # Ambil item pertama (tunggu maksimal 2 detik)
                first = await asyncio.wait_for(pending_queue.get(), timeout=2.0)
                batch.append(first)

                # Ambil item tambahan tanpa tunggu
                while len(batch) < GROUP_SIZE:
                    try:
                        item = pending_queue.get_nowait()
                        batch.append(item)
                    except asyncio.QueueEmpty:
                        break

            except asyncio.TimeoutError:
                # Queue kosong
                await asyncio.sleep(1)
                continue

            if not batch:
                continue

            # Proses batch
            is_sending = True
            media_items = []

            for item in batch:
                try:
                    file_id, media_type, fp = item
                    fp_hash = make_hash(fp)

                    # Cek duplikat
                    if await is_duplicate(file_id, fp_hash):
                        logging.info(f"⏭️ Skip duplikat: {file_id[:8]}...")
                        pending_queue.task_done()
                        continue

                    media_items.append((file_id, media_type, fp_hash))

                except (ValueError, TypeError) as e:
                    logging.warning(f"⚠️ Item queue korup, skip: {e}")
                    pending_queue.task_done()
                    continue

            if not media_items:
                is_sending = False
                continue

            # Kirim
            send_list = [(fid, mtype) for fid, mtype, _ in media_items]
            success = await send_media_group_with_retry(bot, TARGET_CHAT_ID, send_list)

            if success:
                async with sending_lock:
                    daily_count += len(media_items)

                sent_since_pause += len(media_items)

                # Mark sent
                for file_id, media_type, fp_hash in media_items:
                    await mark_sent(file_id, fp_hash)

                await flood_ctrl.record_success()
                await save_daily()

                # Hitung delay yang akan digunakan
                group_delay  = flood_ctrl.get_group_delay()
                send_delay   = DELAY_BETWEEN_SEND + random.uniform(
                    DELAY_RANDOM_MIN, DELAY_RANDOM_MAX
                )
                total_delay  = group_delay + send_delay

                logging.info(
                    f"✅ Terkirim {len(media_items)} media | "
                    f"Daily: {daily_count}/{DAILY_LIMIT} | "
                    f"Pending: {pending_queue.qsize()} | "
                    f"⏳ Tunggu: {total_delay:.1f}s"
                )

            else:
                # Kembalikan ke queue jika gagal
                for file_id, media_type, fp_hash in media_items:
                    try:
                        pending_queue.put_nowait((file_id, media_type, {"file_id": file_id}))
                    except asyncio.QueueFull:
                        logging.warning(f"⚠️ Queue penuh, drop: {file_id[:8]}...")

                logging.error(
                    f"❌ Gagal kirim {len(media_items)} media, "
                    f"dikembalikan ke queue"
                )

            # Task done untuk semua item batch
            for _ in batch:
                try:
                    pending_queue.task_done()
                except Exception:
                    pass

            is_sending = False

            # Auto-save periodik
            now = datetime.now(timezone.utc)
            if (now - last_save_time).total_seconds() >= AUTO_SAVE_INTERVAL:
                await save_all()

            # Cleanup local_sent periodik
            iteration_count += 1
            if iteration_count % 100 == 0:
                _local_sent_cleanup()

            # Batch pause setelah BATCH_PAUSE_EVERY item
            if sent_since_pause >= BATCH_PAUSE_EVERY:
                pause = random.uniform(BATCH_PAUSE_MIN, BATCH_PAUSE_MAX)
                logging.info(
                    f"⏸️ Batch pause {pause:.0f}s "
                    f"setelah {sent_since_pause} media terkirim"
                )
                sent_since_pause = 0
                await asyncio.sleep(pause)
            else:
                # Delay antar group
                group_delay = flood_ctrl.get_group_delay()
                send_delay  = DELAY_BETWEEN_SEND + random.uniform(
                    DELAY_RANDOM_MIN, DELAY_RANDOM_MAX
                )
                total_delay = group_delay + send_delay
                logging.debug(
                    f"⏳ Delay: {total_delay:.1f}s | "
                    f"{flood_ctrl.get_status()}"
                )
                await asyncio.sleep(total_delay)

        except asyncio.CancelledError:
            logging.info("🛑 Worker dibatalkan, menyimpan state...")
            await save_all()
            break

        except Exception as e:
            logging.error(f"❌ Worker error: {e}", exc_info=True)
            is_sending = False
            await asyncio.sleep(5)

    logging.info("✅ Queue worker berhenti")

# ============================================================
# === HANDLERS ===
# ============================================================
ALLOWED_SOURCE_CHATS = _parse_id_list("ALLOWED_SOURCE_CHATS")

async def forward_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg:
        return

    # ✅ Validasi sumber chat
    if ALLOWED_SOURCE_CHATS and update.effective_chat.id not in ALLOWED_SOURCE_CHATS:
        logging.debug(
            f"⛔ Pesan dari chat tidak diizinkan: {update.effective_chat.id}"
        )
        return

    if not (msg.video or msg.document):
        return

    fp = get_fingerprint(msg)
    if not fp.get("file_id"):
        return

    file_id  = fp["file_id"]
    fp_hash  = make_hash(fp)
    mtype    = fp["media_type"]

    # Cek duplikat sebelum masuk queue
    if await is_duplicate(file_id, fp_hash):
        logging.info(f"⏭️ Duplikat ditolak sebelum queue: {file_id[:8]}...")
        return

    # Masukkan ke queue
    try:
        if pending_queue.full():
            logging.warning(
                f"⚠️ Queue penuh ({pending_queue.qsize()}), "
                f"drop media: {file_id[:8]}..."
            )
            return

        await pending_queue.put((file_id, mtype, fp))
        logging.info(
            f"📥 Masuk queue: {file_id[:8]}... | "
            f"Type: {mtype} | "
            f"Queue: {pending_queue.qsize()}"
        )

    except Exception as e:
        logging.error(f"❌ Gagal masukkan ke queue: {e}")

# ============================================================
# === ADMIN COMMANDS ===
# ============================================================
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Health check"""
    await update.message.reply_text("🏓 Pong!")

async def cmd_uptime(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return
    now = datetime.now(timezone.utc)
    uptime = now - start_time
    hours, remainder = divmod(int(uptime.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    text = f"⏱️ Bot sudah berjalan: {hours}h {minutes}m {seconds}s"
    await update.message.reply_text(text)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Tampilkan daftar command"""
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return

    role = get_role(update)
    text = (
        "📋 *DAFTAR COMMAND*\n\n"
        "*Moderator:*\n"
        "├ /ping — Health check\n"
        "├ /status — Status bot\n"
        "├ /stats — Statistik realtime\n"
        "├ /pause — Jeda worker\n"
        "├ /resume — Lanjutkan worker\n"
        "└ /uptime — Lama bot berjalan\n"
    )
    if role == "superadmin":
        text += (
            "\n*Superadmin:*\n"
            "├ /setlimit <n> — Set daily limit (1-5000)\n"
            "├ /setdelay <n> — Set delay antar kirim (0.1-10.0)\n"
            "├ /flushpending — Kosongkan queue\n"
            "├ /resetdaily — Reset counter harian\n"
            "├ /log — Lihat 15 baris log terakhir\n" 
            "└ /shutdown — Matikan bot\n"
        )
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return

    # Ambil total sent
    try:
        total_sent = await redis_client.scard(KEY_SENT) if redis_client else len(local_sent)
    except Exception:
        total_sent = len(local_sent)

    queue_size   = pending_queue.qsize() if pending_queue else 0
    redis_status = "✅ Terhubung" if redis_client else "❌ Offline (mode lokal)"
    worker_status = "⏸️ Dijeda" if is_paused else ("🔄 Mengirim" if is_sending else "✅ Idle")
    daily_pct    = (daily_count / DAILY_LIMIT * 100) if DAILY_LIMIT > 0 else 0
    bar_filled   = int(daily_pct / 10)
    bar          = "🟩" * bar_filled + "⬜" * (10 - bar_filled)

    # Format waktu terakhir save
    last_save_str = "N/A"
    if last_save_time:
        now = datetime.now(timezone.utc)
        diff = now - last_save_time
        if diff.total_seconds() < 60:
            last_save_str = "Baru saja"
        elif diff.total_seconds() < 3600:
            last_save_str = f"{int(diff.total_seconds() // 60)} menit lalu"
        else:
            last_save_str = f"{int(diff.total_seconds() // 3600)} jam lalu"

    # Flood status
    flood_status = flood_ctrl.get_status() if flood_ctrl else "N/A"

    # Format pesan
    text = (
        "📊 *STATUS BOT HANAYA v5.0*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 *Worker*       : {worker_status}\n"
        f"📦 *Pending*      : {queue_size} media\n"
        f"📤 *Terkirim*     : {total_sent} media\n"
        f"📅 *Daily*        : {daily_count}/{DAILY_LIMIT} ({daily_pct:.1f}%)\n"
        f"    └─ {bar}\n"
        f"⏱️ *Last Save*    : {last_save_str}\n"
        f"📡 *Redis*        : {redis_status}\n"
        f"⚡ *Flood Ctrl*   : {flood_status}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 Gunakan /help untuk daftar command"
    )

    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return

    try:
        total_sent = await redis_client.scard(KEY_SENT) if redis_client else len(local_sent)
    except Exception:
        total_sent = len(local_sent)

    queue_size   = pending_queue.qsize() if pending_queue else 0
    remaining    = max(0, DAILY_LIMIT - daily_count)
    pct_used     = (daily_count / DAILY_LIMIT * 100) if DAILY_LIMIT > 0 else 0
    bar_filled   = int(pct_used / 10)
    bar          = "█" * bar_filled + "░" * (10 - bar_filled)

    text = (
        f"📈 *STATISTIK REALTIME*\n\n"
        f"*Pengiriman Hari Ini*\n"
        f"[{bar}] {pct_used:.1f}%\n"
        f"├ Terkirim  : {daily_count}\n"
        f"├ Sisa      : {remaining}\n"
        f"└ Limit     : {DAILY_LIMIT}\n\n"
        f"*Queue*\n"
        f"├ Pending   : {queue_size}\n"
        f"└ Total Sent: {total_sent}\n\n"
        f"*Flood Control*\n"
        f"└ {flood_ctrl.get_status() if flood_ctrl else 'N/A'}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return
    global is_paused
    is_paused = True
    name = update.effective_user.first_name
    await update.message.reply_text(f"⏸️ Worker dijeda oleh {name}")
    logging.info(f"⏸️ Worker dijeda oleh {name} ({update.effective_user.id})")

async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return
    global is_paused
    is_paused = False
    name = update.effective_user.first_name
    await update.message.reply_text(f"▶️ Worker dilanjutkan oleh {name}")
    logging.info(f"▶️ Worker dilanjutkan oleh {name} ({update.effective_user.id})")

async def cmd_flushpending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa flush pending")
        return

    count = pending_queue.qsize()
    # Kosongkan queue
    while not pending_queue.empty():
        try:
            pending_queue.get_nowait()
            pending_queue.task_done()
        except asyncio.QueueEmpty:
            break

    await save_all()
    name = update.effective_user.first_name
    await update.message.reply_text(
        f"🗑️ Queue dikosongkan ({count} item) oleh {name}"
    )
    logging.info(f"🗑️ Queue dikosongkan ({count} item) oleh {name}")

async def cmd_resetdaily(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa reset daily")
        return
    global daily_count, daily_reset_date
    prev = daily_count
    daily_count      = 0
    daily_reset_date = datetime.now(timezone.utc).date()
    await save_daily()
    name = update.effective_user.first_name
    await update.message.reply_text(
        f"🔄 Daily counter direset oleh {name}\n"
        f"└ Sebelumnya: {prev} terkirim"
    )
    logging.info(f"🔄 Daily reset oleh {name}, sebelumnya: {prev}")

async def cmd_setlimit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa ubah limit")
        return
    try:
        arg = _flatten_arg(context.args)
        if not arg:
            await update.message.reply_text("❌ Contoh: /setlimit 1000")
            return
        new_limit = int(arg)
        if not (1 <= new_limit <= 5000):
            await update.message.reply_text("❌ Limit harus antara 1-5000")
            return
        global DAILY_LIMIT
        async with config_lock:
            DAILY_LIMIT = new_limit
        await r_set(KEY_DAILY_LIMIT, str(DAILY_LIMIT))
        name = update.effective_user.first_name
        await update.message.reply_text(f"✅ Daily limit diubah ke {DAILY_LIMIT} oleh {name}")
        logging.info(f"⚙️ Daily limit → {DAILY_LIMIT} oleh {name}")
    except (ValueError, TypeError):
        await update.message.reply_text("❌ Contoh: /setlimit 1000")

async def cmd_setdelay(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa ubah delay")
        return
    try:
        arg = _flatten_arg(context.args)
        if not arg:
            await update.message.reply_text("❌ Contoh: /setdelay 2.5")
            return
        new_delay = float(arg)
        if not (0.1 <= new_delay <= 10.0):
            await update.message.reply_text("❌ Delay harus antara 0.1-10.0 detik")
            return
        global DELAY_BETWEEN_SEND
        async with config_lock:
            DELAY_BETWEEN_SEND = new_delay
        await r_set(KEY_SEND_DELAY, str(DELAY_BETWEEN_SEND))
        name = update.effective_user.first_name
        await update.message.reply_text(f"✅ Delay diubah ke {DELAY_BETWEEN_SEND:.1f}s oleh {name}")
        logging.info(f"⚙️ Send delay → {DELAY_BETWEEN_SEND:.1f}s oleh {name}")
    except (ValueError, TypeError):
        await update.message.reply_text("❌ Contoh: /setdelay 2.5")

async def cmd_log(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa lihat log")
        return
    try:
        with open("bot.log", "r", encoding="utf-8") as f:
            lines = f.readlines()[-15:]  # 15 baris terakhir
        log_text = "".join(lines)
        await update.message.reply_text(f"```\n{log_text}```", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Gagal baca log: {e}")

async def cmd_shutdown(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa matikan bot")
        return
    name = update.effective_user.first_name
    await update.message.reply_text(f"🛑 Bot dimatikan oleh {name}")
    logging.info(f"🛑 Bot dimatikan oleh {name}")
    await save_all()
    await context.application.stop()

# ============================================================
# === STARTUP & SHUTDOWN ===
# ============================================================
async def on_startup(app) -> None:
    global sending_lock, config_lock, pending_queue, redis_client, flood_ctrl

    sending_lock = asyncio.Lock()
    config_lock  = asyncio.Lock()
    pending_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)

    await connect_redis_async()

    # Load state
    await load_all()

    # Inisialisasi flood controller
    if flood_ctrl is None:
        flood_ctrl = SmartFloodController()

    # Start worker
    asyncio.create_task(queue_worker(app.bot))

    logging.info("🚀 Bot siap!")

async def on_shutdown(app) -> None:
    await save_all()
    logging.info("🛑 Bot berhenti, data tersimpan")

# ============================================================
# === SIGNAL HANDLER ===
# ============================================================
def handle_shutdown(signum, frame):
    logging.info("⚠️ Shutdown signal diterima, menyimpan data...")
    try:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(lambda: asyncio.create_task(_async_shutdown()))
    except RuntimeError:
        logging.info("✅ Data tersimpan, bot berhenti")
        sys.exit(0)

async def _async_shutdown():
    try:
        await save_all()
        logging.info("✅ Data tersimpan, bot berhenti")
    except Exception as e:
        logging.error(f"❌ Error saat save_all(): {e}")
    finally:
        try:
            loop = asyncio.get_running_loop()
            loop.stop()
        except RuntimeError:
            sys.exit(0)

# ============================================================
# === ERROR HANDLER ===
# ============================================================
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    error = context.error
    if error is None:
        return

    logging.error(
        f"❌ Update caused error:\n"
        f"   Update: {update}\n"
        f"   Error: {error}",
        exc_info=error if isinstance(error, Exception) else None
    )

# ============================================================
# === MAIN ===
# ============================================================
def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    app.add_error_handler(error_handler)
    app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL, forward_media))

    # Admin commands
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("uptime", cmd_uptime))
    app.add_handler(CommandHandler("flushpending", cmd_flushpending))
    app.add_handler(CommandHandler("resetdaily", cmd_resetdaily))
    app.add_handler(CommandHandler("setlimit", cmd_setlimit))
    app.add_handler(CommandHandler("setdelay", cmd_setdelay))
    app.add_handler(CommandHandler("log", cmd_log))
    app.add_handler(CommandHandler("shutdown", cmd_shutdown))

    logging.info("╔══════════════════════════════════╗")
    logging.info("║ 🌸 HANAYA BOT v5.0 (Async Redis) ║")
    logging.info(f"║  Daily Limit : {DAILY_LIMIT} Media/hari   ║")
    logging.info(f"║  Group Size  : {GROUP_SIZE} Media/kelompok  ║")
    logging.info(f"║  Max Pending : {MAX_QUEUE_SIZE} Media        ║")
    logging.info("╚══════════════════════════════════╝")

    try:
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
    except KeyboardInterrupt:
        logging.info("⚠️ Keyboard interrupt diterima")
    except Exception as e:
        logging.error(f"❌ Fatal error: {e}", exc_info=True)
    finally:
        logging.info("✅ Bot shutdown complete")

if __name__ == "__main__":
    main()