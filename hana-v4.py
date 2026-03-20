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
import redis

load_dotenv()

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
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
import traceback
import sys

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
        # Jangan tampilkan traceback panjang untuk network error yang berulang
        if self._is_network_error(record):
            return self._format_network_error(record)
        
        # Format normal untuk log lainnya
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S,%03d"
        )
        level = record.levelname
        message = record.getMessage()
        
        # Tambahkan warna untuk terminal
        color = self.COLORS.get(level, '')
        reset = self.COLORS['RESET']
        
        if record.exc_info and record.levelno >= logging.ERROR:
            # Tampilkan error singkat saja
            exc_type, exc_value, _ = record.exc_info
            exc_name = exc_type.__name__ if exc_type else "Unknown"
            return f"{timestamp} [{color}{level}{reset}] {message}\n    └─ {exc_name}: {str(exc_value)[:150]}"
        
        return f"{timestamp} [{color}{level}{reset}] {message}"
    
    @staticmethod
    def _is_network_error(record):
        """Deteksi network error yang sering terjadi"""
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
            exc_type = record.exc_info
            exc_name = exc_type.__name__ if exc_type else ""
            
            return any(err in exc_name for err in network_errors)
        
        return any(err in record.getMessage() for err in network_errors)
    
    @staticmethod
    def _format_network_error(record):
        """Format network error secara ringkas"""
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S,%03d"
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
    """Filter untuk mengurangi spam dari network error yang berulang"""
    
    def __init__(self, max_same_errors=5):
        super().__init__()
        self.error_cache = {}
        self.max_same_errors = max_same_errors
    
    def filter(self, record):
        """Return False untuk skip log, True untuk lanjutkan"""
        
        # Network error yang berulang dalam 60 detik, skip beberapa
        if record.levelno >= logging.ERROR:
            exc_info = record.exc_info
            if exc_info:
                exc_type = exc_info
                exc_name = exc_type.__name__ if exc_type else "Unknown"
                
                # Jika error sama berulang, skip beberapa kali
                if exc_name in self.error_cache:
                    count, last_time = self.error_cache[exc_name]
                    now = datetime.now(timezone.utc).timestamp()
                    
                    # Reset counter setelah 

                    if now - last_time > 60:  # Reset setelah 60 detik
                        self.error_cache[exc_name] = (1, now)
                    else:
                        self.error_cache[exc_name] = (count + 1, now)
                        if count >= self.max_same_errors:
                            return False  # Skip log ini
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

# Tambahkan filter untuk mengurangi spam
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
pending_media:    list        = []
is_sending:       bool        = False
daily_count:      int         = 0
daily_reset_date              = datetime.now(timezone.utc).date()
last_save_time                = datetime.now(timezone.utc)
sending_lock                  = None  # Akan diinisialisasi di async context
is_paused:        bool        = False
local_sent:       OrderedDict = OrderedDict()
MAX_LOCAL_SENT                = 5000
flood_ctrl:       dict | None = None
config_lock                  = None  # Akan diinisialisasi di async context

# ============================================================
# === REDIS CONNECTION ===
# ============================================================
redis_client: redis.Redis | None = None

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
            client.ping()
            redis_client = client
            logging.info("✅ Redis terhubung!")
            return
        except Exception as e:
            logging.error(f"❌ Redis gagal (percobaan {attempt+1}/5): {e}")
            await asyncio.sleep(5)  # Non-blocking

    logging.error("❌ Redis tidak bisa terhubung, fallback ke lokal")
    redis_client = None

async def ensure_redis() -> None:
    global redis_client
    if redis_client is None:
        return
    try:
        redis_client.ping()
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
        return redis_client.get(key)
    except Exception as e:
        logging.error(f"Redis GET error [{key}]: {e}")
        return None

async def r_set(key: str, value: str) -> None:
    if redis_client is None:
        return
    try:
        await ensure_redis()
        redis_client.set(key, value)
    except Exception as e:
        logging.error(f"Redis SET error [{key}]: {e}")

async def r_sismember(key: str, value: str) -> bool:
    if redis_client is None:
        return False
    try:
        await ensure_redis()
        return bool(redis_client.sismember(key, value))
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
        pipe.execute()
    except Exception as e:
        logging.error(f"Redis SADD+TTL error [{key}]: {e}")

async def r_set_json(key: str, data: dict) -> None:
    if redis_client is None:
        return
    try:
        await ensure_redis()
        redis_client.set(key, json.dumps(data))
    except Exception as e:
        logging.error(f"Redis SET JSON error [{key}]: {e}")

async def r_get_json(key: str) -> dict | None:
    if redis_client is None:
        return None
    try:
        await ensure_redis()
        raw = redis_client.get(key)
        if raw:
            return json.loads(raw)
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Redis JSON decode error [{key}]: {e}")
        try:
            redis_client.delete(key)
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
            f"Delay: {self.group_delay_min:.0f}-{self.group_delay_max:.0f}s"
        )

    def to_dict(self) -> dict:
        return {
            "flood_count"    : self.flood_count,
            "total_flood"    : self.total_flood,
            "last_flood_time": (
                self.last_flood_time.isoformat()
                if self.last_flood_time else None
            ),
            "penalty"        : self.penalty,
            "is_cooling"     : self.is_cooling,
            "group_delay_min": self.group_delay_min,
            "group_delay_max": self.group_delay_max,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SmartFloodController":
        instance = cls()
        try:
            instance.flood_count     = int(data.get("flood_count", 0))
            instance.total_flood     = int(data.get("total_flood", 0))
            instance.last_flood_time = instance._parse_last_flood_time(
                data.get("last_flood_time")
            )
            instance.penalty         = float(data.get("penalty", 0.0))
            instance.is_cooling      = bool(data.get("is_cooling", False))
            instance.group_delay_min = float(data.get("group_delay_min", DELAY_BETWEEN_GROUP_MIN))
            instance.group_delay_max = float(data.get("group_delay_max", DELAY_BETWEEN_GROUP_MAX))
        except (TypeError, ValueError) as e:
            logging.warning(f"⚠️ Gagal load state flood control: {e}")
            instance = cls()
        return instance

    async def save_state(self) -> None:
        try:
            await r_set_json(KEY_FLOOD_CTRL, self.to_dict())
        except Exception as e:
            logging.error(f"❌ Gagal save flood ctrl state: {e}")

# ============================================================
# === LOCAL SENT HELPERS ===
# ============================================================
def _local_sent_add(key: str, max_size: int = MAX_LOCAL_SENT) -> None:
    """Tambah ke local_sent dengan eviction FIFO jika melebihi batas."""
    if key in local_sent:
        return
    local_sent[key] = True
    while len(local_sent) > max_size:
        local_sent.popitem(last=False)

# ============================================================
# === LOAD & SAVE ===
# ============================================================
async def load_all() -> None:
    global pending_media, daily_count, daily_reset_date, flood_ctrl

    async with config_lock:
        # Load pending media
        try:
            raw = await r_get(KEY_PENDING)
            if raw:
                pending_media = json.loads(raw)
            else:
                pending_media = []
        except (json.JSONDecodeError, TypeError) as e:
            logging.error(f"❌ Gagal load pending: {e}")
            pending_media = []

        # Load daily count
        try:
            count_str  = await r_get(KEY_DAILY_COUNT)
            date_str   = await r_get(KEY_DAILY_DATE)
            today      = datetime.now(timezone.utc).date()
            saved_date = (
                datetime.strptime(date_str, "%Y-%m-%d").date()
                if date_str else today
            )
            daily_count      = int(count_str) if count_str and saved_date == today else 0
            daily_reset_date = today
        except (ValueError, TypeError) as e:
            logging.error(f"❌ Gagal load daily count: {e}")
            daily_count      = 0
            daily_reset_date = datetime.now(timezone.utc).date()

        # Load flood controller
        try:
            flood_data = await r_get_json(KEY_FLOOD_CTRL)
            if flood_data:
                flood_ctrl = SmartFloodController.from_dict(flood_data)
                logging.info("🔄 Flood control state loaded dari Redis")
            else:
                flood_ctrl = SmartFloodController()
                logging.info("✅ Flood control state baru dibuat")
        except Exception as e:
            logging.warning(f"⚠️ Gagal load flood control: {e}")
            flood_ctrl = SmartFloodController()

        # Load daily limit
        try:
            limit_str = await r_get(KEY_DAILY_LIMIT)
            if limit_str:
                global DAILY_LIMIT
                DAILY_LIMIT = int(limit_str)
        except (ValueError, TypeError) as e:
            logging.warning(f"⚠️ Gagal load daily limit: {e}")

        # Load send delay
        try:
            delay_str = await r_get(KEY_SEND_DELAY)
            if delay_str:
                global DELAY_BETWEEN_SEND
                DELAY_BETWEEN_SEND = float(delay_str)
        except (ValueError, TypeError) as e:
            logging.warning(f"⚠️ Gagal load send delay: {e}")

        logging.info(
            f"📂 Data dimuat | Pending: {len(pending_media)} | "
            f"Daily: {daily_count}/{DAILY_LIMIT} | "
            f"Flood: {flood_ctrl.get_status()}"
        )

async def save_pending() -> None:
    try:
        await r_set(KEY_PENDING, json.dumps(pending_media))
    except Exception as e:
        logging.error(f"❌ Gagal save pending: {e}")

async def save_daily() -> None:
    try:
        await r_set(KEY_DAILY_COUNT, str(daily_count))
        await r_set(KEY_DAILY_DATE, str(daily_reset_date))
    except Exception as e:
        logging.error(f"❌ Gagal save daily: {e}")

async def save_flood_ctrl() -> None:
    try:
        if flood_ctrl:
            await r_set_json(KEY_FLOOD_CTRL, flood_ctrl.to_dict())
    except Exception as e:
        logging.error(f"❌ Gagal save flood ctrl: {e}")

async def save_daily_limit() -> None:
    try:
        await r_set(KEY_DAILY_LIMIT, str(DAILY_LIMIT))
    except Exception as e:
        logging.error(f"❌ Gagal save daily limit: {e}")

async def save_send_delay() -> None:
    try:
        await r_set(KEY_SEND_DELAY, str(DELAY_BETWEEN_SEND))
    except Exception as e:
        logging.error(f"❌ Gagal save send delay: {e}")

async def save_all() -> None:
    await asyncio.gather(
        save_pending(),
        save_daily(),
        save_flood_ctrl(),
        save_daily_limit(),
        save_send_delay()
    )

# ============================================================
# === DUPLIKAT CHECK ===
# ============================================================
def get_fingerprint(msg) -> dict | None:
    if not msg.video:
        return None
    v = msg.video
    return {
        'type'     : 'video',
        'duration' : v.duration,
        'width'    : v.width,
        'height'   : v.height,
        'file_size': v.file_size,
        'file_id'  : v.file_id,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

async def is_duplicate(fingerprint: dict, pending_snapshot: list | None = None) -> bool:
    """
    Cek duplikat dengan 3 lapis.
    pending_snapshot: snapshot list pending (aman dari concurrent modification)
    """
    if not fingerprint:
        return False

    file_id = fingerprint.get('file_id', '')
    fp_hash = make_hash(fingerprint)

    # Lapis 1: Redis
    if redis_client:
        try:
            if await r_sismember(KEY_SENT, file_id):
                logging.debug(f"✅ Duplikat (file_id) di Redis: {file_id[:8]}...")
                return True
            if await r_sismember(KEY_SENT, fp_hash):
                logging.debug(f"✅ Duplikat (hash) di Redis: {fp_hash[:8]}...")
                return True
        except Exception as e:
            logging.warning(f"⚠️ Redis check error: {e}")

    # Lapis 2: local_sent
    if file_id in local_sent or fp_hash in local_sent:
        logging.debug(f"✅ Duplikat (local) {file_id[:8]}...")
        return True

    # Lapis 3: pending snapshot
    snapshot = pending_snapshot if pending_snapshot is not None else pending_media
    for item in snapshot:
        try:
            q_file_id, _, q_fp = item
            if q_file_id == file_id or make_hash(q_fp) == fp_hash:
                return True
        except (ValueError, TypeError):
            continue

    return False

async def mark_sent(fingerprint: dict) -> None:
    """
    Mark video sebagai sudah dikirim.
    Jika Redis gagal, fallback ke local_sent.
    """
    if not fingerprint:
        return

    file_id = fingerprint.get('file_id', '')
    fp_hash = make_hash(fingerprint)

    try:
        await r_sadd_with_ttl(KEY_SENT, file_id, SENT_TTL)
        await r_sadd_with_ttl(KEY_SENT, fp_hash, SENT_TTL)
        logging.debug(f"✅ Marked sent (Redis): {file_id[:8]}...")
    except Exception as e:
        logging.warning(f"⚠️ Redis mark_sent error, fallback ke local: {e}")
        _local_sent_add(file_id)
        _local_sent_add(fp_hash)

# ============================================================
# === DAILY RESET ===
# ============================================================
async def check_daily_reset() -> None:
    global daily_count, daily_reset_date
    today = datetime.now(timezone.utc).date()
    if today != daily_reset_date:
        logging.info(f"🔄 Reset harian | Kemarin terkirim: {daily_count}")
        daily_count      = 0
        daily_reset_date = today
        await save_daily()

# ============================================================
# === KIRIM MEDIA GROUP ===
# ============================================================
async def send_media_group_with_retry(
    bot,
    chat_id: int,
    media_items: list,
    admin_chat_id: int | None = None,
    timeout: int = 30
) -> bool:
    global flood_ctrl

    for attempt in range(MAX_RETRIES):
        try:
            if not media_items:
                return False

            input_media = [InputMediaVideo(media=fid) for fid, _ in media_items]
            await asyncio.wait_for(
                bot.send_media_group(chat_id=chat_id, media=input_media),
                timeout=timeout
            )

            await flood_ctrl.record_success()
            return True

        except asyncio.TimeoutError:
            logging.error(
                f"❌ Timeout saat mengirim grup media "
                f"(attempt {attempt+1}/{MAX_RETRIES})"
            )
            if attempt == MAX_RETRIES - 1:
                return False

        except Exception as e:
            err = str(e)

            if "Flood control" in err or "Too Many Requests" in err:
                suggested  = parse_retry_after(err)
                total_wait = await flood_ctrl.record_flood(suggested)

                if (
                    flood_ctrl.flood_count >= FLOOD_WARN_THRESHOLD
                    and admin_chat_id != 0
                ):
                    try:
                        await bot.send_message(
                            chat_id=admin_chat_id,
                            text=(
                                f"⚠️ *FLOOD ALERT*\n"
                                f"├ Flood ke-{flood_ctrl.flood_count} berturut-turut\n"
                                f"├ Total flood: {flood_ctrl.total_flood}x\n"
                                f"├ Tunggu: {total_wait:.0f}s\n"
                                f"└ Penalti: {flood_ctrl.penalty:.0f}s"
                            ),
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass

                await asyncio.sleep(total_wait)

            elif "timed out" in err.lower() or "timeout" in err.lower():
                wait = (30 * (attempt + 1)) + random.uniform(5, 15)
                logging.warning(
                    f"⏳ Timeout (attempt {attempt+1}/{MAX_RETRIES}) "
                    f"— retry dalam {wait:.1f}s"
                )
                await asyncio.sleep(wait)

            elif "chat not found" in err.lower() or "forbidden" in err.lower():
                logging.error(
                    f"❌ Akses ditolak ke chat {chat_id} — bot mungkin diblokir"
                )
                if admin_chat_id != 0:
                    try:
                        await bot.send_message(
                            chat_id=admin_chat_id,
                            text=(
                                f"❌ *AKSES DITOLAK*\n"
                                f"Chat ID: `{chat_id}`\n"
                                f"Error: {err[:200]}"
                            ),
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass
                return False

            elif "invalid file" in err.lower() or "not found" in err.lower():
                logging.warning(f"⚠️ File tidak valid, dilewati: {err[:100]}")
                return False

            else:
                base  = min(60, (2 ** attempt) * 5)
                extra = random.uniform(5, 20)
                wait  = base + extra
                logging.error(
                    f"❌ Error tidak dikenal (attempt {attempt+1}/{MAX_RETRIES})\n"
                    f"   ├─ Error : {err[:150]}\n"
                    f"   └─ Tunggu: {wait:.1f}s"
                )
                await asyncio.sleep(wait)

                if attempt == MAX_RETRIES - 1:
                    logging.error(f"💀 Gagal total setelah {MAX_RETRIES} percobaan")
                    return False

    return False

# ============================================================
# === BACKGROUND WORKER ===
# ============================================================
async def queue_worker(bot):
    global pending_media, is_sending, daily_count, last_save_time

    logging.info("✅ Worker started")
    while True:
        await asyncio.sleep(WAIT_TIME)

        # Auto save berkala
        now = datetime.now(timezone.utc)
        if (now - last_save_time).total_seconds() >= AUTO_SAVE_INTERVAL:
            await save_all()
            last_save_time = now

        await check_daily_reset()

        if is_paused:
            logging.info("⏸️ Worker sedang dijeda, menunggu resume...")
            continue

        if not pending_media or is_sending:
            continue

        if daily_count >= DAILY_LIMIT:
            logging.warning(f"⛔ Daily limit tercapai ({DAILY_LIMIT})")
            continue

        # Atomic batch grab dalam satu lock, cegah race condition
        async with sending_lock:
            if is_sending:
                continue
            is_sending = True

            if len(pending_media) > MAX_QUEUE_SIZE:
                overflow      = len(pending_media) - int(MAX_QUEUE_SIZE * 0.95)
                removed       = pending_media[:overflow]
                pending_media = pending_media[overflow:]
                await save_pending()
                logging.warning(f"⚠️ Pending overflow: removed {len(removed)} items")

            batch = pending_media.copy()
            pending_media.clear()
            await save_pending()

        total      = len(batch)
        sent_count = 0

        try:
            logging.info(
                f"▶️ Mulai kirim {total} video | Daily: {daily_count}/{DAILY_LIMIT}"
            )

            for i in range(0, total, GROUP_SIZE):
                await check_daily_reset()

                if daily_count >= DAILY_LIMIT:
                    async with sending_lock:
                        pending_media.extend(batch[i:])
                    await save_pending()
                    logging.warning(f"⛔ Limit tercapai, {total - i} video ditunda")
                    break

                group     = batch[i:i + GROUP_SIZE]
                group_num = (i // GROUP_SIZE) + 1
                total_grp = (total + GROUP_SIZE - 1) // GROUP_SIZE

                # Snapshot pending untuk cek duplikat (hindari iterasi concurrent)
                async with sending_lock:
                    pending_snapshot = pending_media.copy()

                items = []
                for file_id, media_type, fp in group:
                    if not await is_duplicate(fp, pending_snapshot):
                        items.append((file_id, media_type))
                        await mark_sent(fp)
                    else:
                        logging.info(f"ℹ️ Skip duplicate: {file_id[:8]}...")

                if items:
                    success = await send_media_group_with_retry(
                        bot, TARGET_CHAT_ID, items, ADMIN_CHAT_ID
                    )
                    if success:
                        # Update daily_count dalam lock
                        async with sending_lock:
                            daily_count += len(items)
                        sent_count += len(items)
                        await save_daily()
                        for file_id, _, fp in group:
                            logging.info(
                                f"✅ Terkirim: {file_id[:8]}... | "
                                f"{fp['width']}x{fp['height']} | {fp['duration']}s"
                            )
                    else:
                        logging.error(f"❌ Gagal kirim group {group_num}/{total_grp}")
                        async with sending_lock:
                            pending_media.extend(group)
                        await save_pending()

                # Baca DELAY_BETWEEN_SEND dengan config_lock
                async with config_lock:
                    current_delay = DELAY_BETWEEN_SEND
                delay = current_delay + random.uniform(DELAY_RANDOM_MIN, DELAY_RANDOM_MAX)
                await asyncio.sleep(delay)

                if sent_count > 0 and sent_count % BATCH_PAUSE_EVERY == 0:
                    pause = random.uniform(BATCH_PAUSE_MIN, BATCH_PAUSE_MAX)
                    logging.info(f"⏸️ Jeda panjang {pause:.0f}s setelah {sent_count} video")
                    await asyncio.sleep(pause)
                else:
                    group_delay = flood_ctrl.get_group_delay()
                    logging.info(
                        f"⏳ Jeda group: {group_delay:.1f}s | {flood_ctrl.get_status()}"
                    )
                    await asyncio.sleep(group_delay)

        except Exception as e:
            logging.error(f"❌ Error worker: {e}", exc_info=True)
            remaining = batch[sent_count:]
            if remaining:
                async with sending_lock:
                    pending_media.extend(remaining)
                await save_pending()

        finally:
            async with sending_lock:
                is_sending = False

            await save_all()

            try:
                total_sent    = redis_client.scard(KEY_SENT) if redis_client else len(local_sent)
                total_pending = len(pending_media)
                logging.info(
                    f"✅ Selesai | Terkirim: {sent_count}/{total} | "
                    f"Daily: {daily_count}/{DAILY_LIMIT} | "
                    f"Sent={total_sent} | Pending={total_pending}"
                )
            except Exception as e:
                logging.warning(f"⚠️ Gagal ambil stats akhir: {e}")

    logging.info("✅ Worker stopped")
    
# ============================================================
# === HANDLER ===
# ============================================================
async def forward_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    fp = get_fingerprint(msg)
    if not fp:
        return

    if await is_duplicate(fp):
        logging.info(
            f"❌ Duplikat: {fp['file_id'] [:8]}... | "
            f"{fp['width']}x{fp['height']} | {fp['duration']}s"
        )
        return

    file_id    = fp.get('file_id')
    media_type = fp.get('type')

    async with sending_lock:
        pending_media.append((file_id, media_type, fp))
        await save_pending()
        logging.info(
            f"⏳ Pending: {file_id[:8]}... | {fp['width']}x{fp['height']} | {fp['duration']}s"
        )

    try:
        if redis_client:
            total_sent = redis_client.scard(KEY_SENT)
        else:
            total_sent = len(local_sent)
    except Exception as e:
        logging.warning(f"⚠️ Gagal ambil total_sent: {e}")
        total_sent = len(local_sent)

    total_pending = len(pending_media)
    logging.info(f"📊 Total: Terkirim={total_sent} | Pending={total_pending} | Failed=0")

# ============================================================
# === ADMIN ROLE HELPERS ===
# ============================================================
def get_role(update: Update) -> str | None:
    if update.effective_user is None:
        return None
    role = ADMINS.get(update.effective_user.id)
    logging.debug(f"User {update.effective_user.id} role: {role}")
    return role

def is_superadmin(update: Update) -> bool:
    return get_role(update) == "superadmin"

def is_moderator(update: Update) -> bool:
    return get_role(update) in ["moderator", "superadmin"]

async def notify_admins(bot, text: str):
    for admin_id in ADMINS:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=f"[ADMIN ACTION] {text}",
                parse_mode="Markdown"
            )
        except Exception as e:
            logging.warning(f"⚠️ Gagal kirim notifikasi ke {admin_id}: {e}")

# ============================================================
# === ADMIN COMMANDS ===
# ============================================================
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_moderator(update):
        await update.message.reply_text("❌ Anda bukan moderator/admin")
        return
    try:
        total_sent = redis_client.scard(KEY_SENT) if redis_client else len(local_sent)
    except Exception as e:
        logging.warning(f"⚠️ Gagal ambil total_sent: {e}")
        total_sent = len(local_sent)
    
    total_pending = len(pending_media)
    text = (
        f"📊 STATUS BOT\n"
        f"├ Pending : {total_pending}\n"
        f"├ Sent    : {total_sent}\n"
        f"├ Daily   : {daily_count}/{DAILY_LIMIT}\n"
        f"├ Delay   : {DELAY_BETWEEN_SEND:.1f}s\n"
        f"└ Flood   : {flood_ctrl.get_status() if flood_ctrl else 'N/A'}"
    )
    await update.message.reply_text(text)

async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_moderator(update):
        await update.message.reply_text("❌ Anda bukan moderator/admin")
        return
    global is_paused
    is_paused = True
    msg = f"⏸️ Worker dijeda oleh {update.effective_user.first_name}"
    await update.message.reply_text(msg)
    await notify_admins(context.bot, msg)

async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_moderator(update):
        await update.message.reply_text("❌ Anda bukan moderator/admin")
        return
    global is_paused
    is_paused = False
    msg = f"▶️ Worker dilanjutkan oleh {update.effective_user.first_name}"
    await update.message.reply_text(msg)
    await notify_admins(context.bot, msg)

async def cmd_flushpending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa menghapus pending")
        return
    global pending_media
    async with sending_lock:
        pending_media.clear()
    await save_pending()
    msg = f"🗑️ Pending queue dikosongkan oleh {update.effective_user.first_name}"
    await update.message.reply_text(msg)
    await notify_admins(context.bot, msg)

async def cmd_resetdaily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa reset daily")
        return
    global daily_count, daily_reset_date
    async with sending_lock:
        daily_count      = 0
        daily_reset_date = datetime.now(timezone.utc).date()
    await save_daily()
    msg = f"🔄 Daily counter direset oleh {update.effective_user.first_name}"
    await update.message.reply_text(msg)
    await notify_admins(context.bot, msg)

# — _flatten_arg: infinite loop diperbaiki
def _flatten_arg(arg) -> str | None:
    """Flatten nested list/tuple menjadi string."""
    while isinstance(arg, (list, tuple)):
        if len(arg) == 0:
            return None
        arg = arg  # Ambil elemen pertama, bukan arg = arg
    return str(arg).strip()

async def cmd_setlimit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa mengubah limit")
        return
    try:
        if not context.args:
            await update.message.reply_text("❌ Format salah. Gunakan: /setlimit 1000")
            return

        arg_str = _flatten_arg(context.args)
        if arg_str is None:
            await update.message.reply_text("❌ Format salah. Gunakan: /setlimit 1000")
            return

        new_limit = int(arg_str)
        if not (1 <= new_limit <= 5000):
            await update.message.reply_text("❌ Limit harus antara 1-5000")
            return

        # Gunakan config_lock untuk update DAILY_LIMIT
        async with config_lock:
            global DAILY_LIMIT
            DAILY_LIMIT = new_limit
        await save_daily_limit()
        msg = f"⚙️ Daily limit diubah ke {DAILY_LIMIT} oleh {update.effective_user.first_name}"
        await update.message.reply_text(f"✅ {msg}")
        await notify_admins(context.bot, msg)

    except (ValueError, TypeError) as e:
        logging.error(f"❌ Error parsing setlimit: {e}")
        await update.message.reply_text("❌ Format salah. Gunakan: /setlimit 1000")

async def cmd_setdelay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa mengubah delay")
        return
    try:
        if not context.args:
            await update.message.reply_text("❌ Format salah. Gunakan: /setdelay 2.5")
            return

        arg_str = _flatten_arg(context.args)
        if arg_str is None:
            await update.message.reply_text("❌ Format salah. Gunakan: /setdelay 2.5")
            return

        new_delay = float(arg_str)
        if not (0.1 <= new_delay <= 10.0):
            await update.message.reply_text("❌ Delay harus antara 0.1-10.0 detik")
            return

        # Gunakan config_lock untuk update DELAY_BETWEEN_SEND
        async with config_lock:
            global DELAY_BETWEEN_SEND
            DELAY_BETWEEN_SEND = new_delay
        await save_send_delay()
        msg = f"⚙️ Delay diubah ke {DELAY_BETWEEN_SEND:.1f}s oleh {update.effective_user.first_name}"
        await update.message.reply_text(f"✅ {msg}")
        await notify_admins(context.bot, msg)

    except (ValueError, TypeError) as e:
        logging.error(f"❌ Error parsing setdelay: {e}")
        await update.message.reply_text("❌ Format salah. Gunakan: /setdelay 2.5")

async def cmd_shutdown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa mematikan bot")
        return
    msg = f"🛑 Bot dimatikan oleh {update.effective_user.first_name}"
    await update.message.reply_text(msg)
    await notify_admins(context.bot, msg)
    await save_all()
    await context.application.stop()
    logging.info("✅ Bot berhenti dengan aman")

# ============================================================
# === STARTUP & SHUTDOWN ===
# ============================================================
async def on_startup(app):
    global sending_lock, config_lock
    
    # Inisialisasi locks di async context
    sending_lock = asyncio.Lock()
    config_lock = asyncio.Lock()
    
    # Koneksi Redis
    await connect_redis_async()
    
    # Load semua data
    await load_all()
    
    # Mulai queue worker
    asyncio.create_task(queue_worker(app.bot))
    
    logging.info("🚀 Bot siap!")

async def on_shutdown(app):
    await save_all()
    logging.info("🛑 Bot berhenti, data tersimpan")

# ============================================================
# === SIGNAL HANDLER ===
# ============================================================
def handle_shutdown(signum, frame):
    logging.info("⚠️ Shutdown signal diterima, menyimpan data...")
    try:
        loop = asyncio.get_running_loop()
        # Buat task untuk save_all() secara async
        asyncio.create_task(_async_shutdown())
    except RuntimeError:
        # Tidak ada running loop, exit langsung
        logging.info("✅ Data tersimpan, bot berhenti")
        sys.exit(0)

async def _async_shutdown():
    """Helper untuk shutdown async"""
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
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors dari update processing"""
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
    app.add_handler(MessageHandler(filters.VIDEO, forward_media))

    app.add_handler(CommandHandler("status",       cmd_status))
    app.add_handler(CommandHandler("pause",        cmd_pause))
    app.add_handler(CommandHandler("resume",       cmd_resume))
    app.add_handler(CommandHandler("flushpending", cmd_flushpending))
    app.add_handler(CommandHandler("resetdaily",   cmd_resetdaily))
    app.add_handler(CommandHandler("setlimit",     cmd_setlimit))
    app.add_handler(CommandHandler("setdelay",     cmd_setdelay))
    app.add_handler(CommandHandler("shutdown",     cmd_shutdown))

    logging.info("╔══════════════════════════════════╗")
    logging.info("║ 🌸 HANAYA BOT v4.7 (Smart Flood) ║")
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
