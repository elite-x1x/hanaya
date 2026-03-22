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
from pathlib import Path

from telegram import Update, InputMediaVideo
from telegram.ext import (
    ApplicationBuilder, ContextTypes,
    MessageHandler, filters, CommandHandler
)
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# === KONFIGURASI UTAMA ===
# ============================================================
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", 0))
ADMIN_CHAT_ID  = int(os.getenv("ADMIN_CHAT_ID", 0))
BOT_NAME       = os.getenv("BOT_NAME", "")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN tidak diatur di .env")
if TARGET_CHAT_ID == 0:
    raise ValueError("❌ TARGET_CHAT_ID tidak diatur di .env")

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

ALLOWED_SOURCE_CHATS = _parse_id_list("ALLOWED_SOURCE_CHATS")

# ============================================================
# === DIREKTORI DATA ===
# ============================================================
DATA_DIR  = Path(f"data/{BOT_NAME}")
SENT_DIR  = DATA_DIR / "sent"
STATE_DIR = DATA_DIR / "state"

DATA_DIR.mkdir(parents=True, exist_ok=True)
SENT_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# === FILE PATH ===
# ============================================================
FILE_PENDING     = STATE_DIR / "pending.json"
FILE_DAILY       = STATE_DIR / "daily.json"
FILE_FLOOD       = STATE_DIR / "flood.json"
FILE_CONFIG      = STATE_DIR / "config.json"
SENT_FILE_PREFIX = "sent"
SENT_FILE_EXT    = ".json"
MAX_SENT_PER_FILE = 1000  # ✅ Maksimal entry per file

# ============================================================
# === RATE LIMIT SETTINGS ===
# ============================================================
DELAY_BETWEEN_SEND      = 2.0
DELAY_RANDOM_MIN        = 0.5
DELAY_RANDOM_MAX        = 2.5
GROUP_SIZE              = 5
DELAY_BETWEEN_GROUP_MIN = 15
DELAY_BETWEEN_GROUP_MAX = 45
BATCH_PAUSE_EVERY       = 100
BATCH_PAUSE_MIN         = 120
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

# ============================================================
# === LOGGING SYSTEM ===
# ============================================================
class CompactFormatter(logging.Formatter):
    COLORS = {
        'DEBUG':    '\033[36m',
        'INFO':     '\033[32m',
        'WARNING':  '\033[33m',
        'ERROR':    '\033[31m',
        'CRITICAL': '\033[35m',
        'RESET':    '\033[0m'
    }

    def format(self, record):
        if self._is_network_error(record):
            return self._format_network_error(record)

        timestamp = datetime.fromtimestamp(record.created).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        level   = record.levelname
        message = record.getMessage()
        color   = self.COLORS.get(level, '')
        reset   = self.COLORS['RESET']

        if record.exc_info and isinstance(record.exc_info, tuple):
            exc_type  = record.exc_info
            exc_value = record.exc_info
            if exc_type and record.levelno >= logging.ERROR:
                exc_name = exc_type.__name__
                return (
                    f"{timestamp} [{color}{level}{reset}] {message}\n"
                    f"    └─ {exc_name}: {str(exc_value)[:150]}"
                )

        return f"{timestamp} [{color}{level}{reset}] {message}"

    @staticmethod
    def _is_network_error(record):
        network_errors = [
            'httpx.ReadError', 'httpx.ConnectError',
            'httpx.TimeoutException', 'ConnectionError',
            'TimeoutError', 'OSError', 'socket.error'
        ]
        if record.exc_info and isinstance(record.exc_info, tuple):
            exc_type = record.exc_info
            exc_name = exc_type.__name__ if exc_type else ""
            return any(err in exc_name for err in network_errors)
        return any(err in record.getMessage() for err in network_errors)

    @staticmethod
    def _format_network_error(record):
        timestamp = datetime.fromtimestamp(record.created).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        level   = record.levelname
        message = record.getMessage()
        color   = CompactFormatter.COLORS.get(level, '')
        reset   = CompactFormatter.COLORS['RESET']

        if record.exc_info and isinstance(record.exc_info, tuple):
            exc_type  = record.exc_info
            exc_value = record.exc_info
            exc_name  = exc_type.__name__ if exc_type else "Unknown"
            exc_msg   = str(exc_value)[:100]
            return f"{timestamp} [{color}{level}{reset}] {exc_name}: {exc_msg}"

        return f"{timestamp} [{color}{level}{reset}] {message}"


class NetworkErrorFilter(logging.Filter):
    def __init__(self, max_same_errors=5):
        super().__init__()
        self.error_cache     = {}
        self.max_same_errors = max_same_errors

    def filter(self, record):
        if record.levelno >= logging.ERROR:
            exc_info = record.exc_info
            if exc_info and isinstance(exc_info, tuple) and len(exc_info) >= 1:
                exc_type = exc_info
                exc_name = exc_type.__name__ if exc_type else "Unknown"

                if exc_name in self.error_cache:
                    count, last_time = self.error_cache[exc_name]
                    now = datetime.now(timezone.utc).timestamp()
                    if now - last_time > 60:
                        self.error_cache[exc_name] = (1, now)
                    else:
                        self.error_cache[exc_name] = (count + 1, now)
                        if count >= self.max_same_errors:
                            return False
                else:
                    self.error_cache[exc_name] = (
                        1, datetime.now(timezone.utc).timestamp()
                    )
        return True


# Setup logging
_log_formatter  = CompactFormatter()
_file_handler   = RotatingFileHandler(
    f"{BOT_NAME}.log",
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
    encoding="utf-8"
)
_file_handler.setFormatter(_log_formatter)
_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_log_formatter)

_network_filter = NetworkErrorFilter(max_same_errors=3)
_stream_handler.addFilter(_network_filter)
_file_handler.addFilter(_network_filter)

logging.basicConfig(
    level=logging.INFO,
    handlers=[_file_handler, _stream_handler]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ============================================================
# === GLOBAL STATE ===
# ============================================================
pending_queue:   asyncio.Queue | None = None
is_sending:      bool                 = False
daily_count:     int                  = 0
daily_reset_date                      = datetime.now(timezone.utc).date()
last_save_time                        = datetime.now(timezone.utc)
sending_lock:    asyncio.Lock | None  = None
config_lock:     asyncio.Lock | None  = None
is_paused:       bool                 = False
flood_ctrl:      "SmartFloodController | None" = None
start_time                            = datetime.now(timezone.utc)

# ============================================================
# === SENT FILE MANAGER ===
# ✅ Sistem file duplikat per 1000 entry
# ============================================================
class SentFileManager:
    """
    Mengelola file duplikat lokal.
    Setiap file menyimpan maksimal MAX_SENT_PER_FILE entry.
    Saat penuh, otomatis buat file baru.
    Saat load, baca SEMUA file lama + baru.
    """

    def __init__(self, sent_dir: Path, prefix: str, max_per_file: int):
        self.sent_dir    = sent_dir
        self.prefix      = prefix
        self.max_per_file = max_per_file
        self._cache:  set  = set()   # Cache in-memory untuk cek cepat
        self._current_file: Path | None = None
        self._current_count: int = 0
        self._file_lock = asyncio.Lock()

    def _get_all_sent_files(self) -> list[Path]:
        """Ambil semua file sent, diurutkan dari lama ke baru"""
        files = sorted(
            self.sent_dir.glob(f"{self.prefix}_*.json"),
            key=lambda f: int(f.stem.split("_")[-1])
        )
        return files

    def _get_next_file_index(self) -> int:
        """Dapatkan index file berikutnya"""
        files = self._get_all_sent_files()
        if not files:
            return 1
        last = files[-1]
        try:
            return int(last.stem.split("_")[-1]) + 1
        except (ValueError, IndexError):
            return len(files) + 1

    async def load_all(self) -> None:
        """Load semua file sent ke cache"""
        self._cache.clear()
        files = self._get_all_sent_files()

        if not files:
            logging.info("📂 Tidak ada file sent tersimpan")
            await self._init_new_file()
            return

        total = 0
        for f in files:
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    data = json.load(fp)
                    if isinstance(data, list):
                        self._cache.update(data)
                        total += len(data)
                        logging.info(
                            f"📂 Load sent file: {f.name} "
                            f"({len(data)} entry)"
                        )
            except Exception as e:
                logging.error(f"❌ Gagal load {f.name}: {e}")

        # Set current file ke file terakhir
        last_file = files[-1]
        try:
            with open(last_file, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                self._current_count = len(data) if isinstance(data, list) else 0
        except Exception:
            self._current_count = 0

        self._current_file = last_file

        # Jika file terakhir sudah penuh, buat file baru
        if self._current_count >= self.max_per_file:
            await self._init_new_file()

        logging.info(
            f"✅ Total sent dimuat: {total} entry dari {len(files)} file | "
            f"File aktif: {self._current_file.name} "
            f"({self._current_count}/{self.max_per_file})"
        )

    async def _init_new_file(self) -> None:
        """Buat file sent baru"""
        idx = self._get_next_file_index()
        self._current_file  = self.sent_dir / f"{self.prefix}_{idx:04d}.json"
        self._current_count = 0

        # Tulis file kosong
        try:
            with open(self._current_file, "w", encoding="utf-8") as fp:
                json.dump([], fp)
            logging.info(f"📄 File sent baru dibuat: {self._current_file.name}")
        except Exception as e:
            logging.error(f"❌ Gagal buat file sent baru: {e}")

    async def add(self, key: str) -> None:
        """Tambahkan key ke file sent"""
        if key in self._cache:
            return

        async with self._file_lock:
            # Cek apakah perlu file baru
            if (
                self._current_file is None or
                self._current_count >= self.max_per_file
            ):
                await self._init_new_file()

            try:
                # Baca isi file saat ini
                with open(self._current_file, "r", encoding="utf-8") as fp:
                    data = json.load(fp)

                # Tambahkan key baru
                if key not in data:
                    data.append(key)
                    self._current_count = len(data)

                    # Tulis kembali
                    with open(self._current_file, "w", encoding="utf-8") as fp:
                        json.dump(data, fp)

                # Update cache
                self._cache.add(key)

                # Jika file penuh, log info
                if self._current_count >= self.max_per_file:
                    logging.info(
                        f"📄 File sent penuh: {self._current_file.name} "
                        f"({self._current_count} entry) — "
                        f"File baru akan dibuat saat entry berikutnya"
                    )

            except Exception as e:
                logging.error(f"❌ Gagal simpan ke sent file: {e}")
                # Fallback: simpan ke cache saja
                self._cache.add(key)

    def check(self, key: str) -> bool:
        """Cek apakah key sudah ada di cache"""
        return key in self._cache

    async def cleanup_old_files(self, keep_last_n: int = 5) -> None:
        """Hapus file lama, hanya simpan N file terbaru"""
        files = self._get_all_sent_files()
        if len(files) <= keep_last_n:
            return

        to_delete = files[:-keep_last_n]  # File lama
        for f in to_delete:
            try:
                f.unlink()
                logging.info(f"🗑️ File sent lama dihapus: {f.name}")
            except Exception as e:
                logging.error(f"❌ Gagal hapus {f.name}: {e}")

# ============================================================
# === INISIALISASI SENT FILE MANAGER ===
# ============================================================
sent_manager = SentFileManager(
    sent_dir=SENT_DIR,
    prefix=SENT_FILE_PREFIX,
    max_per_file=MAX_SENT_PER_FILE
)

# ============================================================
# === LOCAL STATE MANAGER ===
# ============================================================
class LocalStateManager:
    """Mengelola state lokal (pending, daily, flood)"""

    def __init__(self, state_dir: Path):
        self.state_dir = state_dir

    async def save_pending(self, pending: list) -> None:
        try:
            with open(FILE_PENDING, "w", encoding="utf-8") as fp:
                json.dump(pending, fp)
            logging.debug(f"💾 Save pending: {len(pending)} item")
        except Exception as e:
            logging.error(f"❌ Gagal save pending: {e}")

    async def load_pending(self) -> list:
        try:
            if not FILE_PENDING.exists():
                return []
            with open(FILE_PENDING, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                if isinstance(data, list):
                    return data
                return []
        except Exception as e:
            logging.error(f"❌ Gagal load pending: {e}")
            return []

    async def save_daily(self, count: int, reset_date: datetime.date) -> None:
        try:
            data = {
                "count": count,
                "reset_date": reset_date.isoformat()
            }
            with open(FILE_DAILY, "w", encoding="utf-8") as fp:
                json.dump(data, fp)
            logging.debug(f"💾 Save daily: {count} | {reset_date}")
        except Exception as e:
            logging.error(f"❌ Gagal save daily: {e}")

    async def load_daily(self) -> tuple[int, datetime.date]:
        try:
            if not FILE_DAILY.exists():
                return 0, datetime.now(timezone.utc).date()
            with open(FILE_DAILY, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                count = int(data.get("count", 0))
                reset_date_str = data.get("reset_date", "")
                try:
                    reset_date = datetime.fromisoformat(reset_date_str).date()
                except (ValueError, TypeError):
                    reset_date = datetime.now(timezone.utc).date()
                return count, reset_date
        except Exception as e:
            logging.error(f"❌ Gagal load daily: {e}")
            return 0, datetime.now(timezone.utc).date()

    async def save_flood(self, flood_data: dict) -> None:
        try:
            with open(FILE_FLOOD, "w", encoding="utf-8") as fp:
                json.dump(flood_data, fp)
            logging.debug("💾 Save flood state")
        except Exception as e:
            logging.error(f"❌ Gagal save flood: {e}")

    async def load_flood(self) -> dict:
        try:
            if not FILE_FLOOD.exists():
                return {}
            with open(FILE_FLOOD, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                if isinstance(data, dict):
                    return data
                return {}
        except Exception as e:
            logging.error(f"❌ Gagal load flood: {e}")
            return {}

    async def save_config(self, config: dict) -> None:
        try:
            with open(FILE_CONFIG, "w", encoding="utf-8") as fp:
                json.dump(config, fp)
            logging.debug("💾 Save config")
        except Exception as e:
            logging.error(f"❌ Gagal save config: {e}")

    async def load_config(self) -> dict:
        try:
            if not FILE_CONFIG.exists():
                return {}
            with open(FILE_CONFIG, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                if isinstance(data, dict):
                    return data
                return {}
        except Exception as e:
            logging.error(f"❌ Gagal load config: {e}")
            return {}

# ============================================================
# === INISIALISASI STATE MANAGER ===
# ============================================================
state_manager = LocalStateManager(STATE_DIR)

# ============================================================
# === HELPERS ===
# ============================================================
def make_hash(fingerprint: dict) -> str:
    fp_copy = {
        k: v for k, v in fingerprint.items()
        if k not in ("file_id", "timestamp")
    }
    return hashlib.md5(
        json.dumps(fp_copy, sort_keys=True).encode()
    ).hexdigest()

def parse_retry_after(err: str) -> int:
    match = re.search(r"[Rr]etry in (\d+)", err)
    if match:
        return int(match.group(1))
    match = re.search(r"retry_after=(\d+)", err)
    if match:
        return int(match.group(1))
    return 60

def _flatten_arg(arg) -> str | None:
    while isinstance(arg, (list, tuple)):
        if len(arg) == 0:
            return None
        arg = arg
    return str(arg).strip() if arg is not None else None

# ============================================================
# === DUPLICATE CHECK (LOKAL) ===
# ============================================================
async def is_duplicate(file_id: str, fp_hash: str) -> bool:
    # Cek di sent manager (cache)
    if sent_manager.check(file_id):
        return True
    if sent_manager.check(fp_hash):
        return True

    return False

async def mark_sent(file_id: str, fp_hash: str) -> None:
    # Simpan ke file
    await sent_manager.add(file_id)
    await sent_manager.add(fp_hash)

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
        await load_daily_limit()

async def save_daily() -> None:
    await state_manager.save_daily(daily_count, daily_reset_date)

async def load_daily_limit() -> None:
    global DAILY_LIMIT, DELAY_BETWEEN_SEND
    try:
        config = await state_manager.load_config()
        if "daily_limit" in config:
            DAILY_LIMIT = int(config["daily_limit"])
            logging.info(f"📥 Daily limit dimuat: {DAILY_LIMIT}")
    except (ValueError, TypeError) as e:
        logging.warning(f"⚠️ Gagal load daily limit: {e}")

    try:
        config = await state_manager.load_config()
        if "send_delay" in config:
            DELAY_BETWEEN_SEND = float(config["send_delay"])
            logging.info(f"📥 Send delay dimuat: {DELAY_BETWEEN_SEND}s")
    except (ValueError, TypeError) as e:
        logging.warning(f"⚠️ Gagal load send delay: {e}")

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
            await state_manager.save_flood(self.to_dict())
        except Exception as e:
            logging.error(f"❌ Gagal save flood state: {e}")

# ============================================================
# === PERSIST: SAVE & LOAD ALL ===
# ============================================================
async def save_all() -> None:
    global last_save_time

    # Save pending
    pending_snapshot = list(pending_queue._queue) if pending_queue else []
    try:
        await state_manager.save_pending(pending_snapshot)
    except Exception as e:
        logging.error(f"❌ Gagal save pending: {e}")

    # Save daily
    await save_daily()

    # Save flood
    if flood_ctrl:
        await flood_ctrl.save_state()

    # Save config (daily limit & delay)
    config = {
        "daily_limit": DAILY_LIMIT,
        "send_delay": DELAY_BETWEEN_SEND
    }
    await state_manager.save_config(config)

    last_save_time = datetime.now(timezone.utc)
    logging.debug(f"💾 Auto-save selesai | Pending: {len(pending_snapshot)}")

async def load_all() -> None:
    global daily_count, daily_reset_date

    # Load pending
    try:
        loaded = await state_manager.load_pending()
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
    except Exception as e:
        logging.error(f"❌ Gagal load pending, reset: {e}")

    # Load daily
    try:
        daily_count, daily_reset_date = await state_manager.load_daily()
    except Exception as e:
        logging.warning(f"⚠️ Gagal load daily: {e}")

    # Load flood
    try:
        flood_data = await state_manager.load_flood()
        if flood_data:
            global flood_ctrl
            flood_ctrl = SmartFloodController.from_dict(flood_data)
            logging.info(f"📥 Flood ctrl dimuat: {flood_ctrl.get_status()}")
    except Exception as e:
        logging.warning(f"⚠️ Gagal load flood ctrl: {e}")

    # Load config
    try:
        config = await state_manager.load_config()
        if "daily_limit" in config:
            DAILY_LIMIT = int(config["daily_limit"])
            logging.info(f"📥 Daily limit dimuat: {DAILY_LIMIT}")
        if "send_delay" in config:
            DELAY_BETWEEN_SEND = float(config["send_delay"])
            logging.info(f"📥 Send delay dimuat: {DELAY_BETWEEN_SEND}s")
    except Exception as e:
        logging.warning(f"⚠️ Gagal load config: {e}")

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
                continue

            # Chat tidak ditemukan / migrasi
            elif "chat not found" in err or "forbidden" in err:
                migrated = re.search(r"migrated to chat (\-?\d+)", str(e))
                if migrated:
                    new_id = int(migrated.group(1))
                    logging.info(f"🔄 Chat dipindahkan ke {new_id}, update target...")
                    global TARGET_CHAT_ID
                    TARGET_CHAT_ID = new_id
                    # Simpan ke config
                    config = {
                        "daily_limit": DAILY_LIMIT,
                        "send_delay": DELAY_BETWEEN_SEND,
                        "target_chat_id": TARGET_CHAT_ID
                    }
                    await state_manager.save_config(config)
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
                        continue  # ✅ Jangan task_done() di sini

                    media_items.append((file_id, media_type, fp_hash))

                except (ValueError, TypeError) as e:
                    logging.warning(f"⚠️ Item queue korup, skip: {e}")
                    continue  # ✅ Jangan task_done() di sini

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

                # Hitung delay
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

            # Task done untuk semua item batch — hanya sekali
            for _ in batch:
                try:
                    pending_queue.task_done()
                except ValueError:
                    pass  # Ignore if already done

            is_sending = False

            # Auto-save periodik
            now = datetime.now(timezone.utc)
            if (now - last_save_time).total_seconds() >= AUTO_SAVE_INTERVAL:
                await save_all()

            # Cleanup local_sent periodik
            iteration_count += 1
            if iteration_count % 100 == 0:
                # Cleanup file lama (simpan 5 file terbaru)
                await sent_manager.cleanup_old_files(keep_last_n=5)

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
async def forward_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg:
        return

    # Validasi sumber chat
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
    await update.message.reply_text("🏓 Pong!")

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
        "└ /resume — Lanjutkan worker\n"
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

    # Hitung total sent dari semua file
    total_sent = len(sent_manager._cache)

    queue_size   = pending_queue.qsize() if pending_queue else 0
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

    # Uptime
    now = datetime.now(timezone.utc)
    uptime = now - start_time
    hours, remainder = divmod(int(uptime.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)

    text = (
        "📊 *STATUS BOT HANAYA v5.0 — " + BOT_NAME + "*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Worker       : {worker_status}\n"
        f"📦 Pending      : {queue_size} media\n"
        f"📤 Terkirim     : {daily_count}/{DAILY_LIMIT} ({daily_pct:.1f}%)\n"
        f"🌍 Global Sent  : {total_sent} media\n"
        f"    └─ {bar}\n"
        f"⏱️ Last Save    : {last_save_str}\n"
        f"⚡ Flood Ctrl   : {flood_status}\n"
        f"⏱️ Uptime       : {hours}h {minutes}m {seconds}s\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 Gunakan /help untuk daftar command"
    )

    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return

    total_sent = len(sent_manager._cache)
    queue_size   = pending_queue.qsize() if pending_queue else 0
    remaining    = max(0, DAILY_LIMIT - daily_count)
    pct_used     = (daily_count / DAILY_LIMIT * 100) if DAILY_LIMIT > 0 else 0
    bar_filled   = int(pct_used / 10)
    bar          = "█" * bar_filled + "░" * (10 - bar_filled)

    text = (
        f"📈 *STATISTIK REALTIME — {BOT_NAME}*\n\n"
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
        # Simpan ke config
        config = {
            "daily_limit": DAILY_LIMIT,
            "send_delay": DELAY_BETWEEN_SEND
        }
        await state_manager.save_config(config)
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
        # Simpan ke config
        config = {
            "daily_limit": DAILY_LIMIT,
            "send_delay": DELAY_BETWEEN_SEND
        }
        await state_manager.save_config(config)
        name = update.effective_user.first_name
        await update.message.reply_text(
            f"✅ Delay diubah ke {DELAY_BETWEEN_SEND:.1f}s oleh {name}"
        )
        logging.info(f"⚙️ Send delay → {DELAY_BETWEEN_SEND:.1f}s oleh {name}")
    except (ValueError, TypeError):
        await update.message.reply_text("❌ Contoh: /setdelay 2.5")

async def cmd_log(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa lihat log")
        return
    try:
        with open(f"{BOT_NAME}.log", "r", encoding="utf-8") as f:
            lines = f.readlines()[-15:]
        log_text = "".join(lines)
        await update.message.reply_text(
            f"```\n{log_text}```", parse_mode="Markdown"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Gagal baca log: {e}")

async def cmd_sentfiles(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Tampilkan info file sent yang tersimpan"""
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return

    files = sent_manager._get_all_sent_files()
    total_cache = len(sent_manager._cache)

    if not files:
        await update.message.reply_text("📂 Tidak ada file sent tersimpan")
        return

    lines = [
        f"📂 *FILE SENT — {BOT_NAME}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Total cache : {total_cache} entry\n"
        f"📄 Jumlah file : {len(files)} file\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    ]

    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                count = len(data) if isinstance(data, list) else 0
            size_kb = f.stat().st_size / 1024
            is_active = "✅ Aktif" if f == sent_manager._current_file else "📦 Arsip"
            lines.append(
                f"{is_active} | {f.name} | "
                f"{count}/{MAX_SENT_PER_FILE} entry | "
                f"{size_kb:.1f} KB\n"
            )
        except Exception as e:
            lines.append(f"❌ {f.name} — Error: {e}\n")

    await update.message.reply_text("".join(lines), parse_mode="Markdown")

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
    global sending_lock, config_lock, pending_queue, flood_ctrl

    sending_lock  = asyncio.Lock()
    config_lock   = asyncio.Lock()
    pending_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)

    # Load sent files (semua file lama + baru)
    await sent_manager.load_all()

    # Load state
    await load_all()

    # Inisialisasi flood controller
    if flood_ctrl is None:
        flood_ctrl = SmartFloodController()

    # Start worker
    asyncio.create_task(queue_worker(app.bot))

    logging.info(f"🚀 Bot {BOT_NAME} siap!")

async def on_shutdown(app) -> None:
    await save_all()
    logging.info(f"🛑 Bot {BOT_NAME} berhenti, data tersimpan")

# ============================================================
# === SIGNAL HANDLER ===
# ============================================================
def handle_shutdown(signum, frame):
    logging.info("⚠️ Shutdown signal diterima, menyimpan data...")
    try:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(
            lambda: asyncio.create_task(_async_shutdown())
        )
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
    app.add_handler(MessageHandler(
        filters.VIDEO | filters.Document.ALL, forward_media
    ))

    # Admin commands
    app.add_handler(CommandHandler("ping",         cmd_ping))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(CommandHandler("status",       cmd_status))
    app.add_handler(CommandHandler("stats",        cmd_stats))
    app.add_handler(CommandHandler("pause",        cmd_pause))
    app.add_handler(CommandHandler("resume",       cmd_resume))
    app.add_handler(CommandHandler("flushpending", cmd_flushpending))
    app.add_handler(CommandHandler("resetdaily",   cmd_resetdaily))
    app.add_handler(CommandHandler("setlimit",     cmd_setlimit))
    app.add_handler(CommandHandler("setdelay",     cmd_setdelay))
    app.add_handler(CommandHandler("log",          cmd_log))
    app.add_handler(CommandHandler("sentfiles",    cmd_sentfiles))
    app.add_handler(CommandHandler("shutdown",     cmd_shutdown))

    logging.info("╔══════════════════════════════════╗")
    logging.info(f"║ 🌸 HANAYA BOT v5.0 — {BOT_NAME}     ║")
    logging.info(f"║  Daily Limit : {DAILY_LIMIT} Media/hari   ║")
    logging.info(f"║  Group Size  : {GROUP_SIZE} Media/kelompok  ║")
    logging.info(f"║  Max Pending : {MAX_QUEUE_SIZE} Media        ║")
    logging.info(f"║  Sent/File   : {MAX_SENT_PER_FILE} Entry/file   ║")
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