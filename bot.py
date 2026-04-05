import logging
import json
import os
import asyncio
import random
import hashlib
import re
import signal
import sys
import time
import threading
import platform
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import deque

from telegram import Update, InputMediaVideo, InputMediaPhoto, InputMediaDocument
from telegram.ext import (
    ApplicationBuilder, ContextTypes,
    MessageHandler, filters, CommandHandler
)
from dotenv import load_dotenv
from flask import Flask, jsonify

# ============================================================
# === PLATFORM-SPECIFIC IMPORTS ===
# ============================================================
if platform.system() != "Windows":
    import fcntl
    HAS_FCNTL = True
else:
    HAS_FCNTL = False

# ============================================================
# === LOAD ENV ===
# ============================================================
_env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(_env_file)

# ============================================================
# === KONFIGURASI UTAMA ===
# ============================================================
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", 0))
ADMIN_CHAT_ID  = int(os.getenv("ADMIN_CHAT_ID", 0))
BOT_NAME       = os.getenv("BOT_NAME", "")
FLASK_PORT     = int(os.getenv("FLASK_PORT", 5000))

# ============================================================
# === LOGGING CONFIGURATION (GLOBAL) ===
# ============================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Multi-target support
TARGET_CHAT_IDS: List[int] = []
for _i in range(1, 6):
    _val = os.getenv(f"TARGET_CHAT_ID_{_i}")
    if _val:
        try:
            TARGET_CHAT_IDS.append(int(_val))
        except ValueError:
            pass

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN tidak diatur di .env")
if TARGET_CHAT_ID == 0 and not TARGET_CHAT_IDS:
    raise ValueError("❌ TARGET_CHAT_ID tidak diatur di .env")

# ============================================================
# === ADMIN CONFIG ===
# ============================================================
def _parse_id_list(env_key: str) -> List[int]:
    raw = os.getenv(env_key, "")
    result = []
    for uid in raw.split(","):
        uid = uid.strip()
        if uid:
            try:
                result.append(int(uid))
            except ValueError:
                pass
    return result

ADMINS: Dict[int, str] = {}
for _uid in _parse_id_list("SUPERADMINS"):
    ADMINS[_uid] = "superadmin"
for _uid in _parse_id_list("MODERATORS"):
    ADMINS[_uid] = "moderator"

ALLOWED_SOURCE_CHATS = _parse_id_list("ALLOWED_SOURCE_CHATS")

# ============================================================
# === MEDIA TYPE CONFIGURATION (GLOBAL) ===
# ============================================================
def _parse_media_types(env_key: str) -> set:
    """Parse media types dari ENV dengan validasi ketat"""
    raw = os.getenv(env_key, "").lower().strip()
    
    ALL_TYPES = {
        "video", "photo", "document", "audio",
        "animation", "voice", "video_note", "sticker"
    }
    
    # Default yang reasonable
    if not raw:
        default = {"video", "photo", "document"}
        return default
    
    allowed_types = set()
    for media_type in raw.split(","):
        media_type = media_type.strip().lower()
        if media_type in ALL_TYPES:
            allowed_types.add(media_type)
    
    # Jika hasil parsing kosong, gunakan default
    if not allowed_types:
        default = {"video", "photo", "document"}
        return default
    
    return allowed_types

# Gunakan ALLOWED_MEDIA_TYPES global
ALLOWED_MEDIA_TYPES = _parse_media_types("ALLOWED_MEDIA_TYPES")

# Mapping untuk display
MEDIA_TYPE_DISPLAY = {
    "video": "🎬 Video (MP4, WebM, MKV, AVI, MOV, FLV, WMV, 3GP)",
    "photo": "🖼️ Photo (JPG, PNG, WebP, HEIC, BMP, TIFF)",
    "document": "📄 Document/File (ZIP, RAR, 7Z, PDF, DOCX, XLSX, TXT, JSON)",
    "audio": "🎵 Audio (MP3, WAV, FLAC, AAC, OGG, M4A, OPUS)",
    "animation": "🎞️ Animation/GIF (MP4/WebM converted GIF)",
    "voice": "🎤 Voice Message (OGG Opus)",
    "video_note": "📹 Video Message (Circular video)",
    "sticker": "🎨 Sticker (WebP, WEBM)",
}

# ============================================================
# === RATE LIMIT SETTINGS (GLOBAL) ===
# ============================================================
DELAY_BETWEEN_SEND      = float(os.getenv("DELAY_BETWEEN_SEND", "0.5"))
DELAY_RANDOM_MIN        = float(os.getenv("DELAY_RANDOM_MIN", "0.1"))
DELAY_RANDOM_MAX        = float(os.getenv("DELAY_RANDOM_MAX", "0.7"))
GROUP_SIZE              = int(os.getenv("GROUP_SIZE", "5"))
DELAY_BETWEEN_GROUP_MIN = int(os.getenv("DELAY_BETWEEN_GROUP_MIN", "15"))
DELAY_BETWEEN_GROUP_MAX = int(os.getenv("DELAY_BETWEEN_GROUP_MAX", "45"))
BATCH_PAUSE_EVERY       = int(os.getenv("BATCH_PAUSE_EVERY", "500"))
BATCH_PAUSE_MIN         = int(os.getenv("BATCH_PAUSE_MIN", "30"))
BATCH_PAUSE_MAX         = int(os.getenv("BATCH_PAUSE_MAX", "120"))
DAILY_LIMIT             = int(os.getenv("DAILY_LIMIT", "2500"))
MAX_RETRIES             = int(os.getenv("MAX_RETRIES", "2"))
MAX_QUEUE_SIZE          = int(os.getenv("MAX_QUEUE_SIZE", "1000"))
AUTO_SAVE_INTERVAL      = int(os.getenv("AUTO_SAVE_INTERVAL", "60"))

# Validasi config
if not (100 <= MAX_QUEUE_SIZE <= 10000):
    raise ValueError("MAX_QUEUE_SIZE harus antara 100-10000")

# ============================================================
# === SMART FLOOD CONTROL SETTINGS ===
# ============================================================
FLOOD_RANDOM_MIN   = 10
FLOOD_RANDOM_MAX   = 30
FLOOD_PENALTY_STEP = 15
FLOOD_MAX_PENALTY  = 300
FLOOD_RESET_AFTER  = 600

# ============================================================
# === DIREKTORI DATA ===
# ============================================================
GLOBAL_DATA_DIR = Path("data/global")
GLOBAL_SENT_DIR = GLOBAL_DATA_DIR / "sent"

LOCAL_DATA_DIR  = Path(f"data/{BOT_NAME}")
STATE_DIR       = LOCAL_DATA_DIR / "state"

GLOBAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
GLOBAL_SENT_DIR.mkdir(parents=True, exist_ok=True)
LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# === FILE PATH ===
# ============================================================
SENT_FILE_PREFIX  = "sent"
SENT_FILE_EXT     = ".json"
MAX_SENT_PER_FILE = 2000

FILE_PENDING   = STATE_DIR / "pending.json"
FILE_DAILY     = STATE_DIR / "daily.json"
FILE_FLOOD     = STATE_DIR / "flood.json"
FILE_CONFIG    = STATE_DIR / "config.json"
FILE_RATELIMIT = STATE_DIR / "ratelimit.json"

# ============================================================
# === CLEAN CONSOLE OUTPUT MANAGER ===
# ============================================================
class ConsoleOutputManager:
    """Manager untuk output console yang clean dari log file"""
    
    def __init__(self, log_file: Path, max_lines: int = 50):
        self.log_file = log_file
        self.max_lines = max_lines
    
    def get_latest_logs(self) -> List[str]:
        """Ambil log terbaru dari file"""
        try:
            if not self.log_file.exists():
                return []
            
            with open(self.log_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
                return lines[-self.max_lines:] if lines else []
        except Exception as e:
            return [f"❌ Error reading log: {e}"]
    
    def print_startup_banner(self, bot_name: str):
        """Print banner startup yang clean"""
        banner = f"""
╔════════════════════════════════════════════════════════════════
║                                                               
║               🌸 HANAYA BOT v5.7 — PRODUCTION       
║                                                                
║  Bot Name       : {bot_name:<45} 
║  Status         : ✅ STARTING                                
║  Mode           : Polling (Long-polling)                     
║  Anti-Duplikat  : ✅ TRIPLE-CHECK + QUEUE DEDUP          
║  Global Sent    : data/global/sent/ (SHARED)                  
║  Lock Type      : OS-level (fcntl) + asyncio                  
║  Reload Interval: 10 detik                                    
║                                                                
║  📊 Dashboard   : http://localhost:{FLASK_PORT}/dashboard      
║  🏥 Health Check: http://localhost:{FLASK_PORT}/health         
║                                                                
║  Logs Directory : logs/                                        
║  Data Directory : data/{bot_name}/                             
║  Global Sent    : data/global/sent/                            
║                                                                
╚════════════════════════════════════════════════════════════════
"""
        print(banner)
    
    def print_config_summary(self):
        """Print summary konfigurasi"""
        allowed_chats_str = (
            f"{TARGET_CHAT_ID}" if TARGET_CHAT_ID != 0 
            else f"{', '.join(map(str, TARGET_CHAT_IDS))}"
        )
        
        summary = f"""
⚙️  KONFIGURASI AKTIF:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 Media Types yang Diizinkan:
   {', '.join(sorted(ALLOWED_MEDIA_TYPES))}

📊 Rate Limiting:
   ├─ Delay antar kirim      : {DELAY_BETWEEN_SEND}s (+ {DELAY_RANDOM_MIN}-{DELAY_RANDOM_MAX}s random)
   ├─ Group size             : {GROUP_SIZE} media/kelompok
   ├─ Delay antar group      : {DELAY_BETWEEN_GROUP_MIN}-{DELAY_BETWEEN_GROUP_MAX}s
   ├─ Batch pause every      : {BATCH_PAUSE_EVERY} media
   ├─ Batch pause duration   : {BATCH_PAUSE_MIN}-{BATCH_PAUSE_MAX}s
   └─ Daily limit            : {DAILY_LIMIT} media/hari

📦 Queue Configuration:
   ├─ Max queue size         : {MAX_QUEUE_SIZE} media
   ├─ Max retries            : {MAX_RETRIES}
   ├─ Auto-save interval     : {AUTO_SAVE_INTERVAL}s
   └─ Max per sent file      : {MAX_SENT_PER_FILE} entry

🔒 Anti-Duplikat:
   ├─ Triple-check           : ✅ ENABLED
   ├─ Queue deduplication    : ✅ ENABLED
   ├─ Atomic write + fsync   : ✅ ENABLED
   ├─ OS-level file lock     : ✅ ENABLED
   └─ Reload interval        : 10 detik

🎯 Target Chats:
   └─ {allowed_chats_str}

👥 Admin Configuration:
   ├─ Superadmins           : {len([u for u, r in ADMINS.items() if r == "superadmin"])} user
   ├─ Moderators            : {len([u for u, r in ADMINS.items() if r == "moderator"])} user
   └─ Allowed source chats   : {len(ALLOWED_SOURCE_CHATS)} chat(s)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        print(summary)
    
    def print_startup_logs(self):
        """Print log startup terbaru"""
        logs = self.get_latest_logs()
        if logs:
            print("\n📋 STARTUP LOGS (Latest 20):\n")
            print("─" * 70)
            for line in logs[-20:]:
                print(line.rstrip())
            print("─" * 70)
    
    def print_status_line(self, status_msg: str):
        """Print status line yang clean"""
        print(f"\n✨ {status_msg}")
    
    def print_ready(self):
        """Print ready message"""
        ready = f"""
╔════════════════════════════════════════════════════════════════
║                                                                
║                    ✅ BOT READY & LISTENING                   
║                                                                
║  🚀 Queue Worker        : ACTIVE                              
║  📡 Message Handler     : ACTIVE                              
║  🌐 Flask Dashboard     : ACTIVE (:{FLASK_PORT})              
║  💾 Auto-Save           : ACTIVE ({AUTO_SAVE_INTERVAL}s)    
║  🔄 Global Sync         : ACTIVE (10s)                     
║                                                               
║  📊 Commands:                                                
║     /ping         - Health check                              
║     /status       - Bot status                                
║     /stats        - Realtime stats                            
║     /help         - Daftar command                            
║                                                                
║  🌐 Web Access:                                                
║     Dashboard  : http://localhost:{FLASK_PORT}/dashboard  
║     Health API : http://localhost:{FLASK_PORT}/health         
║                                                               
║  📁 Logs:                                                     
║     Main   : logs/{BOT_NAME}_main.log                        
║     Network: logs/{BOT_NAME}_network.log                    
║     Debug  : logs/{BOT_NAME}_debug.log                      
║     Reload : logs/{BOT_NAME}_reload.log                      
║                                                                
║  Press Ctrl+C untuk shutdown gracefully...                   
║                                                                
╚════════════════════════════════════════════════════════════════
"""
        print(ready)

# ============================================================
# === ADVANCED LOGGING SYSTEM ===
# ============================================================
class CompactFormatter(logging.Formatter):
    """Formatter yang compact dengan warna dan error handling"""
    COLORS = {
        'DEBUG':    '\033[36m',
        'INFO':     '\033[32m',
        'WARNING':  '\033[33m',
        'ERROR':    '\033[31m',
        'CRITICAL': '\033[35m',
        'RESET':    '\033[0m'
    }

    def format(self, record):
        """Format log dengan penanganan error yang baik"""
        timestamp = datetime.fromtimestamp(record.created).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        level   = record.levelname
        message = record.getMessage()
        color   = self.COLORS.get(level, '')
        reset   = self.COLORS['RESET']

        if record.exc_info:
            try:
                exc_type, exc_value, exc_tb = record.exc_info
                
                if exc_type and hasattr(exc_type, '__name__'):
                    exc_name = exc_type.__name__
                    exc_msg = str(exc_value)[:150] if exc_value else "Unknown"
                    
                    if record.levelno >= logging.ERROR:
                        return (
                            f"{timestamp} [{color}{level}{reset}] {message}\n"
                            f"    └─ {exc_name}: {exc_msg}"
                        )
            except Exception as e:
                logging.debug(f"⚠️ Error formatting exc_info: {e}")

        return f"{timestamp} [{color}{level}{reset}] {message}"


class NetworkErrorFilter(logging.Filter):
    """Filter untuk mengurangi spam network errors dengan cleanup"""
    def __init__(self, max_same_errors: int = 3, cache_ttl: int = 3600):
        super().__init__()
        self.error_cache: Dict[str, Tuple[int, float]] = {}
        self.max_same_errors: int = max_same_errors
        self.cache_ttl: int = cache_ttl
        self.last_cleanup: float = time.time()

    def _cleanup_cache(self) -> None:
        """✅ Bersihkan cache yang sudah lama"""
        now = time.time()
        
        if now - self.last_cleanup < 300:
            return
        
        expired = [
            k for k, (_, last_time) in self.error_cache.items()
            if now - last_time > self.cache_ttl
        ]
        
        for k in expired:
            del self.error_cache[k]
        
        if expired:
            logging.debug(
                f"🧹 NetworkErrorFilter cache cleaned: {len(expired)} entries"
            )
        
        self.last_cleanup = now

    def filter(self, record):
        """Filter log berdasarkan error type"""
        self._cleanup_cache()
        
        if record.levelno >= logging.ERROR:
            exc_info = record.exc_info
            if exc_info and isinstance(exc_info, tuple) and len(exc_info) >= 1:
                exc_type = exc_info[0]
                exc_name = (
                    exc_type.__name__ 
                    if exc_type and hasattr(exc_type, '__name__') 
                    else "Unknown"
                )

                if exc_name in self.error_cache:
                    count, last_time = self.error_cache[exc_name]
                    now = time.time()
                    if now - last_time > 60:
                        self.error_cache[exc_name] = (1, now)
                    else:
                        self.error_cache[exc_name] = (count + 1, now)
                        if count >= self.max_same_errors:
                            return False
                else:
                    self.error_cache[exc_name] = (1, time.time())
        
        return True


class LogFilter(logging.Filter):
    """Filter untuk memisahkan log berdasarkan kategori"""
    
    def __init__(self, filter_type: str = "main"):
        super().__init__()
        self.filter_type = filter_type

    def filter(self, record):
        """Deteksi dan filter log berdasarkan tipe"""
        message = record.getMessage()
        level = record.levelname
        logger_name = record.name
        
        # === DETEKSI KATEGORI ===
        
        # 1. NETWORK (prioritas tertinggi)
        is_network = (
            "api.telegram.org" in message or
            "GET " in message or "POST " in message or
            "HTTP/" in message or
            "127.0.0.1" in message or
            "connection" in message.lower() or
            "timeout" in message.lower() or
            "network" in message.lower() or
            "socket" in message.lower() or
            "ssl" in message.lower() or
            "certificate" in message.lower() or
            logger_name.startswith("httpx") or
            logger_name.startswith("httpcore") or
            logger_name.startswith("urllib3")
        )
        
        # 2. RELOAD/CACHE
        is_reload = (
            "Cache reloaded" in message or 
            "[GLOBAL]" in message or
            "[RELOAD]" in message or
            "cleanup" in message.lower() or
            "file lama dihapus" in message
        )
        
        # 3. DEBUG (prioritas terendah)
        is_debug = (
            level == "DEBUG" or
            "send_request" in message or
            "receive_response" in message or
            "task" in message.lower() or
            "coroutine" in message.lower()
        )
        
        # === ROUTING ===
        
        if self.filter_type == "main":
            return not (is_network or is_reload or is_debug)
        elif self.filter_type == "network":
            return is_network
        elif self.filter_type == "reload":
            return is_reload
        elif self.filter_type == "debug":
            return is_debug and not is_network and not is_reload
        elif self.filter_type == "all":
            return True
        
        return True


def setup_logging(bot_name: str):
    """Setup logging dengan file terpisah dan formatter yang konsisten"""
    
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    formatter = CompactFormatter()
    
    # MAIN LOG
    main_log_file = log_dir / f"{bot_name}_main.log"
    main_handler = RotatingFileHandler(
        main_log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    main_handler.setFormatter(formatter)
    main_handler.addFilter(LogFilter(filter_type="main"))
    main_handler.addFilter(NetworkErrorFilter(max_same_errors=3))
    
    # NETWORK LOG
    network_log_file = log_dir / f"{bot_name}_network.log"
    network_handler = RotatingFileHandler(
        network_log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8"
    )
    network_handler.setFormatter(formatter)
    network_handler.addFilter(LogFilter(filter_type="network"))
    
    # DEBUG LOG
    debug_log_file = log_dir / f"{bot_name}_debug.log"
    debug_handler = RotatingFileHandler(
        debug_log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=2,
        encoding="utf-8"
    )
    debug_handler.setFormatter(formatter)
    debug_handler.addFilter(LogFilter(filter_type="debug"))
    
    # RELOAD LOG
    reload_log_file = log_dir / f"{bot_name}_reload.log"
    reload_handler = RotatingFileHandler(
        reload_log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8"
    )
    reload_handler.setFormatter(formatter)
    reload_handler.addFilter(LogFilter(filter_type="reload"))
    
    # ✅ CONSOLE HANDLER YANG CLEAN (hanya important messages)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.addFilter(NetworkErrorFilter(max_same_errors=3))
    # ✅ Filter untuk console: hanya WARNING, ERROR, CRITICAL
    console_handler.setLevel(logging.WARNING)
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    root_logger.addHandler(main_handler)
    root_logger.addHandler(network_handler)
    root_logger.addHandler(debug_handler)
    root_logger.addHandler(reload_handler)
    root_logger.addHandler(console_handler)
    
    # ✅ Library logging levels
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("telegram.ext").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)

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
ratelimit_lock:  asyncio.Lock | None  = None
is_paused:       bool                 = False
flood_ctrl:      "SmartFloodController | None" = None
start_time                            = datetime.now(timezone.utc)
user_ratelimit:  Dict[int, float]     = {}
admin_ratelimit: Dict[int, float]     = {}
_worker_task:    asyncio.Task | None  = None
_shutdown_event: asyncio.Event | None = None
console_mgr:     ConsoleOutputManager | None = None

# ============================================================
# === HELPER FUNCTIONS ===
# ============================================================
def get_queue_snapshot(q: asyncio.Queue) -> list:
    """✅ Ambil snapshot queue dengan aman"""
    try:
        return list(q._queue)  # type: ignore[attr-defined]
    except AttributeError:
        result = []
        temp = []
        while not q.empty():
            try:
                item = q.get_nowait()
                result.append(item)
                temp.append(item)
            except asyncio.QueueEmpty:
                break
        for item in temp:
            try:
                q.put_nowait(item)
            except asyncio.QueueFull:
                pass
        return result


def _flatten_arg(arg, max_depth: int = 10) -> str | None:
    """✅ Flatten argument dari command dengan depth limit"""
    depth = 0
    while isinstance(arg, (list, tuple)) and depth < max_depth:
        if len(arg) == 0:
            return None
        arg = arg[0]
        depth += 1
    
    if depth >= max_depth:
        logging.warning(f"⚠️ Arg nesting terlalu dalam, truncate")
        return None
    
    if arg is None:
        return None
    
    result = str(arg).strip()
    return result if result else None


def make_hash(fingerprint: dict) -> str:
    """✅ Buat hash dari fingerprint dengan normalisasi"""
    fp_copy = {
        k: v for k, v in fingerprint.items()
        if k not in ("file_id", "timestamp") and v is not None
    }
    
    normalized = {}
    for k in sorted(fp_copy.keys()):
        v = fp_copy[k]
        if isinstance(v, (int, float)):
            normalized[k] = str(v)
        elif v is None:
            continue
        else:
            normalized[k] = str(v)
    
    return hashlib.md5(
        json.dumps(normalized, sort_keys=True).encode()
    ).hexdigest()


def parse_retry_after(err: str) -> int:
    """✅ Parse retry_after dari error message dengan robust handling"""
    err_str = str(err).lower()
    
    patterns = [
        r"retry\s+(?:in|after)\s+(\d+)",
        r"retry_after[=:\s]+(\d+)",
        r"(\d+)\s*(?:seconds?|secs?)",
        r"please\s+retry\s+in\s+(\d+)",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, err_str)
        if match:
            try:
                retry_seconds = int(match.group(1))
                return max(1, min(retry_seconds, 3600))
            except (ValueError, IndexError):
                continue
    
    default_retry = 30
    logging.warning(
        f"⚠️ Tidak bisa parse retry_after dari: {err_str[:100]}, "
        f"gunakan default: {default_retry}s"
    )
    return default_retry


# ============================================================
# === GLOBAL SENT FILE MANAGER ===
# ============================================================
class GlobalSentFileManager:
    """Mengelola file duplikat GLOBAL yang dipakai bersama semua bot"""

    def __init__(
        self,
        sent_dir:        Path,
        prefix:          str,
        max_per_file:    int,
        reload_interval: int = 10,
    ):
        self.sent_dir        = sent_dir
        self.prefix          = prefix
        self.max_per_file    = max_per_file
        self.reload_interval = reload_interval

        self._cache:          set           = set()
        self._current_file:   Path | None   = None
        self._current_count:  int           = 0
        self._async_lock      = asyncio.Lock()
        self._last_reload:    float         = 0.0
        self._known_files:    List[Path]    = []

    def _acquire_file_lock(self, fp) -> None:
        """✅ Kunci file di level OS (antar proses)"""
        if not HAS_FCNTL:
            return
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
        except Exception as e:
            logging.warning(f"⚠️ [GLOBAL] Gagal acquire file lock: {e}")

    def _release_file_lock(self, fp) -> None:
        """✅ Lepas kunci file di level OS"""
        if not HAS_FCNTL:
            return
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
        except Exception as e:
            logging.warning(f"⚠️ [GLOBAL] Gagal release file lock: {e}")

    def _get_all_sent_files(self) -> List[Path]:
        """Ambil semua file sent, diurutkan dari lama ke baru"""
        try:
            files = sorted(
                self.sent_dir.glob(f"{self.prefix}_*.json"),
                key=lambda f: int(f.stem.split("_")[-1])
            )
            return files
        except Exception as e:
            logging.error(f"❌ [GLOBAL] Gagal list sent files: {e}")
            return []

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

    def _has_new_files(self) -> bool:
        """✅ Cek apakah ada file baru (dengan cache)"""
        try:
            current_files = self._get_all_sent_files()
            
            if len(current_files) != len(self._known_files):
                return True
            
            current_set = set(current_files)
            known_set = set(self._known_files)
            
            if current_set != known_set:
                return True
            
            return False
        except Exception as e:
            logging.warning(f"⚠️ Error di _has_new_files: {e}")
            return False

    def _reload_cache_sync(self, force: bool = False) -> None:
        """Reload cache dari SEMUA file (SYNCHRONOUS)"""
        try:
            files = self._get_all_sent_files()
            total_files = len(files)

            if total_files == 0:
                self._last_reload = time.time()
                return

            self._cache.clear()
            total_loaded = 0

            for f in files:
                try:
                    with open(f, "r", encoding="utf-8") as fp:
                        self._acquire_file_lock(fp)
                        try:
                            fp.seek(0)
                            data = json.load(fp)
                            if isinstance(data, list):
                                self._cache.update(data)
                                total_loaded += len(data)
                        finally:
                            self._release_file_lock(fp)
                except json.JSONDecodeError as e:
                    logging.error(f"❌ [GLOBAL] JSON corrupt {f.name}: {e}")
                except FileNotFoundError:
                    pass
                except Exception as e:
                    logging.error(f"❌ [GLOBAL] Gagal reload {f.name}: {e}")

            self._known_files = files
            self._last_reload = time.time()

            if total_loaded > 0:
                logging.info(
                    f"✅ [GLOBAL] Cache reloaded: {total_loaded} entries "
                    f"dari {total_files} file"
                )

        except Exception as e:
            logging.error(f"❌ [GLOBAL] Gagal reload cache sync: {e}")

    async def _reload_cache_async_safe(self) -> None:
        """✅ Reload cache secara async-safe menggunakan executor"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._reload_cache_sync)

    async def load_all(self) -> None:
        """Load semua file sent ke cache dengan recovery"""
        self._cache.clear()
        files = self._get_all_sent_files()

        if not files:
            logging.info("📂 [GLOBAL] Tidak ada file sent tersimpan")
            await self._init_new_file()
            return

        total = 0
        corrupted = []
        
        for f in files:
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    self._acquire_file_lock(fp)
                    try:
                        fp.seek(0)
                        data = json.load(fp)
                        if isinstance(data, list):
                            self._cache.update(data)
                            total += len(data)
                            logging.info(
                                f"📂 [GLOBAL] Load: {f.name} "
                                f"({len(data)} entry)"
                            )
                        else:
                            logging.warning(
                                f"⚠️ [GLOBAL] {f.name} bukan list, skip"
                            )
                    finally:
                        self._release_file_lock(fp)
            
            except json.JSONDecodeError as e:
                logging.error(f"❌ [GLOBAL] JSON corrupt {f.name}: {e}")
                corrupted.append(f)
                
                # ✅ Try backup
                backup = f.with_suffix(".json.backup")
                if backup.exists():
                    try:
                        with open(backup, "r", encoding="utf-8") as fp:
                            self._acquire_file_lock(fp)
                            try:
                                data = json.load(fp)
                                if isinstance(data, list):
                                    self._cache.update(data)
                                    total += len(data)
                                    logging.info(
                                        f"✅ [GLOBAL] Recovered dari backup: "
                                        f"{f.name} ({len(data)} entry)"
                                    )
                                    f.write_bytes(backup.read_bytes())
                            finally:
                                self._release_file_lock(fp)
                    except Exception as e2:
                        logging.error(
                            f"❌ [GLOBAL] Gagal recover dari backup: {e2}"
                        )
            
            except Exception as e:
                logging.error(f"❌ [GLOBAL] Gagal load {f.name}: {e}")
                corrupted.append(f)

        if corrupted:
            logging.warning(
                f"⚠️ [GLOBAL] {len(corrupted)} file corrupt: "
                f"{[f.name for f in corrupted]}"
            )

        last_file = files[-1] if files else None
        if last_file:
            try:
                with open(last_file, "r", encoding="utf-8") as fp:
                    self._acquire_file_lock(fp)
                    try:
                        data = json.load(fp)
                        self._current_count = (
                            len(data) if isinstance(data, list) else 0
                        )
                    finally:
                        self._release_file_lock(fp)
            except Exception:
                self._current_count = 0

        self._current_file = last_file
        self._known_files  = files
        self._last_reload  = time.time()

        if self._current_count >= self.max_per_file:
            await self._init_new_file()

        logging.info(
            f"✅ [GLOBAL] Startup load selesai: {total} entries "
            f"dari {len(files)} file"
        )

    async def _init_new_file(self) -> None:
        """✅ Buat file sent baru dengan safety check"""
        idx = self._get_next_file_index()
        self._current_file  = self.sent_dir / f"{self.prefix}_{idx:04d}.json"
        self._current_count = 0

        try:
            # ✅ Check file sudah ada
            if self._current_file.exists():
                logging.warning(
                    f"⚠️ [GLOBAL] File sudah ada: {self._current_file.name}, "
                    f"load existing data"
                )
                try:
                    with open(self._current_file, "r", encoding="utf-8") as fp:
                        self._acquire_file_lock(fp)
                        try:
                            data = json.load(fp)
                            self._current_count = (
                                len(data) if isinstance(data, list) else 0
                            )
                        finally:
                            self._release_file_lock(fp)
                except Exception as e:
                    logging.error(f"❌ Gagal load existing file: {e}")
                    idx += 1
                    self._current_file = (
                        self.sent_dir / f"{self.prefix}_{idx:04d}.json"
                    )
                
                if self._current_count < self.max_per_file:
                    return
            
            # ✅ Create new file
            with open(self._current_file, "w", encoding="utf-8") as fp:
                self._acquire_file_lock(fp)
                try:
                    json.dump([], fp)
                finally:
                    self._release_file_lock(fp)
            
            logging.info(
                f"📄 [GLOBAL] File baru dibuat: {self._current_file.name}"
            )
        except Exception as e:
            logging.error(f"❌ [GLOBAL] Gagal buat file: {e}")

    async def add(self, key: str) -> None:
        """✅ Tambahkan key ke file sent (dengan double-check dan atomic write)"""
        # ✅ Langsung masuk lock, jangan check dulu di luar
        async with self._async_lock:
            # Check di dalam lock
            if key in self._cache:
                return
            
            await self._reload_cache_async_safe()
            
            if key in self._cache:
                return

            if (
                self._current_file is None or
                self._current_count >= self.max_per_file
            ):
                await self._init_new_file()

            try:
                with open(self._current_file, "r+", encoding="utf-8") as fp:
                    self._acquire_file_lock(fp)
                    try:
                        fp.seek(0)
                        data = json.load(fp)

                        if key not in data:
                            data.append(key)
                            self._current_count = len(data)

                            fp.seek(0)
                            fp.truncate()
                            json.dump(data, fp)
                            fp.flush()
                            os.fsync(fp.fileno())

                        self._cache.add(key)

                        if self._current_count >= self.max_per_file:
                            logging.info(
                                f"📄 [GLOBAL] File penuh: "
                                f"{self._current_file.name} "
                                f"({self._current_count} entry)"
                            )
                    finally:
                        self._release_file_lock(fp)

            except Exception as e:
                logging.error(f"❌ [GLOBAL] Gagal add key: {e}")
                self._cache.add(key)

    def check(self, key: str) -> bool:
        """Cek apakah key sudah ada (thread-safe untuk read)"""
        if key in self._cache:
            return True

        now = time.time()
        cache_stale = (now - self._last_reload) > self.reload_interval
        has_new = self._has_new_files()

        if cache_stale or has_new:
            try:
                self._reload_cache_sync()
            except Exception as e:
                logging.error(f"❌ Error reload di check(): {e}")
        
        return key in self._cache

    async def cleanup_old_files(self, keep_last_n: int = 15) -> None:
        """Hapus file lama, simpan N file terbaru"""
        files = self._get_all_sent_files()
        total = len(files)

        if total <= keep_last_n:
            return

        to_delete = files[:-keep_last_n]
        deleted   = 0

        for f in to_delete:
            if f == self._current_file:
                continue
            try:
                f.unlink()
                deleted += 1
                logging.info(f"🗑️ [GLOBAL] File lama dihapus: {f.name}")
            except Exception as e:
                logging.error(f"❌ [GLOBAL] Gagal hapus {f.name}: {e}")

        if deleted > 0:
            self._reload_cache_sync(force=True)
            logging.info(
                f"✅ [GLOBAL] Cleanup selesai: {deleted} file dihapus"
            )

    def get_info(self) -> dict:
        """Dapatkan info lengkap untuk dashboard/status"""
        files = self._get_all_sent_files()
        now   = time.time()
        return {
            "total_entries":    len(self._cache),
            "total_files":      len(files),
            "current_file":     self._current_file.name if self._current_file else "N/A",
            "current_count":    self._current_count,
            "max_per_file":     self.max_per_file,
            "last_reload":      datetime.fromtimestamp(
                                    self._last_reload
                                ).strftime("%H:%M:%S") if self._last_reload else "N/A",
            "reload_age":       f"{now - self._last_reload:.0f}s" if self._last_reload else "N/A",
            "reload_interval":  f"{self.reload_interval}s",
            "known_files":      len(self._known_files),
        }


global_sent_manager = GlobalSentFileManager(
    sent_dir=GLOBAL_SENT_DIR,
    prefix=SENT_FILE_PREFIX,
    max_per_file=MAX_SENT_PER_FILE,
    reload_interval=10,
)

# ============================================================
# === LOCAL STATE MANAGER ===
# ============================================================
class LocalStateManager:
    """Mengelola state lokal (pending, daily, flood) — per bot"""

    def __init__(self, state_dir: Path):
        self.state_dir = state_dir

    async def save_pending(self, pending: list) -> None:
        try:
            with open(FILE_PENDING, "w", encoding="utf-8") as fp:
                json.dump(pending, fp)
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal save pending: {e}")

    async def load_pending(self) -> list:
        """✅ Load pending dengan validasi format"""
        try:
            if not FILE_PENDING.exists():
                return []
            
            with open(FILE_PENDING, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                
                if not isinstance(data, list):
                    logging.warning(f"⚠️ Pending file bukan list, skip")
                    return []
                
                # ✅ Validate setiap item
                valid_items = []
                for i, item in enumerate(data):
                    if isinstance(item, (list, tuple)) and len(item) >= 2:
                        try:
                            file_id, media_type = item, item
                            if isinstance(file_id, str) and isinstance(media_type, str):
                                valid_items.append(item)
                            else:
                                logging.warning(
                                    f"⚠️ Item {i} format invalid, skip"
                                )
                        except (ValueError, TypeError):
                            logging.warning(f"⚠️ Item {i} error, skip")
                    else:
                        logging.warning(f"⚠️ Item {i} format invalid, skip")
                
                if len(valid_items) < len(data):
                    logging.warning(
                        f"⚠️ {len(data) - len(valid_items)}/{len(data)} "
                        f"item pending invalid"
                    )
                
                return valid_items
        
        except json.JSONDecodeError as e:
            logging.error(f"❌ Pending file JSON corrupt: {e}")
            return []
        except Exception as e:
            logging.error(f"❌ Gagal load pending: {e}")
            return []

    async def save_daily(self, count: int, reset_date: datetime.date) -> None:
        try:
            data = {
                "count":      count,
                "reset_date": reset_date.isoformat()
            }
            with open(FILE_DAILY, "w", encoding="utf-8") as fp:
                json.dump(data, fp)
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal save daily: {e}")

    async def load_daily(self) -> Tuple[int, datetime.date]:
        try:
            if not FILE_DAILY.exists():
                return 0, datetime.now(timezone.utc).date()
            with open(FILE_DAILY, "r", encoding="utf-8") as fp:
                data       = json.load(fp)
                count      = int(data.get("count", 0))
                date_str   = data.get("reset_date", "")
                try:
                    reset_date = datetime.fromisoformat(date_str).date()
                except (ValueError, TypeError):
                    reset_date = datetime.now(timezone.utc).date()
                return count, reset_date
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal load daily: {e}")
            return 0, datetime.now(timezone.utc).date()

    async def save_flood(self, flood_data: dict) -> None:
        try:
            with open(FILE_FLOOD, "w", encoding="utf-8") as fp:
                json.dump(flood_data, fp)
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal save flood: {e}")

    async def load_flood(self) -> dict:
        try:
            if not FILE_FLOOD.exists():
                return {}
            with open(FILE_FLOOD, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                return data if isinstance(data, dict) else {}
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal load flood: {e}")
            return {}

    async def save_config(self, config: dict) -> None:
        try:
            with open(FILE_CONFIG, "w", encoding="utf-8") as fp:
                json.dump(config, fp)
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal save config: {e}")

    async def load_config(self) -> dict:
        try:
            if not FILE_CONFIG.exists():
                return {}
            with open(FILE_CONFIG, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                return data if isinstance(data, dict) else {}
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal load config: {e}")
            return {}

    async def save_ratelimit(self, ratelimit: dict) -> None:
        try:
            with open(FILE_RATELIMIT, "w", encoding="utf-8") as fp:
                json.dump(ratelimit, fp)
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal save ratelimit: {e}")

    async def load_ratelimit(self) -> dict:
        try:
            if not FILE_RATELIMIT.exists():
                return {}
            with open(FILE_RATELIMIT, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                return data if isinstance(data, dict) else {}
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal load ratelimit: {e}")
            return {}

state_manager = LocalStateManager(STATE_DIR)

# ============================================================
# === ENHANCED DUPLICATE DETECTION ===
# ============================================================
class EnhancedDuplicateChecker:
    """Sistem anti-duplikat yang lebih aman dengan triple-check"""
    
    def __init__(self, sent_manager: GlobalSentFileManager):
        self.sent_manager = sent_manager
        self._check_lock = asyncio.Lock()
    
    async def is_duplicate_safe(
        self,
        file_id: str,
        fp_hash: str,
        timeout: float = 10.0
    ) -> bool:
        """✅ Triple-check untuk duplikat dengan timeout"""
        async with self._check_lock:
            # CHECK 1: Cache (cepat)
            if self.sent_manager.check(file_id):
                return True
            if self.sent_manager.check(fp_hash):
                return True
            
            # CHECK 2: Force reload dengan timeout
            try:
                loop = asyncio.get_event_loop()
                await asyncio.wait_for(
                    loop.run_in_executor(
                        None,
                        self.sent_manager._reload_cache_sync,
                        True
                    ),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logging.warning(
                    f"⚠️ Duplicate check timeout, skip reload"
                )
                if self.sent_manager.check(file_id):
                    return True
                if self.sent_manager.check(fp_hash):
                    return True
            except Exception as e:
                logging.error(f"❌ Error di reload cache: {e}")
            
            if self.sent_manager.check(file_id):
                return True
            if self.sent_manager.check(fp_hash):
                return True
            
            # CHECK 3: Direct file check dengan timeout
            try:
                result = await asyncio.wait_for(
                    self._check_files_directly(file_id, fp_hash),
                    timeout=timeout
                )
                return result
            except asyncio.TimeoutError:
                logging.warning(
                    f"⚠️ Direct file check timeout, assume not duplicate"
                )
                return False
            except Exception as e:
                logging.error(f"❌ Error di _check_files_directly: {e}")
                return False
    
    async def _check_files_directly(self, file_id: str, fp_hash: str) -> bool:
        """Cek langsung di file tanpa cache"""
        try:
            files = self.sent_manager._get_all_sent_files()
            for f in files:
                try:
                    with open(f, "r", encoding="utf-8") as fp:
                        self.sent_manager._acquire_file_lock(fp)
                        try:
                            data = json.load(fp)
                            if isinstance(data, list):
                                if file_id in data or fp_hash in data:
                                    return True
                        finally:
                            self.sent_manager._release_file_lock(fp)
                except Exception as e:
                    logging.warning(f"⚠️ Gagal check file {f.name}: {e}")
            return False
        except Exception as e:
            logging.error(f"❌ Error di _check_files_directly: {e}")
            return False
    
    async def mark_sent_safe(self, file_id: str, fp_hash: str) -> bool:
        """Mark sent dengan verification"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                await self.sent_manager.add(file_id)
                await self.sent_manager.add(fp_hash)
                
                await asyncio.sleep(0.1)
                if await self._check_files_directly(file_id, fp_hash):
                    return True
                else:
                    logging.warning(
                        f"⚠️ [VERIFY] Mark sent gagal diverifikasi, retry {attempt+1}/{max_retries}"
                    )
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logging.error(
                    f"❌ [MARK_SENT] Error attempt {attempt+1}/{max_retries}: {e}"
                )
                await asyncio.sleep(1)
        
        logging.error(f"❌ [MARK_SENT] Gagal setelah {max_retries} percobaan")
        return False

duplicate_checker: EnhancedDuplicateChecker | None = None

# ============================================================
# === QUEUE DEDUPLICATION ===
# ============================================================
class QueueDeduplicator:
    """Mencegah duplikat di dalam queue sendiri"""
    
    def __init__(self, max_size: int = 10000):
        self._queue_items: Dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._max_size = max_size
    
    async def add_to_queue(
        self,
        file_id: str,
        fp_hash: str,
        item: tuple
    ) -> bool:
        """✅ Tambah item ke queue dengan cek duplikat"""
        async with self._lock:
            composite_key = f"{file_id}:{fp_hash}"
            
            if composite_key in self._queue_items:
                logging.info(
                    f"⏭️ [QUEUE] Duplikat di queue terdeteksi: {file_id[:8]}... "
                    f"(sudah ada {self._queue_items[composite_key]}x)"
                )
                return False
            
            # ✅ Cek ukuran maksimum
            if len(self._queue_items) >= self._max_size:
                logging.warning(
                    f"⚠️ [QUEUE] Deduplicator penuh ({self._max_size}), "
                    f"skip tracking"
                )
                try:
                    await pending_queue.put(item)
                    return True
                except asyncio.QueueFull:
                    return False
            
            try:
                await pending_queue.put(item)
                self._queue_items[composite_key] = 1
                return True
            except asyncio.QueueFull:
                logging.warning(f"⚠️ [QUEUE] Queue penuh, drop item")
                return False
    
    async def remove_from_queue(
        self,
        file_id: str,
        fp_hash: str
    ) -> None:
        """Hapus item dari tracking saat di-process"""
        async with self._lock:
            composite_key = f"{file_id}:{fp_hash}"
            if composite_key in self._queue_items:
                del self._queue_items[composite_key]
    
    def get_queue_size(self) -> int:
        """Dapatkan jumlah unique items di queue"""
        return len(self._queue_items)
    
    async def clear_on_startup(self) -> None:
        """Clear tracking saat startup"""
        async with self._lock:
            self._queue_items.clear()
            logging.info("🔄 [QUEUE] Deduplicator cleared on startup")
    
    async def cleanup_stale(self, max_age_seconds: int = 3600) -> None:
        """✅ Bersihkan item yang sudah lama"""
        async with self._lock:
            if len(self._queue_items) > self._max_size * 0.9:
                old_size = len(self._queue_items)
                self._queue_items.clear()
                logging.warning(
                    f"⚠️ [QUEUE] Deduplicator dibersihkan: {old_size} items"
                )

queue_deduplicator: QueueDeduplicator | None = None

# ============================================================
# === DAILY COUNTER (LOCAL) ===
# ============================================================
async def check_daily_reset() -> None:
    """Cek apakah perlu reset counter harian"""
    global daily_count, daily_reset_date
    today = datetime.now(timezone.utc).date()
    if today != daily_reset_date:
        logging.info(
            f"🔄 [LOCAL] Reset harian | Kemarin terkirim: {daily_count}"
        )
        daily_count      = 0
        daily_reset_date = today
        await save_daily()

async def save_daily() -> None:
    """Simpan counter harian ke file lokal"""
    await state_manager.save_daily(daily_count, daily_reset_date)

# ============================================================
# === SMART FLOOD CONTROLLER (LOCAL) ===
# ============================================================
class SmartFloodController:
    """Flood controller per bot (LOCAL)"""

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
            except (ValueError, TypeError):
                return None
        return None

    async def record_flood(self, suggested_wait: int) -> float:
        """Catat flood event"""
        now = datetime.now(timezone.utc)

        if self.last_flood_time is not None:
            elapsed = (now - self.last_flood_time).total_seconds()
            if elapsed > FLOOD_RESET_AFTER:
                logging.info(
                    f"🔄 [LOCAL] Flood counter direset "
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

        self.group_delay_min = min(
            120.0,
            DELAY_BETWEEN_GROUP_MIN + (self.flood_count * 5)
        )
        self.group_delay_max = min(
            180.0,
            DELAY_BETWEEN_GROUP_MAX + (self.flood_count * 10)
        )

        logging.warning(
            f"🚨 [LOCAL] FLOOD #{self.flood_count} terdeteksi!\n"
            f"   ├─ Saran Telegram   : {suggested_wait}s\n"
            f"   ├─ Random tambahan  : {random_add:.1f}s\n"
            f"   ├─ Penalti kumulatif: {self.penalty:.0f}s\n"
            f"   ├─ Total tunggu     : {total_wait:.1f}s\n"
            f"   └─ Delay group baru : {self.group_delay_min:.0f}s - {self.group_delay_max:.0f}s"
        )

        await self.save_state()
        return total_wait

    async def record_success(self) -> None:
        """✅ Catat pengiriman sukses dengan penalty decay"""
        # ✅ Decay penalty lebih cepat
        if self.penalty > 0:
            decay_rate = 10.0
            self.penalty = max(0.0, self.penalty - decay_rate)
        
        # ✅ Decay flood count
        if self.flood_count > 0:
            self.flood_count = max(0, self.flood_count - 1)
            if self.flood_count == 0:
                self.last_flood_time = None
                self.penalty = 0.0
                logging.info(f"✅ [LOCAL] Flood status normal kembali")
        
        # ✅ Decay group delay
        if self.group_delay_min > DELAY_BETWEEN_GROUP_MIN:
            self.group_delay_min = max(
                float(DELAY_BETWEEN_GROUP_MIN),
                self.group_delay_min - 5.0
            )
        if self.group_delay_max > DELAY_BETWEEN_GROUP_MAX:
            self.group_delay_max = max(
                float(DELAY_BETWEEN_GROUP_MAX),
                self.group_delay_max - 8.0
            )
        
        self.is_cooling = False
        await self.save_state()

    def get_group_delay(self) -> float:
        """Dapatkan delay antar group"""
        return random.uniform(self.group_delay_min, self.group_delay_max)

    def get_status(self) -> str:
        """Dapatkan status flood controller"""
        return (
            f"FloodCtrl | Count: {self.flood_count} | "
            f"Total: {self.total_flood} | "
            f"Penalty: {self.penalty:.0f}s | "
            f"Cooling: {self.is_cooling}"
        )

    def to_dict(self) -> dict:
        """Convert ke dict untuk disimpan"""
        return {
            "flood_count":     self.flood_count,
            "total_flood":     self.total_flood,
            "last_flood_time": (
                self.last_flood_time.isoformat()
                if self.last_flood_time else None
            ),
            "penalty":         self.penalty,
            "is_cooling":      self.is_cooling,
            "group_delay_min": self.group_delay_min,
            "group_delay_max": self.group_delay_max,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SmartFloodController":
        """Create dari dict"""
        obj = cls()
        obj.flood_count     = int(data.get("flood_count", 0))
        obj.total_flood     = int(data.get("total_flood", 0))
        obj.last_flood_time = cls._parse_last_flood_time(
            data.get("last_flood_time")
        )
        obj.penalty         = float(data.get("penalty", 0.0))
        obj.is_cooling      = bool(data.get("is_cooling", False))
        obj.group_delay_min = float(
            data.get("group_delay_min", DELAY_BETWEEN_GROUP_MIN)
        )
        obj.group_delay_max = float(
            data.get("group_delay_max", DELAY_BETWEEN_GROUP_MAX)
        )
        return obj

    async def save_state(self) -> None:
        """Simpan state ke file lokal"""
        try:
            await state_manager.save_flood(self.to_dict())
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal save flood state: {e}")

# ============================================================
# === PERSIST: SAVE & LOAD ALL ===
# ============================================================
async def save_all() -> None:
    """✅ Simpan semua state dengan atomic semantics"""
    global last_save_time

    try:
        # ✅ Collect semua data dulu
        pending_snapshot = get_queue_snapshot(pending_queue) if pending_queue else []
        
        daily_data = {
            "count": daily_count,
            "reset_date": daily_reset_date.isoformat()
        }
        
        flood_data = flood_ctrl.to_dict() if flood_ctrl else {}
        
        config_data = {
            "daily_limit": DAILY_LIMIT,
            "send_delay": DELAY_BETWEEN_SEND
        }
        
        ratelimit_data = {str(k): v for k, v in user_ratelimit.items()}
        
        # ✅ Save semua dengan error handling per item
        errors = []
        
        try:
            await state_manager.save_pending(pending_snapshot)
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal save pending: {e}")
            errors.append(("pending", e))
        
        try:
            await state_manager.save_daily(daily_count, daily_reset_date)
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal save daily: {e}")
            errors.append(("daily", e))
        
        try:
            await state_manager.save_flood(flood_data)
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal save flood: {e}")
            errors.append(("flood", e))
        
        try:
            await state_manager.save_config(config_data)
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal save config: {e}")
            errors.append(("config", e))
        
        try:
            await state_manager.save_ratelimit(ratelimit_data)
        except Exception as e:
            logging.error(f"❌ [LOCAL] Gagal save ratelimit: {e}")
            errors.append(("ratelimit", e))
        
        # ✅ Report hasil
        if errors:
            failed = [name for name, _ in errors]
            logging.warning(
                f"⚠️ [LOCAL] Save_all partial success, failed: {failed}"
            )
        else:
            logging.debug(f"✅ [LOCAL] Save_all selesai")
        
        last_save_time = datetime.now(timezone.utc)
    
    except Exception as e:
        logging.error(f"❌ [LOCAL] Unexpected error di save_all: {e}")


async def load_local_state() -> None:
    """Load LOCAL state SAJA (pending, daily, flood, config, ratelimit)"""
    global daily_count, daily_reset_date, DAILY_LIMIT, \
           DELAY_BETWEEN_SEND, flood_ctrl, user_ratelimit

    # Load LOCAL pending
    try:
        if FILE_PENDING.exists():
            backup = FILE_PENDING.with_suffix(".json.backup")
            try:
                backup.write_bytes(FILE_PENDING.read_bytes())
                logging.info(f"💾 [LOCAL] Backup pending dibuat")
            except Exception as e:
                logging.warning(f"⚠️ [LOCAL] Gagal buat backup pending: {e}")

        loaded = await state_manager.load_pending()
        
        valid_count = 0
        for item in loaded:
            try:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    if not pending_queue.full():
                        await pending_queue.put(item)
                        valid_count += 1
            except Exception as e:
                logging.warning(f"⚠️ [LOCAL] Skip item pending korup: {e}")
        
        logging.info(
            f"📥 [LOCAL] Pending dimuat: {valid_count}/{len(loaded)} item "
            f"(Queue: {pending_queue.qsize()})"
        )
        
        if valid_count < len(loaded):
            logging.warning(
                f"⚠️ [LOCAL] {len(loaded) - valid_count} item pending korup/hilang"
            )
            
    except Exception as e:
        logging.error(f"❌ [LOCAL] Gagal load pending: {e}")
        backup = FILE_PENDING.with_suffix(".json.backup")
        if backup.exists():
            try:
                FILE_PENDING.write_bytes(backup.read_bytes())
                loaded = await state_manager.load_pending()
                for item in loaded:
                    if not pending_queue.full():
                        await pending_queue.put(item)
                logging.info(
                    f"✅ [LOCAL] Pending dipulihkan dari backup: "
                    f"{pending_queue.qsize()} item"
                )
                backup.unlink()
            except Exception as e2:
                logging.error(
                    f"❌ [LOCAL] Gagal pulihkan pending dari backup: {e2}"
                )

    # Load LOCAL daily
    try:
        daily_count, daily_reset_date = await state_manager.load_daily()
        logging.info(f"📥 [LOCAL] Daily dimuat: {daily_count}/{DAILY_LIMIT}")
    except Exception as e:
        logging.warning(f"⚠️ [LOCAL] Gagal load daily: {e}")

    # Load LOCAL flood
    try:
        flood_data = await state_manager.load_flood()
        if flood_data:
            flood_ctrl = SmartFloodController.from_dict(flood_data)
            logging.info(
                f"📥 [LOCAL] Flood ctrl dimuat: {flood_ctrl.get_status()}"
            )
    except Exception as e:
        logging.warning(f"⚠️ [LOCAL] Gagal load flood ctrl: {e}")

    # Load LOCAL config
    try:
        config = await state_manager.load_config()
        if "daily_limit" in config:
            DAILY_LIMIT = int(config["daily_limit"])
            logging.info(f"📥 [LOCAL] Daily limit dimuat: {DAILY_LIMIT}")
        if "send_delay" in config:
            DELAY_BETWEEN_SEND = float(config["send_delay"])
            logging.info(
                f"📥 [LOCAL] Send delay dimuat: {DELAY_BETWEEN_SEND}s"
            )
    except Exception as e:
        logging.warning(f"⚠️ [LOCAL] Gagal load config: {e}")

    # Load LOCAL ratelimit
    try:
        raw_rl = await state_manager.load_ratelimit()
        user_ratelimit = {int(k): v for k, v in raw_rl.items()}
        logging.info(
            f"📥 [LOCAL] Ratelimit dimuat: {len(user_ratelimit)} user"
        )
    except Exception as e:
        logging.warning(f"⚠️ [LOCAL] Gagal load ratelimit: {e}")

    logging.info(
        f"📊 [LOAD] LOCAL state dimuat | Daily: {daily_count}/{DAILY_LIMIT} | "
        f"Pending: {pending_queue.qsize()}"
    )

# ============================================================
# === FINGERPRINT ===
# ============================================================
def get_fingerprint(msg) -> dict:
    """✅ Ambil fingerprint dari message dengan support SEMUA media type"""
    fp = {
        "file_id":    None,
        "media_type": None,
        "file_size":  None,
        "duration":   None,
        "width":      None,
        "height":     None,
        "file_name":  None,
        "mime_type":  None,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }
    
    try:
        # VIDEO
        if msg.video:
            v = msg.video
            fp.update({
                "file_id":    v.file_id,
                "media_type": "video",
                "file_size":  v.file_size,
                "duration":   v.duration,
                "width":      v.width,
                "height":     v.height,
                "mime_type":  v.mime_type,
            })
            return fp
        
        # DOCUMENT
        if msg.document:
            d = msg.document
            fp.update({
                "file_id":    d.file_id,
                "media_type": "document",
                "file_size":  d.file_size,
                "file_name":  d.file_name,
                "mime_type":  d.mime_type,
            })
            return fp
        
        # PHOTO
        if msg.photo and len(msg.photo) > 0:
            p = msg.photo[-1]
            fp.update({
                "file_id":    p.file_id,
                "media_type": "photo",
                "file_size":  p.file_size,
                "width":      p.width,
                "height":     p.height,
            })
            return fp
        
        # AUDIO
        if msg.audio:
            a = msg.audio
            fp.update({
                "file_id":    a.file_id,
                "media_type": "audio",
                "file_size":  a.file_size,
                "duration":   a.duration,
                "file_name":  a.file_name,
                "mime_type":  a.mime_type,
            })
            return fp
        
        # ANIMATION
        if msg.animation:
            anim = msg.animation
            fp.update({
                "file_id":    anim.file_id,
                "media_type": "animation",
                "file_size":  anim.file_size,
                "duration":   anim.duration,
                "width":      anim.width,
                "height":     anim.height,
                "file_name":  anim.file_name,
                "mime_type":  anim.mime_type,
            })
            return fp
        
        # VOICE
        if msg.voice:
            v = msg.voice
            fp.update({
                "file_id":    v.file_id,
                "media_type": "voice",
                "file_size":  v.file_size,
                "duration":   v.duration,
                "mime_type":  v.mime_type,
            })
            return fp
        
        # VIDEO NOTE
        if msg.video_note:
            vn = msg.video_note
            fp.update({
                "file_id":    vn.file_id,
                "media_type": "video_note",
                "file_size":  vn.file_size,
                "duration":   vn.duration,
                "width":      vn.width,
                "height":     vn.height,
            })
            return fp
        
        # STICKER
        if msg.sticker:
            s = msg.sticker
            fp.update({
                "file_id":    s.file_id,
                "media_type": "sticker",
                "file_size":  s.file_size,
                "width":      s.width,
                "height":     s.height,
                "mime_type":  s.mime_type,
            })
            return fp
    
    except Exception as e:
        logging.error(f"❌ Error saat get_fingerprint: {e}")
    
    return fp

# ============================================================
# === ADMIN HELPERS ===
# ============================================================
def get_role(update: Update) -> str | None:
    """Dapatkan role admin"""
    uid = update.effective_user.id if update.effective_user else None
    return ADMINS.get(uid)

def is_superadmin(update: Update) -> bool:
    """Cek apakah superadmin"""
    return get_role(update) == "superadmin"

def is_moderator(update: Update) -> bool:
    """Cek apakah moderator atau superadmin"""
    return get_role(update) in ("superadmin", "moderator")

async def rate_limit_check(user_id: int, interval: int = 60) -> bool:
    """✅ Rate limit per user (forward media) dengan thread-safety"""
    global user_ratelimit
    
    async with ratelimit_lock:
        now = time.time()
        
        if user_id in user_ratelimit:
            if now - user_ratelimit[user_id] < interval:
                return False
        
        user_ratelimit[user_id] = now
        return True

async def admin_rate_limit_check(user_id: int, interval: int = 5) -> bool:
    """✅ Rate limit per admin (admin commands) dengan thread-safety"""
    global admin_ratelimit
    
    async with ratelimit_lock:
        now = time.time()
        
        if user_id in admin_ratelimit:
            if now - admin_ratelimit[user_id] < interval:
                return False
        
        admin_ratelimit[user_id] = now
        return True

def get_target_chat_id() -> int:
    """✅ Pilih target chat (multi-target support) dengan validation"""
    valid_targets = []
    
    if TARGET_CHAT_ID != 0:
        valid_targets.append(TARGET_CHAT_ID)
    
    for cid in TARGET_CHAT_IDS:
        if cid != 0 and cid not in valid_targets:
            valid_targets.append(cid)
    
    if not valid_targets:
        logging.error(
            f"❌ [BOT: {BOT_NAME}] Tidak ada target chat ID yang valid!"
        )
        raise ValueError("No valid target chat ID configured")
    
    return random.choice(valid_targets)

# ============================================================
# === SEND MEDIA WITH RETRY (UNIVERSAL - ANTI CRASH) ===
# ============================================================
async def send_media_with_retry(
    bot,
    chat_id: int,
    media_items: List[Tuple[str, str]],
    max_retries: int = MAX_RETRIES,
) -> bool:
    """✅ Kirim media universal dengan retry, partial success, dan error handling"""
    
    if not media_items:
        return False
    
    # Kelompokkan media berdasarkan tipe
    media_groups = {
        "video": [],
        "photo": [],
        "document": [],
        "audio": [],
        "animation": [],
        "voice": [],
        "video_note": [],
        "sticker": [],
    }
    
    for file_id, media_type in media_items:
        if media_type in media_groups:
            media_groups[media_type].append(file_id)
    
    attempt = 0
    partial_success = False
    
    while attempt < max_retries:
        try:
            # 1. KIRIM VIDEO (GROUP)
            if media_groups["video"]:
                try:
                    media_group = [
                        InputMediaVideo(media=fid)
                        for fid in media_groups["video"]
                    ]
                    await asyncio.wait_for(
                        bot.send_media_group(
                            chat_id=chat_id,
                            media=media_group
                        ),
                        timeout=60,
                    )
                    logging.info(
                        f"✅ Terkirim {len(media_groups['video'])} video"
                    )
                    partial_success = True
                    media_groups["video"] = []
                except Exception as e:
                    logging.error(f"❌ Error kirim video: {e}")
            
            # 2. KIRIM PHOTO (GROUP)
            if media_groups["photo"]:
                try:
                    media_group = [
                        InputMediaPhoto(media=fid)
                        for fid in media_groups["photo"]
                    ]
                    await asyncio.wait_for(
                        bot.send_media_group(
                            chat_id=chat_id,
                            media=media_group
                        ),
                        timeout=60,
                    )
                    logging.info(
                        f"✅ Terkirim {len(media_groups['photo'])} photo"
                    )
                    partial_success = True
                    media_groups["photo"] = []
                except Exception as e:
                    logging.error(f"❌ Error kirim photo: {e}")
            
            # 3. KIRIM DOCUMENT (INDIVIDUAL)
            if media_groups["document"]:
                sent_count = 0
                for fid in media_groups["document"]:
                    try:
                        await asyncio.wait_for(
                            bot.send_document(chat_id=chat_id, document=fid),
                            timeout=60,
                        )
                        sent_count += 1
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logging.warning(
                            f"⚠️ Gagal kirim document {fid[:8]}...: {e}"
                        )
                        continue
                
                if sent_count > 0:
                    logging.info(f"✅ Terkirim {sent_count} document")
                    partial_success = True
                    media_groups["document"] = []
            
            # 4. KIRIM AUDIO (INDIVIDUAL)
            if media_groups["audio"]:
                sent_count = 0
                for fid in media_groups["audio"]:
                    try:
                        await asyncio.wait_for(
                            bot.send_audio(chat_id=chat_id, audio=fid),
                            timeout=60,
                        )
                        sent_count += 1
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logging.warning(
                            f"⚠️ Gagal kirim audio {fid[:8]}...: {e}"
                        )
                        continue
                
                if sent_count > 0:
                    logging.info(f"✅ Terkirim {sent_count} audio")
                    partial_success = True
                    media_groups["audio"] = []
            
            # 5. KIRIM ANIMATION (INDIVIDUAL)
            if media_groups["animation"]:
                sent_count = 0
                for fid in media_groups["animation"]:
                    try:
                        await asyncio.wait_for(
                            bot.send_animation(chat_id=chat_id, animation=fid),
                            timeout=60,
                        )
                        sent_count += 1
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logging.warning(
                            f"⚠️ Gagal kirim animation {fid[:8]}...: {e}"
                        )
                        continue
                
                if sent_count > 0:
                    logging.info(f"✅ Terkirim {sent_count} animation")
                    partial_success = True
                    media_groups["animation"] = []
            
            # 6. KIRIM VOICE (INDIVIDUAL)
            if media_groups["voice"]:
                sent_count = 0
                for fid in media_groups["voice"]:
                    try:
                        await asyncio.wait_for(
                            bot.send_voice(chat_id=chat_id, voice=fid),
                            timeout=60,
                        )
                        sent_count += 1
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logging.warning(
                            f"⚠️ Gagal kirim voice {fid[:8]}...: {e}"
                        )
                        continue
                
                if sent_count > 0:
                    logging.info(f"✅ Terkirim {sent_count} voice")
                    partial_success = True
                    media_groups["voice"] = []
            
            # 7. KIRIM VIDEO NOTE (INDIVIDUAL)
            if media_groups["video_note"]:
                sent_count = 0
                for fid in media_groups["video_note"]:
                    try:
                        await asyncio.wait_for(
                            bot.send_video_note(chat_id=chat_id, video_note=fid),
                            timeout=60,
                        )
                        sent_count += 1
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logging.warning(
                            f"⚠️ Gagal kirim video_note {fid[:8]}...: {e}"
                        )
                        continue
                
                if sent_count > 0:
                    logging.info(f"✅ Terkirim {sent_count} video_note")
                    partial_success = True
                    media_groups["video_note"] = []
            
            # 8. KIRIM STICKER (INDIVIDUAL)
            if media_groups["sticker"]:
                sent_count = 0
                for fid in media_groups["sticker"]:
                    try:
                        await asyncio.wait_for(
                            bot.send_sticker(chat_id=chat_id, sticker=fid),
                            timeout=60,
                        )
                        sent_count += 1
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logging.warning(
                            f"⚠️ Gagal kirim sticker {fid[:8]}...: {e}"
                        )
                        continue
                
                if sent_count > 0:
                    logging.info(f"✅ Terkirim {sent_count} sticker")
                    partial_success = True
                    media_groups["sticker"] = []
            
            # ✅ Jika ada partial success, return True
            if partial_success:
                return True
            
            return False

        except asyncio.TimeoutError:
            logging.warning(
                f"⏱️ Timeout kirim media (attempt {attempt+1}/{max_retries})"
            )
            attempt += 1
            await asyncio.sleep(10)

        except Exception as e:
            err = str(e).lower()

            # Flood wait
            if "flood" in err or "too many requests" in err or "retry" in err:
                try:
                    suggested  = parse_retry_after(str(e))
                    total_wait = await flood_ctrl.record_flood(suggested)
                    logging.warning(
                        f"⏳ Flood wait {total_wait:.1f}s sebelum retry..."
                    )
                    await asyncio.sleep(total_wait)
                    continue
                except Exception as flood_err:
                    logging.error(f"❌ Error handling flood: {flood_err}")
                    attempt += 1
                    await asyncio.sleep(60)
                    continue

            # Chat tidak ditemukan / migrasi
            elif "chat not found" in err or "forbidden" in err:
                migrated = re.search(r"migrated to chat (\-?\d+)", str(e))
                if migrated:
                    try:
                        new_id = int(migrated.group(1))
                        logging.info(
                            f"🔄 Chat dipindahkan ke {new_id}, update target..."
                        )
                        global TARGET_CHAT_ID
                        TARGET_CHAT_ID = new_id
                        config = {
                            "daily_limit": DAILY_LIMIT,
                            "send_delay": DELAY_BETWEEN_SEND,
                            "target_chat_id": TARGET_CHAT_ID
                        }
                        await state_manager.save_config(config)
                        attempt += 1
                        continue
                    except Exception as migrate_err:
                        logging.error(f"❌ Error handling migration: {migrate_err}")
                
                logging.error(f"❌ Chat tidak ditemukan / forbidden: {e}")
                return partial_success

            # Network error
            elif (
                "connection" in err or "timeout" in err or
                "network" in err or "socket" in err
            ):
                logging.warning(f"🌐 Network error: {e}")
                await asyncio.sleep(15 * (attempt + 1))
                try:
                    await bot.get_me()
                except Exception:
                    pass
                attempt += 1
                continue

            # Unsupported media type atau error lainnya
            elif "unsupported" in err or "not supported" in err or "invalid" in err:
                logging.error(
                    f"❌ Media type tidak support atau invalid: {e}"
                )
                return partial_success

            # Error lainnya
            else:
                logging.error(
                    f"❌ Gagal kirim (attempt {attempt+1}/{max_retries}): {e}"
                )
                attempt += 1
                await asyncio.sleep(5 * attempt)

    logging.error(f"❌ Gagal kirim setelah {max_retries} percobaan")
    return partial_success

# ============================================================
# === QUEUE WORKER ===
# ============================================================
async def queue_worker(bot) -> None:
    """Main worker untuk kirim media universal"""
    global daily_count, is_sending, is_paused, flood_ctrl

    if flood_ctrl is None:
        flood_ctrl = SmartFloodController()

    logging.info(f"🚀 [BOT: {BOT_NAME}] Queue worker dimulai")
    sent_since_pause = 0
    iteration_count  = 0
    last_pending_save = datetime.now(timezone.utc)

    try:
        while True:
            try:
                # Cek pause
                if is_paused:
                    await asyncio.sleep(5)
                    continue

                # Cek daily limit
                await check_daily_reset()
                if daily_count >= DAILY_LIMIT:
                    logging.info(
                        f"🛑 [BOT: {BOT_NAME}] Daily limit tercapai "
                        f"({daily_count}/{DAILY_LIMIT})"
                    )
                    await asyncio.sleep(60)
                    continue

                # Kumpulkan batch dari queue
                batch: list = []
                try:
                    first = await asyncio.wait_for(
                        pending_queue.get(),
                        timeout=2.0
                    )
                    batch.append(first)

                    while len(batch) < GROUP_SIZE:
                        try:
                            item = pending_queue.get_nowait()
                            batch.append(item)
                        except asyncio.QueueEmpty:
                            break

                except asyncio.TimeoutError:
                    # ✅ Cek apakah queue masih penuh
                    if pending_queue.qsize() > MAX_QUEUE_SIZE * 0.9:
                        logging.warning(
                            f"⚠️ [BOT: {BOT_NAME}] Queue hampir penuh ({pending_queue.qsize()}), "
                            f"tapi timeout mengambil batch"
                        )
                    
                    # ✅ Save pending secara periodik
                    now = datetime.now(timezone.utc)
                    if (now - last_pending_save).total_seconds() >= 30:
                        try:
                            await state_manager.save_pending(
                                get_queue_snapshot(pending_queue)
                            )
                            last_pending_save = now
                        except Exception as e:
                            logging.error(f"❌ Gagal save pending: {e}")
                    
                    await asyncio.sleep(1)
                    continue

                if not batch:
                    continue

                # ✅ Track berapa item yang benar-benar di-get dari queue
                items_gotten = len(batch)

                # Proses batch
                is_sending = True
                media_items = []

                for item in batch:
                    try:
                        # ✅ Handle berbagai format
                        if not isinstance(item, (list, tuple)):
                            logging.warning(f"⚠️ Item queue bukan list/tuple, skip")
                            continue
                        
                        if len(item) == 2:
                            # Old format: (file_id, media_type)
                            file_id, media_type = item
                            fp = {"file_id": file_id}
                        elif len(item) == 3:
                            # New format: (file_id, media_type, fp)
                            file_id, media_type, fp = item
                        else:
                            logging.warning(
                                f"⚠️ Item queue format tidak dikenal (len={len(item)}), skip"
                            )
                            continue
                        
                        # ✅ Validate types
                        if not isinstance(file_id, str) or not file_id:
                            logging.warning(f"⚠️ File ID invalid, skip")
                            continue
                        
                        if not isinstance(media_type, str) or not media_type:
                            logging.warning(f"⚠️ Media type invalid, skip")
                            continue
                        
                        if media_type not in ALLOWED_MEDIA_TYPES:
                            logging.info(
                                f"⏭️ Media type '{media_type}' tidak diizinkan, skip"
                            )
                            continue
                        
                        fp_hash = make_hash(fp)

                        if await duplicate_checker.is_duplicate_safe(file_id, fp_hash):
                            logging.info(
                                f"⏭️ [BOT: {BOT_NAME}] Skip duplikat: "
                                f"{file_id[:8]}... ({media_type})"
                            )
                            if queue_deduplicator:
                                await queue_deduplicator.remove_from_queue(
                                    file_id, fp_hash
                                )
                            continue

                        media_items.append((file_id, media_type, fp_hash, fp))

                    except (ValueError, TypeError, AttributeError) as e:
                        logging.warning(
                            f"⚠️ [BOT: {BOT_NAME}] Item queue korup, skip: {e}"
                        )
                        continue
                    except Exception as e:
                        logging.error(
                            f"❌ Unexpected error processing item: {e}"
                        )
                        continue

                if not media_items:
                    is_sending = False
                    # ✅ Hanya panggil task_done sebanyak item yang di-get
                    for _ in range(items_gotten):
                        try:
                            pending_queue.task_done()
                        except ValueError:
                            break
                    continue

                target_chat_id = get_target_chat_id()

                send_list = [
                    (fid, mtype) for fid, mtype, _, _ in media_items
                ]
                
                try:
                    success = await send_media_with_retry(
                        bot,
                        target_chat_id,
                        send_list
                    )
                except Exception as e:
                    logging.error(f"❌ Unexpected error in send_media_with_retry: {e}")
                    success = False

                if success:
                    try:
                        async with sending_lock:
                            # ✅ Add upper bound
                            daily_count = min(
                                daily_count + len(media_items),
                                DAILY_LIMIT + len(media_items)
                            )
                            await check_daily_reset()

                        sent_since_pause += len(media_items)

                        for file_id, media_type, fp_hash, fp_original in media_items:
                            try:
                                success_mark = await duplicate_checker.mark_sent_safe(
                                    file_id, fp_hash
                                )
                                if success_mark and queue_deduplicator:
                                    await queue_deduplicator.remove_from_queue(
                                        file_id, fp_hash
                                    )
                            except Exception as e:
                                logging.error(f"❌ Error marking sent: {e}")
                                continue

                        await flood_ctrl.record_success()
                        await save_daily()

                        logging.info(
                            f"✅ [BOT: {BOT_NAME}] Terkirim {len(media_items)} media | "
                            f"Daily: {daily_count}/{DAILY_LIMIT} | "
                            f"Pending: {pending_queue.qsize()} | "
                            f"Queue unique: {queue_deduplicator.get_queue_size() if queue_deduplicator else '?'}"
                        )
                        
                        try:
                            await state_manager.save_pending(
                                get_queue_snapshot(pending_queue)
                            )
                            last_pending_save = datetime.now(timezone.utc)
                        except Exception as e:
                            logging.error(f"❌ Gagal save pending: {e}")
                    
                    except Exception as e:
                        logging.error(f"❌ Error processing success: {e}")
                    
                else:
                    try:
                        for file_id, media_type, fp_hash, fp_original in media_items:
                            try:
                                if not pending_queue.full():
                                    # ✅ Kembalikan fp original
                                    pending_queue.put_nowait(
                                        (file_id, media_type, fp_original)
                                    )
                            except asyncio.QueueFull:
                                logging.warning(
                                    f"⚠️ [BOT: {BOT_NAME}] Queue penuh, drop: "
                                    f"{file_id[:8]}..."
                                )
                                if queue_deduplicator:
                                    await queue_deduplicator.remove_from_queue(
                                        file_id, fp_hash
                                    )
                            except Exception as e:
                                logging.error(f"❌ Error re-queuing item: {e}")

                        logging.error(
                            f"❌ [BOT: {BOT_NAME}] Gagal kirim {len(media_items)} media, "
                            f"dikembalikan ke queue"
                        )
                        
                        try:
                            await state_manager.save_pending(
                                get_queue_snapshot(pending_queue)
                            )
                            last_pending_save = datetime.now(timezone.utc)
                        except Exception as e:
                            logging.error(f"❌ Gagal save pending: {e}")
                    
                    except Exception as e:
                        logging.error(f"❌ Error processing failure: {e}")

                # ✅ Task done
                for _ in range(items_gotten):
                    try:
                        pending_queue.task_done()
                    except ValueError:
                        break

                is_sending = False

                # Auto-save periodik
                now = datetime.now(timezone.utc)
                if (
                    (now - last_save_time).total_seconds() >=
                    AUTO_SAVE_INTERVAL
                ):
                    for i in range(3):
                        try:
                            await save_all()
                            break
                        except Exception as e:
                            logging.error(
                                f"❌ [BOT: {BOT_NAME}] Gagal save_all "
                                f"(retry {i+1}/3): {e}"
                            )
                            await asyncio.sleep(5)

                # Cleanup periodik
                iteration_count += 1
                if iteration_count % 100 == 0:
                    try:
                        await global_sent_manager.cleanup_old_files(
                            keep_last_n=15
                        )
                        if queue_deduplicator:
                            await queue_deduplicator.cleanup_stale()
                    except Exception as e:
                        logging.error(f"❌ Error cleanup: {e}")

                # Batch pause
                if sent_since_pause >= BATCH_PAUSE_EVERY:
                    pause = random.uniform(BATCH_PAUSE_MIN, BATCH_PAUSE_MAX)
                    logging.info(
                        f"⏸️ [BOT: {BOT_NAME}] Batch pause {pause:.0f}s "
                        f"setelah {sent_since_pause} media terkirim"
                    )
                    sent_since_pause = 0
                    await asyncio.sleep(pause)
                else:
                    try:
                        group_delay = flood_ctrl.get_group_delay()
                        send_delay  = DELAY_BETWEEN_SEND + random.uniform(
                            DELAY_RANDOM_MIN, DELAY_RANDOM_MAX
                        )
                        total_delay = group_delay + send_delay
                        await asyncio.sleep(total_delay)
                    except Exception as e:
                        logging.error(f"❌ Error calculating delay: {e}")
                        await asyncio.sleep(5)

            except asyncio.CancelledError:
                logging.info(
                    f"⏹️ [BOT: {BOT_NAME}] Worker menerima cancel signal"
                )
                is_sending = False
                
                try:
                    pending_snapshot = get_queue_snapshot(pending_queue)
                    await state_manager.save_pending(pending_snapshot)
                    logging.info(
                        f"💾 [BOT: {BOT_NAME}] Pending disimpan: {len(pending_snapshot)} item"
                    )
                except Exception as e:
                    logging.error(
                        f"❌ [BOT: {BOT_NAME}] Gagal save pending saat cancel: {e}"
                    )
                
                for i in range(3):
                    try:
                        await save_all()
                        break
                    except Exception as e:
                        logging.error(
                            f"❌ [BOT: {BOT_NAME}] Gagal save_all saat cancel "
                            f"(retry {i+1}/3): {e}"
                        )
                        await asyncio.sleep(5)
                
                raise

            except Exception as e:
                logging.error(
                    f"❌ [BOT: {BOT_NAME}] Worker error: {e}",
                    exc_info=True
                )
                is_sending = False
                await asyncio.sleep(5)

    except asyncio.CancelledError:
        logging.info(f"✅ [BOT: {BOT_NAME}] Queue worker berhenti dengan baik")
        raise
    except Exception as e:
        logging.error(
            f"❌ [BOT: {BOT_NAME}] Unexpected error di worker: {e}",
            exc_info=True
        )
    finally:
        logging.info(f"✅ [BOT: {BOT_NAME}] Queue worker cleanup selesai")

# ============================================================
# === HANDLERS ===
# ============================================================
async def forward_media(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """✅ Handler untuk forward media universal dengan enhanced duplicate check"""
    msg = update.message
    if not msg:
        return

    # ✅ Check source chat dulu
    if (
        ALLOWED_SOURCE_CHATS and
        update.effective_chat.id not in ALLOWED_SOURCE_CHATS
    ):
        return

    # ✅ Cek apakah message memiliki media
    has_media = (
        msg.video or msg.document or msg.photo or msg.audio or
        msg.animation or msg.voice or msg.video_note or msg.sticker
    )
    
    if not has_media:
        return

    try:
        # ✅ Get fingerprint dengan error handling
        try:
            fp = get_fingerprint(msg)
        except Exception as e:
            logging.error(f"❌ Error get_fingerprint: {e}")
            return
        
        if not fp.get("file_id"):
            logging.warning(f"⚠️ File ID tidak ditemukan di message")
            return
        
        media_type = fp.get("media_type")
        
        # ✅ Cek apakah media type diizinkan
        if media_type not in ALLOWED_MEDIA_TYPES:
            logging.info(
                f"⏭️ [BOT: {BOT_NAME}] Media type '{media_type}' tidak diizinkan"
            )
            return

        file_id  = fp["file_id"]
        fp_hash  = make_hash(fp)
        mtype    = fp["media_type"]
        file_name = fp.get("file_name", "unknown")

        # ✅ CHECK 1: GLOBAL SENT
        try:
            if await duplicate_checker.is_duplicate_safe(file_id, fp_hash):
                logging.info(
                    f"⏭️ [BOT: {BOT_NAME}] Duplikat GLOBAL terdeteksi: "
                    f"{file_id[:8]}... ({media_type})"
                )
                return
        except Exception as e:
            logging.error(f"❌ Error duplicate check: {e}")

        # ✅ CHECK 2: QUEUE
        if queue_deduplicator:
            if queue_deduplicator._queue_items.get(f"{file_id}:{fp_hash}"):
                logging.info(
                    f"⏭️ [BOT: {BOT_NAME}] Duplikat QUEUE terdeteksi: "
                    f"{file_id[:8]}... ({media_type})"
                )
                return

        # ✅ TAMBAH KE QUEUE dengan dedup
        if queue_deduplicator:
            success = await queue_deduplicator.add_to_queue(
                file_id, fp_hash, (file_id, mtype, fp)
            )
            if not success:
                logging.warning(
                    f"⚠️ [BOT: {BOT_NAME}] Gagal tambah ke queue: {file_id[:8]}..."
                )
                return
        else:
            try:
                if pending_queue.full():
                    logging.warning(
                        f"⚠️ [BOT: {BOT_NAME}] Queue penuh, drop: {file_id[:8]}..."
                    )
                    return
                await pending_queue.put((file_id, mtype, fp))
            except Exception as e:
                logging.error(
                    f"❌ [BOT: {BOT_NAME}] Gagal masukkan ke queue: {e}"
                )
                return

        logging.info(
            f"📥 [BOT: {BOT_NAME}] Masuk queue: {file_id[:8]}... | "
            f"Type: {mtype} | "
            f"File: {file_name} | "
            f"Queue unique: {queue_deduplicator.get_queue_size() if queue_deduplicator else '?'}"
        )
    
    except Exception as e:
        logging.error(f"❌ Error di forward_media handler: {e}", exc_info=True)

# ============================================================
# === ADMIN COMMANDS ===
# ============================================================
async def cmd_ping(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Ping command"""
    try:
        await update.message.reply_text("🏓 Pong!")
    except Exception as e:
        logging.error(f"❌ Error di cmd_ping: {e}")

async def cmd_help(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Help command"""
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return

    try:
        role = get_role(update)
        
        media_info = "📋 *MEDIA YANG SUPPORT:*\n"
        for media_type in sorted(ALLOWED_MEDIA_TYPES):
            media_info += f"  • {MEDIA_TYPE_DISPLAY.get(media_type, media_type)}\n"
        
        text = (
            media_info + "\n"
            "*Moderator Commands:*\n"
            "├ /ping — Health check\n"
            "├ /status — Status bot\n"
            "├ /stats — Statistik realtime\n"
            "├ /pause — Jeda worker\n"
            "└ /resume — Lanjutkan worker\n"
        )
        if role == "superadmin":
            text += (
                "\n*Superadmin Commands:*\n"
                "├ /flushpending — Kosongkan queue\n"
                "├ /setlimit <n> — Set daily limit (1-5000)\n"
                "├ /setdelay <n> — Set delay antar kirim (0.1-10.0)\n"
                "├ /resetdaily — Reset counter harian\n"
                "├ /log — Lihat 15 baris log terakhir\n"
                "├ /globalstats — Lihat statistik global\n"
                "├ /sentfiles — Lihat file sent\n"
                "└ /shutdown — Matikan bot\n"
            )
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"❌ Error di cmd_help: {e}")
        await update.message.reply_text("❌ Error menampilkan help")

async def cmd_status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Status command"""
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return

    try:
        global_info = global_sent_manager.get_info()
        total_sent_global = global_info["total_entries"]

        queue_size = pending_queue.qsize() if pending_queue else 0
        worker_status = (
            "⏸️ Dijeda" if is_paused else
            ("🔄 Mengirim" if is_sending else "✅ Idle")
        )
        daily_pct = (
            (daily_count / DAILY_LIMIT * 100)
            if DAILY_LIMIT > 0 else 0
        )
        bar_filled = int(daily_pct / 10)
        bar = "🟩" * bar_filled + "⬜" * (10 - bar_filled)

        last_save_str = "N/A"
        if last_save_time:
            now = datetime.now(timezone.utc)
            diff = now - last_save_time
            if diff.total_seconds() < 60:
                last_save_str = "Baru saja"
            elif diff.total_seconds() < 3600:
                last_save_str = (
                    f"{int(diff.total_seconds() // 60)} menit lalu"
                )
            else:
                last_save_str = (
                    f"{int(diff.total_seconds() // 3600)} jam lalu"
                )

        flood_status = (
            flood_ctrl.get_status() if flood_ctrl else "N/A"
        )

        now = datetime.now(timezone.utc)
        uptime = now - start_time
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)

        text = (
            "📊 *Status Bot HANAYA v5.7 — " + BOT_NAME + "*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 Worker        : {worker_status}\n"
            f"📦 Pending      : {queue_size} media\n"
            f"📤 Terkirim      : {daily_count}/{DAILY_LIMIT} ({daily_pct:.1f}%)\n"
            f"🌍 Global Sent : {total_sent_global} media (SHARED)\n"
            f"    └─ {bar}\n"
            f"⏱️ Last Save    : {last_save_str}\n"
            f"⚡ Flood Ctrl    : {flood_status}\n"
            f"⏱️ Uptime         : {hours}h {minutes}m {seconds}s\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "💡 Gunakan /help untuk daftar command"
        )

        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"❌ Error di cmd_status: {e}")
        await update.message.reply_text("❌ Error menampilkan status")

async def cmd_stats(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Stats command"""
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return

    try:
        global_info = global_sent_manager.get_info()
        total_sent_global = global_info["total_entries"]

        queue_size = pending_queue.qsize() if pending_queue else 0
        remaining = max(0, DAILY_LIMIT - daily_count)
        pct_used = (
            (daily_count / DAILY_LIMIT * 100)
            if DAILY_LIMIT > 0 else 0
        )
        bar_filled = int(pct_used / 10)
        bar = "█" * bar_filled + "░" * (10 - bar_filled)

        text = (
            f"📈 *STATISTIK REALTIME — {BOT_NAME}*\n\n"
            f"*Pengiriman Hari Ini (LOCAL)*\n"
            f"[{bar}] {pct_used:.1f}%\n"
            f"├ Terkirim  : {daily_count}\n"
            f"├ Sisa      : {remaining}\n"
            f"└ Limit     : {DAILY_LIMIT}\n\n"
            f"*Queue*\n"
            f"├ Pending   : {queue_size}\n"
            f"└ Total Sent: {total_sent_global} (GLOBAL)\n\n"
            f"*Flood Control*\n"
            f"└ {flood_ctrl.get_status() if flood_ctrl else 'N/A'}"
        )
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"❌ Error di cmd_stats: {e}")
        await update.message.reply_text("❌ Error menampilkan stats")

async def cmd_globalstats(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Global stats command"""
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return

    try:
        global_info = global_sent_manager.get_info()

        text = (
            f"🌍 *STATISTIK GLOBAL SENT — SHARED*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Total Entry      : {global_info['total_entries']}\n"
            f"📄 Jumlah File      : {global_info['total_files']}\n"
            f"📁 File Aktif       : {global_info['current_file']}\n"
            f"📝 Entry di File    : {global_info['current_count']}/{global_info['max_per_file']}\n"
            f"🔒 Lock Type        : OS-level (fcntl) + asyncio\n"
            f"🔄 Last Reload      : {global_info['last_reload']}\n"
            f"⏱️ Reload Age       : {global_info['reload_age']}\n"
            f"⏲️ Reload Interval  : {global_info['reload_interval']}\n"
            f"📂 Known Files      : {global_info['known_files']}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ *Anti-Duplikat:*\n"
            f"   • Triple-check sebelum kirim\n"
            f"   • Queue deduplication\n"
            f"   • Atomic write dengan fsync\n"
        )

        files = global_sent_manager._get_all_sent_files()
        if files:
            text += f"\n📝 *File Details (Last 5):*\n"
            for f in files[-5:]:
                try:
                    with open(f, "r", encoding="utf-8") as fp:
                        data = json.load(fp)
                        count = len(data) if isinstance(data, list) else 0
                    size_kb = f.stat().st_size / 1024
                    is_active = "✅" if f == global_sent_manager._current_file else "📦"
                    text += (
                        f"{is_active} {f.name} | "
                        f"{count}/{global_info['max_per_file']} | "
                        f"{size_kb:.1f}KB\n"
                    )
                except Exception as e:
                    logging.warning(f"⚠️ Error reading file {f.name}: {e}")
                    text += f"❌ {f.name}\n"

        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"❌ Error di cmd_globalstats: {e}")
        await update.message.reply_text("❌ Error menampilkan global stats")

async def cmd_pause(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Pause command"""
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return
    
    try:
        global is_paused
        is_paused = True
        name = update.effective_user.first_name
        await update.message.reply_text(f"⏸️ Worker dijeda oleh {name}")
        logging.info(
            f"⏸️ [BOT: {BOT_NAME}] Worker dijeda oleh {name} "
            f"({update.effective_user.id})"
        )
    except Exception as e:
        logging.error(f"❌ Error di cmd_pause: {e}")

async def cmd_resume(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Resume command"""
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return
    
    try:
        global is_paused
        is_paused = False
        name = update.effective_user.first_name
        await update.message.reply_text(f"▶️ Worker dilanjutkan oleh {name}")
        logging.info(
            f"▶️ [BOT: {BOT_NAME}] Worker dilanjutkan oleh {name} "
            f"({update.effective_user.id})"
        )
    except Exception as e:
        logging.error(f"❌ Error di cmd_resume: {e}")

async def cmd_flushpending(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Flush pending command"""
    if not is_superadmin(update):
        await update.message.reply_text(
            "❌ Hanya superadmin yang bisa flush pending"
        )
        return

    if not await admin_rate_limit_check(update.effective_user.id):
        await update.message.reply_text("⏳ Terlalu cepat, tunggu 5 detik")
        return

    try:
        count = pending_queue.qsize()
        while not pending_queue.empty():
            try:
                pending_queue.get_nowait()
                pending_queue.task_done()
            except asyncio.QueueEmpty:
                break

        if queue_deduplicator:
            await queue_deduplicator.clear_on_startup()

        for i in range(3):
            try:
                await save_all()
                break
            except Exception as e:
                logging.error(
                    f"❌ [BOT: {BOT_NAME}] Gagal save_all setelah flush "
                    f"(retry {i+1}/3): {e}"
                )
                await asyncio.sleep(5)

        name = update.effective_user.first_name
        await update.message.reply_text(
            f"🗑️ Queue dikosongkan ({count} item) oleh {name}"
        )
        logging.info(
            f"🗑️ [BOT: {BOT_NAME}] Queue dikosongkan ({count} item) oleh {name}"
        )
    except Exception as e:
        logging.error(f"❌ Error di cmd_flushpending: {e}")
        await update.message.reply_text("❌ Error flushing pending")

async def cmd_resetdaily(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Reset daily command"""
    if not is_superadmin(update):
        await update.message.reply_text(
            "❌ Hanya superadmin yang bisa reset daily"
        )
        return

    if not await admin_rate_limit_check(update.effective_user.id):
        await update.message.reply_text("⏳ Terlalu cepat, tunggu 5 detik")
        return

    try:
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
        logging.info(
            f"🔄 [BOT: {BOT_NAME}] Daily reset oleh {name}, sebelumnya: {prev}"
        )
    except Exception as e:
        logging.error(f"❌ Error di cmd_resetdaily: {e}")
        await update.message.reply_text("❌ Error resetting daily")

async def cmd_setlimit(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Set limit command dengan atomic update"""
    if not is_superadmin(update):
        await update.message.reply_text(
            "❌ Hanya superadmin yang bisa ubah limit"
        )
        return

    if not await admin_rate_limit_check(update.effective_user.id):
        await update.message.reply_text("⏳ Terlalu cepat, tunggu 5 detik")
        return

    if config_lock is None:
        await update.message.reply_text("⚠️ Bot belum siap, coba lagi")
        return

    try:
        arg = _flatten_arg(context.args)
        if not arg:
            await update.message.reply_text("❌ Contoh: /setlimit 1000")
            return
        new_limit = int(arg)
        if not (1 <= new_limit <= 5000):
            await update.message.reply_text(
                "❌ Limit harus antara 1-5000"
            )
            return
        global DAILY_LIMIT
        
        # ✅ Atomic update dengan lock
        async with config_lock:
            DAILY_LIMIT = new_limit
            config = {
                "daily_limit": DAILY_LIMIT,
                "send_delay": DELAY_BETWEEN_SEND
            }
            await state_manager.save_config(config)
        
        name = update.effective_user.first_name
        await update.message.reply_text(
            f"✅ Daily limit diubah ke {DAILY_LIMIT} oleh {name}"
        )
        logging.info(
            f"⚙️ [BOT: {BOT_NAME}] Daily limit → {DAILY_LIMIT} oleh {name}"
        )
    except (ValueError, TypeError):
        await update.message.reply_text("❌ Contoh: /setlimit 1000")
    except Exception as e:
        logging.error(f"❌ Error di cmd_setlimit: {e}")
        await update.message.reply_text("❌ Error setting limit")

async def cmd_setdelay(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Set delay command"""
    if not is_superadmin(update):
        await update.message.reply_text(
            "❌ Hanya superadmin yang bisa ubah delay"
        )
        return

    if not await admin_rate_limit_check(update.effective_user.id):
        await update.message.reply_text("⏳ Terlalu cepat, tunggu 5 detik")
        return

    if config_lock is None:
        await update.message.reply_text("⚠️ Bot belum siap, coba lagi")
        return

    try:
        arg = _flatten_arg(context.args)
        if not arg:
            await update.message.reply_text("❌ Contoh: /setdelay 2.5")
            return
        new_delay = float(arg)
        if not (0.1 <= new_delay <= 10.0):
            await update.message.reply_text(
                "❌ Delay harus antara 0.1-10.0 detik"
            )
            return
        global DELAY_BETWEEN_SEND
        async with config_lock:
            DELAY_BETWEEN_SEND = new_delay
        config = {
            "daily_limit": DAILY_LIMIT,
            "send_delay": DELAY_BETWEEN_SEND
        }
        await state_manager.save_config(config)
        name = update.effective_user.first_name
        await update.message.reply_text(
            f"✅ Delay diubah ke {DELAY_BETWEEN_SEND:.1f}s oleh {name}"
        )
        logging.info(
            f"⚙️ [BOT: {BOT_NAME}] Send delay → {DELAY_BETWEEN_SEND:.1f}s "
            f"oleh {name}"
        )
    except (ValueError, TypeError):
        await update.message.reply_text("❌ Contoh: /setdelay 2.5")
    except Exception as e:
        logging.error(f"❌ Error di cmd_setdelay: {e}")
        await update.message.reply_text("❌ Error setting delay")

async def cmd_log(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """✅ Log command dengan error handling"""
    if not is_superadmin(update):
        await update.message.reply_text(
            "❌ Hanya superadmin yang bisa lihat log"
        )
        return

    if not await admin_rate_limit_check(update.effective_user.id):
        await update.message.reply_text("⏳ Terlalu cepat, tunggu 5 detik")
        return

    try:
        log_dir = Path("logs")
        main_log = log_dir / f"{BOT_NAME}_main.log"
        
        if not main_log.exists():
            await update.message.reply_text("❌ File log tidak ditemukan")
            return
        
        # ✅ Check file size
        file_size = main_log.stat().st_size
        if file_size == 0:
            await update.message.reply_text("📝 File log kosong")
            return
        
        # ✅ Read dengan safe
        try:
            with open(main_log, "r", encoding="utf-8") as f:
                lines = f.readlines()
                
                if not lines:
                    await update.message.reply_text("📝 File log kosong")
                    return
                
                log_lines = lines[-20:] if len(lines) > 20 else lines
                log_text = "".join(log_lines)
                
                # ✅ Truncate jika terlalu panjang
                if len(log_text) > 4000:
                    log_text = "...\n" + log_text[-3996:]
                
                await update.message.reply_text(
                    f"```\n{log_text}```",
                    parse_mode="Markdown"
                )
        except UnicodeDecodeError:
            await update.message.reply_text(
                "❌ File log tidak bisa dibaca (encoding error)"
            )
    
    except Exception as e:
        logging.error(f"❌ Error di cmd_log: {e}")
        await update.message.reply_text(f"❌ Gagal baca log: {e}")

async def cmd_sentfiles(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Sent files command"""
    if not is_moderator(update):
        await update.message.reply_text("❌ Akses ditolak")
        return

    try:
        files = global_sent_manager._get_all_sent_files()
        total_cache = len(global_sent_manager._cache)

        if not files:
            await update.message.reply_text(
                "📂 Tidak ada file sent tersimpan"
            )
            return

        lines = [
            f"📂 *FILE SENT GLOBAL — SHARED*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Total cache : {total_cache} entry\n"
            f"📄 Jumlah file : {len(files)} file\n"
            f"🔒 Lock Type   : OS-level (fcntl) + asyncio\n"
            f"🔄 Auto-reload : Setiap 10 detik\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        ]

        for f in files:
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    data = json.load(fp)
                    count = len(data) if isinstance(data, list) else 0
                size_kb = f.stat().st_size / 1024
                is_active = (
                    "✅ Aktif" if f == global_sent_manager._current_file
                    else "📦 Arsip"
                )
                lines.append(
                    f"{is_active} | {f.name} | "
                    f"{count}/{MAX_SENT_PER_FILE} entry | "
                    f"{size_kb:.1f} KB\n"
                )
            except Exception as e:
                logging.warning(f"⚠️ Error reading file {f.name}: {e}")
                lines.append(f"❌ {f.name} — Error: {e}\n")

        await update.message.reply_text(
            "".join(lines),
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"❌ Error di cmd_sentfiles: {e}")
        await update.message.reply_text("❌ Error menampilkan sent files")

async def cmd_shutdown(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Shutdown command"""
    if not is_superadmin(update):
        await update.message.reply_text(
            "❌ Hanya superadmin yang bisa matikan bot"
        )
        return

    if not await admin_rate_limit_check(update.effective_user.id):
        await update.message.reply_text("⏳ Terlalu cepat, tunggu 5 detik")
        return

    try:
        name = update.effective_user.first_name
        await update.message.reply_text(f"🛑 Bot dimatikan oleh {name}")
        logging.info(f"🛑 [BOT: {BOT_NAME}] Bot dimatikan oleh {name}")
        for i in range(3):
            try:
                await save_all()
                break
            except Exception as e:
                logging.error(
                    f"❌ [BOT: {BOT_NAME}] Gagal save_all saat shutdown "
                    f"(retry {i+1}/3): {e}"
                )
                await asyncio.sleep(5)
        await context.application.stop()
    except Exception as e:
        logging.error(f"❌ Error di cmd_shutdown: {e}")

# ============================================================
# === FLASK WEB SERVER ===
# ============================================================
flask_app = Flask(__name__)

@flask_app.route('/health', methods=['GET'])
def health_check():
    """✅ Health check endpoint dengan proper status"""
    try:
        now = datetime.now(timezone.utc)
        uptime = now - start_time
        
        # ✅ Check global state
        global_ok = True
        global_files = 0
        try:
            global_files = len(global_sent_manager._get_all_sent_files())
        except Exception as e:
            global_ok = False
            logging.error(f"❌ Global state check failed: {e}")
        
        # ✅ Check filesystem
        fs_ok = True
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            GLOBAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            fs_ok = False
            logging.error(f"❌ Filesystem check failed: {e}")
        
        # ✅ Check queue
        queue_ok = pending_queue is not None
        
        # ✅ Determine overall status
        if global_ok and fs_ok and queue_ok:
            status = "up"
            http_code = 200
        elif global_ok and queue_ok:
            status = "degraded"
            http_code = 503
        else:
            status = "down"
            http_code = 503
        
        global_info = global_sent_manager.get_info()

        return jsonify({
            "status": status,
            "http_code": http_code,
            "bot_name": BOT_NAME,
            "queue_size": pending_queue.qsize() if pending_queue else 0,
            "daily_count": daily_count,
            "daily_limit": DAILY_LIMIT,
            "total_sent_global": global_info["total_entries"],
            "global_files": global_files,
            "global_ok": global_ok,
            "fs_ok": fs_ok,
            "queue_ok": queue_ok,
            "worker_status": (
                "paused" if is_paused else
                ("sending" if is_sending else "idle")
            ),
            "uptime_seconds": int(uptime.total_seconds()),
            "flood_status": flood_ctrl.get_status() if flood_ctrl else "N/A",
            "timestamp": now.isoformat()
        }), http_code
    
    except Exception as e:
        logging.error(f"❌ Error di health_check: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }), 500

@flask_app.route('/dashboard', methods=['GET'])
def dashboard():
    """Web dashboard"""
    try:
        now = datetime.now(timezone.utc)
        uptime = now - start_time
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)

        daily_pct = (
            (daily_count / DAILY_LIMIT * 100)
            if DAILY_LIMIT > 0 else 0
        )
        global_info = global_sent_manager.get_info()
        total_sent_global = global_info["total_entries"]
        
        queue_usage = (
            (pending_queue.qsize() / MAX_QUEUE_SIZE * 100)
            if pending_queue and MAX_QUEUE_SIZE > 0 else 0
        )

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>HANAYA Bot Dashboard v5.7</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0f172a; color: #e2e8f0; }}
                .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
                .header {{ text-align: center; margin-bottom: 30px; }}
                .header h1 {{ font-size: 2.5em; color: #60a5fa; margin-bottom: 10px; }}
                .header p {{ color: #94a3b8; font-size: 1.1em; }}
                .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 30px; }}
                .card {{ background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 20px; }}
                .card h3 {{ color: #60a5fa; margin-bottom: 15px; font-size: 1.2em; }}
                .stat {{ display: flex; justify-content: space-between; margin: 10px 0; padding: 8px 0; border-bottom: 1px solid #334155; }}
                .stat-label {{ color: #cbd5e1; }}
                .stat-value {{ color: #10b981; font-weight: bold; }}
                .stat-value.warning {{ color: #f59e0b; }}
                .stat-value.danger {{ color: #ef4444; }}
                .progress-bar {{ background: #334155; height: 30px; border-radius: 4px; overflow: hidden; margin: 10px 0; }}
                .progress-fill {{ background: linear-gradient(90deg, #10b981, #60a5fa); height: 100%; display: flex; align-items: center; justify-content: center; color: white; font-weight: bold; }}
                .status-badge {{ display: inline-block; padding: 5px 12px; border-radius: 20px; font-size: 0.9em; font-weight: bold; }}
                .status-up {{ background: #10b981; color: white; }}
                .status-paused {{ background: #f59e0b; color: white; }}
                .status-sending {{ background: #3b82f6; color: white; }}
                .badge-global {{ background: #8b5cf6; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; }}
                .badge-safe {{ background: #10b981; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; }}
                .footer {{ text-align: center; color: #64748b; font-size: 0.9em; margin-top: 30px; }}
                .refresh-info {{ text-align: center; color: #94a3b8; font-size: 0.85em; margin-top: 10px; }}
            </style>
            <meta http-equiv="refresh" content="10">
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🌸 HANAYA Bot v5.7</h1>
                    <p>Dashboard — {BOT_NAME} |
                       <span class="badge-global">GLOBAL SENT</span>
                       <span class="badge-safe">ANTI-DUPLIKAT AMAN</span>
                    </p>
                </div>

                <div class="grid">
                    <div class="card">
                        <h3>📊 Status</h3>
                        <div class="stat">
                            <span class="stat-label">Bot Status</span>
                            <span class="status-badge status-up">✅ UP</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Worker</span>
                            <span class="status-badge {'status-paused' if is_paused else ('status-sending' if is_sending else 'status-up')}">
                                {'⏸️ Paused' if is_paused else ('🔄 Sending' if is_sending else '✅ Idle')}
                            </span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Uptime</span>
                            <span class="stat-value">{hours}h {minutes}m {seconds}s</span>
                        </div>
                    </div>

                    <div class="card">
                        <h3>📤 Daily Quota (LOCAL)</h3>
                        <div class="stat">
                            <span class="stat-label">Sent Today</span>
                            <span class="stat-value">{daily_count}/{DAILY_LIMIT}</span>
                        </div>
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: {min(daily_pct, 100):.1f}%">
                                {daily_pct:.1f}%
                            </div>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Remaining</span>
                            <span class="stat-value">{max(0, DAILY_LIMIT - daily_count)}</span>
                        </div>
                    </div>

                    <div class="card">
                        <h3>📦 Queue</h3>
                        <div class="stat">
                            <span class="stat-label">Pending Items</span>
                            <span class="stat-value {'warning' if (pending_queue.qsize() if pending_queue else 0) > 1000 else ''}">
                                {pending_queue.qsize() if pending_queue else 0}
                            </span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Queue Usage</span>
                            <span class="stat-value">{queue_usage:.1f}%</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Unique Items</span>
                            <span class="stat-value">{queue_deduplicator.get_queue_size() if queue_deduplicator else '?'}</span>
                        </div>
                    </div>

                    <div class="card">
                        <h3>🌍 Global Sent (SHARED)</h3>
                        <div class="stat">
                            <span class="stat-label">Total Entries</span>
                            <span class="stat-value">{total_sent_global}</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Sent Files</span>
                            <span class="stat-value">{global_info['total_files']}</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Last Reload</span>
                            <span class="stat-value">{global_info['reload_age']} ago</span>
                        </div>
                    </div>

                    <div class="card">
                        <h3>⚡ Flood Control</h3>
                        <div class="stat">
                            <span class="stat-label">Flood Count</span>
                            <span class="stat-value {'danger' if (flood_ctrl.flood_count if flood_ctrl else 0) > 5 else 'warning' if (flood_ctrl.flood_count if flood_ctrl else 0) > 0 else ''}">
                                {flood_ctrl.flood_count if flood_ctrl else 0}
                            </span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Total Floods</span>
                            <span class="stat-value">{flood_ctrl.total_flood if flood_ctrl else 0}</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Current Penalty</span>
                            <span class="stat-value">{f"{flood_ctrl.penalty:.0f}s" if flood_ctrl else "0s"}</span>
                        </div>
                    </div>

                    <div class="card">
                        <h3>🔄 Anti-Duplikat</h3>
                        <div class="stat">
                            <span class="stat-label">Reload Interval</span>
                            <span class="stat-value">{global_info['reload_interval']}</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Triple-Check</span>
                            <span class="stat-value">✅ Enabled</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Queue Dedup</span>
                            <span class="stat-value">✅ Enabled</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Atomic Write</span>
                            <span class="stat-value">✅ Enabled</span>
                        </div>
                    </div>
                </div>

                <div class="refresh-info">
                    🔄 Auto-refresh setiap 10 detik | API: /health |
                    <span class="badge-global">GLOBAL SENT: data/global/sent/</span> |
                    <span class="badge-safe">✅ FULLY PROTECTED</span>
                </div>
                <div class="footer">
                    <p>HANAYA Bot v5.7 © 2026 | Multi-Bot with Enhanced Duplicate Detection |
                    Last updated: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
                </div>
            </div>
        </body>
        </html>
        """
        return html, 200
    except Exception as e:
        logging.error(f"❌ Error di dashboard: {e}")
        return f"❌ Error: {e}", 500

def run_flask():
    """Run Flask server di thread terpisah"""
    try:
        flask_app.run(
            host='0.0.0.0',
            port=FLASK_PORT,
            debug=False,
            use_reloader=False,
            threaded=True
        )
    except Exception as e:
        logging.error(f"❌ Error running Flask: {e}")

# ============================================================
# === STARTUP & SHUTDOWN ===
# ============================================================
async def on_startup(app) -> None:
    """Startup handler dengan clean console output"""
    global sending_lock, config_lock, ratelimit_lock, pending_queue, flood_ctrl, \
           duplicate_checker, queue_deduplicator, _worker_task, _shutdown_event, console_mgr
           
    # ✅ Setup logging PERTAMA
    setup_logging(BOT_NAME)
    
    # ✅ Print banner
    console_mgr = ConsoleOutputManager(Path("logs") / f"{BOT_NAME}_main.log")
    console_mgr.print_startup_banner(BOT_NAME)
    console_mgr.print_config_summary()

    sending_lock  = asyncio.Lock()
    config_lock   = asyncio.Lock()
    ratelimit_lock = asyncio.Lock()
    pending_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
    duplicate_checker = EnhancedDuplicateChecker(global_sent_manager)
    queue_deduplicator = QueueDeduplicator(max_size=10000)
    _shutdown_event = asyncio.Event()

    console_mgr.print_status_line("Loading GLOBAL sent files...")
    logging.info(f"📥 [BOT: {BOT_NAME}] Loading GLOBAL sent files...")
    await global_sent_manager.load_all()

    console_mgr.print_status_line("Loading LOCAL state...")
    logging.info(f"📥 [BOT: {BOT_NAME}] Loading LOCAL state...")
    await load_local_state()

    if flood_ctrl is None:
        flood_ctrl = SmartFloodController()

    await queue_deduplicator.clear_on_startup()

    _worker_task = asyncio.create_task(queue_worker(app.bot))

    console_mgr.print_status_line(f"Queue worker started!")
    logging.info(f"🚀 [BOT: {BOT_NAME}] Queue worker started!")
    
    # ✅ Print startup logs
    console_mgr.print_startup_logs()
    
    # ✅ Print ready message
    console_mgr.print_ready()
    
    logging.info(f"🚀 [BOT: {BOT_NAME}] Bot siap!")

async def on_shutdown(app) -> None:
    """✅ Shutdown handler dengan proper cleanup"""
    global _worker_task
    
    print("\n" + "="*70)
    print("🛑 BOT SHUTDOWN INITIATED")
    print("="*70)
    
    logging.info(f"🛑 [BOT: {BOT_NAME}] Shutdown dimulai...")
    
    # ✅ Cancel worker task
    if _worker_task and not _worker_task.done():
        print("\n⏹️  Membatalkan queue worker...")
        logging.info(f"⏹️ [BOT: {BOT_NAME}] Membatalkan queue worker...")
        _worker_task.cancel()
        
        try:
            await asyncio.wait_for(_worker_task, timeout=10)
        except asyncio.CancelledError:
            print("✅ Queue worker dibatalkan")
            logging.info(f"✅ [BOT: {BOT_NAME}] Queue worker dibatalkan")
        except asyncio.TimeoutError:
            print("⚠️  Queue worker timeout, force stop")
            logging.warning(
                f"⚠️ [BOT: {BOT_NAME}] Queue worker timeout, force stop"
            )
            _worker_task.cancel()
            try:
                await asyncio.wait_for(_worker_task, timeout=2)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        except Exception as e:
            print(f"❌ Error cancel worker: {e}")
            logging.error(f"❌ Error cancel worker: {e}")
    
    # ✅ Final save dengan retry
    print("\n💾 Final save semua data...")
    logging.info(f"💾 [BOT: {BOT_NAME}] Final save semua data...")
    pending_snapshot = get_queue_snapshot(pending_queue) if pending_queue else []
    
    for i in range(5):
        try:
            await state_manager.save_pending(pending_snapshot)
            print(f"   ├─ Pending saved: {len(pending_snapshot)} item")
            logging.info(
                f"💾 [BOT: {BOT_NAME}] Pending final save: {len(pending_snapshot)} item"
            )
            
            await save_all()
            print(f"   └─ All state saved")
            logging.info(f"✅ [BOT: {BOT_NAME}] Final save selesai")
            break
        except Exception as e:
            print(f"   ⚠️  Save attempt {i+1}/5 failed: {e}")
            logging.error(
                f"❌ [BOT: {BOT_NAME}] Gagal final save (retry {i+1}/5): {e}"
            )
            await asyncio.sleep(2)
    
    print("\n" + "="*70)
    print("✅ BOT SHUTDOWN COMPLETE - Data saved")
    print("="*70 + "\n")
    logging.info(f"✅ [BOT: {BOT_NAME}] Bot berhenti, data tersimpan")

# ============================================================
# === SIGNAL HANDLER ===
# ============================================================
def handle_shutdown(signum, frame):
    """✅ Handle shutdown signal dengan timeout"""
    logging.info(
        f"⚠️ [BOT: {BOT_NAME}] Shutdown signal diterima ({signum}), "
        f"graceful shutdown dalam 30 detik..."
    )
    try:
        loop = asyncio.get_running_loop()
        
        # ✅ Set event untuk signal shutdown
        if _shutdown_event:
            loop.call_soon_threadsafe(_shutdown_event.set)
        
        # ✅ Juga stop loop dengan timeout
        def stop_loop():
            loop.stop()
        
        loop.call_later(30, stop_loop)
        
    except RuntimeError:
        logging.info(f"✅ [BOT: {BOT_NAME}] Loop tidak berjalan, exit langsung")
        sys.exit(0)
    except Exception as e:
        logging.error(f"❌ Error di handle_shutdown: {e}")
        sys.exit(1)

# ============================================================
# === ERROR HANDLER ===
# ============================================================
async def error_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Global error handler"""
    error = context.error
    if error is None:
        return

    logging.error(
        f"❌ [BOT: {BOT_NAME}] Update caused error:\n"
        f"   Update: {update}\n"
        f"   Error: {error}",
        exc_info=error if isinstance(error, Exception) else None
    )

# ============================================================
# === MAIN ===
# ============================================================
def main():
    """Main entry point"""
    # ✅ Cross-platform signal handling
    try:
        signal.signal(signal.SIGINT, handle_shutdown)
    except Exception as e:
        print(f"⚠️ Gagal setup SIGINT: {e}")
    
    # ✅ SIGTERM hanya di Unix/Linux
    if hasattr(signal, 'SIGTERM'):
        try:
            signal.signal(signal.SIGTERM, handle_shutdown)
        except Exception as e:
            print(f"⚠️ Gagal setup SIGTERM: {e}")

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    app.add_error_handler(error_handler)
    
    # Message filter untuk semua media type
    app.add_handler(MessageHandler(
        filters.VIDEO,
        forward_media
    ))
    app.add_handler(MessageHandler(
        filters.PHOTO,
        forward_media
    ))
    app.add_handler(MessageHandler(
        filters.AUDIO,
        forward_media
    ))
    app.add_handler(MessageHandler(
        filters.VOICE,
        forward_media
    ))
    app.add_handler(MessageHandler(
        filters.VIDEO_NOTE,
        forward_media
    ))
    app.add_handler(MessageHandler(
        filters.Document.ALL,
        forward_media
    ))
    
    # Try tambah animation dan sticker
    try:
        app.add_handler(MessageHandler(
            filters.Animation,
            forward_media
        ))
    except AttributeError:
        logging.info("⚠️ Animation filter tidak tersedia")
    
    try:
        app.add_handler(MessageHandler(
            filters.Sticker.ALL,
            forward_media
        ))
    except AttributeError:
        logging.info("⚠️ Sticker filter tidak tersedia")
    
    # Admin commands
    app.add_handler(CommandHandler("ping",         cmd_ping))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(CommandHandler("status",       cmd_status))
    app.add_handler(CommandHandler("stats",        cmd_stats))
    app.add_handler(CommandHandler("globalstats",  cmd_globalstats))
    app.add_handler(CommandHandler("pause",        cmd_pause))
    app.add_handler(CommandHandler("resume",       cmd_resume))
    app.add_handler(CommandHandler("flushpending", cmd_flushpending))
    app.add_handler(CommandHandler("resetdaily",   cmd_resetdaily))
    app.add_handler(CommandHandler("setlimit",     cmd_setlimit))
    app.add_handler(CommandHandler("setdelay",     cmd_setdelay))
    app.add_handler(CommandHandler("log",          cmd_log))
    app.add_handler(CommandHandler("sentfiles",    cmd_sentfiles))
    app.add_handler(CommandHandler("shutdown",     cmd_shutdown))

    # Start Flask server
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logging.info(
        f"🌐 [BOT: {BOT_NAME}] Flask web server dimulai di port {FLASK_PORT}"
    )

    try:
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
    except KeyboardInterrupt:
        print("\n⚠️  Keyboard interrupt diterima")
        logging.info(f"⚠️ [BOT: {BOT_NAME}] Keyboard interrupt diterima")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        logging.error(
            f"❌ [BOT: {BOT_NAME}] Fatal error: {e}",
            exc_info=True
        )
    finally:
        logging.info(f"✅ [BOT: {BOT_NAME}] Bot shutdown complete")

if __name__ == "__main__":
    main()