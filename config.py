import os
from dotenv import load_dotenv

load_dotenv()

# === BOT CONFIG ===
BOT_TOKEN      = os.getenv("BOT_F_LOKAL", "")
TARGET_CHAT_ID = int(os.getenv("CHAT_ID_BEDUL", 0))
ADMIN_CHAT_ID  = int(os.getenv("CHAT_ID_ADMIN", 0))

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN tidak diatur di .env")
if TARGET_CHAT_ID == 0:
    raise ValueError("❌ CHAT_ID_TARGET tidak diatur di .env")
if ADMIN_CHAT_ID == 0:
    import logging
    logging.warning("⚠️ ADMIN_CHAT_ID tidak diatur — flood alert dinonaktifkan")

# === REDIS CONFIG ===
REDIS_HOST     = os.getenv("REDIS_HOST", "")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

# === RATE LIMIT SETTINGS ===
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

# === FLOOD CONTROL SETTINGS ===
FLOOD_RANDOM_MIN     = 10
FLOOD_RANDOM_MAX     = 30
FLOOD_PENALTY_STEP   = 15
FLOOD_MAX_PENALTY    = 300
FLOOD_RESET_AFTER    = 600
FLOOD_WARN_THRESHOLD = 3

# === TTL CONFIG ===
SENT_TTL = 60 * 60 * 24 * 30  # 30 hari
