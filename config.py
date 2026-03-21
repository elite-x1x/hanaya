import os
from dotenv import load_dotenv
import json
load_dotenv()

# Bot & Chat
BOT_TOKEN      = os.getenv("BOT_F_LOKAL", "")
TARGET_CHAT_ID = int(os.getenv("CHAT_ID_BEDUL", 0))
ADMIN_CHAT_ID  = int(os.getenv("CHAT_ID_ADMIN", 0))

# Admins
def _parse_id_list(key: str) -> list[int]:
    return [int(x.strip()) for x in os.getenv(key, "").split(",") if x.strip().lstrip("-").isdigit()]

ADMINS = {
    **{uid: "superadmin" for uid in _parse_id_list("SUPERADMINS")},
    **{uid: "moderator"  for uid in _parse_id_list("MODERATORS")},
}

# Redis
REDIS_HOST     = os.getenv("REDIS_HOST", "")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

# Redis Keys
KEY_SENT        = "hanaya:sent"
KEY_PENDING     = "hanaya:pending"
KEY_DAILY_COUNT = "hanaya:daily_count"
KEY_DAILY_DATE  = "hanaya:daily_date"
KEY_FLOOD_CTRL  = "hanaya:flood_ctrl"
KEY_DAILY_LIMIT = "hanaya:daily_limit"
KEY_SEND_DELAY  = "hanaya:send_delay"

# Rate Limits
SENT_TTL                = 60 * 60 * 24 * 30  # 30 hari
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

# Flood Control
FLOOD_RANDOM_MIN     = 10
FLOOD_RANDOM_MAX     = 30
FLOOD_PENALTY_STEP   = 15
FLOOD_MAX_PENALTY    = 300
FLOOD_RESET_AFTER    = 600
FLOOD_WARN_THRESHOLD = 3