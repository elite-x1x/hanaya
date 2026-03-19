import os, logging
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

BOT_TOKEN      = os.getenv("BOT_F_LOKAL", "xxxx")
TARGET_CHAT_ID = int(os.getenv("CHAT_ID_BEDUL", 1234))
ADMIN_CHAT_ID  = int(os.getenv("CHAT_ID_ADMIN", 5678))

SUPERADMINS = [int(uid.strip()) for uid in os.getenv("SUPERADMINS", "").split(",") if uid.strip()]
MODERATORS  = [int(uid.strip()) for uid in os.getenv("MODERATORS", "").split(",") if uid.strip()]
ADMINS = {uid: "superadmin" for uid in SUPERADMINS}
ADMINS.update({uid: "moderator" for uid in MODERATORS})

# Redis
REDIS_HOST     = os.getenv("REDIS_HOST", "isi_host_anda")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "token_hanaya")

# Keys
KEY_SENT        = "hanaya:sent"
KEY_PENDING     = "hanaya:pending"
KEY_DAILY_COUNT = "hanaya:daily_count"
KEY_DAILY_DATE  = "hanaya:daily_date"
KEY_FLOOD_CTRL  = "hanaya:flood_ctrl"

# Limits
DAILY_LIMIT = 1500
GROUP_SIZE  = 5
MAX_QUEUE_SIZE = 10000
AUTO_SAVE_INTERVAL = 60
WAIT_TIME = 20
MAX_RETRIES = 2

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("bot.log", encoding="utf-8"), logging.StreamHandler()]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# Global state
pending_media    = []
is_sending       = False
daily_count      = 0
daily_reset_date = datetime.now(timezone.utc).date()
last_save_time   = datetime.now(timezone.utc)
is_paused        = False
local_sent       = set()
