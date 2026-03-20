import logging
import json
import os
import asyncio
import random
import hashlib
from datetime import datetime, timezone
import signal
import sys

from telegram import Update, InputMediaVideo
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
from dotenv import load_dotenv
import redis

load_dotenv()

# ============================================================
# === KONFIGURASI UTAMA ===
# ============================================================
BOT_TOKEN      = os.getenv("BOT_F_LOKAL", "xxxx")
TARGET_CHAT_ID = int(os.getenv("CHAT_ID_BEDUL", 1234))
ADMIN_CHAT_ID  = int(os.getenv("CHAT_ID_ADMIN", 5678))

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN tidak diatur di .env")
if TARGET_CHAT_ID == 0:
    raise ValueError("❌ CHAT_ID_TARGET tidak diatur di .env")
if ADMIN_CHAT_ID == 0:
    logging.warning("⚠️ ADMIN_CHAT_ID tidak diatur — alert flood akan dinonaktifkan")

# ============================================================
# === REDIS CONFIG ===
# ============================================================
REDIS_HOST     = os.getenv("REDIS_HOST", "isi_host_anda")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "token_hanaya")

# ============================================================
# === REDIS KEYS ===
# ============================================================
KEY_SENT       = "hanaya:sent"
KEY_PENDING    = "hanaya:pending"
KEY_FAILED     = "hanaya:failed"
KEY_DAILY_COUNT = "hanaya:daily_count"
KEY_DAILY_DATE  = "hanaya:daily_date"
KEY_FLOOD_CTRL  = "hanaya:flood_ctrl"

# ============================================================
# === TTL CONFIG ===
# ============================================================
SENT_TTL = 60 * 60 * 24 * 30  # 30 hari

# ============================================================
# === RATE LIMIT SETTINGS ===
# ============================================================
DELAY_BETWEEN_SEND      = 2.0
DELAY_RANDOM_MIN        = 0.5
DELAY_RANDOM_MAX        = 2.0
GROUP_SIZE              = 5
DELAY_BETWEEN_GROUP_MIN = 20
DELAY_BETWEEN_GROUP_MAX = 40
WAIT_TIME               = 20
BATCH_PAUSE_EVERY       = 30
BATCH_PAUSE_MIN         = 300
BATCH_PAUSE_MAX         = 600
DAILY_LIMIT             = 1500
MAX_RETRIES             = 2
MAX_QUEUE_SIZE          = 10000
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
# === SETUP LOGGING ===
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# ============================================================
# === GLOBAL STATE ===
# ============================================================
pending_media    = []
is_sending       = False
daily_count      = 0
daily_reset_date = datetime.now(timezone.utc).date()
last_save_time   = datetime.now(timezone.utc)
sending_lock     = asyncio.Lock()
flood_ctrl       = None  # ← diinisialisasi di load_all()

# ============================================================
# === REDIS CONNECTION ===
# ============================================================
redis_client = None

def connect_redis():
    global redis_client
    for attempt in range(5):
        try:
            redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                ssl=True,
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=10
            )
            redis_client.ping()
            logging.info("✅ Redis terhubung!")
            return
        except Exception as e:
            logging.error(f"❌ Redis gagal (percobaan {attempt+1}/5): {e}")
            import time; time.sleep(5)
    logging.error("❌ Redis tidak bisa terhubung, fallback ke lokal")
    redis_client = None

def ensure_redis():
    global redis_client
    try:
        redis_client.ping()
    except Exception:
        logging.warning("⚠️ Redis terputus, mencoba reconnect...")
        connect_redis()

# ============================================================
# === REDIS HELPERS ===
# ============================================================
def r_get(key):
    try:
        ensure_redis()
        if redis_client:
            return redis_client.get(key)
        return None
    except Exception as e:
        logging.error(f"Redis GET error [{key}]: {e}")
        return None

def r_set(key, value):
    try:
        ensure_redis()
        if redis_client:
            redis_client.set(key, value)
    except Exception as e:
        logging.error(f"Redis SET error [{key}]: {e}")

def r_sismember(key, value):
    try:
        ensure_redis()
        return redis_client.sismember(key, value) if redis_client else False
    except Exception as e:
        logging.error(f"Redis SISMEMBER error [{key}]: {e}")
        return False

def r_sadd_with_ttl(key, value, ttl):
    try:
        ensure_redis()
        if redis_client:
            pipe = redis_client.pipeline()
            pipe.sadd(key, value)
            pipe.expire(key, ttl)
            pipe.execute()
    except Exception as e:
        logging.error(f"Redis SADD+TTL error [{key}]: {e}")

def r_set_json(key, data):
    try:
        ensure_redis()
        if redis_client:
            redis_client.set(key, json.dumps(data))
    except Exception as e:
        logging.error(f"Redis SET JSON error [{key}]: {e}")

def r_get_json(key):
    try:
        ensure_redis()
        if redis_client:
            data = redis_client.get(key)
            if data:
                return json.loads(data)
    except Exception as e:
        logging.error(f"Redis GET JSON error [{key}]: {e}")
    return None

# ============================================================
# === MAKE HASH ===
# ============================================================
def make_hash(fingerprint):
    fp_copy = {k: v for k, v in fingerprint.items() if k != 'file_id'}
    return hashlib.md5(
        json.dumps(fp_copy, sort_keys=True).encode()
    ).hexdigest()

# ============================================================
# === LOAD & SAVE ===
# ============================================================
def load_all():
    global pending_media, daily_count, daily_reset_date, flood_ctrl

    try:
        raw = r_get(KEY_PENDING)
        pending_media = json.loads(raw) if raw else []
    except Exception as e:
        logging.error(f"❌ Gagal load pending: {e}")
        pending_media = []

    try:
        count_str  = r_get(KEY_DAILY_COUNT)
        date_str   = r_get(KEY_DAILY_DATE)
        today      = datetime.now(timezone.utc).date()
        saved_date = (
            datetime.strptime(date_str, "%Y-%m-%d").date()
            if date_str else today
        )
        daily_count      = int(count_str) if count_str and saved_date == today else 0
        daily_reset_date = today
    except Exception as e:
        logging.error(f"❌ Gagal load daily count: {e}")
        daily_count      = 0
        daily_reset_date = datetime.now(timezone.utc).date()

    # Load flood controller state dari Redis
    try:
        flood_data = r_get_json(KEY_FLOOD_CTRL)
        if flood_data:
            flood_ctrl = SmartFloodController()
            # Validasi last_flood_time agar tidak error
            if isinstance(flood_data.get("last_flood_time"), str):
                try:
                    flood_data["last_flood_time"] = datetime.fromisoformat(flood_data["last_flood_time"])
                except Exception:
                    flood_data["last_flood_time"] = None
            flood_ctrl.__dict__.update(flood_data)
            logging.info("🔄 Flood control state loaded dari Redis")
        else:
            flood_ctrl = SmartFloodController()
    except Exception as e:
        logging.warning(f"⚠️ Gagal load flood control: {e}")
        flood_ctrl = SmartFloodController()

    logging.info(
        f"📂 Data dimuat | Pending: {len(pending_media)} | "
        f"Daily: {daily_count}/{DAILY_LIMIT}"
    )

def save_pending():
    try:
        r_set(KEY_PENDING, json.dumps(pending_media))
    except Exception as e:
        logging.error(f"❌ Gagal save pending: {e}")

def save_daily():
    try:
        r_set(KEY_DAILY_COUNT, str(daily_count))
        r_set(KEY_DAILY_DATE, str(daily_reset_date))
    except Exception as e:
        logging.error(f"❌ Gagal save daily: {e}")

def save_flood_ctrl():
    try:
        if flood_ctrl:
            r_set_json(KEY_FLOOD_CTRL, flood_ctrl.__dict__)
    except Exception as e:
        logging.error(f"❌ Gagal save flood ctrl: {e}")

def save_all():
    save_pending()
    save_daily()
    save_flood_ctrl()

# ============================================================
# === DUPLIKAT CHECK ===
# ============================================================
def get_fingerprint(msg):
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

def is_duplicate(fingerprint):
    if not fingerprint:
        return False

    file_id = fingerprint.get('file_id', '')
    fp_hash = make_hash(fingerprint)

    # --- Lapis 1: Cek Redis SENT ---
    if redis_client:
        try:
            if r_sismember(KEY_SENT, file_id):
                logging.debug(f"✅ Duplikat (file_id) di Redis: {file_id[:8]}...")
                return True
            if r_sismember(KEY_SENT, fp_hash):
                logging.debug(f"✅ Duplikat (hash) di Redis: {fp_hash[:8]}...")
                return True
        except Exception as e:
            logging.warning(f"⚠️ Redis check error: {e}")

    # --- Lapis 2: Cek pending lokal ---
    for item in pending_media:
        q_file_id, _, q_fp = item
        if q_file_id == file_id or make_hash(q_fp) == fp_hash:
            return True

    return False

def mark_sent(fingerprint):
    if not fingerprint:
        return False

    file_id = fingerprint.get('file_id', '')
    fp_hash = make_hash(fingerprint)
    success = True

    try:
        r_sadd_with_ttl(KEY_SENT, file_id, SENT_TTL)
        r_sadd_with_ttl(KEY_SENT, fp_hash, SENT_TTL)
        logging.debug(f"✅ Marked sent: {file_id[:8]}...")
    except Exception as e:
        logging.error(f"❌ Gagal mark sent: {e}")
        success = False

    return success

# ============================================================
# === DAILY RESET ===
# ============================================================
def check_daily_reset():
    global daily_count, daily_reset_date
    today = datetime.now(timezone.utc).date()
    if today != daily_reset_date:
        logging.info(f"🔄 Reset harian | Kemarin terkirim: {daily_count}")
        daily_count      = 0
        daily_reset_date = today

# ============================================================
# === SMART FLOOD CONTROLLER ===
# ============================================================
class SmartFloodController:
    def __init__(self):
        self.flood_count        = 0
        self.total_flood        = 0
        self.last_flood_time    = None
        self.penalty            = 0
        self.is_cooling         = False
        self.group_delay_min    = DELAY_BETWEEN_GROUP_MIN
        self.group_delay_max    = DELAY_BETWEEN_GROUP_MAX

    def record_flood(self, suggested_wait: int) -> float:
        now = datetime.now(timezone.utc)

        if self.last_flood_time:
            if isinstance(self.last_flood_time, str):
                try:
                    self.last_flood_time = datetime.fromisoformat(self.last_flood_time)
                except Exception:
                    self.last_flood_time = None

            if self.last_flood_time:
                elapsed = (now - self.last_flood_time).total_seconds()
                if elapsed > FLOOD_RESET_AFTER:
                    logging.info(
                        f"🔄 Flood counter direset "
                        f"(tidak ada flood selama {elapsed:.0f}s)"
                    )
                    self.flood_count = 0
                    self.penalty     = 0

        self.flood_count     += 1
        self.total_flood     += 1
        self.last_flood_time   = now
        self.is_cooling         = True

        self.penalty = min(FLOOD_MAX_PENALTY, self.flood_count * FLOOD_PENALTY_STEP)

        random_add  = random.uniform(FLOOD_RANDOM_MIN, FLOOD_RANDOM_MAX)
        total_wait  = suggested_wait + random_add + self.penalty

        self.group_delay_min = min(120, DELAY_BETWEEN_GROUP_MIN + (self.flood_count * 5))
        self.group_delay_max = min(180, DELAY_BETWEEN_GROUP_MAX + (self.flood_count * 10))

        logging.warning(
            f"🚨 FLOOD #{self.flood_count} terdeteksi!\n"
            f"   ├─ Saran Telegram  : {suggested_wait}s\n"
            f"   ├─ Random tambahan : {random_add:.1f}s\n"
            f"   ├─ Penalti kumulatif: {self.penalty:.0f}s\n"
            f"   ├─ Total tunggu    : {total_wait:.1f}s\n"
            f"   └─ Delay group baru: {self.group_delay_min:.0f}s - {self.group_delay_max:.0f}s"
        )

        return total_wait

    def record_success(self):
        if self.penalty > 0:
            self.penalty = max(0, self.penalty - 5)

        if self.flood_count > 0:
            self.flood_count = max(0, self.flood_count - 1)

        if self.group_delay_min > DELAY_BETWEEN_GROUP_MIN:
            self.group_delay_min = max(DELAY_BETWEEN_GROUP_MIN, self.group_delay_min - 2)
        if self.group_delay_max > DELAY_BETWEEN_GROUP_MAX:
            self.group_delay_max = max(DELAY_BETWEEN_GROUP_MAX, self.group_delay_max - 3)

        self.is_cooling = False

    def get_group_delay(self) -> float:
        return random.uniform(self.group_delay_min, self.group_delay_max)

    def get_status(self) -> str:
        return (
            f"FloodCtrl | Count: {self.flood_count} | "
            f"Total: {self.total_flood} | "
            f"Penalty: {self.penalty:.0f}s | "
            f"Delay: {self.group_delay_min:.0f}-{self.group_delay_max:.0f}s"
        )

    def to_dict(self):
        return {
            'flood_count': self.flood_count,
            'total_flood': self.total_flood,
            'last_flood_time': self.last_flood_time.isoformat() if self.last_flood_time else None,
            'penalty': self.penalty,
            'is_cooling': self.is_cooling,
            'group_delay_min': self.group_delay_min,
            'group_delay_max': self.group_delay_max
        }

# Inisialisasi global — akan di-override di load_all()
flood_ctrl = None

# ============================================================
# === KIRIM MEDIA GROUP ===
# ============================================================
async def send_media_group_with_retry(bot, chat_id, media_items, admin_chat_id=None, timeout=30):
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

            flood_ctrl.record_success()
            return True

        except asyncio.TimeoutError:
            logging.error(f"❌ Timeout saat mengirim grup media (attempt {attempt+1}/{MAX_RETRIES})")
            if attempt == MAX_RETRIES - 1:
                return False

        except Exception as e:
            err = str(e)

            if "Flood control" in err or "Too Many Requests" in err:
                try:
                    suggested = int(err.split("Retry in ")[-1].split(" ")[0])
                except Exception:
                    suggested = 60

                total_wait = flood_ctrl.record_flood(suggested)

                # Hanya kirim alert jika admin_chat_id valid
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
                logging.error(f"❌ Akses ditolak ke chat {chat_id} — bot mungkin diblokir")
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

    while True:
        await asyncio.sleep(WAIT_TIME)

        # Auto save berkala
        now = datetime.now(timezone.utc)
        if (now - last_save_time).seconds >= AUTO_SAVE_INTERVAL:
            save_all()
            last_save_time = now

        check_daily_reset()

        if not pending_media or is_sending:
            continue

        if daily_count >= DAILY_LIMIT:
            logging.warning(f"⛔ Daily limit tercapai ({DAILY_LIMIT})")
            continue

        # Trim pending jika terlalu besar
        if len(pending_media) > MAX_QUEUE_SIZE:
            overflow = len(pending_media) - int(MAX_QUEUE_SIZE * 0.95)
            removed = pending_media[:overflow]
            pending_media = pending_media[overflow:]
            save_pending()
            logging.warning(f"⚠️ Pending overflow: removed {len(removed)} items")

        async with sending_lock:
            is_sending = True

        try:
            batch = pending_media.copy()
            pending_media.clear()
            save_pending()

            total      = len(batch)
            sent_count = 0

            logging.info(f"▶️ Mulai kirim {total} video | Daily: {daily_count}/{DAILY_LIMIT}")

            for i in range(0, total, GROUP_SIZE):
                check_daily_reset()

                if daily_count >= DAILY_LIMIT:
                    pending_media.extend(batch[i:])
                    save_pending()
                    logging.warning(f"⛔ Limit tercapai, {total - i} video ditunda")
                    break

                group      = batch[i:i + GROUP_SIZE]
                group_num  = (i // GROUP_SIZE) + 1
                total_grp  = (total + GROUP_SIZE - 1) // GROUP_SIZE

                items = []
                for file_id, media_type, fp in group:
                    if not is_duplicate(fp):
                        items.append((file_id, media_type))
                        if not mark_sent(fp):
                            logging.warning(f"⚠️ Gagal menandai {file_id} sebagai sent")
                    else:
                        logging.debug(f"ℹ️ Skip duplicate: {file_id[:8]}...")

                if items:
                    success = await send_media_group_with_retry(
                        bot, TARGET_CHAT_ID, items, ADMIN_CHAT_ID
                    )
                    if success:
                        daily_count += len(items)
                        sent_count  += len(items)
                        save_daily()
                        for file_id, _, fp in group:
                            logging.info(f"✅ Terkirim: {file_id[:8]}... | {fp['width']}x{fp['height']} | {fp['duration']}s")
                    else:
                        logging.error(f"❌ Gagal kirim group {group_num}/{total_grp}")
                        # Kembalikan ke pending
                        pending_media.extend(group)
                        save_pending()

                delay = DELAY_BETWEEN_SEND + random.uniform(DELAY_RANDOM_MIN, DELAY_RANDOM_MAX)
                await asyncio.sleep(delay)

                if sent_count > 0 and sent_count % BATCH_PAUSE_EVERY == 0:
                    pause = random.uniform(BATCH_PAUSE_MIN, BATCH_PAUSE_MAX)
                    logging.info(f"⏸️ Jeda panjang {pause:.0f}s setelah {sent_count} video")
                    await asyncio.sleep(pause)
                else:
                    group_delay = flood_ctrl.get_group_delay()
                    logging.info(f"⏳ Jeda group: {group_delay:.1f}s | {flood_ctrl.get_status()}")
                    await asyncio.sleep(group_delay)

        except Exception as e:
            logging.error(f"❌ Error worker: {e}")
            remaining = batch[sent_count:]
            if remaining:
                pending_media.extend(remaining)
                save_pending()

        finally:
            async with sending_lock:
                is_sending = False
            save_all()
            total_sent = redis_client.scard(KEY_SENT) if redis_client else 0
            total_pending = len(pending_media)
            logging.info(
                f"✅ Selesai | Terkirim: {sent_count}/{total} | Daily: {daily_count}/{DAILY_LIMIT} | 📊 Total: Sent={total_sent} | Pending={total_pending}"
            )

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

    if is_duplicate(fp):
        logging.info(
            f"❌ Duplikat: {fp['file_id'][:8]}... | {fp['width']}x{fp['height']} | {fp['duration']}s"
        )
        return

    file_id    = fp.get('file_id')
    media_type = fp.get('type')

    # Simpan ke pending
    async with sending_lock:
        pending_media.append((file_id, media_type, fp))
        save_pending()
        logging.info(f"⏳ Pending: {file_id[:8]}... | {fp['width']}x{fp['height']} | {fp['duration']}s")

    # Notifikasi total
    total_pending = len(pending_media)
    total_sent = redis_client.scard(KEY_SENT) if redis_client else 0
    logging.info(f"📊 Total: Terkirim={total_sent} | Pending={total_pending} | Failed=0")

# ============================================================
# === STARTUP & SHUTDOWN ===
# ============================================================
async def on_startup(app):
    load_all()
    asyncio.create_task(queue_worker(app.bot))
    logging.info("🚀 Bot siap!")

async def on_shutdown(app):
    save_all()
    logging.info("🛑 Bot berhenti, data tersimpan")

# ============================================================
# === SIGNAL HANDLER ===
# ============================================================
def handle_shutdown(signum, frame):
    logging.info("⚠️ Shutdown signal diterima, menyimpan data...")
    save_all()
    logging.info("✅ Data tersimpan, bot berhenti")
    try:
        loop = asyncio.get_running_loop()
        loop.stop()
    except RuntimeError:
        sys.exit(0)

# ============================================================
# === ERROR HANDLER ===
# ============================================================
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.error(f"Update {update} caused error {context.error}")

# ============================================================
# === MAIN ===
# ============================================================
def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    connect_redis()

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    app.add_error_handler(error_handler)
    app.add_handler(MessageHandler(filters.VIDEO, forward_media))

    logging.info("╔══════════════════════════════════╗")
    logging.info("║ 🌸 HANAYA BOT v3.0 (Smart Flood) ║")
    logging.info(f"║  Daily Limit : {DAILY_LIMIT} Media/hari   ║")
    logging.info(f"║  Group Size  : {GROUP_SIZE} video/kelompok  ║")
    logging.info(f"║  Max Pending : {MAX_QUEUE_SIZE} Media       ║")
    logging.info("╚══════════════════════════════════╝")

    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )

if __name__ == "__main__":
    main()
