import asyncio
import hashlib
import json
import logging
import random
import re
from collections import OrderedDict
from datetime import datetime, timezone

from telegram import InputMediaVideo

import config as cfg
from config import (
    KEY_SENT, KEY_PENDING, KEY_DAILY_COUNT, KEY_DAILY_DATE,
    KEY_FLOOD_CTRL, KEY_DAILY_LIMIT, KEY_SEND_DELAY,
    SENT_TTL, MAX_RETRIES, GROUP_SIZE,
    DELAY_BETWEEN_SEND, DELAY_RANDOM_MIN, DELAY_RANDOM_MAX,
    DELAY_BETWEEN_GROUP_MIN, DELAY_BETWEEN_GROUP_MAX,
    BATCH_PAUSE_EVERY, BATCH_PAUSE_MIN, BATCH_PAUSE_MAX,
    DAILY_LIMIT, MAX_QUEUE_SIZE, AUTO_SAVE_INTERVAL,
    WAIT_TIME, FLOOD_WARN_THRESHOLD,
    TARGET_CHAT_ID, ADMIN_CHAT_ID
)
from redis_client import (
    redis_client, r_get, r_set, r_sismember,
    r_sadd_with_ttl, r_get_json, r_set_json
)
from flood_controller import SmartFloodController

# ============================================================
# === GLOBAL LOCKS & STATE ===
# ============================================================
sending_lock = None
config_lock = None

async def initialize_locks() -> None:
    """Inisialisasi semua lock async"""
    global sending_lock, config_lock
    sending_lock = asyncio.Lock()
    config_lock = asyncio.Lock()
    logging.info("✅ Locks initialized")

# ============================================================
# === GLOBAL STATE ===
# ============================================================
pending_media:    list        = []
is_sending:       bool        = False
daily_count:      int         = 0
daily_reset_date              = datetime.now(timezone.utc).date()
last_save_time                = datetime.now(timezone.utc)
is_paused:        bool        = False
local_sent:       OrderedDict = OrderedDict()
MAX_LOCAL_SENT                = 5000
flood_ctrl:       SmartFloodController | None = None

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
# === LOCAL SENT HELPERS ===
# ============================================================
def _local_sent_add(key: str, max_size: int = MAX_LOCAL_SENT) -> None:
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
        try:
            raw = await r_get(KEY_PENDING)
            if raw:
                pending_media = json.loads(raw)
            else:
                pending_media = []
        except (json.JSONDecodeError, TypeError) as e:
            logging.error(f"❌ Gagal load pending: {e}")
            pending_media = []

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

        try:
            limit_str = await r_get(KEY_DAILY_LIMIT)
            if limit_str:
                global DAILY_LIMIT
                DAILY_LIMIT = int(limit_str)
        except (ValueError, TypeError) as e:
            logging.warning(f"⚠️ Gagal load daily limit: {e}")

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
            f"Flood: {flood_ctrl.get_status() if flood_ctrl else 'N/A'}"
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