import asyncio, logging, random
from telegram import InputMediaVideo, Update
from telegram.ext import ContextTypes
from datetime import datetime, timezone
from .config import (
    pending_media, is_sending, daily_count, last_save_time,
    WAIT_TIME, AUTO_SAVE_INTERVAL, DAILY_LIMIT, GROUP_SIZE,
    DELAY_BETWEEN_SEND, MAX_QUEUE_SIZE, MAX_RETRIES,
    KEY_PENDING, KEY_DAILY_COUNT, KEY_DAILY_DATE,
    TARGET_CHAT_ID, ADMIN_CHAT_ID, sending_lock, is_paused
)
from .utils import is_duplicate, mark_sent
from .redis_helpers import r_set, redis_client
from .flood import SmartFloodController

flood_ctrl = SmartFloodController()

# ============================================================
# === SEND MEDIA GROUP DENGAN RETRY
# ============================================================
async def send_media_group_with_retry(bot, chat_id, media_items, admin_chat_id=None, timeout=30):
    global flood_ctrl
    for attempt in range(MAX_RETRIES):
        try:
            if not media_items: return False
            input_media = [InputMediaVideo(media=fid) for fid, _ in media_items]
            await asyncio.wait_for(bot.send_media_group(chat_id=chat_id, media=input_media), timeout=timeout)
            flood_ctrl.record_success()
            return True
        except Exception as e:
            err = str(e)
            if "Flood control" in err or "Too Many Requests" in err:
                suggested = 60
                try: suggested = int(err.split("Retry in ")[-1].split(" ")[0])
                except: pass
                total_wait = flood_ctrl.record_flood(suggested)
                if flood_ctrl.flood_count >= 3 and admin_chat_id != 0:
                    try:
                        await bot.send_message(chat_id=admin_chat_id, text=f"⚠️ FLOOD ALERT\nCount:{flood_ctrl.flood_count} | Wait:{total_wait:.0f}s")
                    except: pass
                await asyncio.sleep(total_wait)
            else:
                wait = (2**attempt)*5 + random.uniform(5,20)
                logging.error(f"❌ Error (attempt {attempt+1}/{MAX_RETRIES}): {err[:150]} | Tunggu {wait:.1f}s")
                await asyncio.sleep(wait)
                if attempt == MAX_RETRIES-1: return False
    return False

# ============================================================
# === QUEUE WORKER
# ============================================================
async def queue_worker(bot):
    global pending_media, is_sending, daily_count, last_save_time
    while True:
        await asyncio.sleep(WAIT_TIME)
        now = datetime.now(timezone.utc)
        if (now - last_save_time).seconds >= AUTO_SAVE_INTERVAL:
            r_set(KEY_PENDING, str(pending_media))
            r_set(KEY_DAILY_COUNT, str(daily_count))
            r_set(KEY_DAILY_DATE, str(datetime.now(timezone.utc).date()))
            last_save_time = now

        if is_paused:
            logging.info("⏸️ Worker dijeda, menunggu resume...")
            continue
        if not pending_media or is_sending: continue
        if daily_count >= DAILY_LIMIT:
            logging.warning("⛔ Daily limit tercapai")
            continue

        if len(pending_media) > MAX_QUEUE_SIZE:
            overflow = len(pending_media) - int(MAX_QUEUE_SIZE*0.95)
            pending_media = pending_media[overflow:]
            r_set(KEY_PENDING, str(pending_media))
            logging.warning(f"⚠️ Pending overflow, trimmed {overflow}")

        async with sending_lock: is_sending = True
        try:
            batch = pending_media.copy()
            pending_media.clear()
            r_set(KEY_PENDING, str(pending_media))
            total, sent_count = len(batch), 0
            logging.info(f"▶️ Mulai kirim {total} video | Daily:{daily_count}/{DAILY_LIMIT}")

            for i in range(0, total, GROUP_SIZE):
                if daily_count >= DAILY_LIMIT:
                    pending_media.extend(batch[i:])
                    r_set(KEY_PENDING, str(pending_media))
                    break
                group = batch[i:i+GROUP_SIZE]
                items = []
                for fid, mtype, fp in group:
                    if not is_duplicate(fp):
                        items.append((fid, mtype))
                        mark_sent(fp)
                if items:
                    success = await send_media_group_with_retry(bot, TARGET_CHAT_ID, items, ADMIN_CHAT_ID)
                    if success:
                        daily_count += len(items)
                        sent_count += len(items)
                        r_set(KEY_DAILY_COUNT, str(daily_count))
                        for fid, _, fp in group:
                            logging.info(f"✅ Sent: {fid[:8]}... | {fp['width']}x{fp['height']} | {fp['duration']}s")
                await asyncio.sleep(DELAY_BETWEEN_SEND + random.uniform(0.5,2.0))
        except Exception as e:
            logging.error(f"❌ Worker error: {e}")
        finally:
            async with sending_lock: is_sending = False
            total_sent = redis_client.scard(KEY_SENT) if redis_client else 0
            logging.info(f"✅ Worker selesai | Sent:{sent_count}/{total} | Daily:{daily_count}/{DAILY_LIMIT} | Total Sent:{total_sent} | Pending:{len(pending_media)}")

# ============================================================
# === FORWARD MEDIA HANDLER
# ============================================================
async def forward_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.video: return
    fp = {"file_id": msg.video.file_id, "type":"video", "width":msg.video.width, "height":msg.video.height, "duration":msg.video.duration, "file_size":msg.video.file_size}
    if is_duplicate(fp):
        logging.info(f"❌ Duplikat: {fp['file_id'][:8]}...")
        return
    async with sending_lock:
        pending_media.append((fp["file_id"], fp["type"], fp))
        r_set(KEY_PENDING, str(pending_media))
    logging.info(f"⏳ Pending: {fp['file_id'][:8]}... | {fp['width']}x{fp['height']} | {fp['duration']}s")
