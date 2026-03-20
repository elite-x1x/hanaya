import logging
import asyncio
from telegram import InputMediaVideo
from telegram.ext import MessageHandler, filters
from duplicate_checker import get_fingerprint, is_duplicate, mark_sent
from state_manager import pending_media, save_pending, daily_count, save_daily
from flood_control import SmartFloodController

flood_ctrl: SmartFloodController | None = None

async def handle_video(update, context):
    """Tangani pesan video, cek duplikat, lalu masukkan ke pending queue"""
    msg = update.message
    fingerprint = get_fingerprint(msg)
    if not fingerprint:
        return

    if await is_duplicate(fingerprint, pending_snapshot=pending_media):
        logging.info("♻️ Video duplikat, dilewati")
        return

    pending_media.append((fingerprint['file_id'], msg.chat_id, fingerprint))
    await save_pending()
    logging.info(f"📥 Video ditambahkan ke pending queue ({len(pending_media)} total)")

    # Update daily count
    global daily_count
    daily_count += 1
    await save_daily()

async def send_media_group_with_retry(bot, chat_id: int, media_items: list, timeout: int = 30) -> bool:
    """Kirim media group dengan retry dan flood control"""
    global flood_ctrl
    for attempt in range(3):
        try:
            if not media_items:
                return False
            input_media = [InputMediaVideo(media=fid) for fid, _ in media_items]
            await asyncio.wait_for(bot.send_media_group(chat_id=chat_id, media=input_media), timeout=timeout)
            if flood_ctrl:
                await flood_ctrl.record_success()
            return True
        except Exception as e:
            err = str(e)
            logging.error(f"❌ Error kirim media group: {err}")
            if "Flood control" in err or "Too Many Requests" in err:
                if flood_ctrl:
                    wait_time = await flood_ctrl.record_flood(30)
                    await asyncio.sleep(wait_time)
            else:
                if attempt == 2:
                    return False
    return False

def get_handler():
    """Return handler untuk video messages"""
    return MessageHandler(filters.VIDEO, handle_video)
