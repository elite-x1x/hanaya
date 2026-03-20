import logging
import json
import asyncio
from datetime import datetime, timezone
from redis_utils import r_get, r_set, r_get_json, r_set_json
from flood_control import SmartFloodController
from config import DAILY_LIMIT, DELAY_BETWEEN_SEND

# Redis keys
KEY_PENDING     = "hanaya:pending"
KEY_DAILY_COUNT = "hanaya:daily_count"
KEY_DAILY_DATE  = "hanaya:daily_date"
KEY_FLOOD_CTRL  = "hanaya:flood_ctrl"
KEY_DAILY_LIMIT = "hanaya:daily_limit"
KEY_SEND_DELAY  = "hanaya:send_delay"

# Global state
pending_media: list = []
daily_count: int = 0
daily_reset_date = datetime.now(timezone.utc).date()
flood_ctrl: SmartFloodController | None = None

async def load_all() -> None:
    """Load semua state dari Redis"""
    global pending_media, daily_count, daily_reset_date, flood_ctrl

    # Load pending media
    try:
        raw = await r_get(KEY_PENDING)
        pending_media = json.loads(raw) if raw else []
    except Exception as e:
        logging.error(f"❌ Gagal load pending: {e}")
        pending_media = []

    # Load daily count
    try:
        count_str = await r_get(KEY_DAILY_COUNT)
        date_str = await r_get(KEY_DAILY_DATE)
        today = datetime.now(timezone.utc).date()
        saved_date = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else today
        daily_count = int(count_str) if count_str and saved_date == today else 0
        daily_reset_date = today
    except Exception as e:
        logging.error(f"❌ Gagal load daily count: {e}")
        daily_count = 0
        daily_reset_date = datetime.now(timezone.utc).date()

    # Load flood controller
    try:
        flood_data = await r_get_json(KEY_FLOOD_CTRL)
        flood_ctrl = SmartFloodController.from_dict(flood_data) if flood_data else SmartFloodController()
        logging.info("🔄 Flood control state loaded")
    except Exception as e:
        logging.warning(f"⚠️ Gagal load flood control: {e}")
        flood_ctrl = SmartFloodController()

    # Load daily limit
    try:
        limit_str = await r_get(KEY_DAILY_LIMIT)
        if limit_str:
            global DAILY_LIMIT
            DAILY_LIMIT = int(limit_str)
    except Exception as e:
        logging.warning(f"⚠️ Gagal load daily limit: {e}")

    # Load send delay
    try:
        delay_str = await r_get(KEY_SEND_DELAY)
        if delay_str:
            global DELAY_BETWEEN_SEND
            DELAY_BETWEEN_SEND = float(delay_str)
    except Exception as e:
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
