import asyncio
import logging
from telegram.ext import ApplicationBuilder

from config import BOT_TOKEN
from logging_utils import setup_logging
from redis_utils import connect_redis_async
from flood_control import SmartFloodController
from state_manager import load_all, save_all
from handlers import media_handler, admin_handler

async def main():
    # Setup logging
    setup_logging()
    logging.info("🚀 Bot starting...")

    # Koneksi Redis
    await connect_redis_async()

    # Load state dari Redis
    await load_all()

    # Inisialisasi flood controller
    flood_ctrl = SmartFloodController()
    logging.info("✅ Flood controller initialized")

    # Build aplikasi Telegram
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Register handlers
    app.add_handler(media_handler.get_handler())
    app.add_handler(admin_handler.get_handler())

    logging.info("🤖 Bot is running...")
    try:
        await app.run_polling()
    finally:
        # Simpan state saat bot berhenti
        await save_all()
        logging.info("💾 State disimpan sebelum shutdown")

if __name__ == "__main__":
    asyncio.run(main())
