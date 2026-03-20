import asyncio
import logging
from telegram.ext import ApplicationBuilder

from config import BOT_TOKEN
from logging_utils import setup_logging
from redis_utils import connect_redis_async
from flood_control import SmartFloodController
# nanti kita bisa tambah: from handlers import media_handler, admin_handler, dll

async def main():
    # Setup logging modular
    setup_logging()
    logging.info("🚀 Bot starting...")

    # Koneksi Redis
    await connect_redis_async()

    # Inisialisasi flood controller
    flood_ctrl = SmartFloodController()
    logging.info("✅ Flood controller initialized")

    # Build aplikasi Telegram
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # TODO: register handlers di sini
    # app.add_handler(media_handler.get_handler())
    # app.add_handler(admin_handler.get_handler())

    logging.info("🤖 Bot is running...")
    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
