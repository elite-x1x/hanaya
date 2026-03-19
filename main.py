import signal
import sys
import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, CommandHandler

# Import modul internal
from .config import BOT_TOKEN, DAILY_LIMIT, GROUP_SIZE, MAX_QUEUE_SIZE
from .redis_helpers import connect_redis
from .worker import forward_media, queue_worker
from .admin import (
    cmd_status, cmd_pause, cmd_resume,
    cmd_flushpending, cmd_resetdaily,
    cmd_setlimit, cmd_setdelay
)

# ============================================================
# === SIGNAL HANDLER
# ============================================================
def handle_shutdown(signum, frame):
    logging.info("⚠️ Shutdown signal diterima, menyimpan data...")
    sys.exit(0)

# ============================================================
# === MAIN ENTRY POINT
# ============================================================
def main():
    # Setup signal
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Build aplikasi Telegram
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(lambda app: connect_redis())
        .build()
    )

    # Handler video masuk
    app.add_handler(MessageHandler(filters.VIDEO, forward_media))

    # Admin commands
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("flushpending", cmd_flushpending))
    app.add_handler(CommandHandler("resetdaily", cmd_resetdaily))
    app.add_handler(CommandHandler("setlimit", cmd_setlimit))
    app.add_handler(CommandHandler("setdelay", cmd_setdelay))

    # Logging banner
    logging.info("╔══════════════════════════════════╗")
    logging.info("║ 🌸 HANAYA BOT v3.1 (Modular)      ║")
    logging.info(f"║  Daily Limit : {DAILY_LIMIT} video/hari   ║")
    logging.info(f"║  Group Size  : {GROUP_SIZE} video/kelompok  ║")
    logging.info(f"║  Max Pending : {MAX_QUEUE_SIZE} video       ║")
    logging.info("╚══════════════════════════════════╝")

    # Jalankan polling
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
