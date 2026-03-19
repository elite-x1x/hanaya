import signal, sys, logging
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, CommandHandler
from .config import BOT_TOKEN
from .admin import cmd_status, cmd_pause, cmd_resume, cmd_flushpending, cmd_resetdaily, cmd_setlimit, cmd_setdelay
from .worker import queue_worker
from .redis_helpers import connect_redis

def handle_shutdown(signum, frame):
    logging.info("⚠️ Shutdown signal diterima, menyimpan data...")
    sys.exit(0)

# ============================================================
# === MAIN (lanjutan)
# ============================================================
def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(lambda app: connect_redis())
        .build()
    )

    # Handler video masuk
    from .worker import forward_media
    app.add_handler(MessageHandler(filters.VIDEO, forward_media))

    # Admin commands
    from .admin import (
        cmd_status, cmd_pause, cmd_resume,
        cmd_flushpending, cmd_resetdaily,
        cmd_setlimit, cmd_setdelay
    )
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("flushpending", cmd_flushpending))
    app.add_handler(CommandHandler("resetdaily", cmd_resetdaily))
    app.add_handler(CommandHandler("setlimit", cmd_setlimit))
    app.add_handler(CommandHandler("setdelay", cmd_setdelay))

    logging.info("╔══════════════════════════════════╗")
    logging.info("║ 🌸 HANAYA BOT v3.1 (Modular)      ║")
    logging.info(f"║  Daily Limit : {DAILY_LIMIT} video/hari   ║")
    logging.info(f"║  Group Size  : {GROUP_SIZE} video/kelompok  ║")
    logging.info(f"║  Max Pending : {MAX_QUEUE_SIZE} video       ║")
    logging.info("╚══════════════════════════════════╝")

    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
