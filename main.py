import asyncio
import logging
import signal
import sys
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, ContextTypes,
    MessageHandler, CommandHandler, filters
)

from logger import *
from config import BOT_TOKEN, TARGET_CHAT_ID, DAILY_LIMIT, GROUP_SIZE, MAX_QUEUE_SIZE
from redis_client import connect_redis_async
from core import (
    load_all, save_all, queue_worker,
    initialize_locks
)
from handlers import (
    forward_media,
    cmd_status, cmd_pause, cmd_resume,
    cmd_flushpending, cmd_resetdaily,
    cmd_setlimit, cmd_setdelay, cmd_shutdown
)

# ============================================================
# === STARTUP / SHUTDOWN ===
# ============================================================
async def on_startup(app):
    try:
        await connect_redis_async()
        await initialize_locks()
        await load_all()
        asyncio.create_task(queue_worker(app.bot))
        logging.info("🚀 Bot siap!")
    except Exception as e:
        logging.error(f"❌ Error saat startup: {e}", exc_info=True)
        raise

async def on_shutdown(app):
    try:
        await save_all()
        logging.info("🛑 Bot berhenti, data tersimpan")
    except Exception as e:
        logging.error(f"❌ Error saat shutdown: {e}", exc_info=True)

# ============================================================
# === SIGNAL HANDLER ===
# ============================================================
def handle_shutdown(signum, frame):
    logging.info("⚠️ Shutdown signal diterima, menyimpan data...")
    try:
        loop = asyncio.get_running_loop()
        asyncio.create_task(_async_shutdown())
    except RuntimeError:
        logging.info("✅ Data tersimpan, bot berhenti")
        sys.exit(0)

async def _async_shutdown():
    try:
        await save_all()
        logging.info("✅ Data tersimpan, bot berhenti")
    except Exception as e:
        logging.error(f"❌ Error saat save_all(): {e}")
    finally:
        try:
            loop = asyncio.get_running_loop()
            loop.stop()
        except RuntimeError:
            sys.exit(0)

# ============================================================
# === ERROR HANDLER ===
# ============================================================
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    error = context.error
    if error is None:
        return

    err_str = str(error)

    # Abaikan error koneksi yang tidak kritis
    ignored_errors = [
        "httpx.ReadError",
        "httpx.ConnectError",
        "No address associated with hostname",
        "ConnectionError",
        "TimeoutError",
        "NetworkError",
    ]
    if any(e in err_str for e in ignored_errors):
        logging.warning(f"⚠️ Network error (ignored): {err_str[:100]}")
        return

    logging.error(
        f"❌ Update caused error:\n"
        f"   Update: {update}\n"
        f"   Error : {error}",
        exc_info=error if isinstance(error, Exception) else None
    )

# ============================================================
# === MAIN ===
# ============================================================
def main():
    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    if not BOT_TOKEN:
        logging.error("❌ BOT_TOKEN kosong! Periksa file .env")
        sys.exit(1)

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .connect_timeout(20)
        .read_timeout(20)
        .write_timeout(20)
        .pool_timeout(20)
        .build()
    )

    # Error handler
    app.add_error_handler(error_handler)

    # Media handler
    app.add_handler(MessageHandler(filters.VIDEO, forward_media))

    # Command handlers
    app.add_handler(CommandHandler("status",       cmd_status))
    app.add_handler(CommandHandler("pause",        cmd_pause))
    app.add_handler(CommandHandler("resume",       cmd_resume))
    app.add_handler(CommandHandler("flushpending", cmd_flushpending))
    app.add_handler(CommandHandler("resetdaily",   cmd_resetdaily))
    app.add_handler(CommandHandler("setlimit",     cmd_setlimit))
    app.add_handler(CommandHandler("setdelay",     cmd_setdelay))
    app.add_handler(CommandHandler("shutdown",     cmd_shutdown))

    logging.info("╔══════════════════════════════════╗")
    logging.info("║ 🌸 HANAYA BOT v4.7 (Smart Flood) ║")
    logging.info(f"║  Daily Limit : {DAILY_LIMIT:<6} Media/hari   ║")
    logging.info(f"║  Group Size  : {GROUP_SIZE:<6} Media/kelompok║")
    logging.info(f"║  Max Pending : {MAX_QUEUE_SIZE:<6} Media        ║")
    logging.info("╚══════════════════════════════════╝")

    try:
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            timeout=30,
            #reconnect_timeout=10,
        )
    except KeyboardInterrupt:
        logging.info("⚠️ Keyboard interrupt diterima")
    except Exception as e:
        logging.error(f"❌ Fatal error: {e}", exc_info=True)
    finally:
        logging.info("✅ Bot shutdown complete")

if __name__ == "__main__":
    main()