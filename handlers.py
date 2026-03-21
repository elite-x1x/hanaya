import asyncio
import logging
from datetime import datetime, timezone
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler, MessageHandler, filters

from config import (
    ADMINS, TARGET_CHAT_ID, ADMIN_CHAT_ID,
    KEY_SENT, DAILY_LIMIT, DELAY_BETWEEN_SEND
)
from core import (
    pending_media, is_paused, daily_count,
    flood_ctrl, sending_lock, config_lock, local_sent,
    get_fingerprint, is_duplicate, mark_sent, save_pending,
    load_all, save_all, check_daily_reset,
    save_daily, save_daily_limit, save_send_delay
)
from redis_client import redis_client, r_get, r_set, r_get_json, r_set_json

# ============================================================
# === ADMIN ROLE HELPERS ===
# ============================================================
def get_role(update: Update) -> str | None:
    if update.effective_user is None:
        return None
    role = ADMINS.get(update.effective_user.id)
    logging.debug(f"User {update.effective_user.id} role: {role}")
    return role

def is_superadmin(update: Update) -> bool:
    return get_role(update) == "superadmin"

def is_moderator(update: Update) -> bool:
    return get_role(update) in ["moderator", "superadmin"]

async def notify_admins(bot, text: str):
    for admin_id in ADMINS:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=f"[ADMIN ACTION] {text}",
                parse_mode="Markdown"
            )
        except Exception as e:
            logging.warning(f"⚠️ Gagal kirim notifikasi ke {admin_id}: {e}")

# ============================================================
# === FORWARD MEDIA ===
# ============================================================
async def forward_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    fp = get_fingerprint(msg)
    if not fp:
        return

    if await is_duplicate(fp):
        logging.info(
            f"❌ Duplikat: {fp['file_id'] [:8]}... | "
            f"{fp['width']}x{fp['height']} | {fp['duration']}s"
        )
        return

    file_id    = fp.get('file_id')
    media_type = fp.get('type')

    async with sending_lock:
        pending_media.append((file_id, media_type, fp))
        await save_pending()
        logging.info(
            f"⏳ Pending: {file_id[:8]}... | {fp['width']}x{fp['height']} | {fp['duration']}s"
        )

    try:
        if redis_client:
            total_sent = redis_client.scard(KEY_SENT)
        else:
            total_sent = len(local_sent)
    except Exception as e:
        logging.warning(f"⚠️ Gagal ambil total_sent: {e}")
        total_sent = len(local_sent)

    total_pending = len(pending_media)
    logging.info(f"📊 Total: Terkirim={total_sent} | Pending={total_pending} | Failed=0")

# ============================================================
# === ADMIN COMMANDS ===
# ============================================================
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_moderator(update):
        await update.message.reply_text("❌ Anda bukan moderator/admin")
        return
    try:
        total_sent = redis_client.scard(KEY_SENT) if redis_client else len(local_sent)
    except Exception as e:
        logging.warning(f"⚠️ Gagal ambil total_sent: {e}")
        total_sent = len(local_sent)
    
    total_pending = len(pending_media)
    text = (
        f"📊 STATUS BOT\n"
        f"├ Pending : {total_pending}\n"
        f"├ Sent    : {total_sent}\n"
        f"├ Daily   : {daily_count}/{DAILY_LIMIT}\n"
        f"├ Delay   : {DELAY_BETWEEN_SEND:.1f}s\n"
        f"└ Flood   : {flood_ctrl.get_status() if flood_ctrl else 'N/A'}"
    )
    await update.message.reply_text(text)

async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_moderator(update):
        await update.message.reply_text("❌ Anda bukan moderator/admin")
        return
    global is_paused
    is_paused = True
    msg = f"⏸️ Worker dijeda oleh {update.effective_user.first_name}"
    await update.message.reply_text(msg)
    await notify_admins(context.bot, msg)

async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_moderator(update):
        await update.message.reply_text("❌ Anda bukan moderator/admin")
        return
    global is_paused
    is_paused = False
    msg = f"▶️ Worker dilanjutkan oleh {update.effective_user.first_name}"
    await update.message.reply_text(msg)
    await notify_admins(context.bot, msg)

async def cmd_flushpending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa menghapus pending")
        return
    async with sending_lock:
        pending_media.clear()
    await save_pending()
    msg = f"🗑️ Pending queue dikosongkan oleh {update.effective_user.first_name}"
    await update.message.reply_text(msg)
    await notify_admins(context.bot, msg)

async def cmd_resetdaily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa reset daily")
        return
    async with sending_lock:
        # Gunakan fungsi dari core untuk reset
        await check_daily_reset()
    msg = f"🔄 Daily counter direset oleh {update.effective_user.first_name}"
    await update.message.reply_text(msg)
    await notify_admins(context.bot, msg)

def _flatten_arg(arg) -> str | None:
    """Flatten nested list/tuple to single value"""
    while isinstance(arg, (list, tuple)):
        if len(arg) == 0:
            return None
        arg = arg  # Ambil elemen pertama
    return str(arg).strip()

async def cmd_setlimit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa mengubah limit")
        return
    try:
        if not context.args:
            await update.message.reply_text("❌ Format salah. Contoh: /setlimit 1000")
            return

        arg_str = _flatten_arg(context.args)
        if arg_str is None:
            await update.message.reply_text("❌ Format salah. Contoh: /setlimit 1000")
            return

        new_limit = int(arg_str)
        if not (1 <= new_limit <= 5000):
            await update.message.reply_text("❌ Limit harus antara 1-5000")
            return

        async with config_lock:
            global DAILY_LIMIT
            DAILY_LIMIT = new_limit
        await save_daily_limit()
        msg = f"⚙️ Daily limit diubah ke {DAILY_LIMIT} oleh {update.effective_user.first_name}"
        await update.message.reply_text(f"✅ {msg}")
        await notify_admins(context.bot, msg)

    except (ValueError, TypeError) as e:
        logging.error(f"❌ Error parsing setlimit: {e}")
        await update.message.reply_text("❌ Format salah. Contoh: /setlimit 1000")

async def cmd_setdelay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa mengubah delay")
        return
    try:
        if not context.args:
            await update.message.reply_text("❌ Format salah. Contoh: /setdelay 2.5")
            return

        arg_str = _flatten_arg(context.args)
        if arg_str is None:
            await update.message.reply_text("❌ Format salah. Contoh: /setdelay 2.5")
            return

        new_delay = float(arg_str)
        if not (0.1 <= new_delay <= 10.0):
            await update.message.reply_text("❌ Delay harus antara 0.1-10.0 detik")
            return

        async with config_lock:
            global DELAY_BETWEEN_SEND
            DELAY_BETWEEN_SEND = new_delay
        await save_send_delay()
        msg = f"⚙️ Delay diubah ke {DELAY_BETWEEN_SEND:.1f}s oleh {update.effective_user.first_name}"
        await update.message.reply_text(f"✅ {msg}")
        await notify_admins(context.bot, msg)

    except (ValueError, TypeError) as e:
        logging.error(f"❌ Error parsing setdelay: {e}")
        await update.message.reply_text("❌ Format salah. Contoh: /setdelay 2.5")
        
async def cmd_shutdown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_superadmin(update):
        await update.message.reply_text("❌ Hanya superadmin yang bisa mematikan bot")
        return
    msg = f"🛑 Bot dimatikan oleh {update.effective_user.first_name}"
    await update.message.reply_text(msg)
    await notify_admins(context.bot, msg)
    await save_all()
    await context.application.stop()
    logging.info("✅ Bot berhenti dengan aman")