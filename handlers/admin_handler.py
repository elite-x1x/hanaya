import logging
from telegram.ext import CommandHandler
from state_manager import daily_count, DAILY_LIMIT

async def stats(update, context):
    """Command /stats untuk admin"""
    await update.message.reply_text(
        f"📊 Stats:\n"
        f"- Daily count: {daily_count}/{DAILY_LIMIT}\n"
    )
    logging.info("📊 Admin memanggil /stats")

def get_handler():
    """Return handler untuk command admin"""
    return CommandHandler("stats", stats)
