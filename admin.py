import logging
from telegram import Update
from telegram.ext import ContextTypes
from .config import ADMINS, pending_media, local_sent, daily_count, DAILY_LIMIT
from .flood import SmartFloodController

flood_ctrl = SmartFloodController()

def get_role(update: Update): return ADMINS.get(update.effective_user.id,None)
def is_superadmin(update: Update): return get_role(update)=="superadmin"
def is_moderator(update: Update): return get_role(update) in ["moderator","superadmin"]

async def notify_admins(bot,text): 
    for aid in ADMINS: 
        try: await bot.send_message(chat_id=aid,text=f"[ADMIN] {text}")
        except: pass

# contoh command
async def cmd_status(update,context):
    if not is_moderator(update): return
    await update.message.reply_text(f"📊 STATUS\nPending:{len(pending_media)} | Sent:{len(local_sent)} | Daily:{daily_count}/{DAILY_LIMIT}\n{flood_ctrl.get_status()}")
