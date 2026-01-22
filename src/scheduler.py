from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from src.database import Database
from src.utils import PIHV_VARIANTS
import datetime
import random
import logging

logger = logging.getLogger(__name__)

async def send_weekly_bonus(bot: Bot, db: Database, chat_ids: list):
    """
    Sends the ASCII art bonus message to all active chats.
    """
    await db.set_event_state("weekly_pihv", datetime.date.today(), True)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Вставити! 👉👌", callback_data="insert_bonus")]
    ])
    
    for chat_id in chat_ids:
        try:
            # Pick a random variant for each send, or the same for all?
            # Let's pick a random one for each chat for variety
            ascii_art = random.choice(PIHV_VARIANTS)
            
            await bot.send_message(
                chat_id,
                f"🚨 **ЕКСТРЕНИЙ ВИПУСК!** 🚨\n\nЗ'явилася нічийна піхва! Будь першим, хто вставить!\n```\n{ascii_art}\n```",
                reply_markup=keyboard,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Failed to send bonus to {chat_id}: {e}")

def setup_scheduler(bot: Bot, db: Database, chat_ids: list):
    scheduler = AsyncIOScheduler()
    
    # Schedule weekly event at a random time once a week
    # To make it truly random, we can reschedule it after each run
    def schedule_next():
        day = random.randint(0, 6)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        
        scheduler.add_job(
            send_weekly_bonus,
            'cron',
            day_of_week=day,
            hour=hour,
            minute=minute,
            args=[bot, db, chat_ids],
            id='weekly_bonus',
            replace_existing=True
        )
        logger.info(f"Next bonus scheduled for day {day} at {hour:02d}:{minute:02d}")

    # Initial schedule
    schedule_next()
    
    # Job to reschedule for next week after execution
    scheduler.add_job(schedule_next, 'interval', weeks=1)
    
    return scheduler
