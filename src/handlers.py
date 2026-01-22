import datetime
import time
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from src.database import Database
from src.utils import get_fair_pisun_delta, PISUN_PHRASES, PIHV_VARIANTS, INSERT_RESPONSES, get_kyiv_now, get_kyiv_today
import random

router = Router()
db = Database("data/pisun.db")

@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Привіт! Я пісюн-бот. Пиши `/pisun` щоб дізнатися правду про свій розмір. Раз на день, все чесно! 🍆\n\nТакож чекай на мої спеціальні повідомлення...")

@router.message(Command("pisun"))
async def cmd_pisun(message: Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    username = message.from_user.full_name
    
    user_data = await db.get_user(user_id, chat_id)
    today = get_kyiv_today()
    
    if user_data:
        length, count, last_measure, _ = user_data
        if last_measure == str(today):
            # Calculate time until midnight
            now = get_kyiv_now()
            tomorrow = datetime.datetime.combine(today + datetime.timedelta(days=1), datetime.time.min, tzinfo=now.tzinfo)
            remaining = tomorrow - now
            
            hours, remainder = divmod(int(remaining.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)
            
            timer_text = f"\n\nСпробуйте знову через: **{hours} год. {minutes} хв. {seconds} сек.**"
            base_phrase = random.choice(PISUN_PHRASES["already_measured"])
            
            await message.reply(base_phrase + timer_text, parse_mode="Markdown")
            return
    else:
        length, count = 0.0, 0
    
    delta = get_fair_pisun_delta(count, length)
    new_length = round(length + delta, 1)
    new_count = count + 1
    
    await db.update_user(user_id, chat_id, username, new_length, new_count, today)
    
    if delta > 0:
        phrase = random.choice(PISUN_PHRASES["plus"])
    elif delta < 0:
        phrase = random.choice(PISUN_PHRASES["minus"])
    else:
        phrase = random.choice(PISUN_PHRASES["zero"])
        
    await message.reply(phrase.format(delta=abs(delta), total=new_length))

@router.message(Command("top"))
async def cmd_top(message: Message):
    top_users = await db.get_top_users(message.chat.id)
    if not top_users:
        await message.answer("Поки що ніхто не мірявся... Будь першим! 🍌")
        return
        
    res = "🏆 **Топ володарів гігантських шлангів:**\n\n"
    for i, (user, length) in enumerate(top_users, 1):
        res += f"{i}. {user} — {length} см\n"
    
    await message.answer(res, parse_mode="Markdown")

@router.callback_query(F.data == "insert_bonus")
async def process_insert(callback: CallbackQuery):
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id
    username = callback.from_user.full_name
    
    # Atomic claim
    success = await db.claim_bonus_event("weekly_pihv", user_id)
    
    if not success:
        await callback.answer("Ти запізнився! Хтось вже вставив... 😔", show_alert=True)
        # Optionally remove the button if it's still there, but usually we just edit the text on success
        return

    # Update user length
    user_data = await db.get_user(user_id, chat_id)
    if user_data:
        length, count, last_measure, _ = user_data
    else:
        # Handling the case where a user clicks bonus but hasn't played /pisun yet
        # We allow it, but we need to initialize them.
        # Use a dummy date for last_measure so they can still play /pisun today if they haven't?
        # Or should this count as a daily measure?
        # Let's say it's a BONUS, so it doesn't affect daily limit.
        length, count, last_measure = 0.0, 0, None
        
    new_length = round(length + 15.0, 1)
    
    # We pass last_measure as existing one to NOT burn their daily turn if they haven't used it
    # If last_measure starts as None, passing None might be an issue if DB expects date.
    # But in DB schema last_measure is DATE. SQLite handles None as NULL.
    # Let's ensure if it was None, we keep it None or handle correctly.
    # Actually update_user expects datetime.date.
    # If last_measure is None (new user), and we pass None, it's fine.
    
    await db.update_user(user_id, chat_id, username, new_length, count, last_measure)
    
    # Extract original ASCII art to preserve it
    # Message text likely doesn't have the markdown code blocks preserved in .text attribute in the same way,
    # but the content is there.
    # However, since we are re-rendering with markdown mode, we need to wrap it again.
    # Let's try to extract relevant part.
    # The message structure is: Header ... \n```\nART\n```
    # If we can't extract cleanly, we pick a random one as fallback.
    original_text = callback.message.text or ""
    # Attempt to find the art between typical boundaries if possible, but .text usually strips backticks? 
    # No, bot API usually preserves entity info but .text is plain.
    # Wait, aiogram Message.text gives plain text. If the original message had entities, they are separate.
    # If we sent it as Markdown, Telegram parses it. The .text property will NOT contain backticks for code blocks!
    # It will just be the monospaced text.
    # So we can't easily rely on splitting by ```.
    # BUT, we can just pick a new random variant? The user asked to "preserve" it.
    # Creating a stateful mapping of message_id -> art is too complex for this task.
    # A simple hack: We know the header ends with "вставить!".
    # Everything after that is likely the art.
    
    art_part = original_text
    if "вставить!" in original_text:
        parts = original_text.split("вставить!", 1)
        if len(parts) > 1:
            art_part = parts[1].strip()
            
    # If extraction fails or looks empty, fallback to random
    if len(art_part) < 10:
        art_part = random.choice(PIHV_VARIANTS)
    
    response_phrase = random.choice(INSERT_RESPONSES)
    
    final_text = f"🚨 **ПІХВА РОЗІГРАНА!** 🚨\n\nГерой: {username} (+15 см)\n```\n{art_part}\n```\n{response_phrase}"
    
    await callback.message.edit_text(
        final_text,
        parse_mode="Markdown"
    )
    await callback.answer("Вітаю! +15 см твої!")
