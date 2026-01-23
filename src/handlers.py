import datetime
import time
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from src.database import Database
from src.utils import get_fair_pisun_delta, PISUN_PHRASES, PIHV_VARIANTS, INSERT_RESPONSES, TRAP_RESPONSES, get_kyiv_now, get_kyiv_today
from src.scheduler import send_weekly_bonus
from src.facts import FACTS
from aiogram import Bot
import random
import os

router = Router()
db = Database("data/pisun.db")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Привіт! Я пісюн-бот. 🍆\n\n"
        "**Команди:**\n"
        "/pisun — дізнатися правду про свій розмір (раз на день)\n"
        "/top — загальний рейтинг чату\n"
        "/top_week — рейтинг за цей тиждень\n"
        "\nПриєднуйся до гри та чекай на спеціальні події! 🔥",
        parse_mode="Markdown"
    )

@router.message(Command("pisun"))
async def cmd_pisun(message: Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    username = message.from_user.full_name
    
    user_data = await db.get_user(user_id, chat_id)
    today = get_kyiv_today()
    current_week = f"{today.isocalendar()[0]}-W{today.isocalendar()[1]}"
    
    if user_data:
        length, count, last_measure, _, weekly_length, last_reset_week = user_data
        
        # Reset weekly stats if it's a new week
        if last_reset_week != current_week:
            weekly_length = 0.0
            last_reset_week = current_week
            
        if last_measure == str(today):
            # Calculate time until midnight
            now = get_kyiv_now()
            tomorrow = datetime.datetime.combine(today + datetime.timedelta(days=1), datetime.time.min, tzinfo=now.tzinfo)
            remaining = tomorrow - now
            
            hours, remainder = divmod(int(remaining.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # --- Fact Logic ---
            shown_indices = await db.get_shown_facts(user_id, chat_id)
            available_indices = [i for i in range(len(FACTS)) if i not in shown_indices]
            
            if not available_indices:
                # Reset if all shown
                await db.clear_shown_facts(user_id, chat_id)
                available_indices = list(range(len(FACTS)))
            
            if available_indices:
                fact_idx = random.choice(available_indices)
                fact_text = FACTS[fact_idx]
                await db.add_shown_fact(user_id, chat_id, fact_idx)
                
                fact_block = f"\n\n🎓 **Цікавий факт:**\n{fact_text}"
            else:
                # Fallback just in case list is empty somehow
                fact_block = ""
            
            timer_text = f"\n\nСпробуйте знову через: **{hours} год. {minutes} хв. {seconds} сек.**"
            base_phrase = random.choice(PISUN_PHRASES["already_measured"])
            
            await message.reply(base_phrase + timer_text + fact_block, parse_mode="Markdown")
            return
    else:
        length, count, weekly_length, last_reset_week = 0.0, 0, 0.0, current_week
    
    delta = get_fair_pisun_delta(count, length)
    new_length = round(length + delta, 1)
    new_weekly_length = round(weekly_length + delta, 1)
    new_count = count + 1
    
    await db.update_user(user_id, chat_id, username, new_length, new_count, today, new_weekly_length, last_reset_week)
    
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

@router.message(Command("top_week"))
async def cmd_top_week(message: Message):
    top_users = await db.get_top_weekly_users(message.chat.id)
    if not top_users:
        await message.answer("Цього тижня ще ніхто не підріс... Будь першим! 🍌")
        return
        
    res = "📅 **Топ за цей тиждень (хто виріс найбільше):**\n\n"
    for i, (user, weekly_length) in enumerate(top_users, 1):
        # Determine sign
        sign = "+" if weekly_length > 0 else ""
        res += f"{i}. {user} — {sign}{weekly_length} см\n"
    
    await message.answer(res, parse_mode="Markdown")

@router.callback_query(F.data == "insert_bonus")
async def process_insert(callback: CallbackQuery):
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id
    username = callback.from_user.full_name
    
    # Atomic claim
    success = await db.claim_chat_event(chat_id, "weekly_pihv", user_id)
    
    if not success:
        await callback.answer("Ти запізнився! Хтось вже вставив... 😔", show_alert=True)
        # Optionally remove the button if it's still there, but usually we just edit the text on success
        return

    # Update user stats
    user_data = await db.get_user(user_id, chat_id)
    today = get_kyiv_today()
    current_week = f"{today.isocalendar()[0]}-W{today.isocalendar()[1]}"
    
    if user_data:
        length, count, last_measure, _, weekly_length, last_reset_week = user_data
        if last_reset_week != current_week:
            weekly_length = 0.0
            last_reset_week = current_week
    else:
        length, count, last_measure, weekly_length, last_reset_week = 0.0, 0, None, 0.0, current_week
        
    # Trap logic: 1/3 chance
    is_trap = random.randint(1, 3) == 1
    
    if is_trap:
        new_delta = random.randint(5, 9)
        new_length = round(max(0.0, length - float(new_delta)), 1)
        actual_delta = new_length - length
        new_weekly_length = round(weekly_length + actual_delta, 1)
        display_delta = f"-{new_delta}"
        alert_text = f"О ноу! -{new_delta} см! 😭"
        header_text = "🚨 **ПАСТКА! ПІХВА ВІДКУСИЛА!** 🚨"
        phrase_list = TRAP_RESPONSES
    else:
        new_delta = random.randint(5, 12)
        new_length = round(length + float(new_delta), 1)
        new_weekly_length = round(weekly_length + float(new_delta), 1)
        display_delta = f"+{new_delta}"
        alert_text = f"Вітаю! +{new_delta} см твої!"
        header_text = "🚨 **ПІХВА РОЗІГРАНА!** 🚨"
        phrase_list = INSERT_RESPONSES

    await db.update_user(user_id, chat_id, username, new_length, count, last_measure, new_weekly_length, last_reset_week)
    
    # Extract original ASCII art to preserve it
    original_text = callback.message.text or ""
    art_part = original_text
    if "вставить!" in original_text:
        parts = original_text.split("вставить!", 1)
        if len(parts) > 1:
            art_part = parts[1].strip()
            
    if len(art_part) < 10:
        art_part = random.choice(PIHV_VARIANTS)
    
    response_phrase = random.choice(phrase_list).format(delta=new_delta)
    
    final_text = f"{header_text}\n\nЖертва/Герой: {username} ({display_delta} см)\n```\n{art_part}\n```\n{response_phrase}"
    
    await callback.message.edit_text(
        final_text,
        parse_mode="Markdown"
    )
    await callback.answer(alert_text, show_alert=is_trap)

# --- Admin Commands ---

@router.message(Command("drop_pihv"))
async def cmd_drop_pihv(message: Message, bot: Bot):
    """
    Secret command for admins to manually trigger the bonus event.
    Works only in private chat.
    """
    if message.chat.type != "private":
        return
    
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас немає прав для цієї команди. 🛑")
        return

    await message.answer("🚀 Починаю масове розповсюдження піхви...")
    
    chat_ids = await db.get_active_chats()
    
    # We call it without force=True so it STILL respects the weekly limit
    # unless we want admins to be able to bypass it?
    # The user said "but lock it for the current week as well, so another one doesn't drop again".
    # This implies the manual drop SHOULD count as the weekly drop.
    
    # Actually, if the admin wants to drop it AFTER it already dropped (maybe they want a second one?),
    # they can't with the current logic. But the user specifically asked for locking.
    
    # If the admin wants to trigger the FIRST one of the week manually:
    await send_weekly_bonus(bot, db, chat_ids, force=False)
    
    # Check if it actually sent (by checking DB state or trusting the call)
    await message.answer("Done! Перевір чати. Якщо нічого не прийшло - значить цього тижня вже був дроп.")

@router.message(Command("reset_pihv"))
async def cmd_reset_pihv(message: Message, bot: Bot):
    """
    Secret command for admins to reset the pihv drop for the CURRENT chat.
    Triggers a new drop immediately for this chat.
    """
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас немає прав для цієї команди. 🛑")
        return

    chat_id = message.chat.id
    
    # 1. De-activate any current game in this chat
    await db.set_chat_event_state(chat_id, "weekly_pihv", False)
    
    # 2. Trigger a fresh drop just for this chat
    # force=True bypasses the global weekly lock
    # update_global=False ensures we don't accidentally "use up" the week's global slot
    await send_weekly_bonus(bot, db, [chat_id], force=True, update_global=False)
    
    await message.answer("✅ Скинуто! Дроп для цього чату перезапущено.")

# --- Catch-all Handlers to silence "Not handled" logs ---

@router.message()
async def skip_unhandled_messages(message: Message):
    """
    This handler catches any messages that didn't match previous filters (like random text in groups).
    It does nothing, which marks the update as 'handled' and silences the warning.
    """
    pass

@router.callback_query()
async def skip_unhandled_callbacks(callback: CallbackQuery):
    """
    Catches any callback queries that didn't match specific filters.
    """
    await callback.answer()
