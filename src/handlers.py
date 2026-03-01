import datetime
import html
import logging
import os
import random
import time
from typing import Any, Dict, Optional, Tuple

from aiogram import Bot, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.database import Database
from src.facts import FACTS
from src.scheduler import send_weekly_bonus, start_game_event
from src.utils import (
    EVENT_TYPES,
    HISTORY_SOURCE_LABELS,
    INSERT_RESPONSES,
    PIHV_VARIANTS,
    PISUN_PHRASES,
    QUIET_HOURS_END,
    QUIET_HOURS_START,
    STAKE_TIERS,
    TRAP_RESPONSES,
    get_fair_pisun_delta,
    get_kyiv_now,
    get_kyiv_today,
    get_utc_now,
    parse_iso_datetime,
    to_kyiv_datetime,
)

logger = logging.getLogger(__name__)

router = Router()
db = Database("data/pisun.db")
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
MANUAL_EVENT_COOLDOWN_SEC = 10 * 60
COOLDOWN_NOTICE_SUPPRESS_SEC = 60
_last_cooldown_notice_by_user_chat: Dict[Tuple[int, int], float] = {}


class EventCreateStates(StatesGroup):
    waiting_event_type = State()
    waiting_stake = State()


def _current_week_id(today: datetime.date) -> str:
    return f"{today.isocalendar()[0]}-W{today.isocalendar()[1]}"


def _format_remaining(now: datetime.datetime, today: datetime.date) -> str:
    tomorrow = datetime.datetime.combine(today + datetime.timedelta(days=1), datetime.time.min, tzinfo=now.tzinfo)
    remaining = tomorrow - now
    hours, remainder = divmod(int(remaining.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours} год. {minutes} хв. {seconds} сек."


def _get_skip_reason_text(reason: Optional[str]) -> Optional[str]:
    if reason == "already_sent_this_week":
        return "цього тижня дроп уже був"
    if reason == "no_eligible_chats":
        return "немає доступних чатів для дропу"
    if reason:
        return reason
    return None


def _format_drop_result(scope_label: str, result: Dict[str, Any]) -> str:
    lines = [
        f"📦 Результат дропу ({scope_label}):",
        f"- спроб: {result.get('attempted', 0)}",
        f"- успішно: {result.get('sent', 0)}",
        f"- помилок: {result.get('failed', 0)}",
        f"- деактивовано чатів: {result.get('deactivated', 0)}",
    ]
    reason = _get_skip_reason_text(result.get("skipped_reason"))
    if reason:
        lines.append(f"- статус: {reason}")
    return "\n".join(lines)


async def _safe_callback_answer(callback: CallbackQuery, text: Optional[str] = None, show_alert: bool = False):
    try:
        if text is None:
            await callback.answer()
        else:
            await callback.answer(text, show_alert=show_alert)
    except TelegramBadRequest as exc:
        lowered = str(exc).lower()
        if "query is too old" in lowered or "query id is invalid" in lowered:
            logger.info("Skipping stale callback answer: %s", callback.id)
            return
        raise


def _upsert_participants_line(text: str, participants: int) -> str:
    lines = text.splitlines()
    marker = "👥 Учасники:"
    for i, line in enumerate(lines):
        if line.startswith(marker):
            lines[i] = f"{marker} {participants}"
            return "\n".join(lines)

    if lines and lines[-1].strip():
        lines.append("")
    lines.append(f"{marker} {participants}")
    return "\n".join(lines)


async def _register_message_chat(message: Message):
    migrate_to = getattr(message, "migrate_to_chat_id", None)
    migrate_from = getattr(message, "migrate_from_chat_id", None)

    if migrate_to and message.chat:
        old_chat_id = message.chat.id
        new_chat_id = int(migrate_to)
        await db.migrate_chat_data(old_chat_id, new_chat_id, "supergroup")
        await db.register_chat(new_chat_id, "supergroup")
        return

    if migrate_from and message.chat:
        old_chat_id = int(migrate_from)
        new_chat_id = message.chat.id
        await db.migrate_chat_data(old_chat_id, new_chat_id, "supergroup")
        await db.register_chat(new_chat_id, "supergroup")
        return

    await db.register_chat(message.chat.id, message.chat.type)


async def _register_callback_chat(callback: CallbackQuery):
    if callback.message and callback.message.chat:
        await db.register_chat(callback.message.chat.id, callback.message.chat.type)


async def _is_chat_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in {"administrator", "creator"}
    except Exception as exc:
        logger.warning("Failed to check admin status chat=%s user=%s: %s", chat_id, user_id, exc)
        return False


async def _require_group(message: Message) -> bool:
    if message.chat.type not in {"group", "supergroup"}:
        await message.answer("Ця команда працює тільки в групах.")
        return False
    return True


async def _require_group_admin(message: Message, bot: Bot) -> bool:
    if not await _require_group(message):
        return False
    is_admin = await _is_chat_admin(bot, message.chat.id, message.from_user.id)
    if not is_admin:
        await message.answer("Ця команда доступна лише адмінам групи або глобальним адмінам.")
        return False
    return True


def _event_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⚔️ Дуель", callback_data="eventfsm:type:duel"),
                InlineKeyboardButton(text="🎰 Джекпот", callback_data="eventfsm:type:jackpot"),
            ],
            [
                InlineKeyboardButton(text="🪤 Пастка", callback_data="eventfsm:type:trap"),
            ],
            [
                InlineKeyboardButton(text="❌ Скасувати", callback_data="eventfsm:cancel"),
            ],
        ]
    )


def _event_stake_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="5 см", callback_data="eventfsm:stake:5"),
                InlineKeyboardButton(text="10 см", callback_data="eventfsm:stake:10"),
                InlineKeyboardButton(text="20 см", callback_data="eventfsm:stake:20"),
            ],
            [
                InlineKeyboardButton(text="❌ Скасувати", callback_data="eventfsm:cancel"),
            ],
        ]
    )


def _format_cooldown_remaining(total_seconds: int) -> str:
    seconds = max(0, total_seconds)
    minutes, rem = divmod(seconds, 60)
    return f"{minutes} хв. {rem} сек."


def _parse_event_command(text: Optional[str]) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    parts = (text or "").split()
    if len(parts) <= 1:
        return None, None, None
    if len(parts) > 3:
        return None, None, "Використання: /event <duel|jackpot|trap> [5|10|20]"

    event_type = parts[1].lower().strip()
    if event_type not in EVENT_TYPES:
        return None, None, "Невідомий тип події. Доступно: duel, jackpot, trap."

    stake = 10
    if len(parts) == 3:
        try:
            stake = int(parts[2])
        except ValueError:
            return None, None, "Ставка має бути числом: 5, 10 або 20."

    if stake not in STAKE_TIERS:
        return None, None, "Доступні ставки: 5, 10, 20."

    return event_type, stake, None


async def _create_manual_event(
    bot: Bot,
    chat_id: int,
    user_id: int,
    event_type: str,
    stake: int,
    creator_username: str,
) -> Tuple[bool, str, str]:
    if event_type not in EVENT_TYPES:
        return False, "Невідомий тип події. Доступно: duel, jackpot, trap.", "invalid"
    if stake not in STAKE_TIERS:
        return False, "Доступні ставки: 5, 10, 20.", "invalid"

    active = await db.get_active_game_event(chat_id)
    if active:
        return False, "У чаті вже є активна подія. Дочекайся завершення.", "active"

    if event_type == "duel":
        user_data = await db.get_user(user_id, chat_id)
        current_length = float(user_data[0]) if user_data else 0.0
        if current_length < stake:
            return False, f"Недостатньо довжини для ставки {stake} см.", "insufficient_length"

    is_admin = await _is_chat_admin(bot, chat_id, user_id)
    now = get_utc_now()

    if not is_admin:
        last_manual_event_at = await db.get_last_manual_event_at(chat_id)
        if last_manual_event_at:
            try:
                last_manual_dt = parse_iso_datetime(last_manual_event_at)
                allowed_at = last_manual_dt + datetime.timedelta(seconds=MANUAL_EVENT_COOLDOWN_SEC)
                if now < allowed_at:
                    remaining_seconds = int((allowed_at - now).total_seconds())
                    return (
                        False,
                        "Зачекай перед запуском нової ручної події: "
                        f"{_format_cooldown_remaining(remaining_seconds)}.",
                        "cooldown",
                    )
            except Exception:
                logger.warning("Failed to parse last manual event time for chat %s", chat_id)

    # Re-check right before create to narrow race window.
    active = await db.get_active_game_event(chat_id)
    if active:
        return False, "У чаті вже є активна подія. Дочекайся завершення.", "active"

    event_id = await start_game_event(
        bot=bot,
        db=db,
        chat_id=chat_id,
        event_type=event_type,
        stake=stake,
        auto=False,
        creator_id=user_id,
        creator_username=creator_username,
    )
    if not event_id:
        return False, "Не вдалося запустити подію. Спробуй ще раз.", "error"

    await db.set_last_manual_event_at(chat_id, now.isoformat(timespec="seconds"))
    return True, f"Подію запущено: {event_type} (ставка {stake} см).", "ok"


async def _precheck_manual_event_start(bot: Bot, chat_id: int, user_id: int) -> Tuple[bool, Optional[str], str]:
    active = await db.get_active_game_event(chat_id)
    if active:
        return False, "У чаті вже є активна подія. Дочекайся завершення.", "active"

    is_admin = await _is_chat_admin(bot, chat_id, user_id)
    if is_admin:
        return True, None, "ok"

    now = get_utc_now()
    last_manual_event_at = await db.get_last_manual_event_at(chat_id)
    if not last_manual_event_at:
        return True, None, "ok"

    try:
        last_manual_dt = parse_iso_datetime(last_manual_event_at)
        allowed_at = last_manual_dt + datetime.timedelta(seconds=MANUAL_EVENT_COOLDOWN_SEC)
        if now < allowed_at:
            remaining_seconds = int((allowed_at - now).total_seconds())
            return (
                False,
                "Зачекай перед запуском нової ручної події: "
                f"{_format_cooldown_remaining(remaining_seconds)}.",
                "cooldown",
            )
    except Exception:
        logger.warning("Failed to parse last manual event time for chat %s", chat_id)

    return True, None, "ok"


def _should_emit_cooldown_notice(chat_id: int, user_id: int) -> bool:
    now = time.monotonic()
    key = (chat_id, user_id)
    last_seen = _last_cooldown_notice_by_user_chat.get(key, 0.0)
    if now - last_seen < COOLDOWN_NOTICE_SUPPRESS_SEC:
        return False
    _last_cooldown_notice_by_user_chat[key] = now
    return True


@router.message(Command("start"))
async def cmd_start(message: Message):
    await _register_message_chat(message)
    await message.answer(
        "Привіт! Я пісюн-бот. 🍆\n\n"
        "<b>Команди:</b>\n"
        "<code>/pisun</code> - дізнатися правду про свій розмір (раз на день)\n"
        "<code>/top</code> - загальний рейтинг чату\n"
        "<code>/top_week</code> - рейтинг за цей тиждень\n"
        "<code>/me</code> - особиста статистика\n"
        "<code>/history</code> - останні зміни\n"
        "\nПодії та адмін-команди доступні в групах.",
        parse_mode="HTML",
    )


@router.message(Command("pisun"))
async def cmd_pisun(message: Message):
    await _register_message_chat(message)

    user_id = message.from_user.id
    chat_id = message.chat.id
    username = message.from_user.full_name

    user_data = await db.get_user(user_id, chat_id)
    today = get_kyiv_today()
    current_week = _current_week_id(today)

    if user_data:
        length, count, last_measure, _, weekly_length, last_reset_week = user_data

        if last_reset_week != current_week:
            weekly_length = 0.0
            last_reset_week = current_week

        if last_measure == str(today):
            now = get_kyiv_now()
            shown_indices = await db.get_shown_facts(user_id, chat_id)
            available_indices = [i for i in range(len(FACTS)) if i not in shown_indices]

            if not available_indices:
                await db.clear_shown_facts(user_id, chat_id)
                available_indices = list(range(len(FACTS)))

            if available_indices:
                fact_idx = random.choice(available_indices)
                fact_text = FACTS[fact_idx]
                await db.add_shown_fact(user_id, chat_id, fact_idx)
                fact_block = f"\n\n🎓 <b>Цікавий факт:</b>\n{html.escape(fact_text)}"
            else:
                fact_block = ""

            timer_text = f"\n\nСпробуйте знову через: <b>{_format_remaining(now, today)}</b>"
            base_phrase = html.escape(random.choice(PISUN_PHRASES["already_measured"]))

            await message.reply(base_phrase + timer_text + fact_block, parse_mode="HTML")
            return
    else:
        length, count, weekly_length, last_reset_week = 0.0, 0, 0.0, current_week

    delta = get_fair_pisun_delta(count, length)
    new_length = round(length + delta, 1)
    new_weekly_length = round(weekly_length + delta, 1)
    new_count = count + 1

    await db.update_user(user_id, chat_id, username, new_length, new_count, today, new_weekly_length, last_reset_week)
    await db.add_measurement(
        user_id,
        chat_id,
        delta,
        new_length,
        "pisun",
        meta={"measure_count": new_count},
    )

    if delta > 0:
        phrase = random.choice(PISUN_PHRASES["plus"])
    elif delta < 0:
        phrase = random.choice(PISUN_PHRASES["minus"])
    else:
        phrase = random.choice(PISUN_PHRASES["zero"])

    await message.reply(phrase.format(delta=abs(delta), total=new_length))


@router.message(Command("me"))
async def cmd_me(message: Message):
    await _register_message_chat(message)

    user_id = message.from_user.id
    chat_id = message.chat.id
    user_data = await db.get_user(user_id, chat_id)
    if not user_data:
        await message.answer("У тебе ще немає статистики в цьому чаті. Напиши /pisun, щоб стартувати.")
        return

    length, count, last_measure, _, weekly_length, last_reset_week = user_data
    today = get_kyiv_today()
    current_week = _current_week_id(today)
    if last_reset_week != current_week:
        weekly_length = 0.0

    rank, total = await db.get_user_rank_and_total(user_id, chat_id)
    rank_text = f"#{rank}" if rank is not None else "-"

    if last_measure == str(today):
        status_text = f"доступно через {_format_remaining(get_kyiv_now(), today)}"
    else:
        status_text = "можна міряти зараз"

    sign = "+" if weekly_length > 0 else ""
    await message.answer(
        (
            "📊 <b>Твоя картка</b>\n\n"
            f"Довжина: <b>{length} см</b>\n"
            f"Тижневий приріст: <b>{sign}{weekly_length} см</b>\n"
            f"Всього замірів: <b>{count}</b>\n"
            f"Статус: <b>{html.escape(status_text)}</b>\n"
            f"Позиція в чаті: <b>{rank_text}</b> з <b>{total}</b>"
        ),
        parse_mode="HTML",
    )


@router.message(Command("history"))
async def cmd_history(message: Message):
    await _register_message_chat(message)

    user_id = message.from_user.id
    chat_id = message.chat.id
    rows = await db.get_user_history(user_id, chat_id, limit=10)
    if not rows:
        await message.answer("Історія порожня. Зроби /pisun або зіграй у подіях.")
        return

    lines = ["🧾 <b>Останні 10 змін:</b>", ""]
    for delta, new_length, source, _meta, created_at in rows:
        source_label = HISTORY_SOURCE_LABELS.get(source, source)
        sign_delta = f"{delta:+.1f}"
        try:
            kyiv_dt = to_kyiv_datetime(created_at)
            time_label = kyiv_dt.strftime("%d.%m %H:%M")
        except Exception:
            time_label = created_at
        lines.append(
            f"• {time_label} | {html.escape(source_label)} | {sign_delta} см -> {new_length:.1f} см"
        )

    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("top"))
async def cmd_top(message: Message):
    await _register_message_chat(message)

    top_users = await db.get_top_users(message.chat.id)
    if not top_users:
        await message.answer("Поки що ніхто не мірявся... Будь першим! 🍌")
        return

    lines = ["🏆 <b>Топ володарів гігантських шлангів:</b>", ""]
    for i, (user, length) in enumerate(top_users, 1):
        safe_user = html.escape(user or "Невідомий")
        lines.append(f"{i}. {safe_user} - {length} см")

    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("top_week"))
async def cmd_top_week(message: Message):
    await _register_message_chat(message)

    top_users = await db.get_top_weekly_users(message.chat.id)
    if not top_users:
        await message.answer("Цього тижня ще ніхто не підріс... Будь першим! 🍌")
        return

    lines = ["📅 <b>Топ за цей тиждень (хто виріс найбільше):</b>", ""]
    for i, (user, weekly_length) in enumerate(top_users, 1):
        safe_user = html.escape(user or "Невідомий")
        sign = "+" if weekly_length > 0 else ""
        lines.append(f"{i}. {safe_user} - {sign}{weekly_length} см")

    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("drops_on"))
async def cmd_drops_on(message: Message, bot: Bot):
    await _register_message_chat(message)
    if not await _require_group_admin(message, bot):
        return

    await db.set_drops_enabled(message.chat.id, True)
    await message.answer("✅ Тижневі дропи увімкнено для цього чату.")


@router.message(Command("drops_off"))
async def cmd_drops_off(message: Message, bot: Bot):
    await _register_message_chat(message)
    if not await _require_group_admin(message, bot):
        return

    await db.set_drops_enabled(message.chat.id, False)
    await message.answer("⛔ Тижневі дропи вимкнено для цього чату.")


@router.message(Command("drops_status"))
async def cmd_drops_status(message: Message):
    await _register_message_chat(message)
    if not await _require_group(message):
        return

    settings = await db.get_chat_settings(message.chat.id)
    if not settings:
        await message.answer("Налаштування чату не знайдено.")
        return

    enabled_text = "увімкнено" if settings["drops_enabled"] else "вимкнено"
    reason = settings.get("disabled_reason") or "-"
    disabled_at = settings.get("disabled_at")
    if disabled_at:
        try:
            disabled_label = to_kyiv_datetime(disabled_at).strftime("%d.%m.%Y %H:%M")
        except Exception:
            disabled_label = disabled_at
    else:
        disabled_label = "-"

    await message.answer(
        (
            "📦 <b>Статус дропів</b>\n"
            f"Стан: <b>{enabled_text}</b>\n"
            f"Причина вимкнення: <b>{html.escape(reason)}</b>\n"
            f"Вимкнено о: <b>{disabled_label}</b>"
        ),
        parse_mode="HTML",
    )


@router.message(Command("events_on"))
async def cmd_events_on(message: Message, bot: Bot):
    await _register_message_chat(message)
    if not await _require_group_admin(message, bot):
        return

    await db.set_events_enabled(message.chat.id, True)
    await message.answer("✅ Автоподії увімкнено для цього чату.")


@router.message(Command("events_off"))
async def cmd_events_off(message: Message, bot: Bot):
    await _register_message_chat(message)
    if not await _require_group_admin(message, bot):
        return

    await db.set_events_enabled(message.chat.id, False)
    await message.answer("⛔ Автоподії вимкнено для цього чату.")


@router.message(Command("events_status"))
async def cmd_events_status(message: Message):
    await _register_message_chat(message)
    if not await _require_group(message):
        return

    settings = await db.get_chat_settings(message.chat.id)
    if not settings:
        await message.answer("Налаштування чату не знайдено.")
        return

    enabled_text = "увімкнено" if settings["events_enabled"] else "вимкнено"
    next_auto = settings.get("next_auto_event_at")
    if next_auto:
        try:
            next_auto_label = to_kyiv_datetime(next_auto).strftime("%d.%m.%Y %H:%M")
        except Exception:
            next_auto_label = next_auto
    else:
        next_auto_label = "не заплановано"

    await message.answer(
        (
            "🎯 <b>Статус подій</b>\n"
            f"Стан: <b>{enabled_text}</b>\n"
            f"Тиха зона: <b>{QUIET_HOURS_START:02d}:00-{QUIET_HOURS_END:02d}:00</b> (Europe/Kyiv)\n"
            f"Наступна автоподія: <b>{next_auto_label}</b>"
        ),
        parse_mode="HTML",
    )


@router.message(Command("event"))
async def cmd_event(message: Message, bot: Bot, state: FSMContext):
    await _register_message_chat(message)
    current_state = await state.get_state()

    if not await _require_group(message):
        return

    event_type, stake, error = _parse_event_command(message.text)
    if error:
        await message.answer(error)
        return

    if event_type is None:
        if current_state in {
            EventCreateStates.waiting_event_type.state,
            EventCreateStates.waiting_stake.state,
        }:
            return
        await state.clear()
        can_start, start_text, start_code = await _precheck_manual_event_start(
            bot=bot,
            chat_id=message.chat.id,
            user_id=message.from_user.id,
        )
        if not can_start:
            if start_code == "cooldown":
                if _should_emit_cooldown_notice(message.chat.id, message.from_user.id):
                    await message.answer(start_text or "Зачекай перед новим запуском.")
                return
            await message.answer(start_text or "Подію зараз запустити не можна.")
            return
        await state.set_state(EventCreateStates.waiting_event_type)
        await message.answer(
            "Обери тип події:",
            reply_markup=_event_type_keyboard(),
        )
        return

    await state.clear()
    success, response_text, response_code = await _create_manual_event(
        bot=bot,
        chat_id=message.chat.id,
        user_id=message.from_user.id,
        event_type=event_type,
        stake=stake or 10,
        creator_username=message.from_user.full_name,
    )
    if success:
        return
    if response_code == "cooldown":
        if _should_emit_cooldown_notice(message.chat.id, message.from_user.id):
            await message.answer(response_text)
        return
    await message.answer(response_text)


@router.callback_query(F.data.startswith("eventfsm:type:"))
async def process_eventfsm_type(callback: CallbackQuery, state: FSMContext):
    await _register_callback_chat(callback)
    current_state = await state.get_state()
    if current_state != EventCreateStates.waiting_event_type.state:
        await _safe_callback_answer(callback, "Майстер запуску неактивний.", show_alert=True)
        return

    if not callback.message or not callback.message.chat:
        await state.clear()
        await _safe_callback_answer(callback, "Подія вже неактуальна.", show_alert=True)
        return

    data = callback.data or ""
    parts = data.split(":")
    if len(parts) != 3:
        await state.clear()
        await _safe_callback_answer(callback, "Некоректна дія.", show_alert=True)
        return

    event_type = parts[2].lower().strip()
    if event_type not in EVENT_TYPES:
        await state.clear()
        await _safe_callback_answer(callback, "Некоректний тип події.", show_alert=True)
        return

    await state.update_data(event_type=event_type)
    await state.set_state(EventCreateStates.waiting_stake)
    await callback.message.edit_text("Обери ставку:", reply_markup=_event_stake_keyboard())
    await _safe_callback_answer(callback, "Тип обрано.")


@router.callback_query(F.data.startswith("eventfsm:stake:"))
async def process_eventfsm_stake(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await _register_callback_chat(callback)
    current_state = await state.get_state()
    if current_state != EventCreateStates.waiting_stake.state:
        await _safe_callback_answer(callback, "Майстер запуску неактивний.", show_alert=True)
        return

    if not callback.message or not callback.message.chat:
        await state.clear()
        await _safe_callback_answer(callback, "Подія вже неактуальна.", show_alert=True)
        return

    data = callback.data or ""
    parts = data.split(":")
    if len(parts) != 3:
        await state.clear()
        await _safe_callback_answer(callback, "Некоректна дія.", show_alert=True)
        return

    try:
        stake = int(parts[2])
    except ValueError:
        await state.clear()
        await _safe_callback_answer(callback, "Некоректна ставка.", show_alert=True)
        return

    if stake not in STAKE_TIERS:
        await state.clear()
        await _safe_callback_answer(callback, "Доступні ставки: 5, 10, 20.", show_alert=True)
        return

    fsm_data = await state.get_data()
    event_type = (fsm_data.get("event_type") or "").strip().lower()
    if event_type not in EVENT_TYPES:
        await state.clear()
        await _safe_callback_answer(callback, "Не вдалося визначити тип події.", show_alert=True)
        return

    success, response_text, response_code = await _create_manual_event(
        bot=bot,
        chat_id=callback.message.chat.id,
        user_id=callback.from_user.id,
        event_type=event_type,
        stake=stake,
        creator_username=callback.from_user.full_name,
    )
    await state.clear()
    if success:
        await callback.message.edit_text("✅ Подію створено.")
        await _safe_callback_answer(callback, "Подію запущено.")
        return
    if response_code == "cooldown":
        await callback.message.edit_text("⏳ На запуск події ще діє затримка.")
        await _safe_callback_answer(callback, response_text, show_alert=True)
        return
    await callback.message.edit_text("❌ Не вдалося створити подію.")
    await _safe_callback_answer(callback, response_text, show_alert=True)


@router.callback_query(F.data == "eventfsm:cancel")
async def process_eventfsm_cancel(callback: CallbackQuery, state: FSMContext):
    await _register_callback_chat(callback)
    current_state = await state.get_state()
    if current_state in {
        EventCreateStates.waiting_event_type.state,
        EventCreateStates.waiting_stake.state,
    }:
        await state.clear()
        if callback.message:
            await callback.message.edit_text("Створення події скасовано.")
        await _safe_callback_answer(callback, "Скасовано.")
        return
    await _safe_callback_answer(callback, "Немає активного створення події.", show_alert=True)


@router.callback_query(F.data.startswith("eventfsm:"))
async def process_eventfsm_unknown(callback: CallbackQuery, state: FSMContext):
    await _register_callback_chat(callback)
    await state.clear()
    await _safe_callback_answer(callback, "Некоректна дія.", show_alert=True)


@router.callback_query(F.data == "insert_bonus")
async def process_insert(callback: CallbackQuery):
    await _register_callback_chat(callback)

    if not callback.message or not callback.message.chat:
        await _safe_callback_answer(callback, "Подія вже неактуальна.")
        return

    user_id = callback.from_user.id
    chat_id = callback.message.chat.id
    username = callback.from_user.full_name

    success = await db.claim_chat_event(chat_id, "weekly_pihv", user_id)
    if not success:
        await _safe_callback_answer(callback, "Ти запізнився! Хтось вже вставив... 😔", show_alert=True)
        return

    user_data = await db.get_user(user_id, chat_id)
    today = get_kyiv_today()
    current_week = _current_week_id(today)

    if user_data:
        length, count, last_measure, _, weekly_length, last_reset_week = user_data
        if last_reset_week != current_week:
            weekly_length = 0.0
            last_reset_week = current_week
    else:
        length, count, last_measure, weekly_length, last_reset_week = 0.0, 0, None, 0.0, current_week

    is_trap = random.randint(1, 3) == 1

    if is_trap:
        new_delta = random.randint(5, 9)
        new_length = round(max(0.0, length - float(new_delta)), 1)
        actual_delta = round(new_length - length, 1)
        new_weekly_length = round(weekly_length + actual_delta, 1)
        display_delta = f"-{new_delta}"
        alert_text = f"О ноу! -{new_delta} см! 😭"
        header_text = "ПАСТКА! ПІХВА ВІДКУСИЛА!"
        phrase_list = TRAP_RESPONSES
    else:
        new_delta = random.randint(5, 12)
        new_length = round(length + float(new_delta), 1)
        actual_delta = round(new_length - length, 1)
        new_weekly_length = round(weekly_length + actual_delta, 1)
        display_delta = f"+{new_delta}"
        alert_text = f"Вітаю! +{new_delta} см твої!"
        header_text = "ПІХВА РОЗІГРАНА!"
        phrase_list = INSERT_RESPONSES

    await db.update_user(user_id, chat_id, username, new_length, count, last_measure, new_weekly_length, last_reset_week)
    await db.add_measurement(
        user_id,
        chat_id,
        actual_delta,
        new_length,
        "weekly_pihv",
        meta={"is_trap": is_trap},
    )

    original_text = callback.message.text or ""
    art_part = original_text
    lowered = original_text.lower()
    marker = "вставить!"
    if marker in lowered:
        marker_index = lowered.index(marker)
        art_part = original_text[marker_index + len(marker) :].strip()

    if len(art_part) < 10:
        art_part = random.choice(PIHV_VARIANTS)

    response_phrase = random.choice(phrase_list).format(delta=new_delta)

    final_text = (
        f"🚨 <b>{html.escape(header_text)}</b> 🚨\n\n"
        f"Жертва/Герой: {html.escape(username)} ({display_delta} см)\n"
        f"<pre>{html.escape(art_part)}</pre>\n"
        f"{html.escape(response_phrase)}"
    )

    await callback.message.edit_text(final_text, parse_mode="HTML")
    await _safe_callback_answer(callback, alert_text, show_alert=is_trap)


@router.callback_query(F.data.startswith("event:"))
async def process_game_event_callback(callback: CallbackQuery):
    await _register_callback_chat(callback)

    data = callback.data or ""
    parts = data.split(":")
    if len(parts) < 3:
        await _safe_callback_answer(callback, "Некоректна дія.")
        return

    _, event_id, action, *tail = parts
    event = await db.get_game_event(event_id)
    if not event or event.get("status") != "active":
        await _safe_callback_answer(callback, "Подія вже завершена.", show_alert=True)
        return

    if not callback.message or not callback.message.chat:
        await _safe_callback_answer(callback, "Подія вже неактуальна.", show_alert=True)
        return

    chat_id = callback.message.chat.id
    if chat_id != event.get("chat_id"):
        await _safe_callback_answer(callback, "Ця подія не для цього чату.")
        return

    expires_at = parse_iso_datetime(event["expires_at"])
    if get_utc_now() >= expires_at:
        await _safe_callback_answer(callback, "Час участі вийшов.", show_alert=True)
        return

    stake = int(event["stake"])
    user_id = callback.from_user.id
    username = callback.from_user.full_name

    user_data = await db.get_user(user_id, chat_id)
    current_length = float(user_data[0]) if user_data else 0.0
    if current_length < stake:
        await _safe_callback_answer(
            callback,
            f"Недостатньо довжини для ставки {stake} см.",
            show_alert=True,
        )
        return

    event_type = event.get("event_type")

    if action == "join":
        if event_type not in {"duel", "jackpot"}:
            await _safe_callback_answer(callback, "Для цієї події потрібно обрати A/B/C.", show_alert=True)
            return
        if event_type == "duel":
            join_status = await db.join_game_event_limited(event_id, user_id, username, max_participants=2)
            if join_status == "joined":
                entries = await db.get_game_event_entries(event_id)
                participant_count = len(entries)
                if callback.message and callback.message.text:
                    updated_text = _upsert_participants_line(callback.message.text, participant_count)
                    should_disable_join = participant_count >= 2
                    current_markup = getattr(callback.message, "reply_markup", None)
                    should_edit = (updated_text != callback.message.text) or (should_disable_join and current_markup is not None)
                    if should_edit:
                        try:
                            await callback.message.edit_text(
                                updated_text,
                                reply_markup=None if should_disable_join else current_markup,
                            )
                        except TelegramBadRequest as exc:
                            if "message is not modified" not in str(exc).lower():
                                raise
                if participant_count >= 2:
                    await _safe_callback_answer(callback, "Дуель прийнято! ⚔️")
                else:
                    await _safe_callback_answer(callback, "Ти в грі! ✅")
            elif join_status == "already_joined":
                await _safe_callback_answer(callback, "Ти вже у цій дуелі.")
            else:
                entries = await db.get_game_event_entries(event_id)
                participant_count = len(entries)
                if callback.message and callback.message.text and participant_count >= 2:
                    updated_text = _upsert_participants_line(callback.message.text, participant_count)
                    current_markup = getattr(callback.message, "reply_markup", None)
                    should_edit = (updated_text != callback.message.text) or (current_markup is not None)
                    if should_edit:
                        try:
                            await callback.message.edit_text(updated_text, reply_markup=None)
                        except TelegramBadRequest as exc:
                            if "message is not modified" not in str(exc).lower():
                                raise
                await _safe_callback_answer(callback, "Дуель уже прийнята іншим гравцем.", show_alert=True)
            return

        joined = await db.join_game_event(event_id, user_id, username)
        if joined:
            participant_count = len(await db.get_game_event_entries(event_id))
            if callback.message and callback.message.text:
                updated_text = _upsert_participants_line(callback.message.text, participant_count)
                if updated_text != callback.message.text:
                    try:
                        await callback.message.edit_text(
                            updated_text,
                            reply_markup=getattr(callback.message, "reply_markup", None),
                        )
                    except TelegramBadRequest as exc:
                        if "message is not modified" not in str(exc).lower():
                            raise
            await _safe_callback_answer(callback, "Ти в грі! ✅")
        else:
            await _safe_callback_answer(callback, "Ти вже приєднався.")
        return

    if action == "choice":
        if event_type != "trap":
            await _safe_callback_answer(callback, "Це не подія з вибором.", show_alert=True)
            return
        if not tail:
            await _safe_callback_answer(callback, "Некоректний вибір.")
            return
        choice = tail[0].upper()
        if choice not in {"A", "B", "C"}:
            await _safe_callback_answer(callback, "Некоректний вибір.")
            return
        await db.join_game_event(event_id, user_id, username, choice=choice)
        await db.record_event_choice(event_id, user_id, choice)
        await _safe_callback_answer(callback, f"Твій вибір: {choice}")
        return

    await _safe_callback_answer(callback, "Невідома дія.")


@router.message(Command("drop_pihv"))
async def cmd_drop_pihv(message: Message, bot: Bot):
    await _register_message_chat(message)

    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас немає прав для цієї команди. 🛑")
        return

    if message.chat.type == "private":
        result = await send_weekly_bonus(bot, db, force=False, update_global=True, groups_only=True)
        await message.answer(_format_drop_result("усі групи", result))
        return

    if message.chat.type in {"group", "supergroup"}:
        result = await send_weekly_bonus(
            bot,
            db,
            chat_ids=[message.chat.id],
            force=True,
            update_global=False,
            groups_only=True,
        )
        await message.answer(_format_drop_result("поточний чат", result))
        return

    await message.answer("Ця команда підтримується тільки в приваті або в групі.")


@router.message(Command("reset_pihv"))
async def cmd_reset_pihv(message: Message, bot: Bot):
    await _register_message_chat(message)

    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас немає прав для цієї команди. 🛑")
        return

    chat_id = message.chat.id

    await db.set_chat_event_state(chat_id, "weekly_pihv", False)
    result = await send_weekly_bonus(
        bot,
        db,
        [chat_id],
        force=True,
        update_global=False,
        groups_only=False,
    )

    await message.answer("✅ Скинуто! Дроп для цього чату перезапущено.\n\n" + _format_drop_result("поточний чат", result))


@router.message()
async def skip_unhandled_messages(message: Message):
    await _register_message_chat(message)


@router.callback_query()
async def skip_unhandled_callbacks(callback: CallbackQuery):
    await _register_callback_chat(callback)
    await _safe_callback_answer(callback)
