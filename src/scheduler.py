import datetime
import logging
import random
import uuid
from typing import Any, Dict, List, Optional, Sequence

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from src.database import Database
from src.utils import (
    EVENT_DURATIONS_SEC,
    EVENT_LABELS,
    EVENT_TYPES,
    KYIV_TZ,
    PIHV_VARIANTS,
    QUIET_HOURS_END,
    STAKE_TIERS,
    get_kyiv_today,
    get_utc_now,
    get_utc_now_iso,
    is_same_week,
    send_message_with_retry,
)

logger = logging.getLogger(__name__)


def _classify_permanent_send_error(exc: Exception) -> Optional[str]:
    message = str(exc).lower()
    if isinstance(exc, TelegramForbiddenError):
        if "kicked" in message:
            return "bot_kicked"
        if "blocked" in message:
            return "bot_blocked"
        return "forbidden"

    if isinstance(exc, TelegramBadRequest):
        if "chat not found" in message:
            return "chat_not_found"
        if "group chat was upgraded" in message:
            # Migration path should remap IDs; do not disable chat permanently.
            return None

    if "bot was kicked" in message:
        return "bot_kicked"
    if "bot was blocked" in message:
        return "bot_blocked"
    if "chat not found" in message:
        return "chat_not_found"
    return None


async def send_weekly_bonus(
    bot: Bot,
    db: Database,
    chat_ids: Optional[Sequence[int]] = None,
    force: bool = False,
    update_global: bool = True,
    groups_only: bool = True,
) -> Dict[str, Any]:
    """
    Sends the ASCII art bonus message to all active chats.
    If not forced, checks if it already happened this week.
    """
    result: Dict[str, Any] = {
        "attempted": 0,
        "sent": 0,
        "failed": 0,
        "deactivated": 0,
        "skipped_reason": None,
    }
    today = get_kyiv_today()
    if not force:
        state = await db.get_event_state("weekly_pihv")
        if state:
            last_run_str, _, _ = state
            if last_run_str:
                try:
                    if isinstance(last_run_str, str):
                        last_run = datetime.date.fromisoformat(last_run_str)
                    else:
                        last_run = last_run_str

                    if is_same_week(last_run, today):
                        logger.info("Weekly bonus already sent this week. Skipping.")
                        result["skipped_reason"] = "already_sent_this_week"
                        return result
                except Exception as exc:
                    logger.error("Error parsing last_run date: %s", exc)

    if chat_ids is None:
        target_chat_ids = await db.get_drop_chats(groups_only=groups_only)
    else:
        target_chat_ids = list(dict.fromkeys(chat_ids))

    if not target_chat_ids:
        logger.info("No eligible chats for weekly_pihv drop. groups_only=%s", groups_only)
        result["skipped_reason"] = "no_eligible_chats"
        return result

    logger.info(
        "weekly_pihv drop started: force=%s update_global=%s groups_only=%s targets=%d",
        force,
        update_global,
        groups_only,
        len(target_chat_ids),
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Вставити! 👉👌", callback_data="insert_bonus")]]
    )

    for chat_id in target_chat_ids:
        result["attempted"] += 1
        try:
            ascii_art = random.choice(PIHV_VARIANTS)
            await send_message_with_retry(
                bot,
                chat_id,
                (
                    "🚨 **ЕКСТРЕНИЙ ВИПУСК!** 🚨\n\n"
                    "З'явилася нічийна піхва! Будь першим, хто вставить!\n```\n"
                    f"{ascii_art}\n"
                    "```"
                ),
                reply_markup=keyboard,
                parse_mode="Markdown",
            )
            await db.set_chat_event_state(chat_id, "weekly_pihv", True)
            result["sent"] += 1
        except Exception as exc:
            result["failed"] += 1
            permanent_reason = _classify_permanent_send_error(exc)
            if permanent_reason:
                await db.disable_chat_drops(chat_id, permanent_reason)
                await db.set_chat_event_state(chat_id, "weekly_pihv", False)
                result["deactivated"] += 1
                logger.warning(
                    "Deactivated chat %s for weekly drops. reason=%s error=%s",
                    chat_id,
                    permanent_reason,
                    exc,
                )
            else:
                logger.error("Failed to send bonus to %s: %s", chat_id, exc)

    if update_global and result["sent"] > 0:
        await db.set_event_state("weekly_pihv", today, False)
    elif update_global and result["sent"] == 0:
        logger.info("weekly_pihv not marked as sent globally: sent=%d", result["sent"])

    logger.info(
        "weekly_pihv drop finished: attempted=%d sent=%d failed=%d deactivated=%d skipped_reason=%s",
        result["attempted"],
        result["sent"],
        result["failed"],
        result["deactivated"],
        result["skipped_reason"],
    )
    return result


def _event_source(event_type: str) -> str:
    if event_type == "duel":
        return "event_duel"
    if event_type == "jackpot":
        return "event_jackpot"
    return "event_trap"


def _event_keyboard(event_id: str, event_type: str) -> InlineKeyboardMarkup:
    if event_type == "duel":
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Прийняти дуель", callback_data=f"event:{event_id}:join")]
            ]
        )

    if event_type == "jackpot":
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Приєднатися", callback_data=f"event:{event_id}:join")]
            ]
        )

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="A", callback_data=f"event:{event_id}:choice:A"),
                InlineKeyboardButton(text="B", callback_data=f"event:{event_id}:choice:B"),
                InlineKeyboardButton(text="C", callback_data=f"event:{event_id}:choice:C"),
            ]
        ]
    )


def _event_intro(event_type: str, stake: int, duration: int, auto: bool) -> str:
    prefix = "🎰 Автоподія" if auto else "🎮 Нова подія"
    if event_type == "duel":
        rules = (
            "Дуель 1v1 (макс 2 гравці). "
            "Для ручної дуелі автор уже в грі, потрібен лише один суперник. "
            "Після таймера обидва кидають кубик: переможець +ставка, програвший -ставка."
        )
    elif event_type == "jackpot":
        rules = "Усі учасники кидають кубик. Найвищий забирає банк, інші платять ставку."
    else:
        rules = "Обери A/B/C. Випадковий слот виграє +2*ставка, інші -ставка."

    return (
        f"{prefix}: <b>{EVENT_LABELS[event_type]}</b>\n"
        f"Ставка: <b>{stake} см</b>\n"
        f"Час на участь: <b>{duration} сек</b>\n\n"
        f"{rules}"
    )


async def start_game_event(
    bot: Bot,
    db: Database,
    chat_id: int,
    event_type: str,
    stake: int,
    auto: bool = False,
    creator_id: Optional[int] = None,
    creator_username: Optional[str] = None,
) -> Optional[str]:
    if event_type not in EVENT_TYPES or stake not in STAKE_TIERS:
        return None

    active = await db.get_active_game_event(chat_id)
    if active:
        return None

    duration = EVENT_DURATIONS_SEC[event_type]
    created_at = get_utc_now()
    expires_at = created_at + datetime.timedelta(seconds=duration)
    event_id = uuid.uuid4().hex[:12]

    payload = {
        "auto": bool(auto),
        "creator_id": creator_id,
        "duration_sec": duration,
    }

    await db.create_game_event(
        event_id=event_id,
        chat_id=chat_id,
        event_type=event_type,
        stake=stake,
        status="active",
        created_at=created_at.isoformat(timespec="seconds"),
        expires_at=expires_at.isoformat(timespec="seconds"),
        payload=payload,
    )

    try:
        if event_type == "duel" and creator_id is not None:
            fallback_username = f"user_{creator_id}"
            effective_username = (creator_username or "").strip() or fallback_username
            join_status = await db.join_game_event_limited(
                event_id=event_id,
                user_id=creator_id,
                username=effective_username,
                max_participants=2,
            )
            if join_status != "joined":
                raise RuntimeError(f"failed to auto-enroll duel creator: status={join_status}")

        message = await bot.send_message(
            chat_id,
            _event_intro(event_type, stake, duration, auto),
            reply_markup=_event_keyboard(event_id, event_type),
            parse_mode="HTML",
        )
        message_id = getattr(message, "message_id", None)
        if isinstance(message_id, int):
            await db.set_game_event_message_id(event_id, message_id)
        return event_id
    except Exception as exc:
        logger.error("Failed to start game event %s in chat %s: %s", event_type, chat_id, exc)
        await db.set_event_status(event_id, "canceled", payload={"reason": "send_failed"})
        return None


async def start_random_game_event(bot: Bot, db: Database, chat_id: int, auto: bool = True) -> Optional[str]:
    return await start_game_event(
        bot=bot,
        db=db,
        chat_id=chat_id,
        event_type=random.choice(EVENT_TYPES),
        stake=random.choice(STAKE_TIERS),
        auto=auto,
        creator_id=None,
    )


async def _roll_dice_value(bot: Bot, chat_id: int) -> int:
    send_dice = getattr(bot, "send_dice", None)
    if callable(send_dice):
        try:
            msg = await send_dice(chat_id, emoji="🎲")
            dice = getattr(msg, "dice", None)
            value = getattr(dice, "value", None)
            if isinstance(value, int):
                return value
        except Exception:
            pass
    return random.randint(1, 6)


def _current_week_id(today: datetime.date) -> str:
    return f"{today.isocalendar()[0]}-W{today.isocalendar()[1]}"


async def _apply_user_delta(
    db: Database,
    user_id: int,
    chat_id: int,
    username: str,
    delta: float,
    source: str,
    meta: Optional[Dict[str, Any]] = None,
) -> float:
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

    proposed_length = round(length + float(delta), 1)
    new_length = round(max(0.0, proposed_length), 1)
    actual_delta = round(new_length - length, 1)
    new_weekly_length = round(weekly_length + actual_delta, 1)

    await db.update_user(user_id, chat_id, username, new_length, count, last_measure, new_weekly_length, last_reset_week)
    await db.add_measurement(user_id, chat_id, actual_delta, new_length, source, meta=meta)
    return actual_delta


async def _cancel_event_with_message(
    bot: Bot,
    db: Database,
    event: Dict[str, Any],
    reason_code: str,
    reason_text: str,
):
    event_id = event["event_id"]
    chat_id = event["chat_id"]
    event_label = EVENT_LABELS.get(event.get("event_type", ""), "Подія")
    cancel_text = f"⛔ {event_label} скасовано: {reason_text}."

    await db.set_event_status(event_id, "canceled", payload={"reason": reason_code})

    deleted = await _delete_event_message(bot, chat_id, event.get("message_id"))
    if deleted:
        await bot.send_message(chat_id, cancel_text)
        return

    message_id = event.get("message_id")
    if isinstance(message_id, int):
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=cancel_text,
                reply_markup=None,
            )
            return
        except TelegramBadRequest as exc:
            lowered = str(exc).lower()
            if "message is not modified" in lowered:
                return
            logger.warning("Failed to edit canceled event message event_id=%s: %s", event_id, exc)
        except Exception as exc:
            logger.warning("Failed to edit canceled event message event_id=%s: %s", event_id, exc)

    await bot.send_message(chat_id, cancel_text)


async def _delete_event_message(bot: Bot, chat_id: int, message_id: Any) -> bool:
    if not isinstance(message_id, int):
        return False
    delete_message = getattr(bot, "delete_message", None)
    if not callable(delete_message):
        return False

    try:
        await delete_message(chat_id=chat_id, message_id=message_id)
        return True
    except TelegramBadRequest as exc:
        lowered = str(exc).lower()
        if (
            "message to delete not found" in lowered
            or "message can't be deleted" in lowered
            or "message identifier is not specified" in lowered
            or "message id is invalid" in lowered
        ):
            logger.info("Could not delete event message chat=%s message_id=%s: %s", chat_id, message_id, exc)
            return False
        logger.warning("Failed to delete event message chat=%s message_id=%s: %s", chat_id, message_id, exc)
        return False
    except Exception as exc:
        logger.warning("Failed to delete event message chat=%s message_id=%s: %s", chat_id, message_id, exc)
        return False


async def _settle_duel(bot: Bot, db: Database, event: Dict[str, Any], entries: Sequence[Dict[str, Any]]):
    event_id = event["event_id"]
    chat_id = event["chat_id"]
    stake = int(event["stake"])

    if len(entries) < 2:
        await _cancel_event_with_message(bot, db, event, "not_enough_players", "недостатньо учасників")
        return

    fighters = list(entries[:2])
    selected_note = ""
    if len(entries) > 2:
        selected_note = "\n(Було більше 2 учасників, враховано лише перших двох за часом входу.)"

    p1, p2 = fighters[0], fighters[1]
    r1 = await _roll_dice_value(bot, chat_id)
    r2 = await _roll_dice_value(bot, chat_id)
    await db.record_event_dice(event_id, p1["user_id"], r1)
    await db.record_event_dice(event_id, p2["user_id"], r2)

    reroll_note = ""
    if r1 == r2:
        rr1 = await _roll_dice_value(bot, chat_id)
        rr2 = await _roll_dice_value(bot, chat_id)
        await db.record_event_dice(event_id, p1["user_id"], rr1)
        await db.record_event_dice(event_id, p2["user_id"], rr2)
        reroll_note = f"\nПерекид: {p1['username']}={rr1}, {p2['username']}={rr2}."
        r1, r2 = rr1, rr2

    if r1 == r2:
        await db.set_event_status(event_id, "settled", payload={"result": "double_tie"})
        await bot.send_message(
            chat_id,
            (
                f"⚔️ Дуель завершена нічиєю.{selected_note}\n"
                f"Перший кидок: {p1['username']}={r1}, {p2['username']}={r2}.{reroll_note}\n"
                "Змін довжини немає."
            ),
        )
        await _delete_event_message(bot, chat_id, event.get("message_id"))
        return

    winner = p1 if r1 > r2 else p2
    loser = p2 if winner is p1 else p1
    winner_roll = r1 if winner is p1 else r2
    loser_roll = r2 if winner is p1 else r1

    w_delta = await _apply_user_delta(
        db,
        winner["user_id"],
        chat_id,
        winner["username"],
        float(stake),
        source="event_duel",
        meta={"event_id": event_id, "role": "winner", "stake": stake},
    )
    l_delta = await _apply_user_delta(
        db,
        loser["user_id"],
        chat_id,
        loser["username"],
        float(-stake),
        source="event_duel",
        meta={"event_id": event_id, "role": "loser", "stake": stake},
    )

    await db.set_event_status(event_id, "settled", winner_user_id=winner["user_id"], payload={"winner_roll": winner_roll, "loser_roll": loser_roll})
    await bot.send_message(
        chat_id,
        (
            f"⚔️ Дуель завершена{selected_note}\n"
            f"{winner['username']} ({winner_roll}) переміг {loser['username']} ({loser_roll}).\n"
            f"{winner['username']}: {w_delta:+.1f} см\n"
            f"{loser['username']}: {l_delta:+.1f} см"
        ),
    )
    await _delete_event_message(bot, chat_id, event.get("message_id"))


def _split_pool_evenly(pool_amount: float, winners_count: int) -> List[float]:
    if winners_count <= 0:
        return []
    pool_tenths = int(round(pool_amount * 10))
    base_share = pool_tenths // winners_count
    remainder = pool_tenths % winners_count
    shares_tenths = [base_share + (1 if i < remainder else 0) for i in range(winners_count)]
    return [round(share / 10.0, 1) for share in shares_tenths]


async def _settle_jackpot(bot: Bot, db: Database, event: Dict[str, Any], entries: Sequence[Dict[str, Any]]):
    event_id = event["event_id"]
    chat_id = event["chat_id"]
    stake = int(event["stake"])

    if len(entries) < 2:
        await _cancel_event_with_message(bot, db, event, "not_enough_players", "недостатньо учасників")
        return

    participants = list(entries)
    rolls: Dict[int, int] = {}
    for player in participants:
        value = await _roll_dice_value(bot, chat_id)
        rolls[player["user_id"]] = value
        await db.record_event_dice(event_id, player["user_id"], value)

    max_roll = max(rolls.values())
    winners = [player for player in participants if rolls[player["user_id"]] == max_roll]
    winner_ids = [player["user_id"] for player in winners]
    is_tie = len(winners) > 1
    winner_gain_map: Dict[int, float] = {}
    if is_tie:
        total_pool = float(stake * (len(participants) - len(winners)))
        shares = _split_pool_evenly(total_pool, len(winners))
        for idx, winner in enumerate(winners):
            winner_gain_map[winner["user_id"]] = shares[idx]
    else:
        winner_gain_map[winner_ids[0]] = float(stake * (len(participants) - 1))

    payouts = []
    for player in participants:
        if player["user_id"] in winner_gain_map:
            winner_gain = winner_gain_map[player["user_id"]]
            delta = await _apply_user_delta(
                db,
                player["user_id"],
                chat_id,
                player["username"],
                winner_gain,
                source="event_jackpot",
                meta={
                    "event_id": event_id,
                    "role": "winner",
                    "stake": stake,
                    "participants": len(participants),
                    "tie_for_max": is_tie,
                    "winner_count": len(winners),
                },
            )
        else:
            delta = await _apply_user_delta(
                db,
                player["user_id"],
                chat_id,
                player["username"],
                float(-stake),
                source="event_jackpot",
                meta={
                    "event_id": event_id,
                    "role": "loser",
                    "stake": stake,
                    "participants": len(participants),
                    "tie_for_max": is_tie,
                    "winner_count": len(winners),
                },
            )
        payouts.append((player["username"], delta, rolls.get(player["user_id"], 0)))

    payload = {
        "winner_roll": max_roll,
        "participants": len(participants),
        "tie_for_max": is_tie,
        "winner_count": len(winners),
        "winner_user_ids": winner_ids,
    }
    winner_user_id = winner_ids[0] if len(winner_ids) == 1 else None

    await db.set_event_status(
        event_id,
        "settled",
        winner_user_id=winner_user_id,
        payload=payload,
    )

    payout_lines = [f"{name}: кидок {roll}, {delta:+.1f} см" for name, delta, roll in payouts]
    if is_tie:
        winners_text = ", ".join(player["username"] for player in winners)
        header = f"🎰 Джекпот розіграно. Переможці (нічия): {winners_text}"
    else:
        header = f"🎰 Джекпот розіграно. Переможець: {winners[0]['username']}"
    await bot.send_message(
        chat_id,
        (
            f"{header}\n"
            + "\n".join(payout_lines)
        ),
    )
    await _delete_event_message(bot, chat_id, event.get("message_id"))


async def _settle_trap(bot: Bot, db: Database, event: Dict[str, Any], entries: Sequence[Dict[str, Any]]):
    event_id = event["event_id"]
    chat_id = event["chat_id"]
    stake = int(event["stake"])

    if not entries:
        await _cancel_event_with_message(bot, db, event, "no_entries", "ніхто не взяв участь")
        return

    winning_choice = random.choice(["A", "B", "C"])
    lines = []
    winner_id = None

    for player in entries:
        player_choice = (player.get("choice") or "-").upper()
        if player_choice == winning_choice:
            delta = await _apply_user_delta(
                db,
                player["user_id"],
                chat_id,
                player["username"],
                float(stake * 2),
                source="event_trap",
                meta={"event_id": event_id, "choice": player_choice, "winning_choice": winning_choice, "stake": stake},
            )
            winner_id = winner_id or player["user_id"]
        else:
            delta = await _apply_user_delta(
                db,
                player["user_id"],
                chat_id,
                player["username"],
                float(-stake),
                source="event_trap",
                meta={"event_id": event_id, "choice": player_choice, "winning_choice": winning_choice, "stake": stake},
            )
        lines.append(f"{player['username']} ({player_choice}): {delta:+.1f} см")

    await db.set_event_status(
        event_id,
        "settled",
        winner_user_id=winner_id,
        payload={"winning_choice": winning_choice},
    )
    await bot.send_message(
        chat_id,
        (
            f"🪤 Пастка спрацювала. Виграшна літера: {winning_choice}\n"
            + "\n".join(lines)
        ),
    )
    await _delete_event_message(bot, chat_id, event.get("message_id"))


async def settle_game_event(bot: Bot, db: Database, event_id: str):
    event = await db.get_game_event(event_id)
    if not event or event.get("status") != "active":
        return

    entries = await db.get_game_event_entries(event_id)
    event_type = event.get("event_type")

    if event_type == "duel":
        await _settle_duel(bot, db, event, entries)
        return
    if event_type == "jackpot":
        await _settle_jackpot(bot, db, event, entries)
        return
    await _settle_trap(bot, db, event, entries)


async def settle_due_game_events(bot: Bot, db: Database) -> int:
    now_iso = get_utc_now_iso()
    due_events = await db.get_due_active_game_events(now_iso)
    settled_count = 0
    for event in due_events:
        try:
            await settle_game_event(bot, db, event["event_id"])
            settled_count += 1
        except Exception as exc:
            logger.error("Failed to settle event %s: %s", event.get("event_id"), exc)
    return settled_count


async def assign_daily_event_times(db: Database) -> int:
    today = get_kyiv_today()
    today_iso = today.isoformat()
    candidates = await db.get_auto_assign_chats(today_iso)
    if not candidates:
        return 0

    start_hour = max(QUIET_HOURS_END, 7)
    start_dt = datetime.datetime.combine(today, datetime.time(hour=start_hour, minute=0, tzinfo=KYIV_TZ))
    end_dt = datetime.datetime.combine(today, datetime.time(hour=23, minute=59, tzinfo=KYIV_TZ))
    total_minutes = max(1, int((end_dt - start_dt).total_seconds() // 60))

    assigned = 0
    for chat_id in candidates:
        minute_offset = random.randint(0, total_minutes)
        event_dt_kyiv = start_dt + datetime.timedelta(minutes=minute_offset)
        event_dt_utc_iso = event_dt_kyiv.astimezone(datetime.timezone.utc).isoformat(timespec="seconds")
        await db.set_next_auto_event_at(chat_id, event_dt_utc_iso)
        assigned += 1
    return assigned


async def dispatch_due_auto_events(bot: Bot, db: Database) -> int:
    now_iso = get_utc_now_iso()
    due_chat_ids = await db.get_due_auto_event_chats(now_iso)
    if not due_chat_ids:
        return 0

    triggered = 0
    today_iso = get_kyiv_today().isoformat()
    for chat_id in due_chat_ids:
        active_event = await db.get_active_game_event(chat_id)
        if active_event:
            continue
        event_id = await start_random_game_event(bot, db, chat_id, auto=True)
        if not event_id:
            continue
        await db.set_last_auto_event_date(chat_id, today_iso)
        await db.set_next_auto_event_at(chat_id, None)
        triggered += 1
    return triggered


def setup_scheduler(bot: Bot, db: Database):
    scheduler = AsyncIOScheduler(timezone=KYIV_TZ)

    # Weekly pihv schedule remains random once per week.
    def schedule_next_weekly_bonus():
        day = random.randint(0, 6)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)

        scheduler.add_job(
            send_weekly_bonus,
            "cron",
            day_of_week=day,
            hour=hour,
            minute=minute,
            args=[bot, db],
            kwargs={"groups_only": True},
            id="weekly_bonus",
            replace_existing=True,
        )
        logger.info("Next bonus scheduled for day %d at %02d:%02d Europe/Kyiv", day, hour, minute)

    schedule_next_weekly_bonus()

    scheduler.add_job(
        schedule_next_weekly_bonus,
        "interval",
        weeks=1,
        id="weekly_bonus_reschedule",
        replace_existing=True,
    )

    scheduler.add_job(
        assign_daily_event_times,
        "cron",
        hour=7,
        minute=5,
        args=[db],
        id="daily_event_assignment",
        replace_existing=True,
    )

    scheduler.add_job(
        dispatch_due_auto_events,
        "interval",
        minutes=1,
        args=[bot, db],
        id="daily_event_dispatch",
        replace_existing=True,
    )

    scheduler.add_job(
        settle_due_game_events,
        "interval",
        seconds=20,
        args=[bot, db],
        id="game_event_settlement",
        replace_existing=True,
    )

    # On startup assign today's events soon after boot in case bot starts after 07:05.
    scheduler.add_job(
        assign_daily_event_times,
        "date",
        run_date=datetime.datetime.now(KYIV_TZ) + datetime.timedelta(seconds=10),
        args=[db],
        id="daily_event_assignment_bootstrap",
        replace_existing=True,
    )

    return scheduler
