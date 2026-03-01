import datetime
import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.database import Database
from src.scheduler import (
    assign_daily_event_times,
    dispatch_due_auto_events,
    send_weekly_bonus,
    settle_due_game_events,
    start_game_event,
)
from src.utils import get_kyiv_today, get_utc_now, to_kyiv_datetime


class _Dice:
    def __init__(self, value: int):
        self.value = value


class _Message:
    def __init__(self, message_id: int, dice_value: int = 0):
        self.message_id = message_id
        self.dice = _Dice(dice_value)


class FakeBot:
    def __init__(self, failures=None, dice_values=None):
        self.failures = failures or {}
        self.sent_chat_ids = []
        self.sent_messages = []
        self.deleted_messages = []
        self.dice_values = list(dice_values or [])
        self.dice_calls = 0
        self._msg_seq = 100

    async def send_message(self, chat_id, text, **kwargs):
        error = self.failures.get(chat_id)
        if error:
            raise error
        self.sent_chat_ids.append(chat_id)
        self.sent_messages.append((chat_id, text, kwargs))
        self._msg_seq += 1
        return _Message(self._msg_seq)

    async def send_dice(self, chat_id, emoji="🎲"):
        self.dice_calls += 1
        if self.dice_values:
            value = self.dice_values.pop(0)
        else:
            value = 3
        self._msg_seq += 1
        return _Message(self._msg_seq, dice_value=value)

    async def delete_message(self, chat_id, message_id):
        self.deleted_messages.append((chat_id, message_id))
        return True


class SchedulerDropTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "pisun_scheduler_test.db"
        self.db = Database(str(self.db_path))
        await self.db.init()

    async def asyncTearDown(self):
        self.tmpdir.cleanup()

    async def test_send_weekly_bonus_groups_only_and_global_update(self):
        await self.db.register_chat(-10001, "group")
        await self.db.register_chat(101, "private")

        bot = FakeBot()
        result = await send_weekly_bonus(bot, self.db, force=True, update_global=True, groups_only=True)

        self.assertEqual(result["attempted"], 1)
        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(bot.sent_chat_ids, [-10001])

        state = await self.db.get_event_state("weekly_pihv")
        self.assertIsNotNone(state)

        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT is_active FROM chat_events WHERE chat_id = ? AND event_id = ?",
            (-10001, "weekly_pihv"),
        ).fetchone()
        conn.close()
        self.assertEqual(row[0], 1)

    async def test_no_eligible_chats_does_not_mark_global_run(self):
        await self.db.register_chat(501, "private")

        bot = FakeBot()
        result = await send_weekly_bonus(bot, self.db, force=False, update_global=True, groups_only=True)

        self.assertEqual(result["skipped_reason"], "no_eligible_chats")
        self.assertEqual(result["attempted"], 0)
        self.assertEqual(result["sent"], 0)

        state = await self.db.get_event_state("weekly_pihv")
        self.assertIsNone(state)

    async def test_weekly_lock_prevents_second_global_drop(self):
        await self.db.register_chat(-10011, "group")
        await self.db.set_event_state("weekly_pihv", get_kyiv_today(), False)

        bot = FakeBot()
        result = await send_weekly_bonus(bot, self.db, force=False, update_global=True, groups_only=True)

        self.assertEqual(result["skipped_reason"], "already_sent_this_week")
        self.assertEqual(result["attempted"], 0)
        self.assertEqual(bot.sent_chat_ids, [])

    async def test_permanent_error_deactivates_chat(self):
        await self.db.register_chat(-10021, "group")

        bot = FakeBot(failures={-10021: RuntimeError("Telegram server says - Bad Request: chat not found")})
        result = await send_weekly_bonus(bot, self.db, force=True, update_global=False, groups_only=True)

        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["deactivated"], 1)

        active_groups = await self.db.get_drop_chats(groups_only=True)
        self.assertEqual(active_groups, [])

    async def test_chat_migration_error_does_not_deactivate_chat(self):
        await self.db.register_chat(-10031, "group")

        bot = FakeBot(failures={-10031: RuntimeError("Bad Request: group chat was upgraded to a supergroup chat")})
        result = await send_weekly_bonus(bot, self.db, force=True, update_global=False, groups_only=True)

        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["deactivated"], 0)

        active_groups = await self.db.get_drop_chats(groups_only=True)
        self.assertEqual(active_groups, [-10031])

    async def test_assign_daily_event_times_sets_next_auto_event(self):
        await self.db.register_chat(-10041, "group")

        assigned = await assign_daily_event_times(self.db)
        self.assertEqual(assigned, 1)

        settings = await self.db.get_chat_settings(-10041)
        self.assertIsNotNone(settings["next_auto_event_at"])

        kyiv_dt = to_kyiv_datetime(settings["next_auto_event_at"])
        self.assertGreaterEqual(kyiv_dt.hour, 7)

    async def test_dispatch_due_auto_events_creates_event_and_marks_date(self):
        await self.db.register_chat(-10051, "group")
        past_iso = (get_utc_now() - datetime.timedelta(minutes=2)).isoformat(timespec="seconds")
        await self.db.set_next_auto_event_at(-10051, past_iso)

        bot = FakeBot()
        triggered = await dispatch_due_auto_events(bot, self.db)

        self.assertEqual(triggered, 1)
        active_event = await self.db.get_active_game_event(-10051)
        self.assertIsNotNone(active_event)

        settings = await self.db.get_chat_settings(-10051)
        self.assertEqual(settings["last_auto_event_date"], get_kyiv_today().isoformat())
        self.assertIsNone(settings["next_auto_event_at"])

    async def test_settle_due_game_events_settles_jackpot(self):
        chat_id = -10061
        await self.db.register_chat(chat_id, "group")
        await self.db.update_user(1, chat_id, "U1", 20.0, 0, None, 0.0, "2026-W1")
        await self.db.update_user(2, chat_id, "U2", 20.0, 0, None, 0.0, "2026-W1")

        created = (get_utc_now() - datetime.timedelta(minutes=3)).isoformat(timespec="seconds")
        expired = (get_utc_now() - datetime.timedelta(minutes=1)).isoformat(timespec="seconds")
        await self.db.create_game_event(
            event_id="evt-jp",
            chat_id=chat_id,
            event_type="jackpot",
            stake=5,
            status="active",
            created_at=created,
            expires_at=expired,
            message_id=777,
        )
        await self.db.join_game_event("evt-jp", 1, "U1")
        await self.db.join_game_event("evt-jp", 2, "U2")

        bot = FakeBot(dice_values=[6, 1])
        settled = await settle_due_game_events(bot, self.db)

        self.assertEqual(settled, 1)
        event = await self.db.get_game_event("evt-jp")
        self.assertEqual(event["status"], "settled")
        self.assertEqual(event["winner_user_id"], 1)

        u1 = await self.db.get_user(1, chat_id)
        u2 = await self.db.get_user(2, chat_id)
        self.assertEqual(u1[0], 25.0)
        self.assertEqual(u2[0], 15.0)
        self.assertEqual(bot.deleted_messages, [(chat_id, 777)])

    async def test_start_game_event_prevents_parallel_active_events(self):
        chat_id = -10071
        await self.db.register_chat(chat_id, "group")

        bot = FakeBot()
        first = await start_game_event(bot, self.db, chat_id, "duel", 5, auto=False, creator_id=1)
        second = await start_game_event(bot, self.db, chat_id, "trap", 10, auto=False, creator_id=1)

        self.assertIsNotNone(first)
        self.assertIsNone(second)

    async def test_start_game_event_auto_enrolls_manual_duel_creator(self):
        chat_id = -10072
        await self.db.register_chat(chat_id, "group")

        bot = FakeBot()
        event_id = await start_game_event(
            bot,
            self.db,
            chat_id,
            "duel",
            10,
            auto=False,
            creator_id=777,
            creator_username="Caller",
        )

        self.assertIsNotNone(event_id)
        entries = await self.db.get_game_event_entries(event_id)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["user_id"], 777)
        self.assertEqual(entries[0]["username"], "Caller")

    async def test_settle_due_game_events_splits_jackpot_tie_winners(self):
        chat_id = -10073
        await self.db.register_chat(chat_id, "group")
        await self.db.update_user(1, chat_id, "U1", 20.0, 0, None, 0.0, "2026-W1")
        await self.db.update_user(2, chat_id, "U2", 20.0, 0, None, 0.0, "2026-W1")
        await self.db.update_user(3, chat_id, "U3", 20.0, 0, None, 0.0, "2026-W1")

        created = (get_utc_now() - datetime.timedelta(minutes=3)).isoformat(timespec="seconds")
        expired = (get_utc_now() - datetime.timedelta(minutes=1)).isoformat(timespec="seconds")
        await self.db.create_game_event(
            event_id="evt-jp-tie",
            chat_id=chat_id,
            event_type="jackpot",
            stake=5,
            status="active",
            created_at=created,
            expires_at=expired,
            message_id=778,
        )
        await self.db.join_game_event("evt-jp-tie", 1, "U1")
        await self.db.join_game_event("evt-jp-tie", 2, "U2")
        await self.db.join_game_event("evt-jp-tie", 3, "U3")

        bot = FakeBot(dice_values=[6, 6, 2])
        settled = await settle_due_game_events(bot, self.db)

        self.assertEqual(settled, 1)
        self.assertEqual(bot.dice_calls, 3)

        event = await self.db.get_game_event("evt-jp-tie")
        self.assertEqual(event["status"], "settled")
        self.assertIsNone(event["winner_user_id"])
        self.assertEqual(event["payload"]["winner_user_ids"], [1, 2])
        self.assertEqual(event["payload"]["winner_count"], 2)
        self.assertTrue(event["payload"]["tie_for_max"])

        u1 = await self.db.get_user(1, chat_id)
        u2 = await self.db.get_user(2, chat_id)
        u3 = await self.db.get_user(3, chat_id)
        self.assertEqual(u1[0], 22.5)
        self.assertEqual(u2[0], 22.5)
        self.assertEqual(u3[0], 15.0)

        entries = await self.db.get_game_event_entries("evt-jp-tie")
        self.assertEqual(entries[0]["dice_value"], 6)
        self.assertEqual(entries[1]["dice_value"], 6)
        self.assertEqual(entries[2]["dice_value"], 2)
        self.assertEqual(bot.deleted_messages, [(chat_id, 778)])

    async def test_settle_due_game_events_canceled_event_deletes_message(self):
        chat_id = -10074
        await self.db.register_chat(chat_id, "group")
        await self.db.update_user(1, chat_id, "U1", 20.0, 0, None, 0.0, "2026-W1")

        created = (get_utc_now() - datetime.timedelta(minutes=3)).isoformat(timespec="seconds")
        expired = (get_utc_now() - datetime.timedelta(minutes=1)).isoformat(timespec="seconds")
        await self.db.create_game_event(
            event_id="evt-duel-cancel",
            chat_id=chat_id,
            event_type="duel",
            stake=5,
            status="active",
            created_at=created,
            expires_at=expired,
            message_id=779,
        )
        await self.db.join_game_event("evt-duel-cancel", 1, "U1")

        bot = FakeBot()
        settled = await settle_due_game_events(bot, self.db)

        self.assertEqual(settled, 1)
        event = await self.db.get_game_event("evt-duel-cancel")
        self.assertEqual(event["status"], "canceled")
        self.assertEqual(bot.deleted_messages, [(chat_id, 779)])


if __name__ == "__main__":
    unittest.main()
