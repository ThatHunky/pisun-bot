import datetime
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.database import Database
from src.scheduler import settle_game_event
from src.utils import get_utc_now


class _Dice:
    def __init__(self, value: int):
        self.value = value


class _Message:
    def __init__(self, message_id: int, dice_value: int = 0):
        self.message_id = message_id
        self.dice = _Dice(dice_value)


class FakeBot:
    def __init__(self, dice_values=None):
        self.dice_values = list(dice_values or [])
        self.sent = []
        self._msg_seq = 200

    async def send_message(self, chat_id, text, **kwargs):
        self.sent.append((chat_id, text, kwargs))
        self._msg_seq += 1
        return _Message(self._msg_seq)

    async def send_dice(self, chat_id, emoji="🎲"):
        if self.dice_values:
            value = self.dice_values.pop(0)
        else:
            value = 3
        self._msg_seq += 1
        return _Message(self._msg_seq, dice_value=value)


class GameEventsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "pisun_events_test.db"
        self.db = Database(str(self.db_path))
        await self.db.init()

    async def asyncTearDown(self):
        self.tmpdir.cleanup()

    async def test_duel_double_tie_results_in_no_delta(self):
        chat_id = -100100
        await self.db.register_chat(chat_id, "group")
        await self.db.update_user(1, chat_id, "A", 20.0, 0, None, 0.0, "2026-W1")
        await self.db.update_user(2, chat_id, "B", 20.0, 0, None, 0.0, "2026-W1")

        created = (get_utc_now() - datetime.timedelta(minutes=3)).isoformat(timespec="seconds")
        expired = (get_utc_now() - datetime.timedelta(minutes=1)).isoformat(timespec="seconds")
        await self.db.create_game_event(
            event_id="evt-duel-tie",
            chat_id=chat_id,
            event_type="duel",
            stake=5,
            status="active",
            created_at=created,
            expires_at=expired,
        )
        await self.db.join_game_event("evt-duel-tie", 1, "A")
        await self.db.join_game_event("evt-duel-tie", 2, "B")

        bot = FakeBot(dice_values=[3, 3, 4, 4])
        await settle_game_event(bot, self.db, "evt-duel-tie")

        event = await self.db.get_game_event("evt-duel-tie")
        self.assertEqual(event["status"], "settled")
        self.assertIsNone(event["winner_user_id"])

        u1 = await self.db.get_user(1, chat_id)
        u2 = await self.db.get_user(2, chat_id)
        self.assertEqual(u1[0], 20.0)
        self.assertEqual(u2[0], 20.0)

    async def test_trap_applies_winner_and_loser_payouts(self):
        chat_id = -100200
        await self.db.register_chat(chat_id, "group")
        await self.db.update_user(1, chat_id, "A", 20.0, 0, None, 0.0, "2026-W1")
        await self.db.update_user(2, chat_id, "B", 20.0, 0, None, 0.0, "2026-W1")

        created = (get_utc_now() - datetime.timedelta(minutes=3)).isoformat(timespec="seconds")
        expired = (get_utc_now() - datetime.timedelta(minutes=1)).isoformat(timespec="seconds")
        await self.db.create_game_event(
            event_id="evt-trap",
            chat_id=chat_id,
            event_type="trap",
            stake=5,
            status="active",
            created_at=created,
            expires_at=expired,
        )
        await self.db.join_game_event("evt-trap", 1, "A", choice="A")
        await self.db.join_game_event("evt-trap", 2, "B", choice="B")

        bot = FakeBot()
        with patch("src.scheduler.random.choice", return_value="A"):
            await settle_game_event(bot, self.db, "evt-trap")

        event = await self.db.get_game_event("evt-trap")
        self.assertEqual(event["status"], "settled")
        self.assertEqual(event["winner_user_id"], 1)

        u1 = await self.db.get_user(1, chat_id)
        u2 = await self.db.get_user(2, chat_id)
        self.assertEqual(u1[0], 30.0)
        self.assertEqual(u2[0], 15.0)

        history_a = await self.db.get_user_history(1, chat_id, limit=5)
        history_b = await self.db.get_user_history(2, chat_id, limit=5)
        self.assertEqual(history_a[0][2], "event_trap")
        self.assertEqual(history_b[0][2], "event_trap")

    async def test_duel_with_legacy_extra_entries_uses_first_two_only(self):
        chat_id = -100300
        await self.db.register_chat(chat_id, "group")
        await self.db.update_user(1, chat_id, "A", 20.0, 0, None, 0.0, "2026-W1")
        await self.db.update_user(2, chat_id, "B", 20.0, 0, None, 0.0, "2026-W1")
        await self.db.update_user(3, chat_id, "C", 20.0, 0, None, 0.0, "2026-W1")

        created = (get_utc_now() - datetime.timedelta(minutes=3)).isoformat(timespec="seconds")
        expired = (get_utc_now() - datetime.timedelta(minutes=1)).isoformat(timespec="seconds")
        await self.db.create_game_event(
            event_id="evt-duel-legacy",
            chat_id=chat_id,
            event_type="duel",
            stake=5,
            status="active",
            created_at=created,
            expires_at=expired,
        )
        await self.db.join_game_event("evt-duel-legacy", 1, "A")
        await self.db.join_game_event("evt-duel-legacy", 2, "B")
        await self.db.join_game_event("evt-duel-legacy", 3, "C")

        bot = FakeBot(dice_values=[6, 1])
        await settle_game_event(bot, self.db, "evt-duel-legacy")

        event = await self.db.get_game_event("evt-duel-legacy")
        self.assertEqual(event["status"], "settled")
        self.assertEqual(event["winner_user_id"], 1)

        u1 = await self.db.get_user(1, chat_id)
        u2 = await self.db.get_user(2, chat_id)
        u3 = await self.db.get_user(3, chat_id)
        self.assertEqual(u1[0], 25.0)
        self.assertEqual(u2[0], 15.0)
        self.assertEqual(u3[0], 20.0)


if __name__ == "__main__":
    unittest.main()
