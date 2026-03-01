import datetime
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src import handlers
from src.database import Database
from src.utils import get_utc_now


class FakeState:
    async def clear(self):
        return None

    async def set_state(self, state):
        return None

    async def get_state(self):
        return None

    async def update_data(self, **kwargs):
        return None

    async def get_data(self):
        return {}


class FakeMessage:
    def __init__(self, chat_id: int, user_id: int, text: str):
        self.chat = SimpleNamespace(id=chat_id, type="group")
        self.from_user = SimpleNamespace(id=user_id, full_name=f"u{user_id}")
        self.text = text
        self.answers = []

    async def answer(self, text, **kwargs):
        self.answers.append((text, kwargs))
        return SimpleNamespace(message_id=len(self.answers) + 1)


class FakeBot:
    def __init__(self, status: str = "member"):
        self.status = status

    async def get_chat_member(self, chat_id, user_id):
        return SimpleNamespace(status=self.status)


class ManualEventCooldownTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "manual_cooldown_test.db"
        self.db = Database(str(self.db_path))
        await self.db.init()

        self.old_db = handlers.db
        self.old_admin_ids = set(handlers.ADMIN_IDS)
        handlers.db = self.db
        handlers.ADMIN_IDS = set()
        handlers._last_cooldown_notice_by_user_chat.clear()

    async def asyncTearDown(self):
        handlers.db = self.old_db
        handlers.ADMIN_IDS = self.old_admin_ids
        self.tmpdir.cleanup()

    async def test_non_admin_blocked_when_cooldown_active(self):
        chat_id = -3001
        await self.db.register_chat(chat_id, "group")
        now_iso = get_utc_now().isoformat(timespec="seconds")
        await self.db.set_last_manual_event_at(chat_id, now_iso)

        message = FakeMessage(chat_id=chat_id, user_id=101, text="/event duel 10")
        bot = FakeBot(status="member")
        state = FakeState()

        with patch("src.handlers.start_game_event", new=AsyncMock(return_value="evt-x")) as mocked_start:
            await handlers.cmd_event(message, bot, state)

        mocked_start.assert_not_awaited()
        self.assertIn("Зачекай", message.answers[-1][0])

    async def test_non_admin_repeated_spam_gets_single_notice(self):
        chat_id = -3010
        await self.db.register_chat(chat_id, "group")
        now_iso = get_utc_now().isoformat(timespec="seconds")
        await self.db.set_last_manual_event_at(chat_id, now_iso)

        message = FakeMessage(chat_id=chat_id, user_id=110, text="/event duel 10")
        bot = FakeBot(status="member")
        state = FakeState()

        with patch("src.handlers.start_game_event", new=AsyncMock(return_value="evt-nope")):
            await handlers.cmd_event(message, bot, state)
            await handlers.cmd_event(message, bot, state)

        self.assertEqual(len(message.answers), 1)
        self.assertIn("Зачекай", message.answers[0][0])

    async def test_non_admin_allowed_after_cooldown(self):
        chat_id = -3002
        await self.db.register_chat(chat_id, "group")
        old_iso = (get_utc_now() - datetime.timedelta(minutes=11)).isoformat(timespec="seconds")
        await self.db.set_last_manual_event_at(chat_id, old_iso)

        message = FakeMessage(chat_id=chat_id, user_id=102, text="/event trap 5")
        bot = FakeBot(status="member")
        state = FakeState()

        with patch("src.handlers.start_game_event", new=AsyncMock(return_value="evt-ok")) as mocked_start:
            await handlers.cmd_event(message, bot, state)

        mocked_start.assert_awaited_once()
        self.assertEqual(message.answers, [])

    async def test_group_admin_bypasses_cooldown(self):
        chat_id = -3003
        await self.db.register_chat(chat_id, "group")
        now_iso = get_utc_now().isoformat(timespec="seconds")
        await self.db.set_last_manual_event_at(chat_id, now_iso)

        message = FakeMessage(chat_id=chat_id, user_id=103, text="/event jackpot 20")
        bot = FakeBot(status="administrator")
        state = FakeState()

        with patch("src.handlers.start_game_event", new=AsyncMock(return_value="evt-admin")) as mocked_start:
            await handlers.cmd_event(message, bot, state)

        mocked_start.assert_awaited_once()
        self.assertEqual(message.answers, [])

    async def test_global_admin_bypasses_cooldown(self):
        chat_id = -3004
        await self.db.register_chat(chat_id, "group")
        now_iso = get_utc_now().isoformat(timespec="seconds")
        await self.db.set_last_manual_event_at(chat_id, now_iso)
        handlers.ADMIN_IDS = {104}

        message = FakeMessage(chat_id=chat_id, user_id=104, text="/event duel 10")
        bot = FakeBot(status="member")
        state = FakeState()

        with patch("src.handlers.start_game_event", new=AsyncMock(return_value="evt-global")) as mocked_start:
            await handlers.cmd_event(message, bot, state)

        mocked_start.assert_awaited_once()
        self.assertEqual(message.answers, [])


if __name__ == "__main__":
    unittest.main()
