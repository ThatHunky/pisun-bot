import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src import handlers
from src.database import Database


class FakeState:
    def __init__(self):
        self._state = None

    async def clear(self):
        self._state = None

    async def set_state(self, state):
        self._state = getattr(state, "state", state)

    async def get_state(self):
        return self._state

    async def update_data(self, **kwargs):
        return None

    async def get_data(self):
        return {}


class FakeMessage:
    def __init__(self, chat_id: int, user_id: int, text: str, chat_type: str = "group"):
        self.chat = SimpleNamespace(id=chat_id, type=chat_type)
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


class EventPermissionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "handlers_permissions_test.db"
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

    async def test_events_off_does_not_block_manual_event(self):
        chat_id = -2001
        await self.db.register_chat(chat_id, "group")
        await self.db.set_events_enabled(chat_id, False)

        message = FakeMessage(chat_id=chat_id, user_id=70, text="/event duel 10")
        bot = FakeBot(status="member")
        state = FakeState()

        with patch("src.handlers.start_game_event", new=AsyncMock(return_value="evt-open")) as mocked_start:
            await handlers.cmd_event(message, bot, state)

        mocked_start.assert_awaited_once()
        settings = await self.db.get_chat_settings(chat_id)
        self.assertFalse(settings["events_enabled"])
        self.assertEqual(message.answers, [])

    async def test_events_off_command_stays_admin_only(self):
        chat_id = -2002
        message = FakeMessage(chat_id=chat_id, user_id=71, text="/events_off")
        bot = FakeBot(status="member")

        await handlers.cmd_events_off(message, bot)

        self.assertIn("лише адмінам", message.answers[-1][0])
        settings = await self.db.get_chat_settings(chat_id)
        self.assertTrue(settings["events_enabled"])


if __name__ == "__main__":
    unittest.main()
