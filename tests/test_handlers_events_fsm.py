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
    def __init__(self):
        self._state = None
        self._data = {}

    async def clear(self):
        self._state = None
        self._data = {}

    async def set_state(self, state):
        self._state = getattr(state, "state", state)

    async def get_state(self):
        return self._state

    async def update_data(self, **kwargs):
        self._data.update(kwargs)

    async def get_data(self):
        return dict(self._data)


class FakeMessage:
    def __init__(self, chat_id: int, user_id: int, text: str, chat_type: str = "group", full_name: str = "User"):
        self.chat = SimpleNamespace(id=chat_id, type=chat_type)
        self.from_user = SimpleNamespace(id=user_id, full_name=full_name)
        self.text = text
        self.answers = []
        self.edits = []
        self.reply_markup = None

    async def answer(self, text, **kwargs):
        self.answers.append((text, kwargs))
        return SimpleNamespace(message_id=len(self.answers) + 100)

    async def edit_text(self, text, **kwargs):
        self.text = text
        if "reply_markup" in kwargs:
            self.reply_markup = kwargs["reply_markup"]
        self.edits.append((text, kwargs))
        return self


class FakeCallback:
    def __init__(self, data: str, message: FakeMessage, user_id: int, full_name: str = "User"):
        self.data = data
        self.message = message
        self.from_user = SimpleNamespace(id=user_id, full_name=full_name)
        self.answers = []
        self.id = f"cb-{user_id}"

    async def answer(self, text=None, show_alert=False):
        self.answers.append((text, show_alert))


class FakeBot:
    def __init__(self, status: str = "member"):
        self.status = status

    async def get_chat_member(self, chat_id, user_id):
        return SimpleNamespace(status=self.status)


class EventFSMHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "handlers_fsm_test.db"
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

    async def test_non_admin_event_command_starts_fsm(self):
        message = FakeMessage(chat_id=-1001, user_id=11, text="/event")
        bot = FakeBot(status="member")
        state = FakeState()

        await handlers.cmd_event(message, bot, state)

        self.assertEqual(await state.get_state(), handlers.EventCreateStates.waiting_event_type.state)
        self.assertTrue(message.answers)
        text, kwargs = message.answers[-1]
        self.assertIn("Обери тип події", text)
        keyboard = kwargs.get("reply_markup")
        callback_data = [btn.callback_data for row in keyboard.inline_keyboard for btn in row]
        self.assertIn("eventfsm:type:duel", callback_data)
        self.assertIn("eventfsm:type:jackpot", callback_data)
        self.assertIn("eventfsm:type:trap", callback_data)
        self.assertIn("eventfsm:cancel", callback_data)

    async def test_non_admin_event_command_blocked_by_cooldown_before_fsm(self):
        chat_id = -1009
        await self.db.register_chat(chat_id, "group")
        await self.db.set_last_manual_event_at(chat_id, get_utc_now().isoformat(timespec="seconds"))

        message = FakeMessage(chat_id=chat_id, user_id=19, text="/event")
        bot = FakeBot(status="member")
        state = FakeState()

        await handlers.cmd_event(message, bot, state)

        self.assertIsNone(await state.get_state())
        self.assertIn("Зачекай", message.answers[-1][0])

    async def test_fsm_type_and_stake_create_event(self):
        message = FakeMessage(chat_id=-1002, user_id=22, text="/event")
        bot = FakeBot(status="member")
        state = FakeState()
        await self.db.update_user(22, -1002, "User", 30.0, 0, None, 0.0, "2026-W1")

        await handlers.cmd_event(message, bot, state)
        cb_type = FakeCallback("eventfsm:type:duel", message=message, user_id=22)
        await handlers.process_eventfsm_type(cb_type, state)

        self.assertEqual(await state.get_state(), handlers.EventCreateStates.waiting_stake.state)
        data = await state.get_data()
        self.assertEqual(data.get("event_type"), "duel")
        self.assertIn("Обери ставку", message.edits[-1][0])

        with patch("src.handlers.start_game_event", new=AsyncMock(return_value="evt-abc")) as mocked_start:
            cb_stake = FakeCallback("eventfsm:stake:10", message=message, user_id=22)
            await handlers.process_eventfsm_stake(cb_stake, state, bot)

        self.assertIsNone(await state.get_state())
        mocked_start.assert_awaited_once()
        _, kwargs = mocked_start.await_args
        self.assertEqual(kwargs["chat_id"], -1002)
        self.assertEqual(kwargs["event_type"], "duel")
        self.assertEqual(kwargs["stake"], 10)
        self.assertEqual(kwargs["creator_id"], 22)
        self.assertEqual(kwargs["creator_username"], "User")

        last_manual = await self.db.get_last_manual_event_at(-1002)
        self.assertIsNotNone(last_manual)
        self.assertIn("Подію створено", message.edits[-1][0])

    async def test_fsm_cancel_clears_state(self):
        message = FakeMessage(chat_id=-1003, user_id=33, text="/event")
        state = FakeState()
        await state.set_state(handlers.EventCreateStates.waiting_event_type)

        cb_cancel = FakeCallback("eventfsm:cancel", message=message, user_id=33)
        await handlers.process_eventfsm_cancel(cb_cancel, state)

        self.assertIsNone(await state.get_state())
        self.assertIn("скасовано", message.edits[-1][0].lower())

    async def test_legacy_event_args_still_work(self):
        message = FakeMessage(chat_id=-1004, user_id=44, text="/event jackpot 20")
        bot = FakeBot(status="member")
        state = FakeState()

        with patch("src.handlers.start_game_event", new=AsyncMock(return_value="evt-jp")) as mocked_start:
            await handlers.cmd_event(message, bot, state)

        mocked_start.assert_awaited_once()
        _, kwargs = mocked_start.await_args
        self.assertEqual(kwargs["event_type"], "jackpot")
        self.assertEqual(kwargs["stake"], 20)
        self.assertEqual(message.answers, [])

    async def test_invalid_fsm_payload_is_rejected(self):
        message = FakeMessage(chat_id=-1005, user_id=55, text="/event")
        bot = FakeBot(status="member")
        state = FakeState()

        await state.set_state(handlers.EventCreateStates.waiting_stake)
        await state.update_data(event_type="duel")

        cb_invalid = FakeCallback("eventfsm:stake:999", message=message, user_id=55)
        await handlers.process_eventfsm_stake(cb_invalid, state, bot)

        self.assertIsNone(await state.get_state())
        self.assertTrue(cb_invalid.answers)
        self.assertIn("Доступні ставки", cb_invalid.answers[-1][0])

    async def test_manual_duel_requires_creator_length(self):
        message = FakeMessage(chat_id=-1006, user_id=56, text="/event duel 20", full_name="Low")
        bot = FakeBot(status="member")
        state = FakeState()

        with patch("src.handlers.start_game_event", new=AsyncMock(return_value="evt-low")) as mocked_start:
            await handlers.cmd_event(message, bot, state)

        mocked_start.assert_not_awaited()
        self.assertTrue(message.answers)
        self.assertIn("Недостатньо довжини", message.answers[-1][0])

    async def test_duel_callback_allows_only_one_acceptor(self):
        chat_id = -1010
        await self.db.register_chat(chat_id, "group")
        await self.db.update_user(1, chat_id, "Caller", 30.0, 0, None, 0.0, "2026-W1")
        await self.db.update_user(2, chat_id, "User2", 30.0, 0, None, 0.0, "2026-W1")
        await self.db.update_user(3, chat_id, "User3", 30.0, 0, None, 0.0, "2026-W1")

        created = get_utc_now().isoformat(timespec="seconds")
        expires = (get_utc_now() + datetime.timedelta(minutes=5)).isoformat(timespec="seconds")
        await self.db.create_game_event(
            event_id="evt-duel-cb",
            chat_id=chat_id,
            event_type="duel",
            stake=10,
            status="active",
            created_at=created,
            expires_at=expires,
        )
        await self.db.join_game_event("evt-duel-cb", 1, "Caller")

        message = FakeMessage(chat_id=chat_id, user_id=1, text="⚔️ Duel")
        message.reply_markup = object()

        cb_second = FakeCallback("event:evt-duel-cb:join", message=message, user_id=2, full_name="User2")
        await handlers.process_game_event_callback(cb_second)
        self.assertIn("Дуель прийнято", cb_second.answers[-1][0])
        self.assertIsNone(message.reply_markup)

        cb_third = FakeCallback("event:evt-duel-cb:join", message=message, user_id=3, full_name="User3")
        await handlers.process_game_event_callback(cb_third)
        self.assertIn("Дуель уже прийнята", cb_third.answers[-1][0])

        entries = await self.db.get_game_event_entries("evt-duel-cb")
        self.assertEqual(len(entries), 2)


if __name__ == "__main__":
    unittest.main()
