import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.database import Database
from src.utils import get_utc_now_iso


class DatabaseChatRegistryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "pisun_test.db"

    async def asyncTearDown(self):
        self.tmpdir.cleanup()

    async def test_backfill_chats_from_users(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            """
            CREATE TABLE users (
                user_id INTEGER,
                chat_id INTEGER,
                username TEXT,
                length REAL DEFAULT 0.0,
                measure_count INTEGER DEFAULT 0,
                last_measure DATE,
                weekly_length REAL DEFAULT 0.0,
                last_reset_week TEXT,
                PRIMARY KEY (user_id, chat_id)
            )
            """
        )
        conn.execute(
            "INSERT INTO users (user_id, chat_id, username) VALUES (?, ?, ?)",
            (1, -100123, "group_user"),
        )
        conn.execute(
            "INSERT INTO users (user_id, chat_id, username) VALUES (?, ?, ?)",
            (2, 123456, "private_user"),
        )
        conn.commit()
        conn.close()

        db = Database(str(self.db_path))
        await db.init()

        conn = sqlite3.connect(self.db_path)
        rows = conn.execute(
            "SELECT chat_id, chat_type FROM chats ORDER BY chat_id"
        ).fetchall()
        conn.close()

        self.assertEqual(rows, [(-100123, "group"), (123456, "private")])

    async def test_register_chat_reenables_auto_disabled_drop(self):
        db = Database(str(self.db_path))
        await db.init()

        await db.register_chat(-100999, "group")
        await db.disable_chat_drops(-100999, "chat_not_found")
        await db.register_chat(-100999, "supergroup")

        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT chat_type, drops_enabled, disabled_reason, disabled_at FROM chats WHERE chat_id = ?",
            (-100999,),
        ).fetchone()
        conn.close()

        self.assertEqual(row[0], "supergroup")
        self.assertEqual(row[1], 1)
        self.assertIsNone(row[2])
        self.assertIsNone(row[3])

    async def test_get_drop_chats_groups_only(self):
        db = Database(str(self.db_path))
        await db.init()

        await db.register_chat(-1001, "group")
        await db.register_chat(-1002, "supergroup")
        await db.register_chat(2001, "private")
        await db.disable_chat_drops(-1002, "bot_kicked")

        groups_only = await db.get_drop_chats(groups_only=True)
        all_enabled = await db.get_drop_chats(groups_only=False)

        self.assertEqual(groups_only, [-1001])
        self.assertEqual(all_enabled, [-1001, 2001])

    async def test_measurements_history_and_settings(self):
        db = Database(str(self.db_path))
        await db.init()

        await db.register_chat(-1001, "group")
        await db.set_drops_enabled(-1001, False)
        await db.set_events_enabled(-1001, False)

        await db.add_measurement(11, -1001, 2.5, 12.5, "pisun", meta={"a": 1}, created_at="2026-01-01T10:00:00+00:00")
        await db.add_measurement(11, -1001, -1.0, 11.5, "event_duel", created_at="2026-01-02T10:00:00+00:00")

        history = await db.get_user_history(11, -1001, limit=10)
        settings = await db.get_chat_settings(-1001)

        self.assertEqual(len(history), 2)
        self.assertEqual(history[0][0], -1.0)
        self.assertEqual(history[0][2], "event_duel")
        self.assertFalse(settings["drops_enabled"])
        self.assertFalse(settings["events_enabled"])
        self.assertIsNone(settings["last_manual_event_at"])

        await db.set_last_manual_event_at(-1001, "2026-01-03T10:00:00+00:00")
        last_manual = await db.get_last_manual_event_at(-1001)
        self.assertEqual(last_manual, "2026-01-03T10:00:00+00:00")

    async def test_game_event_crud_and_entries(self):
        db = Database(str(self.db_path))
        await db.init()

        await db.create_game_event(
            event_id="evt1",
            chat_id=-1001,
            event_type="duel",
            stake=10,
            status="active",
            created_at=get_utc_now_iso(),
            expires_at=get_utc_now_iso(),
            payload={"x": 1},
        )
        active = await db.get_active_game_event(-1001)
        self.assertIsNotNone(active)
        self.assertEqual(active["event_id"], "evt1")

        joined = await db.join_game_event("evt1", 100, "User A")
        self.assertTrue(joined)
        joined_again = await db.join_game_event("evt1", 100, "User A")
        self.assertFalse(joined_again)

        await db.record_event_choice("evt1", 100, "B")
        await db.record_event_dice("evt1", 100, 5)

        entries = await db.get_game_event_entries("evt1")
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["choice"], "B")
        self.assertEqual(entries[0]["dice_value"], 5)

        await db.set_event_status("evt1", "settled", winner_user_id=100, payload={"done": True})
        event = await db.get_game_event("evt1")
        self.assertEqual(event["status"], "settled")
        self.assertEqual(event["winner_user_id"], 100)

    async def test_join_game_event_limited_enforces_capacity(self):
        db = Database(str(self.db_path))
        await db.init()

        await db.create_game_event(
            event_id="evt-limit",
            chat_id=-1001,
            event_type="duel",
            stake=10,
            status="active",
            created_at=get_utc_now_iso(),
            expires_at=get_utc_now_iso(),
        )

        first = await db.join_game_event_limited("evt-limit", 100, "U100", max_participants=2)
        repeat_first = await db.join_game_event_limited("evt-limit", 100, "U100", max_participants=2)
        second = await db.join_game_event_limited("evt-limit", 101, "U101", max_participants=2)
        third = await db.join_game_event_limited("evt-limit", 102, "U102", max_participants=2)

        self.assertEqual(first, "joined")
        self.assertEqual(repeat_first, "already_joined")
        self.assertEqual(second, "joined")
        self.assertEqual(third, "full")

        entries = await db.get_game_event_entries("evt-limit")
        self.assertEqual(len(entries), 2)

    async def test_migrate_chat_data_moves_user_and_events(self):
        db = Database(str(self.db_path))
        await db.init()

        old_chat_id = -1001
        new_chat_id = -2001

        await db.register_chat(old_chat_id, "group")
        await db.set_last_manual_event_at(old_chat_id, "2026-01-05T08:00:00+00:00")
        await db.update_user(1, old_chat_id, "Old User", 15.0, 3, None, 2.0, "2026-W1")
        await db.add_shown_fact(1, old_chat_id, 1)
        await db.set_chat_event_state(old_chat_id, "weekly_pihv", True)
        await db.add_measurement(1, old_chat_id, 1.0, 15.0, "pisun")
        await db.create_game_event(
            event_id="evt-mig",
            chat_id=old_chat_id,
            event_type="trap",
            stake=5,
            status="active",
            created_at=get_utc_now_iso(),
            expires_at=get_utc_now_iso(),
        )

        await db.migrate_chat_data(old_chat_id, new_chat_id)

        migrated_user = await db.get_user(1, new_chat_id)
        self.assertIsNotNone(migrated_user)

        conn = sqlite3.connect(self.db_path)
        old_user = conn.execute(
            "SELECT 1 FROM users WHERE chat_id = ?",
            (old_chat_id,),
        ).fetchone()
        moved_fact = conn.execute(
            "SELECT 1 FROM user_facts WHERE chat_id = ?",
            (new_chat_id,),
        ).fetchone()
        moved_chat_event = conn.execute(
            "SELECT is_active FROM chat_events WHERE chat_id = ? AND event_id = 'weekly_pihv'",
            (new_chat_id,),
        ).fetchone()
        moved_measurement = conn.execute(
            "SELECT 1 FROM measurements WHERE chat_id = ?",
            (new_chat_id,),
        ).fetchone()
        moved_game_event = conn.execute(
            "SELECT 1 FROM game_events WHERE chat_id = ? AND event_id = 'evt-mig'",
            (new_chat_id,),
        ).fetchone()
        moved_manual = conn.execute(
            "SELECT last_manual_event_at FROM chats WHERE chat_id = ?",
            (new_chat_id,),
        ).fetchone()
        old_chat_row = conn.execute(
            "SELECT 1 FROM chats WHERE chat_id = ?",
            (old_chat_id,),
        ).fetchone()
        conn.close()

        self.assertIsNone(old_user)
        self.assertIsNotNone(moved_fact)
        self.assertEqual(moved_chat_event[0], 1)
        self.assertIsNotNone(moved_measurement)
        self.assertIsNotNone(moved_game_event)
        self.assertEqual(moved_manual[0], "2026-01-05T08:00:00+00:00")
        self.assertIsNone(old_chat_row)


if __name__ == "__main__":
    unittest.main()
