import aiosqlite
import datetime
import json
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple


AUTO_RECOVER_DISABLE_REASONS = {
    "chat_not_found",
    "chat_migrated",
    "bot_kicked",
    "bot_blocked",
    "forbidden",
}


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
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

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS user_facts (
                    user_id INTEGER,
                    chat_id INTEGER,
                    fact_index INTEGER,
                    PRIMARY KEY (user_id, chat_id, fact_index)
                )
                """
            )

            # Migration for existing databases
            try:
                await db.execute("ALTER TABLE users ADD COLUMN weekly_length REAL DEFAULT 0.0")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE users ADD COLUMN last_reset_week TEXT")
            except Exception:
                pass

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS global_events (
                    event_id TEXT PRIMARY KEY,
                    last_run DATE,
                    is_active BOOLEAN DEFAULT 0,
                    winner_id INTEGER
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_events (
                    chat_id INTEGER,
                    event_id TEXT,
                    is_active BOOLEAN DEFAULT 0,
                    winner_id INTEGER,
                    PRIMARY KEY (chat_id, event_id)
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS chats (
                    chat_id INTEGER PRIMARY KEY,
                    chat_type TEXT NOT NULL,
                    drops_enabled BOOLEAN DEFAULT 1,
                    events_enabled BOOLEAN DEFAULT 1,
                    last_seen_at TEXT NOT NULL,
                    disabled_reason TEXT,
                    disabled_at TEXT,
                    last_auto_event_date DATE,
                    next_auto_event_at TEXT,
                    last_manual_event_at TEXT
                )
                """
            )

            # Migration for existing chats table
            try:
                await db.execute("ALTER TABLE chats ADD COLUMN events_enabled BOOLEAN DEFAULT 1")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE chats ADD COLUMN last_auto_event_date DATE")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE chats ADD COLUMN next_auto_event_at TEXT")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE chats ADD COLUMN last_manual_event_at TEXT")
            except Exception:
                pass

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS measurements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    delta REAL NOT NULL,
                    new_length REAL NOT NULL,
                    source TEXT NOT NULL,
                    meta TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS game_events (
                    event_id TEXT PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    stake INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    message_id INTEGER,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    winner_user_id INTEGER,
                    payload TEXT
                )
                """
            )

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS game_event_entries (
                    event_id TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    username TEXT NOT NULL,
                    choice TEXT,
                    dice_value INTEGER,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (event_id, user_id)
                )
                """
            )

            await self._backfill_chats_from_users_conn(db)
            await db.commit()

    def _normalize_chat_type(self, chat_type: str) -> str:
        if chat_type in {"private", "group", "supergroup", "channel"}:
            return chat_type
        return "private"

    def _guess_chat_type(self, chat_id: int) -> str:
        return "group" if chat_id < 0 else "private"

    def _utc_now_iso(self) -> str:
        return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")

    async def _backfill_chats_from_users_conn(self, db: aiosqlite.Connection):
        now = self._utc_now_iso()
        await db.execute(
            """
            INSERT INTO chats (chat_id, chat_type, drops_enabled, events_enabled, last_seen_at)
            SELECT DISTINCT
                users.chat_id,
                CASE
                    WHEN users.chat_id < 0 THEN 'group'
                    ELSE 'private'
                END,
                1,
                1,
                ?
            FROM users
            WHERE NOT EXISTS (
                SELECT 1 FROM chats WHERE chats.chat_id = users.chat_id
            )
            """,
            (now,),
        )

    async def backfill_chats_from_users(self):
        async with aiosqlite.connect(self.db_path) as db:
            await self._backfill_chats_from_users_conn(db)
            await db.commit()

    async def register_chat(self, chat_id: int, chat_type: str):
        normalized_type = self._normalize_chat_type(chat_type)
        now = self._utc_now_iso()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO chats (
                    chat_id,
                    chat_type,
                    drops_enabled,
                    events_enabled,
                    last_seen_at,
                    disabled_reason,
                    disabled_at,
                    last_auto_event_date,
                    next_auto_event_at,
                    last_manual_event_at
                )
                VALUES (?, ?, 1, 1, ?, NULL, NULL, NULL, NULL, NULL)
                ON CONFLICT(chat_id) DO UPDATE SET
                    chat_type = excluded.chat_type,
                    last_seen_at = excluded.last_seen_at,
                    drops_enabled = CASE
                        WHEN chats.disabled_reason IN ('chat_not_found', 'chat_migrated', 'bot_kicked', 'bot_blocked', 'forbidden')
                        THEN 1
                        ELSE chats.drops_enabled
                    END,
                    disabled_reason = CASE
                        WHEN chats.disabled_reason IN ('chat_not_found', 'chat_migrated', 'bot_kicked', 'bot_blocked', 'forbidden')
                        THEN NULL
                        ELSE chats.disabled_reason
                    END,
                    disabled_at = CASE
                        WHEN chats.disabled_reason IN ('chat_not_found', 'chat_migrated', 'bot_kicked', 'bot_blocked', 'forbidden')
                        THEN NULL
                        ELSE chats.disabled_at
                    END
                """,
                (chat_id, normalized_type, now),
            )
            await db.commit()

    async def get_drop_chats(self, groups_only: bool = True) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:
            if groups_only:
                query = """
                    SELECT chat_id
                    FROM chats
                    WHERE drops_enabled = 1 AND chat_type IN ('group', 'supergroup')
                    ORDER BY chat_id
                """
            else:
                query = """
                    SELECT chat_id
                    FROM chats
                    WHERE drops_enabled = 1
                    ORDER BY chat_id
                """

            async with db.execute(query) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def get_event_enabled_group_chats(self) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT chat_id
                FROM chats
                WHERE events_enabled = 1 AND chat_type IN ('group', 'supergroup')
                ORDER BY chat_id
                """
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def disable_chat_drops(self, chat_id: int, reason: str):
        now = self._utc_now_iso()
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                UPDATE chats
                SET drops_enabled = 0,
                    disabled_reason = ?,
                    disabled_at = ?
                WHERE chat_id = ?
                """,
                (reason, now, chat_id),
            )
            if cursor.rowcount == 0:
                await db.execute(
                    """
                    INSERT INTO chats (
                        chat_id,
                        chat_type,
                        drops_enabled,
                        events_enabled,
                        last_seen_at,
                        disabled_reason,
                        disabled_at,
                        last_auto_event_date,
                        next_auto_event_at,
                        last_manual_event_at
                    )
                    VALUES (?, ?, 0, 1, ?, ?, ?, NULL, NULL, NULL)
                    """,
                    (chat_id, self._guess_chat_type(chat_id), now, reason, now),
                )
            await db.commit()

    async def set_drops_enabled(self, chat_id: int, enabled: bool):
        now = self._utc_now_iso()
        reason = None if enabled else "manual_disabled"
        disabled_at = None if enabled else now
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                UPDATE chats
                SET drops_enabled = ?,
                    disabled_reason = ?,
                    disabled_at = ?,
                    last_seen_at = ?
                WHERE chat_id = ?
                """,
                (1 if enabled else 0, reason, disabled_at, now, chat_id),
            )
            if cursor.rowcount == 0:
                await db.execute(
                    """
                    INSERT INTO chats (
                        chat_id,
                        chat_type,
                        drops_enabled,
                        events_enabled,
                        last_seen_at,
                        disabled_reason,
                        disabled_at,
                        last_auto_event_date,
                        next_auto_event_at,
                        last_manual_event_at
                    )
                    VALUES (?, ?, ?, 1, ?, ?, ?, NULL, NULL, NULL)
                    """,
                    (chat_id, self._guess_chat_type(chat_id), 1 if enabled else 0, now, reason, disabled_at),
                )
            await db.commit()

    async def set_events_enabled(self, chat_id: int, enabled: bool):
        now = self._utc_now_iso()
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                UPDATE chats
                SET events_enabled = ?,
                    last_seen_at = ?
                WHERE chat_id = ?
                """,
                (1 if enabled else 0, now, chat_id),
            )
            if cursor.rowcount == 0:
                await db.execute(
                    """
                    INSERT INTO chats (
                        chat_id,
                        chat_type,
                        drops_enabled,
                        events_enabled,
                        last_seen_at,
                        disabled_reason,
                        disabled_at,
                        last_auto_event_date,
                        next_auto_event_at,
                        last_manual_event_at
                    )
                    VALUES (?, ?, 1, ?, ?, NULL, NULL, NULL, NULL, NULL)
                    """,
                    (chat_id, self._guess_chat_type(chat_id), 1 if enabled else 0, now),
                )
            await db.commit()

    async def get_chat_settings(self, chat_id: int) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT
                    chat_type,
                    drops_enabled,
                    events_enabled,
                    last_seen_at,
                    disabled_reason,
                    disabled_at,
                    last_auto_event_date,
                    next_auto_event_at,
                    last_manual_event_at
                FROM chats
                WHERE chat_id = ?
                """,
                (chat_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return {
                    "chat_type": row[0],
                    "drops_enabled": bool(row[1]),
                    "events_enabled": bool(row[2]),
                    "last_seen_at": row[3],
                    "disabled_reason": row[4],
                    "disabled_at": row[5],
                    "last_auto_event_date": row[6],
                    "next_auto_event_at": row[7],
                    "last_manual_event_at": row[8],
                }

    async def set_next_auto_event_at(self, chat_id: int, ts_iso: Optional[str]):
        now = self._utc_now_iso()
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                UPDATE chats
                SET next_auto_event_at = ?,
                    last_seen_at = ?
                WHERE chat_id = ?
                """,
                (ts_iso, now, chat_id),
            )
            if cursor.rowcount == 0:
                await db.execute(
                    """
                    INSERT INTO chats (
                        chat_id,
                        chat_type,
                        drops_enabled,
                        events_enabled,
                        last_seen_at,
                        disabled_reason,
                        disabled_at,
                        last_auto_event_date,
                        next_auto_event_at,
                        last_manual_event_at
                    )
                    VALUES (?, ?, 1, 1, ?, NULL, NULL, NULL, ?, NULL)
                    """,
                    (chat_id, self._guess_chat_type(chat_id), now, ts_iso),
                )
            await db.commit()

    async def get_due_auto_event_chats(self, now_iso: str) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT chat_id
                FROM chats
                WHERE events_enabled = 1
                  AND chat_type IN ('group', 'supergroup')
                  AND next_auto_event_at IS NOT NULL
                  AND next_auto_event_at <= ?
                ORDER BY next_auto_event_at ASC
                """,
                (now_iso,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def get_auto_assign_chats(self, today_iso: str) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT chat_id
                FROM chats
                WHERE events_enabled = 1
                  AND chat_type IN ('group', 'supergroup')
                  AND (last_auto_event_date IS NULL OR last_auto_event_date <> ?)
                ORDER BY chat_id
                """,
                (today_iso,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def set_last_auto_event_date(self, chat_id: int, date_iso: str):
        now = self._utc_now_iso()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE chats
                SET last_auto_event_date = ?,
                    last_seen_at = ?
                WHERE chat_id = ?
                """,
                (date_iso, now, chat_id),
            )
            await db.commit()

    async def get_last_manual_event_at(self, chat_id: int) -> Optional[str]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT last_manual_event_at FROM chats WHERE chat_id = ?",
                (chat_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return row[0]

    async def set_last_manual_event_at(self, chat_id: int, ts_iso: Optional[str]):
        now = self._utc_now_iso()
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                UPDATE chats
                SET last_manual_event_at = ?, last_seen_at = ?
                WHERE chat_id = ?
                """,
                (ts_iso, now, chat_id),
            )
            if cursor.rowcount == 0:
                await db.execute(
                    """
                    INSERT INTO chats (
                        chat_id,
                        chat_type,
                        drops_enabled,
                        events_enabled,
                        last_seen_at,
                        disabled_reason,
                        disabled_at,
                        last_auto_event_date,
                        next_auto_event_at,
                        last_manual_event_at
                    )
                    VALUES (?, ?, 1, 1, ?, NULL, NULL, NULL, NULL, ?)
                    """,
                    (chat_id, self._guess_chat_type(chat_id), now, ts_iso),
                )
            await db.commit()

    async def get_user(self, user_id: int, chat_id: int) -> Optional[Tuple]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT length, measure_count, last_measure, username, weekly_length, last_reset_week
                FROM users
                WHERE user_id = ? AND chat_id = ?
                """,
                (user_id, chat_id),
            ) as cursor:
                return await cursor.fetchone()

    async def update_user(
        self,
        user_id: int,
        chat_id: int,
        username: str,
        length: float,
        measure_count: int,
        last_measure: datetime.date,
        weekly_length: float,
        last_reset_week: str,
    ):
        if isinstance(last_measure, datetime.date):
            last_measure_value = last_measure.isoformat()
        else:
            last_measure_value = last_measure
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO users (user_id, chat_id, username, length, measure_count, last_measure, weekly_length, last_reset_week)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, chat_id) DO UPDATE SET
                    username = excluded.username,
                    length = excluded.length,
                    measure_count = excluded.measure_count,
                    last_measure = excluded.last_measure,
                    weekly_length = excluded.weekly_length,
                    last_reset_week = excluded.last_reset_week
                """,
                (user_id, chat_id, username, length, measure_count, last_measure_value, weekly_length, last_reset_week),
            )
            await db.commit()

    async def get_top_users(self, chat_id: int, limit: int = 10) -> List[Tuple]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT username, length FROM users WHERE chat_id = ? ORDER BY length DESC LIMIT ?",
                (chat_id, limit),
            ) as cursor:
                return await cursor.fetchall()

    async def get_top_weekly_users(self, chat_id: int, limit: int = 10) -> List[Tuple]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT username, weekly_length FROM users WHERE chat_id = ? AND weekly_length > 0 ORDER BY weekly_length DESC LIMIT ?",
                (chat_id, limit),
            ) as cursor:
                return await cursor.fetchall()

    async def get_user_rank_and_total(self, user_id: int, chat_id: int) -> Tuple[Optional[int], int]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM users WHERE chat_id = ?",
                (chat_id,),
            ) as total_cursor:
                total_row = await total_cursor.fetchone()
                total = int(total_row[0]) if total_row else 0

            async with db.execute(
                "SELECT length FROM users WHERE user_id = ? AND chat_id = ?",
                (user_id, chat_id),
            ) as len_cursor:
                len_row = await len_cursor.fetchone()
                if not len_row:
                    return None, total
                user_length = float(len_row[0])

            async with db.execute(
                "SELECT COUNT(*) FROM users WHERE chat_id = ? AND length > ?",
                (chat_id, user_length),
            ) as rank_cursor:
                rank_row = await rank_cursor.fetchone()
                rank = int(rank_row[0]) + 1 if rank_row else None
                return rank, total

    async def get_event_state(self, event_id: str) -> Optional[Tuple]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT last_run, is_active, winner_id FROM global_events WHERE event_id = ?",
                (event_id,),
            ) as cursor:
                return await cursor.fetchone()

    async def set_event_state(
        self,
        event_id: str,
        last_run: datetime.date,
        is_active: bool,
        winner_id: Optional[int] = None,
    ):
        if isinstance(last_run, datetime.date):
            last_run_value = last_run.isoformat()
        else:
            last_run_value = last_run
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO global_events (event_id, last_run, is_active, winner_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    last_run = excluded.last_run,
                    is_active = excluded.is_active,
                    winner_id = excluded.winner_id
                """,
                (event_id, last_run_value, is_active, winner_id),
            )
            await db.commit()

    async def get_active_chats(self) -> List[int]:
        return await self.get_drop_chats(groups_only=False)

    async def claim_bonus_event(self, event_id: str, winner_id: int) -> bool:
        """Deprecated: use claim_chat_event for per-chat logic."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "UPDATE global_events SET is_active = 0, winner_id = ? WHERE event_id = ? AND is_active = 1",
                (winner_id, event_id),
            )
            await db.commit()
            return cursor.rowcount > 0

    async def set_chat_event_state(self, chat_id: int, event_id: str, is_active: bool, winner_id: Optional[int] = None):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO chat_events (chat_id, event_id, is_active, winner_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id, event_id) DO UPDATE SET
                    is_active = excluded.is_active,
                    winner_id = excluded.winner_id
                """,
                (chat_id, event_id, is_active, winner_id),
            )
            await db.commit()

    async def claim_chat_event(self, chat_id: int, event_id: str, winner_id: int) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "UPDATE chat_events SET is_active = 0, winner_id = ? WHERE chat_id = ? AND event_id = ? AND is_active = 1",
                (winner_id, chat_id, event_id),
            )
            await db.commit()
            return cursor.rowcount > 0

    async def get_shown_facts(self, user_id: int, chat_id: int) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT fact_index FROM user_facts WHERE user_id = ? AND chat_id = ?",
                (user_id, chat_id),
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def add_shown_fact(self, user_id: int, chat_id: int, fact_index: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO user_facts (user_id, chat_id, fact_index) VALUES (?, ?, ?)",
                (user_id, chat_id, fact_index),
            )
            await db.commit()

    async def clear_shown_facts(self, user_id: int, chat_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "DELETE FROM user_facts WHERE user_id = ? AND chat_id = ?",
                (user_id, chat_id),
            )
            await db.commit()

    async def add_measurement(
        self,
        user_id: int,
        chat_id: int,
        delta: float,
        new_length: float,
        source: str,
        meta: Optional[Dict[str, Any]] = None,
        created_at: Optional[str] = None,
    ):
        ts = created_at or self._utc_now_iso()
        meta_json = json.dumps(meta, ensure_ascii=False) if meta is not None else None
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO measurements (user_id, chat_id, delta, new_length, source, meta, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, chat_id, delta, new_length, source, meta_json, ts),
            )
            await db.commit()

    async def get_user_history(self, user_id: int, chat_id: int, limit: int = 10) -> List[Tuple]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT delta, new_length, source, meta, created_at
                FROM measurements
                WHERE user_id = ? AND chat_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (user_id, chat_id, limit),
            ) as cursor:
                return await cursor.fetchall()

    async def create_game_event(
        self,
        event_id: str,
        chat_id: int,
        event_type: str,
        stake: int,
        status: str,
        created_at: str,
        expires_at: str,
        message_id: Optional[int] = None,
        winner_user_id: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None,
    ):
        payload_json = json.dumps(payload, ensure_ascii=False) if payload is not None else None
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO game_events (
                    event_id,
                    chat_id,
                    event_type,
                    stake,
                    status,
                    message_id,
                    created_at,
                    expires_at,
                    winner_user_id,
                    payload
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    chat_id,
                    event_type,
                    stake,
                    status,
                    message_id,
                    created_at,
                    expires_at,
                    winner_user_id,
                    payload_json,
                ),
            )
            await db.commit()

    async def set_game_event_message_id(self, event_id: str, message_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE game_events SET message_id = ? WHERE event_id = ?",
                (message_id, event_id),
            )
            await db.commit()

    async def get_game_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT
                    event_id,
                    chat_id,
                    event_type,
                    stake,
                    status,
                    message_id,
                    created_at,
                    expires_at,
                    winner_user_id,
                    payload
                FROM game_events
                WHERE event_id = ?
                """,
                (event_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return {
                    "event_id": row[0],
                    "chat_id": row[1],
                    "event_type": row[2],
                    "stake": row[3],
                    "status": row[4],
                    "message_id": row[5],
                    "created_at": row[6],
                    "expires_at": row[7],
                    "winner_user_id": row[8],
                    "payload": json.loads(row[9]) if row[9] else None,
                }

    async def get_active_game_event(self, chat_id: int) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT
                    event_id,
                    chat_id,
                    event_type,
                    stake,
                    status,
                    message_id,
                    created_at,
                    expires_at,
                    winner_user_id,
                    payload
                FROM game_events
                WHERE chat_id = ? AND status = 'active'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (chat_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return {
                    "event_id": row[0],
                    "chat_id": row[1],
                    "event_type": row[2],
                    "stake": row[3],
                    "status": row[4],
                    "message_id": row[5],
                    "created_at": row[6],
                    "expires_at": row[7],
                    "winner_user_id": row[8],
                    "payload": json.loads(row[9]) if row[9] else None,
                }

    async def get_due_active_game_events(self, now_iso: str, limit: int = 50) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT
                    event_id,
                    chat_id,
                    event_type,
                    stake,
                    status,
                    message_id,
                    created_at,
                    expires_at,
                    winner_user_id,
                    payload
                FROM game_events
                WHERE status = 'active' AND expires_at <= ?
                ORDER BY expires_at ASC
                LIMIT ?
                """,
                (now_iso, limit),
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    {
                        "event_id": row[0],
                        "chat_id": row[1],
                        "event_type": row[2],
                        "stake": row[3],
                        "status": row[4],
                        "message_id": row[5],
                        "created_at": row[6],
                        "expires_at": row[7],
                        "winner_user_id": row[8],
                        "payload": json.loads(row[9]) if row[9] else None,
                    }
                    for row in rows
                ]

    async def join_game_event(self, event_id: str, user_id: int, username: str, choice: Optional[str] = None) -> bool:
        now = self._utc_now_iso()
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                INSERT OR IGNORE INTO game_event_entries (event_id, user_id, username, choice, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (event_id, user_id, username, choice, now),
            )
            await db.commit()
            return cursor.rowcount > 0

    async def join_game_event_limited(
        self,
        event_id: str,
        user_id: int,
        username: str,
        max_participants: int,
        choice: Optional[str] = None,
    ) -> Literal["joined", "already_joined", "full"]:
        now = self._utc_now_iso()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                async with db.execute(
                    """
                    SELECT 1
                    FROM game_event_entries
                    WHERE event_id = ? AND user_id = ?
                    LIMIT 1
                    """,
                    (event_id, user_id),
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        await db.commit()
                        return "already_joined"

                async with db.execute(
                    """
                    SELECT COUNT(*)
                    FROM game_event_entries
                    WHERE event_id = ?
                    """,
                    (event_id,),
                ) as cursor:
                    count_row = await cursor.fetchone()
                    current_participants = int(count_row[0]) if count_row else 0

                if current_participants >= max_participants:
                    await db.commit()
                    return "full"

                await db.execute(
                    """
                    INSERT INTO game_event_entries (event_id, user_id, username, choice, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (event_id, user_id, username, choice, now),
                )
                await db.commit()
                return "joined"
            except Exception:
                await db.rollback()
                raise

    async def record_event_choice(self, event_id: str, user_id: int, choice: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE game_event_entries
                SET choice = ?
                WHERE event_id = ? AND user_id = ?
                """,
                (choice, event_id, user_id),
            )
            await db.commit()

    async def record_event_dice(self, event_id: str, user_id: int, dice_value: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE game_event_entries
                SET dice_value = ?
                WHERE event_id = ? AND user_id = ?
                """,
                (dice_value, event_id, user_id),
            )
            await db.commit()

    async def get_game_event_entries(self, event_id: str) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT event_id, user_id, username, choice, dice_value, created_at
                FROM game_event_entries
                WHERE event_id = ?
                ORDER BY created_at ASC
                """,
                (event_id,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    {
                        "event_id": row[0],
                        "user_id": row[1],
                        "username": row[2],
                        "choice": row[3],
                        "dice_value": row[4],
                        "created_at": row[5],
                    }
                    for row in rows
                ]

    async def set_event_status(
        self,
        event_id: str,
        status: str,
        winner_user_id: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None,
    ):
        payload_json = json.dumps(payload, ensure_ascii=False) if payload is not None else None
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE game_events
                SET status = ?, winner_user_id = ?, payload = COALESCE(?, payload)
                WHERE event_id = ?
                """,
                (status, winner_user_id, payload_json, event_id),
            )
            await db.commit()

    async def migrate_chat_data(self, old_chat_id: int, new_chat_id: int, new_chat_type: str = "supergroup"):
        if old_chat_id == new_chat_id:
            return

        now = self._utc_now_iso()
        normalized_type = self._normalize_chat_type(new_chat_type)

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("BEGIN")
            try:
                async with db.execute(
                    """
                    SELECT
                        chat_type,
                        drops_enabled,
                        events_enabled,
                        last_seen_at,
                        disabled_reason,
                        disabled_at,
                        last_auto_event_date,
                        next_auto_event_at,
                        last_manual_event_at
                    FROM chats
                    WHERE chat_id = ?
                    """,
                    (old_chat_id,),
                ) as cursor:
                    old_chat = await cursor.fetchone()

                async with db.execute(
                    """
                    SELECT
                        chat_type,
                        drops_enabled,
                        events_enabled,
                        last_seen_at,
                        disabled_reason,
                        disabled_at,
                        last_auto_event_date,
                        next_auto_event_at,
                        last_manual_event_at
                    FROM chats
                    WHERE chat_id = ?
                    """,
                    (new_chat_id,),
                ) as cursor:
                    new_chat = await cursor.fetchone()

                if new_chat is None:
                    if old_chat:
                        await db.execute(
                            """
                            INSERT INTO chats (
                                chat_id,
                                chat_type,
                                drops_enabled,
                                events_enabled,
                                last_seen_at,
                                disabled_reason,
                                disabled_at,
                                last_auto_event_date,
                                next_auto_event_at,
                                last_manual_event_at
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                new_chat_id,
                                normalized_type,
                                old_chat[1],
                                old_chat[2],
                                now,
                                old_chat[4],
                                old_chat[5],
                                old_chat[6],
                                old_chat[7],
                                old_chat[8],
                            ),
                        )
                    else:
                        await db.execute(
                            """
                            INSERT INTO chats (
                                chat_id,
                                chat_type,
                                drops_enabled,
                                events_enabled,
                                last_seen_at,
                                disabled_reason,
                                disabled_at,
                                last_auto_event_date,
                                next_auto_event_at,
                                last_manual_event_at
                            )
                            VALUES (?, ?, 1, 1, ?, NULL, NULL, NULL, NULL, NULL)
                            """,
                            (new_chat_id, normalized_type, now),
                        )
                else:
                    old_last_manual = old_chat[8] if old_chat else None
                    await db.execute(
                        """
                        UPDATE chats
                        SET
                            chat_type = ?,
                            last_seen_at = ?,
                            last_manual_event_at = COALESCE(last_manual_event_at, ?)
                        WHERE chat_id = ?
                        """,
                        (normalized_type, now, old_last_manual, new_chat_id),
                    )

                # Users: merge old rows into new chat_id.
                await db.execute(
                    """
                    INSERT INTO users (user_id, chat_id, username, length, measure_count, last_measure, weekly_length, last_reset_week)
                    SELECT user_id, ?, username, length, measure_count, last_measure, weekly_length, last_reset_week
                    FROM users
                    WHERE chat_id = ?
                    ON CONFLICT(user_id, chat_id) DO UPDATE SET
                        username = excluded.username,
                        length = MAX(users.length, excluded.length),
                        measure_count = MAX(users.measure_count, excluded.measure_count),
                        last_measure = CASE
                            WHEN users.last_measure IS NULL THEN excluded.last_measure
                            WHEN excluded.last_measure IS NULL THEN users.last_measure
                            WHEN users.last_measure >= excluded.last_measure THEN users.last_measure
                            ELSE excluded.last_measure
                        END,
                        weekly_length = MAX(users.weekly_length, excluded.weekly_length),
                        last_reset_week = CASE
                            WHEN users.last_reset_week IS NULL THEN excluded.last_reset_week
                            WHEN excluded.last_reset_week IS NULL THEN users.last_reset_week
                            WHEN users.last_reset_week >= excluded.last_reset_week THEN users.last_reset_week
                            ELSE excluded.last_reset_week
                        END
                    """,
                    (new_chat_id, old_chat_id),
                )
                await db.execute("DELETE FROM users WHERE chat_id = ?", (old_chat_id,))

                await db.execute(
                    """
                    INSERT OR IGNORE INTO user_facts (user_id, chat_id, fact_index)
                    SELECT user_id, ?, fact_index
                    FROM user_facts
                    WHERE chat_id = ?
                    """,
                    (new_chat_id, old_chat_id),
                )
                await db.execute("DELETE FROM user_facts WHERE chat_id = ?", (old_chat_id,))

                await db.execute(
                    """
                    INSERT INTO chat_events (chat_id, event_id, is_active, winner_id)
                    SELECT ?, event_id, is_active, winner_id
                    FROM chat_events
                    WHERE chat_id = ?
                    ON CONFLICT(chat_id, event_id) DO UPDATE SET
                        is_active = CASE
                            WHEN chat_events.is_active = 1 OR excluded.is_active = 1 THEN 1
                            ELSE 0
                        END,
                        winner_id = COALESCE(chat_events.winner_id, excluded.winner_id)
                    """,
                    (new_chat_id, old_chat_id),
                )
                await db.execute("DELETE FROM chat_events WHERE chat_id = ?", (old_chat_id,))

                await db.execute("UPDATE measurements SET chat_id = ? WHERE chat_id = ?", (new_chat_id, old_chat_id))
                await db.execute("UPDATE game_events SET chat_id = ? WHERE chat_id = ?", (new_chat_id, old_chat_id))

                await db.execute("DELETE FROM chats WHERE chat_id = ?", (old_chat_id,))

                await db.commit()
            except Exception:
                await db.rollback()
                raise
