import aiosqlite
import datetime
from typing import Optional, List, Tuple

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER,
                    chat_id INTEGER,
                    username TEXT,
                    length REAL DEFAULT 0.0,
                    measure_count INTEGER DEFAULT 0,
                    last_measure DATE,
                    PRIMARY KEY (user_id, chat_id)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS global_events (
                    event_id TEXT PRIMARY KEY,
                    last_run DATE,
                    is_active BOOLEAN DEFAULT 0,
                    winner_id INTEGER
                )
            """)
            await db.commit()

    async def get_user(self, user_id: int, chat_id: int) -> Optional[Tuple]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT length, measure_count, last_measure, username FROM users WHERE user_id = ? AND chat_id = ?",
                (user_id, chat_id)
            ) as cursor:
                return await cursor.fetchone()

    async def update_user(self, user_id: int, chat_id: int, username: str, length: float, measure_count: int, last_measure: datetime.date):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO users (user_id, chat_id, username, length, measure_count, last_measure)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, chat_id) DO UPDATE SET
                    username = excluded.username,
                    length = excluded.length,
                    measure_count = excluded.measure_count,
                    last_measure = excluded.last_measure
            """, (user_id, chat_id, username, length, measure_count, last_measure))
            await db.commit()

    async def get_top_users(self, chat_id: int, limit: int = 10) -> List[Tuple]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT username, length FROM users WHERE chat_id = ? ORDER BY length DESC LIMIT ?",
                (chat_id, limit)
            ) as cursor:
                return await cursor.fetchall()
    
    async def get_event_state(self, event_id: str) -> Optional[Tuple]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT last_run, is_active, winner_id FROM global_events WHERE event_id = ?",
                (event_id,)
            ) as cursor:
                return await cursor.fetchone()

    async def set_event_state(self, event_id: str, last_run: datetime.date, is_active: bool, winner_id: Optional[int] = None):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO global_events (event_id, last_run, is_active, winner_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    last_run = excluded.last_run,
                    is_active = excluded.is_active,
                    winner_id = excluded.winner_id
            """, (event_id, last_run, is_active, winner_id))
            await db.commit()

    async def get_active_chats(self) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT DISTINCT chat_id FROM users") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def claim_bonus_event(self, event_id: str, winner_id: int) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "UPDATE global_events SET is_active = 0, winner_id = ? WHERE event_id = ? AND is_active = 1",
                (winner_id, event_id)
            )
            await db.commit()
            return cursor.rowcount > 0
