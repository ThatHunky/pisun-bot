import asyncio
import os
import time
import logging
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.types import Message, CallbackQuery
from dotenv import load_dotenv
from src.handlers import router
from src.database import Database
from src.scheduler import setup_scheduler
from typing import Callable, Dict, Any, Awaitable, Tuple, Union

load_dotenv()

# Light throttle: read-only commands get shorter cooldown, no backoff
LIGHT_COMMANDS = ("/start", "/top", "/top_week", "/me", "/history", "/drops_status", "/events_status")
IDLE_TTL_SEC = 3600  # Prune entries allowed more than 1h ago
LOG_RATE_LIMIT_SEC = 60  # At most one throttle log per key per 60s


def _throttle_key(user_id: int, chat_id: int) -> str:
    return f"{user_id}:{chat_id}"


class ThrottlingMiddleware(BaseMiddleware):
    """2s base + exponential backoff per (user_id, chat_id). Light tier for read-only commands."""

    def __init__(
        self,
        base_interval: float = 2.0,
        max_backoff_exponent: int = 5,
        light_interval: float = 0.5,
        admin_ids: Tuple[int, ...] = (),
        idle_ttl: int = IDLE_TTL_SEC,
        log_rate_limit: int = LOG_RATE_LIMIT_SEC,
    ):
        self.base_interval = base_interval
        self.max_backoff_exponent = max_backoff_exponent
        self.light_interval = light_interval
        self.admin_ids = frozenset(admin_ids)
        self.idle_ttl = idle_ttl
        self.log_rate_limit = log_rate_limit
        # key -> (next_allowed_time: float, strike_count: int)
        self._state: Dict[str, Tuple[float, int]] = {}
        self._last_log: Dict[str, float] = {}
        self._log_counter = 0
        self.logger = logging.getLogger(__name__)
        super().__init__()

    def _get_user_chat(self, event: Union[Message, CallbackQuery]) -> Tuple[int, int]:
        user_id = event.from_user.id
        if isinstance(event, CallbackQuery):
            if event.message and event.message.chat:
                chat_id = event.message.chat.id
            else:
                chat_id = user_id
        else:
            chat_id = event.chat.id
        return user_id, chat_id

    def _is_light(self, event: Union[Message, CallbackQuery]) -> bool:
        if isinstance(event, CallbackQuery):
            return False
        text = (event.text or "").strip()
        return any(text.startswith(cmd) for cmd in LIGHT_COMMANDS)

    def _prune(self, now: float) -> None:
        cutoff = now - self.idle_ttl
        to_remove = [k for k, (next_allowed, _) in self._state.items() if next_allowed < cutoff]
        for k in to_remove:
            del self._state[k]
        if to_remove:
            self.logger.debug("Throttle state pruned %d entries", len(to_remove))

    def _maybe_log_throttle(self, key: str, now: float) -> None:
        last = self._last_log.get(key, 0.0)
        if now - last >= self.log_rate_limit:
            self._last_log[key] = now
            self.logger.debug("Throttled key %s", key)

    async def __call__(
        self,
        handler: Callable[..., Awaitable[Any]],
        event: Union[Message, CallbackQuery],
        data: Dict[str, Any],
    ) -> Any:
        now = time.monotonic()
        user_id, chat_id = self._get_user_chat(event)
        key = _throttle_key(user_id, chat_id)

        if user_id in self.admin_ids:
            return await handler(event, data)

        # Lazy prune every 500th check to bound memory
        self._log_counter += 1
        if self._log_counter % 500 == 0:
            self._prune(now)

        is_light = self._is_light(event)
        if is_light:
            interval = self.light_interval
            use_backoff = False
        else:
            interval = self.base_interval
            use_backoff = True

        if key in self._state:
            next_allowed, strike_count = self._state[key]
            if now < next_allowed:
                if isinstance(event, CallbackQuery):
                    await event.answer()
                self._maybe_log_throttle(key, now)
                if use_backoff:
                    cap = min(strike_count, self.max_backoff_exponent)
                    delay = interval * (2 ** cap)
                    self._state[key] = (now + delay, strike_count + 1)
                return None

        # Allowed: call handler and set cooldown
        self._state[key] = (now + interval, 0)
        return await handler(event, data)

async def main():
    logging.basicConfig(level=logging.INFO)
    
    token = os.getenv("BOT_TOKEN")
    if not token:
        print("Error: BOT_TOKEN not found in .env")
        return

    bot = Bot(token=token)
    dp = Dispatcher()
    
    db = Database("data/pisun.db")
    await db.init()

    admin_ids = tuple(int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip())
    throttle = ThrottlingMiddleware(base_interval=2.0, admin_ids=admin_ids)

    dp.include_router(router)
    dp.message.middleware(throttle)
    dp.callback_query.middleware(throttle)
    
    scheduler = setup_scheduler(bot, db)
    scheduler.start()

    print("Pisun Bot is running...")
    
    # Set bot commands
    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="pisun", description="Поміряти пісюн (раз на день)"),
        BotCommand(command="top", description="Топ гігантів чату"),
        BotCommand(command="top_week", description="Топ за тиждень"),
        BotCommand(command="me", description="Моя статистика"),
        BotCommand(command="history", description="Моя історія змін"),
        BotCommand(command="event", description="Старт події: duel|jackpot|trap"),
        BotCommand(command="drops_status", description="Статус тижневих дропів"),
        BotCommand(command="events_status", description="Статус автоподій"),
        BotCommand(command="start", description="Інформація про бота"),
    ])
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass
    asyncio.run(main())
