import asyncio
import os
import logging
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.types import Message
from dotenv import load_dotenv
from src.handlers import router
from src.database import Database
from src.scheduler import setup_scheduler
from typing import Callable, Dict, Any, Awaitable

load_dotenv()

# Basic Throttling Middleware
class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, limit: float = 0.5):
        self.limit = limit
        self.users = {}
        self.logger = logging.getLogger(__name__)
        super().__init__()

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:
        user_id = event.from_user.id
        now = asyncio.get_event_loop().time()
        
        if user_id in self.users:
            if now - self.users[user_id] < self.limit:
                self.logger.info(f"Throttled message from user {user_id}")
                return # Ignore fast messages
        
        self.users[user_id] = now
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
    
    dp.include_router(router)
    dp.message.middleware(ThrottlingMiddleware(limit=0.5))
    
    # Fetch active chats from DB
    chat_ids = await db.get_active_chats()
    
    scheduler = setup_scheduler(bot, db, chat_ids)
    scheduler.start()

    print("Pisun Bot is running...")
    
    # Set bot commands
    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="pisun", description="Поміряти пісюн (раз на день)"),
        BotCommand(command="top", description="Топ гігантів чату"),
        BotCommand(command="start", description="Інформація про бота")
    ])
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass
    asyncio.run(main())
