# app/main.py
import asyncio
from app.bot import bot, dp
from app.utils.logging import setup_logging
from app.services.db import init_db
from app.handlers import start, menu  # noqa: F401
from app.autosend.runner import start_autosend

async def main():
    setup_logging()
    init_db()
    start_autosend(bot)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
