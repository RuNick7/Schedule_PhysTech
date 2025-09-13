# app/main.py
import asyncio
from app.bot import bot, dp
from app.utils.logging import setup_logging
from app.services.db import init_db
from app.handlers import start, menu  # noqa: F401
from app.autosend.runner import start_autosend
import os, logging, sys

# >>> логирование — в самом верху файла!
import logging, os, sys
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "DEBUG").upper(), logging.DEBUG),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,  # перебивает чужие конфиги
)
logging.getLogger("aiogram").setLevel(logging.INFO)
logging.getLogger("gcal").setLevel(logging.DEBUG)
logging.getLogger("gcal.api").setLevel(logging.DEBUG)
logging.getLogger("gcal.mapper").setLevel(logging.DEBUG)
print(">>> BOT STARTED", flush=True)

async def main():
    setup_logging()
    init_db()
    start_autosend(bot)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
