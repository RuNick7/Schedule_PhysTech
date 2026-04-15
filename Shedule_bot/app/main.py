# app/main.py
import asyncio
import logging
import os
import sys

from app.bot import bot, dp
from app.handlers import start, menu  # noqa: F401
from app.services.db import init_db, migrate_gcal_autosync
from app.services.isu_db import init_isu_db
from app.services.isu_indexer import start_isu_indexer
from app.autosend.runner import start_autosend
from app.utils.logging import setup_logging

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "DEBUG").upper(), logging.DEBUG),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logging.getLogger("aiogram").setLevel(logging.INFO)
logging.getLogger("gcal").setLevel(logging.DEBUG)
logging.getLogger("gcal.api").setLevel(logging.DEBUG)
logging.getLogger("gcal.mapper").setLevel(logging.DEBUG)


async def main():
    setup_logging()
    init_db()
    migrate_gcal_autosync()
    init_isu_db()
    start_isu_indexer()
    start_autosend(bot)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
