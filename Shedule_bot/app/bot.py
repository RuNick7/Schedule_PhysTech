# app/bot.py
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

from .config import settings

# Инициализация бота с HTML по умолчанию
bot = Bot(
    token=settings.bot_token,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)

# Диспетчер + FSM-хранилище в памяти (для меню и ввода настроек)
dp = Dispatcher(storage=MemoryStorage())
