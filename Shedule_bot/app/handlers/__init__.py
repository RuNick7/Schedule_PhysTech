# app/handlers/__init__.py
from aiogram import Router
from .start import router as start_router
from .menu import router as menu_router
from app.handlers.schedule_view import router as schedule_view_router
from app.handlers.autosend import router as autosend_router
from ..bot import dp
from app.handlers import gcal_sync

dp.include_router(start_router)
dp.include_router(menu_router)
dp.include_router(schedule_view_router)
dp.include_router(autosend_router)
dp.include_router(gcal_sync.router)