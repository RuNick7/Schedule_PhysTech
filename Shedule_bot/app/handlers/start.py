# app/handlers/start.py
from __future__ import annotations

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder
from app.handlers.menu import open_settings as menu_open_settings

from app.services.db import (
    init_db, upsert_user, get_user, set_course, set_group, set_message_id
)
from app.services.groups import list_groups_for_course

router = Router()

def _kb_courses():
    kb = InlineKeyboardBuilder()
    for i in (1, 2, 3, 4):
        kb.button(text=f"{i} курс", callback_data=f"start:course:{i}")
    kb.adjust(4)
    return kb.as_markup()

def _kb_groups(course: int):
    groups = list_groups_for_course(course)
    kb = InlineKeyboardBuilder()
    for g in groups:
        kb.button(text=g, callback_data=f"start:group:{g}")
    kb.adjust(3)
    return kb.as_markup()

def _kb_main_menu():
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.button(text="📅 Показать расписание", callback_data="main:schedule")
    kb.button(text="⚙️ Настройки", callback_data="main:settings")
    kb.button(text="📆 Google Calendar", callback_data="main:gcal")
    kb.adjust(1, 1, 1)
    return kb.as_markup()

@router.message(CommandStart())
async def start_cmd(msg: Message):
    # Инициализация и апдейт t.me/username
    init_db()
    upsert_user(msg.from_user.id, msg.from_user.username)
    user = get_user(msg.from_user.id)

    # 1) Есть группа → сразу главное меню
    if user and user.get("group_code"):
        text = (
            f"С возвращением, <b>{msg.from_user.full_name}</b>!\n"
            f"Текущая группа: <b>{user['group_code']}</b>.\n\nВыберите действие:"
        )
        m = await msg.answer(text, reply_markup=_kb_main_menu())
        set_message_id(msg.from_user.id, m.message_id)
        return

    # 2) Есть курс, но нет группы → сразу выбор группы
    if user and user.get("course"):
        course = int(user["course"])
        m = await msg.answer(
            f"Курс: <b>{course}</b>\nТеперь выбери <b>группу</b>:",
            reply_markup=_kb_groups(course)
        )
        set_message_id(msg.from_user.id, m.message_id)
        return

    # 3) Новичок → выбор курса
    m = await msg.answer(
        "Привет! Давай настроим профиль.\n\nВыбери <b>курс</b>:",
        reply_markup=_kb_courses()
    )
    set_message_id(msg.from_user.id, m.message_id)

@router.callback_query(F.data.startswith("start:course:"))
async def choose_course(q: CallbackQuery):
    course = int(q.data.split(":")[-1])
    set_course(q.from_user.id, course)

    await q.message.edit_text(
        f"Курс: <b>{course}</b>\nТеперь выбери <b>группу</b>:",
        reply_markup=_kb_groups(course)
    )
    await q.answer()

@router.callback_query(F.data.startswith("start:group:"))
async def choose_group(q: CallbackQuery):
    group = q.data.split(":", 2)[-1]
    set_group(q.from_user.id, group)

    # сразу спросим про автоотправку
    kb = InlineKeyboardBuilder()
    kb.button(text="Да, настроить автоотправку", callback_data="autosend:open")
    kb.button(text="Нет, в главное меню", callback_data="start:to_main")
    kb.adjust(1, 1)

    await q.message.edit_text(
        f"Группа: <b>{group}</b>\n\nВключить автоотправку расписания?",
        reply_markup=kb.as_markup()
    )
    await q.answer()

@router.callback_query(F.data == "start:to_main")
async def to_main(q: CallbackQuery):
    await q.message.edit_text("Готово! Что дальше?", reply_markup=_kb_main_menu())
    await q.answer()

@router.callback_query(F.data == "main:settings")
async def open_settings(q: CallbackQuery):
    # просто передаём управление хендлеру из menu.py
    await menu_open_settings(q)
