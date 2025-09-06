from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.services.db import get_user, set_course, set_group
from app.services.groups import list_groups_for_course

router = Router()

# --- keyboards ---

def _kb_settings(user) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="👥 Сменить группу", callback_data="settings:change_group")
    kb.button(text="🎓 Сменить курс", callback_data="settings:change_course")
    kb.button(text="📨 Настроить автоотправку", callback_data="autosend:open")
    kb.button(text="⬅️ Назад", callback_data="settings:back")
    kb.adjust(1, 1, 1, 1)
    return kb

def _kb_courses():
    kb = InlineKeyboardBuilder()
    for i in (1, 2, 3, 4):
        kb.button(text=f"{i} курс", callback_data=f"settings:course:{i}")
    kb.button(text="⬅️ Назад", callback_data="settings:open")
    kb.adjust(4, 1)
    return kb.as_markup()

def _kb_groups(course: int):
    groups = list_groups_for_course(course)
    kb = InlineKeyboardBuilder()
    # выводим рядами по 3
    for g in groups:
        kb.button(text=g, callback_data=f"settings:group:{g}")
    kb.button(text="⬅️ Назад", callback_data="settings:open")
    kb.adjust(3, 1)
    return kb.as_markup()

# --- text ---

def _autosend_summary(u) -> str:
    enabled = bool(u.get("autosend_enabled"))
    if not enabled:
        return "⛔️ Выключена"
    return f"✅"

def _settings_text(u) -> str:
    group = u.get("group_code") or "—"
    course = u.get("course") or "—"
    auto = _autosend_summary(u)
    lines = [
        "⚙️ <b>Настройки</b>",
        f"👥 Группа: <b>{group}</b>",
        f"🎓 Курс: <b>{course}</b>",
        f"📨 Автоотправка: {auto}",
        "",
        "Выберите, что изменить:",
    ]
    return "\n".join(lines)

# --- handlers ---

@router.callback_query(F.data == "main:settings")
@router.callback_query(F.data == "settings:open")
async def open_settings(q: CallbackQuery):
    u = get_user(q.from_user.id)
    if not u:
        await q.answer("Сначала /start", show_alert=True); return
    kb = _kb_settings(u).as_markup()
    await q.message.edit_text(_settings_text(u), reply_markup=kb)
    await q.answer()

@router.callback_query(F.data == "settings:back")
async def settings_back(q: CallbackQuery):
    # назад в главное меню
    from app.handlers.start import _kb_main_menu
    await q.message.edit_text("Что дальше?", reply_markup=_kb_main_menu())
    await q.answer()

@router.callback_query(F.data == "settings:change_course")
async def settings_change_course(q: CallbackQuery):
    await q.message.edit_text("Выберите курс:", reply_markup=_kb_courses())
    await q.answer()

@router.callback_query(F.data.startswith("settings:course:"))
async def settings_set_course(q: CallbackQuery):
    course = int(q.data.split(":")[-1])
    set_course(q.from_user.id, course)
    # сразу предложим выбрать группу
    await q.message.edit_text(
        f"Курс: <b>{course}</b>\nТеперь выберите группу:",
        reply_markup=_kb_groups(course)
    )
    await q.answer()

@router.callback_query(F.data == "settings:change_group")
async def settings_change_group(q: CallbackQuery):
    u = get_user(q.from_user.id)
    if not u or not u.get("course"):
        await q.message.edit_text("Сначала выберите курс:", reply_markup=_kb_courses())
        await q.answer()
        return
    await q.message.edit_text(
        f"Курс: <b>{u['course']}</b>\nВыберите группу:",
        reply_markup=_kb_groups(int(u["course"]))
    )
    await q.answer()

@router.callback_query(F.data.startswith("settings:group:"))
async def settings_set_group(q: CallbackQuery):
    group = q.data.split(":", 2)[-1]
    set_group(q.from_user.id, group)
    u = get_user(q.from_user.id)
    await q.message.edit_text(
        f"Группа обновлена: <b>{group}</b>\n",
        reply_markup=_kb_settings(u).as_markup()
    )
    await q.answer()
