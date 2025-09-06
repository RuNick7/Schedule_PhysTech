# app/handlers/autosend.py
from __future__ import annotations
import re

from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.services.db import (
    get_user,
    set_autosend_enabled,
    set_autosend_mode,
    set_autosend_time,
)
from app.config import settings

router = Router()

TIME_RE = re.compile(r"^(?:[0-1]?\d|2[0-3]):[0-5]\d$")  # допускаем '6:05' и '06:05'
ALLOWED_TIMES = ["06:00", "06:30", "07:00", "07:30", "08:00"]

def _safe_default_time() -> str:
    t = getattr(settings, "autosend_default_time", "07:30")
    return t if t in ALLOWED_TIMES else "07:30"

def _kb_root(mode: int | None, time_str: str):
    kb = InlineKeyboardBuilder()
    kb.button(
        text=f"Режим: {'① Утром день' if mode == 1 else '② Ближайшая пара' if mode == 2 else '—'}",
        callback_data="autosend:choose_mode"
    )
    kb.button(text=f"⏰ Время: {time_str}", callback_data="autosend:choose_time")
    kb.button(text="⛔️ Отключить автоотправку", callback_data="autosend:disable")
    kb.button(text="⬅️ Назад", callback_data="autosend:back")
    kb.adjust(1, 1, 1, 1)
    return kb.as_markup()

def _kb_modes():
    kb = InlineKeyboardBuilder()
    kb.button(text="① Утром: расписание на день", callback_data="autosend:mode:1")
    kb.button(text="② Утром: ближайшая пара (автообновление)", callback_data="autosend:mode:2")
    kb.button(text="⬅️ Назад", callback_data="autosend:open")
    kb.adjust(1, 1, 1)
    return kb.as_markup()

def _kb_times(current: str):
    kb = InlineKeyboardBuilder()
    for t in ALLOWED_TIMES:
        prefix = "• " if t == current else ""
        kb.button(text=f"{prefix}{t}", callback_data=f"autosend:time:{t}")
    kb.button(text="✏️ Ввести своё время", callback_data="autosend:time:manual")
    kb.button(text="⬅️ Назад", callback_data="autosend:open")
    kb.adjust(3, 2, 1, 1)
    return kb.as_markup()

def _text_root(user) -> str:
    mode = user.get("autosend_mode")
    hhmm = user.get("autosend_time") or _safe_default_time()
    lines = ["<b>Автоотправка расписания</b>", "Статус: ✅ включена"]
    lines.append(f"Время отправки: {hhmm}")
    lines.append(f"Режим: {'① Утром: расписание на день' if mode == 1 else '② Утром: ближайшая пара (автообновление)'}")
    return "\n".join(lines)

def _normalize_hhmm(s: str) -> str:
    """Превращаем '6:5'/'6:05' в '06:05'."""
    h, m = s.strip().split(":")
    return f"{int(h):02d}:{int(m):02d}"
# ===== вход =====

@router.callback_query(F.data == "autosend:open")
async def autosend_open(q: CallbackQuery):
    user = get_user(q.from_user.id)
    if not user or not user.get("group_code"):
        await q.answer("Сначала выберите группу.", show_alert=True); return

    # автovключаем + дефолтный режим 1
    if not user.get("autosend_enabled"):
        set_autosend_enabled(q.from_user.id, True)
    if not user.get("autosend_mode"):
        set_autosend_mode(q.from_user.id, 1)
    # ВАЖНО: не трогаем уже заданное время, даже если оно вне быстрых вариантов
    if not user.get("autosend_time"):
        set_autosend_time(q.from_user.id, _safe_default_time())

    user = get_user(q.from_user.id)
    await q.message.edit_text(
        _text_root(user),
        reply_markup=_kb_root(user.get("autosend_mode"), user.get("autosend_time") or _safe_default_time())
    )
    await q.answer()

@router.callback_query(F.data == "autosend:back")
async def autosend_back(q: CallbackQuery):
    from app.handlers.start import _kb_main_menu
    await q.message.edit_text("Что дальше?", reply_markup=_kb_main_menu())
    await q.answer()

# ===== отключение =====

@router.callback_query(F.data == "autosend:disable")
async def autosend_disable(q: CallbackQuery):
    set_autosend_enabled(q.from_user.id, False)
    # Покажем короткое подтверждение и вернёмся в меню
    await q.answer("Автоотправка выключена.")
    from app.handlers.start import _kb_main_menu
    await q.message.edit_text("Готово! Что дальше?", reply_markup=_kb_main_menu())

# ===== выбор режима =====

@router.callback_query(F.data == "autosend:choose_mode")
async def autosend_choose_mode(q: CallbackQuery):
    await q.message.edit_text("Выберите режим автоотправки:", reply_markup=_kb_modes())
    await q.answer()

@router.callback_query(F.data.startswith("autosend:mode:"))
async def autosend_set_mode(q: CallbackQuery):
    mode = int(q.data.split(":")[-1])
    if mode not in (1, 2):
        await q.answer("Неверный режим", show_alert=True); return
    set_autosend_mode(q.from_user.id, mode)
    await autosend_open(q)

# ===== выбор времени (только 06:00–08:00) =====

@router.callback_query(F.data == "autosend:choose_time")
async def autosend_choose_time(q: CallbackQuery):
    user = get_user(q.from_user.id)
    hhmm = user.get("autosend_time") or _safe_default_time()
    await q.message.edit_text("Выберите время отправки:", reply_markup=_kb_times(hhmm))
    await q.answer()

@router.callback_query(F.data.startswith("autosend:time:"))
async def autosend_set_time(q: CallbackQuery):
    val = q.data.split(":")[-1]
    if val == "manual":
        return await autosend_time_manual_prompt(q)
    if val not in ALLOWED_TIMES:
        await q.answer("Доступны быстрые варианты 06:00–08:00, либо введите своё время вручную.", show_alert=True); return
    set_autosend_time(q.from_user.id, val)
    await autosend_open(q)

@router.callback_query(F.data == "autosend:time:manual")
async def autosend_time_manual_prompt(q: CallbackQuery):
    await q.message.edit_text(
        "Введите время в формате <b>HH:MM</b> (например, <b>06:45</b> или <b>9:05</b>).\n"
        "Быстрые варианты доступны в меню: 06:00–08:00."
    )
    await q.answer()

@router.message(F.text.regexp(TIME_RE))
async def autosend_time_manual_set(msg: Message):
    from app.services.db import set_autosend_time, get_user
    hhmm = _normalize_hhmm(msg.text)
    set_autosend_time(msg.from_user.id, hhmm)
    u = get_user(msg.from_user.id)
    await msg.answer(f"⏰ Время обновлено: <b>{hhmm}</b>.\nОткройте меню автоотправки для проверки.")
