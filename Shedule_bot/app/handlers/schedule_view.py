from __future__ import annotations
from datetime import timedelta
from typing import List
from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.services.db import get_user, set_message_id
from app.services.sheets_client import fetch_sheet_values_and_links
from app.services.schedule_expand import expand_merged_matrix
from app.services.schedule_list import list_lessons_matrix
from app.utils.week_parity import week_parity_for_date
from app.utils.dt import now_tz
from app.utils.format_schedule import format_day, format_week_compact_mono
from app.config import settings
from app.utils.subjects_alert import detect_special_subjects_in_matrix

router = Router()

_NO_LESSONS_DEFAULT = [
    "— Пар не найдено —",
    "Свободный день! ✨",
    "Сегодня без пар — можно выдохнуть 🙂",
    "Ничего в расписании. Береги силы 💪",
    "Пары отсутствуют. Хорошего дня! 🌿",
]

# ---------- клавиатуры ----------
def _kb_main_menu():
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.button(text="📅 Показать расписание", callback_data="main:schedule")
    kb.button(text="⚙️ Настройки", callback_data="main:settings")
    kb.button(text="📆 Google Calendar", callback_data="main:gcal")
    kb.adjust(1, 1, 1)
    return kb.as_markup()

def kb_schedule_root():
    kb = InlineKeyboardBuilder()
    kb.button(text="Сегодня", callback_data="sched:day:today")
    kb.button(text="Завтра", callback_data="sched:day:tomorrow")
    kb.button(text="Неделя", callback_data="sched:week:auto")
    kb.button(text="Назад", callback_data="start:to_main")
    kb.adjust(1, 1, 1, 1)
    return kb.as_markup()

def kb_day_controls(day_name: str, parity: str):
    kb = InlineKeyboardBuilder()
    # показываем альтернативную чётность для этого же дня
    alt = "чёт" if parity == "нечёт" else "нечёт"
    kb.button(text=f"Другая чётность ({'Чёт' if alt=='чёт' else 'Нечёт'})", callback_data=f"sched:day:same:{day_name}:{alt}")
    kb.button(text="Показать неделю", callback_data="sched:week:auto")
    kb.button(text="Назад", callback_data="sched:root")
    kb.adjust(1, 1, 1)
    return kb.as_markup()

def kb_week_controls(parity: str):
    kb = InlineKeyboardBuilder()
    alt = "чёт" if parity == "нечёт" else "нечёт"
    kb.button(text=f"Сменить на {'Чётную' if alt == 'чёт' else 'Нечётную'}", callback_data=f"sched:week:{alt}")
    kb.button(text="Сегодня", callback_data="sched:day:today")
    kb.button(text="Назад", callback_data="sched:root")
    kb.adjust(1, 1, 1)  # каждая кнопка на своей строке, «Назад» — в самом низу

# ---------- вспомогательное ----------

def _russian_day_name(dt) -> str:
    # 0=Mon → ПОНЕДЕЛЬНИК
    names = ["ПОНЕДЕЛЬНИК","ВТОРНИК","СРЕДА","ЧЕТВЕРГ","ПЯТНИЦА","СУББОТА","ВОСКРЕСЕНЬЕ"]
    return names[dt.weekday()]

async def _load_lessons_for_user_group(user: dict):
    vals, links, merges = fetch_sheet_values_and_links(
        spreadsheet_id=settings.spreadsheet_id,
        sheet_gid=settings.sheet_gid,
        creds_path=settings.google_credentials,
    )
    mtx_vals  = expand_merged_matrix(vals, merges=merges)
    mtx_links = expand_merged_matrix(links, merges=merges)
    all_lessons = list_lessons_matrix(mtx_vals, mtx_links)
    return [it for it in all_lessons if it["group"] == user["group_code"]]

def _filter_by_day_and_parity(lessons: List[dict], day_name: str, parity: str) -> List[dict]:
    return [it for it in lessons if str(it["day"]).strip().upper() == day_name and it["parity"] == parity]

def _filter_by_parity(lessons: List[dict], parity: str) -> List[dict]:
    return [it for it in lessons if it["parity"] == parity]

async def _send_or_edit(q: CallbackQuery, text: str, kb):
    user = get_user(q.from_user.id)
    # Режим 1 — редактируем одно сообщение
    if user and user.get("type") == 1 and user.get("message_id"):
        try:
            await q.message.bot.edit_message_text(
                chat_id=q.message.chat.id,
                message_id=user["message_id"],
                text=text,
                reply_markup=kb
            )
            await q.answer()
            return
        except Exception:
            pass  # упало редактирование — отправим новое и обновим message_id

    m = await q.message.answer(text, reply_markup=kb, disable_web_page_preview=True)
    if user and user.get("type") == 1:
        set_message_id(q.from_user.id, m.message_id)
    await q.answer()

# ---------- вход из главного меню ----------
@router.callback_query(F.data == "start:to_main")
async def to_main(q: CallbackQuery):
    await q.message.edit_text("Готово! Что дальше?", reply_markup=_kb_main_menu())
    await q.answer()

@router.callback_query(F.data == "main:schedule")
async def schedule_entry(q: CallbackQuery):
    user = get_user(q.from_user.id)
    if not user or not user.get("group_code"):
        await q.answer("Сначала завершите онбординг: курс и группа.", show_alert=True)
        return
    await _send_or_edit(q, "Выберите период:", kb_schedule_root())

# ---------- корень выбора периода ----------
@router.callback_query(F.data == "sched:root")
async def sched_root(q: CallbackQuery):
    await _send_or_edit(q, "Выберите период:", kb_schedule_root())

# ---------- сегодня / завтра ----------
@router.callback_query(F.data.in_({"sched:day:today", "sched:day:tomorrow"}))
async def sched_day_today_tomorrow(q: CallbackQuery):
    user = get_user(q.from_user.id)
    if not user or not user.get("group_code"):
        await q.answer("Сначала выберите группу.", show_alert=True)
        return

    tz = user.get("timezone") or settings.timezone
    now = now_tz(tz)
    is_tomorrow = q.data.endswith("tomorrow")
    target = now + timedelta(days=1 if is_tomorrow else 0)

    parity = week_parity_for_date(target, tz)
    # День недели в UPPER, как в парсере
    DAY_UP = ["ПОНЕДЕЛЬНИК","ВТОРНИК","СРЕДА","ЧЕТВЕРГ","ПЯТНИЦА","СУББОТА","ВОСКРЕСЕНЬЕ"]
    day_upper = DAY_UP[target.weekday()]

    # пары для пользователя
    lessons = await _load_lessons_for_user_group(user)
    day_lessons = [it for it in lessons if it["parity"] == parity and it["day"] == day_upper]

    text = format_day(user["group_code"], day_upper, parity, day_lessons)

    # клавиатура под день (как и раньше)
    kb = kb_day_controls(day_upper, parity)  # если у тебя уже есть эта функция
    # или собери здесь InlineKeyboardBuilder

    await _send_or_edit(q, text, kb)

# тот же день, но принудительная смена чётности
@router.callback_query(F.data.startswith("sched:day:same:"))
async def sched_day_same(q: CallbackQuery):
    # data: sched:day:same:{DAY}:{parity}
    _, _, _, day_name, parity = q.data.split(":")
    user = get_user(q.from_user.id)
    if not user or not user.get("group_code"):
        await q.answer("Сначала выберите группу.", show_alert=True)
        return

    # грузим значения и ссылки (для Zoom)
    vals, links, merges = fetch_sheet_values_and_links(
        spreadsheet_id=settings.spreadsheet_id,
        sheet_gid=settings.sheet_gid,
        creds_path=settings.google_credentials,
    )
    mtx_vals = expand_merged_matrix(vals, merges=merges)
    mtx_links = expand_merged_matrix(links, merges=merges)
    all_lessons = list_lessons_matrix(mtx_vals, mtx_links)
    group_lessons = [it for it in all_lessons if it["group"] == user["group_code"]]

    day_upper = str(day_name).strip().upper()
    day_lessons = [it for it in group_lessons if it["parity"] == parity and it["day"] == day_upper]

    text = format_day(user["group_code"], day_upper, parity, day_lessons)
    await _send_or_edit(q, text, kb_day_controls(day_upper, parity))

@router.callback_query(F.data.startswith("sched:week"))
async def sched_week(q: CallbackQuery):
    user = get_user(q.from_user.id)
    if not user or not user.get("group_code"):
        await q.answer("Сначала выберите группу.", show_alert=True)
        return

    tz = user.get("timezone") or settings.timezone
    parity = q.data.split(":")[-1]
    if parity == "auto":
        parity = week_parity_for_date(None, tz)

    # значения + ссылки (для Zoom)
    vals, links, merges = fetch_sheet_values_and_links(
        spreadsheet_id=settings.spreadsheet_id,
        sheet_gid=settings.sheet_gid,
        creds_path=settings.google_credentials,
    )
    mtx_vals = expand_merged_matrix(vals, merges=merges)
    mtx_links = expand_merged_matrix(links, merges=merges)
    lessons_all = list_lessons_matrix(mtx_vals, mtx_links)
    lessons_grp = [it for it in lessons_all if it["group"] == user["group_code"]]
    week_lessons = [it for it in lessons_grp if it["parity"] == parity]

    text = format_week_compact_mono(user["group_code"], parity, week_lessons)

    # клавиатура
    kb = InlineKeyboardBuilder()
    alt = "чёт" if parity == "нечёт" else "нечёт"
    kb.button(text=f"Сменить на {'Чётную' if alt=='чёт' else 'Нечётную'}", callback_data=f"sched:week:{alt}")
    kb.button(text="Сегодня", callback_data="sched:day:today")
    kb.button(text="Назад", callback_data="sched:root")
    kb.adjust(1, 1, 1)

    # режим редактируемого сообщения (type=1)
    if user.get("type") == 1 and user.get("message_id"):
        try:
            await q.message.bot.edit_message_text(
                chat_id=q.message.chat.id,
                message_id=user["message_id"],
                text=text,
                reply_markup=kb.as_markup()
            )
            await q.answer()
            return
        except Exception:
            pass

    m = await q.message.answer(text, reply_markup=kb.as_markup(), disable_web_page_preview=True)
    if user.get("type") == 1:
        set_message_id(q.from_user.id, m.message_id)
    await q.answer()
