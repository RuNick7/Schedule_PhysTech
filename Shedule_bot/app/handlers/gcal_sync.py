from __future__ import annotations

import os
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton
from aiogram import Router, F
from datetime import timedelta
from aiogram.types import CallbackQuery
from Shedule_bot.app.services.gcal_client import upsert_event
from Shedule_bot.app.handlers.schedule_view import _load_lessons_for_user_group
from Shedule_bot.app.services.gcal_mapper import lesson_to_event
from Shedule_bot.app.utils.dt import now_tz
from Shedule_bot.app.services.db import set_gcal_last_sync
from Shedule_bot.app.utils.week_parity import week_parity_for_date

from Shedule_bot.app.services.db import (
    get_user,
    # ожидаем, что есть эти функции (добавь в db.py при необходимости):
    # set_gcal_connected(telegram_id: int, connected: bool) -> None
    # set_gcal_tokens(telegram_id: int, access: str, refresh: str|None, expiry_iso: str) -> None
    # set_gcal_calendar_id(telegram_id: int, cal_id: str) -> None
    # set_gcal_last_sync(telegram_id: int, iso: str) -> None
)
from Shedule_bot.app.config import settings

import logging
log = logging.getLogger("gcal")

router = Router()

# ---------- helpers ----------
def _weekday_upper(dt) -> str:
    return ["ПОНЕДЕЛЬНИК","ВТОРНИК","СРЕДА","ЧЕТВЕРГ","ПЯТНИЦА","СУББОТА","ВОСКРЕСЕНЬЕ"][dt.weekday()]


def _public_base_url() -> str:
    # пробуем из pydantic-конфига; если нет — из ENV
    return (getattr(settings, "public_base_url", None) or os.getenv("PUBLIC_BASE_URL", "")).rstrip("/")

def _oauth_connect_url(telegram_id: int) -> str:
    base = _public_base_url()
    if not base:
        # пусть лучше бросит понятную ошибку на экране
        return "about:blank"
    return f"{base}/oauth2/connect?state={telegram_id}"

def _kb_root(user: dict):
    kb = InlineKeyboardBuilder()
    connected = bool(user.get("gcal_connected"))
    cal = user.get("gcal_calendar_id") or "primary"
    if connected:
        kb.button(text="🔄 Синхронизировать сегодня", callback_data="gcal:sync:today")
        kb.button(text="📅 Синхронизировать неделю", callback_data="gcal:sync:week")
        kb.button(text=f"🗂 Календарь: {cal}", callback_data="gcal:choose_cal")
        kb.button(text="🔌 Отключить", callback_data="gcal:disconnect")
    else:
        # Кнопка с прямой ссылкой на OAuth
        kb.button(text="🔗 Подключить Google Calendar", url=_oauth_connect_url(user["telegram_id"]))
    kb.button(text="⬅️ Назад", callback_data="settings:open")
    kb.adjust(1, 1, 1, 1) if connected else kb.adjust(1, 1)
    return kb.as_markup()

def _kb_choose_calendar(current: str | None):
    kb = InlineKeyboardBuilder()
    cur = (current or "primary")
    prefix = "• "  # пометка выбранного
    kb.button(text=f"{prefix if cur=='primary' else ''}Primary", callback_data="gcal:cal:primary")
    # оставим заготовку на отдельный календарь (создадим позже через API)
    kb.button(text="➕ Создать отдельный календарь", callback_data="gcal:cal:create")
    kb.button(text="⬅️ Назад", callback_data="gcal:open")
    kb.adjust(1, 1, 1)
    return kb.as_markup()

def _status_text(u: dict) -> str:
    connected = bool(u.get("gcal_connected"))
    cal = u.get("gcal_calendar_id") or "primary"
    last = u.get("gcal_last_sync") or "—"
    lines = [
        "📆 <b>Google Calendar</b>",
        f"Статус: {'✅ Подключен' if connected else '⛔️ Не подключен'}",
    ]
    if connected:
        lines += [
            f"Календарь: <b>{cal}</b>",
            f"Последняя синхронизация: {last}",
            "",
            "Выберите действие:",
        ]
    else:
        lines += [
            "",
            "Нажмите «Подключить Google Calendar», затем вернитесь в бота.",
        ]
    return "\n".join(lines)

# ---------- entry ----------

@router.callback_query(F.data.in_({"main:gcal", "gcal:open"}))
async def gcal_open(q: CallbackQuery):
    u = get_user(q.from_user.id)
    if not u:
        await q.answer("Сначала /start", show_alert=True); return
    # добавим в объект user поле telegram_id, чтобы собрать URL с state
    u = {**u, "telegram_id": q.from_user.id}
    await q.message.edit_text(
        _status_text(u),
        reply_markup=_kb_root(u),
        disable_web_page_preview=True,
    )
    await q.answer()

# ---------- choose calendar ----------

@router.callback_query(F.data == "gcal:choose_cal")
async def gcal_choose_calendar(q: CallbackQuery):
    u = get_user(q.from_user.id)
    if not u or not u.get("gcal_connected"):
        await q.answer("Сначала подключите Google Calendar.", show_alert=True); return
    await q.message.edit_text(
        "Выберите календарь для синхронизации:",
        reply_markup=_kb_choose_calendar(u.get("gcal_calendar_id")),
        disable_web_page_preview=True,
    )
    await q.answer()

@router.callback_query(F.data == "gcal:cal:primary")
async def gcal_set_primary(q: CallbackQuery):
    try:
        from app.services.db import set_gcal_calendar_id  # type: ignore
    except Exception:
        await q.answer("Не найдена функция set_gcal_calendar_id в БД.", show_alert=True); return
    set_gcal_calendar_id(q.from_user.id, "primary")
    await gcal_open(q)

@router.callback_query(F.data == "gcal:cal:create")
async def gcal_create_separate(q: CallbackQuery):
    """
    Заглушка: создание отдельного календаря добавим после gcal_client.
    Пока просто сообщим пользователю.
    """
    await q.answer("Создание отдельного календаря будет доступно скоро.", show_alert=True)

# ---------- sync actions (stubs for now) ----------

@router.callback_query(F.data == "gcal:sync:today")
async def gcal_sync_today(q: CallbackQuery):
    u = get_user(q.from_user.id)
    if not u or not u.get("gcal_connected"):
        await q.answer("Сначала подключите Google Calendar.", show_alert=True); return

    tz = u.get("timezone") or settings.timezone
    now = now_tz(tz)
    parity = week_parity_for_date(now, tz)
    day_upper = _weekday_upper(now)

    # пары пользователя
    lessons = await _load_lessons_for_user_group(u)
    day_lessons = [it for it in lessons if it["parity"] == parity and it["day"] == day_upper]

    cal_id = u.get("gcal_calendar_id") or "primary"
    ok, fail = 0, 0
    for lesson in day_lessons:
        try:
            event, key = lesson_to_event(u, lesson, now)  # или dt_day
            await q.bot.loop.run_in_executor(None, lambda: upsert_event(q.from_user.id, cal_id, event, key))
            ok += 1
        except Exception as e:
            fail += 1
            log.exception("GCAL sync failed for lesson=%s time=%s day=%s", lesson.get("subject") or lesson.get("text"),
                          lesson.get("time"), lesson.get("day"))

    log.info("sync_today done user=%s ok=%d fail=%d", q.from_user.id, ok, fail)
    # отметка о синхронизации
    try:
        from datetime import datetime, timezone
        set_gcal_last_sync(q.from_user.id, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    except Exception:
        pass

    # Обновляем экран
    msg = _status_text({**u, "telegram_id": q.from_user.id})
    msg += f"\n\nГотово: добавлено/обновлено {ok}, ошибок {fail}."
    await q.message.edit_text(msg, reply_markup=_kb_root({**u, "telegram_id": q.from_user.id}), disable_web_page_preview=True)
    await q.answer()

@router.callback_query(F.data == "gcal:sync:week")
async def gcal_sync_week(q: CallbackQuery):
    u = get_user(q.from_user.id)
    if not u or not u.get("gcal_connected"):
        await q.answer("Сначала подключите Google Calendar.", show_alert=True); return

    tz = u.get("timezone") or settings.timezone
    # auto/чёт/нечёт мы уже обрабатываем в UI; здесь возьмём auto
    from Shedule_bot.app.utils.week_parity import week_parity_for_date
    parity = week_parity_for_date(None, tz)

    lessons = await _load_lessons_for_user_group(u)
    week_lessons = [it for it in lessons if it["parity"] == parity]

    # Дата-референс: ближайший понедельник текущей недели
    base = now_tz(tz)
    # создадим мапу day->offset
    day_to_off = {"ПОНЕДЕЛЬНИК":0,"ВТОРНИК":1,"СРЕДА":2,"ЧЕТВЕРГ":3,"ПЯТНИЦА":4,"СУББОТА":5,"ВОСКРЕСЕНЬЕ":6}
    monday = base - timedelta(days=base.weekday())

    cal_id = u.get("gcal_calendar_id") or "primary"
    ok, fail = 0, 0
    for lesson in week_lessons:
        try:
            offset = day_to_off[str(lesson["day"]).strip().upper()]
            dt_day = monday + timedelta(days=offset)
            event, key = lesson_to_event(u, lesson, dt_day)
            await q.bot.loop.run_in_executor(
                None, lambda: upsert_event(q.from_user.id, cal_id, event, key)
            )
            ok += 1
        except Exception:
            fail += 1

    try:
        from datetime import datetime, timezone
        set_gcal_last_sync(q.from_user.id, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    except Exception:
        pass

    msg = _status_text({**u, "telegram_id": q.from_user.id})
    msg += f"\n\nГотово: добавлено/обновлено {ok}, ошибок {fail}."
    await q.message.edit_text(msg, reply_markup=_kb_root({**u, "telegram_id": q.from_user.id}), disable_web_page_preview=True)
    await q.answer()

# ---------- disconnect ----------

@router.callback_query(F.data == "gcal:disconnect")
async def gcal_disconnect(q: CallbackQuery):
    try:
        from app.services.db import set_gcal_connected, set_gcal_tokens, set_gcal_calendar_id  # type: ignore
    except Exception:
        await q.answer("Не найдены функции в БД для отключения GCAL.", show_alert=True); return

    # сбрасываем флаги и токены
    set_gcal_connected(q.from_user.id, False)
    set_gcal_tokens(q.from_user.id, "", None, "")
    set_gcal_calendar_id(q.from_user.id, None)  # допускаем None в реал. БД

    # обновим экран
    u = get_user(q.from_user.id) or {}
    u = {**u, "telegram_id": q.from_user.id}
    await q.message.edit_text(
        _status_text(u),
        reply_markup=_kb_root(u),
        disable_web_page_preview=True,
    )
    await q.answer()
