from __future__ import annotations

import os
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from app.services.gcal_client import list_calendars, create_calendar
from app.services.db import get_user, set_gcal_calendar_id
from contextlib import suppress
from aiogram.types import Message
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton
from aiogram import Router, F
from datetime import timedelta
from aiogram.types import CallbackQuery
from app.services.gcal_client import upsert_event
from app.handlers.schedule_view import _load_lessons_for_user_group
from app.services.gcal_mapper import lesson_to_event
from app.utils.dt import now_tz
from app.services.db import set_gcal_last_sync
from app.utils.week_parity import week_parity_for_date
from app.services.db import set_gcal_autosync_weekday, set_gcal_autosync_time, get_gcal_autosync, set_gcal_autosync_mode, set_gcal_autosync_enabled

from app.services.db import (
    get_user,
    # ожидаем, что есть эти функции (добавь в db.py при необходимости):
    # set_gcal_connected(telegram_id: int, connected: bool) -> None
    # set_gcal_tokens(telegram_id: int, access: str, refresh: str|None, expiry_iso: str) -> None
    # set_gcal_calendar_id(telegram_id: int, cal_id: str) -> None
    # set_gcal_last_sync(telegram_id: int, iso: str) -> None
)
from app.config import settings

import logging
log = logging.getLogger("gcal")

router = Router()

class AutoSyncTime(StatesGroup):
    waiting_time = State()

# ---------- helpers ----------

async def _sync_next_days_for_user(user_id: int, days: int = 7) -> tuple[int, int]:
    u = get_user(user_id)
    if not u or not u.get("gcal_connected"):
        return (0, 0)

    tz = u.get("timezone") or settings.timezone
    base = now_tz(tz)
    lessons = await _load_lessons_for_user_group(u)
    cal_id = u.get("gcal_calendar_id") or "primary"

    ok = fail = 0
    day_to_off = {  # для названия дня из данных
        "ПОНЕДЕЛЬНИК": 0, "ВТОРНИК": 1, "СРЕДА": 2, "ЧЕТВЕРГ": 3, "ПЯТНИЦА": 4, "СУББОТА": 5, "ВОСКРЕСЕНЬЕ": 6
    }

    for i in range(days):
        dt_day = base + timedelta(days=i)
        day_upper = _weekday_upper(dt_day)
        parity = week_parity_for_date(dt_day, tz)  # чётность конкретного дня!

        day_lessons = [
            it for it in lessons
            if str(it.get("parity","")).strip().lower() == str(parity).strip().lower()
            and str(it.get("day","")).strip().upper() == day_upper
        ]

        for lesson in day_lessons:
            try:
                event, key = lesson_to_event(u, lesson, dt_day)
                await asyncio.to_thread(upsert_event, user_id, cal_id, event, key)
                ok += 1
            except Exception:
                fail += 1
                log.exception("sync_next_days failed user=%s day=%s lesson=%r", user_id, dt_day.date(), lesson)

    try:
        from datetime import datetime, timezone
        set_gcal_last_sync(user_id, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    except Exception:
        pass

    return ok, fail

HUSH_UNKNOWN_SUBJECTS = {"история"}  # тут можно расширять: {"история", "английский язык"}
def _is_unknown_time(lesson: dict) -> bool:
    text = (lesson.get("subject") or lesson.get("text") or "").lower()
    # твой парсер уже ставит special=True для «см. прилож.» — используем это
    return bool(lesson.get("special")) or "⚠" in text or "см. прилож" in text

def _is_hushed_unknown(lesson: dict) -> bool:
    text = (lesson.get("subject") or lesson.get("text") or "").lower()
    return _is_unknown_time(lesson) and any(s in text for s in HUSH_UNKNOWN_SUBJECTS)

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
        kb.button(text="⚙️ Автосинхронизация", callback_data="gcal:auto:open")
        kb.button(text="🔄 Синхронизировать сегодня", callback_data="gcal:sync:today")
        kb.button(text="📅 Синхронизировать неделю", callback_data="gcal:sync:week")
        kb.button(text=f"🗂 Календарь: {cal}", callback_data="gcal:choose_cal")
        kb.button(text="🔌 Отключить", callback_data="gcal:disconnect")
    else:
        # Кнопка с прямой ссылкой на OAuth
        kb.button(text="🔗 Подключить Google Calendar", url=_oauth_connect_url(user["telegram_id"]))
    kb.button(text="⬅️ Назад", callback_data="settings:open")
    kb.adjust(1, 1, 1, 1, 1) if connected else kb.adjust(1, 1)
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

def _kb_choose_calendar_dynamic(current_id: str | None, cals: list[dict]):
    """
    Кнопки вида:
      • <summary> (primary)
      <summary>
      ➕ Создать календарь
      🔄 Обновить   ⬅️ Назад
    Выбор делаем по индексу, чтобы не класть длинный calendarId в callback_data.
    """
    kb = InlineKeyboardBuilder()
    cur = (current_id or "primary")

    for idx, it in enumerate(cals):
        mark = "• " if it["id"] == cur else ""
        suffix = " (primary)" if it.get("primary") else ""
        title = (it.get("summary") or it["id"]) + suffix
        kb.button(text=f"{mark}{title}", callback_data=f"gcal:cal:sel:{idx}")

    kb.button(text="➕ Создать отдельный календарь", callback_data="gcal:cal:create")
    kb.button(text="🔄 Обновить", callback_data="gcal:cal:refresh")
    kb.button(text="⬅️ Назад", callback_data="gcal:open")
    # раскладка: по одному в строке для читабельности
    kb.adjust(*([1] * (len(cals) + 2)), 1)
    return kb.as_markup()


def _kb_disconnect_confirm():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔌 Только отвязать", callback_data="gcal:disconnect:confirm:keep")
    kb.button(text="🧹 Отвязать и удалить события", callback_data="gcal:disconnect:confirm:purge")
    kb.button(text="⬅️ Отмена", callback_data="gcal:open")
    kb.adjust(1, 1, 1)
    return kb.as_markup()

def _fmt_last_sync_human(u: dict) -> str:
    """
    Превращает ISO в 'сегодня/вчера в HH:MM (TZ)' или 'DD.MM.YYYY в HH:MM (TZ)'.
    Ожидает u['gcal_last_sync'] вида 'YYYY-MM-DDTHH:MM:SSZ'.
    """
    s = u.get("gcal_last_sync")
    if not s:
        return "—"
    try:
        dt_utc = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return s  # на всякий случай покажем как есть

    tzname = u.get("timezone") or settings.timezone
    try:
        local = dt_utc.astimezone(ZoneInfo(tzname))
    except Exception:
        local = dt_utc  # fallback: UTC

    now_local = now_tz(tzname)
    if local.date() == now_local.date():
        day_part = "сегодня"
    elif (now_local.date() - local.date()).days == 1:
        day_part = "вчера"
    else:
        day_part = local.strftime("%d.%m.%Y")

    return f"{day_part} в {local.strftime('%H:%M')} ({tzname})"

def _status_text(u: dict) -> str:
    connected = bool(u.get("gcal_connected"))
    cal = u.get("gcal_calendar_id") or "primary"
    last = _fmt_last_sync_human(u)
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
async def gcal_choose_calendar(q: CallbackQuery, state: FSMContext):
    with suppress(TelegramBadRequest):
        await q.answer()

    u = get_user(q.from_user.id)
    if not u or not u.get("gcal_connected"):
        await (q.message.answer if q.message else q.bot.send_message)(
            q.from_user.id, "Сначала подключите Google Calendar."
        )
        return

    # грузим список календарей в отдельном потоке
    try:
        cals = await asyncio.to_thread(list_calendars, q.from_user.id)
    except Exception:
        log.exception("list_calendars failed user=%s", q.from_user.id)
        await (q.message.answer if q.message else q.bot.send_message)(
            q.from_user.id, "Не удалось получить список календарей. Попробуйте позже."
        )
        return

    # сохраняем мапу idx->calendarId в FSM (на одного пользователя)
    await state.update_data(gcal_calmap={str(i): it["id"] for i, it in enumerate(cals)})

    text = "Выберите календарь для синхронизации:"
    markup = _kb_choose_calendar_dynamic(u.get("gcal_calendar_id"), cals)
    if q.message:
        await q.message.edit_text(text, reply_markup=markup, disable_web_page_preview=True)
    else:
        await q.bot.send_message(q.from_user.id, text, reply_markup=markup)

@router.callback_query(F.data == "gcal:cal:refresh")
async def gcal_cal_refresh(q: CallbackQuery, state: FSMContext):
    with suppress(TelegramBadRequest):
        await q.answer("Обновляю…")

    u = get_user(q.from_user.id) or {}
    try:
        cals = await asyncio.to_thread(list_calendars, q.from_user.id)
    except Exception:
        log.exception("list_calendars failed user=%s", q.from_user.id)
        await (q.message.answer if q.message else q.bot.send_message)(
            q.from_user.id, "Не удалось обновить список календарей."
        )
        return

    await state.update_data(gcal_calmap={str(i): it["id"] for i, it in enumerate(cals)})

    markup = _kb_choose_calendar_dynamic(u.get("gcal_calendar_id"), cals)
    if q.message:
        await q.message.edit_text("Выберите календарь для синхронизации:", reply_markup=markup)
    else:
        await q.bot.send_message(q.from_user.id, "Выберите календарь для синхронизации:", reply_markup=markup)

@router.callback_query(F.data.startswith("gcal:cal:sel:"))
async def gcal_cal_select(q: CallbackQuery, state: FSMContext):
    with suppress(TelegramBadRequest):
        await q.answer("Сохраняю…")

    idx = q.data.split(":")[-1]
    data = await state.get_data()
    calmap: dict = data.get("gcal_calmap") or {}
    cal_id = calmap.get(idx)

    if not cal_id:
        # мапа устарела — откроем список заново
        return await gcal_choose_calendar(q, state)

    try:
        set_gcal_calendar_id(q.from_user.id, cal_id)
    except Exception:
        log.exception("set_gcal_calendar_id failed user=%s id=%s", q.from_user.id, cal_id)

    # вернуться на главный экран GCAL
    from app.handlers.gcal_sync import gcal_open  # если функция в этом же файле — просто вызываем
    await gcal_open(q)

@router.callback_query(F.data == "gcal:cal:create")
async def gcal_create_separate(q: CallbackQuery):
    with suppress(TelegramBadRequest):
        await q.answer("Создаю календарь…")

    u = get_user(q.from_user.id) or {}
    title = f"Расписание ({u.get('group_code') or 'бот'})"
    tz = u.get("timezone") or settings.timezone

    try:
        new_id = await asyncio.to_thread(create_calendar, q.from_user.id, title, tz)
        set_gcal_calendar_id(q.from_user.id, new_id)
        msg = f"✅ Календарь «{title}» создан и выбран."
    except Exception:
        log.exception("create_calendar failed user=%s", q.from_user.id)
        msg = "⛔ Не удалось создать календарь. Попробуйте позже."

    # показываем статус GCAL
    try:
        await (q.message.answer if q.message else q.bot.send_message)(q.from_user.id, msg)
    except Exception:
        pass

    from app.handlers.gcal_sync import gcal_open
    await gcal_open(q)

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
    with suppress(TelegramBadRequest):
        await q.answer("Синхронизация на сегодня…")
    u = get_user(q.from_user.id)
    if not u or not u.get("gcal_connected"):
        await q.answer("Сначала подключите Google Calendar.", show_alert=True); return

    tz = u.get("timezone") or settings.timezone
    now = now_tz(tz)
    parity = week_parity_for_date(now, tz)
    day_upper = _weekday_upper(now)

    try:
        await q.message.edit_text("⏳ Синхронизирую расписание на сегодня…")
    except Exception:
        pass

    # пары пользователя
    lessons = await _load_lessons_for_user_group(u)
    day_lessons = [it for it in lessons if it["parity"] == parity and it["day"] == day_upper]

    cal_id = u.get("gcal_calendar_id") or "primary"
    ok, fail = 0, 0
    for lesson in day_lessons:
        try:
            event, key = lesson_to_event(u, lesson, now)  # или dt_day
            await asyncio.to_thread(upsert_event, q.from_user.id, cal_id, event, key)
            ok += 1
        except Exception as e:
            fail += 1
            log.exception("GCAL sync failed for lesson=%s time=%s day=%s", lesson.get("subject") or lesson.get("text"),
                          lesson.get("time"), lesson.get("day"))

    log.info("sync_today done user=%s ok=%d fail=%d", q.from_user.id, ok, fail)
    # отметка о синхронизации
    try:
        from datetime import datetime, timezone
        set_gcal_last_sync(
            q.from_user.id,
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        )
    except Exception:
        log.exception("set_gcal_last_sync failed user=%s", q.from_user.id)

    log.info("sync_today done user=%s ok=%d fail=%d", q.from_user.id, ok, fail)

    # Перерисовываем экран статуса
    u_ref = {**(get_user(q.from_user.id) or u), "telegram_id": q.from_user.id}
    msg = _status_text(u_ref)
    msg += f"\n\nГотово: добавлено/обновлено {ok}, ошибок {fail}."
    await q.message.edit_text(
        msg,
        reply_markup=_kb_root(u_ref),
        disable_web_page_preview=True,
    )

@router.callback_query(F.data == "gcal:sync:week")
async def gcal_sync_week(q: CallbackQuery):
    # 1) МГНОВЕННО подтверждаем callback (чтобы не истёк)
    with suppress(TelegramBadRequest):
        await q.answer("Запускаю синхронизацию недели…")  # можно без текста

    u = get_user(q.from_user.id)
    if not u or not u.get("gcal_connected"):
        # тут уже отвечать не надо — мы подтвердили выше
        await q.message.answer("Сначала подключите Google Calendar.")
        return

    # (опционально) покажем пользователю, что процесс пошёл
    await q.message.edit_text("⏳ Синхронизирую расписание на неделю…")
    u = get_user(q.from_user.id)
    if not u or not u.get("gcal_connected"):
        await q.answer("Сначала подключите Google Calendar.", show_alert=True); return

    tz = u.get("timezone") or settings.timezone
    # auto/чёт/нечёт мы уже обрабатываем в UI; здесь возьмём auto
    from app.utils.week_parity import week_parity_for_date
    parity = week_parity_for_date(None, tz)

    lessons = await _load_lessons_for_user_group(u)
    week_lessons = [it for it in lessons if it["parity"] == parity]

    # Дата-референс: ближайший понедельник текущей недели
    base = now_tz(tz)
    # создадим мапу day->offset
    day_to_off = {"ПОНЕДЕЛЬНИК":0,"ВТОРНИК":1,"СРЕДА":2,"ЧЕТВЕРГ":3,"ПЯТНИЦА":4,"СУББОТА":5,"ВОСКРЕСЕНЬЕ":6}
    monday = base - timedelta(days=base.weekday())

    cal_id = u.get("gcal_calendar_id") or "primary"
    log.info(
        "sync_week start user=%s tz=%s parity=%s total=%d filtered=%d monday=%s cal=%s",
        q.from_user.id, tz, parity, len(lessons), len(week_lessons),
        monday.date().isoformat(), cal_id
    )
    ok, fail = 0, 0
    for idx, lesson in enumerate(week_lessons, 1):
        try:
            day_raw = str(lesson.get("day", "")).strip().upper()
            if day_raw not in day_to_off:
                fail += 1
                log.error("sync_week bad day value: %r | lesson=%r", day_raw, lesson)
                continue

            offset = day_to_off[day_raw]
            dt_day = monday + timedelta(days=offset)

            event, key = lesson_to_event(u, lesson, dt_day)

            log.debug(
                "sync_week build #%d key=%s summary=%r start=%s end=%s location=%r",
                idx, key,
                event.get("summary"),
                (event.get("start") or {}).get("dateTime"),
                (event.get("end") or {}).get("dateTime"),
                event.get("location"),
            )

            created = await asyncio.to_thread(upsert_event, q.from_user.id, cal_id, event, key)

            log.debug(
                "sync_week upsert ok #%d id=%s status=%s link=%s",
                idx, created.get("id"), created.get("status"), created.get("htmlLink")
            )
            ok += 1

        except Exception:
            fail += 1
            log.exception(
                "sync_week failed user=%s cal=%s idx=%d lesson=%r",
                q.from_user.id, cal_id, idx, lesson
            )

    try:
        from datetime import datetime, timezone
        set_gcal_last_sync(
            q.from_user.id, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        )
    except Exception:
        log.exception("set_gcal_last_sync failed user=%s", q.from_user.id)

    log.info("sync_week done user=%s ok=%d fail=%d", q.from_user.id, ok, fail)

    msg = _status_text({**u, "telegram_id": q.from_user.id})
    msg += f"\n\nГотово: добавлено/обновлено {ok}, ошибок {fail}."
    await q.message.edit_text(msg, reply_markup=_kb_root({**u, "telegram_id": q.from_user.id}),disable_web_page_preview=True)

async def _sync_today_for_user(user_id: int) -> tuple[int,int]:
    u = get_user(user_id)
    if not u or not u.get("gcal_connected"):
        return (0, 0)
    tz = u.get("timezone") or settings.timezone
    now = now_tz(tz)
    parity = week_parity_for_date(now, tz)
    day_upper = _weekday_upper(now)
    lessons = await _load_lessons_for_user_group(u)
    unknown_hushed = [it for it in lessons if _is_hushed_unknown(it)]
    today = [it for it in lessons
             if str(it.get("parity","")).strip().lower() == str(parity).strip().lower()
             and str(it.get("day","")).strip().upper() == day_upper]
    if unknown_hushed:
        subj_names = sorted(
            {(it.get("subject") or it.get("text") or "Предмет").split(" —")[0] for it in unknown_hushed})
        note = "⚠️ Сегодня есть " + ", ".join(subj_names) + ", но время не указано. Я не добавлял событие в календарь."
        # показываем пользователю один раз
        await q.message.answer(note)
    cal_id = u.get("gcal_calendar_id") or "primary"
    ok = fail = 0
    for lesson in today:
        try:
            event, key = lesson_to_event(u, lesson, now)
            await asyncio.to_thread(upsert_event, user_id, cal_id, event, key)
            ok += 1
        except Exception:
            fail += 1
            log.exception("sync_today core failed user=%s lesson=%r", user_id, lesson)
    try:
        from datetime import datetime, timezone
        set_gcal_last_sync(user_id, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    except Exception:
        pass
    return ok, fail

async def _sync_week_for_user(user_id: int, weeks_ahead: int = 0) -> tuple[int,int]:
    """
    Синхронизирует одну неделю пользователя.
    weeks_ahead=0 — текущая неделя, =1 — следующая и т.д.
    """
    u = get_user(user_id)
    if not u or not u.get("gcal_connected"):
        return (0, 0)
    tz = u.get("timezone") or settings.timezone

    base = now_tz(tz)
    monday = base - timedelta(days=base.weekday()) + timedelta(days=7*weeks_ahead)

    # чётность берём именно от понедельника той недели
    parity = week_parity_for_date(monday, tz)

    lessons = await _load_lessons_for_user_group(u)

    def norm(x): return str(x or "").strip().lower()
    week_lessons = [it for it in lessons if norm(it.get("parity")) == norm(parity)]

    # (опционально) фильтрация «неизвестных по времени» предметов (например, ИСТОРИЯ «см. прилож.»)
    try:
        if '_is_hushed_unknown' in globals():
            week_lessons = [it for it in week_lessons if not _is_hushed_unknown(it)]
    except Exception:
        pass

    cal_id = u.get("gcal_calendar_id") or "primary"
    day_to_off = {"ПОНЕДЕЛЬНИК":0,"ВТОРНИК":1,"СРЕДА":2,"ЧЕТВЕРГ":3,"ПЯТНИЦА":4,"СУББОТА":5,"ВОСКРЕСЕНЬЕ":6}

    ok = fail = 0
    for lesson in week_lessons:
        try:
            off = day_to_off[str(lesson["day"]).strip().upper()]
            dt_day = monday + timedelta(days=off)
            event, key = lesson_to_event(u, lesson, dt_day)
            await asyncio.to_thread(upsert_event, user_id, cal_id, event, key)
            ok += 1
        except Exception:
            fail += 1
            log.exception("sync_week core failed user=%s weeks_ahead=%s lesson=%r", user_id, weeks_ahead, lesson)

    try:
        from datetime import datetime, timezone
        set_gcal_last_sync(user_id, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    except Exception:
        pass

    return ok, fail

# ---------- disconnect ----------

@router.callback_query(F.data == "gcal:disconnect")
async def gcal_disconnect_open(q: CallbackQuery):
    # быстрый ACK, чтобы не словить таймаут
    with suppress(TelegramBadRequest):
        await q.answer()

    u = get_user(q.from_user.id)
    if not u or not u.get("gcal_connected"):
        await q.message.edit_text("Google Calendar уже не подключён.", reply_markup=_kb_root({**(u or {}), "telegram_id": q.from_user.id}))
        return

    await q.message.edit_text(
        "Вы действительно хотите отключить Google Calendar?\n"
        "Можно просто отвязать аккаунт или отвязать и удалить все созданные ботом события.",
        reply_markup=_kb_disconnect_confirm(),
        disable_web_page_preview=True,
    )

@router.callback_query(F.data.startswith("gcal:disconnect:confirm:"))
async def gcal_disconnect_confirm(q: CallbackQuery):
    with suppress(TelegramBadRequest):
        await q.answer("Отключаю…")

    action = q.data.rsplit(":", 1)[-1]  # keep|purge
    u = get_user(q.from_user.id) or {}
    cal_id = u.get("gcal_calendar_id") or "primary"

    ok_deleted = 0
    try:
        if action == "purge":
            # безопасно выполняем блокирующие вызовы в потоке
            from app.services.gcal_client import delete_events_by_tag  # type: ignore
            ok_deleted = await asyncio.to_thread(
                delete_events_by_tag, q.from_user.id, cal_id, "sched_bot", "1"
            )
    except Exception:
        log.exception("gcal purge events failed user=%s cal=%s", q.from_user.id, cal_id)

    # отзываем токены в Google (не обязательно, но правильно)
    try:
        from app.services.gcal_client import revoke_tokens  # type: ignore
        await asyncio.to_thread(revoke_tokens, q.from_user.id)
    except Exception:
        log.exception("gcal revoke tokens failed user=%s", q.from_user.id)

    # чистим БД-флаги
    try:
        from app.services.db import set_gcal_connected, set_gcal_tokens, set_gcal_calendar_id  # type: ignore
        set_gcal_connected(q.from_user.id, False)
        set_gcal_tokens(q.from_user.id, "", None, "")
        set_gcal_calendar_id(q.from_user.id, None)
    except Exception:
        log.exception("gcal DB cleanup failed user=%s", q.from_user.id)

    # перерисовываем экран
    u2 = get_user(q.from_user.id) or {}
    u2 = {**u2, "telegram_id": q.from_user.id}
    msg = _status_text(u2)
    if action == "purge":
        msg += f"\n\n🧹 Удалено событий: {ok_deleted}."
    await q.message.edit_text(
        msg,
        reply_markup=_kb_root(u2),
        disable_web_page_preview=True,
    )

def _wd_name(i: int) -> str:
    return ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"][i]

def _mode_label(mode: str) -> str:
    return {
        "daily": "Ежедневно",
        "weekly": "Еженедельно",
        "weekly2": "Еженедельно (2 недели)",
    }[mode]

def _kb_auto_settings(u: dict):
    a = get_gcal_autosync(u["telegram_id"])
    mode = (a.get("gcal_autosync_mode") or "daily")
    kb = InlineKeyboardBuilder()
    kb.button(text=("🟢 Вкл" if a.get("gcal_autosync_enabled") else "⚪️ Выкл"), callback_data="gcal:auto:toggle")
    kb.button(text=f"Режим: {_mode_label(mode)}", callback_data="gcal:auto:mode")
    kb.button(text=f"Время: {a.get('gcal_autosync_time') or '08:00'}", callback_data="gcal:auto:time")
    if mode in ("weekly", "weekly2"):  # день недели нужен для обоих weekly-режимов
        wday = int(a.get("gcal_autosync_weekday") if a.get("gcal_autosync_weekday") is not None else 0)
        kb.button(text=f"День: {['Пн','Вт','Ср','Чт','Пт','Сб','Вс'][wday]}", callback_data="gcal:auto:weekday")
    kb.button(text="⬅️ Назад", callback_data="gcal:open")
    rows = [1,1,1]
    if mode in ("weekly", "weekly2"):
        rows.append(1)
    rows.append(1)
    kb.adjust(*rows)
    return kb.as_markup()

@router.callback_query(F.data == "gcal:auto:open")
async def gcal_auto_open(q: CallbackQuery):
    u = get_user(q.from_user.id) or {}
    u = {**u, "telegram_id": q.from_user.id}
    a = get_gcal_autosync(q.from_user.id)
    text = [
        "⚙️ <b>Автосинхронизация</b>",
        f"Статус: {'🟢 Включена' if a.get('gcal_autosync_enabled') else '⚪️ Выключена'}",
        f"Режим: {'Ежедневно' if (a.get('gcal_autosync_mode') or 'daily')=='daily' else 'Еженедельно'}",
        f"Время: {a.get('gcal_autosync_time') or '08:00'}",
    ]
    if (a.get("gcal_autosync_mode") or "daily") == "weekly":
        wd = int(a.get("gcal_autosync_weekday") if a.get("gcal_autosync_weekday") is not None else 0)
        text.append(f"День: {_wd_name(wd)}")
    await q.message.edit_text("\n".join(text), reply_markup=_kb_auto_settings(u))

@router.callback_query(F.data == "gcal:auto:toggle")
async def gcal_auto_toggle(q: CallbackQuery):
    a = get_gcal_autosync(q.from_user.id)
    set_gcal_autosync_enabled(q.from_user.id, not bool(a.get("gcal_autosync_enabled")))
    await gcal_auto_open(q)

@router.callback_query(F.data == "gcal:auto:mode")
async def gcal_auto_mode(q: CallbackQuery):
    a = get_gcal_autosync(q.from_user.id)
    mode = (a.get("gcal_autosync_mode") or "daily")
    order = ["daily", "weekly", "weekly2", "rolling7"]  # <-- добавили weekly2
    new = order[(order.index(mode) + 1) % len(order)]
    set_gcal_autosync_mode(q.from_user.id, new)
    if new in ("weekly", "weekly2") and a.get("gcal_autosync_weekday") is None:
        set_gcal_autosync_weekday(q.from_user.id, 0)  # Пн по умолчанию
    await gcal_auto_open(q)

# Простая сетка популярных времён
def _kb_auto_time():
    kb = InlineKeyboardBuilder()
    # быстрые варианты:
    for t in ("06:00","07:00","08:00","21:00"):
        kb.button(text=t, callback_data=f"gcal:auto:time:set:{t}")
    kb.button(text="✏️ Ввести своё время", callback_data="gcal:auto:time:custom")
    kb.button(text="⬅️ Назад", callback_data="gcal:auto:open")
    kb.adjust(3,3,3,1,1)
    return kb.as_markup()

@router.callback_query(F.data == "gcal:auto:time:custom")
async def gcal_auto_time_custom(q: CallbackQuery, state: FSMContext):
    with suppress(TelegramBadRequest):
        await q.answer()
    await state.set_state(AutoSyncTime.waiting_time)
    await q.message.answer("Пришлите время в формате <b>HH:MM</b> (24-часовой формат). Например: 07:30")

@router.message(AutoSyncTime.waiting_time)
async def gcal_auto_time_custom_set(m: Message, state: FSMContext):
    text = (m.text or "").strip()
    try:
        # тут же сработает проверка формата 'HH:MM'
        set_gcal_autosync_time(m.from_user.id, text)
    except Exception as e:
        await m.answer(f"⛔ {e}\nПопробуйте ещё раз, пример: <code>08:00</code>")
        return
    await state.clear()
    await m.answer("✅ Время автосинхронизации сохранено.")
    # вернём пользователя в экран настроек
    # (если хочешь — можно отредактировать предыдущее сообщение, но проще отправить новое)


@router.callback_query(F.data.startswith("gcal:auto:time:set:"))
async def gcal_auto_time_set(q: CallbackQuery):
    with suppress(TelegramBadRequest):
        await q.answer("Время обновлено")
    prefix = "gcal:auto:time:set:"
    hhmm = q.data[len(prefix):]  # например '07:30'
    try:
        set_gcal_autosync_time(q.from_user.id, hhmm)
    except Exception as e:
        await q.answer(str(e), show_alert=True)
        return
    await gcal_auto_open(q)

@router.callback_query(F.data == "gcal:auto:time")
async def gcal_auto_time_open(q: CallbackQuery):
    with suppress(TelegramBadRequest):
        await q.answer()
    await q.message.edit_text(
        "Выберите время автосинхронизации или введите своё в формате <b>HH:MM</b> (24ч).",
        reply_markup=_kb_auto_time(),
    )

@router.callback_query(F.data.startswith("gcal:auto:time:"))
async def gcal_auto_time_set(q: CallbackQuery):
    hhmm = q.data.split(":")[-1]
    try:
        set_gcal_autosync_time(q.from_user.id, hhmm)
    except Exception as e:
        await q.answer(str(e), show_alert=True); return
    await gcal_auto_open(q)

def _kb_auto_weekday():
    kb = InlineKeyboardBuilder()
    for i, name in enumerate(("Пн","Вт","Ср","Чт","Пт","Сб","Вс")):
        kb.button(text=name, callback_data=f"gcal:auto:weekday:{i}")
    kb.button(text="⬅️ Назад", callback_data="gcal:auto:open")
    kb.adjust(4,4,1)
    return kb.as_markup()

@router.callback_query(F.data == "gcal:auto:weekday")
async def gcal_auto_weekday_open(q: CallbackQuery):
    await q.message.edit_text("Выберите день недели:", reply_markup=_kb_auto_weekday())

@router.callback_query(F.data.startswith("gcal:auto:weekday:"))
async def gcal_auto_weekday_set(q: CallbackQuery):
    wd = int(q.data.split(":")[-1])
    try:
        set_gcal_autosync_weekday(q.from_user.id, wd)
    except Exception as e:
        await q.answer(str(e), show_alert=True); return
    await gcal_auto_open(q)
