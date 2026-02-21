from __future__ import annotations

import asyncio
from datetime import timedelta
import logging

from aiogram import Bot

from app.services.db import (
    list_users_for_autosend_at,
    list_users_mode2_enabled,
    get_autosend_last_date,
    set_autosend_last_date,
    set_autosend_message_id,
    get_autosend_message_id,
    set_autosend_cur_key,
    get_autosend_cur_key,
)
from app.services.lessons_loader import load_lessons_for_user_group
from app.utils.week_parity import week_parity_for_date
from app.utils.dt import now_tz
from app.utils.format_schedule import format_day
from app.config import settings
from app.cron.gcal_autosync import gcal_autosync_tick


log = logging.getLogger("autosend")

DAY_NAMES_UPPER = ["ПОНЕДЕЛЬНИК","ВТОРНИК","СРЕДА","ЧЕТВЕРГ","ПЯТНИЦА","СУББОТА","ВОСКРЕСЕНЬЕ"]


def _norm_parity(p: str) -> str:
    p = str(p or "").strip().lower().replace("ё", "е")
    if "неч" in p:
        return "нечёт"
    if "чет" in p:
        return "чёт"
    return p

def _to_minutes(hhmm_or_hhmmss: str) -> int:
    """
    'HH:MM' или 'HH:MM:SS' -> минуты от полуночи.
    Допускаем однозначные часы '9:05'.
    """
    parts = hhmm_or_hhmmss.strip().split(":")
    h = int(parts[0])
    m = int(parts[1]) if len(parts) > 1 else 0
    return h * 60 + m

def _parse_slot_minutes(slot: str) -> tuple[int, int]:
    """
    'HH:MM-HH:MM' или 'HH:MM:SS–HH:MM:SS' -> (start_min, end_min)
    Поддерживаем дефис и en-dash.
    """
    s = slot.replace("–", "-").strip()
    a, b = [x.strip() for x in s.split("-", 1)]
    # Отрезаем секунды, если есть
    a = a[:5] if len(a) >= 5 else a
    b = b[:5] if len(b) >= 5 else b
    return _to_minutes(a), _to_minutes(b)

async def _build_day_text_for_user(user: dict) -> str:
    tz = getattr(settings, "timezone", "Europe/Moscow")
    dt_now = now_tz(tz)
    parity = week_parity_for_date(dt_now, tz)
    day_upper = DAY_NAMES_UPPER[dt_now.weekday()]

    lessons = await load_lessons_for_user_group(user)
    day_lessons = [
        it for it in lessons
        if _norm_parity(it.get("parity")) == _norm_parity(parity) and str(it["day"]).strip().upper() == day_upper
    ]

    # красивый вывод «на день»
    text = format_day(user["group_code"], day_upper, parity, day_lessons)
    return text

def _pick_next_lesson(lessons: list[dict], now_minutes: int) -> dict | None:
    """
    Возвращает ближайшее занятие:
      - если сейчас идёт пара (start <= now < end) — вернём её (status='ongoing')
      - иначе — первую будущую (start >= now) (status='upcoming')
      - если ничего не осталось — None
    """
    if not lessons:
        return None

    def _start_min(lesson: dict) -> int:
        try:
            sm, _ = _parse_slot_minutes(str(lesson.get("time", "")))
            return sm
        except Exception:
            return 24 * 60 + 1  # в конец

    for it in sorted(lessons, key=_start_min):
        try:
            sm, em = _parse_slot_minutes(str(it.get("time", "")))
        except Exception:
            continue
        if sm <= now_minutes < em:
            return {**it, "_status": "ongoing", "_start_min": sm, "_end_min": em}
        if now_minutes <= sm:
            return {**it, "_status": "upcoming", "_start_min": sm, "_end_min": em}

    return None

async def _morning_send_mode1(bot: Bot, hhmm: str, ymd: str):
    users = list_users_for_autosend_at(hhmm, mode=1)
    log.debug("mode1 scan @%s -> %d users", hhmm, len(users))
    for u in users:
        if get_autosend_last_date(u["telegram_id"]) == ymd:
            log.debug("mode1 skip user=%s already sent today", u["telegram_id"]); continue
        dt_now, parity, day_upper, day_lessons = await _load_todays_lessons_for_user(u)
        log.info("mode1 send to user=%s group=%s lessons=%d parity=%s day=%s",
                 u["telegram_id"], u["group_code"], len(day_lessons), parity, day_upper)
        text = format_day(u["group_code"], day_upper, parity, day_lessons)
        try:
            await bot.send_message(chat_id=u["telegram_id"], text=text)
            set_autosend_last_date(u["telegram_id"], ymd)
        except Exception as e:
            log.exception("mode1 send failed user=%s: %s", u["telegram_id"], e)

async def _morning_send_mode2(bot: Bot, hhmm: str, ymd: str):
    users = list_users_for_autosend_at(hhmm, mode=2)
    log.info("mode2 scan @%s -> %d users", hhmm, len(users))  # INFO вместо DEBUG, чтобы точно видеть
    for u in users:
        last = get_autosend_last_date(u["telegram_id"])
        msg_id = get_autosend_message_id(u["telegram_id"])
        # ⚠️ ФИКС: если уже что-то отправляли сегодня И у нас есть msg_id — пропускаем,
        # а если msg_id нет (например, режим меняли днём) — отправим сейчас.
        if last == ymd and msg_id:
            log.info("mode2 skip user=%s (already sent today, msg_id=%s)", u["telegram_id"], msg_id)
            continue

        dt_now, parity, day_upper, day_lessons = await _load_todays_lessons_for_user(u)
        now_minutes = dt_now.hour * 60 + dt_now.minute
        next_lesson = _pick_next_lesson(day_lessons, now_minutes)
        log.info("mode2 morning user=%s group=%s next=%s lessons=%d parity=%s day=%s",
                 u["telegram_id"], u["group_code"], (next_lesson or {}).get("time"),
                 len(day_lessons), parity, day_upper)
        text = _format_next_text(u, parity, day_upper, next_lesson)
        try:
            m = await bot.send_message(chat_id=u["telegram_id"], text=text)
            set_autosend_message_id(u["telegram_id"], m.message_id)
            set_autosend_cur_key(u["telegram_id"], _make_key(ymd, next_lesson))
            set_autosend_last_date(u["telegram_id"], ymd)
        except Exception as e:
            log.exception("mode2 morning send failed user=%s: %s", u["telegram_id"], e)

async def _live_update_mode2(bot: Bot, ymd: str):
    users = list_users_mode2_enabled()
    log.debug("mode2 live update scan: %d users", len(users))
    for u in users:
        if get_autosend_last_date(u["telegram_id"]) != ymd:
            log.debug("mode2 live skip user=%s (no morning send yet)", u["telegram_id"])
            continue
        msg_id = get_autosend_message_id(u["telegram_id"])
        if not msg_id:
            log.debug("mode2 live skip user=%s (no msg_id)", u["telegram_id"])
            continue
        dt_now, parity, day_upper, day_lessons = await _load_todays_lessons_for_user(u)
        now_minutes = dt_now.hour * 60 + dt_now.minute
        next_lesson = _pick_current_or_next(day_lessons, now_minutes)
        new_key = _make_key(ymd, next_lesson)
        old_key = get_autosend_cur_key(u["telegram_id"]) or ""
        if new_key == old_key:
            continue
        log.info("mode2 edit user=%s msg=%s old=%s new=%s", u["telegram_id"], msg_id, old_key, new_key)
        text = _format_next_text(u, parity, day_upper, next_lesson)
        try:
            await bot.edit_message_text(chat_id=u["telegram_id"], message_id=msg_id, text=text)
            set_autosend_cur_key(u["telegram_id"], new_key)
        except Exception as e:
            log.warning("mode2 edit failed user=%s msg=%s: %s (reset msg id)", u["telegram_id"], msg_id, e)
            set_autosend_message_id(u["telegram_id"], None)

async def _tick(bot: Bot):
    tz = getattr(settings, "timezone", "Europe/Moscow")
    log.info("autosend runner started, timezone=%s", tz)
    while True:
        try:
            now = now_tz(tz)
            hhmm = now.strftime("%H:%M")
            ymd = now.strftime("%Y-%m-%d")
            log.debug("tick now=%s hhmm=%s", now.isoformat(), hhmm)

            await _morning_send_mode1(bot, hhmm, ymd)
            await _morning_send_mode2(bot, hhmm, ymd)
            await _live_update_mode2(bot, ymd)
            await gcal_autosync_tick(bot)

            await asyncio.sleep(30)
        except asyncio.CancelledError:
            log.info("autosend runner cancelled")
            break
        except Exception as e:
            log.exception("autosend tick error: %s", e)
            await asyncio.sleep(5)

_task = None

def start_autosend(bot: Bot):
    global _task
    if _task is None or _task.done():
        loop = asyncio.get_event_loop()
        _task = loop.create_task(_tick(bot))
        log.info("autosend task scheduled")

def _parse_start_minutes(time_range: str) -> int:
    # "8:10-9:40" -> 8*60+10
    try:
        left = time_range.split("-")[0].strip()
        hh, mm = left.split(":")
        return int(hh) * 60 + int(mm)
    except Exception:
        return 10**9  # в конец

async def _load_todays_lessons_for_user(user: dict):
    tz = getattr(settings, "timezone", "Europe/Moscow")
    dt_now = now_tz(tz)
    parity = week_parity_for_date(dt_now, tz)
    day_upper = DAY_NAMES_UPPER[dt_now.weekday()]

    lessons = await load_lessons_for_user_group(user)
    day_lessons = [
        it for it in lessons
        if _norm_parity(it.get("parity")) == _norm_parity(parity) and str(it["day"]).strip().upper() == day_upper
    ]
    return dt_now, parity, day_upper, day_lessons


def _parse_interval_minutes(time_range: str) -> tuple[int, int]:
    # "8:10-9:40" -> (490, 580)
    try:
        left, right = time_range.split("-")
        h1, m1 = map(int, left.strip().split(":"))
        h2, m2 = map(int, right.strip().split(":"))
        return h1*60+m1, h2*60+m2
    except Exception:
        return (10**9, 10**9)

def _pick_current_or_next(day_lessons: list, now_minutes: int):
    # вернёт текущую (если идёт) или ближайшую будущую; сменится только ПОСЛЕ конца
    for it in sorted(day_lessons, key=lambda x: _parse_interval_minutes(x["time"])[0]):
        s, e = _parse_interval_minutes(it["time"])
        if e > now_minutes:   # пока не закончилась — держим её
            return it
    return None

def _make_key(ymd: str, lesson: dict | None) -> str:
    if not lesson:
        return f"{ymd}|NONE"
    return f"{ymd}|{lesson.get('time','')}|{lesson.get('text','')}"

def _format_next_text(user: dict, parity: str, day_upper: str, lesson: dict | None) -> str:
    if not lesson:
        return f"🔔 Ближайшая пара\n\n📅 <b>Группа {user['group_code']}</b> • {('Чётная' if parity=='чёт' else 'Нечётная')} неделя • {day_upper.title()}\n" \
               f"{'-'*50}\nНа сегодня занятий больше нет."
    # используем наш дневной форматтер на одно занятие
    s = format_day(user["group_code"], day_upper, parity, [lesson])
    return "🔔 Ближайшая пара\n\n" + s
