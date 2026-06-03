"""
Алерты об экзаменах.

Логика:
  • за 1 день  — уведомление накануне вечером (или утром — зависит от времени)
    Точнее: если сегодня == exam_date - 1 day и текущее время HH:MM совпадает с
    настроенным временем дневного алерта (по умолчанию 20:00).
  • за 2 часа  — если сейчас >= start-2h и сейчас < start-1h55 (5-минутное окно).

Оба алерта отправляются один раз: факт записывается в exam_alerts.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Optional

from aiogram import Bot

from app.config import settings
from app.services.db import (
    get_bot_mode,
    get_bot_setting,
    list_users_with_group,
    exam_alert_sent,
    mark_exam_alert_sent,
)
from app.services.exam_parser import load_exams
from app.utils.dt import now_tz

log = logging.getLogger("exam_runner")

_ALERT_DAY_HOUR   = 20   # час для алерта «завтра экзамен»
_ALERT_DAY_MINUTE = 0


def _parse_hm(time_range: str) -> Optional[tuple[int, int]]:
    """'8:10-9:40' -> (8, 10)"""
    try:
        left = time_range.replace("–", "-").split("-")[0].strip()
        h, m = left.split(":")
        return int(h), int(m)
    except Exception:
        return None


def _format_exam_alert(exam: dict, kind: str) -> str:
    subj = exam.get("subject", "—")
    room = exam.get("room", "")
    time = exam.get("time", "—")
    date_s = exam.get("date_str", "")
    group = exam.get("group", "")

    if kind == "day":
        header = f"📢 Завтра экзамен!"
    else:
        header = f"⏰ Экзамен через ~2 часа!"

    parts = [header, ""]
    parts.append(f"📅 <b>{date_s}</b> — группа <b>{group}</b>")
    parts.append(f"⏰ {time}")
    parts.append(f"📚 {subj}")
    if room:
        parts.append(f"📍 {room}")
    return "\n".join(parts)


async def exam_alerts_tick(bot: Bot):
    if get_bot_mode() != "exams":
        return

    spreadsheet_id = get_bot_setting("exam_spreadsheet_id")
    sheet_gid_raw  = get_bot_setting("exam_sheet_gid")
    if not spreadsheet_id or sheet_gid_raw is None:
        return

    try:
        sheet_gid = int(sheet_gid_raw)
    except ValueError:
        return

    tz = getattr(settings, "timezone", "Europe/Moscow")
    now = now_tz(tz)
    today: date = now.date()
    now_min = now.hour * 60 + now.minute

    try:
        all_exams = load_exams(spreadsheet_id, sheet_gid)
    except Exception as e:
        log.error("exam_alerts_tick: load_exams failed: %s", e)
        return

    users = [u for u in list_users_with_group() if u.get("exam_alerts_enabled")]

    for user in users:
        group = (user.get("group_code") or "").strip().upper()
        uid   = user["telegram_id"]

        group_exams = [
            e for e in all_exams
            if e["group"].strip().upper() == group and e["date"] is not None
        ]

        for exam in group_exams:
            exam_date: date = exam["date"]
            hm = _parse_hm(exam["time"])
            if hm is None:
                continue
            start_min = hm[0] * 60 + hm[1]

            # ─ алерт за день ─
            if exam_date == today + timedelta(days=1):
                if now.hour == _ALERT_DAY_HOUR and now.minute == _ALERT_DAY_MINUTE:
                    key = f"day|{group}|{exam_date}|{exam['time']}"
                    if not exam_alert_sent(uid, key):
                        try:
                            await bot.send_message(uid, _format_exam_alert(exam, "day"))
                            mark_exam_alert_sent(uid, key)
                        except Exception as e:
                            log.warning("exam day-alert failed uid=%s: %s", uid, e)

            # ─ алерт за 2 часа ─
            if exam_date == today:
                two_h_before = start_min - 120
                # окно 5 минут: [start-120, start-115)
                if two_h_before <= now_min < two_h_before + 5:
                    key = f"2h|{group}|{exam_date}|{exam['time']}"
                    if not exam_alert_sent(uid, key):
                        try:
                            await bot.send_message(uid, _format_exam_alert(exam, "2h"))
                            mark_exam_alert_sent(uid, key)
                        except Exception as e:
                            log.warning("exam 2h-alert failed uid=%s: %s", uid, e)
