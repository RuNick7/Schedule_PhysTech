# app/services/gcal_mapper.py
from __future__ import annotations
from datetime import datetime
from typing import Dict, Tuple, Optional
from Shedule_bot.app.config import settings
from Shedule_bot.app.services.gcal_client import build_event_min
import re
import logging

log = logging.getLogger("gcal.mapper")

_TIME_RE = re.compile(r"^\s*(\d{1,2})[:.](\d{2})\s*[-–—]\s*(\d{1,2})[:.](\d{2})\s*$")

def _parse_time_range(time_str: str) -> tuple[str, str]:
    m = _TIME_RE.match(time_str or "")
    if not m:
        raise ValueError(f"Bad time range: {time_str!r}")
    h1, m1, h2, m2 = map(int, m.groups())
    return f"{h1:02d}:{m1:02d}:00", f"{h2:02d}:{m2:02d}:00"

def _rfc_date(dt: datetime) -> str:
    # YYYY-MM-DD
    return dt.strftime("%Y-%m-%d")

def _clean_subject(text: str) -> str:
    # text — это поле it['text'] из твоего парсера, там "Предмет, ФИО, ..." + аудитория отдельно
    # На неделе ты уже используешь _strip_teachers(...) внутри format_*,
    # но здесь держим простую эвристику: обрезаем по "— Преп." если ты так формируешь,
    # или оставляем как есть.
    subj = text.strip()
    return subj.replace("\n", " ")

def _build_sched_key(date_str: str, start_hms: str, lesson: Dict[str, str]) -> str:
    """
    Идемпотентный ключ события на конкретный слот:
    YYYYMMDDTHHMM-<group>-<subject_abbrev>
    """
    subj = _clean_subject(lesson.get("subject") or lesson.get("text") or "").lower()
    subj = "".join(ch for ch in subj if ch.isalnum())[:18] or "subj"
    group = (lesson.get("group") or "").lower()
    start_hm = start_hms[:5].replace(":", "")
    return f"{date_str.replace('-', '')}T{start_hm}-{group}-{subj}"

def lesson_to_event(
    user: Dict[str, str],
    lesson: Dict[str, str],
    date_dt: datetime,
) -> Tuple[Dict, str]:
    """
    На вход: объект lesson из твоего list_lessons_matrix + точная дата (день недели уже совпадает).
    Возвращает: (event_body, sched_key)
    """
    tz = user.get("timezone") or getattr(settings, "timezone", "Europe/Moscow")

    # Время начала/конца
    start_hms, end_hms = _parse_time_range(lesson["time"])
    date_str = _rfc_date(date_dt)
    start_iso = f"{date_str}T{start_hms}"
    end_iso   = f"{date_str}T{end_hms}"

    # Поля
    subject  = lesson.get("subject") or _clean_subject(lesson.get("text") or "")
    teacher  = lesson.get("teacher") or ""
    room     = lesson.get("room") or ""          # если твой парсер кладёт отдельно
    room_is_zoom = bool(lesson.get("room_is_zoom"))
    room_link    = lesson.get("room_link") or ""

    # Описание + локация
    description_parts = []
    if teacher:
        description_parts.append(f"Преподаватель: {teacher}")
    if room_is_zoom and room_link:
        description_parts.append(f"Ссылка: {room_link}")
    description = "\n".join(description_parts) if description_parts else ""

    location: Optional[str] = None
    if room_is_zoom:
        location = "Zoom"
    elif room:
        location = room

    # Ключ идемпотентности
    sched_key = _build_sched_key(date_str, start_hms, lesson)

    event = build_event_min(
        summary=subject or "Занятие",
        description=description,
        start_iso=start_iso,
        end_iso=end_iso,
        tz=tz,
        location=location,
        private_props={
            "sched_bot": "1",
            "sched_key": sched_key,
            "group": str(lesson.get("group") or ""),
        },
    )
    return event, sched_key
