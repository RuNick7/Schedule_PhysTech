# app/utils/dt.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

# Русские названия дней недели (Пн=0 … Вс=6)
DAY_NAMES_RU_UPPER = ["ПОНЕДЕЛЬНИК", "ВТОРНИК", "СРЕДА", "ЧЕТВЕРГ", "ПЯТНИЦА", "СУББОТА", "ВОСКРЕСЕНЬЕ"]
DAY_NAMES_RU_TITLE = [s.title() for s in DAY_NAMES_RU_UPPER]


def get_tz(tz_name: Optional[str]) -> Optional[ZoneInfo]:
    """Возвращает объект таймзоны или None (если ZoneInfo недоступен/не задано имя)."""
    if not tz_name or not ZoneInfo:
        return None
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return None


def now_tz(tz_name: Optional[str]) -> datetime:
    """Текущее время с учётом IANA-таймзоны (или системной, если таймзона не найдена)."""
    tz = get_tz(tz_name)
    return datetime.now(tz) if tz else datetime.now()


def today_tz(tz_name: Optional[str]) -> date:
    """Текущая дата в указанной таймзоне (или системной)."""
    return now_tz(tz_name).date()


def tomorrow_tz(tz_name: Optional[str]) -> date:
    """Завтрашняя дата в указанной таймзоне (или системной)."""
    return today_tz(tz_name) + timedelta(days=1)


def ensure_tz(dt: datetime, tz_name: Optional[str]) -> datetime:
    """
    Если datetime naive — навешиваем tz; если уже с tz — оставляем.
    """
    if dt.tzinfo is None:
        tz = get_tz(tz_name)
        return dt.replace(tzinfo=tz) if tz else dt
    return dt


def monday_of_week(d: date) -> date:
    """Понедельник той недели, к которой относится дата d (Monday=0)."""
    return d - timedelta(days=d.weekday())


def sunday_of_week(d: date) -> date:
    """Воскресенье той недели, к которой относится дата d (Monday=0)."""
    return monday_of_week(d) + timedelta(days=6)


def add_days(d: date, days: int) -> date:
    """Смещает дату на days дней (может быть отрицательным)."""
    return d + timedelta(days=days)


def day_name_ru(d: date | datetime, *, upper: bool = True) -> str:
    """Название дня недели на русском: ПОНЕДЕЛЬНИК/Понедельник и т.д."""
    wd = (d.weekday() if isinstance(d, (date, datetime)) else int(d)) % 7
    return DAY_NAMES_RU_UPPER[wd] if upper else DAY_NAMES_RU_TITLE[wd]


__all__ = [
    "now_tz",
    "today_tz",
    "tomorrow_tz",
    "ensure_tz",
    "monday_of_week",
    "sunday_of_week",
    "add_days",
    "day_name_ru",
    "DAY_NAMES_RU_UPPER",
    "DAY_NAMES_RU_TITLE",
]
