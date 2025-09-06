# app/utils/week_parity.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def _to_date(dt: Optional[datetime | date], tz: Optional[str]) -> date:
    """
    Приводит вход к date. Если dt=None — берём сегодняшнюю дату с учётом tz.
    """
    if dt is None:
        if tz and ZoneInfo:
            now = datetime.now(ZoneInfo(tz))
        else:
            now = datetime.now()
        return now.date()
    if isinstance(dt, datetime):
        if dt.tzinfo is None and tz and ZoneInfo:
            dt = dt.replace(tzinfo=ZoneInfo(tz))
        return dt.date()
    return dt


def _academic_sep_year(d: date) -> int:
    """
    Возвращает «сентябрьский год» академического года для даты d.
    Если месяц >= 9 → берём текущий год, иначе — предыдущий.
    """
    return d.year if d.month >= 9 else d.year - 1


def _anchor_monday_for_year(sep_year: int) -> date:
    """
    Возвращает ПОНЕДЕЛЬНИК той недели, в которую попадает 1 сентября sep_year.
    Эта неделя считается Нечётной (первая).
    """
    sep1 = date(sep_year, 9, 1)
    # weekday(): Monday=0, Sunday=6
    return sep1 - timedelta(days=sep1.weekday())


def week_index(d: date) -> int:
    """
    0-based номер учебной недели относительно якоря (ПН недели с 1 сентября).
    Неделя с 1 сентября имеет индекс 0.
    """
    sep_year = _academic_sep_year(d)
    anchor_mon = _anchor_monday_for_year(sep_year)
    delta_days = (d - anchor_mon).days
    # Отрицательные даты дадут отрицательные индексы — это нормально для конца августа
    return delta_days // 7


def week_parity_for_date(dt: Optional[datetime | date] = None, tz: Optional[str] = None) -> str:
    """
    Возвращает «нечёт» или «чёт» для указанной даты (или «сегодня», если dt=None).
    Правило: неделя, содержащая 1 сентября, считается НЕЧЁТНОЙ. Далее чередование.
    """
    d = _to_date(dt, tz)
    idx = week_index(d)
    # Индекс 0 → нечёт, 1 → чёт, 2 → нечёт, ...
    return "нечёт" if (idx % 2 == 0) else "чёт"


def is_even_week(dt: Optional[datetime | date] = None, tz: Optional[str] = None) -> bool:
    """Удобный булев помощник: True, если неделя чётная."""
    return week_parity_for_date(dt, tz) == "чёт"


def is_odd_week(dt: Optional[datetime | date] = None, tz: Optional[str] = None) -> bool:
    """Удобный булев помощник: True, если неделя нечётная."""
    return week_parity_for_date(dt, tz) == "нечёт"


if __name__ == "__main__":
    # Пример использования: просто напечатает текущую нечёт/чёт.
    # Можно указать таймзону, например "Europe/Moscow" или "Europe/Berlin".
    tz = None  # замените при необходимости
    today = _to_date(None, tz)
    print(f"Сегодня: {today} — {week_parity_for_date(today, tz)} неделя (index={week_index(today)})")
