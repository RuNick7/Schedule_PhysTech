"""
Парсер расписания экзаменов из Google Sheets.

Структура листа аналогична обычному расписанию:
  Строка 0: курс (1, 2, 3 …)
  Строка 1: тип / пусто (игнорируем, чётности нет)
  Строка 2: название группы
  Строки 3+: пары по две строки подряд:
    нечётная (0-based лекция): дата | № | время | предмет(ы)…
    чётная   (0-based аудитория): пусто | пусто | пусто | аудитория(и)…

  Дата в колонке 0 — формат «15.06», «15.06.26», «15.06.2026»,
  либо русское слово (игнорируем).
"""
from __future__ import annotations

import re
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from app.services.sheets_client import fetch_sheet_values_and_links
from app.services.schedule_expand import expand_merged_matrix
from app.config import settings

log = logging.getLogger("exam_parser")

ROW_GROUP      = 1   # группы в строке 1 (0-based)
ROW_DATA_START = 2   # данные начинаются со строки 2

COL_DATE         = 0
COL_TIME         = 2
COL_FIRST_GROUP  = 3

_DATE_RE  = re.compile(r"(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?")
_TIME_RE  = re.compile(r"^\s*\d{1,2}:\d{2}\s*[-–—]\s*\d{1,2}:\d{2}\s*$")


def _clean(s: Optional[str]) -> str:
    return (s or "").strip()


def _parse_date(raw: str, default_year: int = 0) -> Optional[date]:
    m = _DATE_RE.search(raw or "")
    if not m:
        return None
    day, month = int(m.group(1)), int(m.group(2))
    yr_raw = m.group(3)
    if yr_raw:
        year = int(yr_raw) if len(yr_raw) == 4 else 2000 + int(yr_raw)
    else:
        year = default_year or datetime.now().year
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _is_time_range(s: Optional[str]) -> bool:
    return bool(_TIME_RE.match(_clean(s)))


def parse_exam_matrix(
    matrix: List[List[Optional[str]]],
) -> List[Dict[str, Any]]:
    """
    Возвращает список экзаменов:
      {group, date (date|None), date_str, time, subject, room}
    """
    out: List[Dict[str, Any]] = []
    if not matrix or len(matrix) <= ROW_DATA_START:
        return out

    row_group = matrix[ROW_GROUP] if len(matrix) > ROW_GROUP else []

    max_rows = len(matrix)
    max_cols = max((len(r) for r in matrix), default=0)

    default_year = datetime.now().year

    # Определяем шаг: если строки идут парами (предмет + аудитория) или по одной.
    # Смотрим, есть ли в строке ROW_DATA_START дата+время — если да, шагаем по 1
    # или по 2 в зависимости от того, есть ли у следующей строки дата.
    r = ROW_DATA_START
    while r < max_rows:
        row_lec = matrix[r]

        date_raw = _clean(row_lec[COL_DATE] if COL_DATE < len(row_lec) else "")
        time_raw = _clean(row_lec[COL_TIME] if COL_TIME < len(row_lec) else "")

        if not date_raw or not _is_time_range(time_raw):
            r += 1
            continue

        # Смотрим следующую строку — аудитория или уже следующая дата?
        next_row = matrix[r + 1] if r + 1 < max_rows else []
        next_date = _clean(next_row[COL_DATE] if COL_DATE < len(next_row) else "")
        next_time = _clean(next_row[COL_TIME] if COL_TIME < len(next_row) else "")
        has_room_row = next_row and not next_date and not _is_time_range(next_time)
        row_room = next_row if has_room_row else []

        exam_date = _parse_date(date_raw, default_year)

        for c in range(COL_FIRST_GROUP, max_cols):
            group = _clean(row_group[c] if c < len(row_group) else "")
            if not group:
                continue

            subject = _clean(row_lec[c] if c < len(row_lec) else "")
            room    = _clean(row_room[c] if c < len(row_room) else "")

            if not subject:
                continue

            out.append({
                "group":    group,
                "date":     exam_date,
                "date_str": date_raw,
                "time":     time_raw,
                "subject":  subject,
                "room":     room,
            })

        r += 2 if has_room_row else 1

    return out


def load_exams(
    spreadsheet_id: str,
    sheet_gid: int,
    creds_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Загружает и парсит расписание экзаменов из Google Sheets.
    Возвращает список {group, date, date_str, time, subject, room}.
    """
    creds = creds_path or settings.google_credentials
    try:
        values, links, merges = fetch_sheet_values_and_links(
            spreadsheet_id=spreadsheet_id,
            sheet_gid=sheet_gid,
            creds_path=creds,
        )
    except Exception as e:
        log.error("exam_parser: fetch failed: %s", e)
        return []

    from app.services.schedule_expand import expand_merged_matrix
    matrix = expand_merged_matrix(values, merges=merges)

    return parse_exam_matrix(matrix)


def get_exams_for_group(
    group: str,
    spreadsheet_id: str,
    sheet_gid: int,
) -> List[Dict[str, Any]]:
    all_exams = load_exams(spreadsheet_id, sheet_gid)
    g = (group or "").strip().upper()
    return [e for e in all_exams if e["group"].strip().upper() == g]
