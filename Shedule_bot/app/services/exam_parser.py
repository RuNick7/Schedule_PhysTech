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

ROW_COURSE     = 0
ROW_HEADER     = 1   # строка «тип»/пустая — пропускаем
ROW_GROUP      = 2
ROW_DATA_START = 3

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

    r = ROW_DATA_START
    while r < max_rows:
        row_lec  = matrix[r]
        row_room = matrix[r + 1] if r + 1 < max_rows else []

        date_raw = _clean(row_lec[COL_DATE] if COL_DATE < len(row_lec) else "")
        time_raw = _clean(row_lec[COL_TIME] if COL_TIME < len(row_lec) else "")

        # пропускаем строки без времени или без даты
        if not _is_time_range(time_raw) or not date_raw:
            r += 2
            continue

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

        r += 2

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

    # expand merges так же, как для обычного расписания
    matrix = values
    from app.services.schedule_expand import _apply_merges_into
    _apply_merges_into(matrix, merges)

    return parse_exam_matrix(matrix)


def get_exams_for_group(
    group: str,
    spreadsheet_id: str,
    sheet_gid: int,
) -> List[Dict[str, Any]]:
    all_exams = load_exams(spreadsheet_id, sheet_gid)
    g = (group or "").strip().upper()
    return [e for e in all_exams if e["group"].strip().upper() == g]
