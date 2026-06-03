"""
Парсер расписания экзаменов из Google Sheets.

Реальная структура листа (Расписание сессии весна 2026):
  Строка 0: курс (1 курс, 2 курс …) — игнорируем
  Строка 1: группы (3142, 3143, 3144-3145 …) — COL_FIRST_GROUP=2
  Строка 2+: данные строки (по одной на каждую дату):
    col 0 — день недели (вт, ср, …)
    col 1 — дата (02.06, 03.06 …)
    col 2+ — ячейка группы: «Предмет\nтип\nвремя, ауд. …» (всё в одной ячейке)

Ячейка может содержать:
  «История\nэкзамен»                              → subject=История экзамен, time=None, room=''
  «Физика\nконтрольная\n10:00, ауд. 2432»         → subject=Физика контрольная, time=10:00, room=ауд. 2432
  «Мат. анализ\nэкзамен\n10:00-13:00\nауд. 1234» → subject=…, time=10:00-13:00, room=ауд. 1234
"""
from __future__ import annotations

import csv
import io
import re
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import requests

from app.services.sheets_client import fetch_sheet_values_and_links
from app.config import settings

log = logging.getLogger("exam_parser")

ROW_GROUP      = 1   # группы в строке 1 (0-based)
ROW_DATA_START = 2   # данные начинаются со строки 2

COL_DAY         = 0
COL_DATE        = 1
COL_FIRST_GROUP = 2

_DATE_RE     = re.compile(r"(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?")
_TIME_RE     = re.compile(r"\d{1,2}:\d{2}(?:\s*[-–—]\s*\d{1,2}:\d{2})?")
_ROOM_RE     = re.compile(r"(?:ауд\.?\s*|корп\.?\s*|к\.?\s*)[\w,\s]+", re.I)


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


def _parse_cell(cell_text: str) -> Dict[str, str]:
    """
    Разбирает содержимое ячейки экзамена на поля subject / time / room.

    Ячейка может использовать \n или пробел как разделитель строк
    (CSV-экспорт превращает переносы в \n, сервисный аккаунт — тоже).
    """
    raw = _clean(cell_text)
    if not raw:
        return {"subject": "", "time": "", "room": ""}

    lines = [l.strip() for l in re.split(r"\n|\r", raw) if l.strip()]

    time_str = ""
    room_str = ""
    subject_lines = []

    for line in lines:
        # ищем время в строке: «10:00» или «10:00-12:00» или «10:00, ауд. 123»
        tm = _TIME_RE.search(line)
        if tm and not time_str:
            time_str = tm.group(0).strip()
            # остаток строки после времени — возможно аудитория
            rest = line[tm.end():].strip().lstrip(",").strip()
            if rest and not room_str:
                room_str = rest
            continue

        # ищем аудиторию
        rm = _ROOM_RE.search(line)
        if rm and not room_str:
            room_str = line.strip()
            continue

        subject_lines.append(line)

    subject = " ".join(subject_lines).strip()
    return {"subject": subject or raw, "time": time_str, "room": room_str}


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
    max_cols  = max((len(r) for r in matrix), default=0)
    default_year = datetime.now().year

    for row in matrix[ROW_DATA_START:]:
        date_raw = _clean(row[COL_DATE] if COL_DATE < len(row) else "")
        if not date_raw or not _DATE_RE.search(date_raw):
            continue

        exam_date = _parse_date(date_raw, default_year)

        for c in range(COL_FIRST_GROUP, max_cols):
            group = _clean(row_group[c] if c < len(row_group) else "")
            if not group:
                continue

            cell = _clean(row[c] if c < len(row) else "")
            if not cell:
                continue

            parsed = _parse_cell(cell)
            if not parsed["subject"]:
                continue

            out.append({
                "group":    group,
                "date":     exam_date,
                "date_str": date_raw,
                "time":     parsed["time"],
                "subject":  parsed["subject"],
                "room":     parsed["room"],
            })

    return out


def _fetch_public_csv(spreadsheet_id: str, sheet_gid: int) -> List[List[Optional[str]]]:
    """Загружает публичный лист как CSV без авторизации."""
    url = (
        f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        f"/export?format=csv&gid={sheet_gid}"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    reader = csv.reader(io.StringIO(resp.text))
    return [[cell if cell != "" else None for cell in row] for row in reader]


def load_exams(
    spreadsheet_id: str,
    sheet_gid: int,
    creds_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Загружает и парсит расписание экзаменов.
    Сначала пробует service account (поддерживает мержи),
    при ошибке доступа — публичный CSV-экспорт.
    """
    from app.services.schedule_expand import expand_merged_matrix

    creds = creds_path or settings.google_credentials
    try:
        values, _links, merges = fetch_sheet_values_and_links(
            spreadsheet_id=spreadsheet_id,
            sheet_gid=sheet_gid,
            creds_path=creds,
        )
        matrix = expand_merged_matrix(values, merges=merges)
        log.debug("exam_parser: loaded via service account, rows=%d", len(matrix))
    except Exception as e:
        log.warning("exam_parser: service account failed (%s), trying public CSV", e)
        try:
            matrix = _fetch_public_csv(spreadsheet_id, sheet_gid)
            log.debug("exam_parser: loaded via public CSV, rows=%d", len(matrix))
        except Exception as e2:
            log.error("exam_parser: public CSV also failed: %s", e2)
            return []

    return parse_exam_matrix(matrix)


def get_exams_for_group(
    group: str,
    spreadsheet_id: str,
    sheet_gid: int,
) -> List[Dict[str, Any]]:
    all_exams = load_exams(spreadsheet_id, sheet_gid)
    g = (group or "").strip().upper()
    return [e for e in all_exams if e["group"].strip().upper() == g]
