from __future__ import annotations

import os
import re
from typing import Dict, List, Optional

from app.config import settings
from app.services.sheets_client import fetch_sheet_grid
from app.services.schedule_expand import expand_merged_matrix

# ---- Индексы (строки/столбцы) ----
ROW_COURSE = 0                 # 0-я строка: курс (1,2,3,4)
ROW_PARITY = 1                 # 1-я строка: чёт / нечёт
ROW_GROUP  = 2                 # 2-я строка: группа
ROW_DATA_START = 3             # с этой строки начинаются слоты (двухстрочные)

COL_DAY         = 0            # 0-й столбец: день недели
COL_PAIR_NO     = 1            # 1-й столбец: номер пары
COL_TIME        = 2            # 2-й столбец: время (HH:MM-HH:MM)
COL_FIRST_GROUP = 3            # дальше — столбцы групп

TIME_RE = re.compile(r"^\s*\d{1,2}:\d{2}\s*[-–—]\s*\d{1,2}:\d{2}\s*$", re.I)

# ---- Утилиты ----
def _clean(s: Optional[str]) -> str:
    return (s or "").strip()

def _has_text(val: Optional[str]) -> bool:
    if val is None:
        return False
    t = str(val).strip()
    if not t:
        return False
    t = t.strip("-–—").strip()
    return bool(t)

def _is_zoom_room(text: str) -> bool:
    t = _norm(text)
    return "zoom" in t or "зуум" in t  # на всякий случай

def _cell_text(val: Optional[str]) -> str:
    if not val:
        return ""
    txt = str(val).replace("\r\n", "\n").replace("\r", "\n")
    txt = re.sub(r"\n+", " / ", txt)   # переносы строк -> ' / '
    txt = re.sub(r"[ \t]+", " ", txt).strip()
    return txt

def _is_time_range(s: Optional[str]) -> bool:
    return bool(TIME_RE.match(_clean(s)))

# Чётность строк (жёсткая проверка по твоему правилу):
# индексация строк 1-based: лекция — чётная, аудитория — нечётная
def _is_lecture_row_index0(r0: int) -> bool:
    return ((r0 + 1) % 2) == 0  # чётный в 1-based

def _is_room_row_index0(r0: int) -> bool:
    return ((r0 + 1) % 2) == 1  # нечётный в 1-based

def _norm_compare(s: str) -> str:
    """
    Нормализация для сравнения текстов лекции и аудитории:
    - нижний регистр
    - схлопываем разделители / пробелы
    - убираем лишние знаки препинания по краям
    """
    s = s.lower()
    s = s.replace("\r", "\n").replace("\t", " ")
    s = re.sub(r"[\n/|]+", " ", s)
    s = re.sub(r"[ ]+", " ", s).strip(" .,-–—;:!?\u00A0").strip()
    return s

# ---- Основная логика ----
def _norm(s: str) -> str:
    # низкий регистр + ё→е + схлопывание пробелов
    s = (s or "").lower().replace("ё", "е")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _norm_parity_str(parity_raw: str) -> str:
    """
    Нормализация чётности столбца:
    — сначала ловим 'неч' → 'нечёт', затем 'чет' → 'чёт'.
    (важно: в 'нечет' есть подстрока 'чет', поэтому порядок проверок критичен)
    """
    p = _norm(parity_raw)
    if "неч" in p:
        return "нечёт"
    if "чет" in p:
        return "чёт"
    return p or ""  # если пусто — оставим пустым

def _is_special_subject(text: str) -> bool:
    """История/Английский — любые формы/сокращения."""
    t = _norm(text)
    return ("истор" in t) or ("англ" in t) or ("англий" in t)

def list_lessons_matrix(
    matrix: List[List[Optional[str]]],
    links_matrix: Optional[List[List[Optional[str]]]] = None,
) -> List[Dict[str, str]]:
    """
    Строит список пар из матрицы (после expand_merged_matrix).
    Новое:
      • поддержка links_matrix (та же размерность), чтобы вытащить room_link;
      • для аудитории Zoom ставим флаги room_is_zoom=True и room_link=URL.
    """
    out: List[Dict[str, str]] = []
    if not matrix or len(matrix) <= ROW_DATA_START:
        return out

    row_course = matrix[ROW_COURSE] if len(matrix) > ROW_COURSE else []
    row_parity = matrix[ROW_PARITY] if len(matrix) > ROW_PARITY else []
    row_group  = matrix[ROW_GROUP]  if len(matrix) > ROW_GROUP  else []

    max_rows = len(matrix)
    max_cols = max(len(r) for r in matrix)

    for r in range(ROW_DATA_START, max_rows, 2):  # строки лекций
        row_lecture = matrix[r]
        row_room    = matrix[r + 1] if r + 1 < max_rows else []
        row_room_links = links_matrix[r + 1] if (links_matrix and r + 1 < len(links_matrix)) else None

        day = _clean(row_lecture[COL_DAY] if COL_DAY < len(row_lecture) else "")
        time = _clean(row_lecture[COL_TIME] if COL_TIME < len(row_lecture) else "")
        if not day or not time:
            continue

        for c in range(COL_FIRST_GROUP, max_cols):
            group = _clean(row_group[c] if c < len(row_group) else "")
            if not group:
                continue

            course = _clean(row_course[c] if c < len(row_course) else "")
            parity_raw = _clean(row_parity[c] if c < len(row_parity) else "")
            parity_norm = _norm_parity_str(parity_raw)

            lecture = _clean(row_lecture[c] if c < len(row_lecture) else "")
            room    = _clean(row_room[c]    if c < len(row_room)    else "")

            if not lecture:
                continue

            # линк аудитории
            room_link = None
            if row_room_links and c < len(row_room_links):
                room_link = row_room_links[c]
            room_is_zoom = _is_zoom_room(room) or (room_link and "zoom" in room_link.lower())

            # исключения (как было) — но сохраняем zoom-ссылку
            special = False
            if not room or _norm(lecture) == _norm(room):
                if _is_special_subject(lecture):
                    special = True
                    room_display = "⚠️ см. прилож."
                else:
                    continue
            else:
                room_display = room

            text = f"{lecture} — {room_display}" if room_display else lecture

            out.append({
                "group": group,
                "day": day.strip().upper(),
                "time": time,
                "course": course,
                "parity": parity_norm,
                "text": text,
                "special": special,
                # новое:
                "room_is_zoom": bool(room_is_zoom),
                "room_link": room_link if room_is_zoom else None,
            })

    return out

def main() -> None:
    # ЧИТАЕМ ИЗ ENV (settings)
    sheet = fetch_sheet_grid(
        spreadsheet_id=settings.spreadsheet_id,
        sheet_gid=settings.sheet_gid,
        creds_path=settings.google_credentials,
    )
    matrix = expand_merged_matrix(sheet)
    lessons = list_lessons_matrix(matrix)

    if not lessons:
        print("Пары не найдены.")
        return

    for it in lessons:
        print(f"{it['group']} | {it['day']} | {it['time']} | {it['course']} | {it['parity']} | {it['text']}")

