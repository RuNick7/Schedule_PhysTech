# app/utils/subjects_alert.py
from __future__ import annotations
from typing import Optional, List
import re

# Индексы как в нашем парсере
ROW_COURSE = 0
ROW_PARITY = 1
ROW_GROUP  = 2
ROW_DATA_START = 3

COL_DAY   = 0
COL_PAIR  = 1
COL_TIME  = 2
COL_FIRST_GROUP = 3

def _clean(s: Optional[str]) -> str:
    return (s or "").strip()

def _norm(s: str) -> str:
    s = (s or "").lower().replace("ё", "е")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _norm_parity(p: str) -> str:
    p = _norm(p)
    if "неч" in p:
        return "нечёт"
    if "чет" in p or "чёт" in p:
        return "чёт"
    return p

TRIGGERS = {
    "История": ("истор",),
    "Английский": ("англ", "англий"),
}

def _group_cols_for(matrix: List[List[Optional[str]]], group_code: str, want_parity: Optional[str]) -> List[int]:
    """Найдём все столбцы этой группы (на всякий случай поддержим дубли), с фильтром по чётности если задана."""
    cols: List[int] = []
    row_course = matrix[ROW_COURSE] if len(matrix) > ROW_COURSE else []
    row_parity = matrix[ROW_PARITY] if len(matrix) > ROW_PARITY else []
    row_group  = matrix[ROW_GROUP]  if len(matrix) > ROW_GROUP  else []
    want_parity = _norm_parity(want_parity or "")

    max_cols = max(len(r) for r in matrix)
    for c in range(COL_FIRST_GROUP, max_cols):
        g = _clean(row_group[c] if c < len(row_group) else "")
        if g != group_code:
            continue
        if want_parity:
            p = _norm_parity(_clean(row_parity[c] if c < len(row_parity) else ""))
            if p != want_parity:
                continue
        cols.append(c)
    return cols

def detect_special_subjects_in_matrix(
    matrix: List[List[Optional[str]]],
    *,
    group_code: str,
    parity: Optional[str] = None,
    day_name_upper: Optional[str] = None,
) -> List[str]:
    """
    Сканирует СЫРУЮ матрицу (после expand_merged_matrix) и определяет,
    встречаются ли предметы из TRIGGERS в колонке нужной группы, даже если пара была отброшена.
    Фильтры: чётность столбца (parity), название дня (day_name_upper).
    """
    found = set()
    if not matrix or len(matrix) <= ROW_DATA_START:
        return []

    cols = _group_cols_for(matrix, group_code, parity)
    if not cols:
        return []

    max_rows = len(matrix)
    for r in range(ROW_DATA_START, max_rows, 2):  # только строки ЛЕКЦИЙ (чётные в 1-based)
        row = matrix[r]
        # Фильтр по дню, если задан
        if day_name_upper:
            day_cell = _clean(row[COL_DAY] if COL_DAY < len(row) else "").upper()
            if day_cell != day_name_upper:
                continue

        for c in cols:
            lect = _clean(row[c] if c < len(row) else "")
            if not lect:
                continue
            subj_norm = _norm(lect)  # здесь учителя не удаляем специально — ищем по подстрокам
            for name, keys in TRIGGERS.items():
                if any(k in subj_norm for k in keys):
                    found.add(name)

    order = ["История", "Английский"]
    return [n for n in order if n in found]
