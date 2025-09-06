# ПОЛНОСТЬЮ ЗАМЕНИТЕ файл/функции на эти (или добавьте отсутствующие)

from __future__ import annotations
from typing import List, Optional, Dict, Any

def _apply_merges_into(matrix: List[List[Optional[str]]], merges: List[Dict[str, Any]]) -> None:
    """Копирует значение из верх-левого угла каждого merge-диапазона во все его ячейки."""
    if not matrix or not merges:
        return

    # гарантируем размеры перед обращением по индексам
    max_row_len = max((len(r) for r in matrix), default=0)

    def _ensure_size(rows: int, cols: int):
        nonlocal max_row_len
        # добить строки
        while len(matrix) < rows:
            matrix.append([])
        # добить колонки в каждой строке
        for r in range(len(matrix)):
            need = cols - len(matrix[r])
            if need > 0:
                matrix[r].extend([None] * need)
        max_row_len = max(max_row_len, cols)

    for m in merges:
        sr = int(m.get("startRowIndex", 0) or 0)
        er = int(m.get("endRowIndex", sr + 1) or (sr + 1))
        sc = int(m.get("startColumnIndex", 0) or 0)
        ec = int(m.get("endColumnIndex", sc + 1) or (sc + 1))

        if er <= sr or ec <= sc:
            continue

        _ensure_size(er, ec)

        top = matrix[sr][sc] if sr < len(matrix) and sc < len(matrix[sr]) else None
        for r in range(sr, er):
            for c in range(sc, ec):
                matrix[r][c] = top

def expand_merged_matrix(sheet_or_matrix, merges: Optional[List[Dict[str, Any]]] = None) -> List[List[Optional[str]]]:
    """
    Универсальная функция:
    • Если передан объект листа (dict с ключами 'data', 'merges') — соберём матрицу и развернём мерджи.
    • Если передана 2D-матрица (list[list]) — вернём её копию и (если merges передан) развернём по merges.
    """
    # Вариант 1: уже готовая матрица (list[list])
    if isinstance(sheet_or_matrix, list):
        matrix = [list(row) for row in sheet_or_matrix]  # глубокая копия уровнем строк
        if merges:
            _apply_merges_into(matrix, merges)
        return matrix

    # Вариант 2: объект листа Google Sheets
    if isinstance(sheet_or_matrix, dict):
        data_blocks = sheet_or_matrix.get("data", []) or []
        row_data = data_blocks[0].get("rowData", []) if data_blocks else []
        matrix: List[List[Optional[str]]] = []
        for row in row_data:
            vals_row: List[Optional[str]] = []
            for cell in (row.get("values") or []):
                v = cell.get("formattedValue")
                if v is None:
                    eff = cell.get("effectiveValue") or {}
                    v = eff.get("stringValue") or eff.get("numberValue") or eff.get("boolValue")
                vals_row.append(str(v) if v is not None else None)
            matrix.append(vals_row)

        merges_local = sheet_or_matrix.get("merges", []) or []
        if merges_local:
            _apply_merges_into(matrix, merges_local)
        return matrix

    raise TypeError("expand_merged_matrix: expected dict (sheet) or List[List], got %r" % type(sheet_or_matrix))
