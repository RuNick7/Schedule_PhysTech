# app/services/extend_merges.py
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Константы из окружения
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1heK_XfQjFycJY7yYjaYefjYcDbZ5_TtIkNTMyKQG1ek")
SHEET_GID = int(os.getenv("SHEET_GID", "0"))
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS", "../../google-credentials.json")


def _service(readonly: bool = False):
    scopes = (
        ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        if readonly
        else ["https://www.googleapis.com/auth/spreadsheets"]
    )
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS, scopes=scopes)
    return build("sheets", "v4", credentials=creds)


def _fetch_sheet_grid(spreadsheet_id: str, sheet_gid: int) -> Dict[str, Any]:
    FIELDS = (
        "sheets("
        "properties(sheetId,title,gridProperties(rowCount,columnCount)),"
        "merges,"
        "data(rowData(values(formattedValue,userEnteredValue,effectiveValue)))"
        ")"
    )
    s = _service(readonly=True)
    resp = s.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        includeGridData=True,
        fields=FIELDS,
    ).execute()
    for sh in resp.get("sheets", []):
        if sh["properties"]["sheetId"] == sheet_gid:
            return sh
    raise ValueError(f"Лист с sheetId/gid={sheet_gid} не найден.")


def _grid_to_matrix(sheet: Dict[str, Any]) -> List[List[Optional[str]]]:
    data_blocks = sheet.get("data", [])
    if not data_blocks:
        return []
    rows = data_blocks[0].get("rowData", []) or []
    max_cols = 0
    for r in rows:
        vals = r.get("values", []) or []
        max_cols = max(max_cols, len(vals))
    matrix: List[List[Optional[str]]] = []
    for r in rows:
        vals = r.get("values", []) or []
        row: List[Optional[str]] = []
        for c in range(max_cols):
            cell = vals[c] if c < len(vals) else {}
            v = cell.get("formattedValue")
            row.append(v if v is not None else None)
        matrix.append(row)
    return matrix


def _apply_merges_fill(matrix: List[List[Optional[str]]], merges: List[Dict[str, int]]) -> None:
    for mg in merges or []:
        sr, er = mg["startRowIndex"], mg["endRowIndex"]
        sc, ec = mg["startColumnIndex"], mg["endColumnIndex"]
        base = None
        if sr < len(matrix) and sc < len(matrix[sr]):
            base = matrix[sr][sc]
        for r in range(sr, er):
            if r >= len(matrix):
                continue
            for c in range(sc, ec):
                if c >= len(matrix[r]):
                    continue
                matrix[r][c] = base


def _col_to_a1(n: int) -> str:
    # 1 -> A, 2 -> B, ..., 27 -> AA
    res = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res.append(chr(65 + rem))
    return "".join(reversed(res))


def _resize_sheet(service, spreadsheet_id: str, sheet_id: int, rows: int, cols: int) -> None:
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {"rowCount": max(1, rows), "columnCount": max(1, cols)},
                        },
                        "fields": "gridProperties(rowCount,columnCount)",
                    }
                }
            ]
        },
    ).execute()


def _unmerge_all(service, spreadsheet_id: str, sheet_id: int, merges: List[Dict[str, int]]) -> None:
    if not merges:
        return
    requests = []
    for mg in merges:
        requests.append(
            {
                "unmergeCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": mg["startRowIndex"],
                        "endRowIndex": mg["endRowIndex"],
                        "startColumnIndex": mg["startColumnIndex"],
                        "endColumnIndex": mg["endColumnIndex"],
                    }
                }
            }
        )
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()


def _write_matrix(
    service,
    spreadsheet_id: str,
    sheet_title: str,
    matrix: List[List[Optional[str]]],
) -> None:
    values = [[("" if v is None else str(v)) for v in row] for row in matrix]
    rows = len(values)
    cols = max((len(r) for r in values), default=1)
    end_a1 = f"{_col_to_a1(cols)}{rows}"
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_title}'!A1:{end_a1}",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


def _create_target_sheet(service, spreadsheet_id: str, title: str) -> int:
    resp = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
    ).execute()
    return resp["replies"][0]["addSheet"]["properties"]["sheetId"]


def expand_merges(
    spreadsheet_id: str = SPREADSHEET_ID,
    sheet_gid: int = SHEET_GID,
    inplace: bool = False,
    target_title: Optional[str] = None,
) -> str:
    """
    Разъединяет merged-ячейки и дублирует значения.
    - Если inplace=True: правит исходный лист (unmerge + перезапись значений).
    - Если inplace=False (по умолчанию): создаёт новый лист Expanded_<title> и пишет туда.
    Возвращает имя листа, куда записан результат.
    """
    sheet = _fetch_sheet_grid(spreadsheet_id, sheet_gid)
    props = sheet["properties"]
    src_title = props["title"]
    src_sheet_id = props["sheetId"]
    merges = sheet.get("merges", []) or []

    matrix = _grid_to_matrix(sheet)
    _apply_merges_fill(matrix, merges)

    svc = _service(readonly=False)

    if inplace:
        # 1) unmerge
        _unmerge_all(svc, spreadsheet_id, src_sheet_id, merges)
        # 2) поджать/расширить размеры под матрицу
        _resize_sheet(svc, spreadsheet_id, src_sheet_id, len(matrix), max((len(r) for r in matrix), default=1))
        # 3) записать значения
        _write_matrix(svc, spreadsheet_id, src_title, matrix)
        return src_title

    # В новый лист
    out_title = target_title or f"Expanded_{src_title}"
    out_sheet_id = _create_target_sheet(svc, spreadsheet_id, out_title)
    _resize_sheet(svc, spreadsheet_id, out_sheet_id, len(matrix), max((len(r) for r in matrix), default=1))
    _write_matrix(svc, spreadsheet_id, out_title, matrix)
    return out_title


if __name__ == "__main__":
    # Простое использование из CLI:
    #   SPREADSHEET_ID=... SHEET_GID=0 GOOGLE_CREDENTIALS=google-credentials.json python -m app.services.extend_merges
    # По умолчанию пишет в новый лист Expanded_<Название>.
    INPLACE = os.getenv("INPLACE", "false").lower() in {"1", "true", "yes"}
    TARGET = os.getenv("TARGET_TITLE")  # можно задать имя результирующего листа
    try:
        out = expand_merges(
            spreadsheet_id=SPREADSHEET_ID,
            sheet_gid=SHEET_GID,
            inplace=INPLACE,
            target_title=TARGET,
        )
        print(f"Готово. Результат записан на лист: {out}")
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
