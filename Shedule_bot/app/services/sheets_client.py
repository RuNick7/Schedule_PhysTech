# app/services/sheets_client.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from app.config import settings

FIELDS = (
    "sheets("
    "properties(sheetId,title,gridProperties(rowCount,columnCount)),"
    "merges,"
    "data(rowData(values(formattedValue,userEnteredValue,effectiveValue)))"
    ")"
)

ROOT_DIR = Path(__file__).resolve().parents[2]  # корень проекта (…/Shedule_bot)

def _resolve_creds_path(creds_path: Optional[str]) -> str:
    """
    Возвращает существующий абсолютный путь к JSON сервис-аккаунта.
    Пробуем: аргумент → settings.google_credentials → GOOGLE_APPLICATION_CREDENTIALS.
    Проверяем как абсолютный, так и относительный к ROOT_DIR.
    """
    candidates: list[Path] = []

    def add(p: Optional[str]):
        if not p:
            return
        pp = Path(p).expanduser()
        # как есть
        candidates.append(pp)
        # относительный к корню проекта
        if not pp.is_absolute():
            candidates.append((ROOT_DIR / pp))

    add(creds_path)
    add(settings.google_credentials)
    add(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

    tried: list[str] = []
    for path in candidates:
        # не используем strict, чтобы не падать на .resolve()
        abs_path = path if path.is_absolute() else (ROOT_DIR / path)
        abs_path = abs_path.expanduser()
        tried.append(str(abs_path))
        if abs_path.exists():
            return str(abs_path)

    raise FileNotFoundError(
        "Не найден файл JSON сервис-аккаунта.\n"
        "Проверь переменные .env/окружения и путь к ключу.\n"
        "Пробовал пути:\n  - " + "\n  - ".join(tried)
    )

def _get_service(creds_path: Optional[str] = None, readonly: bool = True):
    scopes = (["https://www.googleapis.com/auth/spreadsheets.readonly"]
              if readonly else
              ["https://www.googleapis.com/auth/spreadsheets"])
    path = _resolve_creds_path(creds_path)
    credentials = Credentials.from_service_account_file(path, scopes=scopes)
    return build("sheets", "v4", credentials=credentials)

def fetch_sheet_grid(
    spreadsheet_id: Optional[str] = None,
    sheet_gid: Optional[int] = None,
    creds_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Возвращает JSON одного листа (includeGridData=True).
    Все параметры по умолчанию берутся из settings.
    """
    spreadsheet_id = spreadsheet_id or settings.spreadsheet_id
    sheet_gid = sheet_gid if sheet_gid is not None else settings.sheet_gid

    service = _get_service(creds_path=creds_path, readonly=True)
    resp = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        includeGridData=True,
        fields=FIELDS,
    ).execute()

    for sh in resp.get("sheets", []):
        if sh["properties"]["sheetId"] == sheet_gid:
            return sh
    raise ValueError(f"Лист с sheetId/gid={sheet_gid} не найден в таблице {spreadsheet_id}.")

def fetch_sheet_values_and_links(
    *,
    spreadsheet_id: str,
    sheet_gid: int,
    creds_path: str,
) -> Tuple[List[List[Optional[str]]], List[List[Optional[str]]], List[dict]]:
    """
    Возвращает (values_matrix, links_matrix, merges) для нужного листа.
    links_matrix: URL или None для каждой ячейки.
    merges: список merge-диапазонов (как в Google Sheets API).
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    credentials = Credentials.from_service_account_file(creds_path, scopes=scopes)
    service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    doc = service.spreadsheets().get(spreadsheetId=spreadsheet_id, includeGridData=True).execute()

    values: List[List[Optional[str]]] = []
    links:  List[List[Optional[str]]] = []
    merges: List[dict] = []

    for sh in doc.get("sheets", []):
        if sh.get("properties", {}).get("sheetId") != sheet_gid:
            continue

        merges = sh.get("merges", []) or []

        grid = sh.get("data", [])
        if not grid:
            break
        row_data = grid[0].get("rowData", []) or []
        for row in row_data:
            row_vals: List[Optional[str]] = []
            row_links: List[Optional[str]] = []
            for cell in (row.get("values") or []):
                # значение
                v = cell.get("formattedValue")
                if v is None:
                    eff = cell.get("effectiveValue") or {}
                    v = eff.get("stringValue") or eff.get("numberValue") or eff.get("boolValue")
                row_vals.append(str(v) if v is not None else None)
                # ссылка
                url = cell.get("hyperlink")
                if not url:
                    for run in (cell.get("textFormatRuns") or []):
                        link = ((run.get("format") or {}).get("link") or {}).get("uri")
                        if link:
                            url = link; break
                row_links.append(url)
            values.append(row_vals)
            links.append(row_links)
        break

    return values, links, merges