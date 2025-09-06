# app/services/groups.py
from __future__ import annotations
from typing import List, Optional

from app.config import settings
from app.services.sheets_client import fetch_sheet_grid
from app.services.schedule_expand import expand_merged_matrix

ROW_COURSE = 0
ROW_GROUP  = 2
COL_FIRST_GROUP = 3

def _clean(s: Optional[str]) -> str:
    return (s or "").strip()

def list_groups_for_course(course: int) -> List[str]:
    # ЧИТАЕМ ИЗ ENV (settings)
    sheet = fetch_sheet_grid(
        spreadsheet_id=settings.spreadsheet_id,
        sheet_gid=settings.sheet_gid,
        creds_path=settings.google_credentials,
    )
    m = expand_merged_matrix(sheet)
    if not m or len(m) < 3:
        return []

    row_course = m[ROW_COURSE]
    row_group  = m[ROW_GROUP]
    groups: List[str] = []
    target = f"{course}"
    for c in range(COL_FIRST_GROUP, max(len(row_course), len(row_group))):
        c_title = _clean(row_course[c] if c < len(row_course) else "")
        g_text  = _clean(row_group[c]  if c < len(row_group)  else "")
        if not g_text:
            continue
        if target in c_title:
            groups.append(g_text)

    seen, out = set(), []
    for g in groups:
        if g not in seen:
            seen.add(g)
            out.append(g)
    out.sort(key=lambda s: (len(s), s))
    return out[:96]
