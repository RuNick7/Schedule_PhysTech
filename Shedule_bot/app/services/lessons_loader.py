from __future__ import annotations
from app.services.sheets_client import fetch_sheet_values_and_links
from app.services.schedule_expand import expand_merged_matrix
from app.services.schedule_list import list_lessons_matrix
from app.config import settings

async def load_lessons_for_user_group(user: dict):
    vals, links, merges = fetch_sheet_values_and_links(
        spreadsheet_id=settings.spreadsheet_id,
        sheet_gid=settings.sheet_gid,
        creds_path=settings.google_credentials,
    )
    mtx_vals  = expand_merged_matrix(vals, merges=merges)
    mtx_links = expand_merged_matrix(links, merges=merges)
    all_lessons = list_lessons_matrix(mtx_vals, mtx_links)
    return [it for it in all_lessons if it["group"] == user["group_code"]]