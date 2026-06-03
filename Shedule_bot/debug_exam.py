"""Запуск: python3 debug_exam.py"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from app.services.db import get_bot_setting
from app.services.sheets_client import fetch_sheet_values_and_links
from app.services.schedule_expand import _apply_merges_into
from app.services.exam_parser import parse_exam_matrix
from app.config import settings

sid = get_bot_setting("exam_spreadsheet_id")
gid = int(get_bot_setting("exam_sheet_gid") or 0)
print(f"Spreadsheet ID : {sid}")
print(f"Sheet GID      : {gid}")
print()

vals, links, merges = fetch_sheet_values_and_links(
    spreadsheet_id=sid, sheet_gid=gid, creds_path=settings.google_credentials
)
_apply_merges_into(vals, merges)

print("=== Первые 5 строк после раскрытия мержей ===")
for i, row in enumerate(vals[:5]):
    print(f"  Строка {i}: {row[:10]}")  # первые 10 колонок

print()
exams = parse_exam_matrix(vals)
print(f"=== Найдено экзаменов: {len(exams)} ===")
groups = sorted({e['group'] for e in exams})
print(f"Группы: {groups}")
print()
for e in exams[:10]:
    print(e)
