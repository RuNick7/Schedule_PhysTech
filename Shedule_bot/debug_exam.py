"""Запуск: python3 debug_exam.py"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from app.services.db import get_bot_setting
from app.services.sheets_client import fetch_sheet_values_and_links
from app.services.schedule_expand import expand_merged_matrix
from app.services.exam_parser import parse_exam_matrix, _fetch_public_csv
from app.config import settings

sid = get_bot_setting("exam_spreadsheet_id")
gid = int(get_bot_setting("exam_sheet_gid") or 0)
print(f"Spreadsheet ID : {sid}")
print(f"Sheet GID      : {gid}")
print()

# Пробуем service account
try:
    vals, links, merges = fetch_sheet_values_and_links(
        spreadsheet_id=sid, sheet_gid=gid, creds_path=settings.google_credentials
    )
    matrix = expand_merged_matrix(vals, merges=merges)
    print("Загружено через: service account")
except Exception as e:
    print(f"Service account failed: {e}")
    print("Загружаем через публичный CSV...")
    matrix = _fetch_public_csv(sid, gid)

print(f"Строк в матрице: {len(matrix)}")
print()
print("=== Первые 5 строк ===")
for i, row in enumerate(matrix[:5]):
    print(f"  [{i}]: {row[:8]}")

print()
exams = parse_exam_matrix(matrix)
groups = sorted({e['group'] for e in exams})
print(f"=== Найдено экзаменов: {len(exams)} ===")
print(f"Группы: {groups}")
print()
for e in exams[:15]:
    print(f"  {e['group']:15} | {e['date_str']:6} | {e['time'] or '—':12} | {e['subject'][:40]:40} | {e['room']}")
