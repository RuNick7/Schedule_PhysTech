from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List

from bs4 import BeautifulSoup

_DAY_NAMES = {
    "понедельник": "ПОНЕДЕЛЬНИК",
    "вторник": "ВТОРНИК",
    "среда": "СРЕДА",
    "четверг": "ЧЕТВЕРГ",
    "пятница": "ПЯТНИЦА",
    "суббота": "СУББОТА",
    "воскресенье": "ВОСКРЕСЕНЬЕ",
}

_TIME_RE = re.compile(r"(\d{1,2}:\d{2})\s*[-–—]\s*(\d{1,2}:\d{2})")


def parse_schedule_html(page_html: str) -> List[Dict[str, Any]]:
    """
    Parse the ISU potok schedule page HTML into structured lesson dicts.
    Returns list of: {day, time, subject, room, teacher, lesson_type, parity}
    """
    soup = BeautifulSoup(page_html, "lxml")
    lessons: List[Dict[str, Any]] = []

    table = soup.find("table", class_="table-bordered")
    if not table:
        table = soup.find("table", class_="table")
    if not table:
        for t in soup.find_all("table"):
            if t.find("th") or t.find("td"):
                table = t
                break
    if not table:
        return lessons

    rows = table.find_all("tr")
    current_day = ""

    for tr in rows:
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue

        texts = [_clean_cell(c) for c in cells]

        day_candidate = _detect_day(texts)
        if day_candidate:
            current_day = day_candidate

        if not current_day:
            continue

        time_match = None
        time_idx = -1
        for i, t in enumerate(texts):
            m = _TIME_RE.search(t)
            if m:
                time_match = f"{m.group(1)}-{m.group(2)}"
                time_idx = i
                break

        if not time_match:
            continue

        remaining = [t for idx, t in enumerate(texts) if idx != time_idx and t]
        remaining = [t for t in remaining if not _is_day_name(t)]

        subject = ""
        room = ""
        teacher = ""
        lesson_type = ""
        parity = ""

        for part in remaining:
            p_lower = part.lower().replace("ё", "е")
            if not subject:
                subject = part
            elif _looks_like_room(part):
                room = part
            elif _looks_like_teacher(part):
                teacher = _append(teacher, part)
            elif _looks_like_type(p_lower):
                lesson_type = part
            elif "чет" in p_lower or "неч" in p_lower:
                parity = "нечёт" if "неч" in p_lower else "чёт"
            elif not room:
                room = part
            else:
                teacher = _append(teacher, part)

        if subject:
            lessons.append({
                "day": current_day,
                "time": time_match,
                "subject": html_lib.unescape(subject.strip()),
                "room": html_lib.unescape(room.strip()),
                "teacher": html_lib.unescape(teacher.strip()),
                "lesson_type": lesson_type.strip(),
                "parity": parity,
            })

    return lessons


def _clean_cell(cell) -> str:
    text = cell.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _detect_day(texts: list) -> str:
    for t in texts:
        low = t.lower().strip()
        for ru, upper in _DAY_NAMES.items():
            if ru in low:
                return upper
    return ""


def _is_day_name(text: str) -> bool:
    low = text.lower().strip()
    return any(ru in low for ru in _DAY_NAMES)


def _looks_like_room(text: str) -> bool:
    t = text.strip()
    if re.match(r"^\d{3,4}[а-яА-Я]?$", t):
        return True
    low = t.lower()
    return any(kw in low for kw in ("ауд", "корп", "zoom", "ломон", "кронв", "биржев"))


def _looks_like_teacher(text: str) -> bool:
    parts = text.strip().split()
    if len(parts) < 2:
        return False
    RU = re.compile(r"^[А-ЯЁа-яё]")
    if not RU.match(parts[0]):
        return False
    if len(parts) >= 2 and re.match(r"^[А-ЯЁ]\.?$", parts[1]):
        return True
    if len(parts) >= 3 and all(re.match(r"^[А-ЯЁа-яё]", p) for p in parts[:3]):
        return True
    return False


def _looks_like_type(low_text: str) -> bool:
    return any(kw in low_text for kw in ("лек", "практ", "лаб", "семин"))


def _append(existing: str, new: str) -> str:
    if existing:
        return f"{existing}, {new}"
    return new
