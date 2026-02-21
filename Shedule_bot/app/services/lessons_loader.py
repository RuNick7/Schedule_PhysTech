from __future__ import annotations
import logging
from datetime import date, datetime, timedelta
import time
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from app.services.sheets_client import fetch_sheet_values_and_links
from app.services.schedule_expand import expand_merged_matrix
from app.services.schedule_list import list_lessons_matrix
from app.config import settings
from app.services.myitmo_client import fetch_personal_schedule, MyItmoError
from app.services.db import set_myitmo_tokens
from app.utils.week_parity import week_parity_for_date

log = logging.getLogger("schedule.loader")

_DAY_UP = ["ПОНЕДЕЛЬНИК", "ВТОРНИК", "СРЕДА", "ЧЕТВЕРГ", "ПЯТНИЦА", "СУББОТА", "ВОСКРЕСЕНЬЕ"]

# Простые in-memory TTL-кэши для ускорения ответов в рантайме.
_SHEETS_CACHE_TTL_SEC = 60
_MYITMO_CACHE_TTL_SEC = 60
_USER_VIEW_CACHE_TTL_SEC = 20

_SHEETS_ALL_CACHE: Optional[Tuple[float, List[Dict]]] = None
_MYITMO_RAW_CACHE: Dict[Tuple[str, str, str], Tuple[float, List[Dict]]] = {}
_RESULT_CACHE: Dict[Tuple[str, str, str, str], Tuple[float, List[Dict]]] = {}


def _norm(s: str) -> str:
    return " ".join((s or "").lower().replace("ё", "е").strip().split())


def _is_special_subject(text: str) -> bool:
    t = _norm(text)
    return ("истор" in t) or ("англ" in t) or ("англий" in t)


def _split_lesson_text(text: str) -> Tuple[str, str]:
    parts = (text or "").split(" — ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return (text or "").strip(), ""


def _short_building_room(building: str, room: str) -> str:
    b = (building or "").strip()
    r = (room or "").strip()
    nb = _norm(b)
    if "ломоносов" in nb:
        return f"Ломо {r}" if r else "Ломо"
    if "кронверк" in nb:
        return f"Кронва {r}" if r else "Кронва"
    if r:
        return r
    return b


def _short_lesson_type(raw_type: str) -> str:
    t = _norm(raw_type)
    if "лек" in t:
        return "лек."
    if "практ" in t:
        return "практ."
    if "лаб" in t:
        return "лаб."
    return ""


def _teacher_short_fio(raw_teacher: str) -> str:
    t = (raw_teacher or "").strip()
    if not t:
        return ""
    parts = [p for p in t.split() if p]
    if not parts:
        return ""
    surname = parts[0]
    if len(parts) == 1:
        return surname
    initials = "".join(f"{p[0].upper()}." for p in parts[1:] if p and p[0].isalpha())
    return f"{surname} {initials}".strip()


def _abbr_subject(subject: str) -> str:
    s = (subject or "").strip()
    ns = _norm(s)
    if "линейная алгебра" in ns:
        return "Лин.Ал."
    if "физическая химия" in ns:
        return "Физ.Хим."
    if "математический анализ" in ns:
        return "Мат.Анал."
    if "истор" in ns:
        return "История"
    if "техники публичных выступлений" in ns:
        return "ТПВ"
    if "программирование" in ns:
        return "Прог."
    if "англ" in ns or "english" in ns:
        lvl = ""
        for cand in ("A1", "A2", "B1", "B2", "C1", "C2"):
            if cand.lower() in ns:
                lvl = f" {cand}"
                break
        return f"Англ.Яз.{lvl}".strip()
    return s



def _weekday_upper(ymd: Optional[str]) -> Optional[str]:
    if not ymd:
        return None
    try:
        dt = date.fromisoformat(ymd)
    except ValueError:
        return None
    return _DAY_UP[dt.weekday()]


def _current_and_next_week_range(tz: str) -> Tuple[str, str]:
    now = date.today()
    try:
        now = datetime.now(ZoneInfo(tz)).date()
    except Exception:
        pass
    monday = now - timedelta(days=now.weekday())
    end = monday + timedelta(days=13)  # текущая + следующая недели
    return monday.isoformat(), end.isoformat()


def _today_and_week_key(tz: str) -> Tuple[str, str]:
    now = date.today()
    try:
        now = datetime.now(ZoneInfo(tz)).date()
    except Exception:
        pass
    iso = now.isocalendar()
    return now.isoformat(), f"{iso.year}-W{iso.week:02d}"


def _parity_from_lesson_date(ymd: Optional[str], tz: str) -> str:
    try:
        if not ymd:
            return week_parity_for_date(None, tz)
        return week_parity_for_date(date.fromisoformat(ymd), tz)
    except Exception:
        return week_parity_for_date(None, tz)


def _build_myitmo_index(raw_lessons: List[Dict]) -> Dict[Tuple[str, str], Dict[str, str]]:
    """
    Индекс my.itmo по (DAY_UPPER, HH:MM-HH:MM) с полезными полями.
    Храним только спец-предметы (англ/история), чтобы не трогать остальные пары.
    """
    idx: Dict[Tuple[str, str], Dict[str, str]] = {}
    for lesson in raw_lessons:
        day = _weekday_upper(lesson.get("date"))
        tstart = (lesson.get("time_start") or "").strip()
        tend = (lesson.get("time_end") or "").strip()
        subject = (lesson.get("subject") or "").strip()
        if not day or not tstart or not tend or not subject:
            continue
        if not _is_special_subject(subject):
            continue

        key = (day, f"{tstart}-{tend}")
        room = (lesson.get("room") or "").strip()
        building = (lesson.get("building") or "").strip()
        teacher = _teacher_short_fio(str(lesson.get("teacher_name") or lesson.get("teacher_fio") or ""))

        room_full = _short_building_room(building, room)

        value: Dict[str, str] = {}
        if room_full:
            value["room"] = room_full
        if teacher:
            value["teacher"] = teacher
        if value:
            idx[key] = value
    return idx


def _build_lessons_from_myitmo(raw_lessons: List[Dict], fallback_group: Optional[str], tz: str) -> List[Dict]:
    out: List[Dict] = []
    for lesson in raw_lessons:
        ymd = lesson.get("date")
        day = _weekday_upper(ymd)
        tstart = (lesson.get("time_start") or "").strip()
        tend = (lesson.get("time_end") or "").strip()
        subject = (lesson.get("subject") or "").strip()
        if not day or not tstart or not tend or not subject:
            continue

        teacher_surname = _teacher_short_fio(str(lesson.get("teacher_name") or lesson.get("teacher_fio") or ""))
        t_short = _short_lesson_type(str(lesson.get("type") or ""))
        subj_short = _abbr_subject(subject)
        if subj_short == "Прог." and t_short:
            lecture = f"Прог.{t_short.capitalize()}"
        else:
            lecture = f"{subj_short} {t_short}".strip() if t_short else subj_short
        if teacher_surname:
            lecture = f"{lecture} {teacher_surname}".strip()

        room = (lesson.get("room") or "").strip()
        building = (lesson.get("building") or "").strip()
        room_full = _short_building_room(building, room)

        special = False
        if not room_full and _is_special_subject(subject):
            special = True
            room_full = "⚠️ см. прилож."

        text = f"{lecture} — {room_full}" if room_full else lecture
        out.append({
            "group": fallback_group or (lesson.get("group") or ""),
            "day": day,
            "time": f"{tstart}-{tend}",
            "course": "",
            "parity": _parity_from_lesson_date(ymd, tz),
            "text": text,
            "special": special,
            "room_is_zoom": bool(lesson.get("zoom_url")),
            "room_link": lesson.get("zoom_url") if lesson.get("zoom_url") else None,
        })
    return out


def _enrich_from_myitmo(lessons: List[Dict], itmo_index: Dict[Tuple[str, str], Dict[str, str]]) -> List[Dict]:
    enriched: List[Dict] = []
    for lesson in lessons:
        lesson_copy = dict(lesson)
        lecture, room = _split_lesson_text(lesson_copy.get("text", ""))
        if not _is_special_subject(lecture):
            enriched.append(lesson_copy)
            continue

        key = (str(lesson_copy.get("day") or "").strip().upper(), str(lesson_copy.get("time") or "").strip())
        src = itmo_index.get(key)
        if not src:
            enriched.append(lesson_copy)
            continue

        # Добавляем преподавателя в лекцию только если его ещё нет в тексте.
        teacher = src.get("teacher")
        if teacher and _norm(teacher) not in _norm(lecture):
            lecture = f"{lecture} {teacher}".strip()

        # Дополняем аудиторию и снимаем special, чтобы форматтер показал обычную 📍 аудиторию.
        if src.get("room"):
            room = src["room"]
            lesson_copy["special"] = False

        lesson_copy["text"] = f"{lecture} — {room}" if room else lecture
        enriched.append(lesson_copy)
    return enriched

async def load_lessons_for_user_group(user: dict):
    mode = str(user.get("schedule_source_mode") or "sheets").strip().lower()
    tz = user.get("timezone") or settings.timezone
    uid = int(user.get("telegram_id") or user.get("id") or 0)
    today_key, week_key = _today_and_week_key(tz)
    group_code = str(user.get("group_code") or "")

    # Для sheets-кейсов из .env расписание одинаково для группы.
    # Если у пользователя задана персональная таблица — scope отдельный.
    # Для my.itmo-режимов кэш строго персональный (по user id), чтобы
    # не смешивать индивидуальные пары (английский/история и т.п.).
    custom_sheet_id = str(user.get("user_spreadsheet_id") or "").strip()
    custom_sheet_gid = str(user.get("user_sheet_gid") if user.get("user_sheet_gid") is not None else "")
    if mode in ("myitmo_full", "hybrid"):
        cache_scope = f"user:{uid}"
    elif custom_sheet_id:
        cache_scope = f"sheet:{custom_sheet_id}:{custom_sheet_gid}"
    else:
        cache_scope = "group_shared"
    result_key = (mode, group_code, today_key, f"{week_key}:{cache_scope}")

    cached_user = _RESULT_CACHE.get(result_key)
    now_ts = time.monotonic()
    if cached_user and cached_user[0] > now_ts:
        return cached_user[1]

    def _load_sheets_lessons() -> List[Dict]:
        global _SHEETS_ALL_CACHE
        spreadsheet_id = str(user.get("user_spreadsheet_id") or settings.spreadsheet_id)
        sheet_gid_raw = user.get("user_sheet_gid")
        try:
            sheet_gid = int(sheet_gid_raw) if sheet_gid_raw is not None else int(settings.sheet_gid)
        except Exception:
            sheet_gid = int(settings.sheet_gid)
        now_local = time.monotonic()
        use_shared_cache = not bool(user.get("user_spreadsheet_id"))
        if use_shared_cache and _SHEETS_ALL_CACHE and _SHEETS_ALL_CACHE[0] > now_local:
            all_lessons = _SHEETS_ALL_CACHE[1]
        else:
            vals, links, merges = fetch_sheet_values_and_links(
                spreadsheet_id=spreadsheet_id,
                sheet_gid=sheet_gid,
                creds_path=settings.google_credentials,
            )
            mtx_vals = expand_merged_matrix(vals, merges=merges)
            mtx_links = expand_merged_matrix(links, merges=merges)
            all_lessons = list_lessons_matrix(mtx_vals, mtx_links)
            if use_shared_cache:
                _SHEETS_ALL_CACHE = (now_local + _SHEETS_CACHE_TTL_SEC, all_lessons)
        return [it for it in all_lessons if it["group"] == user["group_code"]]

    def _load_myitmo_raw() -> List[Dict]:
        username = (user.get("myitmo_username") or "").strip()
        access_token = (user.get("myitmo_access_token") or "").strip()
        refresh_token = (user.get("myitmo_refresh_token") or "").strip()
        token_expiry = (user.get("myitmo_token_expiry") or "").strip()
        if not username or not refresh_token:
            raise MyItmoError("Не подключен my.itmo. Откройте Настройки → my.itmo аккаунт.")
        date_start, date_end = _current_and_next_week_range(tz)
        cache_key = (username, date_start, date_end)
        now_local = time.monotonic()
        cached = _MYITMO_RAW_CACHE.get(cache_key)
        if cached and cached[0] > now_local:
            return cached[1]
        raw, token_bundle = fetch_personal_schedule(
            username=username,
            access_token=access_token or None,
            refresh_token=refresh_token or None,
            token_expiry=token_expiry or None,
            timeout=settings.myitmo_timeout_sec,
            date_start=date_start,
            date_end=date_end,
        )
        if token_bundle.get("refresh_token") and (
            token_bundle.get("access_token") != access_token
            or token_bundle.get("refresh_token") != refresh_token
            or token_bundle.get("token_expiry") != token_expiry
        ):
            set_myitmo_tokens(
                uid,
                token_bundle["access_token"],
                token_bundle["refresh_token"],
                token_bundle["token_expiry"],
            )
        _MYITMO_RAW_CACHE[cache_key] = (now_local + _MYITMO_CACHE_TTL_SEC, raw)
        return raw

    if mode == "sheets":
        lessons = _load_sheets_lessons()
        _RESULT_CACHE[result_key] = (time.monotonic() + _USER_VIEW_CACHE_TTL_SEC, lessons)
        return lessons

    if mode == "myitmo_full":
        try:
            raw_itmo = _load_myitmo_raw()
            lessons = _build_lessons_from_myitmo(raw_itmo, fallback_group=user.get("group_code"), tz=tz)
            _RESULT_CACHE[result_key] = (time.monotonic() + _USER_VIEW_CACHE_TTL_SEC, lessons)
            return lessons
        except (MyItmoError, Exception) as e:
            log.warning("my.itmo full mode unavailable, fallback to sheets: %s", e)
            lessons = _load_sheets_lessons()
            _RESULT_CACHE[result_key] = (time.monotonic() + _USER_VIEW_CACHE_TTL_SEC, lessons)
            return lessons

    # hybrid (по умолчанию для неизвестных значений — тоже hybrid)
    lessons = _load_sheets_lessons()
    try:
        raw_itmo = _load_myitmo_raw()
        idx = _build_myitmo_index(raw_itmo)
        if not idx:
            _RESULT_CACHE[result_key] = (time.monotonic() + _USER_VIEW_CACHE_TTL_SEC, lessons)
            return lessons
        enriched = _enrich_from_myitmo(lessons, idx)
        _RESULT_CACHE[result_key] = (time.monotonic() + _USER_VIEW_CACHE_TTL_SEC, enriched)
        return enriched
    except (MyItmoError, Exception) as e:
        log.warning("Не удалось обогатить расписание через my.itmo: %s", e)
        _RESULT_CACHE[result_key] = (time.monotonic() + _USER_VIEW_CACHE_TTL_SEC, lessons)
        return lessons
