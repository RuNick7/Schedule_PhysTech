from __future__ import annotations

import json
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.config import settings
from app.services.db import get_user, set_gcal_tokens


GCAL_API = "https://www.googleapis.com/calendar/v3"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_REVOKE_URL = "https://oauth2.googleapis.com/revoke"


class GCalError(RuntimeError):
    pass


@dataclass
class TokenBundle:
    access_token: str
    refresh_token: Optional[str]
    expiry_iso: str


# ======== ВНУТРЕННЕЕ: работа с токенами ========
def revoke_tokens(telegram_id: int) -> bool:
    """
    Пытается отозвать refresh/access токены в Google.
    Возвращает True, если запрос(ы) к revoke прошли без фатальной ошибки.
    """
    import requests
    import logging
    log = logging.getLogger("gcal.api")

    u = get_user(telegram_id)
    if not u:
        return True  # ничего отзывать

    ok = True
    for tok in (u.get("gcal_refresh_token"), u.get("gcal_access_token")):
        if not tok:
            continue
        try:
            # 200 — успех, 400 — уже отозван/несуществующий токен (тоже ок)
            r = requests.post(
                GOOGLE_REVOKE_URL,
                data={"token": tok},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10,
            )
            if r.status_code not in (200, 400):
                log.warning("revoke token unexpected status=%s body=%s", r.status_code, r.text)
        except Exception as e:
            ok = False
            log.exception("revoke token failed: %s", e)

    return ok

def _parse_iso_utc(s: str) -> float:
    """
    'YYYY-MM-DDTHH:MM:SSZ' -> epoch seconds (UTC).
    """
    # грубый, но рабочий парсер без зависимостей
    try:
        # 2025-09-05T12:34:56Z
        y = int(s[0:4]); m = int(s[5:7]); d = int(s[8:10])
        hh = int(s[11:13]); mm = int(s[14:16]); ss = int(s[17:19])
        # простая конверсия: time.gmtime/time.mktime не в UTC, поэтому используем calendar.timegm
        import calendar
        return calendar.timegm((y, m, d, hh, mm, ss))
    except Exception:
        return 0.0


def _need_refresh(expiry_iso: Optional[str], skew_sec: int = 60) -> bool:
    if not expiry_iso:
        return True
    return time.time() >= (_parse_iso_utc(expiry_iso) - skew_sec)


def _refresh_access_token(refresh_token: str) -> TokenBundle:
    """
    Обновляет access_token по refresh_token.
    """
    # читаем oauth-client.json, как в oauth_server.py
    import json, os
    GCAL_OAUTH_CLIENT_FILE = getattr(settings, "gcal_oauth_client_file", None) or \
                             os.getenv("GCAL_OAUTH_CLIENT_FILE", "oauth-client.json")
    with open(GCAL_OAUTH_CLIENT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    web = data.get("web") or {}
    client_id = web["client_id"]
    client_secret = web["client_secret"]

    resp = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=15,
    )
    if resp.status_code != 200:
        raise GCalError(f"Refresh token failed: {resp.status_code} {resp.text}")

    tok = resp.json()
    access = tok["access_token"]
    expires_in = int(tok.get("expires_in", 3600))
    expiry_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + expires_in - 30))
    # refresh_token при refresh-е обычно не возвращается
    return TokenBundle(access_token=access, refresh_token=None, expiry_iso=expiry_iso)


def ensure_token(telegram_id: int) -> str:
    """
    Возвращает валидный access_token для пользователя. При необходимости обновляет.
    """
    u = get_user(telegram_id)
    if not u or not u.get("gcal_connected"):
        raise GCalError("Google Calendar не подключён для этого пользователя.")
    access = u.get("gcal_access_token")
    refresh = u.get("gcal_refresh_token")
    expiry = u.get("gcal_token_expiry")

    if not access:
        if not refresh:
            raise GCalError("Нет действующего access_token и refresh_token.")
        bundle = _refresh_access_token(refresh)
        set_gcal_tokens(telegram_id, bundle.access_token, None, bundle.expiry_iso)
        return bundle.access_token

    if _need_refresh(expiry):
        if not refresh:
            # пробуем всё равно — может жить
            return access
        bundle = _refresh_access_token(refresh)
        set_gcal_tokens(telegram_id, bundle.access_token, None, bundle.expiry_iso)
        return bundle.access_token

    return access


def _headers(access_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8",
    }


# ======== Общие операции с календарями ========
def list_calendars(telegram_id: int) -> List[Dict[str, Any]]:
    """
    Возвращает список календарей пользователя:
    [{id, summary, primary: bool}, ...]
    """
    access = ensure_token(telegram_id)
    url = f"{GCAL_API}/users/me/calendarList"
    out: List[Dict[str, Any]] = []
    page_token: Optional[str] = None

    while True:
        params: Dict[str, Any] = {}
        if page_token:
            params["pageToken"] = page_token

        r = requests.get(url, headers=_headers(access), params=params, timeout=15)
        if r.status_code != 200:
            raise GCalError(f"list_calendars failed: {r.status_code} {r.text}")

        data = r.json()
        for it in data.get("items", []):
            out.append({
                "id": it["id"],
                "summary": it.get("summary"),
                "primary": bool(it.get("primary")),
            })

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return out

def delete_events_by_tag_between(
    telegram_id: int,
    calendar_id: str,
    tag_key: str = "sched_bot",
    tag_value: str = "1",
    time_min_iso: Optional[str] = None,  # RFC3339 с таймзоной, напр. '2025-09-15T00:00:00+03:00'
    time_max_iso: Optional[str] = None,  # правая граница (исключительно)
) -> int:
    """
    Удаляет все события пользователя в указанном календаре, помеченные
    privateExtendedProperty (tag_key=tag_value) и попадающие в интервал [timeMin, timeMax).
    Возвращает число удалённых.

    ВАЖНО: time_min_iso/time_max_iso должны быть ISO 8601/RFC3339 с таймзоной.
    Пример: '2025-09-15T00:00:00+03:00' ... '2025-09-29T00:00:00+03:00'
    """
    access = ensure_token(telegram_id)

    url_list = f"{GCAL_API}/calendars/{urllib.parse.quote(calendar_id)}/events"
    url_del_tpl = f"{GCAL_API}/calendars/{urllib.parse.quote(calendar_id)}/events/{{event_id}}"

    deleted = 0
    page_token = None
    prop = f"{tag_key}={tag_value}"

    base_params: Dict[str, str] = {
        "privateExtendedProperty": prop,
        "singleEvents": "true",
        "showDeleted": "false",
        "maxResults": "2500",
        "orderBy": "startTime",
    }
    if time_min_iso:
        base_params["timeMin"] = time_min_iso
    if time_max_iso:
        base_params["timeMax"] = time_max_iso

    while True:
        params = dict(base_params)
        if page_token:
            params["pageToken"] = page_token

        r = requests.get(url_list, headers=_headers(access), params=params, timeout=20)
        if r.status_code != 200:
            raise GCalError(f"list events (window) failed: {r.status_code} {r.text}")
        data = r.json()

        for it in data.get("items", []):
            ev_id = it["id"]
            rdel = requests.delete(
                url_del_tpl.format(event_id=urllib.parse.quote(ev_id)),
                headers=_headers(access),
                timeout=15,
            )
            if rdel.status_code in (204, 200):
                deleted += 1
            else:
                # мягко игнорируем неуспех удаления отдельного события
                pass

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return deleted

def create_calendar(telegram_id: int, title: str, tz: Optional[str] = None) -> str:
    """
    Создаёт новый календарь у пользователя. Возвращает его id.
    """
    access = ensure_token(telegram_id)
    body = {
        "summary": title,
        "timeZone": tz or getattr(settings, "timezone", "Europe/Moscow"),
    }
    r = requests.post(
        f"{GCAL_API}/calendars",
        headers=_headers(access),
        data=json.dumps(body),
        timeout=15,
    )
    if r.status_code not in (200, 201):
        raise GCalError(f"create_calendar failed: {r.status_code} {r.text}")
    return r.json()["id"]

def list_calendars(telegram_id: int) -> List[Dict[str, Any]]:
    """
    Возвращает список календарей пользователя:
    [{id, summary, primary: bool}, ...]
    """
    access = ensure_token(telegram_id)
    url = f"{GCAL_API}/users/me/calendarList"
    out: List[Dict[str, Any]] = []
    page_token = None
    while True:
        params = {}
        if page_token:
            params["pageToken"] = page_token
        r = requests.get(url, headers=_headers(access), params=params, timeout=15)
        if r.status_code != 200:
            raise GCalError(f"list_calendars failed: {r.status_code} {r.text}")
        data = r.json()
        for it in data.get("items", []):
            out.append({
                "id": it["id"],
                "summary": it.get("summary"),
                "primary": bool(it.get("primary")),
            })
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return out


def create_calendar(telegram_id: int, title: str, tz: Optional[str] = None) -> str:
    """
    Создаёт отдельный календарь пользователя. Возвращает id созданного календаря.
    """
    access = ensure_token(telegram_id)
    body = {
        "summary": title,
        "timeZone": tz or getattr(settings, "timezone", "Europe/Moscow"),
    }
    r = requests.post(f"{GCAL_API}/calendars", headers=_headers(access), data=json.dumps(body), timeout=15)
    if r.status_code not in (200, 201):
        raise GCalError(f"create_calendar failed: {r.status_code} {r.text}")
    return r.json()["id"]


# ======== События ========

def _find_event_by_private_key(
    telegram_id: int,
    calendar_id: str,
    key: str,
) -> Optional[Dict[str, Any]]:
    """
    Ищет наше событие по privateExtendedProperty: 'sched_key=<key>'.
    Возвращает объект события (как в API) или None.
    """
    access = ensure_token(telegram_id)
    prop = f"sched_key={key}"
    url = f"{GCAL_API}/calendars/{urllib.parse.quote(calendar_id)}/events"
    params = {
        "privateExtendedProperty": prop,
        "singleEvents": "true",
        "maxResults": 2,
        "showDeleted": "false",
    }
    r = requests.get(url, headers=_headers(access), params=params, timeout=15)
    if r.status_code != 200:
        raise GCalError(f"find_event failed: {r.status_code} {r.text}")
    items = r.json().get("items", [])
    return items[0] if items else None


def upsert_event(
    telegram_id: int,
    calendar_id: str,
    event: Dict[str, Any],
    key: str,
) -> Dict[str, Any]:
    """
    Идемпотентно создаёт/обновляет событие.
    Поисковый ключ — privateExtendedProperty 'sched_key=<key>'.
    Требование: в event['extendedProperties']['private'] должны быть:
      - 'sched_bot': '1'
      - 'sched_key': key
      - (опц.) 'group': '<код группы>'
    """
    # проверка private props
    exprops = ((event.get("extendedProperties") or {}).get("private") or {})
    if exprops.get("sched_key") != key or exprops.get("sched_bot") != "1":
        raise GCalError("event.extendedProperties.private must include sched_bot='1' and sched_key=key.")

    access = ensure_token(telegram_id)
    found = _find_event_by_private_key(telegram_id, calendar_id, key)
    if found:
        event_id = found["id"]
        url = f"{GCAL_API}/calendars/{urllib.parse.quote(calendar_id)}/events/{urllib.parse.quote(event_id)}"
        r = requests.patch(url, headers=_headers(access), data=json.dumps(event), timeout=15)
        if r.status_code != 200:
            raise GCalError(f"update event failed: {r.status_code} {r.text}")
        return r.json()
    else:
        url = f"{GCAL_API}/calendars/{urllib.parse.quote(calendar_id)}/events"
        r = requests.post(url, headers=_headers(access), data=json.dumps(event), timeout=15)
        if r.status_code not in (200, 201):
            raise GCalError(f"insert event failed: {r.status_code} {r.text}")
        return r.json()


def delete_events_by_tag(
    telegram_id: int,
    calendar_id: str,
    tag_key: str = "sched_bot",
    tag_value: str = "1",
) -> int:
    """
    Удаляет все «наши» события по privateExtendedProperty, возвращает число удалённых.
    Пример: tag_key='sched_bot', tag_value='1'
    """
    access = ensure_token(telegram_id)
    deleted = 0
    url_list = f"{GCAL_API}/calendars/{urllib.parse.quote(calendar_id)}/events"
    url_del_tpl = f"{GCAL_API}/calendars/{urllib.parse.quote(calendar_id)}/events/{{event_id}}"

    page_token = None
    prop = f"{tag_key}={tag_value}"

    while True:
        params = {
            "privateExtendedProperty": prop,
            "singleEvents": "true",
            "maxResults": 2500,
            "showDeleted": "false",
        }
        if page_token:
            params["pageToken"] = page_token

        r = requests.get(url_list, headers=_headers(access), params=params, timeout=20)
        if r.status_code != 200:
            raise GCalError(f"list events for delete failed: {r.status_code} {r.text}")
        data = r.json()
        for it in data.get("items", []):
            ev_id = it["id"]
            rdel = requests.delete(url_del_tpl.format(event_id=urllib.parse.quote(ev_id)), headers=_headers(access), timeout=15)
            if rdel.status_code not in (204, 200):
                # не прерываемся, но фиксируем проблему
                # raise GCalError(f"delete event failed: {rdel.status_code} {rdel.text}")
                pass
            else:
                deleted += 1

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return deleted


# ======== Утилиты для сборки события (минимум) ========

def build_event_min(
    *,
    summary: str,
    description: Optional[str],
    start_iso: str,
    end_iso: str,
    tz: Optional[str],
    location: Optional[str],
    private_props: Optional[Dict[str, str]],
) -> Dict[str, Any]:
    """
    Упрощённый конструктор события Google Calendar.
    Ожидает start_iso/end_iso в формате 'YYYY-MM-DDTHH:MM:SS'.
    """
    tzname = tz or getattr(settings, "timezone", "Europe/Moscow")
    ev: Dict[str, Any] = {
        "summary": summary,
        "description": description or "",
        "start": {"dateTime": f"{start_iso}", "timeZone": tzname},
        "end": {"dateTime": f"{end_iso}", "timeZone": tzname},
    }
    if location:
        ev["location"] = location
    if private_props:
        ev["extendedProperties"] = {"private": dict(private_props)}
    return ev
