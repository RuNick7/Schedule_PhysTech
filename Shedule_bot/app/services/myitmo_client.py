from __future__ import annotations

import html
import os
import re
import urllib.parse
from base64 import urlsafe_b64encode
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from time import time
from typing import Any, Dict, List, Optional, Tuple

import requests


class MyItmoError(RuntimeError):
    pass


_CLIENT_ID = "student-personal-cabinet"
_REDIRECT_URI = "https://my.itmo.ru/login/callback"
_PROVIDER = "https://id.itmo.ru/auth/realms/itmo"
_API_BASE_URL = "https://my.itmo.ru/api"
_LOGIN_ACTION_RE = re.compile(r'"loginAction":\s*"(?P<action>[^"]+)"', re.DOTALL)
_FORM_ACTION_RE = re.compile(r'<form\s+.*?\s+action="(?P<action>[^"]+)"', re.DOTALL)
_TOKEN_CACHE: Dict[str, Tuple[str, float]] = {}


def _generate_code_verifier() -> str:
    code_verifier = urlsafe_b64encode(os.urandom(40)).decode("utf-8")
    return re.sub(r"[^a-zA-Z0-9]+", "", code_verifier)


def _get_code_challenge(code_verifier: str) -> str:
    code_challenge_bytes = sha256(code_verifier.encode("utf-8")).digest()
    code_challenge = urlsafe_b64encode(code_challenge_bytes).decode("utf-8")
    return code_challenge.replace("=", "")


def _get_date_range_params(date_start: Optional[str] = None, date_end: Optional[str] = None) -> Dict[str, str]:
    """
    По умолчанию берём ограниченное окно вокруг текущей даты,
    чтобы не тянуть весь учебный год на каждый запрос.
    """
    if date_start and date_end:
        return {"date_start": date_start, "date_end": date_end}

    today = date.today()
    start = today - timedelta(days=14)
    end = today + timedelta(days=28)
    return {
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
    }


def _parse_expiry_to_ts(expiry_iso: Optional[str]) -> float:
    if not expiry_iso:
        return 0.0
    try:
        # Support both ...Z and +00:00.
        return datetime.fromisoformat(expiry_iso.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _expiry_iso(expires_in: int) -> str:
    dt = datetime.now(timezone.utc) + timedelta(seconds=max(60, int(expires_in)))
    return dt.isoformat()


def _extract_login_action(page_text: str) -> str:
    m = _LOGIN_ACTION_RE.search(page_text or "")
    if m:
        return html.unescape(m.group("action"))
    m = _FORM_ACTION_RE.search(page_text or "")
    if m:
        return html.unescape(m.group("action"))
    raise MyItmoError("Не удалось получить loginAction со страницы авторизации.")


def _token_request_by_password(username: str, password: str, timeout: int = 20) -> Dict[str, Any]:
    code_verifier = _generate_code_verifier()
    code_challenge = _get_code_challenge(code_verifier)
    with requests.Session() as session:
        auth_resp = session.get(
            f"{_PROVIDER}/protocol/openid-connect/auth",
            params={
                "protocol": "oauth2",
                "response_type": "code",
                "client_id": _CLIENT_ID,
                "redirect_uri": _REDIRECT_URI,
                "scope": "openid",
                "state": "schedule-bot",
                "code_challenge_method": "S256",
                "code_challenge": code_challenge,
            },
            timeout=timeout,
        )
        auth_resp.raise_for_status()

        form_action = _extract_login_action(auth_resp.text)

        form_resp = session.post(
            url=form_action,
            data={"username": username, "password": password},
            cookies=auth_resp.cookies,
            allow_redirects=True,
            timeout=timeout,
        )
        # На разных конфигурациях после успешного логина может быть и 302, и 200.
        # Признак успеха в обоих случаях — наличие authorization code в URL редиректа.
        redirected_to = form_resp.headers.get("Location", "") or str(form_resp.url)
        query = urllib.parse.urlparse(redirected_to).query
        redirect_params = urllib.parse.parse_qs(query)
        auth_code = (redirect_params.get("code") or [None])[0]
        if not auth_code:
            # Обычно это неверные учётные данные или изменившаяся форма логина.
            raise MyItmoError(
                f"Не удалось получить authorization code (HTTP {form_resp.status_code}). "
                "Проверьте логин и пароль my.itmo."
            )

        token_resp = session.post(
            url=f"{_PROVIDER}/protocol/openid-connect/token",
            data={
                "grant_type": "authorization_code",
                "client_id": _CLIENT_ID,
                "redirect_uri": _REDIRECT_URI,
                "code": auth_code,
                "code_verifier": code_verifier,
            },
            allow_redirects=False,
            timeout=timeout,
        )
        token_resp.raise_for_status()
        return token_resp.json()


def _token_request_by_refresh(refresh_token: str, timeout: int = 20) -> Dict[str, Any]:
    token_resp = requests.post(
        url=f"{_PROVIDER}/protocol/openid-connect/token",
        data={
            "grant_type": "refresh_token",
            "client_id": _CLIENT_ID,
            "refresh_token": refresh_token,
        },
        allow_redirects=False,
        timeout=timeout,
    )
    token_resp.raise_for_status()
    return token_resp.json()


def exchange_password_for_tokens(username: str, password: str, timeout: int = 20) -> Dict[str, str]:
    payload = _token_request_by_password(username=username, password=password, timeout=timeout)
    access = str(payload.get("access_token") or "").strip()
    refresh = str(payload.get("refresh_token") or "").strip()
    if not access:
        raise MyItmoError("my.itmo не вернул access_token.")
    if not refresh:
        raise MyItmoError("my.itmo не вернул refresh_token.")
    expiry = _expiry_iso(int(payload.get("expires_in", 1800)))
    _TOKEN_CACHE[username] = (access, _parse_expiry_to_ts(expiry))
    return {
        "access_token": access,
        "refresh_token": refresh,
        "token_expiry": expiry,
    }


def _ensure_access_token(
    username: str,
    timeout: int = 20,
    access_token: Optional[str] = None,
    token_expiry: Optional[str] = None,
    refresh_token: Optional[str] = None,
    password: Optional[str] = None,
    force_refresh: bool = False,
) -> Dict[str, str]:
    access = (access_token or "").strip()
    refresh = (refresh_token or "").strip()
    expiry_ts = _parse_expiry_to_ts(token_expiry)
    now = time()

    if not force_refresh and access and expiry_ts - now > 30:
        _TOKEN_CACHE[username] = (access, expiry_ts)
        return {
            "access_token": access,
            "refresh_token": refresh,
            "token_expiry": token_expiry or _expiry_iso(1800),
        }

    cached = _TOKEN_CACHE.get(username)
    if not force_refresh and cached and cached[1] - now > 30:
        return {
            "access_token": cached[0],
            "refresh_token": refresh,
            "token_expiry": token_expiry or _expiry_iso(1800),
        }

    if refresh:
        try:
            payload = _token_request_by_refresh(refresh_token=refresh, timeout=timeout)
            access_new = str(payload.get("access_token") or "").strip()
            if not access_new:
                raise MyItmoError("my.itmo не вернул access_token при refresh.")
            refresh_new = str(payload.get("refresh_token") or "").strip() or refresh
            expiry_new = _expiry_iso(int(payload.get("expires_in", 1800)))
            _TOKEN_CACHE[username] = (access_new, _parse_expiry_to_ts(expiry_new))
            return {
                "access_token": access_new,
                "refresh_token": refresh_new,
                "token_expiry": expiry_new,
            }
        except Exception:
            if not password:
                raise MyItmoError("Не удалось обновить токен my.itmo. Введите пароль заново в настройках.")

    if password:
        return exchange_password_for_tokens(username=username, password=password, timeout=timeout)

    raise MyItmoError("Не заданы токены my.itmo. Откройте Настройки → my.itmo аккаунт и выполните вход.")


def fetch_personal_schedule(
    username: str,
    access_token: Optional[str] = None,
    refresh_token: Optional[str] = None,
    token_expiry: Optional[str] = None,
    password: Optional[str] = None,
    timeout: int = 20,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    params = _get_date_range_params(date_start=date_start, date_end=date_end)
    bundle = _ensure_access_token(
        username=username,
        timeout=timeout,
        access_token=access_token,
        refresh_token=refresh_token,
        token_expiry=token_expiry,
        password=password,
        force_refresh=False,
    )
    token = bundle["access_token"]
    resp = requests.get(
        f"{_API_BASE_URL}/schedule/schedule/personal",
        params=params,
        headers={"Authorization": f"Bearer {token}"},
        timeout=timeout,
    )
    if resp.status_code == 401:
        # Токен мог протухнуть между запросами — обновим и повторим один раз.
        bundle = _ensure_access_token(
            username=username,
            timeout=timeout,
            access_token=access_token,
            refresh_token=refresh_token,
            token_expiry=token_expiry,
            password=password,
            force_refresh=True,
        )
        token = bundle["access_token"]
        resp = requests.get(
            f"{_API_BASE_URL}/schedule/schedule/personal",
            params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
        )
    resp.raise_for_status()
    data = resp.json().get("data") or []

    lessons: List[Dict[str, Any]] = []
    for day in data:
        day_date = day.get("date")
        for lesson in day.get("lessons", []):
            lessons.append({"date": day_date, **lesson})
    return lessons, bundle

