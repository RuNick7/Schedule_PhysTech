from __future__ import annotations

import json
import os
import time
import urllib.parse
from typing import Tuple

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

# ---- простые настройки из ENV/конфига ----
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
GCAL_OAUTH_CLIENT_FILE = os.getenv("GCAL_OAUTH_CLIENT_FILE", "oauth-client.json")
GCAL_SCOPES = os.getenv(
    "GCAL_SCOPES",
    "https://www.googleapis.com/auth/calendar.events"
)

if not PUBLIC_BASE_URL:
    raise RuntimeError("PUBLIC_BASE_URL не задан. Добавьте в .env PUBLIC_BASE_URL=https://your-domain")

GOOGLE_AUTH = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN = "https://oauth2.googleapis.com/token"

app = FastAPI(title="ScheduleBot OAuth Callback")

# ===== вспомогалки OAuth =====

def _load_client() -> Tuple[str, str]:
    """Читает client_id/secret из oauth-client.json (тип Web application)."""
    with open(GCAL_OAUTH_CLIENT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    web = data.get("web") or {}
    return web["client_id"], web["client_secret"]

def _build_auth_url(telegram_id: int) -> str:
    client_id, _ = _load_client()
    redirect_uri = f"{PUBLIC_BASE_URL}/oauth2/callback"
    scopes = " ".join(GCAL_SCOPES.split(","))
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": scopes,
        "access_type": "offline",              # хотим refresh_token
        "include_granted_scopes": "true",
        "prompt": "consent",                    # гарантирует refresh_token при повторных вызовах
        "state": str(telegram_id),
    }
    return f"{GOOGLE_AUTH}?{urllib.parse.urlencode(params)}"

def _exchange_code_for_tokens(code: str) -> Tuple[str, str | None, str]:
    """Меняет code на (access_token, refresh_token|None, expiry_iso)."""
    client_id, client_secret = _load_client()
    redirect_uri = f"{PUBLIC_BASE_URL}/oauth2/callback"
    data = {
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }
    r = requests.post(GOOGLE_TOKEN, data=data, timeout=15)
    r.raise_for_status()
    tok = r.json()
    access = tok["access_token"]
    refresh = tok.get("refresh_token")  # может отсутствовать, если не было prompt=consent/первый раз
    expires_in = int(tok.get("expires_in", 3600))
    # небольшой «зазор» на сеть
    expiry_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + expires_in - 30))
    return access, refresh, expiry_iso

# ===== странички =====

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/oauth2/connect")
def oauth_connect(state: str):
    """Редиректит пользователя на Google OAuth. state = telegram_id."""
    try:
        tid = int(state)
    except Exception:
        raise HTTPException(400, "Bad state")
    url = _build_auth_url(tid)
    return RedirectResponse(url)

@app.get("/oauth2/callback")
def oauth_callback(request: Request):
    """
    Обратный вызов от Google после согласия.
    Меняем code -> токены и сохраняем их в БД пользователя (по state=telegram_id).
    """
    params = dict(request.query_params)
    if "error" in params:
        return HTMLResponse(f"<h3>Google OAuth error: {params['error']}</h3>", status_code=400)

    code = params.get("code")
    state = params.get("state")
    if not code or not state:
        raise HTTPException(400, "Missing code/state")

    # 1) находим пользователя в нашей БД
    try:
        tid = int(state)
    except Exception:
        raise HTTPException(400, "Bad state")

    # ленивый импорт, чтобы файл был самодостаточным
    try:
        from app.services.db import get_user, set_gcal_tokens, set_gcal_connected  # type: ignore
    except Exception as e:
        return HTMLResponse(
            "<h3>Бэкенд не готов</h3>"
            "<p>Не найдены функции в БД: <code>get_user</code>, <code>set_gcal_tokens</code>, <code>set_gcal_connected</code>.</p>"
            f"<pre>{e}</pre>",
            status_code=500,
        )

    user = get_user(tid)
    if not user:
        return HTMLResponse("<h3>Пользователь не найден. Откройте бота и нажмите «Google Calendar» снова.</h3>", status_code=404)

    # 2) меняем code на токены
    try:
        access, refresh, expiry = _exchange_code_for_tokens(code)
    except Exception as e:
        return HTMLResponse(f"<h3>Не удалось обменять код на токены</h3><pre>{e}</pre>", status_code=400)

    # 3) сохраняем в БД
    try:
        set_gcal_tokens(tid, access, refresh, expiry)
        set_gcal_connected(tid, True)
    except Exception as e:
        return HTMLResponse(f"<h3>Не удалось сохранить токены</h3><pre>{e}</pre>", status_code=500)

    # 4) показываем аккуратную страничку успеха
    return HTMLResponse(
        "<h3>Google Calendar подключён ✅</h3>"
        "<p>Теперь вернитесь в Telegram-бот и нажмите «Синхронизировать».</p>",
        status_code=200,
    )
