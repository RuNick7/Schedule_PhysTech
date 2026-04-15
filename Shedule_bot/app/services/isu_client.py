from __future__ import annotations

import html as html_lib
import logging
import re
import urllib.parse
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from app.services.myitmo_client import (
    _CLIENT_ID,
    _PROVIDER,
    _REDIRECT_URI,
    _extract_login_action,
    _generate_code_verifier,
    _get_code_challenge,
)

log = logging.getLogger("isu.client")

_ISU_BASE = "https://isu.ifmo.ru"
# Nonce в ссылках APEX: f?p=2143:9:1234567890::...
_NONCE_RE = re.compile(r"f\?p=2143:\d+:(\d{8,})", re.I)


def _looks_like_isu_login_page(html: str) -> bool:
    low = (html or "").lower()
    if "kc-form-login" in low or 'name="username"' in low:
        return True
    if "парол" in low and ("вход" in low or "login" in low):
        return True
    return False


class IsuSessionError(RuntimeError):
    pass


class IsuSession:
    """Authenticated ISU session with nonce for APEX pages."""

    def __init__(self, timeout: int = 180):
        self._timeout = timeout
        self.session: Optional[requests.Session] = None
        self.nonce: Optional[str] = None

    def _connect_read_timeouts(self) -> Tuple[float, float]:
        """Короткий connect, длинный read — типичный ReadTimeout на стороне ИСУ."""
        read = float(max(30, int(self._timeout)))
        connect = min(60.0, max(12.0, read / 5.0))
        return (connect, read)

    def authenticate_by_token(self, refresh_token: str) -> None:
        """
        Establish ISU session using a my.itmo refresh_token.
        Steps:
        1. Refresh the token via Keycloak (establishes SSO cookies for id.itmo.ru)
        2. Start OAuth2 PKCE — Keycloak may auto-approve and redirect to
           my.itmo.ru/login/callback?code=...
        3. Exchange authorization_code for tokens in the same session (required
           for a complete browser-like session; without this ISU often stays logged out)
        4. Open ISU and extract the APEX nonce from the page or redirects
        """
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        })

        token_resp = sess.post(
            f"{_PROVIDER}/protocol/openid-connect/token",
            data={
                "grant_type": "refresh_token",
                "client_id": _CLIENT_ID,
                "refresh_token": refresh_token,
            },
            timeout=self._connect_read_timeouts(),
        )
        token_resp.raise_for_status()
        token_data = token_resp.json()
        new_refresh = token_data.get("refresh_token") or refresh_token

        code_verifier = _generate_code_verifier()
        code_challenge = _get_code_challenge(code_verifier)

        auth_resp = sess.get(
            f"{_PROVIDER}/protocol/openid-connect/auth",
            params={
                "protocol": "oauth2",
                "response_type": "code",
                "client_id": _CLIENT_ID,
                "redirect_uri": _REDIRECT_URI,
                "scope": "openid",
                "state": "isu-token",
                "code_challenge_method": "S256",
                "code_challenge": code_challenge,
            },
            allow_redirects=True,
            timeout=self._connect_read_timeouts(),
        )

        if "loginAction" in (auth_resp.text or "") and "code=" not in str(auth_resp.url):
            raise IsuSessionError(
                "Keycloak требует пароль — сессии refresh_token недостаточно. "
                "Откройте Настройки → my.itmo и подключите аккаунт заново."
            )

        final_url = str(auth_resp.url)
        parsed = urllib.parse.urlparse(final_url)
        qs = urllib.parse.parse_qs(parsed.query)
        auth_code = (qs.get("code") or [None])[0]
        if not auth_code:
            m_code = re.search(r"[?&]code=([^&]+)", final_url)
            if m_code:
                auth_code = urllib.parse.unquote(m_code.group(1))

        if auth_code:
            exch = sess.post(
                f"{_PROVIDER}/protocol/openid-connect/token",
                data={
                    "grant_type": "authorization_code",
                    "client_id": _CLIENT_ID,
                    "redirect_uri": _REDIRECT_URI,
                    "code": auth_code,
                    "code_verifier": code_verifier,
                },
                allow_redirects=False,
                timeout=self._connect_read_timeouts(),
            )
            exch.raise_for_status()
            exch_data = exch.json()
            new_refresh = exch_data.get("refresh_token") or new_refresh

        # Закрепить сессию my.itmo (часто нужно перед переходом в ИСУ)
        try:
            sess.get("https://my.itmo.ru/", allow_redirects=True, timeout=self._connect_read_timeouts())
        except Exception:
            pass

        isu_resp = sess.get(
            f"{_ISU_BASE}/pls/apex/f?p=2143:1",
            allow_redirects=True,
            timeout=self._connect_read_timeouts(),
        )
        isu_resp.raise_for_status()

        html_text = isu_resp.text or ""
        if _looks_like_isu_login_page(html_text) and not _NONCE_RE.search(html_text):
            raise IsuSessionError(
                "ИСУ вернул страницу входа. Подключите my.itmo в настройках бота заново."
            )

        nonce = self._extract_nonce(str(isu_resp.url))
        if not nonce:
            nonce = self._extract_nonce(html_text)
        if not nonce:
            raise IsuSessionError(
                "Не удалось получить nonce ИСУ после входа. "
                "Попробуйте отключить и снова подключить my.itmo в настройках."
            )

        self.session = sess
        self.nonce = nonce
        self._refresh_token = new_refresh
        log.info("ISU authenticated via refresh_token, nonce=%s", nonce)

    def authenticate_by_password(self, username: str, password: str) -> None:
        """Fallback: full OAuth2 PKCE with username+password."""
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": "Mozilla/5.0 (schedule-bot ISU indexer)",
            "Accept-Language": "ru-RU,ru;q=0.9",
        })

        code_verifier = _generate_code_verifier()
        code_challenge = _get_code_challenge(code_verifier)

        auth_resp = sess.get(
            f"{_PROVIDER}/protocol/openid-connect/auth",
            params={
                "protocol": "oauth2",
                "response_type": "code",
                "client_id": _CLIENT_ID,
                "redirect_uri": _REDIRECT_URI,
                "scope": "openid",
                "state": "isu-indexer",
                "code_challenge_method": "S256",
                "code_challenge": code_challenge,
            },
            timeout=self._connect_read_timeouts(),
        )
        auth_resp.raise_for_status()

        form_action = _extract_login_action(auth_resp.text)
        sess.post(
            url=form_action,
            data={"username": username, "password": password},
            cookies=auth_resp.cookies,
            allow_redirects=True,
            timeout=self._connect_read_timeouts(),
        )

        isu_resp = sess.get(
            f"{_ISU_BASE}/pls/apex/f?p=2143:1",
            allow_redirects=True,
            timeout=self._connect_read_timeouts(),
        )
        isu_resp.raise_for_status()

        nonce = self._extract_nonce(str(isu_resp.url))
        if not nonce:
            nonce = self._extract_nonce(isu_resp.text)
        if not nonce:
            raise IsuSessionError("Failed to extract ISU nonce after password auth")

        self.session = sess
        self.nonce = nonce
        log.info("ISU authenticated via password, nonce=%s", nonce)

    @staticmethod
    def _extract_nonce(text: str) -> Optional[str]:
        if not text:
            return None
        m = _NONCE_RE.search(text)
        return m.group(1) if m else None

    def get(self, url: str, **kwargs) -> requests.Response:
        if not self.session:
            raise IsuSessionError("Not authenticated")
        kwargs.setdefault("timeout", self._connect_read_timeouts())
        return self.session.get(url, **kwargs)


def fetch_group_list(isu: IsuSession) -> List[Tuple[str, str]]:
    """Returns list of (group_enc, group_name) from ISU."""
    url = f"{_ISU_BASE}/pls/apex/f?p=2143:9:{isu.nonce}::NO::P9_GR_TYPE:group"
    resp = isu.get(url)
    resp.raise_for_status()
    return _parse_group_or_potok_list(resp.text, list_type="group")


def fetch_potok_list(isu: IsuSession) -> List[Tuple[int, str]]:
    """Returns list of (potok_id, potok_name) from ISU."""
    url = f"{_ISU_BASE}/pls/apex/f?p=2143:9:{isu.nonce}::NO::P9_GR_TYPE:potok"
    resp = isu.get(url)
    resp.raise_for_status()
    return _parse_group_or_potok_list(resp.text, list_type="potok")


def fetch_students_for_group(
    isu: IsuSession, group_enc: str
) -> List[Tuple[int, str]]:
    """Returns list of (student_id, student_name) for a group."""
    url = (
        f"{_ISU_BASE}/pls/apex/f?p=2143:GR:{isu.nonce}"
        f"::NO::GR_GR,GR_TYPE:{group_enc},group"
    )
    resp = isu.get(url)
    resp.raise_for_status()
    return _parse_student_list(resp.text)


def fetch_potok_schedule_html(isu: IsuSession, potok_id: int) -> str:
    """Fetches the raw schedule page HTML for a potok."""
    url = (
        f"{_ISU_BASE}/pls/apex/f?p=2143:15:{isu.nonce}"
        f"::NO::SCH,SCH_POTOK_ID,SCH_TYPE:1,{potok_id},5"
    )
    resp = isu.get(url)
    resp.raise_for_status()
    return resp.text


# ── HTML parsers ────────────────────────────────────────────────────────

def _parse_group_or_potok_list(
    page_html: str, list_type: str = "group"
) -> list:
    soup = BeautifulSoup(page_html, "lxml")
    results = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        name = re.sub(r"\s+", " ", a_tag.get_text(" ", strip=True))
        if not name:
            continue

        if list_type == "group":
            m = re.search(r"GR_GR,GR_TYPE:([^,]+),group", href)
            if m:
                results.append((m.group(1), name))
        else:
            m = re.search(r"ID_POTOK:potok,(\d+)", href)
            if m:
                results.append((int(m.group(1)), name))

    return results


def _parse_student_list(page_html: str) -> List[Tuple[int, str]]:
    soup = BeautifulSoup(page_html, "lxml")
    students: List[Tuple[int, str]] = []
    seen_ids: set = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        m = re.search(r"PERS_ID:(\d+)", href)
        if not m:
            continue
        sid = int(m.group(1))
        if sid in seen_ids:
            continue
        seen_ids.add(sid)
        name = re.sub(r"\s+", " ", a_tag.get_text(" ", strip=True))
        if name:
            students.append((sid, html_lib.unescape(name)))

    if not students:
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            first_text = tds[0].get_text(strip=True)
            second_text = tds[1].get_text(strip=True)
            try:
                sid = int(first_text)
            except ValueError:
                try:
                    sid = int(second_text)
                except ValueError:
                    continue
            if sid in seen_ids:
                continue
            seen_ids.add(sid)
            name_parts = []
            for td in tds:
                t = td.get_text(strip=True)
                if t and not t.isdigit():
                    name_parts.append(t)
            if name_parts:
                students.append((sid, html_lib.unescape(" ".join(name_parts))))

    return students
