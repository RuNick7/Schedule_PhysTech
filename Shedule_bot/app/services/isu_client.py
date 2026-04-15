from __future__ import annotations

import html as html_lib
import json
import logging
import re
import time
import urllib.parse
from html import unescape as html_unescape
from urllib.parse import unquote as url_unquote
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from app.services.myitmo_client import (
    _CLIENT_ID,
    _PROVIDER,
    _REDIRECT_URI,
    _generate_code_verifier,
    _get_code_challenge,
)

log = logging.getLogger("isu.client")

_ISU_BASE = "https://isu.ifmo.ru"
# Как в ITMOStalk: OAuth без PKCE my.itmo, сразу client_id=isu и редирект на SSO ИСУ
_ISU_PASSWORD_AUTH_URL = (
    "https://id.itmo.ru/auth/realms/itmo/protocol/openid-connect/auth"
    "?response_type=code&scope=openid&client_id=isu"
    "&redirect_uri=https://isu.ifmo.ru/api/sso/v1/public/login?apex_params=p=2143:LOGIN:"
)
_LOGIN_ACTION_RE = re.compile(r'"loginAction":\s*"(.+?)"', re.MULTILINE | re.DOTALL)
# Заголовки как у ITMOStalk (httpx + браузерный профиль)
_ISU_INDEXER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Upgrade-Insecure-Requests": "1",
}
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
        """
        Вход по паре логин/пароль — как в ITMOStalk:
        OAuth с client_id=isu, redirect на SSO ИСУ, POST на loginAction,
        редиректы обрабатываются вручную (без allow_redirects на POST).
        """
        sess = requests.Session()
        sess.headers.update(_ISU_INDEXER_HEADERS)
        if hasattr(sess, "trust_env"):
            sess.trust_env = False
        to = self._connect_read_timeouts()

        auth_page = sess.get(_ISU_PASSWORD_AUTH_URL, timeout=to)
        auth_page.raise_for_status()

        m = _LOGIN_ACTION_RE.search(auth_page.text or "")
        if not m:
            raise IsuSessionError(
                "Не удалось найти loginAction (Keycloak, client_id=isu / ITMOStalk)."
            )
        form_action = html_unescape(m.group(1))

        resp = sess.post(
            form_action,
            data={
                "username": username,
                "password": password,
                "rememberMe": "on",
                "credentialId": "",
            },
            cookies=auth_page.cookies,
            allow_redirects=False,
            timeout=to,
        )

        if resp.status_code != 302:
            if resp.status_code == 200 and _looks_like_isu_login_page(resp.text or ""):
                raise IsuSessionError(
                    "Неверный логин или пароль для ИСУ (ISU_INDEX_LOGIN / ISU_INDEX_PASSWORD)."
                )
            raise IsuSessionError(
                f"Вход в ИСУ: ожидался редирект 302, получен HTTP {resp.status_code}."
            )

        time.sleep(0.5)
        cur = resp
        hops = 0
        while cur.status_code in (301, 302, 303, 307, 308) and hops < 30:
            hops += 1
            loc = cur.headers.get("Location")
            if not loc:
                break
            next_url = urllib.parse.urljoin(str(cur.url), loc)
            cur = sess.get(next_url, allow_redirects=False, timeout=to)
        time.sleep(1.0)

        if cur.status_code in (301, 302, 303, 307, 308):
            raise IsuSessionError(
                "Слишком много редиректов или обрыв цепочки входа в ИСУ."
            )

        nonce = self._extract_nonce(str(cur.url))
        if not nonce:
            nonce = self._extract_nonce(cur.text or "")

        if not nonce:
            if _looks_like_isu_login_page(cur.text or ""):
                raise IsuSessionError(
                    "ИСУ вернул страницу входа. Проверьте ISU_INDEX_LOGIN и ISU_INDEX_PASSWORD."
                )
            raise IsuSessionError(
                "Не удалось получить nonce ИСУ после входа (поток как в ITMOStalk)."
            )

        self.session = sess
        self.nonce = nonce
        log.info(
            "ISU authenticated via password (ITMOStalk isu client flow), nonce=%s",
            nonce,
        )

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

def _extract_potok_id_from_string(s: str) -> Optional[int]:
    """Ищет ID учебного потока в фрагменте URL/onclick (в т.ч. URL-encoded)."""
    if not s:
        return None
    raw = html_lib.unescape(s)
    for text in (raw, url_unquote(raw.replace("&amp;", "&"))):
        for pat in (
            r"ID_POTOK:potok,(\d+)",
            r"ID_POTOK%3Apotok%2C(\d+)",
            r"GR_TYPE,ID_POTOK:potok,(\d+)",
            r"SCH,SCH_POTOK_ID,SCH_TYPE:\d+,(\d+),\d+",
            r"SCH_TYPE:\d+,(\d+),\d+",
        ):
            m = re.search(pat, text, re.I)
            if m:
                return int(m.group(1))
    return None


def _parse_potoks_itmostalk_dom(soup: BeautifulSoup) -> List[Tuple[int, str]]:
    """Дерево как в ITMOStalk: span.i_dummy>div.note и соседи с href."""
    current_tag = soup.select_one("span.i_dummy>div.note")
    if not current_tag:
        return []

    out: List[Tuple[int, str]] = []
    group_name = ""
    first_inner = current_tag.find()
    if first_inner:
        group_name = re.sub(r"\n +", " ", first_inner.get_text()).strip()
    else:
        group_name = re.sub(r"\s+", " ", current_tag.get_text()).strip()
    group_name = re.sub(r"^\[.+?\]\s*", "", group_name)

    current_group: List[Tuple[str, int]] = []
    used_group_labels: set[str] = set()

    while True:
        current_tag = current_tag.find_next_sibling()
        if not current_tag:
            break
        if current_tag.name == "div":
            if current_group:
                gk = group_name
                while gk in used_group_labels:
                    gk = gk + " "
                used_group_labels.add(gk)
                for pname, pid in current_group:
                    label = f"{gk} — {pname}".strip(" —")
                    out.append((pid, label))
                current_group = []
            inner = current_tag.find()
            group_name = (
                inner.get_text() if inner else current_tag.get_text()
            )
            group_name = re.sub(r"\n +", " ", group_name).strip()
            group_name = re.sub(r"^\[.+?\]\s*", "", group_name)
        else:
            href = current_tag.get("href")
            if not href:
                continue
            potok_id = _extract_potok_id_from_string(href)
            if potok_id is None:
                parts = href.split(",")
                if len(parts) >= 2:
                    cand = parts[-2].strip()
                    if cand.isdigit():
                        potok_id = int(cand)
            if potok_id is None:
                continue
            potok_name = re.sub(r"\s+", " ", current_tag.get_text(" ", strip=True))
            potok_name = re.sub(r"^\[.+?\]\s*", "", potok_name)
            current_group.append((potok_name, potok_id))

    if current_group:
        gk = group_name
        while gk in used_group_labels:
            gk = gk + " "
        used_group_labels.add(gk)
        for pname, pid in current_group:
            label = f"{gk} — {pname}".strip(" —")
            out.append((pid, label))

    return out


def _parse_potoks_from_all_attributes(soup: BeautifulSoup) -> List[Tuple[int, str]]:
    """href/onclick/data-* у любых тегов — потоки часто не в <a>."""
    out: List[Tuple[int, str]] = []
    for tag in soup.find_all(True):
        for attr in ("href", "onclick", "data-href", "data-url", "data-link"):
            val = tag.get(attr)
            if not val or not isinstance(val, str):
                continue
            pid = _extract_potok_id_from_string(val)
            if pid is None:
                continue
            name = re.sub(r"\s+", " ", tag.get_text(" ", strip=True))
            name = re.sub(r"^\[.+?\]\s*", "", name)
            if not name:
                name = f"Поток {pid}"
            out.append((pid, name))
    return out


def _parse_potoks_raw_regex(page_html: str) -> List[Tuple[int, str]]:
    """Последний шанс: все вхождения ID в сыром HTML (имя — placeholder)."""
    seen: set = set()
    out: List[Tuple[int, str]] = []
    for pat in (
        r"ID_POTOK:potok,(\d+)",
        r"ID_POTOK%3Apotok%2C(\d+)",
        r"GR_TYPE,ID_POTOK:potok,(\d+)",
        r"SCH,SCH_POTOK_ID,SCH_TYPE:\d+,(\d+),\d+",
    ):
        for m in re.finditer(pat, page_html, re.I):
            pid = int(m.group(1))
            if pid in seen:
                continue
            seen.add(pid)
            out.append((pid, f"Поток {pid}"))
    return out


def _parse_group_or_potok_list(
    page_html: str, list_type: str = "group"
) -> list:
    """
    Список групп/потоков с ИСУ. Раньше хватало <a href>; актуальная вёрстка
    (как в ITMOStalk) — mustache в span и href не только у <a>.
    """
    soup = BeautifulSoup(page_html, "lxml")
    results: list = []

    for tag in soup.find_all(True, href=True):
        href = tag.get("href") or ""
        if list_type == "group":
            m = re.search(r"GR_GR,GR_TYPE:([^,]+),group", href)
            if not m:
                continue
            name = re.sub(r"\s+", " ", tag.get_text(" ", strip=True))
            if not name:
                name = m.group(1)
            results.append((m.group(1), name))
        else:
            pid = _extract_potok_id_from_string(href)
            if pid is None:
                continue
            name = re.sub(r"\s+", " ", tag.get_text(" ", strip=True))
            name = re.sub(r"^\[.+?\]\s*", "", name)
            if not name:
                name = f"Поток {pid}"
            results.append((pid, name))

    if results:
        return _dedupe_group_or_potok(results)

    if list_type == "group":
        results = _parse_groups_mustache_spans(soup)
    else:
        results = _parse_potoks_itmostalk_dom(soup)
        if not results:
            results = _parse_potoks_from_all_attributes(soup)
        if not results:
            results = _parse_potoks_raw_regex(page_html)

    if not results and len(page_html or "") > 500:
        log.warning(
            "ISU list page parsed 0 %s (len=%d); markup may have changed",
            list_type,
            len(page_html),
        )
    return _dedupe_group_or_potok(results) if results else results


def _dedupe_group_or_potok(rows: list) -> list:
    seen: set = set()
    out = []
    for row in rows:
        key = row[0]
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _parse_groups_mustache_spans(soup: BeautifulSoup) -> List[Tuple[str, str]]:
    """Как ITMOStalk: JSON в span[data-mustache-template=template-group-group]."""
    out: List[Tuple[str, str]] = []
    for span in soup.select('span[data-mustache-template="template-group-group"]'):
        raw = span.get_text()
        text = re.sub(r"\n\s+", " ", html_lib.unescape(raw)).strip()
        try:
            j = json.loads(text)
        except json.JSONDecodeError:
            continue
        enc = j.get("groupEnc")
        label = (j.get("group") or "").strip()
        if enc:
            out.append((str(enc), label or str(enc)))
    return out


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
