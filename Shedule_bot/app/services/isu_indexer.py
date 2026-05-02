from __future__ import annotations

import asyncio
import logging
import time as _time
import traceback
from typing import Optional

import requests

from app.config import settings
from app.services.isu_client import (
    IsuSession,
    IsuSessionError,
    fetch_group_list,
    fetch_potok_list,
    fetch_students_for_group,
    fetch_students_for_potok,
)
from app.services.isu_db import (
    clear_potok_students,
    save_groups,
    save_potoks,
    save_potok_students,
    save_students_for_group,
    set_meta,
)

log = logging.getLogger("isu.indexer")

_isu_session: Optional[IsuSession] = None
_indexer_task: Optional[asyncio.Task] = None
_isu_throttle_seq: int = 0
_last_service_session_error: Optional[str] = None
_last_service_session_fail_ts: float = 0.0

_LOGIN_ATTEMPTS = 5
_SERVICE_SESSION_COOLDOWN_SEC: float = 120.0


def get_shared_isu_session() -> Optional[IsuSession]:
    """Returns the shared ISU session (may be None if not authenticated)."""
    return _isu_session


def get_last_service_isu_error() -> Optional[str]:
    """Текст последней ошибки входа/проверки сессии ИСУ (для сообщений пользователю)."""
    return _last_service_session_error


def _index_credentials() -> tuple[str, str]:
    login = (settings.isu_index_login or "").strip()
    password = (settings.isu_index_password or "").strip()
    return login, password


def _http_timeout_sec() -> int:
    return max(60, int(settings.isu_http_timeout_sec or 180))


async def _isu_throttle_before_request() -> None:
    """Паузы перед запросом к ИСУ (аналог ITMOStalk: меньше нагрузка — меньше обрывов)."""
    global _isu_throttle_seq
    _isu_throttle_seq += 1
    pause = max(0.0, float(settings.isu_index_request_pause_sec))
    if pause:
        await asyncio.sleep(pause)
    extra = float(settings.isu_index_extra_pause_sec or 0.0)
    if extra > 0 and _isu_throttle_seq % 2 == 0:
        await asyncio.sleep(extra)


async def _login_isu_with_retries() -> IsuSession:
    """Вход в ИСУ по паре ISU_INDEX_* с повторами при таймауте/обрыве."""
    login, password = _index_credentials()
    if not login or not password:
        raise IsuSessionError(
            "Не заданы ISU_INDEX_LOGIN и ISU_INDEX_PASSWORD в .env"
        )

    timeout = _http_timeout_sec()
    last_err: Exception | None = None
    for attempt in range(1, _LOGIN_ATTEMPTS + 1):
        try:
            isu = IsuSession(timeout=timeout)
            await asyncio.to_thread(isu.authenticate_by_password, login, password)
            return isu
        except IsuSessionError:
            raise
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, OSError) as e:
            last_err = e
            log.warning(
                "ISU login attempt %d/%d failed (%s): %s",
                attempt,
                _LOGIN_ATTEMPTS,
                type(e).__name__,
                e,
            )
            if attempt < _LOGIN_ATTEMPTS:
                await asyncio.sleep(min(45, 6 * attempt))
        except requests.exceptions.RequestException as e:
            last_err = e
            log.warning(
                "ISU login attempt %d/%d failed: %s", attempt, _LOGIN_ATTEMPTS, e
            )
            if attempt < _LOGIN_ATTEMPTS:
                await asyncio.sleep(min(45, 6 * attempt))

    msg = f"ИСУ недоступен после {_LOGIN_ATTEMPTS} попыток"
    if last_err:
        msg += f": {last_err}"
    raise IsuSessionError(msg)


async def get_service_isu_session() -> Optional[IsuSession]:
    """
    Сессия ИСУ для загрузки расписаний: общая с индексатором или новая по ISU_INDEX_*.
    Одна быстрая попытка без ретраев — при недоступном ИСУ падает за <2 сек,
    ставит circuit breaker на cooldown, и все остальные потоки fast-fail.
    """
    global _isu_session, _last_service_session_error, _last_service_session_fail_ts
    login, password = _index_credentials()
    if not login or not password:
        _last_service_session_error = None
        return None

    # Circuit breaker: не ретраим пока не истёк cooldown после последнего провала
    if _last_service_session_fail_ts:
        elapsed = _time.monotonic() - _last_service_session_fail_ts
        if elapsed < _SERVICE_SESSION_COOLDOWN_SEC:
            return None

    if _isu_session is not None and _isu_session.session is not None:
        try:
            resp = await asyncio.to_thread(
                _isu_session.get,
                f"https://isu.ifmo.ru/pls/apex/f?p=2143:1:{_isu_session.nonce}",
            )
            if resp.status_code == 200 and "2143" in resp.text:
                _last_service_session_error = None
                _last_service_session_fail_ts = 0.0
                return _isu_session
        except Exception:
            pass

    # Одна попытка без ретраев (ретраи — только в индексаторе через _ensure_session)
    try:
        isu = IsuSession(timeout=30)
        await asyncio.to_thread(isu.authenticate_by_password, login, password)
        _isu_session = isu
        _last_service_session_error = None
        _last_service_session_fail_ts = 0.0
        return isu
    except IsuSessionError as e:
        _last_service_session_error = str(e)
        _last_service_session_fail_ts = _time.monotonic()
        log.warning("get_service_isu_session: %s", e)
        return None
    except Exception as e:
        _last_service_session_error = str(e)
        _last_service_session_fail_ts = _time.monotonic()
        log.warning("get_service_isu_session: auth failed: %s", e)
        return None


def start_isu_indexer() -> None:
    global _indexer_task
    _indexer_task = asyncio.ensure_future(_indexer_loop())
    log.info("ISU indexer task scheduled")


async def _ensure_session() -> None:
    global _isu_session, _last_service_session_error, _last_service_session_fail_ts
    login, password = _index_credentials()
    if not login or not password:
        raise IsuSessionError(
            "Не заданы ISU_INDEX_LOGIN и ISU_INDEX_PASSWORD в .env"
        )

    if _isu_session is not None and _isu_session.session is not None:
        try:
            resp = await asyncio.to_thread(
                _isu_session.get,
                f"https://isu.ifmo.ru/pls/apex/f?p=2143:1:{_isu_session.nonce}",
            )
            if resp.status_code == 200 and "2143" in resp.text:
                return
        except Exception:
            pass

    try:
        _isu_session = await _login_isu_with_retries()
        _last_service_session_error = None
        _last_service_session_fail_ts = 0.0
    except IsuSessionError as e:
        _last_service_session_error = str(e)
        _last_service_session_fail_ts = _time.monotonic()
        raise


async def _indexer_loop() -> None:
    delay = max(1.0, settings.isu_index_delay)
    reindex_interval = max(3600, int(settings.isu_reindex_interval_sec))
    startup_retry = 120

    while True:
        login, password = _index_credentials()
        if not login or not password:
            set_meta("indexer_status", "waiting_credentials")
            set_meta(
                "last_error",
                "Укажите ISU_INDEX_LOGIN и ISU_INDEX_PASSWORD в .env",
            )
            log.info(
                "ISU_INDEX_LOGIN / ISU_INDEX_PASSWORD not set, retry in %ds",
                startup_retry,
            )
            await asyncio.sleep(startup_retry)
            continue

        try:
            set_meta("indexer_status", "authenticating")
            await _ensure_session()
            assert _isu_session is not None

            global _isu_throttle_seq
            _isu_throttle_seq = 0

            set_meta("indexer_status", "fetching_groups")
            await _isu_throttle_before_request()
            groups = await asyncio.to_thread(fetch_group_list, _isu_session)
            save_groups(groups)
            log.info("Indexed %d groups", len(groups))
            await asyncio.sleep(delay)

            set_meta("indexer_status", "fetching_potoks")
            await _isu_throttle_before_request()
            potoks = await asyncio.to_thread(fetch_potok_list, _isu_session)
            save_potoks(potoks)
            log.info("Indexed %d potoks", len(potoks))
            await asyncio.sleep(delay)

            set_meta("indexer_status", "indexing_potoks")
            await asyncio.to_thread(clear_potok_students)
            backoff = delay
            for idx, (potok_id, potok_name) in enumerate(potoks, 1):
                try:
                    await _isu_throttle_before_request()
                    students = await asyncio.to_thread(
                        fetch_students_for_potok, _isu_session, potok_id
                    )
                    await asyncio.to_thread(
                        save_potok_students, potok_id, potok_name, students
                    )
                    backoff = delay
                    if idx % 50 == 0:
                        log.info(
                            "Potok member index progress: %d/%d potoks",
                            idx,
                            len(potoks),
                        )
                except (ConnectionError, OSError, requests.exceptions.Timeout) as e:
                    log.warning(
                        "Network/rate issue at potok %d/%d (%s), backing off %.0fs: %s",
                        idx, len(potoks), potok_name, backoff, e,
                    )
                    set_meta("last_error", f"potok {idx}: {e}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 120)
                    await _ensure_session()
                    try:
                        await _isu_throttle_before_request()
                        students = await asyncio.to_thread(
                            fetch_students_for_potok, _isu_session, potok_id
                        )
                        await asyncio.to_thread(
                            save_potok_students, potok_id, potok_name, students
                        )
                    except Exception:
                        log.warning("Retry failed for potok %s, skipping", potok_name)

                await asyncio.sleep(delay)

            set_meta("indexer_status", "indexing_students")
            backoff = delay
            for idx, (group_enc, group_name) in enumerate(groups, 1):
                try:
                    await _isu_throttle_before_request()
                    students = await asyncio.to_thread(
                        fetch_students_for_group, _isu_session, group_enc
                    )
                    save_students_for_group(group_enc, group_name, students)
                    backoff = delay
                    if idx % 50 == 0:
                        log.info(
                            "Student index progress: %d/%d groups",
                            idx,
                            len(groups),
                        )
                except (ConnectionError, OSError, requests.exceptions.Timeout) as e:
                    log.warning(
                        "Network/rate issue at group %d/%d (%s), backing off %.0fs: %s",
                        idx, len(groups), group_name, backoff, e,
                    )
                    set_meta("last_error", f"group {idx}: {e}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 120)

                    await _ensure_session()
                    try:
                        await _isu_throttle_before_request()
                        students = await asyncio.to_thread(
                            fetch_students_for_group, _isu_session, group_enc
                        )
                        save_students_for_group(group_enc, group_name, students)
                    except Exception:
                        log.warning("Retry failed for group %s, skipping", group_name)

                await asyncio.sleep(delay)

            set_meta("indexer_status", "idle")
            set_meta("last_error", "")
            log.info("ISU indexing complete, sleeping %ds", reindex_interval)

        except IsuSessionError as e:
            set_meta("indexer_status", "error")
            set_meta("last_error", str(e))
            log.error("ISU session error: %s", e)
        except Exception:
            set_meta("indexer_status", "error")
            set_meta("last_error", traceback.format_exc()[-200:])
            log.exception("ISU indexer error")

        await asyncio.sleep(reindex_interval)
