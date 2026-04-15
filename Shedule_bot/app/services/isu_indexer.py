from __future__ import annotations

import asyncio
import logging
import traceback
from typing import Optional

from app.config import settings
from app.services.isu_client import (
    IsuSession,
    IsuSessionError,
    fetch_group_list,
    fetch_potok_list,
    fetch_students_for_group,
)
from app.services.isu_db import (
    save_groups,
    save_potoks,
    save_students_for_group,
    set_meta,
)

log = logging.getLogger("isu.indexer")

_isu_session: Optional[IsuSession] = None
_indexer_task: Optional[asyncio.Task] = None


def get_shared_isu_session() -> Optional[IsuSession]:
    """Returns the shared ISU session (may be None if not authenticated)."""
    return _isu_session


def _index_credentials() -> tuple[str, str]:
    login = (settings.isu_index_login or "").strip()
    password = (settings.isu_index_password or "").strip()
    return login, password


async def get_service_isu_session() -> Optional[IsuSession]:
    """
    Сессия ИСУ для загрузки расписаний: общая с индексатором или новая по ISU_INDEX_*.
    """
    global _isu_session
    login, password = _index_credentials()
    if not login or not password:
        return None

    if _isu_session is not None and _isu_session.session is not None:
        try:
            resp = await asyncio.to_thread(
                _isu_session.get,
                f"https://isu.ifmo.ru/pls/apex/f?p=2143:1:{_isu_session.nonce}",
            )
            if resp.status_code == 200 and "2143" in resp.text:
                return _isu_session
        except Exception:
            pass

    try:
        isu = IsuSession(timeout=30)
        await asyncio.to_thread(isu.authenticate_by_password, login, password)
        _isu_session = isu
        return isu
    except Exception as e:
        log.warning("get_service_isu_session: auth failed: %s", e)
        return None


def start_isu_indexer() -> None:
    global _indexer_task
    _indexer_task = asyncio.ensure_future(_indexer_loop())
    log.info("ISU indexer task scheduled")


async def _ensure_session() -> None:
    global _isu_session
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

    isu = IsuSession(timeout=30)
    await asyncio.to_thread(isu.authenticate_by_password, login, password)
    _isu_session = isu


async def _indexer_loop() -> None:
    delay = max(1.0, settings.isu_index_delay)
    reindex_interval = 24 * 3600
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

            set_meta("indexer_status", "fetching_groups")
            groups = await asyncio.to_thread(fetch_group_list, _isu_session)
            save_groups(groups)
            log.info("Indexed %d groups", len(groups))
            await asyncio.sleep(delay)

            set_meta("indexer_status", "fetching_potoks")
            potoks = await asyncio.to_thread(fetch_potok_list, _isu_session)
            save_potoks(potoks)
            log.info("Indexed %d potoks", len(potoks))
            await asyncio.sleep(delay)

            set_meta("indexer_status", "indexing_students")
            backoff = delay
            for idx, (group_enc, group_name) in enumerate(groups, 1):
                try:
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
                except (ConnectionError, OSError) as e:
                    log.warning(
                        "Rate-limited at group %d/%d (%s), backing off %.0fs",
                        idx, len(groups), group_name, backoff,
                    )
                    set_meta("last_error", f"rate-limit at group {idx}: {e}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 120)

                    await _ensure_session()
                    try:
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
