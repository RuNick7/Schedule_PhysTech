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
    get_meta,
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


def start_isu_indexer() -> None:
    """Launch background indexer. Uses the first available user with my.itmo connected."""
    global _indexer_task
    _indexer_task = asyncio.ensure_future(_indexer_loop())
    log.info("ISU indexer task scheduled")


async def _get_refresh_token() -> Optional[str]:
    """Find any user with a valid my.itmo refresh_token in the DB."""
    from app.services.db import get_any_myitmo_user
    user = await asyncio.to_thread(get_any_myitmo_user)
    if user and user.get("myitmo_refresh_token"):
        return user["myitmo_refresh_token"]
    return None


async def _indexer_loop() -> None:
    delay = max(1.0, settings.isu_index_delay)
    reindex_interval = 24 * 3600
    startup_retry = 60

    while True:
        refresh_token = await _get_refresh_token()
        if not refresh_token:
            set_meta("indexer_status", "waiting_for_user")
            log.info(
                "No user with my.itmo connected yet, "
                "ISU indexer will retry in %ds", startup_retry,
            )
            await asyncio.sleep(startup_retry)
            continue

        try:
            set_meta("indexer_status", "authenticating")
            await _ensure_session(refresh_token)
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

                    refresh_token = await _get_refresh_token() or refresh_token
                    await _ensure_session(refresh_token)
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


async def _ensure_session(refresh_token: str) -> None:
    global _isu_session
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
    await asyncio.to_thread(isu.authenticate_by_token, refresh_token)
    _isu_session = isu
