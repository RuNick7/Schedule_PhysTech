# app/cron/gcal_autosync.py
from __future__ import annotations
import asyncio
import logging
import timedelta
from datetime import datetime, timezone
from app.handlers.gcal_sync import _sync_next_days_for_user
from app.services.gcal_client import delete_events_by_tag_between
from app.config import settings
from app.services.db import (
    list_users_gcal_autosync_enabled,
    set_gcal_autosync_last_key,
    set_gcal_last_sync,
)
from app.utils.dt import now_tz  # если у тебя другая утилита — используй её
from app.handlers.gcal_sync import _sync_week_for_user, _load_lessons_for_user_group
log = logging.getLogger("gcal.autosync")

def _year_week(dt) -> str:
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"

async def gcal_autosync_tick(bot):
    users = list_users_gcal_autosync_enabled()
    if not users:
        return

    for u in users:
        try:
            tz = u.get("timezone") or settings.timezone
            now_local = now_tz(tz)
            hhmm_now = now_local.strftime("%H:%M")
            target = (u.get("gcal_autosync_time") or "").strip()
            if not target or hhmm_now != target:
                continue

            mode = (u.get("gcal_autosync_mode") or "weekly").lower()
            ymd = now_local.strftime("%Y-%m-%d")
            last_key = u.get("gcal_autosync_last_key") or ""
            run_key = f"{'daily' if mode=='daily' else 'two_weeks'}:{ymd}"
            if run_key == last_key:
                continue

            if not u.get("gcal_connected"):
                continue

            uid = u["telegram_id"]
            cal_id = u.get("gcal_calendar_id") or "primary"

            if mode == "daily":
                # как раньше (оставляешь свой код или готовую функцию sync_today_for_user)
                # ok, fail = await sync_today_for_user(uid)
                # при желании можно чистить окно только сегодняшнего дня:
                # start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
                # end_local   = (start_local + timedelta(days=1))
                # await asyncio.to_thread(delete_events_by_tag_between, uid, cal_id, "sched_bot", "1", start_local.isoformat(), end_local.isoformat())
                ok, fail = 0, 0
            else:
                # === Чистка окна текущая+следующая недели ===
                base = now_local
                monday = base - timedelta(days=base.weekday())
                start_local = monday.replace(hour=0, minute=0, second=0, microsecond=0)
                end_local = (monday + timedelta(days=14)).replace(hour=0, minute=0, second=0, microsecond=0)

                deleted = await asyncio.to_thread(
                    delete_events_by_tag_between,
                    uid,
                    cal_id,
                    "sched_bot",
                    "1",
                    start_local.isoformat(),
                    end_local.isoformat(),
                )
                log.info(
                    "gcal autosync pre-clean user=%s cal=%s deleted=%d window=[%s..%s)",
                    uid, cal_id, deleted, start_local.isoformat(), end_local.isoformat()
                )

                # === Тот же пайплайн, что у кнопки: 0-я и 1-я недели ===
                lessons = await _load_lessons_for_user_group(u)
                ok1, fail1 = await _sync_week_for_user({**u, "telegram_id": uid}, lessons, weeks_ahead=0)
                ok2, fail2 = await _sync_week_for_user({**u, "telegram_id": uid}, lessons, weeks_ahead=1)
                ok, fail = ok1 + ok2, fail1 + fail2

                set_gcal_last_sync(uid, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

            set_gcal_autosync_last_key(uid, run_key)
            log.info("gcal autosync user=%s mode=%s ok=%d fail=%d", uid, mode, ok, fail)

        except Exception:
            log.exception("autosync tick failed for user=%s", u.get("telegram_id"))