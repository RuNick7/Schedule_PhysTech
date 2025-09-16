# app/cron/gcal_autosync.py
from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone
from app.handlers.gcal_sync import _sync_next_days_for_user
from app.config import settings
from app.services.db import (
    list_users_gcal_autosync_enabled,
    set_gcal_autosync_last_key,
)
from app.utils.dt import now_tz  # если у тебя другая утилита — используй её
from app.handlers.gcal_sync import _sync_today_for_user, _sync_week_for_user  # добавим ниже
from app.handlers.gcal_sync import _sync_two_weeks_for_user
log = logging.getLogger("gcal.autosync")

def _year_week(dt) -> str:
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"

async def gcal_autosync_tick(bot):
    users = list_users_gcal_autosync_enabled()   # твоя функция, которая отдаёт включённых
    if not users:
        return
    for u in users:
        try:
            tz = u.get("timezone") or settings.timezone
            now_local = now_tz(tz)
            hhmm = now_local.strftime("%H:%M")
            if hhmm != (u.get("gcal_autosync_time") or ""):
                continue

            mode = (u.get("gcal_autosync_mode") or "weekly").lower()
            last_key = u.get("gcal_autosync_last_key") or ""
            uid = u["telegram_id"]

            if mode == "daily":
                key = f"daily:{now_local.strftime('%Y-%m-%d')}"
                if key == last_key:
                    continue
                ok, fail = await _sync_today_for_user(uid)   # как у тебя было
                set_gcal_autosync_last_key(uid, key)
                log.info("autosync daily user=%s ok=%d fail=%d", uid, ok, fail)

            else:  # weekly => КАЖДЫЙ ДЕНЬ, две недели
                key = f"weekly2daily:{now_local.strftime('%Y-%m-%d')}"  # раз в день
                if key == last_key:
                    continue
                ok, fail = await _sync_two_weeks_for_user(uid)
                set_gcal_autosync_last_key(uid, key)
                log.info("autosync weekly(2w daily) user=%s ok=%d fail=%d", uid, ok, fail)

        except Exception:
            log.exception("autosync tick failed for user=%s", u.get("telegram_id"))
