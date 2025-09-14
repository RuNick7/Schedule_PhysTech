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
            hhmm = now_local.strftime("%H:%M")
            if hhmm != (u.get("gcal_autosync_time") or ""):
                continue

            mode = (u.get("gcal_autosync_mode") or "daily").lower()
            last_key = u.get("gcal_autosync_last_key") or ""
            uid = u["telegram_id"]

            if mode == "weekly":
                wday = int(u.get("gcal_autosync_weekday") if u.get("gcal_autosync_weekday") is not None else 0)
                if now_local.weekday() != wday:
                    continue
                key = f"weekly:{_year_week(now_local)}"
                if key == last_key:
                    continue
                ok, fail = await _sync_week_for_user(uid, weeks_ahead=0)
                set_gcal_autosync_last_key(uid, key)
                log.info("autosync weekly user=%s ok=%d fail=%d", uid, ok, fail)

            elif mode == "weekly2":  # <-- НОВОЕ
                wday = int(u.get("gcal_autosync_weekday") if u.get("gcal_autosync_weekday") is not None else 0)
                if now_local.weekday() != wday:
                    continue
                key = f"weekly2:{_year_week(now_local)}"   # один раз на базовую неделю
                if key == last_key:
                    continue
                ok1, fail1 = await _sync_week_for_user(uid, weeks_ahead=0)  # текущая
                ok2, fail2 = await _sync_week_for_user(uid, weeks_ahead=1)  # следующая
                set_gcal_autosync_last_key(uid, key)
                log.info("autosync weekly2 user=%s ok=%d fail=%d (w0:%d/%d, w1:%d/%d)",
                         uid, ok1+ok2, fail1+fail2, ok1, fail1, ok2, fail2)

            elif mode == "rolling7":
                key = f"rolling7:{now_local.strftime('%Y-%m-%d')}"
                if key == last_key:
                    continue
                ok, fail = await _sync_next_days_for_user(uid, days=7)
                set_gcal_autosync_last_key(uid, key)
                log.info("autosync rolling7 user=%s ok=%d fail=%d", uid, ok, fail)

            else:  # daily
                key = f"daily:{now_local.strftime('%Y-%m-%d')}"
                if key == last_key:
                    continue
                ok, fail = await _sync_today_for_user(uid)
                set_gcal_autosync_last_key(uid, key)
                log.info("autosync daily user=%s ok=%d fail=%d", uid, ok, fail)

        except Exception:
            log.exception("autosync tick failed for user=%s", u.get("telegram_id"))
