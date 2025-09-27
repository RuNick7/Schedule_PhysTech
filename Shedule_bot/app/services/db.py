# app/services/db.py
import os
import sqlite3
import datetime
from typing import Optional, Dict, Any
import re

from app.config import settings
from typing import List, Dict, Any, Optional

DB_PATH = settings.db_path  # ./app/data/bot.db

_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")

def _get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    """Создать таблицу users (если её ещё нет)."""
    with _get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                telegram_tag TEXT,
                message_id INTEGER,
                type INTEGER DEFAULT 0,
                timezone TEXT DEFAULT 'Europe/Moscow',
                course INTEGER,
                group_code TEXT,

                autosend_enabled INTEGER NOT NULL DEFAULT 0,
                autosend_mode INTEGER,
                autosend_time TEXT,
                autosend_last_date TEXT,

                autosend_msg_id INTEGER,
                autosend_cur_key TEXT,
                
                gcal_connected INTEGER DEFAULT 0,
                gcal_access_token TEXT,
                gcal_refresh_token TEXT,
                gcal_token_expiry TEXT,
                gcal_calendar_id TEXT,
                gcal_last_sync TEXT
            )
            """
        )
        conn.commit()

def get_user(telegram_id: int) -> Optional[Dict[str, Any]]:
    with _conn() as con:
        cur = con.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def upsert_user(telegram_id: int, telegram_tag: Optional[str]) -> None:
    with _conn() as con:
        con.execute(
            """
            INSERT INTO users(telegram_id, telegram_tag)
            VALUES (?, ?)
            ON CONFLICT(telegram_id)
            DO UPDATE SET telegram_tag = COALESCE(excluded.telegram_tag, users.telegram_tag)
            """,
            (telegram_id, telegram_tag),
        )

def set_message_id(telegram_id: int, message_id: Optional[int]) -> None:
    with _conn() as con:
        con.execute("UPDATE users SET message_id = ? WHERE telegram_id = ?", (message_id, telegram_id))

def set_type(telegram_id: int, value: int) -> None:
    with _conn() as con:
        con.execute("UPDATE users SET type = ? WHERE telegram_id = ?", (value, telegram_id))

def set_timezone(telegram_id: int, tz: str) -> None:
    with _conn() as con:
        con.execute("UPDATE users SET timezone = ? WHERE telegram_id = ?", (tz, telegram_id))

def set_course(telegram_id: int, course: int) -> None:
    with _conn() as con:
        con.execute("UPDATE users SET course = ? WHERE telegram_id = ?", (course, telegram_id))

def set_group(telegram_id: int, group_code: str) -> None:
    with _conn() as con:
        con.execute("UPDATE users SET group_code = ? WHERE telegram_id = ?", (group_code, telegram_id))

def _ensure_user_exists(conn: sqlite3.Connection, telegram_id: int):
    cur = conn.execute("SELECT 1 FROM users WHERE telegram_id = ?", (telegram_id,))
    if not cur.fetchone():
        conn.execute("INSERT INTO users(telegram_id) VALUES (?)", (telegram_id,))

def set_autosend_enabled(telegram_id: int, enabled: bool):
    """
    Включить/выключить автоотправку.
    """
    with _get_conn() as conn:
        _ensure_user_exists(conn, telegram_id)
        conn.execute(
            "UPDATE users SET autosend_enabled = ? WHERE telegram_id = ?",
            (1 if enabled else 0, telegram_id),
        )
        conn.commit()

def set_autosend_mode(telegram_id: int, mode: int):
    """
    Установить режим автоотправки:
      1 — утром отправляем расписание на день
      2 — утром ближайшая пара и потом автообновление
    """
    if mode not in (1, 2):
        raise ValueError("autosend_mode must be 1 or 2")
    with _get_conn() as conn:
        _ensure_user_exists(conn, telegram_id)
        conn.execute(
            "UPDATE users SET autosend_mode = ?, autosend_enabled = COALESCE(autosend_enabled, 0) WHERE telegram_id = ?",
            (mode, telegram_id),
        )
        conn.commit()

def set_gcal_calendar_id(telegram_id: int, cal_id: Optional[str]):
    """
    Сохраняет выбранный пользователем calendarId (или None для сброса).
    """
    with _get_conn() as conn:
        _ensure_user_exists(conn, telegram_id)
        conn.execute(
            "UPDATE users SET gcal_calendar_id = ? WHERE telegram_id = ?",
            (cal_id, telegram_id),
        )
        conn.commit()

def set_autosend_time(telegram_id: int, hhmm: str):
    """
    Установить время автоотправки в формате 'HH:MM' (24ч).
    """
    if not _TIME_RE.match(hhmm or ""):
        raise ValueError("autosend_time must be in 'HH:MM' 24h format")
    with _get_conn() as conn:
        _ensure_user_exists(conn, telegram_id)
        conn.execute(
            "UPDATE users SET autosend_time = ? WHERE telegram_id = ?",
            (hhmm, telegram_id),
        )
        conn.commit()

def list_users_for_autosend_at(hhmm: str) -> list[Dict[str, Any]]:
    """
    Пользователи, кому слать прямо сейчас:
      - autosend_enabled = 1
      - autosend_mode = 1
      - autosend_time == hhmm
      - group_code заполнен
    """
    with _get_conn() as conn:
        cur = conn.execute(
            """
            SELECT * FROM users
            WHERE autosend_enabled = 1
              AND autosend_mode = 1
              AND autosend_time = ?
              AND group_code IS NOT NULL
              AND group_code <> ''
            """,
            (hhmm,),
        )
        return [dict(r) for r in cur.fetchall()]

def get_autosend_last_date(telegram_id: int) -> Optional[str]:
    with _get_conn() as conn:
        cur = conn.execute("SELECT autosend_last_date FROM users WHERE telegram_id = ?", (telegram_id,))
        row = cur.fetchone()
        return row["autosend_last_date"] if row else None

def set_autosend_last_date(telegram_id: int, ymd: str):
    with _get_conn() as conn:
        conn.execute("UPDATE users SET autosend_last_date = ? WHERE telegram_id = ?", (ymd, telegram_id))
        conn.commit()

def list_users_for_autosend_at(hhmm: str, mode: int = 1) -> List[Dict[str, Any]]:
    with _get_conn() as conn:
        cur = conn.execute(
            """
            SELECT * FROM users
            WHERE autosend_enabled = 1
              AND autosend_mode = ?
              AND autosend_time = ?
              AND group_code IS NOT NULL
              AND group_code <> ''
            """,
            (mode, hhmm),
        )
        return [dict(r) for r in cur.fetchall()]

def list_users_mode2_enabled() -> List[Dict[str, Any]]:
    with _get_conn() as conn:
        cur = conn.execute(
            """
            SELECT * FROM users
            WHERE autosend_enabled = 1
              AND autosend_mode = 2
              AND group_code IS NOT NULL
              AND group_code <> ''
            """
        )
        return [dict(r) for r in cur.fetchall()]

def migrate_gcal_autosync():
    with _get_conn() as conn:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(users)")}
        def add(col_name: str, ddl: str):
            if col_name not in cols:
                conn.execute(f"ALTER TABLE users ADD COLUMN {ddl}")

        add("gcal_autosync_enabled",  "gcal_autosync_enabled INTEGER NOT NULL DEFAULT 0")
        add("gcal_autosync_mode",     "gcal_autosync_mode TEXT DEFAULT 'daily'")  # 'daily' | 'weekly'
        add("gcal_autosync_time",     "gcal_autosync_time TEXT")                  # 'HH:MM'
        add("gcal_autosync_weekday",  "gcal_autosync_weekday INTEGER")            # 0..6 (Mon..Sun)
        add("gcal_autosync_last_key", "gcal_autosync_last_key TEXT")              # 'daily:YYYY-MM-DD' / 'weekly:YYYY-Www'
        conn.commit()


def get_autosend_last_date(telegram_id: int) -> Optional[str]:
    with _get_conn() as conn:
        cur = conn.execute("SELECT autosend_last_date FROM users WHERE telegram_id = ?", (telegram_id,))
        r = cur.fetchone()
        return r["autosend_last_date"] if r else None

from typing import Optional

def set_autosend_message_id(telegram_id: int, msg_id: Optional[int]):
    with _get_conn() as conn:
        conn.execute(
            "UPDATE users SET autosend_msg_id = ? WHERE telegram_id = ?",
            (msg_id, telegram_id),
        )
        conn.commit()

def get_autosend_message_id(telegram_id: int) -> Optional[int]:
    with _get_conn() as conn:
        cur = conn.execute("SELECT autosend_msg_id FROM users WHERE telegram_id = ?", (telegram_id,))
        r = cur.fetchone()
        return r["autosend_msg_id"] if r else None

def set_autosend_cur_key(telegram_id: int, key: str):
    with _get_conn() as conn:
        conn.execute("UPDATE users SET autosend_cur_key = ? WHERE telegram_id = ?", (key, telegram_id))
        conn.commit()

def get_autosend_cur_key(telegram_id: int) -> Optional[str]:
    with _get_conn() as conn:
        cur = conn.execute("SELECT autosend_cur_key FROM users WHERE telegram_id = ?", (telegram_id,))
        r = cur.fetchone()
        return r["autosend_cur_key"] if r else None

def set_gcal_connected(telegram_id: int, connected: bool):
    with _get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO users(telegram_id) VALUES (?)", (telegram_id,))
        conn.execute(
            "UPDATE users SET gcal_connected = ? WHERE telegram_id = ?",
            (1 if connected else 0, telegram_id),
        )
        conn.commit()

def set_gcal_tokens(telegram_id: int, access: str, refresh: Optional[str], expiry_iso: str):
    with _get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO users(telegram_id) VALUES (?)", (telegram_id,))
        if refresh is not None:
            conn.execute(
                """
                UPDATE users
                SET gcal_access_token = ?, gcal_refresh_token = ?, gcal_token_expiry = ?
                WHERE telegram_id = ?
                """,
                (access, refresh, expiry_iso, telegram_id),
            )
        else:
            conn.execute(
                """
                UPDATE users
                SET gcal_access_token = ?, gcal_token_expiry = ?
                WHERE telegram_id = ?
                """,
                (access, expiry_iso, telegram_id),
            )
        conn.commit()

def set_gcal_tokens(telegram_id: int, access: str, refresh: Optional[str], expiry_iso: str):
    with _get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO users(telegram_id) VALUES (?)", (telegram_id,))
        if refresh is not None:
            conn.execute(
                """
                UPDATE users
                SET gcal_access_token = ?, gcal_refresh_token = ?, gcal_token_expiry = ?
                WHERE telegram_id = ?
                """,
                (access, refresh, expiry_iso, telegram_id),
            )
        else:
            conn.execute(
                """
                UPDATE users
                SET gcal_access_token = ?, gcal_token_expiry = ?
                WHERE telegram_id = ?
                """,
                (access, expiry_iso, telegram_id),
            )
        conn.commit()

def set_gcal_last_sync(telegram_id: int, iso: Optional[str] = None):
    if iso is None:
        iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    with _get_conn() as conn:
        conn.execute("UPDATE users SET gcal_last_sync = ? WHERE telegram_id = ?", (iso, telegram_id))
        conn.commit()

def set_gcal_autosync_enabled(telegram_id: int, enabled: bool):
    with _get_conn() as conn:
        _ensure_user_exists(conn, telegram_id)
        conn.execute("UPDATE users SET gcal_autosync_enabled = ? WHERE telegram_id = ?",
                     (1 if enabled else 0, telegram_id))
        conn.commit()

def set_gcal_autosync_mode(telegram_id: int, mode: str):
    mode = (mode or "daily").lower()
    if mode not in ("daily", "weekly", "rolling7", "weekly2"):  # <-- добавили weekly2
        raise ValueError("gcal_autosync_mode must be 'daily', 'weekly', 'rolling7' or 'weekly2'")
    with _get_conn() as conn:
        _ensure_user_exists(conn, telegram_id)
        conn.execute("UPDATE users SET gcal_autosync_mode = ? WHERE telegram_id = ?", (mode, telegram_id))
        conn.commit()

def set_gcal_autosync_time(telegram_id: int, hhmm: str):
    if not _TIME_RE.match(hhmm or ""):
        raise ValueError("gcal_autosync_time must be 'HH:MM' 24h")
    with _get_conn() as conn:
        _ensure_user_exists(conn, telegram_id)
        conn.execute("UPDATE users SET gcal_autosync_time = ? WHERE telegram_id = ?", (hhmm, telegram_id))
        conn.commit()

def set_gcal_autosync_weekday(telegram_id: int, weekday: int):
    if weekday not in range(0, 7):
        raise ValueError("gcal_autosync_weekday must be 0..6 (Mon..Sun)")
    with _get_conn() as conn:
        _ensure_user_exists(conn, telegram_id)
        conn.execute("UPDATE users SET gcal_autosync_weekday = ? WHERE telegram_id = ?", (weekday, telegram_id))
        conn.commit()

def set_gcal_autosync_last_key(telegram_id: int, key: str):
    with _get_conn() as conn:
        conn.execute("UPDATE users SET gcal_autosync_last_key = ? WHERE telegram_id = ?", (key, telegram_id))
        conn.commit()

def get_gcal_autosync(user_id: int) -> Dict[str, Any]:
    with _get_conn() as conn:
        r = conn.execute(
            "SELECT gcal_autosync_enabled, gcal_autosync_mode, gcal_autosync_time, "
            "gcal_autosync_weekday, gcal_autosync_last_key FROM users WHERE telegram_id = ?",
            (user_id,)
        ).fetchone()
        return dict(r) if r else {}

def list_users_gcal_autosync_enabled() -> list[Dict[str, Any]]:
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM users WHERE gcal_autosync_enabled = 1 AND gcal_connected = 1 "
            "AND (gcal_autosync_time IS NOT NULL AND gcal_autosync_time <> '')"
        )
        return [dict(x) for x in cur.fetchall()]
