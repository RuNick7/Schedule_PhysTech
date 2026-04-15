from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.config import settings

DB_PATH = settings.isu_cache_db


def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def init_isu_db() -> None:
    with _conn() as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS groups_ (
                group_enc TEXT PRIMARY KEY,
                group_name TEXT NOT NULL,
                updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS students (
                student_id INTEGER,
                student_name TEXT,
                group_enc TEXT,
                group_name TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_students_name
                ON students(student_name COLLATE NOCASE);
            CREATE INDEX IF NOT EXISTS idx_students_group
                ON students(group_enc);
            CREATE TABLE IF NOT EXISTS potoks (
                potok_id INTEGER PRIMARY KEY,
                potok_name TEXT NOT NULL,
                updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS schedule_cache (
                potok_id INTEGER PRIMARY KEY,
                html TEXT,
                fetched_at TEXT
            );
            CREATE TABLE IF NOT EXISTS index_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── groups ──────────────────────────────────────────────────────────────

def save_groups(groups: List[Tuple[str, str]]) -> None:
    now = _now_iso()
    with _conn() as con:
        con.execute("DELETE FROM groups_")
        con.executemany(
            "INSERT INTO groups_(group_enc, group_name, updated_at) VALUES (?, ?, ?)",
            [(enc, name, now) for enc, name in groups],
        )
        _set_meta(con, "groups_count", str(len(groups)))
        _set_meta(con, "groups_updated_at", now)


def get_all_groups() -> List[Dict[str, str]]:
    with _conn() as con:
        return [dict(r) for r in con.execute("SELECT * FROM groups_ ORDER BY group_name")]


def search_groups(query: str) -> List[Dict[str, str]]:
    q = _norm(query)
    if not q:
        return []
    with _conn() as con:
        rows = con.execute(
            "SELECT * FROM groups_ WHERE LOWER(group_name) LIKE ? ORDER BY group_name LIMIT 30",
            (f"%{q}%",),
        )
        return [dict(r) for r in rows]


# ── students ────────────────────────────────────────────────────────────

def save_students_for_group(
    group_enc: str, group_name: str, students: List[Tuple[int, str]]
) -> None:
    with _conn() as con:
        con.execute("DELETE FROM students WHERE group_enc = ?", (group_enc,))
        con.executemany(
            "INSERT INTO students(student_id, student_name, group_enc, group_name) VALUES (?, ?, ?, ?)",
            [(sid, sname, group_enc, group_name) for sid, sname in students],
        )


def search_students_by_fio(query: str) -> List[Dict[str, Any]]:
    tokens = _norm(query).split()
    if not tokens:
        return []
    with _conn() as con:
        sql = "SELECT * FROM students WHERE 1=1"
        params: list = []
        for tok in tokens:
            sql += " AND LOWER(REPLACE(student_name, 'ё', 'е')) LIKE ?"
            params.append(f"%{tok}%")
        sql += " ORDER BY student_name LIMIT 30"
        return [dict(r) for r in con.execute(sql, params)]


# ── potoks ──────────────────────────────────────────────────────────────

def save_potoks(potoks: List[Tuple[int, str]]) -> None:
    now = _now_iso()
    with _conn() as con:
        con.execute("DELETE FROM potoks")
        con.executemany(
            "INSERT INTO potoks(potok_id, potok_name, updated_at) VALUES (?, ?, ?)",
            [(pid, pname, now) for pid, pname in potoks],
        )
        _set_meta(con, "potoks_count", str(len(potoks)))
        _set_meta(con, "potoks_updated_at", now)


def search_potoks_by_group(group_name: str) -> List[Dict[str, Any]]:
    q = _norm(group_name)
    if not q:
        return []
    with _conn() as con:
        rows = con.execute(
            "SELECT * FROM potoks WHERE LOWER(potok_name) LIKE ? ORDER BY potok_name LIMIT 50",
            (f"%{q}%",),
        )
        return [dict(r) for r in rows]


def get_all_potoks() -> List[Dict[str, Any]]:
    with _conn() as con:
        return [dict(r) for r in con.execute("SELECT * FROM potoks ORDER BY potok_name")]


# ── schedule cache ──────────────────────────────────────────────────────

def save_schedule_html(potok_id: int, html: str) -> None:
    now = _now_iso()
    with _conn() as con:
        con.execute(
            "INSERT OR REPLACE INTO schedule_cache(potok_id, html, fetched_at) VALUES (?, ?, ?)",
            (potok_id, html, now),
        )


def get_cached_schedule(potok_id: int, max_age_sec: int = 3600) -> Optional[str]:
    with _conn() as con:
        row = con.execute(
            "SELECT html, fetched_at FROM schedule_cache WHERE potok_id = ?",
            (potok_id,),
        ).fetchone()
        if not row:
            return None
        try:
            fetched = datetime.fromisoformat(row["fetched_at"])
            age = (datetime.now(timezone.utc) - fetched).total_seconds()
            if age > max_age_sec:
                return None
        except Exception:
            pass
        return row["html"]


# ── index meta ──────────────────────────────────────────────────────────

def _set_meta(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute(
        "INSERT OR REPLACE INTO index_meta(key, value) VALUES (?, ?)",
        (key, value),
    )


def set_meta(key: str, value: str) -> None:
    with _conn() as con:
        _set_meta(con, key, value)


def get_meta(key: str) -> Optional[str]:
    with _conn() as con:
        row = con.execute("SELECT value FROM index_meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None


def get_index_progress() -> Dict[str, Any]:
    with _conn() as con:
        meta = {}
        for row in con.execute("SELECT key, value FROM index_meta"):
            meta[row["key"]] = row["value"]

        groups_total = int(meta.get("groups_count", "0") or "0")
        groups_indexed = con.execute(
            "SELECT COUNT(DISTINCT group_enc) FROM students"
        ).fetchone()[0]
        potoks_total = int(meta.get("potoks_count", "0") or "0")

        return {
            "groups_total": groups_total,
            "groups_indexed": groups_indexed,
            "potoks_total": potoks_total,
            "groups_updated_at": meta.get("groups_updated_at"),
            "potoks_updated_at": meta.get("potoks_updated_at"),
            "indexer_status": meta.get("indexer_status", "idle"),
            "last_error": meta.get("last_error"),
        }


# ── helpers ─────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return " ".join((s or "").lower().replace("ё", "е").strip().split())
