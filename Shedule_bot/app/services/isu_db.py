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
            CREATE TABLE IF NOT EXISTS potok_students (
                potok_id INTEGER NOT NULL,
                student_id INTEGER NOT NULL,
                student_name TEXT,
                PRIMARY KEY (potok_id, student_id)
            );
            CREATE INDEX IF NOT EXISTS idx_potok_students_student
                ON potok_students(student_id);
            CREATE INDEX IF NOT EXISTS idx_potok_students_potok
                ON potok_students(potok_id);
            CREATE TABLE IF NOT EXISTS schedule_cache (
                potok_id INTEGER PRIMARY KEY,
                html TEXT,
                fetched_at TEXT
            );
            CREATE TABLE IF NOT EXISTS schedule_entries (
                potok_id INTEGER NOT NULL,
                day TEXT NOT NULL,
                time TEXT NOT NULL,
                subject TEXT,
                room TEXT,
                teacher TEXT,
                lesson_type TEXT,
                parity TEXT,
                updated_at TEXT,
                PRIMARY KEY (potok_id, day, time, subject, room, teacher, lesson_type, parity)
            );
            CREATE INDEX IF NOT EXISTS idx_schedule_entries_potok
                ON schedule_entries(potok_id);
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
    q_compact = _compact(query)
    with _conn() as con:
        rows = con.execute(
            """
            SELECT *
            FROM groups_
            WHERE LOWER(REPLACE(group_name, 'ё', 'е')) LIKE ?
               OR LOWER(REPLACE(group_enc, 'ё', 'е')) LIKE ?
               OR LOWER(REPLACE(REPLACE(group_name, ' ', ''), 'ё', 'е')) LIKE ?
            ORDER BY
                CASE
                    WHEN LOWER(REPLACE(group_name, 'ё', 'е')) = ? THEN 0
                    WHEN LOWER(REPLACE(group_enc, 'ё', 'е')) = ? THEN 1
                    WHEN LOWER(REPLACE(group_name, 'ё', 'е')) LIKE ? THEN 2
                    WHEN LOWER(REPLACE(group_enc, 'ё', 'е')) LIKE ? THEN 3
                    ELSE 4
                END,
                group_name
            LIMIT 50
            """,
            (f"%{q}%", f"%{q}%", f"%{q_compact}%", q, q, f"{q}%", f"{q}%"),
        )
        return [dict(r) for r in rows]


def get_group_by_enc(group_enc: str) -> Optional[Dict[str, str]]:
    with _conn() as con:
        row = con.execute(
            "SELECT * FROM groups_ WHERE group_enc = ?",
            (group_enc,),
        ).fetchone()
        return dict(row) if row else None


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
        sql = """
            SELECT
                student_id,
                student_name,
                group_enc,
                group_name
            FROM students
            WHERE 1=1
        """
        params: list = []
        for tok in tokens:
            sql += " AND LOWER(REPLACE(student_name, 'ё', 'е')) LIKE ?"
            params.append(f"%{tok}%")
        sql += """
            GROUP BY student_id, student_name, group_enc, group_name
            ORDER BY
                CASE
                    WHEN LOWER(REPLACE(student_name, 'ё', 'е')) = ? THEN 0
                    WHEN LOWER(REPLACE(student_name, 'ё', 'е')) LIKE ? THEN 1
                    ELSE 2
                END,
                student_name,
                group_name
            LIMIT 100
        """
        joined = " ".join(tokens)
        params.extend([joined, f"{joined}%"])
        return [dict(r) for r in con.execute(sql, params)]


def get_students_by_group(group_enc: str) -> List[Dict[str, Any]]:
    with _conn() as con:
        rows = con.execute(
            """
            SELECT student_id, student_name, group_enc, group_name
            FROM students
            WHERE group_enc = ?
            GROUP BY student_id, student_name, group_enc, group_name
            ORDER BY student_name
            """,
            (group_enc,),
        )
        return [dict(r) for r in rows]


def get_student_by_id(student_id: int) -> Optional[Dict[str, Any]]:
    with _conn() as con:
        row = con.execute(
            """
            SELECT student_id, student_name, group_enc, group_name
            FROM students
            WHERE student_id = ?
            ORDER BY group_name
            LIMIT 1
            """,
            (student_id,),
        ).fetchone()
        return dict(row) if row else None


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


def clear_potok_students() -> None:
    with _conn() as con:
        con.execute("DELETE FROM potok_students")


def save_potok_students(
    potok_id: int, potok_name: str, students: List[Tuple[int, str]]
) -> None:
    with _conn() as con:
        con.execute(
            "INSERT OR REPLACE INTO potoks(potok_id, potok_name, updated_at) VALUES (?, ?, ?)",
            (potok_id, potok_name, _now_iso()),
        )
        con.execute("DELETE FROM potok_students WHERE potok_id = ?", (potok_id,))
        if students:
            con.executemany(
                """
                INSERT INTO potok_students(potok_id, student_id, student_name)
                VALUES (?, ?, ?)
                """,
                [(potok_id, sid, sname) for sid, sname in students],
            )


def search_potoks_by_group(group_enc: str) -> List[Dict[str, Any]]:
    with _conn() as con:
        rows = con.execute(
            """
            SELECT DISTINCT p.potok_id, p.potok_name, COUNT(ps.student_id) AS matched_students
            FROM students s
            JOIN potok_students ps ON ps.student_id = s.student_id
            JOIN potoks p ON p.potok_id = ps.potok_id
            WHERE s.group_enc = ?
            GROUP BY p.potok_id, p.potok_name
            ORDER BY matched_students DESC, p.potok_name
            LIMIT 100
            """,
            (group_enc,),
        )
        return [dict(r) for r in rows]


def get_potoks_by_student(student_id: int) -> List[Dict[str, Any]]:
    with _conn() as con:
        rows = con.execute(
            """
            SELECT DISTINCT p.potok_id, p.potok_name
            FROM potok_students ps
            JOIN potoks p ON p.potok_id = ps.potok_id
            WHERE ps.student_id = ?
            ORDER BY p.potok_name
            """,
            (student_id,),
        )
        return [dict(r) for r in rows]


def get_potok_name(potok_id: int) -> Optional[str]:
    with _conn() as con:
        row = con.execute(
            "SELECT potok_name FROM potoks WHERE potok_id = ?",
            (potok_id,),
        ).fetchone()
        return row["potok_name"] if row else None


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


def save_schedule_entries(potok_id: int, lessons: List[Dict[str, Any]]) -> None:
    now = _now_iso()
    with _conn() as con:
        con.execute("DELETE FROM schedule_entries WHERE potok_id = ?", (potok_id,))
        if lessons:
            con.executemany(
                """
                INSERT OR REPLACE INTO schedule_entries(
                    potok_id, day, time, subject, room, teacher, lesson_type, parity, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        potok_id,
                        str(it.get("day") or ""),
                        str(it.get("time") or ""),
                        str(it.get("subject") or ""),
                        str(it.get("room") or ""),
                        str(it.get("teacher") or ""),
                        str(it.get("lesson_type") or ""),
                        str(it.get("parity") or ""),
                        now,
                    )
                    for it in lessons
                ],
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


def get_cached_schedule_entries(
    potok_id: int, max_age_sec: int = 3600
) -> List[Dict[str, Any]]:
    with _conn() as con:
        row = con.execute(
            "SELECT fetched_at FROM schedule_cache WHERE potok_id = ?",
            (potok_id,),
        ).fetchone()
        if not row:
            return []
        try:
            fetched = datetime.fromisoformat(row["fetched_at"])
            age = (datetime.now(timezone.utc) - fetched).total_seconds()
            if age > max_age_sec:
                return []
        except Exception:
            return []

        rows = con.execute(
            """
            SELECT day, time, subject, room, teacher, lesson_type, parity
            FROM schedule_entries
            WHERE potok_id = ?
            ORDER BY day, time, subject
            """,
            (potok_id,),
        )
        return [dict(r) for r in rows]


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
        potoks_indexed = con.execute(
            "SELECT COUNT(DISTINCT potok_id) FROM potok_students"
        ).fetchone()[0]

        return {
            "groups_total": groups_total,
            "groups_indexed": groups_indexed,
            "potoks_total": potoks_total,
            "potoks_indexed": potoks_indexed,
            "groups_updated_at": meta.get("groups_updated_at"),
            "potoks_updated_at": meta.get("potoks_updated_at"),
            "indexer_status": meta.get("indexer_status", "idle"),
            "last_error": meta.get("last_error"),
        }


# ── helpers ─────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return " ".join((s or "").lower().replace("ё", "е").strip().split())


def _compact(s: str) -> str:
    return re_sub_spaces(_norm(s))


def re_sub_spaces(s: str) -> str:
    return (s or "").replace(" ", "")
