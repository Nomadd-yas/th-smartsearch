"""История сформированных документов НМЦК.

Хранится в auth.db рядом с таблицей users.
Каждая запись содержит снапшот позиций на момент формирования документа.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Literal

from utils.auth import AUTH_DB


def init_table() -> None:
    """Создаёт таблицу nmck_history. Вызывается из lifespan."""
    with sqlite3.connect(AUTH_DB) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS nmck_history (
                id          TEXT PRIMARY KEY,
                user_id     INTEGER NOT NULL,
                source      TEXT NOT NULL,
                item_count  INTEGER NOT NULL,
                total_nmck  REAL NOT NULL,
                items       TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_history_user
                ON nmck_history(user_id, created_at DESC);
        """)


# ── CRUD ──────────────────────────────────────────────────────────

def add_entry(
    user_id: int,
    source: Literal["cart", "single"],
    items: list[dict],
) -> dict:
    entry_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    total = sum(
        item.get("unit_price", 0.0) * (item.get("quantity") or 1)
        for item in items
    )
    with sqlite3.connect(AUTH_DB) as conn:
        conn.execute(
            """INSERT INTO nmck_history
               (id, user_id, source, item_count, total_nmck, items, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (entry_id, user_id, source, len(items),
             total, json.dumps(items, ensure_ascii=False), now),
        )
    return get_entry(user_id, entry_id)  # type: ignore[return-value]


def list_entries(user_id: int, limit: int = 50) -> list[dict]:
    with sqlite3.connect(AUTH_DB) as conn:
        rows = conn.execute(
            """SELECT id, user_id, source, item_count, total_nmck, items, created_at
               FROM nmck_history
               WHERE user_id = ?
               ORDER BY created_at DESC
               LIMIT ?""",
            (user_id, limit),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_entry(user_id: int, entry_id: str) -> dict | None:
    with sqlite3.connect(AUTH_DB) as conn:
        row = conn.execute(
            """SELECT id, user_id, source, item_count, total_nmck, items, created_at
               FROM nmck_history WHERE id = ? AND user_id = ?""",
            (entry_id, user_id),
        ).fetchone()
    return _row_to_dict(row) if row else None


def delete_entry(user_id: int, entry_id: str) -> bool:
    with sqlite3.connect(AUTH_DB) as conn:
        cur = conn.execute(
            "DELETE FROM nmck_history WHERE id = ? AND user_id = ?",
            (entry_id, user_id),
        )
    return cur.rowcount > 0


# ── Internals ─────────────────────────────────────────────────────

def _row_to_dict(row: tuple) -> dict:
    return {
        "id":         row[0],
        "user_id":    row[1],
        "source":     row[2],
        "item_count": row[3],
        "total_nmck": row[4],
        "items":      json.loads(row[5]),
        "created_at": row[6],
    }
