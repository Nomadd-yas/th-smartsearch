"""Корзина: хранение позиций НМЦК для формирования итогового контракта.

Хранится в auth.db рядом с таблицей users (FK user_id → users.id).
Каждая позиция содержит снапшот NmckData на момент добавления/последнего обновления.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import date, datetime, timezone

from utils.auth import AUTH_DB


def init_table() -> None:
    """Создаёт таблицу cart_items. Вызывается из lifespan после init_db()."""
    with sqlite3.connect(AUTH_DB) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS cart_items (
                id          TEXT PRIMARY KEY,
                user_id     INTEGER NOT NULL,
                name        TEXT NOT NULL,
                quantity    INTEGER NOT NULL,
                unit        TEXT NOT NULL DEFAULT '',
                unit_price  REAL NOT NULL,
                date_added  TEXT NOT NULL,
                nmck_data   TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_cart_user ON cart_items(user_id);
        """)


# ── CRUD ──────────────────────────────────────────────────────────

def add_item(
    user_id: int,
    name: str,
    quantity: int,
    unit: str,
    unit_price: float,
    nmck_data: dict,
) -> dict:
    item_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    today = date.today().isoformat()
    with sqlite3.connect(AUTH_DB) as conn:
        conn.execute(
            """INSERT INTO cart_items
               (id, user_id, name, quantity, unit, unit_price, date_added,
                nmck_data, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (item_id, user_id, name, quantity, unit, unit_price,
             today, json.dumps(nmck_data), now, now),
        )
    return get_item(user_id, item_id)  # type: ignore[return-value]


def list_items(user_id: int) -> list[dict]:
    with sqlite3.connect(AUTH_DB) as conn:
        rows = conn.execute(
            """SELECT id, user_id, name, quantity, unit, unit_price,
                      date_added, nmck_data, created_at, updated_at
               FROM cart_items WHERE user_id = ?
               ORDER BY created_at DESC""",
            (user_id,),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_item(user_id: int, item_id: str) -> dict | None:
    with sqlite3.connect(AUTH_DB) as conn:
        row = conn.execute(
            """SELECT id, user_id, name, quantity, unit, unit_price,
                      date_added, nmck_data, created_at, updated_at
               FROM cart_items WHERE id = ? AND user_id = ?""",
            (item_id, user_id),
        ).fetchone()
    return _row_to_dict(row) if row else None


def update_fields(
    user_id: int,
    item_id: str,
    *,
    name: str | None = None,
    quantity: int | None = None,
    unit: str | None = None,
    unit_price: float | None = None,
) -> dict | None:
    """Обновляет редактируемые поля. Незаданные параметры не изменяются."""
    updates: dict[str, object] = {}
    if name is not None:
        updates["name"] = name
    if quantity is not None:
        updates["quantity"] = quantity
    if unit is not None:
        updates["unit"] = unit
    if unit_price is not None:
        updates["unit_price"] = unit_price
    if not updates:
        return get_item(user_id, item_id)

    now = datetime.now(timezone.utc).isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = [*updates.values(), now, item_id, user_id]
    with sqlite3.connect(AUTH_DB) as conn:
        conn.execute(
            f"UPDATE cart_items SET {set_clause}, updated_at = ? WHERE id = ? AND user_id = ?",
            values,
        )
    return get_item(user_id, item_id)


def update_nmck(
    user_id: int,
    item_id: str,
    unit_price: float,
    nmck_data: dict,
) -> dict | None:
    """Обновляет снапшот НМЦК и цену за единицу; name не меняется."""
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(AUTH_DB) as conn:
        conn.execute(
            """UPDATE cart_items
               SET unit_price = ?, nmck_data = ?, updated_at = ?
               WHERE id = ? AND user_id = ?""",
            (unit_price, json.dumps(nmck_data), now, item_id, user_id),
        )
    return get_item(user_id, item_id)


def delete_item(user_id: int, item_id: str) -> bool:
    with sqlite3.connect(AUTH_DB) as conn:
        cur = conn.execute(
            "DELETE FROM cart_items WHERE id = ? AND user_id = ?",
            (item_id, user_id),
        )
    return cur.rowcount > 0


# ── Internals ─────────────────────────────────────────────────────

def _row_to_dict(row: tuple) -> dict:
    return {
        "id":         row[0],
        "user_id":    row[1],
        "name":       row[2],
        "quantity":   row[3],
        "unit":       row[4],
        "unit_price": row[5],
        "date_added": row[6],
        "nmck_data":  json.loads(row[7]),
        "created_at": row[8],
        "updated_at": row[9],
    }
