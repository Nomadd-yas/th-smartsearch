"""Загрузка контрактов из SQLite базы данных."""

from __future__ import annotations

import sqlite3
from collections import defaultdict

from .db import DB_PATH

COLUMNS = [
    "Наименование закупки",
    "Количество",
    "Единица измерения",
    "Идентификатор контракта",
    "Способ закупки",
    "Начальная стоимость контракта",
    "Стоимость контракта после заключения",
    "% снижения",
    "Ставка НДС",
    "Дата заключения контракта",
    "ИНН заказчика",
    "Регион заказчика",
    "ИНН поставщика",
    "Регион поставщика",
    "Идентификатор СТЕ",
    "Наименование позиции СТЕ",
    "Цена за единицу",
]


def _require_db() -> None:
    if not DB_PATH.exists():
        raise RuntimeError(
            f"База данных не найдена: {DB_PATH}\n"
            "Запустите миграцию: python -m utils.migrate_to_db"
        )



def load_contracts(ste_id: str) -> list[dict]:
    """Контракты для одного СТЕ (прямой запрос к БД)."""
    _require_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        'SELECT * FROM contracts WHERE "Идентификатор СТЕ" = ?', (ste_id,)
    ).fetchall()
    conn.close()
    result = []
    for row in rows:
        d = dict(row)
        d.pop("id", None)
        result.append(d)
    return result


def load_contracts_for_ste_ids(ste_ids: list[str]) -> dict[str, list[dict]]:
    """Контракты для набора СТЕ одним запросом, возвращает dict ste_id → [contract]."""
    if not ste_ids:
        return {}
    _require_db()
    placeholders = ",".join("?" for _ in ste_ids)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f'SELECT * FROM contracts WHERE "Идентификатор СТЕ" IN ({placeholders})',
        ste_ids,
    ).fetchall()
    conn.close()

    index: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        d = dict(row)
        d.pop("id", None)
        ste_id = d.get("Идентификатор СТЕ")
        if ste_id:
            index[ste_id].append(d)
    return dict(index)
