"""Загрузка полных данных СТЕ из SQLite базы данных."""

from __future__ import annotations

import sqlite3

from .db import DB_PATH


def _parse_characteristics(raw: str | None) -> dict[str, str]:
    """Парсит строку 'Ключ:Значение;Ключ2:Значение2' в словарь.

    Значения могут содержать ':', поэтому делим только по первому вхождению.
    """
    if not raw:
        return {}
    result: dict[str, str] = {}
    for pair in raw.split(";"):
        pair = pair.strip()
        if ":" not in pair:
            continue
        key, value = pair.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key:
            result[key] = value
    return result


def _require_db() -> None:
    if not DB_PATH.exists():
        raise RuntimeError(
            f"База данных не найдена: {DB_PATH}\n"
            "Запустите миграцию: python -m utils.migrate_to_db"
        )


def load_all_ste_indexed() -> dict[str, dict]:
    """Загружает все СТЕ в память, индексирует по ste_id.

    Вызывается один раз при первом обращении к /ste/{ste_id}.
    """
    _require_db()
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT ste_id, name, category, manufacturer, characteristics FROM ste"
    ).fetchall()
    conn.close()

    index: dict[str, dict] = {}
    for ste_id, name, category, manufacturer, characteristics in rows:
        index[ste_id] = {
            "ste_id": ste_id,
            "name": name,
            "category": category,
            "manufacturer": manufacturer or None,
            "characteristics": _parse_characteristics(characteristics),
        }
    return index


def get_ste_by_id(ste_id: str) -> dict | None:
    """Прямой запрос к БД для одного СТЕ (без загрузки всей таблицы в память)."""
    _require_db()
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT ste_id, name, category, manufacturer, characteristics "
        "FROM ste WHERE ste_id = ?",
        (ste_id,),
    ).fetchone()
    conn.close()
    if row is None:
        return None
    ste_id, name, category, manufacturer, characteristics = row
    return {
        "ste_id": ste_id,
        "name": name,
        "category": category,
        "manufacturer": manufacturer or None,
        "characteristics": _parse_characteristics(characteristics),
    }
