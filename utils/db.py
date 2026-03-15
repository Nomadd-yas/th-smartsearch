"""Центральный путь к SQLite базе данных проекта."""

from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "smartsearch.db"
