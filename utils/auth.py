"""Аутентификация: JWT access token + opaque refresh token (хранится в SQLite)."""

from __future__ import annotations

import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bcrypt as _bcrypt
from jose import JWTError, jwt

# ── Пути ──────────────────────────────────────────────────────────
AUTH_DB = Path(__file__).parent.parent / "data" / "auth.db"

# ── Константы ─────────────────────────────────────────────────────
from config import settings

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES
REFRESH_TOKEN_EXPIRE_DAYS = settings.REFRESH_TOKEN_EXPIRE_DAYS

_SECRET_KEY: str | None = None  # кешируется после первого чтения из БД


# ── Инициализация ─────────────────────────────────────────────────
def init_db() -> None:
    """Создаёт таблицы при первом запуске."""
    AUTH_DB.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(AUTH_DB) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS users (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                username        TEXT UNIQUE NOT NULL,
                hashed_password TEXT NOT NULL,
                created_at      TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS refresh_tokens (
                token      TEXT PRIMARY KEY,
                user_id    INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
        """)


def _secret_key() -> str:
    """Возвращает постоянный секрет (генерируется один раз и сохраняется в БД)."""
    global _SECRET_KEY
    if _SECRET_KEY is not None:
        return _SECRET_KEY
    with sqlite3.connect(AUTH_DB) as conn:
        row = conn.execute("SELECT value FROM config WHERE key='secret_key'").fetchone()
        if row:
            _SECRET_KEY = row[0]
            return _SECRET_KEY
        key = secrets.token_hex(32)
        conn.execute("INSERT INTO config (key, value) VALUES ('secret_key', ?)", (key,))
        _SECRET_KEY = key
    return _SECRET_KEY


# ── Пароли ────────────────────────────────────────────────────────
def hash_password(password: str) -> str:
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return _bcrypt.checkpw(plain.encode(), hashed.encode())


# ── Пользователи ──────────────────────────────────────────────────
def create_user(username: str, password: str) -> int:
    """Создаёт пользователя. Бросает sqlite3.IntegrityError при дубликате."""
    with sqlite3.connect(AUTH_DB) as conn:
        cur = conn.execute(
            "INSERT INTO users (username, hashed_password, created_at) VALUES (?, ?, ?)",
            (username, hash_password(password), datetime.now(timezone.utc).isoformat()),
        )
        return cur.lastrowid  # type: ignore[return-value]


def get_user(username: str) -> dict | None:
    with sqlite3.connect(AUTH_DB) as conn:
        row = conn.execute(
            "SELECT id, username, hashed_password FROM users WHERE username = ?",
            (username,),
        ).fetchone()
    return {"id": row[0], "username": row[1], "hashed_password": row[2]} if row else None


def get_user_by_id(user_id: int) -> dict | None:
    with sqlite3.connect(AUTH_DB) as conn:
        row = conn.execute(
            "SELECT id, username FROM users WHERE id = ?", (user_id,)
        ).fetchone()
    return {"id": row[0], "username": row[1]} if row else None


# ── Access token ──────────────────────────────────────────────────
def create_access_token(user_id: int, username: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    return jwt.encode(
        {"sub": str(user_id), "username": username, "exp": expire},
        _secret_key(),
        algorithm=ALGORITHM,
    )


def verify_access_token(token: str) -> dict | None:
    try:
        return jwt.decode(token, _secret_key(), algorithms=[ALGORITHM])
    except JWTError:
        return None


# ── Refresh token ─────────────────────────────────────────────────
def create_refresh_token(user_id: int) -> str:
    token = secrets.token_urlsafe(48)
    expires_at = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    with sqlite3.connect(AUTH_DB) as conn:
        conn.execute(
            "INSERT INTO refresh_tokens (token, user_id, expires_at) VALUES (?, ?, ?)",
            (token, user_id, expires_at.isoformat()),
        )
    return token


def rotate_refresh_token(old_token: str) -> tuple[int, str] | None:
    """Атомарно инвалидирует старый токен и выдаёт новый.

    Возвращает (user_id, new_token) или None, если токен невалиден/истёк.
    """
    with sqlite3.connect(AUTH_DB) as conn:
        row = conn.execute(
            "SELECT user_id, expires_at FROM refresh_tokens WHERE token = ?",
            (old_token,),
        ).fetchone()
        if not row:
            return None
        user_id, expires_at = row
        conn.execute("DELETE FROM refresh_tokens WHERE token = ?", (old_token,))
        if datetime.fromisoformat(expires_at) < datetime.now(timezone.utc):
            return None
        new_token = secrets.token_urlsafe(48)
        new_expires = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        conn.execute(
            "INSERT INTO refresh_tokens (token, user_id, expires_at) VALUES (?, ?, ?)",
            (new_token, user_id, new_expires.isoformat()),
        )
    return user_id, new_token


def revoke_refresh_token(token: str) -> None:
    with sqlite3.connect(AUTH_DB) as conn:
        conn.execute("DELETE FROM refresh_tokens WHERE token = ?", (token,))
