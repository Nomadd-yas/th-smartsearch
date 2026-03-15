"""Конфигурация приложения через переменные окружения / .env файл.

Используется как единственный источник истины для всех настроек.
Импортируйте готовый объект:

    from config import settings
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Сервер ────────────────────────────────────────────────────
    HOST: str = "127.0.0.1"
    PORT: int = Field(default=8000, ge=1, le=65535)
    RELOAD: bool = False

    # ── Поисковый движок ──────────────────────────────────────────
    SEARCH_ENGINE_VERSION: Literal["v4", "v5"] = "v5"

    # ── Redis ─────────────────────────────────────────────────────
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = Field(default=6379, ge=1, le=65535)
    REDIS_DB: int = Field(default=0, ge=0, le=15)

    # ── Workspace ─────────────────────────────────────────────────
    WORKSPACE_TTL_SECONDS: int = Field(default=4 * 60 * 60, ge=60)  # мин. 1 минута

    # ── Аутентификация ────────────────────────────────────────────
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=30, ge=1)
    REFRESH_TOKEN_EXPIRE_DAYS: int = Field(default=30, ge=1)

    # ── CORS ──────────────────────────────────────────────────────
    CORS_ORIGINS: list[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
    ]



settings = Settings()
