"""FastAPI application — lifespan и регистрация роутеров."""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from utils.auth import init_db
from utils.cart import init_table as init_cart_table
from utils.history import init_table as init_history_table
from utils.client import SteSearchClient
from app.routers.auth import router as auth_router
from app.routers.cart import router as cart_router
from app.routers.report import router as report_router
from app.routers.search import router as search_router
from app.routers.workspace import router as workspace_router

# ── Логирование ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("smartsearch")


# ── Lifespan ──────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Инициализация БД аутентификации...")
    init_db()
    init_cart_table()
    init_history_table()

    client = SteSearchClient()
    app.state.client = client

    log.info("Загрузка поискового движка...")
    t0 = time.perf_counter()
    client._ensure_engine()
    log.info("Движок готов за %.2fs  (%d docs)", time.perf_counter() - t0, client._engine.size)

    yield

    app.state.client = None
    log.info("Сервер остановлен.")


# ── Приложение ────────────────────────────────────────────────────
app = FastAPI(
    title="SmartSearch — сервис поиска СТЕ",
    description="Поиск по справочнику СТЕ и расчёт НМЦК",
    version="2.0.0",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────────
# Список разрешённых источников. В продакшене замените на реальный домен фронтенда.
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,   # нужно для передачи cookies / Authorization header
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/api/v1")
app.include_router(search_router, prefix="/api/v1")
app.include_router(workspace_router, prefix="/api/v1")
app.include_router(cart_router, prefix="/api/v1")
app.include_router(report_router, prefix="/api/v1")
