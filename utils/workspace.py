"""Управление workspace-сессиями через Redis.

Workspace хранит полное состояние рабочего процесса:
  - параметры поиска
  - список СТЕ
  - отфильтрованные контракты
  - состояние НМЦК (результат + force-корректировки)

TTL обновляется при каждом обращении к workspace.
"""

from __future__ import annotations

import json
import uuid

import redis

from config import settings

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True,
        )
    return _client


def _key(workspace_id: str) -> str:
    return f"ws:{workspace_id}"


def create(state: dict) -> str:
    """Создаёт workspace, возвращает workspace_id."""
    workspace_id = str(uuid.uuid4())
    get_redis().setex(_key(workspace_id), settings.WORKSPACE_TTL_SECONDS, json.dumps(state, default=str))
    return workspace_id


def get(workspace_id: str) -> dict | None:
    """Возвращает состояние workspace и обновляет TTL. None если не найден/истёк."""
    r = get_redis()
    raw = r.get(_key(workspace_id))
    if raw is None:
        return None
    r.expire(_key(workspace_id), settings.WORKSPACE_TTL_SECONDS)
    return json.loads(raw)


def update_nmck(workspace_id: str, nmck_state: dict) -> bool:
    """Обновляет только блок nmck в workspace. False если workspace не найден."""
    state = get(workspace_id)
    if state is None:
        return False
    state["nmck"] = nmck_state
    get_redis().setex(_key(workspace_id), settings.WORKSPACE_TTL_SECONDS, json.dumps(state, default=str))
    return True


def delete(workspace_id: str) -> bool:
    """Удаляет workspace. False если не найден."""
    return bool(get_redis().delete(_key(workspace_id)))
