"""Запуск сервера SmartSearch.

  python run.py
  python run.py --host 0.0.0.0 --port 8000
  python run.py --engine-version v4
  python run.py --engine-version v5
  python run.py --engine-version v6   (+ характеристики взаимозаменяемости)
"""

from __future__ import annotations

import argparse

import uvicorn

from config import settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Сервис поискового движка СТЕ")
    parser.add_argument("--host", default=settings.HOST, help=f"Хост (по умолчанию: {settings.HOST})")
    parser.add_argument("--port", type=int, default=settings.PORT, help=f"Порт (по умолчанию: {settings.PORT})")
    parser.add_argument("--reload", action="store_true", default=settings.RELOAD, help="Авто-перезагрузка при изменении файлов")
    parser.add_argument(
        "--engine-version",
        choices=["v4", "v5", "v6"],
        default=settings.SEARCH_ENGINE_VERSION,
        dest="engine_version",
        help=f"Версия поискового движка (по умолчанию: {settings.SEARCH_ENGINE_VERSION})",
    )
    args = parser.parse_args()

    # CLI-аргумент перекрывает .env для engine_version
    settings.SEARCH_ENGINE_VERSION = args.engine_version

    uvicorn.run("app.main:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
