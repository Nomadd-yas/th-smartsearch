"""
Утилита для поиска СТЕ и получения данных о ценах и контрактах.

Основные сущности:
  SteResult       — результат поиска: ste_id, name, category, score
  SteSearchClient — клиент с методами:
    .search(query)               → list[SteResult]   — поиск СТЕ по тексту
    .get_prices(ste_id)          → list[float]       — цены за единицу
    .get_contracts(ste_id)       → list[dict]        — полные данные контрактов
    .get_last_price(ste_id, ...) → float | None      — цена последнего контракта
    .compute_nmck(results, ...)  → NmckResult | None — НМЦК по найденным СТЕ
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime

from .search_engine import IncrementalProductSearch, build_engine, load_data
from .contracts_by_ste import load_contracts, load_contracts_for_ste_ids
from .nmck import NmckResult, calculate_nmck, _unit_matches
from .ste_details import get_ste_by_id

# Паттерн для удаления долей секунд: "14:32:09.307" → "14:32:09"
_FRAC_SEC = re.compile(r"(\d{2}:\d{2}:\d{2})\.\d+")


@dataclass
class SteResult:
    ste_id: str
    name: str
    category: str
    score: float


def _parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    s = _FRAC_SEC.sub(r"\1", s.strip())  # убираем доли секунд
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


class SteSearchClient:
    """Клиент для поиска СТЕ и получения связанных данных о контрактах."""

    def __init__(self, engine_version: str | None = None) -> None:
        from config import settings
        self._engine_version = engine_version or settings.SEARCH_ENGINE_VERSION
        self._engine: IncrementalProductSearch | None = None
        # Кеш результатов find_interchangeable: (ste_id, top_n, min_score) → list[dict]
        self._interchangeable_cache: dict = {}

    def _ensure_engine(self):
        if self._engine is None:
            data = load_data()
            if self._engine_version == "v6":
                from .search_engine_v6 import build_engine_v6
                self._engine, _ = build_engine_v6(data)
            elif self._engine_version == "v5":
                from .search_engine_v5 import build_engine_v5
                self._engine, _ = build_engine_v5(data)
            else:
                self._engine, _ = build_engine(data)
        return self._engine

    def get_ste_detail(self, ste_id: str) -> dict | None:
        """Полная информация о СТЕ: название, категория, производитель, характеристики."""
        return get_ste_by_id(ste_id)

    def search(
        self,
        query: str,
        top_k: int = 20,
        min_score: float | None = None,
    ) -> list[SteResult]:
        """Поиск СТЕ по текстовому запросу.

        min_score=None — автоматический порог: все результаты, score которых
        не ниже 40% от лучшего совпадения (floor 0.01). Адаптируется к запросу:
        для чётких запросов отсекает слабые совпадения, для размытых —
        остаётся гибким.
        """
        engine = self._ensure_engine()

        if min_score is None:
            # Получаем кандидатов без фильтра, затем вычисляем порог
            raw = engine.search(query, top_k=top_k, min_score=0.0)
            if not raw:
                return []
            threshold = max(raw[0].score * 0.4, 0.01)
            raw = [r for r in raw if r.score >= threshold]
        else:
            raw = engine.search(query, top_k=top_k, min_score=min_score)

        return [
            SteResult(
                ste_id=r.metadata.get("ste_id", ""),
                name=r.metadata.get("name", r.text),
                category=r.metadata.get("category", ""),
                score=r.score,
            )
            for r in raw
        ]

    def get_prices(self, ste_id: str) -> list[float]:
        """Цены за единицу по идентификатору СТЕ."""
        prices: list[float] = []
        for contract in load_contracts(ste_id):
            raw = contract.get("Цена за единицу")
            if raw is None:
                continue
            try:
                prices.append(float(raw))
            except (ValueError, TypeError):
                pass
        return prices

    def get_contracts(self, ste_id: str) -> list[dict]:
        """Список контрактов по идентификатору СТЕ."""
        return load_contracts(ste_id)

    def get_last_price(
        self,
        ste_id: str,
        region: str | None = None,
        unit: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        vat: str | None = None,
    ) -> float | None:
        """Цена за единицу последнего (по дате) контракта для СТЕ.

        Применяет те же фильтры, что и compute_nmck.
        """
        contracts = load_contracts(ste_id)

        region_lower = region.strip().lower() if region else None
        unit_lower = unit.strip().lower() if unit else None
        vat_lower = vat.strip().lower() if vat else None

        dated: list[tuple[datetime, float]] = []
        for c in contracts:
            if region_lower:
                raw = c.get("Регион поставщика")
                if not raw or raw.strip().lower() != region_lower:
                    continue
            if unit_lower and not _unit_matches(c.get("Единица измерения"), unit_lower):
                continue
            if vat_lower:
                raw = c.get("Ставка НДС")
                if not raw or raw.strip().lower() != vat_lower:
                    continue
            dt = _parse_date(c.get("Дата заключения контракта"))
            if dt is None:
                continue
            if date_from and dt.date() < date_from:
                continue
            if date_to and dt.date() > date_to:
                continue
            raw_price = c.get("Цена за единицу")
            if raw_price is None:
                continue
            try:
                dated.append((dt, float(raw_price)))
            except (ValueError, TypeError):
                pass

        if not dated:
            return None

        return max(dated, key=lambda x: x[0])[1]

    def filter_ste_by_contracts(
        self,
        results: list[SteResult],
        region: str | None = None,
        unit: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        vat: str | None = None,
    ) -> list[SteResult]:
        """Оставляет только СТЕ, у которых есть хотя бы один контракт,
        соответствующий всем активным фильтрам.

        Без фильтров возвращает results без изменений.
        """
        if not any([region, unit, date_from, date_to, vat]):
            return results

        index = load_contracts_for_ste_ids([r.ste_id for r in results])
        region_lower = region.strip().lower() if region else None
        unit_lower = unit.strip().lower() if unit else None
        vat_lower = vat.strip().lower() if vat else None

        filtered = []
        for r in results:
            for c in index.get(r.ste_id, []):
                if region_lower:
                    raw = c.get("Регион поставщика")
                    if not raw or raw.strip().lower() != region_lower:
                        continue
                if unit_lower:
                    if not _unit_matches(c.get("Единица измерения"), unit_lower):
                        continue
                if vat_lower:
                    raw = c.get("Ставка НДС")
                    if not raw or raw.strip().lower() != vat_lower:
                        continue
                if date_from or date_to:
                    dt = _parse_date(c.get("Дата заключения контракта"))
                    if dt is None:
                        continue
                    if date_from and dt.date() < date_from:
                        continue
                    if date_to and dt.date() > date_to:
                        continue
                filtered.append(r)
                break  # достаточно одного совпадающего контракта
        return filtered

    def get_contracts_for_nmck(
        self,
        results: list[SteResult],
        region: str | None = None,
        unit: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        vat: str | None = None,
    ) -> list[dict]:
        """Все контракты найденных СТЕ после применения фильтров (до IQR и окна).

        Возвращает ровно то, что уходит в calculate_nmck для расчёта НМЦК.
        """
        index = load_contracts_for_ste_ids([r.ste_id for r in results])
        contracts = [c for r in results for c in index.get(r.ste_id, [])]

        if region:
            rl = region.strip().lower()
            contracts = [c for c in contracts
                         if c.get("Регион поставщика") and
                            c["Регион поставщика"].strip().lower() == rl]
        if unit:
            contracts = [c for c in contracts
                         if _unit_matches(c.get("Единица измерения"), unit)]
        if vat:
            vl = vat.strip().lower()
            contracts = [c for c in contracts
                         if c.get("Ставка НДС") and
                            c["Ставка НДС"].strip().lower() == vl]
        if date_from or date_to:
            out = []
            for c in contracts:
                dt = _parse_date(c.get("Дата заключения контракта"))
                if dt is None:
                    continue
                if date_from and dt.date() < date_from:
                    continue
                if date_to and dt.date() > date_to:
                    continue
                out.append(c)
            contracts = out

        return contracts

    def find_interchangeable(
        self,
        ste_id: str,
        top_n: int = 10,
        min_score: float = 0.3,
    ) -> list[dict]:
        """Коммерчески взаимозаменяемые СТЕ по характеристикам (только v6).

        Результат кешируется в памяти — повторные вызовы бесплатны.
        Возвращает [] если движок не v6 или у СТЕ нет характеристик.
        """
        cache_key = (ste_id, top_n, min_score)
        if cache_key not in self._interchangeable_cache:
            engine = self._ensure_engine()
            if hasattr(engine, "find_interchangeable"):
                self._interchangeable_cache[cache_key] = engine.find_interchangeable(
                    ste_id, top_n=top_n, min_score=min_score
                )
            else:
                self._interchangeable_cache[cache_key] = []
        return self._interchangeable_cache[cache_key]

    def compute_nmck(
        self,
        results: list[SteResult],
        region: str | None = None,
        unit: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        vat: str | None = None,
    ) -> NmckResult | None:
        """Рассчитывает НМЦК по найденным СТЕ."""
        index = load_contracts_for_ste_ids([r.ste_id for r in results])
        contracts = [c for r in results for c in index.get(r.ste_id, [])]
        return calculate_nmck(contracts, region=region, unit=unit,
                              date_from=date_from, date_to=date_to, vat=vat)
