"""
IncrementalProductSearch v6 — v5 + характеристики СТЕ.

Новое по сравнению с v5:
  Characteristic Similarity Index для нахождения коммерчески взаимозаменяемых СТЕ.

  char_sim_index: (norm_key, norm_value) -> {doc_id, ...}
  doc_characteristics: doc_id -> {norm_key: norm_value}
  ste_id_to_doc: ste_id -> doc_id

  find_interchangeable(ste_id):
    1. Берём характеристики источника.
    2. По каждой (key, value) ищем кандидатов в char_sim_index.
    3. Требуем: хотя бы один Tier-1 ключ совпал.
    4. score = sum(weight(matched_key)) / sum(weight(src_key))
    5. Возвращаем top_n по score >= min_score.

  Кеш: engine_cache_v6.pkl (отдельный от v5).
"""

from __future__ import annotations

import pickle  # noqa: S403
import re
import sqlite3
import time
from collections import defaultdict

from .db import DB_PATH
from .search_engine import CACHE_DB, DATA_DIR
from .search_engine_v5 import IncrementalProductSearchV5

ENGINE_CACHE_V6 = DATA_DIR / "data" / "engine_cache_v6.pkl"


# ── Веса характеристик ────────────────────────────────────────────
# 0 / отсутствует = игнорировать (Цвет, ISBN, Автор, Страна производитель...)

_CHAR_WEIGHTS: dict[str, float] = {
    # Tier-1: функциональная идентичность (>= 0.7)
    "мнн или химическое, группировочное наименование":           1.0,
    "лекарственная форма":                                       1.0,
    "дозировка":                                                 1.0,
    "дозированная форма":                                        0.9,
    "признак вхождения в перечень жнвлп":                        0.8,
    "вид товаров":                                               0.8,
    "вид продукции":                                             0.8,
    "вид товара":                                                0.8,
    "вид товары медицинские":                                    0.8,
    "тип":                                                       0.7,
    "назначение":                                                0.7,
    "виды товаров строительных":                                 0.7,
    "виды товаров информационно-технологических, средств связи, оргтехники, электроники (включая программное обеспечение)": 0.7,
    "виды одежды (включая форменную), головных уборов, одежды специальной": 0.7,
    "виды товаров спортивных":                                   0.7,
    # Tier-2: материал / применение (0.4 – 0.6)
    "вид":                                                       0.6,
    "вид литература и полиграфическая продукция":                0.6,
    "виды литературы для обучения":                              0.5,
    "материал":                                                  0.5,
    "вид материала":                                             0.5,
    "область применения":                                        0.5,
    "состав":                                                    0.5,
    "виды материалов расходных инженерно-строительных":          0.5,
    "материал корпуса":                                          0.4,
    "комплектация":                                              0.4,
    "минимальный ресурс картриджа при 5% заполнении страницы формата а4, количество страниц": 0.4,
    # Tier-3: технические параметры (0.15 – 0.3)
    "мощность":                                                  0.3,
    "плотность":                                                 0.25,
    "длина":                                                     0.2,
    "ширина":                                                     0.2,
    "высота":                                                    0.2,
    "диаметр":                                                   0.2,
    "объем":                                                     0.2,
    "вес":                                                       0.2,
    "толщина":                                                   0.2,
    "глубина":                                                   0.2,
    "размер":                                                    0.2,
    "формат":                                                    0.2,
    "габаритные размеры":                                        0.15,
}

_TIER1_KEYS: frozenset[str] = frozenset(k for k, w in _CHAR_WEIGHTS.items() if w >= 0.7)


# ── Нормализация ──────────────────────────────────────────────────

def _norm_key(key: str) -> str:
    return key.strip().lower()


def _norm_value(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def _parse_chars(raw: str | None) -> dict[str, str]:
    """'Ключ:Значение;Ключ2:Значение2' -> {norm_key: norm_value}.

    Хранит только ключи из _CHAR_WEIGHTS с весом > 0.
    """
    if not raw:
        return {}
    result: dict[str, str] = {}
    for pair in raw.split(";"):
        pair = pair.strip()
        if ":" not in pair:
            continue
        k, v = pair.split(":", 1)
        nk = _norm_key(k)
        if _CHAR_WEIGHTS.get(nk, 0.0) > 0.0:
            nv = _norm_value(v)
            if nv:
                result[nk] = nv
    return result


# ── IncrementalProductSearchV6 ────────────────────────────────────

class IncrementalProductSearchV6(IncrementalProductSearchV5):
    """v5 + характеристический индекс взаимозаменяемости.

    Текстовый поиск (search) — без изменений (наследуется от v5).
    Новый метод find_interchangeable(ste_id) — поиск аналогов по характеристикам.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        # (norm_key, norm_value) -> {doc_id}
        self.char_sim_index: dict[tuple[str, str], set[int]] = defaultdict(set)
        # doc_id -> {norm_key: norm_value}
        self.doc_characteristics: dict[int, dict[str, str]] = {}
        # ste_id -> doc_id
        self.ste_id_to_doc: dict[str, int] = {}

    # ── Индексирование характеристик ─────────────────────────────

    def _index_chars(self, doc_id: int, chars: dict[str, str]) -> None:
        self.doc_characteristics[doc_id] = chars
        for k, v in chars.items():
            self.char_sim_index[(k, v)].add(doc_id)

    def _deindex_chars(self, doc_id: int) -> None:
        for k, v in self.doc_characteristics.pop(doc_id, {}).items():
            self.char_sim_index[(k, v)].discard(doc_id)

    def add(
        self,
        text: str,
        metadata: dict | None = None,
        name: str | None = None,
    ) -> int:
        doc_id = super().add(text, metadata, name)
        meta = metadata or {}
        self._index_chars(doc_id, meta.get("_characteristics", {}))
        if ste_id := meta.get("ste_id"):
            self.ste_id_to_doc[ste_id] = doc_id
        return doc_id

    def remove(self, doc_id: int) -> bool:
        ste_id = self.doc_metadata.get(doc_id, {}).get("ste_id")
        self._deindex_chars(doc_id)
        if ste_id:
            self.ste_id_to_doc.pop(ste_id, None)
        return super().remove(doc_id)

    def update(
        self,
        doc_id: int,
        text: str,
        metadata: dict | None = None,
        name: str | None = None,
    ) -> bool:
        self._deindex_chars(doc_id)
        result = super().update(doc_id, text, metadata, name)
        if result:
            self._index_chars(doc_id, (metadata or {}).get("_characteristics", {}))
        return result

    # ── Поиск взаимозаменяемых ───────────────────────────────────

    def find_interchangeable(
        self,
        ste_id: str,
        top_n: int = 10,
        min_score: float = 0.3,
    ) -> list[dict]:
        """Находит коммерчески взаимозаменяемые СТЕ по характеристикам.

        Returns:
          list[dict] с ключами: ste_id, name, category, score, matched_keys.
          matched_keys = {key: value} — характеристики, совпавшие у обоих СТЕ.
        """
        src_doc_id = self.ste_id_to_doc.get(ste_id)
        if src_doc_id is None:
            return []

        src_chars = self.doc_characteristics.get(src_doc_id, {})
        if not src_chars:
            return []

        src_weight_sum = sum(_CHAR_WEIGHTS.get(k, 0.0) for k in src_chars)
        if src_weight_sum == 0.0:
            return []

        raw_scores: dict[int, float] = defaultdict(float)
        tier1_matched: set[int] = set()

        for k, v in src_chars.items():
            weight = _CHAR_WEIGHTS.get(k, 0.0)
            if weight == 0.0:
                continue
            for cand_id in self.char_sim_index.get((k, v), set()):
                if cand_id == src_doc_id:
                    continue
                raw_scores[cand_id] += weight
                if k in _TIER1_KEYS:
                    tier1_matched.add(cand_id)

        if not raw_scores:
            return []

        results: list[dict] = []
        for cand_id, raw in raw_scores.items():
            if cand_id not in tier1_matched:
                continue
            score = raw / src_weight_sum
            if score < min_score:
                continue

            cand_chars = self.doc_characteristics.get(cand_id, {})
            matched_keys = {
                k: v
                for k, v in src_chars.items()
                if cand_chars.get(k) == v and _CHAR_WEIGHTS.get(k, 0.0) > 0.0
            }

            meta = self.doc_metadata.get(cand_id, {})
            results.append({
                "ste_id":      meta.get("ste_id", ""),
                "name":        meta.get("name", ""),
                "category":    meta.get("category", ""),
                "score":       round(score, 4),
                "matched_keys": matched_keys,
            })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_n]

    def stats(self) -> dict:
        s = super().stats()
        s["version"]                   = "v6"
        s["char_sim_index_pairs"]      = len(self.char_sim_index)
        s["docs_with_characteristics"] = len(self.doc_characteristics)
        return s


# ── Загрузка характеристик из SQLite ─────────────────────────────

def _load_characteristics_from_db() -> dict[str, dict[str, str]]:
    """Загружает характеристики всех СТЕ из smartsearch.db."""
    if not DB_PATH.exists():
        print("  Предупреждение: smartsearch.db не найдена, характеристики не загружены.")
        return {}
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT ste_id, characteristics FROM ste").fetchall()
    conn.close()
    return {ste_id: _parse_chars(raw) for ste_id, raw in rows}


# ── Кеш ──────────────────────────────────────────────────────────

def _v6_cache_valid() -> bool:
    return (
        ENGINE_CACHE_V6.exists()
        and CACHE_DB.exists()
        and ENGINE_CACHE_V6.stat().st_mtime >= CACHE_DB.stat().st_mtime
    )


def _load_v6_cache() -> "IncrementalProductSearchV6 | None":
    try:
        with open(ENGINE_CACHE_V6, "rb") as f:
            return pickle.load(f)  # noqa: S301
    except Exception as e:
        print(f"  Предупреждение: не удалось загрузить кэш v6 ({e}), пересборка.")
        return None


def _save_v6_cache(engine: "IncrementalProductSearchV6") -> None:
    try:
        with open(ENGINE_CACHE_V6, "wb") as f:
            pickle.dump(engine, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"  Индекс v6 сохранён -> {ENGINE_CACHE_V6.name}")
    except Exception as e:
        print(f"  Предупреждение: не удалось сохранить кэш v6 ({e}).")


# ── Построение индекса ────────────────────────────────────────────

def build_engine_v6(
    data: list[dict],
    use_cache: bool = True,
) -> tuple["IncrementalProductSearchV6", float]:
    """Строит IncrementalProductSearchV6.

    Использует тот же SQLite-кеш лемматизации, что v4/v5.
    Дополнительно загружает характеристики из smartsearch.db.
    """
    if use_cache and _v6_cache_valid():
        t0 = time.time()
        engine = _load_v6_cache()
        if engine is not None:
            load_time = time.time() - t0
            print(f"  Индекс v6 загружен из кэша за {load_time:.2f}s  ({engine.size:,} docs)")
            return engine, load_time

    print("  Загрузка характеристик СТЕ из БД...")
    t_chars = time.time()
    char_map = _load_characteristics_from_db()
    print(f"  Характеристики: {len(char_map):,} СТЕ за {time.time() - t_chars:.1f}s")

    engine = IncrementalProductSearchV6()

    t0 = time.time()
    for doc_id, item in enumerate(data):
        original    = item["name"] + " " + item["category"]
        text_norm   = item["text_norm"]
        name_norm   = item["name_norm"]
        word_tokens = item["text_lemma"].split()

        engine.documents[doc_id]      = original
        engine.doc_metadata[doc_id]   = {
            "ste_id":   item["ste_id"],
            "name":     item["name"],
            "category": item["category"],
        }
        engine.doc_normalized[doc_id] = text_norm
        engine.doc_tokens[doc_id]     = re.findall(r"[а-яеa-z0-9]+", text_norm)

        engine._index_doc(doc_id, text_norm, name_norm, word_tokens)

        chars = char_map.get(item["ste_id"], {})
        engine._index_chars(doc_id, chars)
        engine.ste_id_to_doc[item["ste_id"]] = doc_id

        if (doc_id + 1) % 50_000 == 0:
            print(f"  Indexed v6: {doc_id + 1:,} ...")

    engine._next_id = len(data)
    build_time = time.time() - t0

    s = engine.stats()
    print(
        f"  Engine v6 built in {build_time:.2f}s  "
        f"(docs={s['total_documents']:,}, "
        f"char_pairs={s['char_sim_index_pairs']:,}, "
        f"with_chars={s['docs_with_characteristics']:,})"
    )

    if use_cache:
        _save_v6_cache(engine)

    return engine, build_time
