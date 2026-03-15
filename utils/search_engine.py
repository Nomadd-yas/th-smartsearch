"""
IncrementalProductSearch v4 — инкрементный поисковый движок для товарных каталогов.

Улучшения по сравнению с v3:
  9.  Мягкая (дробная) coverage: зона [50, threshold) вносит дробный вклад
      вместо бинарного 0/1 — тяжёлые опечатки больше не дают 0 результатов.
  10. Прогрессивный fallback в search(): если после строгого фильтра
      результатов < top_k, автоматически снижаем coverage_min (0.62 → 0.40 → 0.0).
  11. Pre-rank + ограниченный reranking: кандидаты сортируются по сумме BM25
      до дорогого fuzzy/coverage прохода → ускорение 3-словных запросов.
  12. Format token index: точный индекс форматных токенов (а1, а4, б5...)
      гарантирует попадание релевантных документов в кандидатный пул.

Все структуры данных — инкрементные: добавление документа = O(len(doc)).
"""

from __future__ import annotations

import math
import pickle  # noqa: S403 — файл генерируется локально нашим же кодом, внешний ввод не участвует
import re
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pymorphy3
from rapidfuzz import fuzz, utils as rfutils


# ─────────────────────────────────────────────────────────────────
#  Предобработка текста
# ─────────────────────────────────────────────────────────────────

_EN_TO_RU = str.maketrans(
    "qwertyuiop[]asdfghjkl;'zxcvbnm,./"
    'QWERTYUIOP{}ASDFGHJKL:"ZXCVBNM<>?',
    "йцукенгшщзхъфывапролджэячсмитьбю."
    "ЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЯЧСМИТЬБЮ,",
)

STOP_WORDS = frozenset(
    "и в на с для от из до о по к у а но не ни же ли бы то "
    "что как это все его она они мы вы он я".split()
)

_morph: Optional[pymorphy3.MorphAnalyzer] = None


def _get_morph() -> pymorphy3.MorphAnalyzer:
    global _morph
    if _morph is None:
        _morph = pymorphy3.MorphAnalyzer()
    return _morph


def normalize_text(text: str) -> str:
    """Нормализация: lowercase, ё→е, склейка форматных токенов, пунктуация → пробелы.

    FIX 1: "А-1" → "а1", "А-3" → "а3" — форматные обозначения становятся
    атомарными токенами и не дробятся символьными n-граммами.
    """
    text = text.lower().replace("ё", "е")
    # FIX 1: склеиваем форматные обозначения вида «буква-цифра»
    text = re.sub(r"([а-яa-z])-(\d)", r"\1\2", text)
    text = re.sub(r"[^\w\s.,/²³]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def fix_layout(text: str) -> str:
    """Если текст похож на английскую раскладку — конвертируем в русскую."""
    latin_count = sum(1 for c in text if "a" <= c.lower() <= "z")
    cyrillic_count = sum(1 for c in text if "а" <= c.lower() <= "я" or c == "ё")
    if latin_count > cyrillic_count and latin_count > 2:
        return text.translate(_EN_TO_RU)
    return text


def char_ngrams(text: str, ns: tuple[int, ...] = (3, 4)) -> list[str]:
    """Символьные n-граммы нескольких порядков."""
    grams = []
    for n in ns:
        for i in range(len(text) - n + 1):
            grams.append(text[i : i + n])
    return grams


def _extract_format_tokens(text_norm: str) -> set[str]:
    """Извлекает форматные токены вида а1, а4, б5 из нормализованного текста.

    FIX 12: используется для format_index — точного индекса форматных обозначений.
    """
    return {
        t for t in re.findall(r"[а-яa-z0-9]+", text_norm)
        if re.fullmatch(r"[а-яa-z]\d{1,2}", t)
    }


def tokenize_and_lemmatize(text: str) -> list[str]:
    """Токенизация + лемматизация через pymorphy3."""
    morph = _get_morph()
    tokens = re.findall(r"[а-яеa-z0-9]+", text.lower().replace("ё", "е"))
    result = []
    for tok in tokens:
        if tok in STOP_WORDS:
            continue
        parsed = morph.parse(tok)[0]
        lemma = parsed.normal_form
        result.append(lemma)
        if len(lemma) > 5:
            root = lemma[:6]
            if root != lemma:
                result.append(f"~{root}")
    return result


# ─────────────────────────────────────────────────────────────────
#  Инкрементный инвертированный индекс с BM25 скорингом
# ─────────────────────────────────────────────────────────────────

@dataclass
class _PostingList:
    doc_freq: int = 0
    postings: dict[int, int] = field(default_factory=dict)

    def add(self, doc_id: int, tf: int) -> None:
        if doc_id not in self.postings:
            self.doc_freq += 1
        self.postings[doc_id] = tf

    def remove(self, doc_id: int) -> None:
        if doc_id in self.postings:
            del self.postings[doc_id]
            self.doc_freq -= 1


class InvertedIndex:
    """Инкрементный инвертированный индекс с BM25 скорингом."""

    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.index: dict[str, _PostingList] = defaultdict(_PostingList)
        self.doc_lengths: dict[int, int] = {}
        self.total_docs: int = 0
        self.total_length: int = 0

    @property
    def avg_doc_length(self) -> float:
        return self.total_length / self.total_docs if self.total_docs else 1.0

    def add_document(self, doc_id: int, terms: list[str]) -> None:
        if doc_id in self.doc_lengths:
            self.remove_document(doc_id)
        tf_map: dict[str, int] = defaultdict(int)
        for term in terms:
            tf_map[term] += 1
        for term, tf in tf_map.items():
            self.index[term].add(doc_id, tf)
        self.doc_lengths[doc_id] = len(terms)
        self.total_docs += 1
        self.total_length += len(terms)

    def remove_document(self, doc_id: int) -> None:
        if doc_id not in self.doc_lengths:
            return
        to_remove = []
        for term, pl in self.index.items():
            if doc_id in pl.postings:
                pl.remove(doc_id)
                if pl.doc_freq == 0:
                    to_remove.append(term)
        for term in to_remove:
            del self.index[term]
        self.total_length -= self.doc_lengths[doc_id]
        self.total_docs -= 1
        del self.doc_lengths[doc_id]

    def query_bm25(
        self, query_terms: list[str], top_k: int = 100
    ) -> list[tuple[int, float]]:
        if not self.total_docs:
            return []
        avgdl = self.avg_doc_length
        N = self.total_docs
        scores: dict[int, float] = defaultdict(float)
        qtf: dict[str, int] = defaultdict(int)
        for t in query_terms:
            qtf[t] += 1
        for term, qf in qtf.items():
            pl = self.index.get(term)
            if pl is None or pl.doc_freq == 0:
                continue
            df = pl.doc_freq
            idf = math.log((N - df + 0.5) / (df + 0.5) + 1.0)
            for doc_id, tf in pl.postings.items():
                dl = self.doc_lengths[doc_id]
                tf_norm = (tf * (self.k1 + 1)) / (
                    tf + self.k1 * (1 - self.b + self.b * dl / avgdl)
                )
                scores[doc_id] += idf * tf_norm
        if not scores:
            return []
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]


# ─────────────────────────────────────────────────────────────────
#  Основной класс: IncrementalProductSearch v4
# ─────────────────────────────────────────────────────────────────

@dataclass
class SearchResult:
    doc_id: int
    text: str
    score: float
    char_score: float
    word_score: float
    fuzzy_score: float
    name_score: float
    format_score: float = 0.0   # FIX 12
    fallback_level: int = 0     # FIX 10: 0=strict, 1=relaxed, 2=no-filter
    metadata: dict = field(default_factory=dict)


class IncrementalProductSearch:
    """
    Инкрементный поисковый движок v4.

    v3: форматные токены в coverage; coverage_min=0.62; fuzzy threshold=73;
        name_index retrieval = top_k//2.
    FIX 9   _word_coverage: мягкая дробная coverage (зона [50, threshold) → 0..1).
    FIX 10  search(): прогрессивный fallback coverage_min (0.62 → 0.40 → 0.0).
    FIX 11  search(): pre-rank по BM25, fuzzy/coverage только на rerank_top_k.
    FIX 12  format_index: точный индекс форматных токенов (а1, а4...),
            гарантирует попадание релевантных документов в кандидатный пул.
    """

    def __init__(
        self,
        char_ngram_sizes: tuple[int, ...] = (3, 4),
        char_weight: float = 0.38,
        word_weight: float = 0.19,
        fuzzy_weight: float = 0.24,
        name_weight: float = 0.14,
        format_weight: float = 0.05,    # FIX 12: бонус за точное совпадение формата
        bm25_k1: float = 1.5,
        bm25_b: float = 0.75,
        retrieval_top_k: int = 100,
        result_top_k: int = 20,
        rerank_top_k: int = 150,        # FIX 11: лимит кандидатов для fuzzy/coverage
        format_top_k: int = 30,         # FIX 12: макс. новых кандидатов из format_index
        coverage_min: float = 0.62,
        coverage_power: float = 0.7,
    ):
        self.char_ngram_sizes = char_ngram_sizes
        self.char_weight = char_weight
        self.word_weight = word_weight
        self.fuzzy_weight = fuzzy_weight
        self.name_weight = name_weight
        self.format_weight = format_weight
        self.retrieval_top_k = retrieval_top_k
        self.result_top_k = result_top_k
        self.rerank_top_k = rerank_top_k
        self.format_top_k = format_top_k
        self.coverage_min = coverage_min
        self.coverage_power = coverage_power

        self.char_index = InvertedIndex(k1=bm25_k1, b=bm25_b)
        self.word_index = InvertedIndex(k1=bm25_k1, b=bm25_b)
        self.name_index = InvertedIndex(k1=bm25_k1, b=bm25_b)

        self.documents: dict[int, str] = {}
        self.doc_metadata: dict[int, dict] = {}
        self.doc_normalized: dict[int, str] = {}
        self.doc_tokens: dict[int, list[str]] = {}
        # FIX 12: format token index
        self.format_index: dict[str, set[int]] = defaultdict(set)
        self.doc_format_tokens: dict[int, set[str]] = {}
        self._next_id: int = 0

    @property
    def size(self) -> int:
        return len(self.documents)

    # ── Инкрементные операции ──────────────────────────────────

    def add(
        self,
        text: str,
        metadata: dict | None = None,
        name: str | None = None,
    ) -> int:
        """
        Добавить товар в индекс. Возвращает doc_id.

        name — отдельное поле для name_index (FIX 4).
               Если None — используется весь text.
        """
        doc_id = self._next_id
        self._next_id += 1

        self.documents[doc_id] = text
        self.doc_metadata[doc_id] = metadata or {}

        normalized = normalize_text(text)
        self.doc_normalized[doc_id] = normalized

        self.doc_tokens[doc_id] = re.findall(r"[а-яеa-z0-9]+", normalized)

        cgrams = char_ngrams(normalized, self.char_ngram_sizes)
        self.char_index.add_document(doc_id, cgrams)

        word_tokens = tokenize_and_lemmatize(normalized)
        self.word_index.add_document(doc_id, word_tokens)

        name_norm = normalize_text(name if name is not None else text)
        name_cgrams = char_ngrams(name_norm, self.char_ngram_sizes)
        self.name_index.add_document(doc_id, name_cgrams)

        # FIX 12: format token index
        fmt_tokens = _extract_format_tokens(normalized)
        self.doc_format_tokens[doc_id] = fmt_tokens
        for tok in fmt_tokens:
            self.format_index[tok].add(doc_id)

        return doc_id

    def add_batch(self, items: list[str | tuple[str, dict]]) -> list[int]:
        ids = []
        for item in items:
            if isinstance(item, tuple):
                text, meta = item
                ids.append(self.add(text, meta))
            else:
                ids.append(self.add(item))
        return ids

    def remove(self, doc_id: int) -> bool:
        if doc_id not in self.documents:
            return False
        self.char_index.remove_document(doc_id)
        self.word_index.remove_document(doc_id)
        self.name_index.remove_document(doc_id)
        # FIX 12: clean format index
        for tok in self.doc_format_tokens.pop(doc_id, set()):
            self.format_index[tok].discard(doc_id)
        del self.documents[doc_id]
        del self.doc_normalized[doc_id]
        del self.doc_tokens[doc_id]
        self.doc_metadata.pop(doc_id, None)
        return True

    def update(
        self,
        doc_id: int,
        text: str,
        metadata: dict | None = None,
        name: str | None = None,
    ) -> bool:
        if doc_id not in self.documents:
            return False
        self.char_index.remove_document(doc_id)
        self.word_index.remove_document(doc_id)
        self.name_index.remove_document(doc_id)

        self.documents[doc_id] = text
        if metadata is not None:
            self.doc_metadata[doc_id] = metadata

        normalized = normalize_text(text)
        self.doc_normalized[doc_id] = normalized
        self.doc_tokens[doc_id] = re.findall(r"[а-яеa-z0-9]+", normalized)

        cgrams = char_ngrams(normalized, self.char_ngram_sizes)
        self.char_index.add_document(doc_id, cgrams)

        word_tokens = tokenize_and_lemmatize(normalized)
        self.word_index.add_document(doc_id, word_tokens)

        name_norm = normalize_text(name if name is not None else text)
        name_cgrams = char_ngrams(name_norm, self.char_ngram_sizes)
        self.name_index.add_document(doc_id, name_cgrams)

        # FIX 12: refresh format index
        for tok in self.doc_format_tokens.pop(doc_id, set()):
            self.format_index[tok].discard(doc_id)
        fmt_tokens = _extract_format_tokens(normalized)
        self.doc_format_tokens[doc_id] = fmt_tokens
        for tok in fmt_tokens:
            self.format_index[tok].add(doc_id)

        return True

    # ── Поиск ──────────────────────────────────────────────────

    @staticmethod
    def _word_coverage(
        query_words: list[str],
        doc_token_list: list[str],
        threshold: float = 73.0,
    ) -> float:
        """
        FIX 9: Мягкая (дробная) coverage.

        Каждое слово запроса вносит вклад:
          best >= threshold  → 1.0   (полное покрытие)
          50 <= best < threshold → (best-50)/(threshold-50)   (частичное)
          best < 50          → 0.0   (нет покрытия)

        Итог — среднее по словам ∈ [0.0, 1.0].
        """
        if not query_words:
            return 1.0
        total = 0.0
        for qw in query_words:
            best = 0.0
            for dt in doc_token_list:
                if not (set(qw) & set(dt)):
                    continue
                score = fuzz.ratio(qw, dt, processor=None)
                if score > best:
                    best = score
                if best >= threshold:
                    break
            if best >= threshold:
                total += 1.0
            elif best >= 50.0:
                total += (best - 50.0) / (threshold - 50.0)
        return total / len(query_words)

    def search(
        self,
        query: str,
        top_k: int | None = None,
        min_score: float = 0.01,
    ) -> list[SearchResult]:
        top_k = top_k or self.result_top_k

        if not self.documents:
            return []

        query = fix_layout(query)
        query_norm = normalize_text(query)

        query_content_words = [
            w for w in re.findall(r"[а-яёa-z0-9]+", query_norm)
            if w not in STOP_WORDS and len(w) >= 2
        ]
        n_content = len(query_content_words)

        # ── Retrieval ──────────────────────────────────────────
        query_cgrams = char_ngrams(query_norm, self.char_ngram_sizes)
        char_results = self.char_index.query_bm25(query_cgrams, top_k=self.retrieval_top_k)

        query_words = tokenize_and_lemmatize(query_norm)
        word_results = self.word_index.query_bm25(query_words, top_k=self.retrieval_top_k)

        name_results = self.name_index.query_bm25(query_cgrams, top_k=self.retrieval_top_k // 2)

        char_scores: dict[int, float] = {}
        word_scores: dict[int, float] = {}
        name_scores: dict[int, float] = {}

        if char_results:
            max_c = max(s for _, s in char_results) or 1.0
            for doc_id, s in char_results:
                char_scores[doc_id] = s / max_c
        if word_results:
            max_w = max(s for _, s in word_results) or 1.0
            for doc_id, s in word_results:
                word_scores[doc_id] = s / max_w
        if name_results:
            max_n = max(s for _, s in name_results) or 1.0
            for doc_id, s in name_results:
                name_scores[doc_id] = s / max_n

        all_candidates = set(char_scores) | set(word_scores) | set(name_scores)

        # FIX 12: добавляем кандидатов из format_index
        query_fmt = _extract_format_tokens(query_norm)
        format_candidates: set[int] = set()
        for tok in query_fmt:
            format_candidates |= self.format_index.get(tok, set())
        # Только новые (не найденные BM25), ограниченное число
        new_fmt = format_candidates - all_candidates
        if len(new_fmt) > self.format_top_k:
            new_fmt = set(list(new_fmt)[: self.format_top_k])
        all_candidates |= new_fmt

        if not all_candidates:
            return []

        # FIX 11: pre-rank по сумме BM25, ограничиваем reranking
        def _bm25_sum(d: int) -> float:
            return char_scores.get(d, 0.0) + word_scores.get(d, 0.0) + name_scores.get(d, 0.0)

        pre_ranked = sorted(all_candidates, key=_bm25_sum, reverse=True)[: self.rerank_top_k]
        # format_candidates всегда в пуле, даже если выпали из top
        candidates_to_rerank: list[int] = list(
            dict.fromkeys(pre_ranked + list(format_candidates))
        )

        # ── Scoring (дорогой проход — один раз) ───────────────
        # (doc_id, raw, coverage, cs, ws, ns, fs, fmt)
        ScoredItem = tuple[int, float, float, float, float, float, float, float]
        all_scored: list[ScoredItem] = []

        for doc_id in candidates_to_rerank:
            cs  = char_scores.get(doc_id, 0.0)
            ws  = word_scores.get(doc_id, 0.0)
            ns  = name_scores.get(doc_id, 0.0)
            fmt = 1.0 if doc_id in format_candidates else 0.0

            fuzzy_wr = fuzz.WRatio(
                query, self.documents[doc_id],
                processor=rfutils.default_process,
            ) / 100.0
            fuzzy_pr = fuzz.partial_ratio(
                query_norm, self.doc_normalized[doc_id],
            ) / 100.0
            fs = max(fuzzy_wr, fuzzy_pr)

            # FIX 9: мягкая coverage
            coverage = self._word_coverage(query_content_words, self.doc_tokens[doc_id])

            raw = (
                self.char_weight  * cs
                + self.word_weight  * ws
                + self.fuzzy_weight * fs
                + self.name_weight  * ns
                + self.format_weight * fmt  # FIX 12
            )
            all_scored.append((doc_id, raw, coverage, cs, ws, ns, fs, fmt))

        # FIX 10: прогрессивный fallback — снижаем coverage_min пока не наберём top_k
        fallback_levels = [self.coverage_min, 0.40, 0.0] if n_content >= 2 else [0.0]
        results: list[SearchResult] = []
        seen: set[int] = set()

        for level, fb_min in enumerate(fallback_levels):
            if len(results) >= top_k:
                break
            for doc_id, raw, coverage, cs, ws, ns, fs, fmt in all_scored:
                if doc_id in seen:
                    continue
                if n_content >= 2 and coverage < fb_min:
                    continue
                coverage_boost = coverage ** self.coverage_power
                final = raw * coverage_boost
                if final >= min_score:
                    results.append(
                        SearchResult(
                            doc_id=doc_id,
                            text=self.documents[doc_id],
                            score=final,
                            char_score=cs,
                            word_score=ws,
                            fuzzy_score=fs,
                            name_score=ns,
                            format_score=fmt,
                            fallback_level=level,
                            metadata=self.doc_metadata.get(doc_id, {}),
                        )
                    )
                    seen.add(doc_id)
            results.sort(key=lambda r: r.score, reverse=True)

        return results[:top_k]

    # ── Диагностика ────────────────────────────────────────────

    def stats(self) -> dict:
        return {
            "total_documents":    self.size,
            "char_index_terms":   len(self.char_index.index),
            "word_index_terms":   len(self.word_index.index),
            "name_index_terms":   len(self.name_index.index),
            "format_index_tokens": len(self.format_index),  # FIX 12
            "char_avg_doc_len":   round(self.char_index.avg_doc_length, 1),
            "word_avg_doc_len":   round(self.word_index.avg_doc_length, 1),
        }


# ─────────────────────────────────────────────────────────────────
#  Бенчмарк — загрузка СТЕ и тестирование поиска
# ─────────────────────────────────────────────────────────────────

DATA_DIR     = Path(__file__).parent.parent
ENGINE_CACHE = DATA_DIR / "data" / "engine_cache.pkl"    # сериализованный индекс

from .db import DB_PATH as CACHE_DB  # noqa: E402 — импорт после DATA_DIR


def _engine_cache_valid() -> bool:
    """Кэш индекса актуален, если он новее кэша лемматизации."""
    return (
        ENGINE_CACHE.exists()
        and CACHE_DB.exists()
        and ENGINE_CACHE.stat().st_mtime >= CACHE_DB.stat().st_mtime
    )


def _load_engine_cache() -> "IncrementalProductSearch | None":
    try:
        with open(ENGINE_CACHE, "rb") as f:
            return pickle.load(f)  # noqa: S301
    except Exception as e:
        print(f"  Предупреждение: не удалось загрузить кэш индекса ({e}), пересборка.")
        return None


def _save_engine_cache(engine: "IncrementalProductSearch") -> None:
    try:
        with open(ENGINE_CACHE, "wb") as f:
            pickle.dump(engine, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"  Индекс сохранён → {ENGINE_CACHE.name}")
    except Exception as e:
        print(f"  Предупреждение: не удалось сохранить кэш индекса ({e}).")

TEST_QUERIES = [
    "бумага А-1",
    "Aкварельная бумага",
    "Сальфетки обезжиривающие",
]


def load_data(limit: int = 0) -> list[dict]:
    """Загружает данные СТЕ из smartsearch.db.

    Требует предварительного запуска миграции: python -m utils.migrate_to_db
    """
    if not CACHE_DB.exists():
        raise RuntimeError(
            f"База данных не найдена: {CACHE_DB}\n"
            "Запустите миграцию: python -m utils.migrate_to_db"
        )
    return _load_cache(limit)


def _load_cache(limit: int) -> list[dict]:
    t0 = time.time()
    conn = sqlite3.connect(CACHE_DB)
    q = "SELECT ste_id, name, category, text_norm, name_norm, text_lemma FROM ste"
    if limit:
        q += f" LIMIT {limit}"
    rows = conn.execute(q).fetchall()
    conn.close()
    data = [
        {
            "ste_id": r[0], "name": r[1], "category": r[2],
            "text_norm": r[3], "name_norm": r[4], "text_lemma": r[5],
        }
        for r in rows
    ]
    print(f"Loaded {len(data)} records from cache ({time.time() - t0:.2f}s)")
    return data


def build_engine(data: list[dict], use_cache: bool = True) -> tuple["IncrementalProductSearch", float]:
    """Строит IncrementalProductSearch v4 из предобработанных данных.

    При use_cache=True (по умолчанию):
      - Если кэш индекса актуален — загружает из pickle (быстро).
      - Если нет — строит индекс и сохраняет в pickle для следующего запуска.
    """
    if use_cache and _engine_cache_valid():
        t0 = time.time()
        engine = _load_engine_cache()
        if engine is not None:
            load_time = time.time() - t0
            print(f"  Индекс загружен из кэша за {load_time:.2f}s  ({engine.size:,} docs)")
            return engine, load_time

    engine = IncrementalProductSearch()

    t0 = time.time()
    for doc_id, item in enumerate(data):
        original  = item["name"] + " " + item["category"]
        text_norm = item["text_norm"]
        name_norm = item["name_norm"]
        word_tokens = item["text_lemma"].split()

        engine.documents[doc_id]      = original
        engine.doc_metadata[doc_id]   = {
            "ste_id":   item["ste_id"],
            "name":     item["name"],
            "category": item["category"],
        }
        engine.doc_normalized[doc_id] = text_norm

        # FIX 3: кеш токенов
        engine.doc_tokens[doc_id] = re.findall(r"[а-яеa-z0-9]+", text_norm)

        cgrams = char_ngrams(text_norm, engine.char_ngram_sizes)
        engine.char_index.add_document(doc_id, cgrams)
        engine.word_index.add_document(doc_id, word_tokens)

        # FIX 4: name index
        name_cgrams = char_ngrams(name_norm, engine.char_ngram_sizes)
        engine.name_index.add_document(doc_id, name_cgrams)

        # FIX 12: format token index
        fmt_tokens = _extract_format_tokens(text_norm)
        engine.doc_format_tokens[doc_id] = fmt_tokens
        for tok in fmt_tokens:
            engine.format_index[tok].add(doc_id)

        if (doc_id + 1) % 50_000 == 0:
            print(f"  Indexed {doc_id + 1} ...")

    engine._next_id = len(data)
    build_time = time.time() - t0
    s = engine.stats()
    print(
        f"  Engine v4 built in {build_time:.2f}s  "
        f"(docs={s['total_documents']:,}, "
        f"char_terms={s['char_index_terms']:,}, "
        f"word_terms={s['word_index_terms']:,}, "
        f"name_terms={s['name_index_terms']:,}, "
        f"fmt_tokens={s['format_index_tokens']:,})"
    )

    if use_cache:
        _save_engine_cache(engine)

    return engine, build_time


def _print_results(query: str, results: list[SearchResult], search_ms: float) -> None:
    print(f"\n  Query: \"{query}\"  [{search_ms:.1f} ms]")
    print(
        f"  {'#':<3} {'Score':<8} {'ch':<6} {'wd':<6} {'fz':<6} {'nm':<6} {'fmt':<4} {'fb':<3} "
        f"{'Name':<50} {'Category':<22} STE_ID"
    )
    print(
        f"  {'─'*3} {'─'*8} {'─'*6} {'─'*6} {'─'*6} {'─'*6} {'─'*4} {'─'*3} "
        f"{'─'*50} {'─'*22} {'─'*10}"
    )
    for i, r in enumerate(results, 1):
        name  = r.metadata.get("name", r.text)
        cat   = r.metadata.get("category", "")
        sid   = r.metadata.get("ste_id", "")
        name_s = (name[:47] + "...") if len(name) > 50 else name
        cat_s  = (cat[:20] + "..") if len(cat) > 22 else cat
        print(
            f"  {i:<3} {r.score:<8.4f} {r.char_score:<6.3f} {r.word_score:<6.3f} "
            f"{r.fuzzy_score:<6.3f} {r.name_score:<6.3f} {r.format_score:<4.0f} {r.fallback_level:<3} "
            f"{name_s:<50} {cat_s:<22} {sid}"
        )


def _append_to_cache(item: dict) -> None:
    """Дописывает одну запись в SQLite-кеш (для сохранения добавленных СТЕ)."""
    conn = sqlite3.connect(CACHE_DB)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS ste "
        "(ste_id TEXT, name TEXT, category TEXT, "
        " text_norm TEXT, name_norm TEXT, text_lemma TEXT)"
    )
    conn.execute(
        "INSERT INTO ste VALUES (?,?,?,?,?,?)",
        (item["ste_id"], item["name"], item["category"],
         item["text_norm"], item["name_norm"], item["text_lemma"]),
    )
    conn.commit()
    conn.close()


def run_interactive() -> None:
    """
    Интерактивный режим: поиск по живому индексу + добавление новых СТЕ.

    Команды:
      <запрос>                     — поиск
      /add <название> | <категория> — добавить новую СТЕ в индекс (и в кеш)
      /add <название>              — добавить без категории
      /stats                       — статистика индекса
      /top <N>                     — изменить число результатов (по умолч. 10)
      /q                           — выход
    """
    print("=" * 70)
    print("IncrementalProductSearch v4 — интерактивный режим")
    print("=" * 70)

    data = load_data()
    engine, build_time = build_engine(data)
    print(f"\nГотов к поиску. {engine.size:,} записей СТЕ в индексе.")
    print("Команды: /add <название> | <категория>  /stats  /top <N>  /q\n")

    top_k = 10

    while True:
        try:
            raw = input(">> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nВыход.")
            break

        if not raw:
            continue

        # ── Команды ──────────────────────────────────────────
        if raw == "/q" or raw == "/quit":
            print("Выход.")
            break

        if raw == "/stats":
            s = engine.stats()
            print(f"  Документов:       {s['total_documents']:,}")
            print(f"  Char-термов:      {s['char_index_terms']:,}")
            print(f"  Word-термов:      {s['word_index_terms']:,}")
            print(f"  Name-термов:      {s['name_index_terms']:,}")
            print(f"  Format-токенов:   {s['format_index_tokens']:,}")
            print(f"  Avg char doc len: {s['char_avg_doc_len']}")
            print(f"  Avg word doc len: {s['word_avg_doc_len']}")
            continue

        if raw.startswith("/top "):
            try:
                top_k = int(raw.split()[1])
                print(f"  Показывать топ-{top_k} результатов.")
            except (IndexError, ValueError):
                print("  Использование: /top <число>")
            continue

        if raw.startswith("/add "):
            parts = raw[5:].split("|", 1)
            name = parts[0].strip()
            category = parts[1].strip() if len(parts) > 1 else ""

            if not name:
                print("  Использование: /add <название> | <категория>")
                continue

            # Предобработка
            combined = name + " " + category
            norm = normalize_text(combined)
            item = {
                "ste_id":     "НОВЫЙ",
                "name":       name,
                "category":   category,
                "text_norm":  norm,
                "name_norm":  normalize_text(name),
                "text_lemma": " ".join(tokenize_and_lemmatize(norm)),
            }

            t0 = time.time()
            doc_id = engine.add(
                text=combined,
                metadata={"ste_id": item["ste_id"], "name": name, "category": category},
                name=name,
            )
            # Синхронизируем кешированные токены (add() строит через normalize_text,
            # но doc_tokens уже заполнен внутри add())
            add_ms = (time.time() - t0) * 1000

            # Сохраняем в кеш, чтобы следующий запуск подхватил запись
            _append_to_cache(item)

            print(
                f"  Добавлено: doc_id={doc_id}  [{add_ms:.1f} ms]  "
                f"Всего в индексе: {engine.size:,}"
            )
            continue

        # ── Поиск ────────────────────────────────────────────
        t0 = time.time()
        results = engine.search(raw, top_k=top_k)
        search_ms = (time.time() - t0) * 1000
        _print_results(raw, results, search_ms)


def main() -> None:
    import sys
    if "-i" in sys.argv or "--interactive" in sys.argv:
        run_interactive()
    else:
        _run_benchmark()


def _run_benchmark() -> None:
    print("=" * 80)
    print("BENCHMARK: IncrementalProductSearch v4")
    print("  v3: len>=2 coverage / coverage_min=0.62 / threshold=73 / name_index top_k//2")
    print("  FIX9:  мягкая coverage: зона [50, threshold) → дробный вклад (не 0/1)")
    print("  FIX10: прогрессивный fallback coverage_min (0.62 → 0.40 → 0.0)")
    print("  FIX11: pre-rank по BM25, fuzzy/coverage только на rerank_top_k=150")
    print("  FIX12: format_index (а1, а4...) гарантирует попадание форматных доков")
    print("=" * 80)

    data = load_data()
    engine, build_time = build_engine(data)

    total_ms = 0.0
    for query in TEST_QUERIES:
        t0 = time.time()
        results = engine.search(query)
        search_ms = (time.time() - t0) * 1000
        _print_results(query, results, search_ms)
        total_ms += search_ms

    print(f"\n  Avg query time: {total_ms / len(TEST_QUERIES):.1f} ms")
    print(f"  Index build:    {build_time:.2f}s")


if __name__ == "__main__":
    main()
