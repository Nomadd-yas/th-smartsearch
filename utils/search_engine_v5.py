"""
IncrementalProductSearch v5 — улучшенный инкрементный движок.

Новое по сравнению с v4:
  1. Биграммный индекс (word bigrams) — фразовое совпадение.
     "бумага офисная а4" → биграммы "бумага_офисная", "офисная_а4".
     Документы, где слова запроса идут подряд, получают сильный бонус.
  2. Атрибутный индекс — точный матчинг числовых значений и технических кодов
     (80, 160, 5w40, ip65, гост12345).  Extracted = set → O(1) lookup.
  3. RRF (Reciprocal Rank Fusion) для объединения кандидатов от 4 индексов
     (char, word, bigram, name) — не требует ручной настройки весов, устойчив
     к разбросу масштабов BM25.
  4. Биграммный и атрибутный score в финальном взвешенном ранжировании.

Инкрементальность: add/remove/update — O(len(doc)).
Совместимость: возвращает те же SearchResult, что v4.
"""

from __future__ import annotations

import math
import pickle  # noqa: S403
import re
import time
from collections import defaultdict
from pathlib import Path

from rapidfuzz import fuzz, utils as rfutils

from .search_engine import (
    STOP_WORDS,
    InvertedIndex,
    SearchResult,
    _extract_format_tokens,
    char_ngrams,
    fix_layout,
    normalize_text,
    tokenize_and_lemmatize,
    load_data,
    CACHE_DB,
    DATA_DIR,
)

ENGINE_CACHE_V5 = DATA_DIR / "data" / "engine_cache_v5.pkl"


# ─────────────────────────────────────────────────────────────────
#  Вспомогательные функции v5
# ─────────────────────────────────────────────────────────────────

def _word_bigrams(tokens: list[str]) -> list[str]:
    """Последовательные биграммы из токенов.

    ["бумага", "офисная", "а4"] → ["бумага\x00офисная", "офисная\x00а4"]
    Символ \\x00 — разделитель, не встречается в словах.
    """
    return [f"{tokens[i]}\x00{tokens[i + 1]}" for i in range(len(tokens) - 1)]


def _extract_attributes(text_norm: str) -> set[str]:
    """Числовые значения и технические коды для точного матчинга.

    Примеры: "бумага 80г/м2 а4" → {"80", "а4"}
             "масло 5w40"        → {"5w40", "5", "40"}
             "ip65"              → {"ip65"}
    """
    attrs: set[str] = set()
    # Числа (целые и десятичные)
    for m in re.finditer(r"\b\d+(?:[.,]\d+)?\b", text_norm):
        attrs.add(m.group().replace(",", "."))
    # digit+letter+digit (5w40, ip65)
    for m in re.finditer(r"\b\d+[a-zа-яе]+\d*\b", text_norm):
        attrs.add(m.group())
    # letter+digit (а4, б5, ip) — уже есть в format_index, дублируем для единства
    for m in re.finditer(r"\b[a-zа-яе]{1,3}\d{1,4}\b", text_norm):
        attrs.add(m.group())
    return attrs


def _rrf_combine(
    ranked_lists: list[list[tuple[int, float]]],
    k: int = 60,
) -> dict[int, float]:
    """Reciprocal Rank Fusion: score(d) = Σ 1/(k + rank_i + 1)."""
    scores: dict[int, float] = defaultdict(float)
    for ranked in ranked_lists:
        for rank, (doc_id, _) in enumerate(ranked):
            scores[doc_id] += 1.0 / (k + rank + 1)
    return dict(scores)


# ─────────────────────────────────────────────────────────────────
#  IncrementalProductSearchV5
# ─────────────────────────────────────────────────────────────────

class IncrementalProductSearchV5:
    """Инкрементный поисковый движок v5.

    Индексы:
      char_index   — символьные n-граммы (опечатки, частичные совпадения)
      word_index   — лемматизированные слова (морфология)
      bigram_index — биграммы лемм (фразовое совпадение)  ← NEW
      name_index   — символьные n-граммы по полю name (вес на название)
      format_index — точный индекс форматных токенов (а4, б2...)
      attr_index   — точный индекс числовых/технических атрибутов  ← NEW
    """

    def __init__(
        self,
        char_ngram_sizes: tuple[int, ...] = (3, 4),
        # Веса финального scoring (сумма = 1.0)
        char_weight: float   = 0.20,
        word_weight: float   = 0.17,
        bigram_weight: float = 0.28,   # ← NEW: высокий — фраза важна
        fuzzy_weight: float  = 0.17,
        name_weight: float   = 0.12,
        format_weight: float = 0.04,
        attr_weight: float   = 0.02,   # ← NEW
        bm25_k1: float = 1.5,
        bm25_b: float = 0.75,
        retrieval_top_k: int = 100,
        result_top_k: int = 20,
        rerank_top_k: int = 150,
        rrf_k: int = 60,
        coverage_min: float = 0.62,
        coverage_power: float = 0.7,
    ):
        self.char_ngram_sizes = char_ngram_sizes
        self.char_weight    = char_weight
        self.word_weight    = word_weight
        self.bigram_weight  = bigram_weight
        self.fuzzy_weight   = fuzzy_weight
        self.name_weight    = name_weight
        self.format_weight  = format_weight
        self.attr_weight    = attr_weight
        self.retrieval_top_k = retrieval_top_k
        self.result_top_k   = result_top_k
        self.rerank_top_k   = rerank_top_k
        self.rrf_k          = rrf_k
        self.coverage_min   = coverage_min
        self.coverage_power = coverage_power

        self.char_index   = InvertedIndex(k1=bm25_k1, b=bm25_b)
        self.word_index   = InvertedIndex(k1=bm25_k1, b=bm25_b)
        self.bigram_index = InvertedIndex(k1=bm25_k1, b=0.5)   # меньший b для биграмм
        self.name_index   = InvertedIndex(k1=bm25_k1, b=bm25_b)

        self.documents:       dict[int, str]       = {}
        self.doc_metadata:    dict[int, dict]      = {}
        self.doc_normalized:  dict[int, str]       = {}
        self.doc_tokens:      dict[int, list[str]] = {}

        self.format_index:     dict[str, set[int]] = defaultdict(set)
        self.doc_format_tokens: dict[int, set[str]] = {}
        self.attr_index:       dict[str, set[int]] = defaultdict(set)
        self.doc_attrs:        dict[int, set[str]] = {}

        self._next_id: int = 0

    @property
    def size(self) -> int:
        return len(self.documents)

    # ── Индексирование ──────────────────────────────────────────

    def _index_doc(
        self,
        doc_id: int,
        text_norm: str,
        name_norm: str,
        word_tokens: list[str],
    ) -> None:
        cgrams = char_ngrams(text_norm, self.char_ngram_sizes)
        self.char_index.add_document(doc_id, cgrams)

        self.word_index.add_document(doc_id, word_tokens)

        bigrams = _word_bigrams(word_tokens)
        self.bigram_index.add_document(doc_id, bigrams)

        name_cgrams = char_ngrams(name_norm, self.char_ngram_sizes)
        self.name_index.add_document(doc_id, name_cgrams)

        fmt_tokens = _extract_format_tokens(text_norm)
        self.doc_format_tokens[doc_id] = fmt_tokens
        for tok in fmt_tokens:
            self.format_index[tok].add(doc_id)

        attrs = _extract_attributes(text_norm)
        self.doc_attrs[doc_id] = attrs
        for attr in attrs:
            self.attr_index[attr].add(doc_id)

    def _deindex_doc(self, doc_id: int) -> None:
        self.char_index.remove_document(doc_id)
        self.word_index.remove_document(doc_id)
        self.bigram_index.remove_document(doc_id)
        self.name_index.remove_document(doc_id)
        for tok in self.doc_format_tokens.pop(doc_id, set()):
            self.format_index[tok].discard(doc_id)
        for attr in self.doc_attrs.pop(doc_id, set()):
            self.attr_index[attr].discard(doc_id)

    def add(
        self,
        text: str,
        metadata: dict | None = None,
        name: str | None = None,
    ) -> int:
        doc_id = self._next_id
        self._next_id += 1

        self.documents[doc_id]     = text
        self.doc_metadata[doc_id]  = metadata or {}

        normalized = normalize_text(text)
        self.doc_normalized[doc_id] = normalized
        self.doc_tokens[doc_id]    = re.findall(r"[а-яеa-z0-9]+", normalized)

        word_tokens = tokenize_and_lemmatize(normalized)
        name_norm   = normalize_text(name if name is not None else text)
        self._index_doc(doc_id, normalized, name_norm, word_tokens)
        return doc_id

    def remove(self, doc_id: int) -> bool:
        if doc_id not in self.documents:
            return False
        self._deindex_doc(doc_id)
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
        self._deindex_doc(doc_id)

        self.documents[doc_id] = text
        if metadata is not None:
            self.doc_metadata[doc_id] = metadata

        normalized = normalize_text(text)
        self.doc_normalized[doc_id] = normalized
        self.doc_tokens[doc_id]    = re.findall(r"[а-яеa-z0-9]+", normalized)

        word_tokens = tokenize_and_lemmatize(normalized)
        name_norm   = normalize_text(name if name is not None else text)
        self._index_doc(doc_id, normalized, name_norm, word_tokens)
        return True

    # ── Поиск ───────────────────────────────────────────────────

    @staticmethod
    def _word_coverage(
        query_words: list[str],
        doc_token_list: list[str],
        threshold: float = 73.0,
    ) -> float:
        """Мягкая дробная coverage (идентична v4)."""
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

        query     = fix_layout(query)
        query_norm = normalize_text(query)

        query_content_words = [
            w for w in re.findall(r"[а-яёa-z0-9]+", query_norm)
            if w not in STOP_WORDS and len(w) >= 2
        ]
        n_content = len(query_content_words)

        # ── Retrieval: 4 BM25 индекса → RRF ────────────────────
        query_cgrams  = char_ngrams(query_norm, self.char_ngram_sizes)
        char_results  = self.char_index.query_bm25(query_cgrams, top_k=self.retrieval_top_k)

        query_words   = tokenize_and_lemmatize(query_norm)
        word_results  = self.word_index.query_bm25(query_words, top_k=self.retrieval_top_k)

        name_results  = self.name_index.query_bm25(query_cgrams, top_k=self.retrieval_top_k // 2)

        query_bigrams  = _word_bigrams(query_words)
        bigram_results = (
            self.bigram_index.query_bm25(query_bigrams, top_k=self.retrieval_top_k)
            if query_bigrams else []
        )

        # Нормализованные BM25 scores для scoring
        def _norm(res: list[tuple[int, float]]) -> dict[int, float]:
            if not res:
                return {}
            mx = max(s for _, s in res) or 1.0
            return {d: s / mx for d, s in res}

        char_scores   = _norm(char_results)
        word_scores   = _norm(word_results)
        name_scores   = _norm(name_results)
        bigram_scores = _norm(bigram_results)

        # RRF: объединяем кандидатов от всех 4 индексов
        rrf_scores = _rrf_combine(
            [char_results, word_results, name_results, bigram_results],
            k=self.rrf_k,
        )

        # ── Exact-match кандидаты ───────────────────────────────
        query_fmt   = _extract_format_tokens(query_norm)
        query_attrs = _extract_attributes(query_norm)

        exact_candidates: set[int] = set()
        for tok in query_fmt:
            exact_candidates |= self.format_index.get(tok, set())
        for attr in query_attrs:
            exact_candidates |= self.attr_index.get(attr, set())

        all_candidates = set(rrf_scores) | exact_candidates
        if not all_candidates:
            return []

        # Pre-rank по RRF, exact candidates всегда в пуле
        pre_ranked = sorted(
            all_candidates,
            key=lambda d: rrf_scores.get(d, 0.0),
            reverse=True,
        )[: self.rerank_top_k]
        candidates = list(dict.fromkeys(pre_ranked + list(exact_candidates)))

        # ── Scoring ─────────────────────────────────────────────
        all_scored = []

        for doc_id in candidates:
            cs  = char_scores.get(doc_id, 0.0)
            ws  = word_scores.get(doc_id, 0.0)
            bs  = bigram_scores.get(doc_id, 0.0)
            ns  = name_scores.get(doc_id, 0.0)

            doc_fmt  = self.doc_format_tokens.get(doc_id, set())
            doc_attr = self.doc_attrs.get(doc_id, set())
            fmt    = 1.0 if query_fmt & doc_fmt else 0.0
            attr_s = (
                len(query_attrs & doc_attr) / len(query_attrs)
                if query_attrs else 0.0
            )

            fuzzy_wr = fuzz.WRatio(
                query, self.documents[doc_id],
                processor=rfutils.default_process,
            ) / 100.0
            fuzzy_pr = fuzz.partial_ratio(
                query_norm, self.doc_normalized[doc_id],
            ) / 100.0
            fs = max(fuzzy_wr, fuzzy_pr)

            coverage = self._word_coverage(query_content_words, self.doc_tokens[doc_id])

            raw = (
                self.char_weight   * cs
                + self.word_weight   * ws
                + self.bigram_weight * bs
                + self.fuzzy_weight  * fs
                + self.name_weight   * ns
                + self.format_weight * fmt
                + self.attr_weight   * attr_s
            )
            all_scored.append((doc_id, raw, coverage, cs, ws, bs, ns, fs, fmt, attr_s))

        # Прогрессивный fallback (идентично v4)
        fallback_levels = [self.coverage_min, 0.40, 0.0] if n_content >= 2 else [0.0]
        results: list[SearchResult] = []
        seen: set[int] = set()

        for level, fb_min in enumerate(fallback_levels):
            if len(results) >= top_k:
                break
            for doc_id, raw, coverage, cs, ws, bs, ns, fs, fmt, attr_s in all_scored:
                if doc_id in seen:
                    continue
                if n_content >= 2 and coverage < fb_min:
                    continue
                final = raw * (coverage ** self.coverage_power)
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

    def stats(self) -> dict:
        return {
            "version":              "v5",
            "total_documents":      self.size,
            "char_index_terms":     len(self.char_index.index),
            "word_index_terms":     len(self.word_index.index),
            "bigram_index_terms":   len(self.bigram_index.index),
            "name_index_terms":     len(self.name_index.index),
            "format_index_tokens":  len(self.format_index),
            "attr_index_tokens":    len(self.attr_index),
        }


# ─────────────────────────────────────────────────────────────────
#  Кеш + построение индекса
# ─────────────────────────────────────────────────────────────────

def _v5_cache_valid() -> bool:
    return (
        ENGINE_CACHE_V5.exists()
        and CACHE_DB.exists()
        and ENGINE_CACHE_V5.stat().st_mtime >= CACHE_DB.stat().st_mtime
    )


def _load_v5_cache() -> "IncrementalProductSearchV5 | None":
    try:
        with open(ENGINE_CACHE_V5, "rb") as f:
            return pickle.load(f)  # noqa: S301
    except Exception as e:
        print(f"  Предупреждение: не удалось загрузить кэш v5 ({e}), пересборка.")
        return None


def _save_v5_cache(engine: "IncrementalProductSearchV5") -> None:
    try:
        with open(ENGINE_CACHE_V5, "wb") as f:
            pickle.dump(engine, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"  Индекс v5 сохранён → {ENGINE_CACHE_V5.name}")
    except Exception as e:
        print(f"  Предупреждение: не удалось сохранить кэш v5 ({e}).")


def build_engine_v5(
    data: list[dict],
    use_cache: bool = True,
) -> tuple["IncrementalProductSearchV5", float]:
    """Строит IncrementalProductSearchV5.

    Использует тот же SQLite-кеш лемматизации, что и v4 (text_norm, text_lemma),
    дополнительно вычисляет биграммы и атрибуты без повторной лемматизации.
    """
    if use_cache and _v5_cache_valid():
        t0 = time.time()
        engine = _load_v5_cache()
        if engine is not None:
            load_time = time.time() - t0
            print(f"  Индекс v5 загружен из кэша за {load_time:.2f}s  ({engine.size:,} docs)")
            return engine, load_time

    engine = IncrementalProductSearchV5()

    t0 = time.time()
    for doc_id, item in enumerate(data):
        original  = item["name"] + " " + item["category"]
        text_norm = item["text_norm"]
        name_norm = item["name_norm"]
        word_tokens = item["text_lemma"].split()  # из кеша

        engine.documents[doc_id]      = original
        engine.doc_metadata[doc_id]   = {
            "ste_id":   item["ste_id"],
            "name":     item["name"],
            "category": item["category"],
        }
        engine.doc_normalized[doc_id] = text_norm
        engine.doc_tokens[doc_id]     = re.findall(r"[а-яеa-z0-9]+", text_norm)

        engine._index_doc(doc_id, text_norm, name_norm, word_tokens)

        if (doc_id + 1) % 50_000 == 0:
            print(f"  Indexed v5: {doc_id + 1} ...")

    engine._next_id = len(data)
    build_time = time.time() - t0

    s = engine.stats()
    print(
        f"  Engine v5 built in {build_time:.2f}s  "
        f"(docs={s['total_documents']:,}, "
        f"char={s['char_index_terms']:,}, "
        f"word={s['word_index_terms']:,}, "
        f"bigram={s['bigram_index_terms']:,}, "
        f"attrs={s['attr_index_tokens']:,})"
    )

    if use_cache:
        _save_v5_cache(engine)

    return engine, build_time
