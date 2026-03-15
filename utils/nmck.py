from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import pandas as pd
import numpy as np


@dataclass
class AnnotatedContract:
    status: str   # "used" | "outlier" | "force_included" | "force_excluded"
    contract: dict


@dataclass
class NmckResult:
    nmck: float        # НМЦК — медиана оптимального окна
    price_min: float   # минимальная цена после удаления выбросов
    price_max: float   # максимальная цена после удаления выбросов
    n_contracts: int   # контрактов в оптимальном окне
    n_total: int       # контрактов за период (до удаления выбросов)
    n_outliers: int    # удалено выбросов (IQR ± ручные корректировки)
    window_days: int   # длина оптимального окна (дней)
    cv: float          # коэффициент вариации оптимального окна (0..1)
    contracts: list[AnnotatedContract] = field(default_factory=list)
    is_manual: bool = False  # True — НМЦК задана вручную (недостаточно данных)


# ── Порог несовместимости ────────────────────────────────────────

_COMPAT_FACTOR = 3.0  # ручная цена не должна выходить за [min/3, max*3]


class IncompatiblePriceError(ValueError):
    """Ручная цена несопоставима с найденными рыночными ценами."""


def remove_outliers(series: pd.Series) -> pd.Series:
    """Исключаем выбросы по IQR (стандарт для НМЦК)"""
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.85)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return series[(series >= lower) & (series <= upper)]


def _unit_matches(contract_unit: str | None, requested: str) -> bool:
    """Совпадение единицы измерения с учётом формата 'л;см^3'."""
    if not contract_unit:
        return False
    req = requested.strip().lower()
    return req in [v.strip().lower() for v in contract_unit.split(";")]


def calculate_nmck(
    contracts: list[dict],
    region: str | None = None,
    unit: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    vat: str | None = None,
    force_include: list[str] | None = None,
    force_exclude: list[str] | None = None,
) -> NmckResult | None:
    """Рассчитывает НМЦК по методу сопоставимых рыночных цен.

    Параметры:
      contracts     — список контрактов (dict с ключами из COLUMNS)
      region        — фильтр по «Регион поставщика»
      unit          — фильтр по «Единица измерения», поддерживает 'л;см^3'
      date_from     — начало периода (включительно); при отсутствии — 6 месяцев от последней даты
      date_to       — конец периода (включительно)
      vat           — фильтр по «Ставка НДС» (напр. '20%', 'Без НДС')
      force_include — «Идентификатор контракта» выбросов, которые нужно включить в расчёт
      force_exclude — «Идентификатор контракта» учтённых контрактов, которые нужно исключить

    Алгоритм:
      1. Фильтрация по региону, единице, НДС.
      2. Ограничение по дате: явный диапазон или последние 6 месяцев.
      3. Удаляем выбросы по IQR (±1.5×IQR), затем применяем force_include/force_exclude.
      4. Перебираем окна 30..180 дней с шагом 5 — выбираем с CV ≤ 0.33
         максимальной длины (при отсутствии — минимальный CV).
      5. НМЦК = медиана цен в оптимальном окне.
    """
    if not contracts:
        return None

    # ── Фильтрация по региону ──────────────────────────────────────
    if region:
        region_lower = region.strip().lower()
        contracts = [
            c for c in contracts
            if c.get("Регион поставщика") and
               c["Регион поставщика"].strip().lower() == region_lower
        ]

    # ── Фильтрация по единице измерения ───────────────────────────
    if unit:
        contracts = [c for c in contracts if _unit_matches(c.get("Единица измерения"), unit)]

    # ── Фильтрация по НДС ─────────────────────────────────────────
    if vat:
        vat_lower = vat.strip().lower()
        contracts = [
            c for c in contracts
            if c.get("Ставка НДС") and c["Ставка НДС"].strip().lower() == vat_lower
        ]

    if not contracts:
        return None

    # Строим DataFrame, сохраняя индекс в отфильтрованном списке
    cid_col = "Идентификатор контракта"
    ste_col = "Идентификатор СТЕ"
    df = pd.DataFrame([
        {
            "_idx":                        i,
            "Цена за единицу":             c.get("Цена за единицу"),
            "Количество":                  c.get("Количество"),
            "Дата заключения контракта":   c.get("Дата заключения контракта"),
            cid_col:                       c.get(cid_col),
            ste_col:                       c.get(ste_col, ""),
        }
        for i, c in enumerate(contracts)
    ])

    df["Цена за единицу"] = pd.to_numeric(df["Цена за единицу"], errors="coerce")
    df["Количество"] = pd.to_numeric(df["Количество"], errors="coerce").fillna(1.0)

    # ── Агрегация по (контракт, СТЕ) ─────────────────────────────
    # Ключ: (Идентификатор контракта, Идентификатор СТЕ).
    # Это корректно устраняет дублирование строк одного СТЕ внутри
    # одного контракта (разные партии/лоты), при этом НЕ сливает
    # разные СТЕ из одного контракта в одну ценовую точку.
    has_contract_id = df[cid_col].notna() & (df[cid_col] != "")
    if has_contract_id.any():
        df_with_id = df[has_contract_id].copy()

        def _agg(g: pd.DataFrame) -> pd.Series:
            qty   = g["Количество"]
            price = g["Цена за единицу"]
            valid = price.notna() & qty.notna()
            if valid.any():
                wp = (price[valid] * qty[valid]).sum() / qty[valid].sum()
            else:
                wp = price.dropna().iloc[0] if price.notna().any() else float("nan")
            return pd.Series({
                "_idx":                      g["_idx"].iloc[0],
                "Цена за единицу":           wp,
                "Дата заключения контракта": g["Дата заключения контракта"].iloc[0],
            })

        df_agg = (
            df_with_id.groupby([cid_col], sort=False)
            .apply(_agg, include_groups=False)
            .reset_index()
            [["_idx", "Цена за единицу", "Дата заключения контракта", cid_col]]
        )
        df_no_id = df[~has_contract_id][
            ["_idx", "Цена за единицу", "Дата заключения контракта", cid_col]
        ].copy()
        df = pd.concat([df_agg, df_no_id], ignore_index=True)
    else:
        df = df[["_idx", "Цена за единицу", "Дата заключения контракта", cid_col]].copy()

    df["Цена за единицу"] = pd.to_numeric(df["Цена за единицу"], errors="coerce")
    df["Дата"] = pd.to_datetime(
        df["Дата заключения контракта"], dayfirst=True, errors="coerce"
    )
    df = df.dropna(subset=["Цена за единицу", "Дата"]).copy()

    if df.empty:
        return None

    df = df.sort_values("Дата").reset_index(drop=True)

    # ── Фильтрация по дате ────────────────────────────────────────
    if date_from or date_to:
        if date_from:
            df = df[df["Дата"] >= pd.Timestamp(date_from)]
        if date_to:
            df = df[df["Дата"] <= pd.Timestamp(date_to) + pd.Timedelta(days=1)]
        if df.empty:
            return None
        last_date = df["Дата"].max()
    else:
        last_date = df["Дата"].max()
        df = df[df["Дата"] >= (last_date - pd.DateOffset(months=6))].copy()

    if df.empty:
        return None

    n_total = len(df)

    # ── Удаление выбросов по IQR ──────────────────────────────────
    price = df["Цена за единицу"]
    q1 = price.quantile(0.25)
    q3 = price.quantile(0.85)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    mask_clean = (price >= lower) & (price <= upper)

    df_clean = df[mask_clean].copy()
    df_out = df[~mask_clean].copy()

    # ── Ручные корректировки (с отслеживанием изменений) ──────────
    fi_moved_ids: set[str] = set()
    fe_moved_ids: set[str] = set()

    if force_include:
        fi_set = set(force_include)
        mask_fi = df_out["Идентификатор контракта"].isin(fi_set)
        fi_moved_ids = set(df_out.loc[mask_fi, "Идентификатор контракта"].tolist())
        df_clean = pd.concat([df_clean, df_out[mask_fi]], ignore_index=True)
        df_out = df_out[~mask_fi].copy()

    if force_exclude:
        fe_set = set(force_exclude)
        mask_fe = df_clean["Идентификатор контракта"].isin(fe_set)
        fe_moved_ids = set(df_clean.loc[mask_fe, "Идентификатор контракта"].tolist())
        df_out = pd.concat([df_out, df_clean[mask_fe]], ignore_index=True)
        df_clean = df_clean[~mask_fe].copy()

    if df_clean.empty:
        return None

    n_outliers = n_total - len(df_clean)

    df_clean = df_clean.sort_values("Дата").reset_index(drop=True)
    price_clean = df_clean["Цена за единицу"]

    # ── Поиск оптимального окна ───────────────────────────────────
    candidates = []
    for days in range(30, 181, 5):
        start_date = last_date - pd.Timedelta(days=days)
        mask_w = df_clean["Дата"] >= start_date
        n = int(mask_w.sum())
        if n < 5:
            continue
        wp = price_clean[mask_w]
        med = wp.median()
        std = wp.std()
        cv = std / med if med > 0 else float("inf")
        candidates.append((days, cv, n, med, start_date))

    if not candidates:
        return None

    valid = [c for c in candidates if c[1] <= 0.33]
    best = max(valid, key=lambda x: x[0]) if valid else min(candidates, key=lambda x: x[1])

    best_days, best_cv, _, _, best_start = best
    mask_w = df_clean["Дата"] >= best_start
    df_window = df_clean[mask_w]
    window_prices = df_window["Цена за единицу"]

    # ── Сборка аннотированного списка контрактов ──────────────────
    annotated: list[AnnotatedContract] = []

    for i in df_window["_idx"].tolist():
        c = contracts[i]
        cid = c.get("Идентификатор контракта")
        status = "force_included" if cid in fi_moved_ids else "used"
        annotated.append(AnnotatedContract(status=status, contract=c))

    for i in df_out["_idx"].tolist():
        c = contracts[i]
        cid = c.get("Идентификатор контракта")
        status = "force_excluded" if cid in fe_moved_ids else "outlier"
        annotated.append(AnnotatedContract(status=status, contract=c))

    return NmckResult(
        nmck=float(window_prices.median()),
        price_min=float(price_clean.min()),
        price_max=float(price_clean.max()),
        n_contracts=len(window_prices),
        n_total=n_total,
        n_outliers=n_outliers,
        window_days=best_days,
        cv=min(best_cv, 1.0),
        contracts=annotated,
    )
