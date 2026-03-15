"""Генерация текстового обоснования НМЦК (статья 22 ФЗ-44, МР №567).

Публичный API:
  build_justification_text(...)  → str   — текстовый формат для поля justification в API
"""

from __future__ import annotations


# ── text ─────────────────────────────────────────────────────────

def build_justification_text(
    unit_price: float,
    nmck_data: dict,
    quantity: int | None = None,
    unit: str | None = None,
) -> str:
    """Возвращает обоснование НМЦ в текстовом виде (ч. 2 ст. 22 ФЗ-44, МР №567).

    quantity/unit опциональны — если не переданы, расчёт отображается за единицу.
    """

    import statistics as _stats

    # ── Разбор контрактов по статусам ────────────────────────────
    all_annotated = nmck_data.get("contracts", [])
    force_inc  = [c["contract"] for c in all_annotated if c.get("status") == "force_included"]
    force_exc  = [c["contract"] for c in all_annotated if c.get("status") == "force_excluded"]
    calc_contracts = [
        c["contract"] for c in all_annotated
        if c.get("status") in ("used", "force_included")
    ]

    n_total     = nmck_data.get("n_total", len(all_annotated))
    n_outliers  = nmck_data.get("n_outliers", 0)
    n_contracts = nmck_data.get("n_contracts", len(calc_contracts))
    total       = unit_price * quantity if quantity else None

    # ── Цены учтённых контрактов для статистики ──────────────────
    prices: list[float] = []
    for c in calc_contracts:
        try:
            prices.append(float(c.get("Цена за единицу", 0)))
        except (ValueError, TypeError):
            pass
    prices.sort()

    def _percentile(data: list[float], p: float) -> float:
        """Линейная интерполяция (метод numpy linear)."""
        if not data:
            return 0.0
        n = len(data)
        idx = p / 100 * (n - 1)
        lo, hi = int(idx), min(int(idx) + 1, n - 1)
        return data[lo] + (data[hi] - data[lo]) * (idx - lo)

    p_min    = prices[0]  if prices else 0.0
    p_max    = prices[-1] if prices else 0.0
    p_q1     = _percentile(prices, 25)
    p_median = _percentile(prices, 50)
    p_q3     = _percentile(prices, 75)
    p_mean   = (_stats.mean(prices) if prices else 0.0)

    def _ru(value: float) -> str:
        """Российский числовой формат: 1 234,56 ₽"""
        s = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", "\u00a0")
        return f"{s} ₽"

    lw = 38  # ширина метки
    lines: list[str] = []

    # ── Заголовок ────────────────────────────────────────────────
    lines += [
        "Обоснование начальной (максимальной) цены контракта (НМЦ)",
        "",
        "Метод определения НМЦ: метод сопоставимых рыночных цен (анализ рынка)",
        "в соответствии с ч. 2 ст. 22 Федерального закона от 05.04.2013 № 44-ФЗ.",
        "",
    ]

    # ── 1. ИСХОДНЫЕ ДАННЫЕ ───────────────────────────────────────
    lines += ["1. ИСХОДНЫЕ ДАННЫЕ"]
    lines.append(f"   {'Всего найдено закупок:':<{lw}} {n_total} шт.")
    lines.append(f"   {'Выбросы (метод IQR):':<{lw}} {n_outliers} шт. — исключены из расчёта")
    lines.append(f"   {'Включено пользователем:':<{lw}} {len(force_inc)} шт.")
    lines.append(f"   {'Исключено пользователем:':<{lw}} {len(force_exc)} шт.")
    lines.append(f"   {'Итого учтено в расчёте:':<{lw}} {n_contracts} шт.")
    lines.append("")

    # ── 2. СТАТИСТИКА ЦЕН ────────────────────────────────────────
    lines += ["2. СТАТИСТИКА ЦЕН (за единицу)"]
    if prices:
        lines.append(f"   {'Минимальная цена:':<{lw}} {_ru(p_min)}")
        lines.append(f"   {'Нижняя граница диапазона (25%):':<{lw}} {_ru(p_q1)}")
        lines.append(f"   {'Медиана:':<{lw}} {_ru(p_median)}")
        lines.append(f"   {'Верхняя граница диапазона (75%):':<{lw}} {_ru(p_q3)}")
        lines.append(f"   {'Максимальная цена:':<{lw}} {_ru(p_max)}")
        lines.append(f"   {'Средняя (простая):':<{lw}} {_ru(p_mean)}")
    else:
        lines.append("   Нет данных о ценах.")
    lines.append("")

    # ── 3. РАСЧЁТ НМЦ ────────────────────────────────────────────
    lines += ["3. РАСЧЁТ НМЦ"]
    lines.append(f"   {'Средневзвешенная цена ед.:':<{lw}} {_ru(unit_price)}")
    if quantity:
        unit_label = f" {unit}" if unit else ""
        lines.append(f"   {'Требуемое количество:':<{lw}} {quantity}{unit_label}")
        lines += [
            "",
            f"   НМЦ = {_ru(unit_price)} × {quantity} = {_ru(total)}",
        ]
    else:
        lines += [
            "",
            f"   НМЦ (за единицу) = {_ru(unit_price)}",
        ]
    lines.append("")

    # ── 4. ВЫВОД ─────────────────────────────────────────────────
    nmck_value = _ru(total) if total is not None else f"{_ru(unit_price)} за единицу"
    lines += [
        "4. ВЫВОД",
        f"   Начальная (максимальная) цена контракта составляет {nmck_value}.",
        f"   Расчёт выполнен на основании {n_contracts} сопоставимых рыночных цен, "
        "полученных из реестра контрактов ЕИС в сфере закупок.",
    ]
    if n_contracts < 3:
        lines.append(
            "   ВНИМАНИЕ: использовано менее 3 источников "
            "(п. 3.19 МР №567 требует не менее 3)."
        )
    lines.append(
        "   Поиск выполняется с помощью категориальной группировки. "
        "Поиск хорошо покрывает идентичные товары, "
        "а однородные — в зависимости от точности сформулированного запроса. "
        "В любом случае, учитываемые товары/работы/услуги являются функционально взаимозаменяемыми. "
        "Так как поиск выполняется среди товаров/работ/услуг со сходными характеристиками, "
        "они являются функционально и/или коммерчески взаимозаменяемыми."
    )

    return "\n".join(lines)
