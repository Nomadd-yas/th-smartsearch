from __future__ import annotations

import re
from datetime import date

from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

REGIONS: frozenset[str] = frozenset({
    "Республика Адыгея", "Республика Алтай", "Республика Башкортостан",
    "Республика Бурятия", "Республика Дагестан", "Республика Ингушетия",
    "Кабардино-Балкарская Республика", "Республика Калмыкия",
    "Карачаево-Черкесская Республика", "Республика Карелия", "Республика Коми",
    "Республика Крым", "Республика Марий Эл", "Республика Мордовия",
    "Республика Саха (Якутия)", "Республика Северная Осетия — Алания",
    "Республика Татарстан", "Республика Тыва", "Удмуртская Республика",
    "Республика Хакасия", "Чеченская Республика", "Чувашская Республика",
    "Алтайский край", "Забайкальский край", "Камчатский край",
    "Краснодарский край", "Красноярский край", "Пермский край",
    "Приморский край", "Ставропольский край", "Хабаровский край",
    "Амурская область", "Архангельская область", "Астраханская область",
    "Белгородская область", "Брянская область", "Владимирская область",
    "Волгоградская область", "Вологодская область", "Воронежская область",
    "Ивановская область", "Иркутская область", "Калининградская область",
    "Калужская область", "Кемеровская область", "Кировская область",
    "Костромская область", "Курганская область", "Курская область",
    "Ленинградская область", "Липецкая область", "Магаданская область",
    "Московская область", "Мурманская область", "Нижегородская область",
    "Новгородская область", "Новосибирская область", "Омская область",
    "Оренбургская область", "Орловская область", "Пензенская область",
    "Псковская область", "Ростовская область", "Рязанская область",
    "Самарская область", "Саратовская область", "Сахалинская область",
    "Свердловская область", "Смоленская область", "Тамбовская область",
    "Тверская область", "Томская область", "Тульская область",
    "Тюменская область", "Ульяновская область", "Челябинская область",
    "Ярославская область", "Москва", "Санкт-Петербург", "Севастополь",
    "Еврейская автономная область", "Ненецкий автономный округ",
    "Ханты-Мансийский автономный округ — Югра", "Чукотский автономный округ",
    "Ямало-Ненецкий автономный округ",
})


class SearchRequest(BaseModel):
    query: str = Field(..., description="Текстовый запрос")
    top_k: int | None = Field(None, ge=1, description="Пул СТЕ (None = все найденные)")
    min_score: float | None = Field(None, ge=0.0, le=1.0, description="Порог релевантности. null = авто (40% от лучшего совпадения)")
    region: str | None = Field(None, description="Фильтр по региону поставщика")
    unit: str | None = Field(None, description="Фильтр по единице измерения (напр. 'шт', 'л')")
    date_from: date | None = Field(None, description="Начало периода контрактов (YYYY-MM-DD, включительно)")
    date_to: date | None = Field(None, description="Конец периода контрактов (YYYY-MM-DD, включительно)")
    vat: str | None = Field(None, description="Ставка НДС: произвольный процент (напр. '0%', '10%', '20%') или 'Без НДС'")

    @field_validator("region")
    @classmethod
    def validate_region(cls, v: str | None) -> str | None:
        if v is not None and v not in REGIONS:
            raise ValueError(f"Неизвестный регион: {v!r}. Используйте один из допустимых регионов РФ.")
        return v

    @field_validator("vat")
    @classmethod
    def validate_vat(cls, v: str | None) -> str | None:
        if v is not None:
            normalized = v.strip().lower()
            if normalized != "без ндс" and not re.fullmatch(r"\d+(\.\d+)?%", normalized):
                raise ValueError(
                    f"Недопустимая ставка НДС: {v!r}. "
                    "Ожидается процент (напр. '0%', '10%', '20%') или 'Без НДС'"
                )
        return v

    @model_validator(mode="after")
    def validate_date_range(self) -> "SearchRequest":
        if self.date_from and self.date_to and self.date_from > self.date_to:
            raise ValueError("date_from не может быть позже date_to")
        return self


class NmckRequest(BaseModel):
    contracts: list[dict] = Field(..., description="Контракты из ответа /search")
    date_from: date | None = Field(None, description="Начало периода (YYYY-MM-DD, включительно)")
    date_to: date | None = Field(None, description="Конец периода (YYYY-MM-DD, включительно)")
    force_include: list[str] = Field(
        default_factory=list,
        description="Идентификаторы контрактов из contracts_outliers, которые нужно включить в расчёт",
    )
    force_exclude: list[str] = Field(
        default_factory=list,
        description="Идентификаторы контрактов из contracts_used, которые нужно исключить из расчёта",
    )
    # Поля для генерации обоснования (опционально)
    quantity: int | None = Field(None, ge=1, description="Количество для обоснования")
    unit: str | None = Field(None, description="Единица измерения для обоснования")

    @model_validator(mode="after")
    def validate_date_range(self) -> "NmckRequest":
        if self.date_from and self.date_to and self.date_from > self.date_to:
            raise ValueError("date_from не может быть позже date_to")
        return self


class SteItem(BaseModel):
    ste_id: str
    name: str
    category: str
    score: float
    last_price: float | None = None  # цена последнего совершённого контракта


class SteDetail(BaseModel):
    ste_id: str
    name: str
    category: str
    manufacturer: str | None
    characteristics: dict[str, str] = Field(
        default_factory=dict,
        description="Характеристики СТЕ: ключ → значение (набор ключей зависит от категории товара)",
    )


class NmckContractItem(BaseModel):
    status: Literal["used", "outlier", "force_included", "force_excluded"]
    contract: dict


class InterchangeableItem(BaseModel):
    ste_id: str
    name: str
    category: str
    score: float = Field(..., description="Доля совпавших характеристик (0–1)")
    matched_keys: dict[str, str] = Field(
        default_factory=dict,
        description="Совпавшие характеристики: ключ → значение",
    )


class NmckData(BaseModel):
    nmck: float
    price_min: float
    price_max: float
    n_contracts: int   # контрактов в оптимальном окне
    n_total: int       # всего за период (до удаления выбросов)
    n_outliers: int    # удалено как выбросы (IQR ± ручные корректировки)
    window_days: int
    cv: float
    contracts: list[NmckContractItem] = Field(
        default_factory=list,
        description=(
            "Все контракты с пометкой статуса: "
            "used — учтён, outlier — выброс, "
            "force_included — был выброс, добавлен вручную, "
            "force_excluded — был учтён, убран вручную"
        ),
    )
    justification: str | None = Field(
        None,
        description="Текст обоснования НМЦК. Заполняется если в запросе переданы name, quantity, unit.",
    )
