from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class SingleReportRequest(BaseModel):
    """Запрос на формирование документа по одной позиции (минуя корзину)."""
    name: str = Field(..., description="Наименование товара/работы/услуги")
    quantity: int = Field(..., ge=1, description="Количество")
    unit: str = Field(..., description="Единица измерения")
    unit_price: float = Field(..., gt=0, description="Цена за единицу (НМЦ позиции)")
    nmck_data: dict = Field(..., description="Снапшот NmckData из /nmck или /workspace/{id}/nmck")


class HistoryItem(BaseModel):
    id: str
    source: Literal["cart", "single"]
    item_count: int
    total_nmck: float
    created_at: datetime

    class Config:
        from_attributes = True
