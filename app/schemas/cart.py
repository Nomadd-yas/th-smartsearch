from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, Field


class CartAddRequest(BaseModel):
    name: str = Field(..., description="Название позиции (по умолчанию — название СТЕ)")
    quantity: int = Field(..., ge=1, description="Количество")
    unit: str = Field(..., description="Единица измерения")
    unit_price: float = Field(..., gt=0, description="НМЦ позиции (= nmck.nmck)")
    nmck_data: dict = Field(..., description="Снапшот NmckData из /workspace/{id}/nmck или /nmck")


class CartUpdateRequest(BaseModel):
    name: str | None = Field(None, description="Новое название (если нужно изменить)")
    quantity: int | None = Field(None, ge=1, description="Новое количество")
    unit: str | None = Field(None, description="Новая единица измерения")
    unit_price: float | None = Field(None, gt=0, description="Новая цена за единицу")


class CartUpdateNmckRequest(BaseModel):
    unit_price: float = Field(..., gt=0, description="Обновлённая НМЦ позиции (= nmck.nmck)")
    nmck_data: dict = Field(..., description="Новый снапшот NmckData после перерасчёта")


class CartItem(BaseModel):
    id: str
    name: str
    quantity: int
    unit: str
    unit_price: float
    date_added: date
    nmck_data: dict
    created_at: str
    updated_at: str
