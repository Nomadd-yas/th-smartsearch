"""Корзина: добавление позиций НМЦК, редактирование, генерация обоснования."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import current_user
from app.schemas.cart import (
    CartAddRequest,
    CartItem,
    CartUpdateNmckRequest,
    CartUpdateRequest,
)
import utils.cart as cart_db
from utils.justification import build_justification_text

router = APIRouter(prefix="/cart", tags=["cart"])
log = logging.getLogger("smartsearch")


def _require_item(user_id: int, item_id: str) -> dict:
    item = cart_db.get_item(user_id, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Позиция корзины не найдена")
    return item


def _to_schema(raw: dict) -> CartItem:
    return CartItem(
        id=raw["id"],
        name=raw["name"],
        quantity=raw["quantity"],
        unit=raw["unit"],
        unit_price=raw["unit_price"],
        date_added=raw["date_added"],
        nmck_data=raw["nmck_data"],
        created_at=raw["created_at"],
        updated_at=raw["updated_at"],
    )


@router.post("", summary="Добавить позицию в корзину", status_code=201)
def cart_add(
    req: CartAddRequest,
    user: dict = Depends(current_user),
) -> CartItem:
    user_id = int(user["sub"])
    log.info("POST /cart  user=%r  name=%r  qty=%d  price=%.2f",
             user.get("username"), req.name, req.quantity, req.unit_price)
    item = cart_db.add_item(
        user_id=user_id,
        name=req.name,
        quantity=req.quantity,
        unit=req.unit,
        unit_price=req.unit_price,
        nmck_data=req.nmck_data,
    )
    return _to_schema(item)


@router.get("", summary="Список позиций корзины")
def cart_list(
    user: dict = Depends(current_user),
) -> list[CartItem]:
    user_id = int(user["sub"])
    return [_to_schema(i) for i in cart_db.list_items(user_id)]


@router.get("/{item_id}", summary="Позиция корзины")
def cart_get(
    item_id: str,
    user: dict = Depends(current_user),
) -> CartItem:
    user_id = int(user["sub"])
    return _to_schema(_require_item(user_id, item_id))


@router.patch("/{item_id}", summary="Изменить название / количество / цену")
def cart_update(
    item_id: str,
    req: CartUpdateRequest,
    user: dict = Depends(current_user),
) -> CartItem:
    user_id = int(user["sub"])
    _require_item(user_id, item_id)
    updated = cart_db.update_fields(
        user_id, item_id,
        name=req.name,
        quantity=req.quantity,
        unit=req.unit,
        unit_price=req.unit_price,
    )
    log.info("PATCH /cart/%s  user=%r  fields=%s",
             item_id, user.get("username"), req.model_dump(exclude_none=True))
    return _to_schema(updated)  # type: ignore[arg-type]


@router.post("/{item_id}/nmck", summary="Обновить НМЦК и обоснование для позиции")
def cart_update_nmck(
    item_id: str,
    req: CartUpdateNmckRequest,
    user: dict = Depends(current_user),
) -> CartItem:
    """Обновляет снапшот НМЦК и цену за единицу.

    Название позиции **не меняется** — даже если оно было изменено вручную.
    """
    user_id = int(user["sub"])
    _require_item(user_id, item_id)
    updated = cart_db.update_nmck(
        user_id, item_id,
        unit_price=req.unit_price,
        nmck_data=req.nmck_data,
    )
    log.info("POST /cart/%s/nmck  user=%r  new_price=%.2f",
             item_id, user.get("username"), req.unit_price)
    return _to_schema(updated)  # type: ignore[arg-type]


@router.delete("/{item_id}", summary="Удалить позицию из корзины")
def cart_delete(
    item_id: str,
    user: dict = Depends(current_user),
) -> dict:
    user_id = int(user["sub"])
    if not cart_db.delete_item(user_id, item_id):
        raise HTTPException(status_code=404, detail="Позиция корзины не найдена")
    log.info("DELETE /cart/%s  user=%r", item_id, user.get("username"))
    return {"deleted": item_id}


@router.get("/{item_id}/justification", summary="Обоснование НМЦК (текст)")
def cart_justification(
    item_id: str,
    user: dict = Depends(current_user),
) -> dict:
    """Возвращает текст обоснования НМЦК для позиции корзины."""
    user_id = int(user["sub"])
    item = _require_item(user_id, item_id)

    text = build_justification_text(
        unit_price=item["unit_price"],
        nmck_data=item["nmck_data"],
        quantity=item["quantity"],
        unit=item["unit"],
    )

    log.info("GET /cart/%s/justification  user=%r", item_id, user.get("username"))
    return {"justification": text}
