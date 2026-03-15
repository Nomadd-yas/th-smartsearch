"""Формирование и скачивание документа обоснования НМЦК."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from app.dependencies import current_user
from app.schemas.report import HistoryItem, SingleReportRequest
import utils.cart as cart_db
import utils.history as history_db
from utils.docx_report import build_nmck_docx

router = APIRouter(prefix="/report", tags=["report"])
log = logging.getLogger("smartsearch")

_DOCX_MEDIA = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)


def _gen_registry_number() -> str:
    """
    Генерирует внутренний реестровый номер закупки.
    Формат: НМЦК-YYYYMMDD-XXXXXX (6 символов из UUID)
    """
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    short = uuid.uuid4().hex[:6].upper()
    return f"НМЦК-{today}-{short}"


def _docx_response(data: bytes, filename: str) -> StreamingResponse:
    return StreamingResponse(
        iter([data]),
        media_type=_DOCX_MEDIA,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── Из корзины ────────────────────────────────────────────────────

@router.post(
    "/cart",
    summary="Сформировать документ НМЦК из корзины → скачать + сохранить в историю + очистить корзину",
)
def report_from_cart(
    user: dict = Depends(current_user),
) -> StreamingResponse:
    user_id = int(user["sub"])
    items   = cart_db.list_items(user_id)
    log.info("POST /report/cart  user=%r  items=%d", user.get("username"), len(items))

    if not items:
        raise HTTPException(status_code=400, detail="Корзина пуста")

    reg_number = _gen_registry_number()
    docx_bytes = build_nmck_docx(items, registry_number=reg_number)
    history_db.add_entry(user_id, source="cart", items=items)

    for item in items:
        cart_db.delete_item(user_id, item["id"])

    log.info("  документ сформирован  reg=%s  корзина очищена", reg_number)
    return _docx_response(docx_bytes, "nmck_justification.docx")


# ── По одной позиции ──────────────────────────────────────────────

@router.post(
    "/single",
    summary="Сформировать документ НМЦК по одной позиции → скачать + сохранить в историю",
)
def report_single(
    req: SingleReportRequest,
    user: dict = Depends(current_user),
) -> StreamingResponse:
    user_id = int(user["sub"])
    log.info(
        "POST /report/single  user=%r  name=%r  qty=%d  price=%.2f",
        user.get("username"), req.name, req.quantity, req.unit_price,
    )

    item = {
        "name":       req.name,
        "quantity":   req.quantity,
        "unit":       req.unit,
        "unit_price": req.unit_price,
        "nmck_data":  req.nmck_data,
    }
    reg_number = _gen_registry_number()
    docx_bytes = build_nmck_docx([item], registry_number=reg_number)
    history_db.add_entry(user_id, source="single", items=[item])

    log.info("  документ сформирован  reg=%s", reg_number)
    return _docx_response(docx_bytes, "nmck_justification.docx")


# ── История ───────────────────────────────────────────────────────

@router.get("/history", summary="История сформированных документов")
def report_history(
    user: dict = Depends(current_user),
) -> list[HistoryItem]:
    user_id = int(user["sub"])
    entries = history_db.list_entries(user_id)
    log.info("GET /report/history  user=%r  count=%d", user.get("username"), len(entries))
    return [
        HistoryItem(
            id=e["id"],
            source=e["source"],
            item_count=e["item_count"],
            total_nmck=e["total_nmck"],
            created_at=e["created_at"],
        )
        for e in entries
    ]


@router.get("/history/{entry_id}", summary="Переформировать документ из истории")
def report_history_download(
    entry_id: str,
    user: dict = Depends(current_user),
) -> StreamingResponse:
    user_id = int(user["sub"])
    entry   = history_db.get_entry(user_id, entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="Запись истории не найдена")

    reg_number = _gen_registry_number()
    log.info("GET /report/history/%s  user=%r  reg=%s", entry_id, user.get("username"), reg_number)
    docx_bytes = build_nmck_docx(entry["items"], registry_number=reg_number)
    return _docx_response(docx_bytes, f"nmck_{entry_id[:8]}.docx")


@router.delete("/history/{entry_id}", summary="Удалить запись из истории")
def report_history_delete(
    entry_id: str,
    user: dict = Depends(current_user),
) -> dict[str, Any]:
    user_id = int(user["sub"])
    if not history_db.delete_entry(user_id, entry_id):
        raise HTTPException(status_code=404, detail="Запись истории не найдена")
    log.info("DELETE /report/history/%s  user=%r", entry_id, user.get("username"))
    return {"deleted": entry_id}
