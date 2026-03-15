import logging
import time
from typing import Any

from fastapi import APIRouter, Depends

from app.dependencies import current_user, get_client
from app.schemas.search import (
    InterchangeableItem,
    NmckContractItem,
    NmckData,
    NmckRequest,
    SearchRequest,
    SteDetail,
    SteItem,
)
from utils.client import SteSearchClient

router = APIRouter(tags=["search"])
log = logging.getLogger("smartsearch")


@router.get("/health", summary="Статус сервиса")
def health(client: SteSearchClient = Depends(get_client)) -> dict[str, Any]:
    size = client._engine.size if client._engine else 0
    log.info("GET /health  →  docs=%d", size)
    return {"status": "ok", "engine_docs": size}


@router.post("/search", summary="Поиск СТЕ с отфильтрованными контрактами")
def search(
    req: SearchRequest,
    user: dict = Depends(current_user),
    client: SteSearchClient = Depends(get_client),
) -> dict[str, Any]:
    top_k = req.top_k if req.top_k is not None else client._engine.size
    log.info(
        "POST /search  user=%r  query=%r  top_k=%s  region=%r  unit=%r  "
        "date=%s→%s  vat=%r",
        user.get("username"), req.query, req.top_k or "all",
        req.region, req.unit, req.date_from, req.date_to, req.vat,
    )

    t0 = time.perf_counter()
    results = client.search(req.query, top_k=top_k, min_score=req.min_score)
    search_ms = (time.perf_counter() - t0) * 1000
    log.info("  search: %d СТЕ найдено  (%.1f мс)", len(results), search_ms)

    if any([req.region, req.unit, req.date_from, req.date_to, req.vat]):
        before = len(results)
        results = client.filter_ste_by_contracts(
            results,
            region=req.region, unit=req.unit,
            date_from=req.date_from, date_to=req.date_to, vat=req.vat,
        )
        log.info("  фильтр СТЕ: %d → %d", before, len(results))

    ste_list = [
        SteItem(
            ste_id=r.ste_id,
            name=r.name,
            category=r.category,
            score=r.score,
            last_price=client.get_last_price(
                r.ste_id,
                region=req.region, unit=req.unit,
                date_from=req.date_from, date_to=req.date_to, vat=req.vat,
            ),
        )
        for r in results
    ]

    contracts = client.get_contracts_for_nmck(
        results,
        region=req.region, unit=req.unit,
        date_from=req.date_from, date_to=req.date_to, vat=req.vat,
    )
    log.info("  контракты: %d записей", len(contracts))

    return {
        "query": req.query,
        "ste_count": len(ste_list),
        "ste": ste_list,
        "contracts": contracts,
    }


@router.post("/nmck", summary="Расчёт НМЦК по контрактам из /search")
def compute_nmck(
    req: NmckRequest,
    user: dict = Depends(current_user),
) -> NmckData | None:
    from utils.nmck import calculate_nmck
    log.info(
        "POST /nmck  user=%r  contracts=%d  date=%s→%s",
        user.get("username"), len(req.contracts), req.date_from, req.date_to,
    )

    t0 = time.perf_counter()
    result = calculate_nmck(
        req.contracts,
        date_from=req.date_from,
        date_to=req.date_to,
        force_include=req.force_include or None,
        force_exclude=req.force_exclude or None,
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000

    if result:
        log.info(
            "  nmck: %.2f  [min=%.2f max=%.2f  окно=%dд  CV=%.3f"
            "  n=%d  total=%d  outliers=%d  fi=%d  fe=%d]  (%.1f мс)",
            result.nmck, result.price_min, result.price_max,
            result.window_days, result.cv,
            result.n_contracts, result.n_total, result.n_outliers,
            len(req.force_include), len(req.force_exclude),
            elapsed_ms,
        )
        from utils.justification import build_justification_text
        nmck_dict = {
            "cv": result.cv,
            "contracts": [
                {"status": ac.status, "contract": ac.contract}
                for ac in result.contracts
            ],
            "n_total": result.n_total,
            "n_outliers": result.n_outliers,
            "n_contracts": result.n_contracts,
        }
        justification = build_justification_text(
            unit_price=result.nmck,
            nmck_data=nmck_dict,
            quantity=req.quantity,
            unit=req.unit,
        )
        return NmckData(
            nmck=result.nmck,
            price_min=result.price_min,
            price_max=result.price_max,
            n_contracts=result.n_contracts,
            n_total=result.n_total,
            n_outliers=result.n_outliers,
            window_days=result.window_days,
            cv=result.cv,
            contracts=[
                NmckContractItem(status=ac.status, contract=ac.contract)
                for ac in result.contracts
            ],
            justification=justification,
        )

    log.info("  nmck: недостаточно данных  (%.1f мс)", elapsed_ms)
    return None


@router.get("/ste/{ste_id}", summary="Полная информация о СТЕ")
def ste_detail(
    ste_id: str,
    user: dict = Depends(current_user),
    client: SteSearchClient = Depends(get_client),
) -> SteDetail:
    log.info("GET /ste/%s  user=%r", ste_id, user.get("username"))
    detail = client.get_ste_detail(ste_id)
    if detail is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"СТЕ с ID {ste_id!r} не найдена")
    return SteDetail(**detail)


@router.get("/contracts/{ste_id}", summary="Контракты по ID СТЕ")
def contracts(
    ste_id: str,
    user: dict = Depends(current_user),
    client: SteSearchClient = Depends(get_client),
) -> dict[str, Any]:
    log.info("GET /contracts/%s  user=%r", ste_id, user.get("username"))
    t0 = time.perf_counter()
    data = client.get_contracts(ste_id)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    log.info("  contracts: %d записей  (%.1f мс)", len(data), elapsed_ms)
    return {"ste_id": ste_id, "count": len(data), "contracts": data}


@router.get("/interchangeable/{ste_id}", summary="Коммерчески взаимозаменяемые СТЕ (только v6)")
def interchangeable(
    ste_id: str,
    top_n: int = 10,
    min_score: float = 0.3,
    user: dict = Depends(current_user),
    client: SteSearchClient = Depends(get_client),
) -> dict[str, Any]:
    log.info(
        "GET /interchangeable/%s  user=%r  top_n=%d  min_score=%.2f",
        ste_id, user.get("username"), top_n, min_score,
    )
    t0 = time.perf_counter()
    items = client.find_interchangeable(ste_id, top_n=top_n, min_score=min_score)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    log.info("  interchangeable: %d аналогов  (%.1f мс)", len(items), elapsed_ms)
    return {
        "ste_id": ste_id,
        "count": len(items),
        "items": [InterchangeableItem(**item) for item in items],
    }
