import logging
import time

from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import current_user, get_client
from app.schemas.search import NmckContractItem, NmckData, SteItem, SearchRequest
from app.schemas.workspace import (
    NmckDataWithForce,
    WorkspaceNmckRequest,
    WorkspaceNmckResponse,
    WorkspaceSearchResponse,
    WorkspaceStateResponse,
)
from utils.client import SteSearchClient
from utils.nmck import calculate_nmck
import utils.workspace as ws

router = APIRouter(prefix="/workspace", tags=["workspace"])
log = logging.getLogger("smartsearch")


def _build_nmck_data(result) -> NmckData | None:
    if result is None:
        return None
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
    )


def _build_nmck_data_with_force(
    result,
    force_include: list[str],
    force_exclude: list[str],
    date_from,
    date_to,
    justification: str | None = None,
) -> NmckDataWithForce | None:
    if result is None:
        return None
    return NmckDataWithForce(
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
        force_include=force_include,
        force_exclude=force_exclude,
        date_from=date_from,
        date_to=date_to,
        justification=justification,
    )


def _require_workspace(workspace_id: str) -> dict:
    state = ws.get(workspace_id)
    if state is None:
        raise HTTPException(
            status_code=404,
            detail="Workspace не найден или истёк. Выполните поиск заново.",
        )
    return state


@router.post("/search", summary="Поиск СТЕ + создание workspace")
def workspace_search(
    req: SearchRequest,
    user: dict = Depends(current_user),
    client: SteSearchClient = Depends(get_client),
) -> WorkspaceSearchResponse:
    top_k = req.top_k if req.top_k is not None else client._engine.size
    log.info(
        "POST /workspace/search  user=%r  query=%r  region=%r  unit=%r  "
        "date=%s→%s  vat=%r",
        user.get("username"), req.query, req.region, req.unit,
        req.date_from, req.date_to, req.vat,
    )

    t0 = time.perf_counter()
    results = client.search(req.query, top_k=top_k, min_score=req.min_score)

    if any([req.region, req.unit, req.date_from, req.date_to, req.vat]):
        results = client.filter_ste_by_contracts(
            results,
            region=req.region, unit=req.unit,
            date_from=req.date_from, date_to=req.date_to, vat=req.vat,
        )

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

    nmck_result = calculate_nmck(
        contracts,
        date_from=req.date_from,
        date_to=req.date_to,
    )
    nmck_data = _build_nmck_data(nmck_result)

    search_ms = (time.perf_counter() - t0) * 1000
    log.info(
        "  search: %d СТЕ  %d контрактов  nmck=%s  (%.1f мс)",
        len(ste_list), len(contracts),
        f"{nmck_data.nmck:.2f}" if nmck_data else "нет данных",
        search_ms,
    )

    state = {
        "search": {
            "query": req.query,
            "region": req.region,
            "unit": req.unit,
            "date_from": req.date_from.isoformat() if req.date_from else None,
            "date_to": req.date_to.isoformat() if req.date_to else None,
            "vat": req.vat,
            "top_k": req.top_k,
            "min_score": req.min_score,
        },
        "ste": [s.model_dump() for s in ste_list],
        "contracts": contracts,
        "nmck": {
            "result": nmck_data.model_dump() if nmck_data else None,
            "force_include": [],
            "force_exclude": [],
            "date_from": None,
            "date_to": None,
        },
    }

    workspace_id = ws.create(state)
    log.info("  workspace создан: %s", workspace_id)

    return WorkspaceSearchResponse(
        workspace_id=workspace_id,
        query=req.query,
        ste_count=len(ste_list),
        ste=ste_list,
        contracts=contracts,
        nmck=nmck_data,
    )


@router.post("/{workspace_id}/nmck", summary="Пересчёт НМЦК с корректировками")
def workspace_nmck(
    workspace_id: str,
    req: WorkspaceNmckRequest,
    user: dict = Depends(current_user),
) -> WorkspaceNmckResponse:
    state = _require_workspace(workspace_id)

    # Даты: берём из запроса; если не переданы — берём сохранённые в Redis
    stored_nmck = state.get("nmck", {})
    stored_date_from_str = stored_nmck.get("date_from")
    stored_date_to_str = stored_nmck.get("date_to")

    from datetime import date as _date
    def _parse_stored_date(s: str | None) -> _date | None:
        return _date.fromisoformat(s) if s else None

    effective_date_from = req.date_from or _parse_stored_date(stored_date_from_str)
    effective_date_to = req.date_to or _parse_stored_date(stored_date_to_str)

    log.info(
        "POST /workspace/%s/nmck  user=%r  fi=%d  fe=%d  date=%s→%s",
        workspace_id, user.get("username"),
        len(req.force_include), len(req.force_exclude),
        effective_date_from, effective_date_to,
    )

    t0 = time.perf_counter()
    result = calculate_nmck(
        state["contracts"],
        date_from=effective_date_from,
        date_to=effective_date_to,
        force_include=req.force_include or None,
        force_exclude=req.force_exclude or None,
    )

    justification: str | None = None
    if result:
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

    nmck_data = _build_nmck_data_with_force(
        result,
        force_include=req.force_include,
        force_exclude=req.force_exclude,
        date_from=effective_date_from,
        date_to=effective_date_to,
        justification=justification,
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000
    log.info(
        "  nmck: %s  (%.1f мс)",
        f"{nmck_data.nmck:.2f}" if nmck_data else "нет данных",
        elapsed_ms,
    )

    ws.update_nmck(workspace_id, {
        "result": nmck_data.model_dump() if nmck_data else None,
        "force_include": req.force_include,
        "force_exclude": req.force_exclude,
        "date_from": effective_date_from.isoformat() if effective_date_from else None,
        "date_to": effective_date_to.isoformat() if effective_date_to else None,
    })

    return WorkspaceNmckResponse(workspace_id=workspace_id, nmck=nmck_data)


@router.get("/{workspace_id}", summary="Полное состояние workspace")
def workspace_get(
    workspace_id: str,
    user: dict = Depends(current_user),
) -> WorkspaceStateResponse:
    state = _require_workspace(workspace_id)
    log.info("GET /workspace/%s  user=%r", workspace_id, user.get("username"))

    nmck_raw = state.get("nmck", {})
    nmck_result_raw = nmck_raw.get("result")

    if nmck_result_raw:
        nmck_data = NmckDataWithForce.model_validate({
            **nmck_result_raw,
            "force_include": nmck_raw.get("force_include", []),
            "force_exclude": nmck_raw.get("force_exclude", []),
            "date_from": nmck_raw.get("date_from"),
            "date_to": nmck_raw.get("date_to"),
        })
    else:
        nmck_data = None

    return WorkspaceStateResponse(
        workspace_id=workspace_id,
        search=state["search"],
        ste=[SteItem.model_validate(s) for s in state["ste"]],
        contracts=state["contracts"],
        nmck=nmck_data,
    )


@router.delete("/{workspace_id}", summary="Удалить workspace")
def workspace_delete(
    workspace_id: str,
    user: dict = Depends(current_user),
) -> dict:
    found = ws.delete(workspace_id)
    log.info("DELETE /workspace/%s  user=%r  found=%s", workspace_id, user.get("username"), found)
    if not found:
        raise HTTPException(status_code=404, detail="Workspace не найден")
    return {"deleted": workspace_id}
