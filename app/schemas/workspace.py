from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field, model_validator

from app.schemas.search import NmckData, NmckContractItem, SteItem


class NmckDataWithForce(NmckData):
    """NmckData + поля для корректировок и дат периода."""
    force_include: list[str] = Field(default_factory=list)
    force_exclude: list[str] = Field(default_factory=list)
    date_from: date | None = None
    date_to: date | None = None


class WorkspaceSearchResponse(BaseModel):
    workspace_id: str
    query: str
    ste_count: int
    ste: list[SteItem]
    contracts: list[dict]
    nmck: NmckData | None


class WorkspaceNmckRequest(BaseModel):
    force_include: list[str] = Field(default_factory=list)
    force_exclude: list[str] = Field(default_factory=list)
    date_from: date | None = None
    date_to: date | None = None
    # Поля для генерации обоснования (опционально)
    quantity: int | None = Field(None, ge=1)
    unit: str | None = None

    @model_validator(mode="after")
    def validate_dates(self) -> "WorkspaceNmckRequest":
        if self.date_from and self.date_to and self.date_from > self.date_to:
            raise ValueError("date_from не может быть позже date_to")
        return self


class WorkspaceNmckResponse(BaseModel):
    workspace_id: str
    nmck: NmckDataWithForce | None


class WorkspaceStateResponse(BaseModel):
    workspace_id: str
    search: dict
    ste: list[SteItem]
    contracts: list[dict]
    nmck: NmckDataWithForce | None
