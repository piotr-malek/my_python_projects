"""Pydantic models for LLM score output validation."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class JobScorePayload(BaseModel):
    role_relevance: int = Field(ge=0, le=100)
    mission_alignment: int = Field(ge=0, le=100)
    candidate_fit: int = Field(ge=0, le=100)
    remote_ok: bool
    extracted_salary: str | None = None
    top_requirements: list[str] = Field(default_factory=list)
    risks_or_gaps: list[str] = Field(default_factory=list)
    one_line_summary: str = ""

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump()


class EmployerMissionResult(BaseModel):
    company_name: str
    purpose_driven: bool
    reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump()


class EmployerMissionScoreResult(BaseModel):
    company_name: str
    mission_score: int = Field(ge=0, le=100)
    purpose_driven: bool
    reason: str = ""
    mission_type: str = ""

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump()
