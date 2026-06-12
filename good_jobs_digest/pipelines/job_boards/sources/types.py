"""Shared types for job-board source probes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class JobBoardFetchResult:
    source: str
    ok: bool
    method: str
    job_count: int
    available_fields: list[str] = field(default_factory=list)
    sample_job: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    notes: str = ""

    def summary_line(self) -> str:
        status = "OK" if self.ok else "FAIL"
        fields = ", ".join(self.available_fields[:12])
        if len(self.available_fields) > 12:
            fields += ", …"
        extra = f" — {self.notes}" if self.notes else ""
        err = f" ({self.error})" if self.error and not self.ok else ""
        return (
            f"[{status}] {self.source} via {self.method}: "
            f"{self.job_count} jobs; fields: {fields or 'n/a'}{extra}{err}"
        )
