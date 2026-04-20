from __future__ import annotations

from enum import Enum
from dataclasses import dataclass, field
from typing import Any


class PipelineStage(str, Enum):
    """Ordered stages of the AR reconciliation pipeline."""

    INGESTION = "INGESTION"
    VALIDATION = "VALIDATION"
    TRANSFORMATION = "TRANSFORMATION"
    REPORTING = "REPORTING"
    ARCHIVAL = "ARCHIVAL"


class StageStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    PARTIAL = "PARTIAL"


@dataclass
class StageResult:
    """Outcome of a single pipeline stage execution."""

    stage: PipelineStage
    status: StageStatus
    records_processed: int = 0
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return self.status == StageStatus.SUCCESS

    @property
    def failed(self) -> bool:
        return self.status == StageStatus.FAILED
