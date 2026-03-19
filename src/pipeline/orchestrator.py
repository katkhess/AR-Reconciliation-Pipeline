from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog

from config.settings import Settings, get_settings
from src.ingestion.invoice_loader import InvoiceLoader
from src.ingestion.payment_loader import PaymentLoader
from src.ingestion.returns_loader import ReturnsLoader
from src.models.invoice import Invoice
from src.models.payment import Payment
from src.models.return_record import ReturnRecord
from src.pipeline.stages import PipelineStage, StageResult, StageStatus
from src.transformation.discrepancy_detector import DiscrepancyDetector, DiscrepancyReport
from src.transformation.payment_matcher import PaymentMatcher
from src.transformation.sku_reconciler import SKUReconciler
from src.utils.logger import configure_logger

logger = configure_logger("ar_pipeline_orchestrator")


@dataclass
class PipelineResult:
    """Aggregated result of a full pipeline run."""

    stage_results: list[StageResult] = field(default_factory=list)
    invoices: list[Invoice] = field(default_factory=list)
    payments: list[Payment] = field(default_factory=list)
    returns: list[ReturnRecord] = field(default_factory=list)
    discrepancy_report: DiscrepancyReport | None = None
    total_duration_seconds: float = 0.0
    success: bool = False

    @property
    def failed_stages(self) -> list[StageResult]:
        return [r for r in self.stage_results if r.failed]

    @property
    def summary(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "total_duration_seconds": round(self.total_duration_seconds, 3),
            "invoices_processed": len(self.invoices),
            "payments_processed": len(self.payments),
            "returns_processed": len(self.returns),
            "failed_stages": [r.stage.value for r in self.failed_stages],
            "discrepancies": {
                "sku_mismatches": (
                    self.discrepancy_report.total_sku_mismatches
                    if self.discrepancy_report
                    else 0
                ),
                "overpayments": (
                    self.discrepancy_report.total_overpayments
                    if self.discrepancy_report
                    else 0
                ),
                "underpayments": (
                    self.discrepancy_report.total_underpayments
                    if self.discrepancy_report
                    else 0
                ),
                "fraud_alerts": (
                    len(self.discrepancy_report.fraud_alerts)
                    if self.discrepancy_report
                    else 0
                ),
            },
        }


class ARPipelineOrchestrator:
    """Orchestrates the end-to-end AR reconciliation pipeline.

    Pipeline stages:
    1. INGESTION    — load invoices, payments, and returns from source files/S3.
    2. VALIDATION   — validate loaded records against JSON schemas.
    3. TRANSFORMATION — detect SKU mismatches, match payments, generate discrepancy report.
    4. REPORTING    — emit structured log summary and aging report.
    5. ARCHIVAL     — (stub) archive processed data to S3/Redshift.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        invoice_loader: InvoiceLoader | None = None,
        payment_loader: PaymentLoader | None = None,
        returns_loader: ReturnsLoader | None = None,
        sku_reconciler: SKUReconciler | None = None,
        payment_matcher: PaymentMatcher | None = None,
        discrepancy_detector: DiscrepancyDetector | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._log = structlog.get_logger(self.__class__.__name__).bind(
            environment=self._settings.environment
        )

        schema_base = Path(__file__).resolve().parent.parent.parent / "config" / "schemas"

        self._invoice_loader = invoice_loader or InvoiceLoader(
            schema_path=schema_base / "invoices_schema.json",
            aws_region=self._settings.aws_region,
        )
        self._payment_loader = payment_loader or PaymentLoader(
            schema_path=schema_base / "payments_schema.json",
            aws_region=self._settings.aws_region,
        )
        self._returns_loader = returns_loader or ReturnsLoader(
            schema_path=schema_base / "returns_schema.json",
            aws_region=self._settings.aws_region,
        )

        _sku_reconciler = sku_reconciler or SKUReconciler()
        _payment_matcher = payment_matcher or PaymentMatcher()
        self._detector = discrepancy_detector or DiscrepancyDetector(
            sku_reconciler=_sku_reconciler,
            payment_matcher=_payment_matcher,
        )
        self._payment_matcher = _payment_matcher

    def run(
        self,
        invoice_source: str | Path,
        payment_source: str | Path,
        returns_source: str | Path,
    ) -> PipelineResult:
        """Execute all pipeline stages in order.

        Args:
            invoice_source: Path (local) or ``s3://bucket/key`` URI for invoices.
            payment_source: Path (local) or ``s3://bucket/key`` URI for payments.
            returns_source: Path (local) or ``s3://bucket/key`` URI for returns.

        Returns:
            A :class:`PipelineResult` describing what was processed and found.
        """
        pipeline_start = time.perf_counter()
        result = PipelineResult()

        self._log.info(
            "pipeline_started",
            invoice_source=str(invoice_source),
            payment_source=str(payment_source),
            returns_source=str(returns_source),
        )

        # --- STAGE 1: INGESTION ---
        ingestion_result = self._run_ingestion(
            result, invoice_source, payment_source, returns_source
        )
        result.stage_results.append(ingestion_result)
        if ingestion_result.failed:
            result.total_duration_seconds = time.perf_counter() - pipeline_start
            self._log.error("pipeline_aborted_at_ingestion")
            return result

        # --- STAGE 2: VALIDATION ---
        validation_result = self._run_validation(result)
        result.stage_results.append(validation_result)

        # --- STAGE 3: TRANSFORMATION ---
        transformation_result = self._run_transformation(result)
        result.stage_results.append(transformation_result)
        if transformation_result.failed:
            result.total_duration_seconds = time.perf_counter() - pipeline_start
            self._log.error("pipeline_aborted_at_transformation")
            return result

        # --- STAGE 4: REPORTING ---
        reporting_result = self._run_reporting(result)
        result.stage_results.append(reporting_result)

        # --- STAGE 5: ARCHIVAL ---
        archival_result = self._run_archival(result)
        result.stage_results.append(archival_result)

        result.total_duration_seconds = time.perf_counter() - pipeline_start
        result.success = not any(r.failed for r in result.stage_results)

        self._log.info("pipeline_complete", **result.summary)
        return result

    # ------------------------------------------------------------------
    # Private stage runners
    # ------------------------------------------------------------------

    def _run_ingestion(
        self,
        result: PipelineResult,
        invoice_source: str | Path,
        payment_source: str | Path,
        returns_source: str | Path,
    ) -> StageResult:
        stage_start = time.perf_counter()
        self._log.info("stage_started", stage=PipelineStage.INGESTION.value)
        errors: list[str] = []

        try:
            result.invoices = self._invoice_loader.load_from_csv(invoice_source)
        except Exception as exc:
            errors.append(f"Invoice ingestion failed: {exc}")
            self._log.error("invoice_ingestion_failed", error=str(exc))

        try:
            result.payments = self._payment_loader.load_from_csv(payment_source)
        except Exception as exc:
            errors.append(f"Payment ingestion failed: {exc}")
            self._log.error("payment_ingestion_failed", error=str(exc))

        try:
            result.returns = self._returns_loader.load_from_csv(returns_source)
        except Exception as exc:
            errors.append(f"Returns ingestion failed: {exc}")
            self._log.error("returns_ingestion_failed", error=str(exc))

        records = len(result.invoices) + len(result.payments) + len(result.returns)
        status = StageStatus.FAILED if len(errors) == 3 else (
            StageStatus.PARTIAL if errors else StageStatus.SUCCESS
        )
        return StageResult(
            stage=PipelineStage.INGESTION,
            status=status,
            records_processed=records,
            errors=errors,
            duration_seconds=time.perf_counter() - stage_start,
        )

    def _run_validation(self, result: PipelineResult) -> StageResult:
        stage_start = time.perf_counter()
        self._log.info("stage_started", stage=PipelineStage.VALIDATION.value)
        errors: list[str] = []

        # Basic cross-record referential integrity checks
        invoice_ids = {inv.invoice_id for inv in result.invoices}
        orphaned_payments = [
            p.payment_id for p in result.payments if p.invoice_id not in invoice_ids
        ]
        orphaned_returns = [
            r.return_id for r in result.returns if r.original_invoice_id not in invoice_ids
        ]

        if orphaned_payments:
            msg = f"{len(orphaned_payments)} payments reference unknown invoices"
            errors.append(msg)
            self._log.warning("orphaned_payments", count=len(orphaned_payments))

        if orphaned_returns:
            msg = f"{len(orphaned_returns)} returns reference unknown invoices"
            errors.append(msg)
            self._log.warning("orphaned_returns", count=len(orphaned_returns))

        return StageResult(
            stage=PipelineStage.VALIDATION,
            status=StageStatus.SUCCESS if not errors else StageStatus.PARTIAL,
            records_processed=len(result.invoices) + len(result.payments) + len(result.returns),
            errors=errors,
            duration_seconds=time.perf_counter() - stage_start,
        )

    def _run_transformation(self, result: PipelineResult) -> StageResult:
        stage_start = time.perf_counter()
        self._log.info("stage_started", stage=PipelineStage.TRANSFORMATION.value)
        errors: list[str] = []

        try:
            result.discrepancy_report = self._detector.detect_all(
                invoices=result.invoices,
                payments=result.payments,
                returns=result.returns,
            )
        except Exception as exc:
            errors.append(str(exc))
            self._log.error("transformation_failed", error=str(exc))
            return StageResult(
                stage=PipelineStage.TRANSFORMATION,
                status=StageStatus.FAILED,
                errors=errors,
                duration_seconds=time.perf_counter() - stage_start,
            )

        return StageResult(
            stage=PipelineStage.TRANSFORMATION,
            status=StageStatus.SUCCESS,
            records_processed=(
                result.discrepancy_report.total_sku_mismatches
                + result.discrepancy_report.total_overpayments
                + result.discrepancy_report.total_underpayments
            ),
            duration_seconds=time.perf_counter() - stage_start,
            metadata=result.discrepancy_report.__class__.__name__,  # type: ignore[assignment]
        )

    def _run_reporting(self, result: PipelineResult) -> StageResult:
        stage_start = time.perf_counter()
        self._log.info("stage_started", stage=PipelineStage.REPORTING.value)

        if result.discrepancy_report:
            aging_df = self._payment_matcher.generate_aging_report(
                result.invoices, result.discrepancy_report.payment_matches
            )
            high_risk = self._detector.flag_high_risk_accounts(result.discrepancy_report)
            self._log.info(
                "reporting_summary",
                aging_buckets=aging_df["aging_bucket"].value_counts().to_dict()
                if not aging_df.empty
                else {},
                high_risk_customers=high_risk,
            )

        return StageResult(
            stage=PipelineStage.REPORTING,
            status=StageStatus.SUCCESS,
            records_processed=len(result.invoices),
            duration_seconds=time.perf_counter() - stage_start,
        )

    def _run_archival(self, result: PipelineResult) -> StageResult:
        stage_start = time.perf_counter()
        self._log.info("stage_started", stage=PipelineStage.ARCHIVAL.value)
        # In production this would write Parquet files to S3 and load into Redshift.
        self._log.info(
            "archival_skipped_in_non_prod",
            environment=self._settings.environment,
        )
        return StageResult(
            stage=PipelineStage.ARCHIVAL,
            status=StageStatus.SKIPPED if not self._settings.is_production else StageStatus.SUCCESS,
            records_processed=0,
            duration_seconds=time.perf_counter() - stage_start,
        )


def main() -> int:
    """CLI entry point for running the AR pipeline."""
    parser = argparse.ArgumentParser(description="AR Reconciliation Pipeline")
    parser.add_argument("--invoice-source", required=True, help="Path or S3 URI for invoices CSV")
    parser.add_argument("--payment-source", required=True, help="Path or S3 URI for payments CSV")
    parser.add_argument("--returns-source", required=True, help="Path or S3 URI for returns CSV")
    args = parser.parse_args()

    orchestrator = ARPipelineOrchestrator()
    pipeline_result = orchestrator.run(
        invoice_source=args.invoice_source,
        payment_source=args.payment_source,
        returns_source=args.returns_source,
    )

    if pipeline_result.success:
        print("Pipeline completed successfully.")
        return 0
    else:
        print("Pipeline completed with errors.", file=sys.stderr)
        for sr in pipeline_result.failed_stages:
            print(f"  Stage {sr.stage.value}: {sr.errors}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
