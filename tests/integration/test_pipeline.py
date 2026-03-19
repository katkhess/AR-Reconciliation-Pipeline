"""Integration tests for the AR Pipeline Orchestrator."""
from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.orchestrator import ARPipelineOrchestrator, PipelineResult
from src.pipeline.stages import StageStatus

# All integration tests use the sample CSV files shipped with the repo
SAMPLE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "sample"
INVOICE_CSV = SAMPLE_DIR / "invoices.csv"
PAYMENT_CSV = SAMPLE_DIR / "payments.csv"
RETURNS_CSV = SAMPLE_DIR / "returns.csv"


@pytest.fixture()
def orchestrator() -> ARPipelineOrchestrator:
    return ARPipelineOrchestrator()


class TestFullPipelineRun:
    def test_full_pipeline_run(self, orchestrator: ARPipelineOrchestrator) -> None:
        result = orchestrator.run(
            invoice_source=INVOICE_CSV,
            payment_source=PAYMENT_CSV,
            returns_source=RETURNS_CSV,
        )
        assert isinstance(result, PipelineResult)
        assert result.success is True

    def test_full_pipeline_loads_all_records(
        self, orchestrator: ARPipelineOrchestrator
    ) -> None:
        result = orchestrator.run(
            invoice_source=INVOICE_CSV,
            payment_source=PAYMENT_CSV,
            returns_source=RETURNS_CSV,
        )
        assert len(result.invoices) == 10
        assert len(result.payments) == 15
        assert len(result.returns) == 8

    def test_full_pipeline_produces_discrepancy_report(
        self, orchestrator: ARPipelineOrchestrator
    ) -> None:
        result = orchestrator.run(
            invoice_source=INVOICE_CSV,
            payment_source=PAYMENT_CSV,
            returns_source=RETURNS_CSV,
        )
        assert result.discrepancy_report is not None
        assert result.discrepancy_report.has_discrepancies is True

    def test_full_pipeline_detects_sku_mismatches(
        self, orchestrator: ARPipelineOrchestrator
    ) -> None:
        result = orchestrator.run(
            invoice_source=INVOICE_CSV,
            payment_source=PAYMENT_CSV,
            returns_source=RETURNS_CSV,
        )
        assert result.discrepancy_report is not None
        assert result.discrepancy_report.total_sku_mismatches >= 4

    def test_full_pipeline_all_stages_complete(
        self, orchestrator: ARPipelineOrchestrator
    ) -> None:
        result = orchestrator.run(
            invoice_source=INVOICE_CSV,
            payment_source=PAYMENT_CSV,
            returns_source=RETURNS_CSV,
        )
        from src.pipeline.stages import PipelineStage

        executed_stages = {r.stage for r in result.stage_results}
        expected_stages = set(PipelineStage)
        assert expected_stages == executed_stages

    def test_full_pipeline_no_failed_stages(
        self, orchestrator: ARPipelineOrchestrator
    ) -> None:
        result = orchestrator.run(
            invoice_source=INVOICE_CSV,
            payment_source=PAYMENT_CSV,
            returns_source=RETURNS_CSV,
        )
        assert result.failed_stages == []

    def test_full_pipeline_has_duration(self, orchestrator: ARPipelineOrchestrator) -> None:
        result = orchestrator.run(
            invoice_source=INVOICE_CSV,
            payment_source=PAYMENT_CSV,
            returns_source=RETURNS_CSV,
        )
        assert result.total_duration_seconds > 0


class TestPipelineHandlesEmptyReturns:
    def test_pipeline_handles_empty_returns(
        self, orchestrator: ARPipelineOrchestrator, tmp_path: Path
    ) -> None:
        # Create an empty returns CSV (header only)
        empty_returns = tmp_path / "empty_returns.csv"
        empty_returns.write_text(
            "return_id,original_invoice_id,customer_id,return_date,"
            "line_items,reason_code,status,credit_memo_id,notes\n"
        )

        result = orchestrator.run(
            invoice_source=INVOICE_CSV,
            payment_source=PAYMENT_CSV,
            returns_source=empty_returns,
        )
        assert isinstance(result, PipelineResult)
        assert result.returns == []
        assert result.discrepancy_report is not None
        assert result.discrepancy_report.total_sku_mismatches == 0

    def test_pipeline_still_detects_payment_discrepancies_with_empty_returns(
        self, orchestrator: ARPipelineOrchestrator, tmp_path: Path
    ) -> None:
        empty_returns = tmp_path / "empty_returns.csv"
        empty_returns.write_text(
            "return_id,original_invoice_id,customer_id,return_date,"
            "line_items,reason_code,status,credit_memo_id,notes\n"
        )

        result = orchestrator.run(
            invoice_source=INVOICE_CSV,
            payment_source=PAYMENT_CSV,
            returns_source=empty_returns,
        )
        report = result.discrepancy_report
        assert report is not None
        assert report.total_overpayments + report.total_underpayments > 0


class TestPipelineHandlesAllPartialPayments:
    def test_pipeline_handles_all_partial_payments(
        self, orchestrator: ARPipelineOrchestrator, tmp_path: Path
    ) -> None:
        # Build a payments file where every payment is partial
        partial_payments = tmp_path / "partial_payments.csv"
        partial_payments.write_text(
            "payment_id,invoice_id,customer_id,payment_date,amount_paid,"
            "payment_method,reference_number,status,notes\n"
            "PMT-200001,INV-100001,CUST-1001,2024-01-20,100.00,ACH,REF-001,PARTIAL,\n"
            "PMT-200002,INV-100002,CUST-1002,2024-01-25,200.00,WIRE,REF-002,PARTIAL,\n"
            "PMT-200003,INV-100005,CUST-1004,2024-01-28,500.00,ACH,REF-003,PARTIAL,\n"
        )

        result = orchestrator.run(
            invoice_source=INVOICE_CSV,
            payment_source=partial_payments,
            returns_source=RETURNS_CSV,
        )
        assert isinstance(result, PipelineResult)
        report = result.discrepancy_report
        assert report is not None
        # All partial payments should show as underpayments
        assert report.total_underpayments >= 3

    def test_pipeline_summary_includes_discrepancy_counts(
        self, orchestrator: ARPipelineOrchestrator, tmp_path: Path
    ) -> None:
        partial_payments = tmp_path / "partial_payments.csv"
        partial_payments.write_text(
            "payment_id,invoice_id,customer_id,payment_date,amount_paid,"
            "payment_method,reference_number,status,notes\n"
            "PMT-200001,INV-100001,CUST-1001,2024-01-20,100.00,ACH,REF-001,PARTIAL,\n"
        )

        result = orchestrator.run(
            invoice_source=INVOICE_CSV,
            payment_source=partial_payments,
            returns_source=RETURNS_CSV,
        )
        summary = result.summary
        assert "discrepancies" in summary
        assert "underpayments" in summary["discrepancies"]
        assert summary["discrepancies"]["underpayments"] > 0
