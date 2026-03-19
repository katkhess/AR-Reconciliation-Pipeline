"""Unit tests for DiscrepancyDetector."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd
import pytest

from src.models.invoice import Invoice, InvoiceStatus, LineItem
from src.models.payment import Payment, PaymentMethod, PaymentStatus
from src.models.return_record import ReturnLineItem, ReturnReasonCode, ReturnRecord, ReturnStatus
from src.transformation.discrepancy_detector import DiscrepancyDetector, DiscrepancyReport


class TestDetectAll:
    def test_detect_all_returns_report(
        self,
        discrepancy_detector: DiscrepancyDetector,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
        sample_returns: list[ReturnRecord],
    ) -> None:
        report = discrepancy_detector.detect_all(sample_invoices, sample_payments, sample_returns)
        assert isinstance(report, DiscrepancyReport)

    def test_detect_all_finds_sku_mismatches(
        self,
        discrepancy_detector: DiscrepancyDetector,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
        sample_returns: list[ReturnRecord],
    ) -> None:
        report = discrepancy_detector.detect_all(sample_invoices, sample_payments, sample_returns)
        assert report.total_sku_mismatches >= 2

    def test_detect_all_finds_payment_discrepancies(
        self,
        discrepancy_detector: DiscrepancyDetector,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
        sample_returns: list[ReturnRecord],
    ) -> None:
        report = discrepancy_detector.detect_all(sample_invoices, sample_payments, sample_returns)
        assert report.total_overpayments > 0 or report.total_underpayments > 0

    def test_detect_all_classifies_mismatches(
        self,
        discrepancy_detector: DiscrepancyDetector,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
        sample_returns: list[ReturnRecord],
    ) -> None:
        report = discrepancy_detector.detect_all(sample_invoices, sample_payments, sample_returns)
        for mismatch in report.sku_mismatches:
            assert mismatch.classification is not None

    def test_detect_all_empty_inputs(
        self, discrepancy_detector: DiscrepancyDetector
    ) -> None:
        report = discrepancy_detector.detect_all([], [], [])
        assert report.total_sku_mismatches == 0
        assert report.total_overpayments == 0
        assert report.total_underpayments == 0
        assert not report.has_discrepancies


class TestFlagHighRiskAccounts:
    def test_flag_high_risk_accounts(
        self,
        discrepancy_detector: DiscrepancyDetector,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
        sample_returns: list[ReturnRecord],
    ) -> None:
        report = discrepancy_detector.detect_all(sample_invoices, sample_payments, sample_returns)
        high_risk = discrepancy_detector.flag_high_risk_accounts(report)
        assert isinstance(high_risk, list)
        # All flagged customers must be real customer IDs
        known_customers = {inv.customer_id for inv in sample_invoices}
        for cid in high_risk:
            assert cid in known_customers

    def test_flag_high_risk_returns_sorted_list(
        self,
        discrepancy_detector: DiscrepancyDetector,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
        sample_returns: list[ReturnRecord],
    ) -> None:
        report = discrepancy_detector.detect_all(sample_invoices, sample_payments, sample_returns)
        high_risk = discrepancy_detector.flag_high_risk_accounts(report)
        assert high_risk == sorted(high_risk)

    def test_flag_high_risk_empty_report(
        self, discrepancy_detector: DiscrepancyDetector
    ) -> None:
        empty_report = DiscrepancyReport()
        high_risk = discrepancy_detector.flag_high_risk_accounts(empty_report)
        assert high_risk == []


class TestReportToDataframe:
    def test_report_to_dataframe(
        self,
        discrepancy_detector: DiscrepancyDetector,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
        sample_returns: list[ReturnRecord],
    ) -> None:
        report = discrepancy_detector.detect_all(sample_invoices, sample_payments, sample_returns)
        df = discrepancy_detector.to_dataframe(report)
        assert isinstance(df, pd.DataFrame)

    def test_report_dataframe_columns(
        self,
        discrepancy_detector: DiscrepancyDetector,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
        sample_returns: list[ReturnRecord],
    ) -> None:
        report = discrepancy_detector.detect_all(sample_invoices, sample_payments, sample_returns)
        df = discrepancy_detector.to_dataframe(report)
        expected_columns = {
            "discrepancy_type",
            "entity_id",
            "customer_id",
            "reference_id",
            "amount",
            "detail",
            "classification",
        }
        assert expected_columns.issubset(set(df.columns))

    def test_report_dataframe_discrepancy_types(
        self,
        discrepancy_detector: DiscrepancyDetector,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
        sample_returns: list[ReturnRecord],
    ) -> None:
        report = discrepancy_detector.detect_all(sample_invoices, sample_payments, sample_returns)
        df = discrepancy_detector.to_dataframe(report)
        valid_types = {"SKU_MISMATCH", "OVERPAYMENT", "UNDERPAYMENT"}
        assert set(df["discrepancy_type"].unique()).issubset(valid_types)

    def test_report_to_dataframe_empty_report(
        self, discrepancy_detector: DiscrepancyDetector
    ) -> None:
        df = discrepancy_detector.to_dataframe(DiscrepancyReport())
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
