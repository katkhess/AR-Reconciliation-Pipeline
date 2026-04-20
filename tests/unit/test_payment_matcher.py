"""Unit tests for PaymentMatcher."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from src.models.invoice import Invoice, InvoiceStatus, LineItem
from src.models.payment import Payment, PaymentMethod, PaymentStatus
from src.transformation.payment_matcher import PaymentMatcher


class TestMatchPaymentsToInvoices:
    def test_match_payments_to_invoices(
        self,
        payment_matcher: PaymentMatcher,
        sample_payments: list[Payment],
        sample_invoices: list[Invoice],
    ) -> None:
        matches = payment_matcher.match_payments_to_invoices(sample_payments, sample_invoices)
        assert len(matches) > 0

    def test_match_uses_invoice_id_as_key(
        self,
        payment_matcher: PaymentMatcher,
        sample_payments: list[Payment],
        sample_invoices: list[Invoice],
    ) -> None:
        matches = payment_matcher.match_payments_to_invoices(sample_payments, sample_invoices)
        matched_invoice_ids = {m.invoice_id for m in matches}
        # All matched invoice IDs must exist in the sample invoices
        invoice_ids = {inv.invoice_id for inv in sample_invoices}
        assert matched_invoice_ids.issubset(invoice_ids)

    def test_payment_with_unknown_invoice_is_excluded(
        self, payment_matcher: PaymentMatcher, sample_invoices: list[Invoice]
    ) -> None:
        orphan_payment = Payment(
            payment_id="PMT-999001",
            invoice_id="INV-999999",  # Does not exist
            customer_id="CUST-1001",
            payment_date="2024-03-01",
            amount_paid=Decimal("100.00"),
            payment_method=PaymentMethod.ACH,
            status=PaymentStatus.CLEARED,
        )
        matches = payment_matcher.match_payments_to_invoices([orphan_payment], sample_invoices)
        assert len(matches) == 0

    def test_detect_partial_payment(
        self,
        payment_matcher: PaymentMatcher,
        sample_payments: list[Payment],
        sample_invoices: list[Invoice],
    ) -> None:
        matches = payment_matcher.match_payments_to_invoices(sample_payments, sample_invoices)
        partial_matches = [m for m in matches if not m.is_fully_paid]
        assert len(partial_matches) > 0


class TestDetectOverpayments:
    def test_detect_overpayment(
        self,
        payment_matcher: PaymentMatcher,
        sample_payments: list[Payment],
        sample_invoices: list[Invoice],
    ) -> None:
        matches = payment_matcher.match_payments_to_invoices(sample_payments, sample_invoices)
        overpayments = payment_matcher.detect_overpayments(matches)
        assert len(overpayments) > 0

    def test_overpayment_amount_is_positive(
        self,
        payment_matcher: PaymentMatcher,
        sample_payments: list[Payment],
        sample_invoices: list[Invoice],
    ) -> None:
        matches = payment_matcher.match_payments_to_invoices(sample_payments, sample_invoices)
        overpayments = payment_matcher.detect_overpayments(matches)
        for op in overpayments:
            assert op.overpayment_amount > Decimal("0.00")

    def test_no_overpayments_when_all_exact(self, payment_matcher: PaymentMatcher) -> None:
        invoice = Invoice(
            invoice_id="INV-100099",
            customer_id="CUST-1099",
            order_date="2024-01-01",
            line_items=[LineItem(sku="SKU-ZETA0001", quantity=1, unit_price=Decimal("100.00"))],
            total_amount=Decimal("100.00"),
            status=InvoiceStatus.PAID,
        )
        payment = Payment(
            payment_id="PMT-299099",
            invoice_id="INV-100099",
            customer_id="CUST-1099",
            payment_date="2024-01-15",
            amount_paid=Decimal("100.00"),
            payment_method=PaymentMethod.ACH,
            status=PaymentStatus.CLEARED,
        )
        matches = payment_matcher.match_payments_to_invoices([payment], [invoice])
        assert payment_matcher.detect_overpayments(matches) == []


class TestDetectUnderpayments:
    def test_detect_underpayment(
        self,
        payment_matcher: PaymentMatcher,
        sample_payments: list[Payment],
        sample_invoices: list[Invoice],
    ) -> None:
        matches = payment_matcher.match_payments_to_invoices(sample_payments, sample_invoices)
        underpayments = payment_matcher.detect_underpayments(matches)
        assert len(underpayments) > 0

    def test_underpayment_outstanding_balance_is_positive(
        self,
        payment_matcher: PaymentMatcher,
        sample_payments: list[Payment],
        sample_invoices: list[Invoice],
    ) -> None:
        matches = payment_matcher.match_payments_to_invoices(sample_payments, sample_invoices)
        underpayments = payment_matcher.detect_underpayments(matches)
        for up in underpayments:
            assert up.outstanding_balance > Decimal("0.00")


class TestAgingReport:
    def test_aging_report_returns_dataframe(
        self,
        payment_matcher: PaymentMatcher,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
    ) -> None:
        matches = payment_matcher.match_payments_to_invoices(sample_payments, sample_invoices)
        df = payment_matcher.generate_aging_report(sample_invoices, matches)
        assert isinstance(df, pd.DataFrame)

    def test_aging_report_buckets(
        self,
        payment_matcher: PaymentMatcher,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
    ) -> None:
        matches = payment_matcher.match_payments_to_invoices(sample_payments, sample_invoices)
        # Use a fixed as_of_date far in the future so all invoices are past due
        df = payment_matcher.generate_aging_report(
            sample_invoices, matches, as_of_date=date(2025, 6, 1)
        )
        valid_buckets = {"Current", "1-30", "31-60", "61-90", "90+"}
        assert set(df["aging_bucket"].unique()).issubset(valid_buckets)

    def test_aging_report_has_required_columns(
        self,
        payment_matcher: PaymentMatcher,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
    ) -> None:
        matches = payment_matcher.match_payments_to_invoices(sample_payments, sample_invoices)
        df = payment_matcher.generate_aging_report(sample_invoices, matches)
        required_columns = {
            "invoice_id",
            "customer_id",
            "invoice_total",
            "amount_paid",
            "outstanding_balance",
            "aging_bucket",
        }
        assert required_columns.issubset(set(df.columns))

    def test_aging_report_outstanding_balance_non_negative(
        self,
        payment_matcher: PaymentMatcher,
        sample_invoices: list[Invoice],
        sample_payments: list[Payment],
    ) -> None:
        matches = payment_matcher.match_payments_to_invoices(sample_payments, sample_invoices)
        df = payment_matcher.generate_aging_report(sample_invoices, matches)
        assert (df["outstanding_balance"] >= 0).all()

    def test_aging_report_empty_invoices(self, payment_matcher: PaymentMatcher) -> None:
        df = payment_matcher.generate_aging_report([], [])
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
