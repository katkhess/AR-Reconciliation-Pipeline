"""Unit tests for SKUReconciler."""
from __future__ import annotations

from decimal import Decimal

import pandas as pd
import pytest

from src.models.return_record import ReturnLineItem, ReturnReasonCode, ReturnRecord, ReturnStatus
from src.transformation.sku_reconciler import MismatchClassification, SKUMismatch, SKUReconciler


class TestDetectMismatches:
    def test_detect_mismatches_finds_mismatches(
        self, sku_reconciler: SKUReconciler, sample_returns: list[ReturnRecord]
    ) -> None:
        mismatches = sku_reconciler.detect_mismatches(sample_returns)
        # sample_returns has 2 returns with SKU mismatches
        assert len(mismatches) == 2
        return_ids = {m.return_id for m in mismatches}
        assert "RET-300002" in return_ids
        assert "RET-300008" in return_ids

    def test_detect_mismatches_returns_empty_for_clean_returns(
        self, sku_reconciler: SKUReconciler
    ) -> None:
        clean_returns = [
            ReturnRecord(
                return_id="RET-399001",
                original_invoice_id="INV-100001",
                customer_id="CUST-1001",
                return_date="2024-03-01",
                line_items=[
                    ReturnLineItem(
                        original_sku="SKU-ALPHA001",
                        returned_sku="SKU-ALPHA001",
                        quantity=1,
                        unit_price=Decimal("25.00"),
                    )
                ],
                reason_code=ReturnReasonCode.DEFECTIVE,
                status=ReturnStatus.APPROVED,
            )
        ]
        mismatches = sku_reconciler.detect_mismatches(clean_returns)
        assert mismatches == []

    def test_detect_mismatches_populates_fields_correctly(
        self, sku_reconciler: SKUReconciler, sample_returns: list[ReturnRecord]
    ) -> None:
        mismatches = sku_reconciler.detect_mismatches(sample_returns)
        mismatch = next(m for m in mismatches if m.return_id == "RET-300002")

        assert mismatch.original_sku == "SKU-BETA0002"
        assert mismatch.returned_sku == "SKU-GAMM0001"
        assert mismatch.customer_id == "CUST-1002"
        assert mismatch.original_invoice_id == "INV-100002"

    def test_detect_mismatches_with_empty_list(self, sku_reconciler: SKUReconciler) -> None:
        assert sku_reconciler.detect_mismatches([]) == []


class TestClassifyMismatch:
    def test_classify_mismatch_as_error(self, sku_reconciler: SKUReconciler) -> None:
        mismatch = SKUMismatch(
            return_id="RET-399002",
            original_invoice_id="INV-100002",
            customer_id="CUST-1002",
            return_date="2024-02-05",
            original_sku="SKU-BETA0002",
            returned_sku="SKU-GAMM0001",
            quantity=1,
            unit_price=Decimal("150.00"),
            reason_code="WRONG_ITEM",
        )
        result = sku_reconciler.classify_mismatch(mismatch)
        assert result == MismatchClassification.ERROR

    def test_classify_mismatch_as_fraud(self, sku_reconciler: SKUReconciler) -> None:
        mismatch = SKUMismatch(
            return_id="RET-399003",
            original_invoice_id="INV-100005",
            customer_id="CUST-1004",
            return_date="2024-02-20",
            original_sku="SKU-GAMM0001",
            returned_sku="SKU-DELT0001",
            quantity=1,
            unit_price=Decimal("500.00"),
            reason_code="OTHER",  # Not a low-risk reason code
        )
        result = sku_reconciler.classify_mismatch(mismatch)
        assert result == MismatchClassification.FRAUD

    def test_classify_mismatch_as_substitution(self, sku_reconciler: SKUReconciler) -> None:
        mismatch = SKUMismatch(
            return_id="RET-399004",
            original_invoice_id="INV-100007",
            customer_id="CUST-1005",
            return_date="2024-02-10",
            original_sku="SKU-PRMA0001",
            returned_sku="SKU-PRMB0001",
            quantity=2,
            unit_price=Decimal("50.00"),  # Below fraud threshold
            reason_code="CUSTOMER_CHANGE_OF_MIND",
        )
        result = sku_reconciler.classify_mismatch(mismatch)
        assert result == MismatchClassification.SUBSTITUTION

    def test_high_value_wrong_item_is_error_not_fraud(
        self, sku_reconciler: SKUReconciler
    ) -> None:
        mismatch = SKUMismatch(
            return_id="RET-399005",
            original_invoice_id="INV-100005",
            customer_id="CUST-1004",
            return_date="2024-02-15",
            original_sku="SKU-EPSL0001",
            returned_sku="SKU-GAMM0001",
            quantity=1,
            unit_price=Decimal("600.00"),
            reason_code="WRONG_ITEM",  # Low-risk reason code prevents FRAUD
        )
        result = sku_reconciler.classify_mismatch(mismatch)
        assert result == MismatchClassification.ERROR


class TestGenerateMismatchReport:
    def test_generate_mismatch_report_returns_dataframe(
        self, sku_reconciler: SKUReconciler, sample_returns: list[ReturnRecord]
    ) -> None:
        mismatches = sku_reconciler.detect_mismatches(sample_returns)
        df = sku_reconciler.generate_mismatch_report(mismatches)
        assert isinstance(df, pd.DataFrame)

    def test_mismatch_report_has_correct_columns(
        self, sku_reconciler: SKUReconciler, sample_returns: list[ReturnRecord]
    ) -> None:
        mismatches = sku_reconciler.detect_mismatches(sample_returns)
        df = sku_reconciler.generate_mismatch_report(mismatches)
        expected_columns = {
            "return_id",
            "original_invoice_id",
            "customer_id",
            "return_date",
            "original_sku",
            "returned_sku",
            "quantity",
            "unit_price",
            "line_value",
            "reason_code",
            "classification",
        }
        assert set(df.columns) == expected_columns

    def test_mismatch_report_row_count(
        self, sku_reconciler: SKUReconciler, sample_returns: list[ReturnRecord]
    ) -> None:
        mismatches = sku_reconciler.detect_mismatches(sample_returns)
        df = sku_reconciler.generate_mismatch_report(mismatches)
        assert len(df) == 2

    def test_mismatch_report_empty_input(self, sku_reconciler: SKUReconciler) -> None:
        df = sku_reconciler.generate_mismatch_report([])
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
