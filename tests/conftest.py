"""Shared pytest fixtures for the AR Reconciliation Pipeline test suite."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.models.invoice import Invoice, InvoiceStatus, LineItem
from src.models.payment import Payment, PaymentMethod, PaymentStatus
from src.models.return_record import ReturnLineItem, ReturnReasonCode, ReturnRecord, ReturnStatus
from src.transformation.discrepancy_detector import DiscrepancyDetector
from src.transformation.payment_matcher import PaymentMatcher
from src.transformation.sku_reconciler import SKUReconciler


# ---------------------------------------------------------------------------
# Invoice fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_invoices() -> list[Invoice]:
    """Five invoices covering all common statuses."""
    return [
        Invoice(
            invoice_id="INV-100001",
            customer_id="CUST-1001",
            order_date="2024-01-05",
            due_date="2024-02-04",
            line_items=[LineItem(sku="SKU-ALPHA001", quantity=10, unit_price=Decimal("25.00"))],
            total_amount=Decimal("250.00"),
            amount_paid=Decimal("250.00"),
            currency="USD",
            status=InvoiceStatus.PAID,
        ),
        Invoice(
            invoice_id="INV-100002",
            customer_id="CUST-1002",
            order_date="2024-01-08",
            due_date="2024-02-07",
            line_items=[
                LineItem(sku="SKU-BETA0002", quantity=5, unit_price=Decimal("150.00")),
                LineItem(sku="SKU-BETA0003", quantity=2, unit_price=Decimal("75.00")),
            ],
            total_amount=Decimal("900.00"),
            amount_paid=Decimal("450.00"),
            currency="USD",
            status=InvoiceStatus.PARTIAL,
        ),
        Invoice(
            invoice_id="INV-100003",
            customer_id="CUST-1003",
            order_date="2024-01-10",
            due_date="2024-02-09",
            line_items=[LineItem(sku="SKU-GAMM0001", quantity=3, unit_price=Decimal("500.00"))],
            total_amount=Decimal("1500.00"),
            amount_paid=Decimal("0.00"),
            currency="USD",
            status=InvoiceStatus.OVERDUE,
        ),
        Invoice(
            invoice_id="INV-100004",
            customer_id="CUST-1001",
            order_date="2024-01-12",
            due_date="2024-02-11",
            line_items=[LineItem(sku="SKU-DELT0001", quantity=20, unit_price=Decimal("12.50"))],
            total_amount=Decimal("250.00"),
            amount_paid=Decimal("0.00"),
            currency="USD",
            status=InvoiceStatus.OPEN,
        ),
        Invoice(
            invoice_id="INV-100005",
            customer_id="CUST-1004",
            order_date="2024-01-15",
            due_date="2024-02-14",
            line_items=[LineItem(sku="SKU-EPSL0001", quantity=1, unit_price=Decimal("2500.00"))],
            total_amount=Decimal("2500.00"),
            amount_paid=Decimal("2500.00"),
            currency="USD",
            status=InvoiceStatus.PAID,
        ),
    ]


# ---------------------------------------------------------------------------
# Payment fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_payments() -> list[Payment]:
    """Eight payments: full, partial, overpayment, and underpayment examples."""
    return [
        Payment(
            payment_id="PMT-200001",
            invoice_id="INV-100001",
            customer_id="CUST-1001",
            payment_date="2024-01-20",
            amount_paid=Decimal("250.00"),
            payment_method=PaymentMethod.ACH,
            reference_number="ACH-REF-001",
            status=PaymentStatus.CLEARED,
        ),
        # Partial payment for INV-100002
        Payment(
            payment_id="PMT-200002",
            invoice_id="INV-100002",
            customer_id="CUST-1002",
            payment_date="2024-01-25",
            amount_paid=Decimal("450.00"),
            payment_method=PaymentMethod.WIRE,
            reference_number="WIRE-REF-002",
            status=PaymentStatus.PARTIAL,
        ),
        Payment(
            payment_id="PMT-200003",
            invoice_id="INV-100005",
            customer_id="CUST-1004",
            payment_date="2024-01-28",
            amount_paid=Decimal("2500.00"),
            payment_method=PaymentMethod.WIRE,
            reference_number="WIRE-REF-003",
            status=PaymentStatus.CLEARED,
        ),
        # Underpayment for INV-100003 (overdue)
        Payment(
            payment_id="PMT-200008",
            invoice_id="INV-100003",
            customer_id="CUST-1003",
            payment_date="2024-02-12",
            amount_paid=Decimal("750.00"),
            payment_method=PaymentMethod.ACH,
            reference_number="ACH-REF-008",
            status=PaymentStatus.PARTIAL,
        ),
        # Second partial for INV-100002 - results in overpayment (450+500=950 > 900)
        Payment(
            payment_id="PMT-200007",
            invoice_id="INV-100002",
            customer_id="CUST-1002",
            payment_date="2024-02-10",
            amount_paid=Decimal("500.00"),
            payment_method=PaymentMethod.WIRE,
            reference_number="WIRE-REF-007",
            status=PaymentStatus.CLEARED,
        ),
        # Clear overpayment: $300 paid against INV-100001 which only totals $250
        Payment(
            payment_id="PMT-200014",
            invoice_id="INV-100001",
            customer_id="CUST-1001",
            payment_date="2024-02-22",
            amount_paid=Decimal("300.00"),
            payment_method=PaymentMethod.ACH,
            reference_number="ACH-REF-014",
            status=PaymentStatus.CLEARED,
        ),
        Payment(
            payment_id="PMT-200010",
            invoice_id="INV-100004",
            customer_id="CUST-1001",
            payment_date="2024-02-15",
            amount_paid=Decimal("200.00"),
            payment_method=PaymentMethod.EFT,
            reference_number="EFT-REF-010",
            status=PaymentStatus.PARTIAL,
        ),
        Payment(
            payment_id="PMT-200013",
            invoice_id="INV-100003",
            customer_id="CUST-1003",
            payment_date="2024-02-20",
            amount_paid=Decimal("200.00"),
            payment_method=PaymentMethod.CASH,
            status=PaymentStatus.PARTIAL,
        ),
    ]


# ---------------------------------------------------------------------------
# Return fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_returns() -> list[ReturnRecord]:
    """Four returns: two clean, two with SKU mismatches."""
    return [
        # Clean return
        ReturnRecord(
            return_id="RET-300001",
            original_invoice_id="INV-100001",
            customer_id="CUST-1001",
            return_date="2024-02-01",
            line_items=[
                ReturnLineItem(
                    original_sku="SKU-ALPHA001",
                    returned_sku="SKU-ALPHA001",
                    quantity=2,
                    unit_price=Decimal("25.00"),
                )
            ],
            reason_code=ReturnReasonCode.DEFECTIVE,
            status=ReturnStatus.REFUNDED,
        ),
        # SKU mismatch - low value (ERROR classification)
        ReturnRecord(
            return_id="RET-300002",
            original_invoice_id="INV-100002",
            customer_id="CUST-1002",
            return_date="2024-02-05",
            line_items=[
                ReturnLineItem(
                    original_sku="SKU-BETA0002",
                    returned_sku="SKU-GAMM0001",
                    quantity=1,
                    unit_price=Decimal("150.00"),
                )
            ],
            reason_code=ReturnReasonCode.WRONG_ITEM,
            status=ReturnStatus.APPROVED,
        ),
        # Clean high-value return
        ReturnRecord(
            return_id="RET-300003",
            original_invoice_id="INV-100005",
            customer_id="CUST-1004",
            return_date="2024-02-08",
            line_items=[
                ReturnLineItem(
                    original_sku="SKU-EPSL0001",
                    returned_sku="SKU-EPSL0001",
                    quantity=1,
                    unit_price=Decimal("2500.00"),
                )
            ],
            reason_code=ReturnReasonCode.NOT_AS_DESCRIBED,
            status=ReturnStatus.PROCESSED,
        ),
        # High-value SKU mismatch - FRAUD classification candidate
        ReturnRecord(
            return_id="RET-300008",
            original_invoice_id="INV-100005",
            customer_id="CUST-1004",
            return_date="2024-02-20",
            line_items=[
                ReturnLineItem(
                    original_sku="SKU-GAMM0001",
                    returned_sku="SKU-DELT0001",
                    quantity=1,
                    unit_price=Decimal("500.00"),
                )
            ],
            reason_code=ReturnReasonCode.OTHER,
            status=ReturnStatus.PENDING,
        ),
    ]


# ---------------------------------------------------------------------------
# Transformation fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sku_reconciler() -> SKUReconciler:
    return SKUReconciler()


@pytest.fixture()
def payment_matcher() -> PaymentMatcher:
    return PaymentMatcher()


@pytest.fixture()
def discrepancy_detector() -> DiscrepancyDetector:
    return DiscrepancyDetector()
