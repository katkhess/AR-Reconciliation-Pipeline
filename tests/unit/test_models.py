"""Unit tests for Pydantic models."""
from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from src.models.invoice import Invoice, InvoiceStatus, LineItem
from src.models.payment import Payment, PaymentMethod, PaymentStatus
from src.models.return_record import ReturnLineItem, ReturnReasonCode, ReturnRecord, ReturnStatus


class TestInvoiceModel:
    def test_invoice_outstanding_balance_paid(self) -> None:
        invoice = Invoice(
            invoice_id="INV-100001",
            customer_id="CUST-1001",
            order_date="2024-01-05",
            line_items=[LineItem(sku="SKU-ALPHA001", quantity=4, unit_price=Decimal("25.00"))],
            total_amount=Decimal("100.00"),
            amount_paid=Decimal("100.00"),
            status=InvoiceStatus.PAID,
        )
        assert invoice.outstanding_balance == Decimal("0.00")

    def test_invoice_outstanding_balance_partial(self) -> None:
        invoice = Invoice(
            invoice_id="INV-100002",
            customer_id="CUST-1002",
            order_date="2024-01-08",
            line_items=[LineItem(sku="SKU-BETA0002", quantity=2, unit_price=Decimal("200.00"))],
            total_amount=Decimal("400.00"),
            amount_paid=Decimal("150.00"),
            status=InvoiceStatus.PARTIAL,
        )
        assert invoice.outstanding_balance == Decimal("250.00")

    def test_invoice_outstanding_balance_open(self) -> None:
        invoice = Invoice(
            invoice_id="INV-100003",
            customer_id="CUST-1003",
            order_date="2024-01-10",
            line_items=[LineItem(sku="SKU-GAMM0001", quantity=1, unit_price=Decimal("500.00"))],
            total_amount=Decimal("500.00"),
            amount_paid=Decimal("0.00"),
            status=InvoiceStatus.OPEN,
        )
        assert invoice.outstanding_balance == Decimal("500.00")

    def test_invoice_computed_total_matches_line_items(self) -> None:
        invoice = Invoice(
            invoice_id="INV-100004",
            customer_id="CUST-1001",
            order_date="2024-01-12",
            line_items=[
                LineItem(sku="SKU-DELT0001", quantity=10, unit_price=Decimal("10.00")),
                LineItem(sku="SKU-EPSL0001", quantity=2, unit_price=Decimal("50.00")),
            ],
            total_amount=Decimal("200.00"),
            status=InvoiceStatus.OPEN,
        )
        assert invoice.computed_total == Decimal("200.00")

    def test_invoice_requires_at_least_one_line_item(self) -> None:
        with pytest.raises(ValidationError):
            Invoice(
                invoice_id="INV-100099",
                customer_id="CUST-1099",
                order_date="2024-01-01",
                line_items=[],
                total_amount=Decimal("0.00"),
                status=InvoiceStatus.VOID,
            )

    def test_invoice_invalid_currency_raises(self) -> None:
        with pytest.raises(ValidationError):
            Invoice(
                invoice_id="INV-100099",
                customer_id="CUST-1099",
                order_date="2024-01-01",
                line_items=[LineItem(sku="SKU-ALPHA001", quantity=1, unit_price=Decimal("10.00"))],
                total_amount=Decimal("10.00"),
                currency="XYZ",
                status=InvoiceStatus.OPEN,
            )

    def test_invoice_status_enum_values(self) -> None:
        for status in InvoiceStatus:
            assert status.value in ("OPEN", "PARTIAL", "PAID", "OVERDUE", "VOID")


class TestReturnRecordModel:
    def test_return_record_has_sku_mismatch_true(self) -> None:
        return_record = ReturnRecord(
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
        )
        assert return_record.has_sku_mismatch is True

    def test_return_record_has_sku_mismatch_false(self) -> None:
        return_record = ReturnRecord(
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
        )
        assert return_record.has_sku_mismatch is False

    def test_return_record_mismatched_line_items_filter(self) -> None:
        return_record = ReturnRecord(
            return_id="RET-399010",
            original_invoice_id="INV-100001",
            customer_id="CUST-1001",
            return_date="2024-02-01",
            line_items=[
                ReturnLineItem(
                    original_sku="SKU-ALPHA001",
                    returned_sku="SKU-ALPHA001",
                    quantity=1,
                    unit_price=Decimal("25.00"),
                ),
                ReturnLineItem(
                    original_sku="SKU-BETA0002",
                    returned_sku="SKU-GAMM0001",
                    quantity=1,
                    unit_price=Decimal("150.00"),
                ),
            ],
            reason_code=ReturnReasonCode.OTHER,
            status=ReturnStatus.PENDING,
        )
        assert len(return_record.mismatched_line_items) == 1
        assert return_record.mismatched_line_items[0].original_sku == "SKU-BETA0002"

    def test_return_record_total_value(self) -> None:
        return_record = ReturnRecord(
            return_id="RET-399011",
            original_invoice_id="INV-100001",
            customer_id="CUST-1001",
            return_date="2024-02-01",
            line_items=[
                ReturnLineItem(
                    original_sku="SKU-ALPHA001",
                    returned_sku="SKU-ALPHA001",
                    quantity=3,
                    unit_price=Decimal("25.00"),
                )
            ],
            reason_code=ReturnReasonCode.DEFECTIVE,
            status=ReturnStatus.APPROVED,
        )
        assert return_record.total_return_value == Decimal("75.00")


class TestPaymentModel:
    def test_payment_validation(self) -> None:
        payment = Payment(
            payment_id="PMT-200001",
            invoice_id="INV-100001",
            customer_id="CUST-1001",
            payment_date="2024-01-20",
            amount_paid=Decimal("250.00"),
            payment_method=PaymentMethod.ACH,
            reference_number="ACH-REF-001",
            status=PaymentStatus.CLEARED,
        )
        assert payment.amount_paid == Decimal("250.00")
        assert payment.payment_method == PaymentMethod.ACH
        assert payment.status == PaymentStatus.CLEARED

    def test_payment_zero_amount_raises(self) -> None:
        with pytest.raises(ValidationError):
            Payment(
                payment_id="PMT-299001",
                invoice_id="INV-100001",
                customer_id="CUST-1001",
                payment_date="2024-01-20",
                amount_paid=Decimal("0.00"),
                payment_method=PaymentMethod.ACH,
                status=PaymentStatus.CLEARED,
            )

    def test_payment_negative_amount_raises(self) -> None:
        with pytest.raises(ValidationError):
            Payment(
                payment_id="PMT-299002",
                invoice_id="INV-100001",
                customer_id="CUST-1001",
                payment_date="2024-01-20",
                amount_paid=Decimal("-50.00"),
                payment_method=PaymentMethod.WIRE,
                status=PaymentStatus.PENDING,
            )

    def test_payment_unallocated_amount(self) -> None:
        payment = Payment(
            payment_id="PMT-200001",
            invoice_id="INV-100001",
            customer_id="CUST-1001",
            payment_date="2024-01-20",
            amount_paid=Decimal("500.00"),
            payment_method=PaymentMethod.WIRE,
            status=PaymentStatus.PARTIAL,
            allocations=[],
        )
        assert payment.unallocated_amount == Decimal("500.00")

    def test_payment_method_enum_values(self) -> None:
        expected = {"ACH", "WIRE", "CHECK", "CREDIT_CARD", "DEBIT_CARD", "EFT", "CASH"}
        actual = {m.value for m in PaymentMethod}
        assert actual == expected
