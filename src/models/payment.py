from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class PaymentMethod(str, Enum):
    ACH = "ACH"
    WIRE = "WIRE"
    CHECK = "CHECK"
    CREDIT_CARD = "CREDIT_CARD"
    DEBIT_CARD = "DEBIT_CARD"
    EFT = "EFT"
    CASH = "CASH"


class PaymentStatus(str, Enum):
    PENDING = "PENDING"
    CLEARED = "CLEARED"
    FAILED = "FAILED"
    REVERSED = "REVERSED"
    PARTIAL = "PARTIAL"


class PaymentAllocation(BaseModel):
    """Tracks how a payment is split across one or more invoices."""

    model_config = {"frozen": True}

    payment_id: str = Field(..., description="Reference to the payment")
    invoice_id: str = Field(..., description="Reference to the invoice receiving allocation")
    allocated_amount: Decimal = Field(..., ge=Decimal("0"), description="Amount allocated")
    allocation_date: str = Field(..., description="Date of allocation (ISO 8601)")
    notes: Optional[str] = Field(None, description="Notes about this allocation")

    @field_validator("allocated_amount", mode="before")
    @classmethod
    def coerce_to_decimal(cls, v: object) -> Decimal:
        return Decimal(str(v))


class Payment(BaseModel):
    """An Accounts Receivable payment record."""

    model_config = {"frozen": True}

    payment_id: str = Field(..., pattern=r"^PMT-[0-9]{6,}$", description="Unique payment ID")
    invoice_id: str = Field(
        ..., pattern=r"^INV-[0-9]{6,}$", description="Invoice being paid"
    )
    customer_id: str = Field(..., pattern=r"^CUST-[0-9]{4,}$", description="Customer ID")
    payment_date: str = Field(..., description="Date payment was received (ISO 8601)")
    amount_paid: Decimal = Field(..., gt=Decimal("0"), description="Payment amount")
    payment_method: PaymentMethod = Field(..., description="Payment method")
    reference_number: Optional[str] = Field(
        None, description="External reference (check number, wire ref, etc.)"
    )
    status: PaymentStatus = Field(..., description="Current payment status")
    notes: Optional[str] = Field(None, description="Optional notes")
    allocations: list[PaymentAllocation] = Field(
        default_factory=list,
        description="Allocation details for partial or multi-invoice payments",
    )

    @field_validator("amount_paid", mode="before")
    @classmethod
    def coerce_to_decimal(cls, v: object) -> Decimal:
        return Decimal(str(v))

    @property
    def total_allocated(self) -> Decimal:
        """Sum of all allocation amounts for this payment."""
        return sum((a.allocated_amount for a in self.allocations), Decimal("0.00"))

    @property
    def unallocated_amount(self) -> Decimal:
        """Portion of the payment not yet allocated to any invoice."""
        return max(Decimal("0.00"), self.amount_paid - self.total_allocated)

    @property
    def is_partial(self) -> bool:
        """True when this payment does not fully cover the invoiced amount
        (determined externally by comparing to the invoice total)."""
        return self.status == PaymentStatus.PARTIAL
