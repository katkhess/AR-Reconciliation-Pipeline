from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class InvoiceStatus(str, Enum):
    OPEN = "OPEN"
    PARTIAL = "PARTIAL"
    PAID = "PAID"
    OVERDUE = "OVERDUE"
    VOID = "VOID"


class LineItem(BaseModel):
    """A single line item on an invoice."""

    model_config = {"frozen": True}

    sku: str = Field(..., pattern=r"^SKU-[A-Z0-9]{4,}$", description="Stock keeping unit")
    description: Optional[str] = Field(None, description="Human-readable item description")
    quantity: int = Field(..., ge=1, description="Number of units")
    unit_price: Decimal = Field(..., ge=Decimal("0"), description="Price per unit")

    @field_validator("unit_price", mode="before")
    @classmethod
    def coerce_to_decimal(cls, v: object) -> Decimal:
        return Decimal(str(v))

    @property
    def line_total(self) -> Decimal:
        return self.quantity * self.unit_price


class Invoice(BaseModel):
    """An Accounts Receivable invoice record."""

    model_config = {"frozen": True}

    invoice_id: str = Field(..., pattern=r"^INV-[0-9]{6,}$", description="Unique invoice ID")
    customer_id: str = Field(..., pattern=r"^CUST-[0-9]{4,}$", description="Customer ID")
    order_date: str = Field(..., description="Order date (ISO 8601 date string)")
    due_date: Optional[str] = Field(None, description="Payment due date (ISO 8601)")
    line_items: list[LineItem] = Field(..., min_length=1, description="Invoice line items")
    total_amount: Decimal = Field(..., ge=Decimal("0"), description="Total invoice amount")
    amount_paid: Decimal = Field(
        default=Decimal("0.00"), ge=Decimal("0"), description="Amount paid so far"
    )
    currency: str = Field(default="USD", description="ISO 4217 currency code")
    status: InvoiceStatus = Field(..., description="Current invoice status")
    notes: Optional[str] = Field(None, description="Optional notes")

    @field_validator("total_amount", "amount_paid", mode="before")
    @classmethod
    def coerce_to_decimal(cls, v: object) -> Decimal:
        return Decimal(str(v))

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        valid = {"USD", "EUR", "GBP", "CAD", "AUD", "JPY"}
        if v not in valid:
            raise ValueError(f"Currency '{v}' not in allowed set {valid}")
        return v

    @model_validator(mode="after")
    def validate_amount_paid_not_exceed_total(self) -> "Invoice":
        if self.amount_paid > self.total_amount and self.status != InvoiceStatus.VOID:
            raise ValueError(
                f"amount_paid ({self.amount_paid}) cannot exceed total_amount "
                f"({self.total_amount}) for a non-void invoice"
            )
        return self

    @property
    def outstanding_balance(self) -> Decimal:
        """Amount remaining to be paid on this invoice."""
        return max(Decimal("0.00"), self.total_amount - self.amount_paid)

    @property
    def computed_total(self) -> Decimal:
        """Total derived from summing line items."""
        return sum((item.line_total for item in self.line_items), Decimal("0.00"))

    @property
    def is_overdue(self) -> bool:
        """True when status is OVERDUE and there is an outstanding balance."""
        return self.status == InvoiceStatus.OVERDUE and self.outstanding_balance > 0
