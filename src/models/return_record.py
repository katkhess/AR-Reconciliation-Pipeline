from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class ReturnStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    PROCESSED = "PROCESSED"
    REFUNDED = "REFUNDED"


class ReturnReasonCode(str, Enum):
    DEFECTIVE = "DEFECTIVE"
    WRONG_ITEM = "WRONG_ITEM"
    NOT_AS_DESCRIBED = "NOT_AS_DESCRIBED"
    DUPLICATE_ORDER = "DUPLICATE_ORDER"
    CUSTOMER_CHANGE_OF_MIND = "CUSTOMER_CHANGE_OF_MIND"
    DAMAGED_IN_TRANSIT = "DAMAGED_IN_TRANSIT"
    QUALITY_ISSUE = "QUALITY_ISSUE"
    OTHER = "OTHER"


class ReturnLineItem(BaseModel):
    """A single line item within a return, potentially revealing a SKU mismatch."""

    model_config = {"frozen": True}

    original_sku: str = Field(
        ..., pattern=r"^SKU-[A-Z0-9]{4,}$", description="SKU from the original invoice"
    )
    returned_sku: str = Field(
        ..., pattern=r"^SKU-[A-Z0-9]{4,}$", description="SKU of the item physically returned"
    )
    quantity: int = Field(..., ge=1, description="Number of units returned")
    unit_price: Decimal = Field(..., ge=Decimal("0"), description="Unit price at time of purchase")
    notes: Optional[str] = Field(None, description="Notes about this specific return line")

    @field_validator("unit_price", mode="before")
    @classmethod
    def coerce_to_decimal(cls, v: object) -> Decimal:
        return Decimal(str(v))

    @property
    def has_sku_mismatch(self) -> bool:
        """True when the returned SKU differs from the original invoice SKU."""
        return self.original_sku != self.returned_sku

    @property
    def line_total(self) -> Decimal:
        return self.quantity * self.unit_price


class ReturnRecord(BaseModel):
    """An Accounts Receivable return record, possibly containing SKU mismatches."""

    model_config = {"frozen": True}

    return_id: str = Field(..., pattern=r"^RET-[0-9]{6,}$", description="Unique return ID")
    original_invoice_id: str = Field(
        ..., pattern=r"^INV-[0-9]{6,}$", description="Original invoice being returned against"
    )
    customer_id: str = Field(..., pattern=r"^CUST-[0-9]{4,}$", description="Customer ID")
    return_date: str = Field(..., description="Return initiation date (ISO 8601)")
    line_items: list[ReturnLineItem] = Field(
        ..., min_length=1, description="Items being returned"
    )
    reason_code: ReturnReasonCode = Field(..., description="Reason for the return")
    status: ReturnStatus = Field(..., description="Current return status")
    credit_memo_id: Optional[str] = Field(None, description="Credit memo reference, if issued")
    notes: Optional[str] = Field(None, description="Optional notes")

    @property
    def has_sku_mismatch(self) -> bool:
        """True if any line item has a discrepancy between original and returned SKU."""
        return any(item.has_sku_mismatch for item in self.line_items)

    @property
    def mismatched_line_items(self) -> list[ReturnLineItem]:
        """Return only the line items where original_sku != returned_sku."""
        return [item for item in self.line_items if item.has_sku_mismatch]

    @property
    def total_return_value(self) -> Decimal:
        """Total monetary value of all returned items."""
        return sum((item.line_total for item in self.line_items), Decimal("0.00"))
