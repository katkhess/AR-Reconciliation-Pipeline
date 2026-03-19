from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

import pandas as pd
import structlog

from src.models.return_record import ReturnRecord

logger = structlog.get_logger(__name__)


class MismatchClassification(str, Enum):
    """How a SKU mismatch is classified after analysis."""

    SUBSTITUTION = "SUBSTITUTION"
    """A known or acceptable product substitution."""

    ERROR = "ERROR"
    """An operational error (e.g., pick-and-pack mistake)."""

    FRAUD = "FRAUD"
    """A potentially fraudulent return (high-value swap or pattern-based indicator)."""


@dataclass
class SKUMismatch:
    """Captures a single SKU discrepancy found in a return."""

    return_id: str
    original_invoice_id: str
    customer_id: str
    return_date: str
    original_sku: str
    returned_sku: str
    quantity: int
    unit_price: Decimal
    reason_code: str
    classification: MismatchClassification = MismatchClassification.ERROR


class SKUReconciler:
    """Identifies and classifies SKU mismatches in return records."""

    # SKU prefix pairs considered acceptable substitutions (original -> returned)
    SUBSTITUTION_PREFIX_MAP: dict[str, str] = {
        "SKU-PRMA": "SKU-PRMB",
        "SKU-PRMB": "SKU-PRMA",
    }

    # Threshold above which a mismatch is elevated to potential fraud
    FRAUD_VALUE_THRESHOLD: Decimal = Decimal("500.00")

    def __init__(self) -> None:
        self._log = structlog.get_logger(self.__class__.__name__)

    def detect_mismatches(self, returns: list[ReturnRecord]) -> list[SKUMismatch]:
        """Find all return line items where returned_sku != original_sku.

        Args:
            returns: List of return records to inspect.

        Returns:
            A list of :class:`SKUMismatch` instances, one per mismatched line item.
        """
        mismatches: list[SKUMismatch] = []

        for record in returns:
            for item in record.line_items:
                if item.has_sku_mismatch:
                    mismatch = SKUMismatch(
                        return_id=record.return_id,
                        original_invoice_id=record.original_invoice_id,
                        customer_id=record.customer_id,
                        return_date=record.return_date,
                        original_sku=item.original_sku,
                        returned_sku=item.returned_sku,
                        quantity=item.quantity,
                        unit_price=item.unit_price,
                        reason_code=record.reason_code.value,
                    )
                    mismatches.append(mismatch)

        self._log.info(
            "sku_mismatches_detected",
            total_returns=len(returns),
            mismatch_count=len(mismatches),
        )
        return mismatches

    def classify_mismatch(self, mismatch: SKUMismatch) -> MismatchClassification:
        """Classify a single SKU mismatch.

        Rules (applied in priority order):
        1. FRAUD   — item value ≥ threshold AND reason code is not WRONG_ITEM/DEFECTIVE.
        2. SUBSTITUTION — the original/returned SKU pair matches a known substitution map.
        3. ERROR   — all other cases.

        Args:
            mismatch: The mismatch to classify.

        Returns:
            A :class:`MismatchClassification` value.
        """
        line_value = mismatch.unit_price * mismatch.quantity
        low_risk_reason_codes = {"WRONG_ITEM", "DEFECTIVE", "DAMAGED_IN_TRANSIT"}

        # Fraud signal: high-value swap with a suspicious reason code
        if (
            line_value >= self.FRAUD_VALUE_THRESHOLD
            and mismatch.reason_code not in low_risk_reason_codes
        ):
            self._log.warning(
                "potential_fraud_detected",
                return_id=mismatch.return_id,
                customer_id=mismatch.customer_id,
                line_value=str(line_value),
            )
            return MismatchClassification.FRAUD

        # Known substitution pair
        expected_substitute = self.SUBSTITUTION_PREFIX_MAP.get(mismatch.original_sku[:8])
        if expected_substitute and mismatch.returned_sku.startswith(expected_substitute):
            return MismatchClassification.SUBSTITUTION

        return MismatchClassification.ERROR

    def generate_mismatch_report(self, mismatches: list[SKUMismatch]) -> pd.DataFrame:
        """Build a tidy DataFrame summarising all detected SKU mismatches.

        Args:
            mismatches: List of mismatches (already classified).

        Returns:
            A pandas DataFrame with one row per mismatch.
        """
        if not mismatches:
            return pd.DataFrame(
                columns=[
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
                ]
            )

        rows = [
            {
                "return_id": m.return_id,
                "original_invoice_id": m.original_invoice_id,
                "customer_id": m.customer_id,
                "return_date": m.return_date,
                "original_sku": m.original_sku,
                "returned_sku": m.returned_sku,
                "quantity": m.quantity,
                "unit_price": float(m.unit_price),
                "line_value": float(m.unit_price * m.quantity),
                "reason_code": m.reason_code,
                "classification": m.classification.value,
            }
            for m in mismatches
        ]

        df = pd.DataFrame(rows)
        df["return_date"] = pd.to_datetime(df["return_date"])
        df.sort_values("return_date", inplace=True)
        df.reset_index(drop=True, inplace=True)

        self._log.info(
            "mismatch_report_generated",
            rows=len(df),
            fraud_count=int((df["classification"] == MismatchClassification.FRAUD.value).sum()),
            error_count=int((df["classification"] == MismatchClassification.ERROR.value).sum()),
            substitution_count=int(
                (df["classification"] == MismatchClassification.SUBSTITUTION.value).sum()
            ),
        )
        return df
