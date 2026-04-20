from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from src.ingestion.base_loader import BaseLoader
from src.models.payment import Payment, PaymentMethod, PaymentStatus

logger = structlog.get_logger(__name__)


class PaymentLoader(BaseLoader):
    """Loads and validates payment records from CSV files or S3 objects."""

    def load_from_csv(self, file_path: str | Path) -> list[Payment]:
        """Load payments from a local CSV file."""
        path = Path(file_path)
        self._log.info("loading_payments_from_csv", path=str(path))
        df = pd.read_csv(path)
        return self._parse_dataframe(df)

    def load_from_s3(self, bucket: str, key: str) -> list[Payment]:
        """Load payments from an S3 object."""
        df = self._read_csv_from_s3(bucket, key)
        return self._parse_dataframe(df)

    def validate_schema(self, data: dict[str, Any]) -> bool:
        """Validate a single payment record dict against the JSON schema."""
        from src.utils.validators import validate_json_schema

        validate_json_schema(data, self.schema_path)
        return True

    def _parse_dataframe(self, df: pd.DataFrame) -> list[Payment]:
        """Convert a DataFrame of raw payment rows into Payment model instances."""
        payments: list[Payment] = []

        for idx, row in df.iterrows():
            try:
                record = self._row_to_payment_dict(row)
                payment = Payment(**record)
                payments.append(payment)
            except Exception as exc:
                self._log.warning(
                    "payment_parse_failed",
                    row_index=idx,
                    payment_id=row.get("payment_id", "unknown"),
                    error=str(exc),
                )

        self._log.info(
            "payments_loaded",
            total_rows=len(df),
            valid_payments=len(payments),
            skipped=len(df) - len(payments),
        )
        return payments

    @staticmethod
    def _row_to_payment_dict(row: pd.Series) -> dict[str, Any]:  # type: ignore[type-arg]
        """Convert a raw CSV row to a dict suitable for Payment(**dict)."""
        return {
            "payment_id": str(row["payment_id"]),
            "invoice_id": str(row["invoice_id"]),
            "customer_id": str(row["customer_id"]),
            "payment_date": str(row["payment_date"]),
            "amount_paid": Decimal(str(row["amount_paid"])),
            "payment_method": PaymentMethod(str(row["payment_method"])),
            "reference_number": (
                str(row["reference_number"])
                if pd.notna(row.get("reference_number"))
                else None
            ),
            "status": PaymentStatus(str(row["status"])),
            "notes": str(row["notes"]) if pd.notna(row.get("notes")) else None,
            "allocations": [],
        }
