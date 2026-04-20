from __future__ import annotations

import ast
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from src.ingestion.base_loader import BaseLoader
from src.models.invoice import Invoice, InvoiceStatus, LineItem

logger = structlog.get_logger(__name__)


class InvoiceLoader(BaseLoader):
    """Loads and validates invoice records from CSV files or S3 objects."""

    def load_from_csv(self, file_path: str | Path) -> list[Invoice]:
        """Load invoices from a local CSV file.

        Args:
            file_path: Path to the invoices CSV.

        Returns:
            Validated list of :class:`Invoice` instances.
        """
        path = Path(file_path)
        self._log.info("loading_invoices_from_csv", path=str(path))

        df = pd.read_csv(path)
        return self._parse_dataframe(df)

    def load_from_s3(self, bucket: str, key: str) -> list[Invoice]:
        """Load invoices from an S3 object.

        Args:
            bucket: S3 bucket name.
            key: Object key within the bucket.

        Returns:
            Validated list of :class:`Invoice` instances.
        """
        df = self._read_csv_from_s3(bucket, key)
        return self._parse_dataframe(df)

    def validate_schema(self, data: dict[str, Any]) -> bool:
        """Validate a single invoice record dict against the JSON schema."""
        from src.utils.validators import validate_json_schema

        validate_json_schema(data, self.schema_path)
        return True

    def _parse_dataframe(self, df: pd.DataFrame) -> list[Invoice]:
        """Convert a DataFrame of raw invoice rows into Invoice model instances."""
        invoices: list[Invoice] = []

        for idx, row in df.iterrows():
            try:
                record = self._row_to_invoice_dict(row)
                invoice = Invoice(**record)
                invoices.append(invoice)
            except Exception as exc:
                self._log.warning(
                    "invoice_parse_failed",
                    row_index=idx,
                    invoice_id=row.get("invoice_id", "unknown"),
                    error=str(exc),
                )

        self._log.info(
            "invoices_loaded",
            total_rows=len(df),
            valid_invoices=len(invoices),
            skipped=len(df) - len(invoices),
        )
        return invoices

    @staticmethod
    def _row_to_invoice_dict(row: pd.Series) -> dict[str, Any]:  # type: ignore[type-arg]
        """Convert a raw CSV row to a dict suitable for Invoice(**dict)."""
        line_items_raw = row.get("line_items", "[]")
        if isinstance(line_items_raw, str):
            try:
                line_items_data = json.loads(line_items_raw)
            except json.JSONDecodeError:
                line_items_data = ast.literal_eval(line_items_raw)
        else:
            line_items_data = line_items_raw if line_items_raw else []

        line_items = [
            LineItem(
                sku=item["sku"],
                description=item.get("description"),
                quantity=int(item["quantity"]),
                unit_price=Decimal(str(item["unit_price"])),
            )
            for item in line_items_data
        ]

        return {
            "invoice_id": str(row["invoice_id"]),
            "customer_id": str(row["customer_id"]),
            "order_date": str(row["order_date"]),
            "due_date": str(row["due_date"]) if pd.notna(row.get("due_date")) else None,
            "line_items": line_items,
            "total_amount": Decimal(str(row["total_amount"])),
            "amount_paid": Decimal(str(row["amount_paid"])) if pd.notna(row.get("amount_paid")) else Decimal("0.00"),
            "currency": str(row.get("currency", "USD")),
            "status": InvoiceStatus(str(row["status"])),
            "notes": str(row["notes"]) if pd.notna(row.get("notes")) else None,
        }
