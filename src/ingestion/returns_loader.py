from __future__ import annotations

import ast
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from src.ingestion.base_loader import BaseLoader
from src.models.return_record import ReturnLineItem, ReturnReasonCode, ReturnRecord, ReturnStatus

logger = structlog.get_logger(__name__)


class ReturnsLoader(BaseLoader):
    """Loads and validates return records from CSV files or S3 objects."""

    def load_from_csv(self, file_path: str | Path) -> list[ReturnRecord]:
        """Load returns from a local CSV file."""
        path = Path(file_path)
        self._log.info("loading_returns_from_csv", path=str(path))
        df = pd.read_csv(path)
        return self._parse_dataframe(df)

    def load_from_s3(self, bucket: str, key: str) -> list[ReturnRecord]:
        """Load returns from an S3 object."""
        df = self._read_csv_from_s3(bucket, key)
        return self._parse_dataframe(df)

    def validate_schema(self, data: dict[str, Any]) -> bool:
        """Validate a single return record dict against the JSON schema."""
        from src.utils.validators import validate_json_schema

        validate_json_schema(data, self.schema_path)
        return True

    def _parse_dataframe(self, df: pd.DataFrame) -> list[ReturnRecord]:
        """Convert a DataFrame of raw return rows into ReturnRecord model instances."""
        returns: list[ReturnRecord] = []

        for idx, row in df.iterrows():
            try:
                record = self._row_to_return_dict(row)
                return_record = ReturnRecord(**record)
                returns.append(return_record)
            except Exception as exc:
                self._log.warning(
                    "return_parse_failed",
                    row_index=idx,
                    return_id=row.get("return_id", "unknown"),
                    error=str(exc),
                )

        self._log.info(
            "returns_loaded",
            total_rows=len(df),
            valid_returns=len(returns),
            skipped=len(df) - len(returns),
        )
        return returns

    @staticmethod
    def _row_to_return_dict(row: pd.Series) -> dict[str, Any]:  # type: ignore[type-arg]
        """Convert a raw CSV row to a dict suitable for ReturnRecord(**dict)."""
        line_items_raw = row.get("line_items", "[]")
        if isinstance(line_items_raw, str):
            try:
                line_items_data = json.loads(line_items_raw)
            except json.JSONDecodeError:
                line_items_data = ast.literal_eval(line_items_raw)
        else:
            line_items_data = line_items_raw if line_items_raw else []

        line_items = [
            ReturnLineItem(
                original_sku=item["original_sku"],
                returned_sku=item["returned_sku"],
                quantity=int(item["quantity"]),
                unit_price=Decimal(str(item["unit_price"])),
                notes=item.get("notes"),
            )
            for item in line_items_data
        ]

        return {
            "return_id": str(row["return_id"]),
            "original_invoice_id": str(row["original_invoice_id"]),
            "customer_id": str(row["customer_id"]),
            "return_date": str(row["return_date"]),
            "line_items": line_items,
            "reason_code": ReturnReasonCode(str(row["reason_code"])),
            "status": ReturnStatus(str(row["status"])),
            "credit_memo_id": (
                str(row["credit_memo_id"])
                if pd.notna(row.get("credit_memo_id"))
                else None
            ),
            "notes": str(row["notes"]) if pd.notna(row.get("notes")) else None,
        }
