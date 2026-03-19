from __future__ import annotations

import abc
import io
import json
from pathlib import Path
from typing import Any, TypeVar

import boto3
import pandas as pd
import structlog

from src.utils.validators import validate_json_schema

T = TypeVar("T")

logger = structlog.get_logger(__name__)


class BaseLoader(abc.ABC):
    """Abstract base class for all data loaders.

    Subclasses must implement :meth:`load_from_csv`, :meth:`load_from_s3`,
    and :meth:`validate_schema`.
    """

    def __init__(self, schema_path: str | Path, aws_region: str = "us-east-1") -> None:
        self.schema_path = Path(schema_path)
        self.aws_region = aws_region
        self._log = structlog.get_logger(self.__class__.__name__)

        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")

        with self.schema_path.open() as fh:
            self._schema: dict[str, Any] = json.load(fh)

    @abc.abstractmethod
    def load_from_csv(self, file_path: str | Path) -> list[Any]:
        """Load and parse records from a local CSV file.

        Args:
            file_path: Path to the CSV file.

        Returns:
            A list of parsed model instances.
        """

    @abc.abstractmethod
    def load_from_s3(self, bucket: str, key: str) -> list[Any]:
        """Load and parse records from an S3 object.

        Args:
            bucket: S3 bucket name.
            key: S3 object key (path within the bucket).

        Returns:
            A list of parsed model instances.
        """

    @abc.abstractmethod
    def validate_schema(self, data: dict[str, Any]) -> bool:
        """Validate a single record dict against the loader's JSON schema.

        Args:
            data: Dictionary representation of the record.

        Returns:
            True if valid; raises ``jsonschema.ValidationError`` otherwise.
        """

    def _read_csv_from_s3(self, bucket: str, key: str) -> pd.DataFrame:
        """Download an S3 object and return it as a DataFrame."""
        self._log.info("fetching_s3_object", bucket=bucket, key=key)
        s3_client = boto3.client("s3", region_name=self.aws_region)
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
        df = pd.read_csv(io.BytesIO(body))
        self._log.info("s3_object_loaded", bucket=bucket, key=key, rows=len(df))
        return df

    def _validate_dataframe_rows(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Validate every row in *df* against the JSON schema.

        Returns a list of valid row dicts; invalid rows are logged and skipped.
        """
        valid_rows: list[dict[str, Any]] = []
        for idx, row in df.iterrows():
            record = row.where(pd.notna(row), other=None).to_dict()
            try:
                validate_json_schema(record, self.schema_path)
                valid_rows.append(record)
            except Exception as exc:
                self._log.warning(
                    "schema_validation_failed",
                    row_index=idx,
                    error=str(exc),
                )
        return valid_rows
