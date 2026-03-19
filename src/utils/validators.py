from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import jsonschema
import structlog

logger = structlog.get_logger(__name__)

# ISO 4217 subset of commonly used currency codes
_VALID_CURRENCY_CODES: frozenset[str] = frozenset(
    {
        "USD", "EUR", "GBP", "CAD", "AUD", "NZD", "JPY", "CHF",
        "CNY", "HKD", "SGD", "SEK", "NOK", "DKK", "MXN", "BRL",
        "INR", "ZAR", "RUB", "KRW",
    }
)


def validate_json_schema(data: dict[str, Any], schema_path: str | Path) -> None:
    """Validate *data* against the JSON Schema at *schema_path*.

    Args:
        data: Dictionary representation of the record to validate.
        schema_path: Filesystem path to the ``.json`` schema file.

    Raises:
        jsonschema.ValidationError: When *data* does not conform to the schema.
        FileNotFoundError: When *schema_path* does not exist.
    """
    path = Path(schema_path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")

    with path.open() as fh:
        schema: dict[str, Any] = json.load(fh)

    # Use Draft7Validator which matches our $schema declaration
    validator = jsonschema.Draft7Validator(schema)
    errors = sorted(validator.iter_errors(data), key=lambda e: list(e.path))

    if errors:
        first = errors[0]
        logger.warning(
            "json_schema_validation_error",
            path=str(first.absolute_path),
            message=first.message,
            total_errors=len(errors),
        )
        raise jsonschema.ValidationError(
            message=first.message,
            validator=first.validator,
            path=first.absolute_path,
            cause=first.cause,
            context=first.context,
            validator_value=first.validator_value,
            instance=first.instance,
            schema=first.schema,
            schema_path=first.absolute_schema_path,
        )


def validate_date_range(start: date, end: date) -> None:
    """Assert that *start* is not after *end*.

    Args:
        start: The start date.
        end: The end date.

    Raises:
        ValueError: When *start* is later than *end*.
    """
    if start > end:
        raise ValueError(
            f"Start date {start.isoformat()} must not be after end date {end.isoformat()}"
        )


def validate_positive_amount(amount: Decimal | float | int) -> None:
    """Assert that *amount* is strictly positive.

    Args:
        amount: A numeric financial amount.

    Raises:
        ValueError: When *amount* is zero or negative.
    """
    decimal_amount = Decimal(str(amount))
    if decimal_amount <= Decimal("0"):
        raise ValueError(f"Amount must be positive, got {decimal_amount}")


def validate_currency_code(code: str) -> None:
    """Assert that *code* is a recognised ISO 4217 currency code.

    Args:
        code: A three-letter currency code string (e.g. ``"USD"``).

    Raises:
        ValueError: When *code* is not in the supported currency set.
    """
    upper_code = code.upper()
    if upper_code not in _VALID_CURRENCY_CODES:
        raise ValueError(
            f"Currency code '{code}' is not recognised. "
            f"Supported codes: {sorted(_VALID_CURRENCY_CODES)}"
        )
