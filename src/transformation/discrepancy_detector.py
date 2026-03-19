from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

import pandas as pd
import structlog

from src.models.invoice import Invoice
from src.models.payment import Payment
from src.models.return_record import ReturnRecord
from src.transformation.payment_matcher import (
    Overpayment,
    PaymentMatch,
    PaymentMatcher,
    Underpayment,
)
from src.transformation.sku_reconciler import MismatchClassification, SKUMismatch, SKUReconciler

logger = structlog.get_logger(__name__)


@dataclass
class DiscrepancyReport:
    """Consolidated report of all discrepancies found in the reconciliation pipeline."""

    sku_mismatches: list[SKUMismatch] = field(default_factory=list)
    payment_matches: list[PaymentMatch] = field(default_factory=list)
    overpayments: list[Overpayment] = field(default_factory=list)
    underpayments: list[Underpayment] = field(default_factory=list)

    @property
    def total_sku_mismatches(self) -> int:
        return len(self.sku_mismatches)

    @property
    def total_overpayments(self) -> int:
        return len(self.overpayments)

    @property
    def total_underpayments(self) -> int:
        return len(self.underpayments)

    @property
    def fraud_alerts(self) -> list[SKUMismatch]:
        return [m for m in self.sku_mismatches if m.classification == MismatchClassification.FRAUD]

    @property
    def total_overpayment_amount(self) -> Decimal:
        return sum((o.overpayment_amount for o in self.overpayments), Decimal("0.00"))

    @property
    def total_outstanding_amount(self) -> Decimal:
        return sum((u.outstanding_balance for u in self.underpayments), Decimal("0.00"))

    @property
    def has_discrepancies(self) -> bool:
        return bool(self.sku_mismatches or self.overpayments or self.underpayments)


class DiscrepancyDetector:
    """Orchestrates all discrepancy detection across invoices, payments, and returns."""

    def __init__(
        self,
        sku_reconciler: SKUReconciler | None = None,
        payment_matcher: PaymentMatcher | None = None,
    ) -> None:
        self._sku_reconciler = sku_reconciler or SKUReconciler()
        self._payment_matcher = payment_matcher or PaymentMatcher()
        self._log = structlog.get_logger(self.__class__.__name__)

    def detect_all(
        self,
        invoices: list[Invoice],
        payments: list[Payment],
        returns: list[ReturnRecord],
    ) -> DiscrepancyReport:
        """Run all detection routines and return a consolidated report.

        Args:
            invoices: All invoice records.
            payments: All payment records.
            returns: All return records.

        Returns:
            A :class:`DiscrepancyReport` containing every discrepancy found.
        """
        self._log.info(
            "discrepancy_detection_started",
            invoices=len(invoices),
            payments=len(payments),
            returns=len(returns),
        )

        raw_mismatches = self._sku_reconciler.detect_mismatches(returns)
        for mismatch in raw_mismatches:
            mismatch.classification = self._sku_reconciler.classify_mismatch(mismatch)

        matches = self._payment_matcher.match_payments_to_invoices(payments, invoices)
        overpayments = self._payment_matcher.detect_overpayments(matches)
        underpayments = self._payment_matcher.detect_underpayments(matches)

        report = DiscrepancyReport(
            sku_mismatches=raw_mismatches,
            payment_matches=matches,
            overpayments=overpayments,
            underpayments=underpayments,
        )

        self._log.info(
            "discrepancy_detection_complete",
            sku_mismatches=report.total_sku_mismatches,
            overpayments=report.total_overpayments,
            underpayments=report.total_underpayments,
            fraud_alerts=len(report.fraud_alerts),
        )
        return report

    def flag_high_risk_accounts(self, report: DiscrepancyReport) -> list[str]:
        """Identify customer IDs that appear across multiple discrepancy types.

        A customer is flagged as high-risk when they appear in at least two of:
        - SKU mismatch records
        - Overpayment records
        - Underpayment records

        Args:
            report: A previously generated :class:`DiscrepancyReport`.

        Returns:
            Sorted list of high-risk customer IDs.
        """
        from collections import defaultdict

        customer_hit_types: dict[str, set[str]] = defaultdict(set)

        for m in report.sku_mismatches:
            customer_hit_types[m.customer_id].add("sku_mismatch")

        for o in report.overpayments:
            customer_hit_types[o.customer_id].add("overpayment")

        for u in report.underpayments:
            customer_hit_types[u.customer_id].add("underpayment")

        high_risk = sorted(
            customer_id
            for customer_id, hit_types in customer_hit_types.items()
            if len(hit_types) >= 2
        )

        self._log.info("high_risk_accounts_flagged", count=len(high_risk))
        return high_risk

    def to_dataframe(self, report: DiscrepancyReport) -> pd.DataFrame:
        """Flatten a :class:`DiscrepancyReport` into a single audit-ready DataFrame.

        Args:
            report: The report to flatten.

        Returns:
            A pandas DataFrame with one row per discrepancy event.
        """
        rows: list[dict] = []

        for m in report.sku_mismatches:
            rows.append(
                {
                    "discrepancy_type": "SKU_MISMATCH",
                    "entity_id": m.return_id,
                    "customer_id": m.customer_id,
                    "reference_id": m.original_invoice_id,
                    "amount": float(m.unit_price * m.quantity),
                    "detail": f"{m.original_sku} -> {m.returned_sku}",
                    "classification": m.classification.value,
                }
            )

        for o in report.overpayments:
            rows.append(
                {
                    "discrepancy_type": "OVERPAYMENT",
                    "entity_id": o.payment_id,
                    "customer_id": o.customer_id,
                    "reference_id": o.invoice_id,
                    "amount": float(o.overpayment_amount),
                    "detail": (
                        f"paid {float(o.amount_paid):.2f} vs "
                        f"invoiced {float(o.invoice_total):.2f}"
                    ),
                    "classification": "OVERPAYMENT",
                }
            )

        for u in report.underpayments:
            rows.append(
                {
                    "discrepancy_type": "UNDERPAYMENT",
                    "entity_id": u.payment_id,
                    "customer_id": u.customer_id,
                    "reference_id": u.invoice_id,
                    "amount": float(u.outstanding_balance),
                    "detail": (
                        f"paid {float(u.amount_paid):.2f} vs "
                        f"invoiced {float(u.invoice_total):.2f}"
                    ),
                    "classification": "UNDERPAYMENT",
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            df.sort_values(["discrepancy_type", "customer_id"], inplace=True)
            df.reset_index(drop=True, inplace=True)

        return df
