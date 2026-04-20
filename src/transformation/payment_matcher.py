from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import structlog

from src.models.invoice import Invoice, InvoiceStatus
from src.models.payment import Payment

logger = structlog.get_logger(__name__)


@dataclass
class PaymentMatch:
    """Records the association between a payment and an invoice."""

    payment_id: str
    invoice_id: str
    customer_id: str
    invoice_total: Decimal
    amount_paid: Decimal
    payment_date: str
    invoice_due_date: str | None

    @property
    def balance_after_payment(self) -> Decimal:
        return max(Decimal("0.00"), self.invoice_total - self.amount_paid)

    @property
    def is_fully_paid(self) -> bool:
        return self.amount_paid >= self.invoice_total


@dataclass
class Overpayment:
    """An invoice that received more than the invoiced amount."""

    payment_id: str
    invoice_id: str
    customer_id: str
    invoice_total: Decimal
    amount_paid: Decimal

    @property
    def overpayment_amount(self) -> Decimal:
        return self.amount_paid - self.invoice_total


@dataclass
class Underpayment:
    """An invoice that received less than the invoiced amount."""

    payment_id: str
    invoice_id: str
    customer_id: str
    invoice_total: Decimal
    amount_paid: Decimal

    @property
    def outstanding_balance(self) -> Decimal:
        return self.invoice_total - self.amount_paid


class PaymentMatcher:
    """Matches payments to invoices and detects payment discrepancies."""

    def __init__(self) -> None:
        self._log = structlog.get_logger(self.__class__.__name__)

    def match_payments_to_invoices(
        self,
        payments: list[Payment],
        invoices: list[Invoice],
    ) -> list[PaymentMatch]:
        """Associate each payment with its corresponding invoice.

        Payments that reference an invoice not present in *invoices* are logged
        as warnings and excluded from the results.

        Args:
            payments: List of payment records.
            invoices: List of invoice records.

        Returns:
            A list of :class:`PaymentMatch` objects.
        """
        invoice_map: dict[str, Invoice] = {inv.invoice_id: inv for inv in invoices}
        matches: list[PaymentMatch] = []

        for payment in payments:
            invoice = invoice_map.get(payment.invoice_id)
            if invoice is None:
                self._log.warning(
                    "payment_invoice_not_found",
                    payment_id=payment.payment_id,
                    invoice_id=payment.invoice_id,
                )
                continue

            match = PaymentMatch(
                payment_id=payment.payment_id,
                invoice_id=invoice.invoice_id,
                customer_id=invoice.customer_id,
                invoice_total=invoice.total_amount,
                amount_paid=payment.amount_paid,
                payment_date=payment.payment_date,
                invoice_due_date=invoice.due_date,
            )
            matches.append(match)

        self._log.info(
            "payments_matched",
            payments=len(payments),
            invoices=len(invoices),
            matches=len(matches),
        )
        return matches

    def detect_overpayments(self, matches: list[PaymentMatch]) -> list[Overpayment]:
        """Find matches where the payment exceeds the invoice total.

        Args:
            matches: List of payment-invoice matches.

        Returns:
            List of :class:`Overpayment` instances.
        """
        overpayments = [
            Overpayment(
                payment_id=m.payment_id,
                invoice_id=m.invoice_id,
                customer_id=m.customer_id,
                invoice_total=m.invoice_total,
                amount_paid=m.amount_paid,
            )
            for m in matches
            if m.amount_paid > m.invoice_total
        ]
        self._log.info("overpayments_detected", count=len(overpayments))
        return overpayments

    def detect_underpayments(self, matches: list[PaymentMatch]) -> list[Underpayment]:
        """Find matches where the payment is less than the invoice total.

        Args:
            matches: List of payment-invoice matches.

        Returns:
            List of :class:`Underpayment` instances.
        """
        underpayments = [
            Underpayment(
                payment_id=m.payment_id,
                invoice_id=m.invoice_id,
                customer_id=m.customer_id,
                invoice_total=m.invoice_total,
                amount_paid=m.amount_paid,
            )
            for m in matches
            if m.amount_paid < m.invoice_total
        ]
        self._log.info("underpayments_detected", count=len(underpayments))
        return underpayments

    def generate_aging_report(
        self,
        invoices: list[Invoice],
        matches: list[PaymentMatch],
        as_of_date: date | None = None,
    ) -> pd.DataFrame:
        """Build an AR aging report bucketing outstanding balances by days past due.

        Aging buckets:
        - Current    : not yet due or fully paid
        - 1-30 days  : 1 to 30 days past due
        - 31-60 days : 31 to 60 days past due
        - 61-90 days : 61 to 90 days past due
        - 90+ days   : more than 90 days past due

        Args:
            invoices: All invoice records.
            matches: Payment matches (used to determine amount already paid).
            as_of_date: Reference date for aging calculation (defaults to today).

        Returns:
            A pandas DataFrame with aging information per invoice.
        """
        if as_of_date is None:
            as_of_date = date.today()

        paid_by_invoice: dict[str, Decimal] = {}
        for m in matches:
            paid_by_invoice[m.invoice_id] = (
                paid_by_invoice.get(m.invoice_id, Decimal("0.00")) + m.amount_paid
            )

        rows = []
        for inv in invoices:
            if inv.status == InvoiceStatus.VOID:
                continue

            total_paid = paid_by_invoice.get(inv.invoice_id, inv.amount_paid)
            outstanding = max(Decimal("0.00"), inv.total_amount - total_paid)

            if outstanding == Decimal("0.00"):
                aging_bucket = "Current"
                days_past_due = 0
            elif inv.due_date is None:
                aging_bucket = "Current"
                days_past_due = 0
            else:
                due = datetime.strptime(inv.due_date, "%Y-%m-%d").date()
                days_past_due = (as_of_date - due).days

                if days_past_due <= 0:
                    aging_bucket = "Current"
                elif days_past_due <= 30:
                    aging_bucket = "1-30"
                elif days_past_due <= 60:
                    aging_bucket = "31-60"
                elif days_past_due <= 90:
                    aging_bucket = "61-90"
                else:
                    aging_bucket = "90+"

            rows.append(
                {
                    "invoice_id": inv.invoice_id,
                    "customer_id": inv.customer_id,
                    "invoice_total": float(inv.total_amount),
                    "amount_paid": float(total_paid),
                    "outstanding_balance": float(outstanding),
                    "due_date": inv.due_date,
                    "days_past_due": max(0, days_past_due),
                    "aging_bucket": aging_bucket,
                    "status": inv.status.value,
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            bucket_order = pd.CategoricalDtype(
                ["Current", "1-30", "31-60", "61-90", "90+"], ordered=True
            )
            df["aging_bucket"] = df["aging_bucket"].astype(bucket_order)
            df.sort_values(["aging_bucket", "customer_id"], inplace=True)
            df.reset_index(drop=True, inplace=True)

        self._log.info(
            "aging_report_generated",
            invoice_count=len(invoices),
            report_rows=len(df),
            as_of_date=str(as_of_date),
        )
        return df
