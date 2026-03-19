-- AR Reconciliation Summary
-- Executive dashboard query joining invoices, payments, returns, and reconciliation results.
-- Produces a per-customer summary of AR health.
--
-- Target: Amazon Redshift

WITH customer_invoice_summary AS (
    SELECT
        customer_id,
        COUNT(*)                                            AS total_invoices,
        SUM(total_amount)                                   AS total_invoiced,
        SUM(amount_paid)                                    AS total_collected,
        SUM(total_amount) - SUM(amount_paid)                AS total_outstanding,
        SUM(CASE WHEN status = 'PAID'    THEN 1 ELSE 0 END) AS paid_invoices,
        SUM(CASE WHEN status = 'PARTIAL' THEN 1 ELSE 0 END) AS partial_invoices,
        SUM(CASE WHEN status = 'OPEN'    THEN 1 ELSE 0 END) AS open_invoices,
        SUM(CASE WHEN status = 'OVERDUE' THEN 1 ELSE 0 END) AS overdue_invoices,
        MIN(order_date)                                     AS first_invoice_date,
        MAX(order_date)                                     AS last_invoice_date
    FROM
        ar_invoices
    WHERE
        status <> 'VOID'
    GROUP BY
        customer_id
),

customer_payment_summary AS (
    SELECT
        customer_id,
        COUNT(*)                                                AS total_payments,
        SUM(amount_paid)                                        AS total_payments_amount,
        SUM(CASE WHEN status = 'CLEARED' THEN 1 ELSE 0 END)    AS cleared_payments,
        SUM(CASE WHEN status = 'PARTIAL' THEN 1 ELSE 0 END)    AS partial_payments,
        SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END)    AS failed_payments,
        MAX(payment_date)                                       AS last_payment_date
    FROM
        ar_payments
    GROUP BY
        customer_id
),

customer_returns_summary AS (
    SELECT
        customer_id,
        COUNT(*)                                                    AS total_returns,
        SUM(CASE WHEN sku_mismatch_flag = TRUE THEN 1 ELSE 0 END)  AS sku_mismatch_returns,
        SUM(CASE WHEN status = 'REFUNDED'      THEN 1 ELSE 0 END)  AS refunded_returns
    FROM
        ar_returns
    GROUP BY
        customer_id
),

customer_discrepancies AS (
    SELECT
        customer_id,
        COUNT(*)                                                            AS total_discrepancies,
        SUM(CASE WHEN discrepancy_type = 'SKU_MISMATCH'  THEN 1 ELSE 0 END) AS sku_mismatches,
        SUM(CASE WHEN discrepancy_type = 'OVERPAYMENT'   THEN 1 ELSE 0 END) AS overpayments,
        SUM(CASE WHEN discrepancy_type = 'UNDERPAYMENT'  THEN 1 ELSE 0 END) AS underpayments,
        SUM(CASE WHEN is_fraud_alert = TRUE               THEN 1 ELSE 0 END) AS fraud_alerts,
        SUM(COALESCE(discrepancy_amount, 0))                                AS total_discrepancy_amount
    FROM
        ar_reconciliation_results
    WHERE
        is_resolved = FALSE
    GROUP BY
        customer_id
)

SELECT
    ci.customer_id,
    ci.total_invoices,
    ci.total_invoiced,
    ci.total_collected,
    ci.total_outstanding,
    ci.paid_invoices,
    ci.partial_invoices,
    ci.open_invoices,
    ci.overdue_invoices,
    -- Payment metrics
    COALESCE(cp.total_payments, 0)          AS total_payments,
    COALESCE(cp.cleared_payments, 0)        AS cleared_payments,
    COALESCE(cp.partial_payments, 0)        AS partial_payments,
    COALESCE(cp.failed_payments, 0)         AS failed_payments,
    cp.last_payment_date,
    -- Returns metrics
    COALESCE(cr.total_returns, 0)           AS total_returns,
    COALESCE(cr.sku_mismatch_returns, 0)    AS sku_mismatch_returns,
    COALESCE(cr.refunded_returns, 0)        AS refunded_returns,
    -- Discrepancy metrics
    COALESCE(cd.total_discrepancies, 0)         AS open_discrepancies,
    COALESCE(cd.sku_mismatches, 0)              AS open_sku_mismatches,
    COALESCE(cd.overpayments, 0)                AS open_overpayments,
    COALESCE(cd.underpayments, 0)               AS open_underpayments,
    COALESCE(cd.fraud_alerts, 0)                AS fraud_alerts,
    COALESCE(cd.total_discrepancy_amount, 0)    AS at_risk_amount,
    -- Derived risk score: higher = more attention needed
    COALESCE(cd.fraud_alerts, 0) * 10
        + COALESCE(cd.sku_mismatches, 0) * 3
        + COALESCE(ci.overdue_invoices, 0) * 2
        + COALESCE(cd.underpayments, 0)                 AS risk_score,
    ci.first_invoice_date,
    ci.last_invoice_date
FROM
    customer_invoice_summary        ci
    LEFT JOIN customer_payment_summary  cp ON cp.customer_id = ci.customer_id
    LEFT JOIN customer_returns_summary  cr ON cr.customer_id = ci.customer_id
    LEFT JOIN customer_discrepancies    cd ON cd.customer_id = ci.customer_id
ORDER BY
    risk_score DESC,
    ci.total_outstanding DESC;
