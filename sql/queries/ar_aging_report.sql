-- AR Aging Report
-- Buckets outstanding invoice balances by days past due date.
-- Standard AR aging buckets: Current, 1-30, 31-60, 61-90, 90+
--
-- Pass :as_of_date parameter (or replace with CURRENT_DATE for live reports)
-- Target: Amazon Redshift

WITH payment_totals AS (
    SELECT
        invoice_id,
        SUM(amount_paid) AS total_paid
    FROM
        ar_payments
    WHERE
        status IN ('CLEARED', 'PARTIAL')
    GROUP BY
        invoice_id
),

invoice_balances AS (
    SELECT
        i.invoice_id,
        i.customer_id,
        i.order_date,
        i.due_date,
        i.total_amount,
        i.currency,
        i.status,
        COALESCE(p.total_paid, 0.00)                          AS total_paid,
        i.total_amount - COALESCE(p.total_paid, 0.00)         AS outstanding_balance,
        -- Days past due (negative = not yet due)
        CASE
            WHEN i.due_date IS NULL THEN 0
            ELSE DATEDIFF(DAY, i.due_date, CURRENT_DATE)
        END AS days_past_due
    FROM
        ar_invoices         i
        LEFT JOIN payment_totals p ON p.invoice_id = i.invoice_id
    WHERE
        i.status NOT IN ('VOID', 'PAID')
        AND i.total_amount - COALESCE(p.total_paid, 0.00) > 0
),

aged_invoices AS (
    SELECT
        *,
        CASE
            WHEN days_past_due <= 0   THEN 'Current'
            WHEN days_past_due <= 30  THEN '1-30 Days'
            WHEN days_past_due <= 60  THEN '31-60 Days'
            WHEN days_past_due <= 90  THEN '61-90 Days'
            ELSE                           '90+ Days'
        END AS aging_bucket,
        -- Numeric sort key for ordering buckets correctly
        CASE
            WHEN days_past_due <= 0   THEN 0
            WHEN days_past_due <= 30  THEN 1
            WHEN days_past_due <= 60  THEN 2
            WHEN days_past_due <= 90  THEN 3
            ELSE                           4
        END AS aging_sort
    FROM
        invoice_balances
)

-- Detail view
SELECT
    customer_id,
    invoice_id,
    order_date,
    due_date,
    currency,
    status,
    total_amount,
    total_paid,
    outstanding_balance,
    days_past_due,
    aging_bucket
FROM
    aged_invoices
ORDER BY
    aging_sort,
    customer_id,
    outstanding_balance DESC;

-- Uncomment the query below for the aggregated bucket summary (executive view):
/*
SELECT
    aging_bucket,
    COUNT(*)                    AS invoice_count,
    COUNT(DISTINCT customer_id) AS customer_count,
    SUM(outstanding_balance)    AS total_outstanding,
    AVG(outstanding_balance)    AS avg_outstanding
FROM
    aged_invoices
GROUP BY
    aging_bucket,
    aging_sort
ORDER BY
    aging_sort;
*/
