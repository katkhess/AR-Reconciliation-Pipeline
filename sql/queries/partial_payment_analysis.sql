-- Partial Payment Analysis
-- Uses CTEs to compute how much has been paid against each invoice,
-- the outstanding balance, and whether the invoice is partially paid.
--
-- Target: Amazon Redshift

WITH invoice_totals AS (
    -- Base invoice information
    SELECT
        invoice_id,
        customer_id,
        order_date,
        due_date,
        total_amount,
        currency,
        status
    FROM
        ar_invoices
    WHERE
        status NOT IN ('VOID')
),

payment_sums AS (
    -- Aggregate all cleared/partial payments per invoice
    SELECT
        invoice_id,
        SUM(amount_paid)    AS total_amount_paid,
        COUNT(*)            AS payment_count,
        MIN(payment_date)   AS first_payment_date,
        MAX(payment_date)   AS last_payment_date
    FROM
        ar_payments
    WHERE
        status IN ('CLEARED', 'PARTIAL')
    GROUP BY
        invoice_id
),

invoice_payment_status AS (
    -- Combine invoice totals with payment sums
    SELECT
        i.invoice_id,
        i.customer_id,
        i.order_date,
        i.due_date,
        i.total_amount,
        i.currency,
        i.status                                        AS invoice_status,
        COALESCE(p.total_amount_paid, 0.00)             AS total_amount_paid,
        COALESCE(p.payment_count, 0)                    AS payment_count,
        p.first_payment_date,
        p.last_payment_date,
        i.total_amount - COALESCE(p.total_amount_paid, 0.00) AS outstanding_balance,
        CASE
            WHEN COALESCE(p.total_amount_paid, 0.00) = 0              THEN 'UNPAID'
            WHEN COALESCE(p.total_amount_paid, 0.00) >= i.total_amount THEN 'FULLY_PAID'
            ELSE 'PARTIALLY_PAID'
        END AS payment_classification
    FROM
        invoice_totals          i
        LEFT JOIN payment_sums  p   ON p.invoice_id = i.invoice_id
),

partial_payment_details AS (
    -- For partially-paid invoices, show each individual payment
    SELECT
        ips.invoice_id,
        ips.customer_id,
        ips.total_amount,
        ips.total_amount_paid,
        ips.outstanding_balance,
        ips.payment_count,
        ips.payment_classification,
        p.payment_id,
        p.payment_date,
        p.amount_paid               AS individual_payment_amount,
        p.payment_method,
        p.reference_number,
        -- Running total paid at point of each payment
        SUM(p.amount_paid) OVER (
            PARTITION BY p.invoice_id
            ORDER BY p.payment_date, p.payment_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total_paid
    FROM
        invoice_payment_status      ips
        JOIN ar_payments            p   ON p.invoice_id = ips.invoice_id
    WHERE
        ips.payment_classification = 'PARTIALLY_PAID'
        AND p.status IN ('CLEARED', 'PARTIAL')
)

-- Final output: summary of all invoices with payment breakdown
SELECT
    invoice_id,
    customer_id,
    order_date,
    due_date,
    total_amount,
    currency,
    invoice_status,
    total_amount_paid,
    outstanding_balance,
    payment_count,
    first_payment_date,
    last_payment_date,
    payment_classification,
    CASE
        WHEN due_date IS NOT NULL AND outstanding_balance > 0
             AND due_date < CURRENT_DATE
        THEN DATEDIFF(DAY, due_date, CURRENT_DATE)
        ELSE 0
    END AS days_overdue,
    -- Percentage of invoice paid
    CASE
        WHEN total_amount > 0
        THEN ROUND(total_amount_paid / total_amount * 100, 2)
        ELSE 0
    END AS pct_paid
FROM
    invoice_payment_status
ORDER BY
    outstanding_balance DESC,
    customer_id,
    order_date;
