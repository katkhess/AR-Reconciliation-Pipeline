-- SKU Mismatch Detection Query
-- Identifies returns where the physically returned SKU differs from
-- the SKU that was originally invoiced.
--
-- Usage: Run after each pipeline ingestion to populate ar_reconciliation_results
-- Target: Amazon Redshift

WITH return_line_mismatches AS (
    SELECT
        rl.return_id,
        r.original_invoice_id,
        r.customer_id,
        r.return_date,
        r.reason_code,
        r.status                    AS return_status,
        rl.original_sku,
        rl.returned_sku,
        rl.quantity,
        rl.unit_price,
        rl.quantity * rl.unit_price AS line_value,
        -- Join back to the original invoice to get the invoice-level context
        inv.order_date,
        inv.total_amount            AS original_invoice_total,
        inv.currency
    FROM
        ar_return_line_items    rl
        JOIN ar_returns         r   ON r.return_id         = rl.return_id
        JOIN ar_invoices        inv ON inv.invoice_id       = r.original_invoice_id
    WHERE
        rl.original_sku <> rl.returned_sku   -- Only mismatched lines
        AND r.status NOT IN ('REJECTED')      -- Exclude rejected returns
),

-- Flag potential fraud: high-value mismatches with non-defect reason codes
classified_mismatches AS (
    SELECT
        *,
        CASE
            WHEN line_value >= 500.00
                 AND reason_code NOT IN ('WRONG_ITEM', 'DEFECTIVE', 'DAMAGED_IN_TRANSIT')
            THEN 'FRAUD'
            -- Known substitution pair heuristic (extend as needed)
            WHEN original_sku LIKE 'SKU-PRMA%' AND returned_sku LIKE 'SKU-PRMB%' THEN 'SUBSTITUTION'
            WHEN original_sku LIKE 'SKU-PRMB%' AND returned_sku LIKE 'SKU-PRMA%' THEN 'SUBSTITUTION'
            ELSE 'ERROR'
        END AS classification,
        CASE
            WHEN line_value >= 500.00
                 AND reason_code NOT IN ('WRONG_ITEM', 'DEFECTIVE', 'DAMAGED_IN_TRANSIT')
            THEN TRUE
            ELSE FALSE
        END AS is_fraud_alert
    FROM
        return_line_mismatches
)

SELECT
    return_id,
    original_invoice_id,
    customer_id,
    return_date,
    reason_code,
    return_status,
    original_sku,
    returned_sku,
    quantity,
    unit_price,
    line_value,
    currency,
    classification,
    is_fraud_alert,
    order_date          AS original_order_date,
    original_invoice_total,
    DATEDIFF(DAY, order_date, return_date) AS days_to_return
FROM
    classified_mismatches
ORDER BY
    is_fraud_alert DESC,
    line_value     DESC,
    return_date    DESC;
