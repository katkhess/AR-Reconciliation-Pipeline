DROP VIEW IF EXISTS recon_results_active;

CREATE VIEW recon_results_active AS
WITH cfg AS (
  SELECT days_window, tolerance_amount
  FROM recon_config_active
  WHERE id = 1
),
credits_by_invoice AS (
  SELECT applied_invoice_id AS invoice_id, SUM(credit_amount) AS credits_applied
  FROM credits
  WHERE applied_invoice_id IS NOT NULL
  GROUP BY applied_invoice_id
),
returns_by_invoice AS (
  SELECT original_invoice_id AS invoice_id, SUM(credit_amount) AS returns_credit
  FROM returns
  WHERE original_invoice_id IS NOT NULL
  GROUP BY original_invoice_id
),
invoice_net AS (
  SELECT
    i.invoice_id,
    i.customer_id,
    i.invoice_date,
    i.due_date,
    i.invoice_amount,
    COALESCE(c.credits_applied, 0) AS credits_applied,
    COALESCE(r.returns_credit, 0) AS returns_credit,
    (i.invoice_amount - COALESCE(c.credits_applied, 0) - COALESCE(r.returns_credit, 0)) AS invoice_net_due
  FROM invoices i
  LEFT JOIN credits_by_invoice c ON c.invoice_id = i.invoice_id
  LEFT JOIN returns_by_invoice r ON r.invoice_id = i.invoice_id
),
-- invoices eligible for a payment: same customer AND within lookback window ending on payment_date
eligible_invoices AS (
  SELECT
    p.payment_id,
    p.customer_id,
    p.payment_amount,
    p.payment_date,
    inv.invoice_id,
    inv.invoice_date,
    inv.due_date,
    inv.invoice_amount,
    inv.credits_applied,
    inv.returns_credit,
    inv.invoice_net_due
  FROM payments p
  CROSS JOIN cfg
  JOIN invoice_net inv
    ON inv.customer_id = p.customer_id
   AND DATE(inv.invoice_date) >= DATE(p.payment_date, '-' || cfg.days_window || ' days')
   AND DATE(inv.invoice_date) <= DATE(p.payment_date)
),
eligible_counts AS (
  SELECT payment_id, COUNT(*) AS eligible_invoice_count, COALESCE(SUM(invoice_net_due), 0) AS sum_net_due
  FROM eligible_invoices
  GROUP BY payment_id
),
-- SINGLE_MATCH candidate: best invoice by absolute difference to payment_amount
single_best AS (
  SELECT *
  FROM (
    SELECT
      ei.*,
      ABS(ei.payment_amount - ei.invoice_net_due) AS abs_diff,
      ROW_NUMBER() OVER (PARTITION BY ei.payment_id ORDER BY ABS(ei.payment_amount - ei.invoice_net_due) ASC) AS rn
    FROM eligible_invoices ei
  )
  WHERE rn = 1
),
single_match AS (
  SELECT
    sb.payment_id,
    sb.invoice_id AS matched_invoice_id,
    sb.invoice_net_due AS matched_amount,
    sb.abs_diff
  FROM single_best sb
  JOIN cfg
  WHERE sb.abs_diff <= cfg.tolerance_amount
),
-- Greedy oldest-first accumulation
greedy_ranked AS (
  SELECT
    ei.*,
    ROW_NUMBER() OVER (PARTITION BY ei.payment_id ORDER BY DATE(ei.invoice_date) ASC, ei.invoice_id ASC) AS seq
  FROM eligible_invoices ei
),
greedy_running AS (
  SELECT
    gr.*,
    SUM(gr.invoice_net_due) OVER (PARTITION BY gr.payment_id ORDER BY gr.seq ROWS UNBOUNDED PRECEDING) AS running_net_due
  FROM greedy_ranked gr
),
greedy_cutoff AS (
  -- cutoff row = first invoice where running sum meets/exceeds payment_amount
  SELECT *
  FROM (
    SELECT
      payment_id,
      customer_id,
      payment_amount,
      payment_date,
      invoice_id AS cutoff_invoice_id,
      invoice_date AS cutoff_invoice_date,
      running_net_due,
      ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY seq ASC) AS rn
    FROM greedy_running
    WHERE running_net_due >= payment_amount
  )
  WHERE rn = 1
),
classification AS (
  SELECT
    p.payment_id,
    p.customer_id,
    p.payment_amount,
    p.payment_date,
    ec.eligible_invoice_count,
    ec.sum_net_due,
    sm.matched_invoice_id,
    sm.matched_amount,
    sm.abs_diff AS single_match_abs_diff,
    gc.cutoff_invoice_id,
    gc.cutoff_invoice_date,
    gc.running_net_due,
    CASE
      WHEN ec.eligible_invoice_count IS NULL OR ec.eligible_invoice_count = 0 THEN 'NO_INVOICES_IN_WINDOW'
      WHEN sm.payment_id IS NOT NULL THEN 'SINGLE_MATCH'
      WHEN gc.payment_id IS NOT NULL THEN 'MULTI_OR_PARTIAL_GREEDY'
      WHEN ec.sum_net_due < p.payment_amount THEN 'NO_CUTOFF_IN_WINDOW'
      ELSE 'NO_CUTOFF_IN_WINDOW'
    END AS match_type
  FROM payments p
  LEFT JOIN eligible_counts ec ON ec.payment_id = p.payment_id
  LEFT JOIN single_match sm ON sm.payment_id = p.payment_id
  LEFT JOIN greedy_cutoff gc ON gc.payment_id = p.payment_id
)
SELECT
  payment_id,
  customer_id,
  payment_date,
  payment_amount,
  match_type,
  eligible_invoice_count,
  ROUND(sum_net_due, 2) AS sum_net_due_in_window,
  ROUND(payment_amount - sum_net_due, 2) AS gap_amount,
  matched_invoice_id,
  ROUND(matched_amount, 2) AS matched_invoice_net_due,
  ROUND(single_match_abs_diff, 2) AS single_match_abs_diff,
  cutoff_invoice_id,
  cutoff_invoice_date,
  ROUND(running_net_due, 2) AS greedy_running_net_due_at_cutoff
FROM classification;