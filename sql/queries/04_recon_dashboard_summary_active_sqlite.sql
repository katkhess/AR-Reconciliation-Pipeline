DROP VIEW IF EXISTS recon_dashboard_summary_active;

CREATE VIEW recon_dashboard_summary_active AS
SELECT
  match_type,
  COUNT(*) AS payment_count,
  ROUND(SUM(payment_amount), 2) AS total_payment_amount
FROM recon_results_active
GROUP BY match_type
ORDER BY payment_count DESC;