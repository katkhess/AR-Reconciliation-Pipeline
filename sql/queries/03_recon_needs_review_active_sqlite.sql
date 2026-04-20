DROP VIEW IF EXISTS recon_needs_review_active;

CREATE VIEW recon_needs_review_active AS
SELECT *
FROM recon_results_active
WHERE match_type IN ('NO_CUTOFF_IN_WINDOW', 'NO_INVOICES_IN_WINDOW');