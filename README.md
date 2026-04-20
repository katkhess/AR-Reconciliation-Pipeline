# AR-Reconciliation-Pipeline
# AR-Reconciliation-Pipeline
End-to-end AWS data pipeline for automated AR reconciliation, handling SKU mismatches and complex payment mapping with Python and SQL.

## Local (SQLite) quickstart
Run the full local workflow (data generation → SQLite load → reconciliation views → CSV exports):

```bash
python python/generate_data.py
python python/load_sqlite.py
python python/run_reconciliation_local.py
```

Outputs are written to `data/processed/`, including:
- `recon_results_active.csv`
- `recon_needs_review_active.csv`
- `recon_dashboard_summary_active.csv`

## Validate local SQL views
To quickly validate that the local SQLite views build successfully (and catch SQL syntax errors like trailing commas), run:

```bash
python python/validate_sql_views.py
```

This script:
- Builds/refreshes `recon_results_active`, `recon_needs_review_active`, and `recon_dashboard_summary_active`
- Verifies expected row-count relationships (e.g., 1 row per payment in `recon_results_active`)
- Checks required columns exist (e.g., `gap_amount`)
- Prints a `match_type` distribution for quick sanity checking
