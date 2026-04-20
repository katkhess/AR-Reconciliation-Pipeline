# AR-Reconciliation-Pipeline
End-to-end AWS data pipeline for automated AR reconciliation, handling SKU mismatches and complex payment mapping with Python and SQL.

## One-command local run (Makefile)

Run the entire local pipeline with a single command:

```bash
make local
```

This runs: **venv → install → generate → load → reconcile → validate**

Individual targets:

| Target | What it does |
|--------|-------------|
| `make venv` | Creates `.venv` (skipped if it already exists) |
| `make install` | Installs deps from `requirements.txt` into `.venv` |
| `make generate` | Generates synthetic raw CSVs in `data/raw/` |
| `make load` | Loads raw CSVs into `data/processed/ar_recon.db` |
| `make reconcile` | Builds reconciliation views and exports CSVs |
| `make validate` | Validates SQL views and sanity-checks results |
| `make local` | Runs all of the above in order |
| `make clean` | Removes generated artifacts in `data/raw/` and `data/processed/` |

> **Note:** All Python commands use `.venv/bin/python` to avoid interpreter mismatch.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

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

## CI

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every push and pull request. It installs dependencies and executes `python python/validate_sql_views.py` — the workflow fails if the validation script exits with a non-zero status.
