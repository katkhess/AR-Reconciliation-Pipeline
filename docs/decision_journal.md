# Decision Journal — AR Reconciliation Pipeline

This journal captures the reasoning and iterations behind the AR reconciliation workflow. 

## 2026-04-17 — Establish AWS/Athena workflow + weekly snapshots
### Problem
- Need an end-to-end reconciliation workflow that runs in Athena and produces a repeatable weekly exception list.
- Need clean separation between (a) raw data locations and (b) Athena query outputs.

### Constraints
- Athena SQL dialect and view-based workflow (no stored procedures).
- Outputs must be runnable weekly by non-engineers using saved queries.

### Hypothesis
- A small number of well-named saved queries (build views → weekly outputs) will make the process repeatable.
- Separating query-result output to a dedicated bucket/prefix will avoid mixing generated artifacts with raw data.

### Experiment
- Created a two-bucket layout:
  - Data bucket: s3://ar-reconciliation-project-katkhess/ (external tables read from here)
  - Query results bucket/prefix: s3://kathessbucket/metadatahealthcare/athena-query-results/ (Athena workgroup output)
- Documented the structure and setup steps:
  - aws/s3_structure.txt
  - aws/setup_notes.md
- Implemented weekly snapshot exports using Athena CTAS:
  - 06e: needs review detail → Parquet snapshots written to s3://ar-reconciliation-project-katkhess/snapshots/recon_needs_review_history/run_date=YYYY-MM-DD/
  - 06f: dashboard summary → Parquet snapshots written to s3://ar-reconciliation-project-katkhess/snapshots/recon_dashboard_summary_history/run_date=YYYY-MM-DD/

### Result
- Workflow runs end-to-end in Athena and produces both an actionable exception list and trendable snapshots.

### Decision
- Keep project local-first for reviewers, with AWS/Athena as an optional deployment.
- Use weekly snapshot prefixes partitioned by run_date to support trending and future partitioned history tables.

### Next step
- Build the local SQLite pipeline that mirrors Athena outputs:
  - load raw CSVs into SQLite
  - create views recon_results_active / recon_needs_review_active / recon_dashboard_summary_active
  - export processed CSVs into data/processed for non-AWS reviewers

  ## 2026-04-20 — Local-first pipeline mirrors Athena match_type classification
### Problem
- AWS/Athena logic works, but reviewers/interviewers shouldn’t need AWS access to validate the reconciliation approach.

### Constraints
- Must run fully locally in Codespaces.
- Keep outputs consistent with Athena to avoid maintaining two different “truths.”

### Decision
- Made the local SQLite reconciliation payment-centric to match Athena:
  - recon_results_active (payment-level classification)
  - recon_needs_review_active (actionable subset)
  - recon_dashboard_summary_active (counts by match_type)
- Added a local config table (recon_config_active) to replicate Athena “scenario setters” (days_window, tolerance) without changing SQL.

### Outcome
- Local pipeline can generate raw CSVs, load SQLite, build views, and export CSV outputs that mirror Athena’s workflow and are Power BI/Excel friendly.

