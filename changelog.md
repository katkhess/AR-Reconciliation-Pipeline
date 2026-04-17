# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]
- Local-first pipeline: SQLite loader + reconciliation views + exports (planned).

## [2026-04-17] - AWS/Athena setup + weekly snapshot exports
### Added
- Two-bucket AWS layout: S3 data bucket for external table locations and separate Athena workgroup output bucket/prefix for query results.
- AWS documentation:
  - aws/s3_structure.txt (canonical S3 prefix layout; layout only)
  - aws/setup_notes.md (setup notes)
- Athena weekly snapshots:
  - 06e: detailed needs-review snapshot CTAS written to S3 (snapshots/recon_needs_review_history/run_date=YYYY-MM-DD/)
  - 06f: dashboard summary snapshot CTAS written to S3 (snapshots/recon_dashboard_summary_history/run_date=YYYY-MM-DD/)

### Notes
- Snapshot CTAS requires updating the run date in both the table name suffix (YYYY_MM_DD) and the S3 external_location folder (run_date=YYYY-MM-DD) before each weekly run.