# AWS Setup Notes — AR Reconciliation Pipeline (S3 + Athena)

These notes document the one-time AWS setup needed to run the Athena workflow in this repo.

## Goal (end state)
You should be able to:
1) generate messy sample AR data locally (Python)
2) validate/load it locally (SQLite)
3) export to CSV and upload to S3
4) create Athena external tables over those CSVs
5) run the reconciliation workflow (views + weekly queries)

---

## Data flow (how this project works)
1) Run `python/generate_data.py` to generate intentionally messy sample AR data.
2) Load/validate in SQLite using `sql/schema_sqlite.sql` (local iteration / sanity checking).
3) Export SQLite tables to CSV (or use the existing CSVs in `data/raw/`).
4) Upload CSVs to S3 (see `aws/s3_structure.txt`).
5) Create Athena tables (DDL captured in `sql/schema_athena.sql`).
6) Run the reconciliation workflow in Athena (see `docs/TRAINING_SOP.MD`).

---

## Prerequisites
- AWS access to:
  - S3 (write permission to the data bucket; write permission to the query-results bucket)
  - Athena (query permissions)
  - Glue Data Catalog (commonly required by Athena)
- AWS CLI configured locally (optional, but fastest for uploads)

---

## Step 1 — Confirm the two S3 locations (data vs query results)

### 1.1 Data bucket (external tables read from)
This project’s Athena external tables point to **flat prefixes** in:

- `s3://ar-reconciliation-project-katkhess/`

Expected data locations:
- `s3://ar-reconciliation-project-katkhess/customers/`
- `s3://ar-reconciliation-project-katkhess/invoices/`
- `s3://ar-reconciliation-project-katkhess/payments/`
- `s3://ar-reconciliation-project-katkhess/returns/`
- `s3://ar-reconciliation-project-katkhess/credits/`

(See `aws/s3_structure.txt`.)

### 1.2 Athena query results bucket (“metadata” bucket)
To avoid cluttering the data bucket, Athena query results are written to a separate location:

- `s3://kathessbucket/metadatahealthcare/athena-query-results/`

### 1.3 Athena workgroup
- Workgroup: `primary`

In Athena → Workgroups → `primary`, set **Query result location** to:
- `s3://kathessbucket/metadatahealthcare/athena-query-results/`

---

## Step 2 — Upload the CSVs to the data bucket

Local CSVs (repo):
- `data/raw/customers.csv`
- `data/raw/invoices.csv`
- `data/raw/payments.csv`
- `data/raw/returns.csv`
- `data/raw/credits.csv`

### Option A: Upload via AWS Console
S3 → `ar-reconciliation-project-katkhess` → create folders/prefixes if needed → upload each CSV into:

- `customers/`
- `invoices/`
- `payments/`
- `returns/`
- `credits/`

### Option B: Upload via AWS CLI (recommended)
Run from repo root:

```bash
aws s3 cp data/raw/customers.csv s3://ar-reconciliation-project-katkhess/customers/customers.csv
aws s3 cp data/raw/invoices.csv  s3://ar-reconciliation-project-katkhess/invoices/invoices.csv
aws s3 cp data/raw/payments.csv  s3://ar-reconciliation-project-katkhess/payments/payments.csv
aws s3 cp data/raw/returns.csv   s3://ar-reconciliation-project-katkhess/returns/returns.csv
aws s3 cp data/raw/credits.csv   s3://ar-reconciliation-project-katkhess/credits/credits.csv
```

---

## Step 3 — Create Athena database + external tables
DDL is captured in:
- `sql/schema_athena.sql`

In the Athena query editor:
1) select the `primary` workgroup
2) confirm the query results location is set (Step 1.2)
3) run `sql/schema_athena.sql`

### Quick validation queries
After tables are created, verify data is being read correctly:

```sql
SELECT COUNT(*) FROM ar_project.customers;
SELECT COUNT(*) FROM ar_project.invoices;
SELECT COUNT(*) FROM ar_project.payments;
SELECT COUNT(*) FROM ar_project.returns;
SELECT COUNT(*) FROM ar_project.credits;
```

If any count fails or returns 0:
- verify the S3 object exists under the expected prefix
- verify the Athena table `LOCATION` matches the prefix exactly
- confirm the CSV has a header row (this project uses `skip.header.line.count='1'`)

---

## Step 4 — Create reconciliation views / run workflow queries
Athena reconciliation SQL lives in:
- `sql/queries/`

Run the workflow in the order described in:
- `docs/TRAINING_SOP.MD`

Weekly operator flow typically uses:
- **06a Dashboard**
- **06d One-Button Weekly Query**

---

## Common setup issues (and fixes)
### AccessDenied (S3)
Ensure permissions allow:
- Athena/user to **read** `s3://ar-reconciliation-project-katkhess/*`
- Athena/user to **write** to `s3://kathessbucket/metadatahealthcare/athena-query-results/*`

### CSV parsing errors (HIVE_BAD_DATA, etc.)
This project intentionally generates “messy” data. If parsing fails:
- inspect the raw CSV row that breaks parsing
- adjust your generator/export to standardize quoting/escaping
- (or) change the Athena SERDE settings if needed