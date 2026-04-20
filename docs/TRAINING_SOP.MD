# AR Recon (Athena) — Training Manual / SOP

## 1) Overview (what this workflow is)
This workflow reconciles payments against invoices in Athena and produces an **actionable weekly list** of payments that likely require follow-up. It is designed for real-world constraints:
- Payments often cover **multiple invoices**
- Payments can be **partial**
- Returns/credits reduce what’s truly due
- Teams need a **repeatable process** that doesn’t require rerunning 40 ad-hoc queries

**Primary output:** `ar_project.recon_needs_review_active` (queried by “One-Button Weekly Query”).

---

## 2) Audience
- Primary: AR / Finance ops users who need a weekly exception list
- Secondary: Analysts / technical stakeholders who maintain the logic and expand scenarios

---

## 3) Key concepts (non-technical explanation)
### 3.1 Net Due
Invoices aren’t always “invoice_amount”. Returns/credits matter.
- We compute: **net_due = invoice_gross − credits**

### 3.2 Matching strategy (why the workflow has multiple outcomes)
A payment can be “explained” in more than one valid way:
1) **SINGLE_MATCH**: payment approximately equals a single invoice’s net due (within tolerance).
2) **MULTI_OR_PARTIAL_GREEDY**: payment is explainable as paying **oldest invoices first**, with the last invoice potentially **partial** (very common).
3) **NO_CUTOFF_IN_WINDOW**: even after summing all invoices in the lookback window, payment is still larger → likely prepay/unapplied cash, missing invoices, or window too short.
4) **NO_INVOICES_IN_WINDOW**: no eligible invoices in the window to match.

This SOP focuses operational attention on (3) and (4).

---

## 4) Preconditions / access
- Access to AWS Athena
- Ability to query the `ar_project` database
- Saved queries exist and descriptions are filled in (your numbered list 01–06)
- Athena engine supports views used in this workflow

---

## 5) Saved Query Inventory (what exists)
### 5.1 Build / refresh views (run in order when updating logic)
1. **01-Credits by Invoice**
2. **02-Invoice net balance (gross - credits)**
3. **03a–03d Scenario config setters** (sets active parameters)
4. **04-Recon results (active)**
5. **05-Filter to weekly actions list**

### 5.2 Weekly run queries
- **06a Dashboard (counts by match_type)**
- **06b Needs review (detail list)**
- **06c Full detail (if needed)**
- **06d One-Button Weekly Query**

---

## 6) Weekly operating procedure (step-by-step)
### Step 0 — Confirm you’re in the right place (prevents “silent wrong results”)
- Confirm **Database**: `ar_project`
- Confirm you’re not accidentally running in a different workgroup/output location that breaks exports.

### Step 1 — Choose scenario (only if needed)
Default scenario is **03a (60d / $50)**.

Use:
- **03c (120d / $50)** when you suspect invoices exist but are older than 60 days.
- **03b (60d / $100)** when you suspect rounding/fees/currency differences create slightly bigger mismatches.
- **03d (120d / $100)** when you want the broadest net.

**Verification query (optional):**
- Run a quick `SELECT * FROM ar_project.recon_config_active;` and confirm values.

### Step 2 — Run the dashboard
Run: **06a-"DashBoard" Sus Payments search**

What to look for:
- Is `NO_CUTOFF_IN_WINDOW` unexpectedly huge? (Could mean invoices missing, window too small, or returns/credits not being netted properly.)
- Is everything `MULTI_OR_PARTIAL_GREEDY`? (Often normal—many payments are partial/multi-invoice in real AR.)

### Step 3 — Run the weekly action list
Run: **06d-"One-Button" Weekly Query**

This list is what you hand off or work through.

### Step 4 — Export and triage
Export to CSV and triage by `match_type`:
- **NO_CUTOFF_IN_WINDOW** (most actionable)
  - Check for older invoices (try 120 days)
  - Check for missing invoice ingestion
  - Check for unapplied cash / prepayments
- **NO_INVOICES_IN_WINDOW**
  - Check customer_id mapping
  - Check invoice ingestion date ranges
  - Check if the payment is truly a non-invoice transaction

### Step 5 — Deep dive (only if needed)
Run: **06c Full detail (if needed)**

Use this to debug specific payments or to explain “why did it land in this bucket?”

---

## 7) Real-life workflow hangups (and how this SOP prevents them)
This is the “years in the workforce” part—what breaks in practice and how we designed around it.

### Hangup A: People don’t remember which order to run things
**Mitigation:** numbered saved queries (01–06) + “One-Button Weekly” path.

### Hangup B: People rerun too much or get lost in 40 queries
**Mitigation:** single consolidated output view (`recon_results_active`) + a filtered action view (`recon_needs_review_active`) + two weekly buttons.

### Hangup C: “It says query completed but I see nothing”
This happens when running `CREATE VIEW` statements—Athena returns no rows by design.
**Mitigation:** SOP separates “build/refresh views” vs “run SELECT outputs”.

### Hangup D: False positives destroy trust
If users see tons of “suspicious” items that are normal partial payments, they stop using the tool.
**Mitigation:** explicitly treats greedy cutoff overshoot as **MULTI_OR_PARTIAL** (explainable), focusing review on only the truly unresolved cases.

### Hangup E: Changing requirements (60 vs 120 days, $50 vs $100)
Requirements shift week to week.
**Mitigation:** scenario setter queries (03a–03d) so operators can adjust without editing SQL.

### Hangup F: “The data changed but the results didn’t”
Views don’t “cache”, but upstream tables might update and users may assume results are static.
**Mitigation:** weekly flow always re-runs dashboard + action list; rebuild steps exist if logic needs refresh.

---

## 8) Troubleshooting guide
### Error: `Table ... recon_config_active does not exist`
Cause: Step 03 not run / view deleted.
Fix: run **03a** again.

### Error: `... view does not exist`
Cause: steps were not run in order, or a view was dropped.
Fix: run **01 → 05** in order.

### Problem: counts look wrong or suddenly spike
Checklist:
1) Confirm scenario: `SELECT * FROM recon_config_active;`
2) Switch to 120 days and compare dashboard.
3) Validate returns/credits data (step 01/02).
4) Inspect a sample payment in full detail (06c).

### Problem: payment_amount is not numeric
Cause: `payment_amount` is stored as string.
Fix: this workflow uses `TRY_CAST`. If many become NULL, upstream data needs cleanup.

---

## 9) Maintenance / change control (for you as the owner)
When you update logic:
1) Update the relevant view DDL saved query (usually 04).
2) Run steps 01–05 once to refresh.
3) Run 06a to verify distribution.
4) Document the change (add to README “Decision Journal”).

---

## 10) Appendix — What each match_type means (more detail)
- **SINGLE_MATCH**: best invoice match within tolerance.
- **MULTI_OR_PARTIAL_GREEDY**: payment can be applied oldest-first; last invoice may be partial.
- **NO_CUTOFF_IN_WINDOW**: payment is larger than net invoices in the window; suggests prepay/unapplied cash or missing/out-of-window invoices.
- **NO_INVOICES_IN_WINDOW**: no eligible invoices found to evaluate.
