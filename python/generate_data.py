import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path

np.random.seed(42)
random.seed(42)

# Write raw outputs into repo/data/raw
REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# -------------------
# Customers
# -------------------
customers = pd.DataFrame({
    "customer_id": range(1, 16),
    "customer_name": [f"Customer_{i}" for i in range(1, 16)],
    "industry": np.random.choice(["Retail", "Healthcare", "Construction"], 15)
})

# -------------------
# Invoices
# -------------------
invoices = []
for i in range(1, 101):
    cust_id = random.randint(1, 15)
    invoice_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 120))
    due_date = invoice_date + timedelta(days=30)
    amount = random.randint(300, 5000)

    invoices.append([i, cust_id, invoice_date, due_date, amount])

invoices = pd.DataFrame(invoices, columns=[
    "invoice_id", "customer_id", "invoice_date", "due_date", "invoice_amount"
])

# Standardize dates to ISO strings (portable across SQLite/Athena)
invoices["invoice_date"] = pd.to_datetime(invoices["invoice_date"]).dt.strftime("%Y-%m-%d")
invoices["due_date"] = pd.to_datetime(invoices["due_date"]).dt.strftime("%Y-%m-%d")

# -------------------
# Returns (some mismatched)
# -------------------
returns = []
for i in range(1, 40):
    cust_id = random.randint(1, 15)

    # 70% chance linked correctly, 30% wrong or missing
    if random.random() > 0.3:
        invoice_id = int(random.choice(invoices["invoice_id"]))
    else:
        invoice_id = random.randint(200, 300)  # invalid reference

    return_sku = f"SKU_{random.randint(1000, 2000)}"
    credit_amount = random.randint(50, 1000)

    returns.append([i, cust_id, invoice_id, return_sku, credit_amount])

returns = pd.DataFrame(returns, columns=[
    "return_id", "customer_id", "original_invoice_id", "return_sku", "credit_amount"
])

# -------------------
# Credits (sometimes misapplied)
# -------------------
credits = []
for i in range(1, 40):
    cust_id = random.randint(1, 15)
    credit_amount = random.randint(50, 1500)

    # 60% applied, 40% unapplied
    if random.random() > 0.4:
        applied_invoice_id = int(random.choice(invoices["invoice_id"]))
    else:
        applied_invoice_id = None

    credits.append([i, cust_id, credit_amount, applied_invoice_id])

credits = pd.DataFrame(credits, columns=[
    "credit_id", "customer_id", "credit_amount", "applied_invoice_id"
])

# -------------------
# Payments (CHAOS ZONE)
# -------------------
payments = []
for i in range(1, 60):
    cust_id = random.randint(1, 15)

    selected_invoices = random.sample(list(invoices["invoice_id"]), random.randint(1, 3))
    total_amount = invoices[invoices["invoice_id"].isin(selected_invoices)]["invoice_amount"].sum()

    adjustment = random.randint(-500, 200)
    payment_amount = max(50, int(total_amount + adjustment))

    ref_text = " + ".join([f"INV{int(inv)}" for inv in selected_invoices])
    if random.random() > 0.5:
        ref_text += " less credit"
    if random.random() > 0.7:
        ref_text += " partial"

    payment_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 150))

    payments.append([i, cust_id, payment_amount, payment_date, ref_text])

payments = pd.DataFrame(payments, columns=[
    "payment_id", "customer_id", "payment_amount", "payment_date", "reference_notes"
])
payments["payment_date"] = pd.to_datetime(payments["payment_date"]).dt.strftime("%Y-%m-%d")

# -------------------
# Save Files (to data/raw)
# -------------------
customers.to_csv(RAW_DIR / "customers.csv", index=False)
invoices.to_csv(RAW_DIR / "invoices.csv", index=False)
returns.to_csv(RAW_DIR / "returns.csv", index=False)
credits.to_csv(RAW_DIR / "credits.csv", index=False)
payments.to_csv(RAW_DIR / "payments.csv", index=False)

print(f"Messy dataset created in: {RAW_DIR}")