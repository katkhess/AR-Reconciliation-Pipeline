PRAGMA foreign_keys = ON;

-- Customers
CREATE TABLE IF NOT EXISTS customers (
  customer_id   INTEGER PRIMARY KEY,
  customer_name TEXT NOT NULL
);

-- SKU crosswalk: maps "alternate" SKUs (used on returns) to canonical SKUs (used on invoices)
CREATE TABLE IF NOT EXISTS sku_crosswalk (
  customer_id     INTEGER NOT NULL,
  alt_sku         TEXT NOT NULL,
  canonical_sku   TEXT NOT NULL,
  confidence      REAL DEFAULT 1.0,  -- 0..1
  notes           TEXT,
  PRIMARY KEY (customer_id, alt_sku),
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Invoice header
CREATE TABLE IF NOT EXISTS invoices (
  invoice_id     INTEGER PRIMARY KEY,
  customer_id    INTEGER NOT NULL,
  invoice_number TEXT NOT NULL UNIQUE,
  invoice_date   TEXT NOT NULL,      -- ISO: YYYY-MM-DD
  due_date       TEXT,
  total_amount   REAL NOT NULL,
  status         TEXT DEFAULT 'OPEN',
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Invoice lines
CREATE TABLE IF NOT EXISTS invoice_lines (
  invoice_line_id INTEGER PRIMARY KEY,
  invoice_id      INTEGER NOT NULL,
  line_number     INTEGER NOT NULL,
  sku             TEXT NOT NULL,
  qty             REAL NOT NULL,
  unit_price      REAL NOT NULL,
  line_amount     REAL NOT NULL,
  FOREIGN KEY (invoice_id) REFERENCES invoices(invoice_id),
  UNIQUE(invoice_id, line_number)
);

-- Returns (can later become credit memos; for v1 treat these as credits)
CREATE TABLE IF NOT EXISTS returns (
  return_id       INTEGER PRIMARY KEY,
  customer_id     INTEGER NOT NULL,
  return_number   TEXT NOT NULL UNIQUE,
  return_date     TEXT NOT NULL,
  referenced_invoice_number TEXT,    -- often missing/incorrect IRL
  return_sku      TEXT NOT NULL,      -- may not match invoice SKU
  qty             REAL NOT NULL,
  unit_credit     REAL NOT NULL,
  credit_amount   REAL NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Payments (one check can cover many invoices)
CREATE TABLE IF NOT EXISTS payments (
  payment_id      INTEGER PRIMARY KEY,
  customer_id     INTEGER NOT NULL,
  payment_ref     TEXT NOT NULL,      -- check # / ACH ref
  payment_date    TEXT NOT NULL,
  payment_amount  REAL NOT NULL,
  memo_text       TEXT,
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Optional: explicit applications (leave empty today; we’ll infer later)
CREATE TABLE IF NOT EXISTS payment_applications (
  payment_application_id INTEGER PRIMARY KEY,
  payment_id             INTEGER NOT NULL,
  invoice_id             INTEGER NOT NULL,
  applied_amount         REAL NOT NULL,
  applied_date           TEXT NOT NULL,
  FOREIGN KEY (payment_id) REFERENCES payments(payment_id),
  FOREIGN KEY (invoice_id) REFERENCES invoices(invoice_id),
  UNIQUE(payment_id, invoice_id)
);