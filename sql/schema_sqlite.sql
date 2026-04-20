CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name TEXT,
    industry TEXT
);


CREATE TABLE IF NOT EXISTS invoices (
    invoice_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    invoice_date DATE,
    due_date DATE,
    invoice_amount REAL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);


CREATE TABLE IF NOT EXISTS returns (
    return_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    original_invoice_id INTEGER,
    return_sku TEXT,
    credit_amount REAL
);


CREATE TABLE IF NOT EXISTS credits (
    credit_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    credit_amount REAL,
    applied_invoice_id INTEGER
);


CREATE TABLE IF NOT EXISTS payments (
    payment_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    payment_amount REAL,
    payment_date DATE,
    reference_notes TEXT
);


-- Links between a payment and invoice IDs referenced in free-text notes like "INV12 + INV13 ..."
CREATE TABLE IF NOT EXISTS payment_invoice_links (
    payment_id INTEGER NOT NULL,
    invoice_id INTEGER NOT NULL,
    PRIMARY KEY (payment_id, invoice_id),
    FOREIGN KEY (payment_id) REFERENCES payments(payment_id)
);


CREATE TABLE IF NOT EXISTS recon_config_active (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  days_window INTEGER NOT NULL,
  tolerance_amount REAL NOT NULL
);

-- default config row (60 days / $50) if not present
INSERT OR IGNORE INTO recon_config_active (id, days_window, tolerance_amount)
VALUES (1, 60, 50.0);