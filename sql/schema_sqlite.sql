CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name TEXT,
    industry TEXT
);


CREATE TABLE invoices (
    invoice_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    invoice_date DATE,
    due_date DATE,
    invoice_amount REAL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);


CREATE TABLE returns (
    return_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    original_invoice_id INTEGER,
    return_sku TEXT,
    credit_amount REAL
);


CREATE TABLE credits (
    credit_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    credit_amount REAL,
    applied_invoice_id INTEGER
);


CREATE TABLE payments (
    payment_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    payment_amount REAL,
    payment_date DATE,
    reference_notes TEXT
);

