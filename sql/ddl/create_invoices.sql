-- AR Invoices table for Amazon Redshift
-- DISTKEY on customer_id for co-location with payments/returns
-- SORTKEY on order_date for efficient range scans in aging reports

CREATE TABLE IF NOT EXISTS ar_invoices (
    invoice_id          VARCHAR(32)         NOT NULL    ENCODE zstd,
    customer_id         VARCHAR(16)         NOT NULL    ENCODE zstd,
    order_date          DATE                NOT NULL    ENCODE az64,
    due_date            DATE                            ENCODE az64,
    total_amount        NUMERIC(18, 2)      NOT NULL    ENCODE az64,
    amount_paid         NUMERIC(18, 2)      NOT NULL DEFAULT 0.00 ENCODE az64,
    outstanding_balance NUMERIC(18, 2)      GENERATED ALWAYS AS (total_amount - amount_paid) STORED,
    currency            CHAR(3)             NOT NULL DEFAULT 'USD' ENCODE bytedict,
    status              VARCHAR(16)         NOT NULL    ENCODE bytedict,
    notes               VARCHAR(1024)                   ENCODE zstd,
    created_at          TIMESTAMP           NOT NULL DEFAULT GETDATE() ENCODE az64,
    updated_at          TIMESTAMP           NOT NULL DEFAULT GETDATE() ENCODE az64,
    pipeline_run_id     VARCHAR(64)                     ENCODE zstd,

    CONSTRAINT pk_ar_invoices PRIMARY KEY (invoice_id),
    CONSTRAINT chk_invoice_status CHECK (status IN ('OPEN','PARTIAL','PAID','OVERDUE','VOID')),
    CONSTRAINT chk_invoice_currency CHECK (currency IN ('USD','EUR','GBP','CAD','AUD','JPY')),
    CONSTRAINT chk_invoice_total_positive CHECK (total_amount >= 0),
    CONSTRAINT chk_invoice_amount_paid CHECK (amount_paid >= 0)
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (order_date, due_date);


-- Line items are stored in a child table for normalisation and query efficiency
CREATE TABLE IF NOT EXISTS ar_invoice_line_items (
    line_item_id    BIGINT IDENTITY(1, 1)   NOT NULL    ENCODE az64,
    invoice_id      VARCHAR(32)             NOT NULL    ENCODE zstd,
    sku             VARCHAR(32)             NOT NULL    ENCODE zstd,
    description     VARCHAR(256)                        ENCODE zstd,
    quantity        INT                     NOT NULL    ENCODE az64,
    unit_price      NUMERIC(18, 4)          NOT NULL    ENCODE az64,
    line_total      NUMERIC(18, 2)          NOT NULL    ENCODE az64,
    created_at      TIMESTAMP               NOT NULL DEFAULT GETDATE() ENCODE az64,

    CONSTRAINT pk_ar_invoice_line_items PRIMARY KEY (line_item_id),
    CONSTRAINT fk_invoice_line_invoice FOREIGN KEY (invoice_id) REFERENCES ar_invoices(invoice_id),
    CONSTRAINT chk_line_item_qty CHECK (quantity >= 1),
    CONSTRAINT chk_line_item_price CHECK (unit_price >= 0)
)
DISTSTYLE KEY
DISTKEY (invoice_id)
SORTKEY (invoice_id);

COMMENT ON TABLE ar_invoices IS 'Master AR invoice records';
COMMENT ON TABLE ar_invoice_line_items IS 'Individual SKU line items per invoice';
