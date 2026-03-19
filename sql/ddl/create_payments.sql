-- AR Payments table for Amazon Redshift
-- DISTKEY on customer_id for JOIN efficiency with ar_invoices
-- SORTKEY on payment_date for chronological scans

CREATE TABLE IF NOT EXISTS ar_payments (
    payment_id          VARCHAR(32)         NOT NULL    ENCODE zstd,
    invoice_id          VARCHAR(32)         NOT NULL    ENCODE zstd,
    customer_id         VARCHAR(16)         NOT NULL    ENCODE zstd,
    payment_date        DATE                NOT NULL    ENCODE az64,
    amount_paid         NUMERIC(18, 2)      NOT NULL    ENCODE az64,
    payment_method      VARCHAR(16)         NOT NULL    ENCODE bytedict,
    reference_number    VARCHAR(64)                     ENCODE zstd,
    status              VARCHAR(16)         NOT NULL    ENCODE bytedict,
    notes               VARCHAR(1024)                   ENCODE zstd,
    created_at          TIMESTAMP           NOT NULL DEFAULT GETDATE() ENCODE az64,
    updated_at          TIMESTAMP           NOT NULL DEFAULT GETDATE() ENCODE az64,
    pipeline_run_id     VARCHAR(64)                     ENCODE zstd,

    CONSTRAINT pk_ar_payments PRIMARY KEY (payment_id),
    CONSTRAINT fk_payment_invoice FOREIGN KEY (invoice_id) REFERENCES ar_invoices(invoice_id),
    CONSTRAINT chk_payment_method CHECK (
        payment_method IN ('ACH','WIRE','CHECK','CREDIT_CARD','DEBIT_CARD','EFT','CASH')
    ),
    CONSTRAINT chk_payment_status CHECK (
        status IN ('PENDING','CLEARED','FAILED','REVERSED','PARTIAL')
    ),
    CONSTRAINT chk_payment_amount_positive CHECK (amount_paid > 0)
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (payment_date);


-- Payment allocations: supports partial payments split across invoices
CREATE TABLE IF NOT EXISTS ar_payment_allocations (
    allocation_id       BIGINT IDENTITY(1, 1) NOT NULL  ENCODE az64,
    payment_id          VARCHAR(32)           NOT NULL  ENCODE zstd,
    invoice_id          VARCHAR(32)           NOT NULL  ENCODE zstd,
    allocated_amount    NUMERIC(18, 2)        NOT NULL  ENCODE az64,
    allocation_date     DATE                  NOT NULL  ENCODE az64,
    notes               VARCHAR(512)                    ENCODE zstd,
    created_at          TIMESTAMP             NOT NULL DEFAULT GETDATE() ENCODE az64,

    CONSTRAINT pk_ar_payment_allocations PRIMARY KEY (allocation_id),
    CONSTRAINT fk_alloc_payment FOREIGN KEY (payment_id) REFERENCES ar_payments(payment_id),
    CONSTRAINT fk_alloc_invoice FOREIGN KEY (invoice_id) REFERENCES ar_invoices(invoice_id),
    CONSTRAINT chk_allocated_amount_positive CHECK (allocated_amount > 0)
)
DISTSTYLE KEY
DISTKEY (payment_id)
SORTKEY (payment_id, invoice_id);

COMMENT ON TABLE ar_payments IS 'AR payment receipts linked to invoices';
COMMENT ON TABLE ar_payment_allocations IS 'Allocation of payments to invoices (supports partial payments)';
