-- AR Returns table for Amazon Redshift
-- Includes sku_mismatch_flag for fast filtering without joins
-- DISTKEY on customer_id, SORTKEY on return_date

CREATE TABLE IF NOT EXISTS ar_returns (
    return_id               VARCHAR(32)     NOT NULL    ENCODE zstd,
    original_invoice_id     VARCHAR(32)     NOT NULL    ENCODE zstd,
    customer_id             VARCHAR(16)     NOT NULL    ENCODE zstd,
    return_date             DATE            NOT NULL    ENCODE az64,
    reason_code             VARCHAR(32)     NOT NULL    ENCODE bytedict,
    status                  VARCHAR(16)     NOT NULL    ENCODE bytedict,
    sku_mismatch_flag       BOOLEAN         NOT NULL DEFAULT FALSE ENCODE raw,
    credit_memo_id          VARCHAR(32)                 ENCODE zstd,
    notes                   VARCHAR(1024)               ENCODE zstd,
    created_at              TIMESTAMP       NOT NULL DEFAULT GETDATE() ENCODE az64,
    updated_at              TIMESTAMP       NOT NULL DEFAULT GETDATE() ENCODE az64,
    pipeline_run_id         VARCHAR(64)                 ENCODE zstd,

    CONSTRAINT pk_ar_returns PRIMARY KEY (return_id),
    CONSTRAINT fk_return_invoice FOREIGN KEY (original_invoice_id)
        REFERENCES ar_invoices(invoice_id),
    CONSTRAINT chk_return_reason_code CHECK (
        reason_code IN (
            'DEFECTIVE','WRONG_ITEM','NOT_AS_DESCRIBED','DUPLICATE_ORDER',
            'CUSTOMER_CHANGE_OF_MIND','DAMAGED_IN_TRANSIT','QUALITY_ISSUE','OTHER'
        )
    ),
    CONSTRAINT chk_return_status CHECK (
        status IN ('PENDING','APPROVED','REJECTED','PROCESSED','REFUNDED')
    )
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (return_date);


-- Return line items: captures the original vs returned SKU for mismatch detection
CREATE TABLE IF NOT EXISTS ar_return_line_items (
    return_line_id      BIGINT IDENTITY(1, 1) NOT NULL  ENCODE az64,
    return_id           VARCHAR(32)           NOT NULL  ENCODE zstd,
    original_sku        VARCHAR(32)           NOT NULL  ENCODE zstd,
    returned_sku        VARCHAR(32)           NOT NULL  ENCODE zstd,
    quantity            INT                   NOT NULL  ENCODE az64,
    unit_price          NUMERIC(18, 4)        NOT NULL  ENCODE az64,
    line_total          NUMERIC(18, 2)        NOT NULL  ENCODE az64,
    has_sku_mismatch    BOOLEAN               NOT NULL
        GENERATED ALWAYS AS (original_sku <> returned_sku) STORED,
    notes               VARCHAR(512)                    ENCODE zstd,
    created_at          TIMESTAMP             NOT NULL DEFAULT GETDATE() ENCODE az64,

    CONSTRAINT pk_ar_return_line_items PRIMARY KEY (return_line_id),
    CONSTRAINT fk_return_line_return FOREIGN KEY (return_id)
        REFERENCES ar_returns(return_id),
    CONSTRAINT chk_return_line_qty CHECK (quantity >= 1),
    CONSTRAINT chk_return_line_price CHECK (unit_price >= 0)
)
DISTSTYLE KEY
DISTKEY (return_id)
SORTKEY (return_id, has_sku_mismatch);

COMMENT ON TABLE ar_returns IS 'AR return records with SKU mismatch flag';
COMMENT ON TABLE ar_return_line_items IS 'Individual return line items with original vs returned SKU';
COMMENT ON COLUMN ar_returns.sku_mismatch_flag IS
    'Denormalized flag set to TRUE when any line item has original_sku != returned_sku';
