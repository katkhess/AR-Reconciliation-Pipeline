-- AR Reconciliation Results table for Amazon Redshift
-- Stores the output of each pipeline run for auditing and dashboards

CREATE TABLE IF NOT EXISTS ar_reconciliation_results (
    result_id               BIGINT IDENTITY(1, 1) NOT NULL  ENCODE az64,
    pipeline_run_id         VARCHAR(64)           NOT NULL  ENCODE zstd,
    run_date                DATE                  NOT NULL  ENCODE az64,
    run_timestamp           TIMESTAMP             NOT NULL DEFAULT GETDATE() ENCODE az64,
    discrepancy_type        VARCHAR(32)           NOT NULL  ENCODE bytedict,
    customer_id             VARCHAR(16)           NOT NULL  ENCODE zstd,
    entity_id               VARCHAR(32)           NOT NULL  ENCODE zstd,
    reference_id            VARCHAR(32)                     ENCODE zstd,
    discrepancy_amount      NUMERIC(18, 2)                  ENCODE az64,
    detail                  VARCHAR(512)                    ENCODE zstd,
    classification          VARCHAR(32)                     ENCODE bytedict,
    is_fraud_alert          BOOLEAN               NOT NULL DEFAULT FALSE ENCODE raw,
    is_resolved             BOOLEAN               NOT NULL DEFAULT FALSE ENCODE raw,
    resolved_at             TIMESTAMP                       ENCODE az64,
    resolved_by             VARCHAR(64)                     ENCODE zstd,
    resolution_notes        VARCHAR(1024)                   ENCODE zstd,
    environment             VARCHAR(16)           NOT NULL DEFAULT 'dev' ENCODE bytedict,
    created_at              TIMESTAMP             NOT NULL DEFAULT GETDATE() ENCODE az64,

    CONSTRAINT pk_ar_reconciliation_results PRIMARY KEY (result_id),
    CONSTRAINT chk_discrepancy_type CHECK (
        discrepancy_type IN ('SKU_MISMATCH','OVERPAYMENT','UNDERPAYMENT','ORPHAN_PAYMENT',
                             'ORPHAN_RETURN','SCHEMA_ERROR','OTHER')
    ),
    CONSTRAINT chk_classification CHECK (
        classification IN ('FRAUD','ERROR','SUBSTITUTION','OVERPAYMENT',
                           'UNDERPAYMENT','UNKNOWN') OR classification IS NULL
    )
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (run_date, discrepancy_type);


-- Summary statistics per pipeline run
CREATE TABLE IF NOT EXISTS ar_pipeline_run_summary (
    pipeline_run_id         VARCHAR(64)     NOT NULL    ENCODE zstd,
    run_timestamp           TIMESTAMP       NOT NULL DEFAULT GETDATE() ENCODE az64,
    environment             VARCHAR(16)     NOT NULL DEFAULT 'dev' ENCODE bytedict,
    invoices_processed      INT             NOT NULL DEFAULT 0 ENCODE az64,
    payments_processed      INT             NOT NULL DEFAULT 0 ENCODE az64,
    returns_processed       INT             NOT NULL DEFAULT 0 ENCODE az64,
    sku_mismatches_detected INT             NOT NULL DEFAULT 0 ENCODE az64,
    overpayments_detected   INT             NOT NULL DEFAULT 0 ENCODE az64,
    underpayments_detected  INT             NOT NULL DEFAULT 0 ENCODE az64,
    fraud_alerts            INT             NOT NULL DEFAULT 0 ENCODE az64,
    high_risk_customers     INT             NOT NULL DEFAULT 0 ENCODE az64,
    pipeline_success        BOOLEAN         NOT NULL DEFAULT TRUE ENCODE raw,
    total_duration_seconds  NUMERIC(10, 3)              ENCODE az64,
    notes                   VARCHAR(1024)               ENCODE zstd,

    CONSTRAINT pk_ar_pipeline_run_summary PRIMARY KEY (pipeline_run_id)
)
DISTSTYLE ALL
SORTKEY (run_timestamp);

COMMENT ON TABLE ar_reconciliation_results IS
    'Detailed discrepancy records output by the AR reconciliation pipeline';
COMMENT ON TABLE ar_pipeline_run_summary IS
    'Aggregate metrics per pipeline execution for monitoring and reporting';
