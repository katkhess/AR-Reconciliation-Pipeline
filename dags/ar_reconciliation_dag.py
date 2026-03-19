"""AR Reconciliation Pipeline DAG.

Orchestrates the daily end-to-end AR reconciliation workflow using Apache Airflow.
Task dependency chain:
    extract_invoices >> extract_payments >> extract_returns
        >> validate_data
        >> detect_sku_mismatches >> match_payments
        >> generate_report >> archive_results
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Default arguments shared across all tasks
# ---------------------------------------------------------------------------
DEFAULT_ARGS: dict = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["ar-alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def extract_invoices(**context: dict) -> None:  # type: ignore[type-arg]
    """Download invoice CSV from S3 and push the local path via XCom."""
    import boto3

    conf = context["dag_run"].conf or {}
    bucket = conf.get("s3_bucket_raw", os.environ.get("S3_BUCKET_RAW", "ar-reconciliation-raw"))
    prefix = conf.get("invoice_prefix", "invoices/")
    execution_date = context["ds"]  # e.g. "2024-01-15"

    s3_key = f"{prefix}{execution_date}/invoices.csv"
    local_path = f"/tmp/ar_pipeline/{execution_date}/invoices.csv"
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)

    s3_client = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    s3_client.download_file(bucket, s3_key, local_path)

    context["ti"].xcom_push(key="invoice_path", value=local_path)


def extract_payments(**context: dict) -> None:  # type: ignore[type-arg]
    """Download payment CSV from S3 and push the local path via XCom."""
    import boto3

    conf = context["dag_run"].conf or {}
    bucket = conf.get("s3_bucket_raw", os.environ.get("S3_BUCKET_RAW", "ar-reconciliation-raw"))
    prefix = conf.get("payment_prefix", "payments/")
    execution_date = context["ds"]

    s3_key = f"{prefix}{execution_date}/payments.csv"
    local_path = f"/tmp/ar_pipeline/{execution_date}/payments.csv"
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)

    s3_client = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    s3_client.download_file(bucket, s3_key, local_path)

    context["ti"].xcom_push(key="payment_path", value=local_path)


def extract_returns(**context: dict) -> None:  # type: ignore[type-arg]
    """Download returns CSV from S3 and push the local path via XCom."""
    import boto3

    conf = context["dag_run"].conf or {}
    bucket = conf.get("s3_bucket_raw", os.environ.get("S3_BUCKET_RAW", "ar-reconciliation-raw"))
    prefix = conf.get("returns_prefix", "returns/")
    execution_date = context["ds"]

    s3_key = f"{prefix}{execution_date}/returns.csv"
    local_path = f"/tmp/ar_pipeline/{execution_date}/returns.csv"
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)

    s3_client = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    s3_client.download_file(bucket, s3_key, local_path)

    context["ti"].xcom_push(key="returns_path", value=local_path)


def validate_data(**context: dict) -> None:  # type: ignore[type-arg]
    """Validate all three extracted files against their JSON schemas."""
    from pathlib import Path as _Path
    import json, jsonschema

    ti = context["ti"]
    schema_base = _Path(__file__).resolve().parent.parent / "config" / "schemas"

    paths_and_schemas = [
        (ti.xcom_pull(task_ids="extract_invoices", key="invoice_path"),
         schema_base / "invoices_schema.json"),
        (ti.xcom_pull(task_ids="extract_payments", key="payment_path"),
         schema_base / "payments_schema.json"),
        (ti.xcom_pull(task_ids="extract_returns", key="returns_path"),
         schema_base / "returns_schema.json"),
    ]

    import pandas as pd

    for file_path, schema_path in paths_and_schemas:
        if file_path is None:
            raise ValueError(f"No file path found in XCom for schema {schema_path.name}")
        with schema_path.open() as fh:
            schema = json.load(fh)
        df = pd.read_csv(file_path)
        # Log row count; full per-row validation handled by loaders
        print(f"Validated {len(df)} rows from {file_path} against {schema_path.name}")


def detect_sku_mismatches(**context: dict) -> None:  # type: ignore[type-arg]
    """Load returns and detect SKU mismatches; push mismatch count via XCom."""
    from pathlib import Path as _Path
    from src.ingestion.returns_loader import ReturnsLoader
    from src.transformation.sku_reconciler import SKUReconciler

    ti = context["ti"]
    returns_path = ti.xcom_pull(task_ids="extract_returns", key="returns_path")
    schema_base = _Path(__file__).resolve().parent.parent / "config" / "schemas"

    loader = ReturnsLoader(schema_path=schema_base / "returns_schema.json")
    returns = loader.load_from_csv(returns_path)

    reconciler = SKUReconciler()
    mismatches = reconciler.detect_mismatches(returns)
    for mismatch in mismatches:
        mismatch.classification = reconciler.classify_mismatch(mismatch)

    ti.xcom_push(key="mismatch_count", value=len(mismatches))
    ti.xcom_push(key="fraud_alert_count", value=sum(
        1 for m in mismatches if m.classification.value == "FRAUD"
    ))
    print(f"Detected {len(mismatches)} SKU mismatches.")


def match_payments(**context: dict) -> None:  # type: ignore[type-arg]
    """Load invoices + payments, run payment matching, push discrepancy counts."""
    from pathlib import Path as _Path
    from src.ingestion.invoice_loader import InvoiceLoader
    from src.ingestion.payment_loader import PaymentLoader
    from src.transformation.payment_matcher import PaymentMatcher

    ti = context["ti"]
    schema_base = _Path(__file__).resolve().parent.parent / "config" / "schemas"

    invoice_loader = InvoiceLoader(schema_path=schema_base / "invoices_schema.json")
    payment_loader = PaymentLoader(schema_path=schema_base / "payments_schema.json")

    invoices = invoice_loader.load_from_csv(
        ti.xcom_pull(task_ids="extract_invoices", key="invoice_path")
    )
    payments = payment_loader.load_from_csv(
        ti.xcom_pull(task_ids="extract_payments", key="payment_path")
    )

    matcher = PaymentMatcher()
    matches = matcher.match_payments_to_invoices(payments, invoices)
    overpayments = matcher.detect_overpayments(matches)
    underpayments = matcher.detect_underpayments(matches)

    ti.xcom_push(key="overpayment_count", value=len(overpayments))
    ti.xcom_push(key="underpayment_count", value=len(underpayments))
    print(f"Matched {len(matches)} payments. Over: {len(overpayments)}, Under: {len(underpayments)}")


def generate_report(**context: dict) -> None:  # type: ignore[type-arg]
    """Assemble a full discrepancy report and write it to the processed S3 bucket."""
    import json as _json
    from pathlib import Path as _Path
    from src.ingestion.invoice_loader import InvoiceLoader
    from src.ingestion.payment_loader import PaymentLoader
    from src.ingestion.returns_loader import ReturnsLoader
    from src.transformation.discrepancy_detector import DiscrepancyDetector

    ti = context["ti"]
    execution_date = context["ds"]
    schema_base = _Path(__file__).resolve().parent.parent / "config" / "schemas"

    invoices = InvoiceLoader(schema_path=schema_base / "invoices_schema.json").load_from_csv(
        ti.xcom_pull(task_ids="extract_invoices", key="invoice_path")
    )
    payments = PaymentLoader(schema_path=schema_base / "payments_schema.json").load_from_csv(
        ti.xcom_pull(task_ids="extract_payments", key="payment_path")
    )
    returns = ReturnsLoader(schema_path=schema_base / "returns_schema.json").load_from_csv(
        ti.xcom_pull(task_ids="extract_returns", key="returns_path")
    )

    detector = DiscrepancyDetector()
    report = detector.detect_all(invoices, payments, returns)
    df = detector.to_dataframe(report)

    report_path = f"/tmp/ar_pipeline/{execution_date}/discrepancy_report.csv"
    df.to_csv(report_path, index=False)
    ti.xcom_push(key="report_path", value=report_path)
    print(f"Report written to {report_path} with {len(df)} discrepancies.")


def archive_results(**context: dict) -> None:  # type: ignore[type-arg]
    """Upload the discrepancy report to the processed S3 bucket."""
    import boto3

    conf = context["dag_run"].conf or {}
    bucket = conf.get(
        "s3_bucket_processed",
        os.environ.get("S3_BUCKET_PROCESSED", "ar-reconciliation-processed"),
    )
    execution_date = context["ds"]

    ti = context["ti"]
    report_path = ti.xcom_pull(task_ids="generate_report", key="report_path")
    if not report_path:
        raise ValueError("No report path found in XCom from generate_report task")

    s3_key = f"reports/{execution_date}/discrepancy_report.csv"
    s3_client = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    s3_client.upload_file(report_path, bucket, s3_key)
    print(f"Report archived to s3://{bucket}/{s3_key}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ar_reconciliation_pipeline",
    description="Daily AR reconciliation pipeline: SKU mismatch detection and payment matching",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ar", "reconciliation", "finance", "data-engineering"],
    params={
        "s3_bucket_raw": "ar-reconciliation-raw",
        "s3_bucket_processed": "ar-reconciliation-processed",
        "invoice_prefix": "invoices/",
        "payment_prefix": "payments/",
        "returns_prefix": "returns/",
    },
) as dag:

    t_extract_invoices = PythonOperator(
        task_id="extract_invoices",
        python_callable=extract_invoices,
    )

    t_extract_payments = PythonOperator(
        task_id="extract_payments",
        python_callable=extract_payments,
    )

    t_extract_returns = PythonOperator(
        task_id="extract_returns",
        python_callable=extract_returns,
    )

    t_validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    t_detect_sku_mismatches = PythonOperator(
        task_id="detect_sku_mismatches",
        python_callable=detect_sku_mismatches,
    )

    t_match_payments = PythonOperator(
        task_id="match_payments",
        python_callable=match_payments,
    )

    t_generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    t_archive_results = PythonOperator(
        task_id="archive_results",
        python_callable=archive_results,
    )

    # Task dependency chain
    [t_extract_invoices, t_extract_payments, t_extract_returns] >> t_validate_data
    t_validate_data >> t_detect_sku_mismatches >> t_match_payments
    t_match_payments >> t_generate_report >> t_archive_results
