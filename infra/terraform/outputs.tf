output "raw_bucket_name" {
  description = "Name of the S3 bucket for raw ingested data"
  value       = aws_s3_bucket.raw.bucket
}

output "raw_bucket_arn" {
  description = "ARN of the raw S3 bucket"
  value       = aws_s3_bucket.raw.arn
}

output "processed_bucket_name" {
  description = "Name of the S3 bucket for processed/transformed data"
  value       = aws_s3_bucket.processed.bucket
}

output "processed_bucket_arn" {
  description = "ARN of the processed S3 bucket"
  value       = aws_s3_bucket.processed.arn
}

output "archive_bucket_name" {
  description = "Name of the S3 archive bucket"
  value       = aws_s3_bucket.archive.bucket
}

output "glue_database_name" {
  description = "Name of the AWS Glue Data Catalog database"
  value       = aws_glue_catalog_database.ar_reconciliation.name
}

output "glue_crawler_invoices_name" {
  description = "Name of the Glue crawler for invoice data"
  value       = aws_glue_crawler.invoices.name
}

output "glue_crawler_payments_name" {
  description = "Name of the Glue crawler for payment data"
  value       = aws_glue_crawler.payments.name
}

output "glue_crawler_returns_name" {
  description = "Name of the Glue crawler for returns data"
  value       = aws_glue_crawler.returns.name
}

output "redshift_namespace_id" {
  description = "Redshift Serverless namespace identifier"
  value       = aws_redshiftserverless_namespace.ar_reconciliation.id
}

output "redshift_workgroup_name" {
  description = "Redshift Serverless workgroup name"
  value       = aws_redshiftserverless_workgroup.ar_reconciliation.workgroup_name
}

output "redshift_endpoint" {
  description = "Redshift Serverless workgroup endpoint address"
  value       = aws_redshiftserverless_workgroup.ar_reconciliation.endpoint
}

output "pipeline_iam_role_arn" {
  description = "ARN of the IAM role used by the pipeline (Lambda / ECS)"
  value       = aws_iam_role.pipeline.arn
}

output "eventbridge_rule_name" {
  description = "Name of the EventBridge rule that triggers the daily pipeline"
  value       = aws_cloudwatch_event_rule.daily_pipeline.name
}

output "eventbridge_rule_arn" {
  description = "ARN of the EventBridge daily trigger rule"
  value       = aws_cloudwatch_event_rule.daily_pipeline.arn
}
