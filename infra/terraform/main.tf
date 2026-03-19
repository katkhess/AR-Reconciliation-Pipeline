terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    # Override via -backend-config on init or a backend.hcl file
    bucket = "ar-reconciliation-tfstate"
    key    = "ar-reconciliation/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "AR-Reconciliation-Pipeline"
      Environment = var.environment
      ManagedBy   = "Terraform"
      Team        = "DataEngineering"
    }
  }
}

# ---------------------------------------------------------------------------
# S3 Buckets
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "raw" {
  bucket        = "${var.project_name}-${var.environment}-raw"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket" "processed" {
  bucket        = "${var.project_name}-${var.environment}-processed"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket" "archive" {
  bucket        = "${var.project_name}-${var.environment}-archive"
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# Glue Data Catalog
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "ar_reconciliation" {
  name        = "${replace(var.project_name, "-", "_")}_${var.environment}"
  description = "AR Reconciliation Pipeline data catalog"
}

resource "aws_glue_crawler" "invoices" {
  name          = "${var.project_name}-${var.environment}-invoices-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.ar_reconciliation.name
  description   = "Crawls raw invoice CSV files"

  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/invoices/"
  }

  schedule = "cron(0 1 * * ? *)"

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}

resource "aws_glue_crawler" "payments" {
  name          = "${var.project_name}-${var.environment}-payments-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.ar_reconciliation.name
  description   = "Crawls raw payment CSV files"

  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/payments/"
  }

  schedule = "cron(0 1 * * ? *)"
}

resource "aws_glue_crawler" "returns" {
  name          = "${var.project_name}-${var.environment}-returns-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.ar_reconciliation.name
  description   = "Crawls raw return CSV files"

  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/returns/"
  }

  schedule = "cron(0 1 * * ? *)"
}

# ---------------------------------------------------------------------------
# IAM Roles
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_crawler" {
  name               = "${var.project_name}-${var.environment}-glue-crawler"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project_name}-${var.environment}-glue-s3-access"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*",
          aws_s3_bucket.processed.arn,
          "${aws_s3_bucket.processed.arn}/*",
        ]
      }
    ]
  })
}

data "aws_iam_policy_document" "pipeline_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com", "ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${var.project_name}-${var.environment}-pipeline"
  assume_role_policy = data.aws_iam_policy_document.pipeline_assume_role.json
}

resource "aws_iam_role_policy" "pipeline_s3" {
  name = "${var.project_name}-${var.environment}-pipeline-s3"
  role = aws_iam_role.pipeline.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*",
          aws_s3_bucket.processed.arn,
          "${aws_s3_bucket.processed.arn}/*",
          aws_s3_bucket.archive.arn,
          "${aws_s3_bucket.archive.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Redshift Serverless
# ---------------------------------------------------------------------------

resource "aws_redshiftserverless_namespace" "ar_reconciliation" {
  namespace_name      = "${var.project_name}-${var.environment}"
  db_name             = "ar_reconciliation"
  admin_username      = var.redshift_admin_username
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.pipeline.arn]

  tags = {
    Name = "${var.project_name}-${var.environment}-namespace"
  }
}

resource "aws_redshiftserverless_workgroup" "ar_reconciliation" {
  namespace_name = aws_redshiftserverless_namespace.ar_reconciliation.namespace_name
  workgroup_name = "${var.project_name}-${var.environment}"
  base_capacity  = var.redshift_base_capacity
  publicly_accessible = false

  config_parameter {
    parameter_key   = "enable_user_activity_logging"
    parameter_value = "true"
  }
}

# ---------------------------------------------------------------------------
# EventBridge Rule — daily pipeline trigger
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "daily_pipeline" {
  name                = "${var.project_name}-${var.environment}-daily-trigger"
  description         = "Triggers the AR reconciliation pipeline once per day"
  schedule_expression = "cron(0 6 * * ? *)"
  state               = var.environment == "prod" ? "ENABLED" : "DISABLED"
}
