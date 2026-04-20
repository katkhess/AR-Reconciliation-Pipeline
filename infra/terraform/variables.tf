variable "aws_region" {
  description = "AWS region to deploy all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment: dev, staging, or prod"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "project_name" {
  description = "Base name used for resource naming (hyphens allowed)"
  type        = string
  default     = "ar-reconciliation"
}

variable "redshift_admin_username" {
  description = "Admin username for the Redshift Serverless namespace"
  type        = string
  default     = "ar_admin"
  sensitive   = true
}

variable "redshift_admin_password" {
  description = "Admin password for the Redshift Serverless namespace (min 8 chars, mixed case + number)"
  type        = string
  sensitive   = true
}

variable "redshift_base_capacity" {
  description = "Base RPU capacity for Redshift Serverless workgroup (must be a multiple of 8)"
  type        = number
  default     = 8

  validation {
    condition     = var.redshift_base_capacity >= 8 && var.redshift_base_capacity % 8 == 0
    error_message = "redshift_base_capacity must be a positive multiple of 8."
  }
}

variable "enable_s3_lifecycle" {
  description = "Whether to attach lifecycle rules to S3 buckets for cost management"
  type        = bool
  default     = true
}

variable "raw_data_retention_days" {
  description = "Days to retain raw data in the raw S3 bucket before transitioning to Glacier"
  type        = number
  default     = 90
}

variable "processed_data_retention_days" {
  description = "Days to retain processed data before archival"
  type        = number
  default     = 365
}

variable "glue_crawler_schedule" {
  description = "Cron expression for Glue crawler schedule (UTC)"
  type        = string
  default     = "cron(0 1 * * ? *)"
}

variable "vpc_id" {
  description = "VPC ID for Redshift Serverless and other network-bound resources (optional)"
  type        = string
  default     = ""
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for Redshift Serverless workgroup"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}
