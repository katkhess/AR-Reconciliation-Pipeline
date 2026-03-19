terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Credentials are resolved from the standard AWS credential chain:
  # 1. Environment variables  (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
  # 2. Shared credentials file (~/.aws/credentials)
  # 3. IAM role attached to the EC2/ECS/Lambda execution environment

  default_tags {
    tags = merge(
      {
        Project     = "AR-Reconciliation-Pipeline"
        Environment = var.environment
        ManagedBy   = "Terraform"
        Repository  = "AR-Reconciliation-Pipeline"
      },
      var.tags,
    )
  }
}
