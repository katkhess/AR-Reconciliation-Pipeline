from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    All values can be overridden via environment variables or a .env file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Database
    database_url: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/ar_reconciliation",
        description="Primary PostgreSQL/Redshift connection URL",
    )
    redshift_conn_string: str = Field(
        default="redshift+psycopg2://admin:password@localhost:5439/dev",
        description="Amazon Redshift connection string",
    )

    # AWS S3
    s3_bucket_raw: str = Field(
        default="ar-reconciliation-raw",
        description="S3 bucket for raw ingested data",
    )
    s3_bucket_processed: str = Field(
        default="ar-reconciliation-processed",
        description="S3 bucket for transformed/processed data",
    )
    aws_region: str = Field(
        default="us-east-1",
        description="AWS region for all services",
    )

    # Application
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level",
    )
    environment: Literal["dev", "staging", "prod"] = Field(
        default="dev",
        description="Deployment environment",
    )

    # Pipeline
    pipeline_batch_size: int = Field(
        default=1000,
        description="Number of records to process per batch",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum retry attempts for transient failures",
    )
    retry_delay_seconds: int = Field(
        default=5,
        description="Seconds to wait between retry attempts",
    )

    @field_validator("aws_region")
    @classmethod
    def validate_aws_region(cls, v: str) -> str:
        valid_regions = {
            "us-east-1", "us-east-2", "us-west-1", "us-west-2",
            "eu-west-1", "eu-west-2", "eu-central-1",
            "ap-southeast-1", "ap-southeast-2", "ap-northeast-1",
        }
        if v not in valid_regions:
            raise ValueError(f"Invalid AWS region: {v}. Must be one of {valid_regions}")
        return v

    @property
    def is_production(self) -> bool:
        return self.environment == "prod"

    @property
    def is_development(self) -> bool:
        return self.environment == "dev"


def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()
