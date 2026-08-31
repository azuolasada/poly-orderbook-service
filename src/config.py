"""Module for application configuration loaded from environment variables."""

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings sourced from environment variables (or a .env file).

    Attributes:
        APP_ENV (Literal["dev", "prod"]): Deployment environment; controls log level.
        POLYMARKET_SERIES_ID (int): Polymarket series ID to subscribe to.
        S3_ENDPOINT_URL (str): Endpoint URL of the S3-compatible storage.
        S3_ACCESS_KEY_ID (str | None): Access key ID for S3 authentication. Optional — omit to use
            boto3's default credential chain (e.g. an IAM role) instead of a static key.
        S3_SECRET_ACCESS_KEY (str | None): Secret access key for S3 authentication. Optional, see S3_ACCESS_KEY_ID.
        S3_BUCKET (str): Name of the S3 bucket to archive messages to.
    """

    APP_ENV: Literal["dev", "prod"] = "prod"

    # Polymarket
    POLYMARKET_SERIES_ID: int

    # S3
    S3_ENDPOINT_URL: str
    S3_ACCESS_KEY_ID: str | None = None
    S3_SECRET_ACCESS_KEY: str | None = None
    S3_BUCKET: str

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()
