from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal

class Settings(BaseSettings):
    APP_ENV: Literal["dev", "prod"] = "prod"

    # Polymarket
    POLYMARKET_SERIES_ID: int

    # S3
    S3_ENDPOINT_URL: str
    S3_ACCESS_KEY_ID: str
    S3_SECRET_ACCESS_KEY: str
    S3_BUCKET: str

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()
