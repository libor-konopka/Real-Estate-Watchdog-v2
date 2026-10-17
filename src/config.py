from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageSettings(BaseSettings):
    """
    Storage and Lakehouse infrastructure settings.
    Reads values from environment variables or .env file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # MinIO / S3 Credentials
    # default="" uspokojí Pylance, min_length=1 vynutí zadání v .env za běhu
    aws_access_key_id: str = Field(default="", min_length=1, alias="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(
        default="", min_length=1, alias="AWS_SECRET_ACCESS_KEY"
    )
    aws_endpoint_url: str = Field(
        default="http://localhost:9000", alias="AWS_ENDPOINT_URL"
    )
    aws_region: str = Field(default="us-east-1", alias="AWS_REGION")
    aws_allow_http: str = Field(default="true", alias="AWS_ALLOW_HTTP")

    # Nessie / Iceberg Catalog
    iceberg_catalog_uri: str = Field(
        default="http://localhost:19120/iceberg/", alias="ICEBERG_CATALOG_URI"
    )
    iceberg_s3_endpoint: str = Field(
        default="http://localhost:9000", alias="ICEBERG_S3_ENDPOINT"
    )


@lru_cache(maxsize=1)
def get_storage_settings() -> StorageSettings:
    """Returns a cached singleton of storage settings."""
    return StorageSettings()
