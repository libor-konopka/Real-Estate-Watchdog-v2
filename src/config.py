from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageSettings(BaseSettings):
    """Storage and Lakehouse infrastructure settings.

    Loads credentials, endpoints, and storage locations from environment
    variables or a local .env file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # MinIO / S3 Credentials matching docker-compose defaults
    aws_access_key_id: str = Field(
        default="admin", min_length=1, alias="AWS_ACCESS_KEY_ID"
    )
    aws_secret_access_key: SecretStr = Field(
        default=SecretStr("password"), alias="AWS_SECRET_ACCESS_KEY"
    )
    aws_endpoint_url: str = Field(
        default="http://localhost:9000", alias="AWS_ENDPOINT_URL"
    )
    aws_region: str = Field(default="us-east-1", alias="AWS_REGION")

    # MinIO requires path-style addressing: http://endpoint/bucket
    s3_path_style_access: bool = Field(default=True, alias="AWS_S3_PATH_STYLE_ACCESS")

    # Nessie / Iceberg Catalog matching docker-compose ports and bucket
    iceberg_catalog_uri: str = Field(
        default="http://localhost:19120/iceberg/", alias="ICEBERG_CATALOG_URI"
    )
    iceberg_warehouse: str = Field(default="s3://warehouse/", alias="ICEBERG_WAREHOUSE")


@lru_cache(maxsize=1)
def get_storage_settings() -> StorageSettings:
    """Returns a cached singleton of validated storage settings."""
    # Pyright/Pylance cannot infer runtime environment variable injection from BaseSettings.
    return StorageSettings()  # pyright: ignore[reportCallIssue]
