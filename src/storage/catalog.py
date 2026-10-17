import os
from functools import lru_cache

from pyiceberg.catalog import Catalog, load_catalog

from src.config import get_storage_settings


def set_storage_env() -> None:
    """
    Propagates validated settings to os.environ for lower-level S3/fsspec libraries.
    """
    settings = get_storage_settings()
    env_mappings = {
        "AWS_ACCESS_KEY_ID": settings.aws_access_key_id,
        "AWS_SECRET_ACCESS_KEY": settings.aws_secret_access_key,
        "AWS_ENDPOINT_URL": settings.aws_endpoint_url,
        "AWS_ALLOW_HTTP": settings.aws_allow_http,
        "AWS_REGION": settings.aws_region,
    }
    for key, value in env_mappings.items():
        os.environ[key] = value


@lru_cache(maxsize=1)
def get_catalog() -> Catalog:
    """
    Returns a singleton RestCatalog instance connected to Nessie and MinIO.
    All endpoints and authentication are injected from validated configuration.
    """
    set_storage_env()
    settings = get_storage_settings()

    return load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": settings.iceberg_catalog_uri,
            "s3.endpoint": settings.iceberg_s3_endpoint,
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        },
    )
