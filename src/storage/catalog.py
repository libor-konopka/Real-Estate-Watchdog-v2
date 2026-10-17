from functools import lru_cache

from pyiceberg.catalog import Catalog, load_catalog

from src.config import get_storage_settings


@lru_cache(maxsize=1)
def get_catalog() -> Catalog:
    """Initializes and returns a singleton Iceberg REST catalog client.

    Uses PyArrowFileIO for zero-copy Parquet read/write operations against
    MinIO object storage, bypassing global os.environ side-effects.
    """
    settings = get_storage_settings()

    catalog_properties = {
        "type": "rest",
        "uri": settings.iceberg_catalog_uri,
        "warehouse": settings.iceberg_warehouse,
        # Native PyArrow FileIO configuration
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.endpoint": settings.aws_endpoint_url,
        "s3.access-key-id": settings.aws_access_key_id,
        "s3.secret-access-key": settings.aws_secret_access_key.get_secret_value(),
        "s3.region": settings.aws_region,
        "s3.path-style-access": ("true" if settings.s3_path_style_access else "false"),
    }

    return load_catalog("default", **catalog_properties)
