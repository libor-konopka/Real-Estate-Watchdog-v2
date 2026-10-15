import os
from functools import lru_cache

from pyiceberg.catalog import Catalog, load_catalog


def set_storage_env() -> None:
    """Sets required S3/MinIO environment variables if not already defined."""
    defaults = {
        "AWS_ACCESS_KEY_ID": "admin",
        "AWS_SECRET_ACCESS_KEY": "password",
        "AWS_ENDPOINT_URL": "http://localhost:9000",
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": "us-east-1",
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)


@lru_cache(maxsize=1)
def get_catalog() -> Catalog:
    """Returns a singleton RestCatalog instance pointing to Nessie and MinIO."""
    set_storage_env()
    return load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": "http://localhost:19120/iceberg/",
            "s3.endpoint": "http://localhost:9000",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        },
    )
