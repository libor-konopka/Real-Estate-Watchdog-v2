import contextlib
import os
from functools import lru_cache
from typing import Any

import pyarrow as pa
from loguru import logger
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchTableError,
    RESTError,
)
from pyiceberg.table import Table

from src.config import get_storage_settings
from src.contracts.base import SourcePortal
from src.contracts.schemas import (
    BRONZE_PROPERTY_ARROW_SCHEMA,
    SILVER_PROPERTY_ARROW_SCHEMA,
)


class IcebergCatalogManager:
    """Manages Apache Iceberg catalog connections via Project Nessie and MinIO.

    Centralizes namespace initialization, table identifiers, and schema mapping
    across Medallion layers (bronze, silver, gold).
    """

    BRONZE_NAMESPACE: str = "bronze"
    SILVER_NAMESPACE: str = "silver"
    GOLD_NAMESPACE: str = "gold"

    UNIFIED_SILVER_TABLE: str = "properties"
    GOLD_METRICS_TABLE: str = "market_metrics"
    GOLD_OPPORTUNITIES_TABLE: str = "investment_opportunities"

    def __init__(self) -> None:
        self.settings = get_storage_settings()
        self._catalog: Catalog | None = None

    @property
    def silver_table_id(self) -> str:
        """Returns fully qualified unified Silver table identifier."""
        return f"{self.SILVER_NAMESPACE}.{self.UNIFIED_SILVER_TABLE}"

    @property
    def gold_metrics_table_id(self) -> str:
        """Returns fully qualified Gold market metrics table identifier."""
        return f"{self.GOLD_NAMESPACE}.{self.GOLD_METRICS_TABLE}"

    @property
    def gold_opportunities_table_id(self) -> str:
        """Returns fully qualified Gold investment opportunities table identifier."""
        return f"{self.GOLD_NAMESPACE}.{self.GOLD_OPPORTUNITIES_TABLE}"

    def bronze_table_id(self, source: SourcePortal) -> str:
        """Returns fully qualified Bronze table identifier for a given portal."""
        return f"{self.BRONZE_NAMESPACE}.{source.value}_properties"

    def _export_s3_env(self) -> None:
        """Exports S3 credentials to environment and disables EC2 IMDS probing."""
        os.environ.setdefault("AWS_ACCESS_KEY_ID", self.settings.aws_access_key_id)
        os.environ.setdefault(
            "AWS_SECRET_ACCESS_KEY",
            self.settings.aws_secret_access_key.get_secret_value(),
        )
        os.environ.setdefault("AWS_ENDPOINT_URL", self.settings.aws_endpoint_url)
        os.environ.setdefault("AWS_REGION", self.settings.aws_region)
        os.environ.setdefault("AWS_ALLOW_HTTP", "true")
        os.environ["AWS_EC2_METADATA_DISABLED"] = "true"

    def get_catalog(self) -> Catalog:
        """Initializes and returns cached PyIceberg REST catalog."""
        if self._catalog is not None:
            return self._catalog

        self._export_s3_env()
        ssl_enabled = str(
            self.settings.aws_endpoint_url.lower().startswith("https://")
        ).lower()

        catalog_properties: dict[str, Any] = {
            "type": "rest",
            "uri": self.settings.iceberg_catalog_uri,
            "s3.endpoint": self.settings.aws_endpoint_url,
            "s3.access-key-id": self.settings.aws_access_key_id,
            "s3.secret-access-key": (
                self.settings.aws_secret_access_key.get_secret_value()
            ),
            "s3.region": self.settings.aws_region,
            "s3.path-style-access": str(self.settings.s3_path_style_access).lower(),
            "s3.ssl.enabled": ssl_enabled,
            "warehouse": self.settings.iceberg_warehouse,
        }

        logger.info(
            f"Connecting to Iceberg REST catalog at "
            f"{self.settings.iceberg_catalog_uri}..."
        )
        self._catalog = load_catalog("nessie", **catalog_properties)
        self._ensure_medallion_namespaces()
        return self._catalog

    def _ensure_medallion_namespaces(self) -> None:
        """Ensures that bronze, silver, and gold namespaces exist in the catalog."""
        if self._catalog is None:
            return

        for ns in (
            self.BRONZE_NAMESPACE,
            self.SILVER_NAMESPACE,
            self.GOLD_NAMESPACE,
        ):
            try:
                self._catalog.create_namespace(ns)
                logger.info(f"Created Iceberg namespace: '{ns}'.")
            except NamespaceAlreadyExistsError:
                logger.debug(f"Iceberg namespace '{ns}' already exists.")
            except RESTError as exc:
                logger.warning(
                    f"Namespace verification for '{ns}' returned non-critical "
                    f"status: {exc}"
                )

    def get_or_create_bronze_table(self, source: SourcePortal) -> Table:
        """Loads existing Bronze table or initializes it with Bronze schema."""
        catalog = self.get_catalog()
        table_identifier = self.bronze_table_id(source)

        try:
            return catalog.load_table(table_identifier)
        except NoSuchTableError:
            logger.info(
                f"Table '{table_identifier}' does not exist. "
                "Initializing with Bronze schema..."
            )
            return catalog.create_table(
                identifier=table_identifier,
                schema=BRONZE_PROPERTY_ARROW_SCHEMA,
            )

    def get_or_create_silver_table(self) -> Table:
        """Loads existing Silver table or initializes it with Silver schema."""
        catalog = self.get_catalog()
        table_identifier = self.silver_table_id

        try:
            return catalog.load_table(table_identifier)
        except NoSuchTableError:
            logger.info(
                f"Table '{table_identifier}' does not exist. "
                "Initializing with Silver schema..."
            )
            return catalog.create_table(
                identifier=table_identifier,
                schema=SILVER_PROPERTY_ARROW_SCHEMA,
            )

    def overwrite_or_create_table(
        self,
        table_identifier: str,
        arrow_table: pa.Table,
    ) -> Table:
        """Atomically overwrites an Iceberg table or creates it from Arrow schema."""
        catalog = self.get_catalog()
        namespace = table_identifier.split(".", maxsplit=1)[0]

        with contextlib.suppress(NamespaceAlreadyExistsError):
            catalog.create_namespace(namespace)

        try:
            table = catalog.load_table(table_identifier)
            table.overwrite(arrow_table)
        except NoSuchTableError:
            table = catalog.create_table(
                identifier=table_identifier,
                schema=arrow_table.schema,
            )
            table.append(arrow_table)

        return table


@lru_cache(maxsize=1)
def get_catalog_manager() -> IcebergCatalogManager:
    """Returns a cached singleton of IcebergCatalogManager."""
    return IcebergCatalogManager()


def get_catalog() -> Catalog:
    """Convenience accessor returning the initialized PyIceberg Catalog."""
    return get_catalog_manager().get_catalog()
