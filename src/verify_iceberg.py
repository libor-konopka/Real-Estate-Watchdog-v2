from loguru import logger

from src.contracts.base import SourcePortal
from src.storage.catalog import get_catalog_manager


def test_iceberg_connection() -> None:
    """Verifies end-to-end Iceberg catalog connectivity and initializes Medallion tables."""
    logger.info("Testing connection to Nessie REST catalog and MinIO...")

    manager = get_catalog_manager()
    catalog = manager.get_catalog()

    logger.success(f"Connected to Iceberg catalog: {catalog.name}")

    namespaces = catalog.list_namespaces()
    logger.info(f"Available namespaces: {namespaces}")

    # 1. Verify Bronze tables for active portals
    for source in (SourcePortal.BAZOS, SourcePortal.BEZREALITKY):
        bronze_table = manager.get_or_create_bronze_table(source)
        logger.success(
            f"Bronze table 'bronze.{source.value}_properties' ready "
            f"(UUID: {bronze_table.metadata.table_uuid}, fields: {len(bronze_table.schema().fields)})"
        )

    # 2. Verify unified Silver table
    silver_table = manager.get_or_create_silver_table()
    logger.success(
        f"Silver table 'silver.properties' ready "
        f"(UUID: {silver_table.metadata.table_uuid}, fields: {len(silver_table.schema().fields)})"
    )


if __name__ == "__main__":
    test_iceberg_connection()
