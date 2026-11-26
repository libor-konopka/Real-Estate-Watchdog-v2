from loguru import logger

from src.contracts.base import SourcePortal
from src.storage.catalog import get_catalog_manager


def verify_iceberg_connection() -> None:
    """Verifies Iceberg catalog connectivity and initializes core Medallion tables."""
    logger.info("Testing connection to Nessie REST catalog and MinIO...")

    manager = get_catalog_manager()
    catalog = manager.get_catalog()

    logger.success(f"Connected to Iceberg catalog: {catalog.name}")

    namespaces = {ns[0] for ns in catalog.list_namespaces()}
    logger.info(f"Available namespaces: {sorted(namespaces)}")

    # 1. Verify Bronze tables for active portals
    for source in (SourcePortal.BAZOS, SourcePortal.BEZREALITKY):
        bronze_table = manager.get_or_create_bronze_table(source)
        logger.success(
            f"Bronze table '{manager.bronze_table_id(source)}' ready "
            f"(UUID: {bronze_table.metadata.table_uuid}, "
            f"fields: {len(bronze_table.schema().fields)})"
        )

    # 2. Verify unified Silver table
    silver_table = manager.get_or_create_silver_table()
    logger.success(
        f"Silver table '{manager.silver_table_id}' ready "
        f"(UUID: {silver_table.metadata.table_uuid}, "
        f"fields: {len(silver_table.schema().fields)})"
    )

    # 3. Verify Gold namespace readiness
    if manager.GOLD_NAMESPACE in namespaces:
        logger.success(
            f"Gold namespace '{manager.GOLD_NAMESPACE}' ready "
            f"(targets: {manager.gold_metrics_table_id}, "
            f"{manager.gold_opportunities_table_id})"
        )


if __name__ == "__main__":
    verify_iceberg_connection()
