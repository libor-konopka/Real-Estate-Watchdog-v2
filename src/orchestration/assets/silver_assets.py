import polars as pl
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from src.contracts.base import SourcePortal
from src.orchestration.assets.bronze_assets import bronze_bazos_listings
from src.transformations.silver import process_silver

SOURCE = SourcePortal.BAZOS


@asset(
    deps=[bronze_bazos_listings],
    group_name="silver_layer",
    description="Deduplicates, normalizes, and enriches Bazos Bronze listings into Silver Iceberg table.",
    compute_kind="polars",
)
def silver_bazos_properties(context: AssetExecutionContext) -> MaterializeResult:
    """Executes Silver transformation pipeline for Bazos."""
    context.log.info(f"Initiating Silver transformation for {SOURCE.value}...")

    table = process_silver(source=SOURCE)
    table_id = f"silver.{SOURCE.value}_properties"

    if table is None:
        context.log.warning(f"Bronze table is empty. {table_id} was not materialized.")
        return MaterializeResult(
            metadata={
                "target_table": MetadataValue.text(table_id),
                "total_records": MetadataValue.int(0),
                "status": MetadataValue.text("SKIPPED_EMPTY_BRONZE"),
            }
        )

    df = pl.scan_iceberg(table).collect()
    active_count = df.filter(pl.col("is_active") == True).height
    inactive_count = df.filter(pl.col("is_active") == False).height

    return MaterializeResult(
        metadata={
            "target_table": MetadataValue.text(table_id),
            "total_records": MetadataValue.int(df.height),
            "active_listings": MetadataValue.int(active_count),
            "inactive_listings": MetadataValue.int(inactive_count),
        }
    )
