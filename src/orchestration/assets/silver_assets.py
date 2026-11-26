import polars as pl
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.orchestration.assets.bronze_assets import (
    bronze_bazos_listings,
    bronze_bezrealitky_listings,
)
from src.transformations.silver import process_silver


@asset(
    deps=[bronze_bazos_listings, bronze_bezrealitky_listings],
    group_name="silver_layer",
    description=(
        "Consolidates, normalizes, and performs cross-portal entity resolution "
        "across Bazos and Bezrealitky into a unified Silver Iceberg table."
    ),
    compute_kind="polars",
)
def silver_properties(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Executes unified Silver transformation and multi-portal entity resolution."""
    context.log.info("Initiating unified Silver layer materialization...")
    table_id = "silver.properties"

    # Materialize unified Iceberg table across all active portal sources
    table = process_silver()

    if table is None:
        context.log.warning(
            f"No source data available. {table_id} was not materialized."
        )
        return MaterializeResult(
            metadata={
                "target_table": MetadataValue.text(table_id),
                "total_records": MetadataValue.int(0),
                "status": MetadataValue.text("SKIPPED_EMPTY_BRONZE"),
            }
        )

    # Lazy pushdown metrics aggregation over unified Silver dataset
    metrics = (
        pl.scan_iceberg(table)
        .select(
            [
                pl.len().alias("total_records"),
                pl.col("canonical_property_id").n_unique().alias("unique_properties"),
                pl.col("is_active").sum().alias("active_listings"),
                (~pl.col("is_active")).sum().alias("inactive_listings"),
                pl.col("price_per_m2").median().round(2).alias("median_price_per_m2"),
                pl.col("latitude").is_not_null().sum().alias("geocoded_records"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )

    total_records = int(metrics["total_records"])
    unique_properties = int(metrics["unique_properties"] or 0)
    active_listings = int(metrics["active_listings"] or 0)
    inactive_listings = int(metrics["inactive_listings"] or 0)
    geocoded_records = int(metrics["geocoded_records"] or 0)
    median_price_m2 = (
        float(metrics["median_price_per_m2"])
        if metrics["median_price_per_m2"] is not None
        else 0.0
    )

    duplicate_pairs = total_records - unique_properties

    context.log.info(
        f"Unified Silver table {table_id} updated: {total_records} listings "
        f"({unique_properties} unique physical properties, "
        f"{duplicate_pairs} cross-portal duplicates resolved, "
        f"median {median_price_m2} CZK/m²)"
    )

    return MaterializeResult(
        metadata={
            "target_table": MetadataValue.text(table_id),
            "batch_id": MetadataValue.text(context.run_id),
            "total_records": MetadataValue.int(total_records),
            "unique_properties": MetadataValue.int(unique_properties),
            "cross_portal_duplicates": MetadataValue.int(duplicate_pairs),
            "active_listings": MetadataValue.int(active_listings),
            "inactive_listings": MetadataValue.int(inactive_listings),
            "geocoded_records": MetadataValue.int(geocoded_records),
            "median_price_per_m2": MetadataValue.float(median_price_m2),
        }
    )
