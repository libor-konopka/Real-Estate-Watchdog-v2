import polars as pl
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.orchestration.assets.silver_assets import silver_properties
from src.storage.catalog import get_catalog_manager
from src.transformations.gold import process_gold


@asset(
    deps=[silver_properties],
    group_name="gold_layer",
    description=(
        "Computes regional market indicators and discovers undervalued "
        "investment opportunities from unified Silver properties."
    ),
    compute_kind="polars",
)
def gold_market_analytics(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Executes Gold layer data aggregation and anomaly detection."""
    context.log.info("Executing Gold layer aggregation and market analysis...")
    manager = get_catalog_manager()

    metrics_table, opportunities_table = process_gold()

    if metrics_table is None and opportunities_table is None:
        context.log.warning("Gold processing yielded no output (empty Silver table).")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("SKIPPED_EMPTY_SILVER"),
                "metrics_records": MetadataValue.int(0),
                "opportunities_records": MetadataValue.int(0),
            }
        )

    metrics_count = (
        int(
            pl.scan_iceberg(metrics_table)
            .select(pl.len().alias("count"))
            .collect()
            .item()
        )
        if metrics_table is not None
        else 0
    )
    opportunities_count = (
        int(
            pl.scan_iceberg(opportunities_table)
            .select(pl.len().alias("count"))
            .collect()
            .item()
        )
        if opportunities_table is not None
        else 0
    )

    context.log.info(
        f"Gold tables materialized: {metrics_count} segment metrics, "
        f"{opportunities_count} bargain opportunities discovered."
    )

    return MaterializeResult(
        metadata={
            "batch_id": MetadataValue.text(context.run_id),
            "metrics_table": MetadataValue.text(manager.gold_metrics_table_id),
            "opportunities_table": MetadataValue.text(
                manager.gold_opportunities_table_id
            ),
            "total_market_segments": MetadataValue.int(metrics_count),
            "bargain_opportunities_count": MetadataValue.int(opportunities_count),
        }
    )
