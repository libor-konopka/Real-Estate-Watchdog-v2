import polars as pl
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.orchestration.assets.silver_assets import silver_properties
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

    metrics_table, opportunities_table = process_gold()

    if metrics_table is None or opportunities_table is None:
        context.log.warning("Gold processing yielded no output (empty Silver table).")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("SKIPPED_EMPTY_SILVER"),
                "metrics_records": MetadataValue.int(0),
                "opportunities_records": MetadataValue.int(0),
            }
        )

    # Gather high-level counts via lazy scan without deep memory allocation
    metrics_count = (
        pl.scan_iceberg(metrics_table).select(pl.len().alias("count")).collect().item()
    )
    opportunities_count = (
        pl.scan_iceberg(opportunities_table)
        .select(pl.len().alias("count"))
        .collect()
        .item()
    )

    context.log.info(
        f"Gold tables materialized successfully: {metrics_count} segment metrics, "
        f"{opportunities_count} bargain opportunities discovered."
    )

    return MaterializeResult(
        metadata={
            "batch_id": MetadataValue.text(context.run_id),
            "metrics_table": MetadataValue.text("gold.market_metrics"),
            "opportunities_table": MetadataValue.text("gold.investment_opportunities"),
            "total_market_segments": MetadataValue.int(int(metrics_count)),
            "bargain_opportunities_count": MetadataValue.int(int(opportunities_count)),
        }
    )
