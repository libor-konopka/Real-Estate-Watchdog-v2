from datetime import UTC, datetime

import polars as pl
from loguru import logger
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.table import Table

from src.storage.catalog import get_catalog

SILVER_NAMESPACE = "silver"
SILVER_TABLE_NAME = "properties"
GOLD_NAMESPACE = "gold"
GOLD_METRICS_TABLE = "market_metrics"
GOLD_OPPORTUNITIES_TABLE = "investment_opportunities"


def _save_gold_table(
    df: pl.DataFrame,
    table_name: str,
) -> Table | None:
    """Safely converts Polars DataFrame to Arrow and atomically overwrites Iceberg Gold table."""
    if df.is_empty():
        logger.warning(f"DataFrame for {table_name} is empty. Skipped writing.")
        return None

    catalog = get_catalog()
    table_identifier = f"{GOLD_NAMESPACE}.{table_name}"
    arrow_table = df.to_arrow()

    try:
        catalog.create_namespace(GOLD_NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    try:
        table = catalog.load_table(table_identifier)
        table.overwrite(arrow_table)
    except NoSuchTableError:
        table = catalog.create_table(
            identifier=table_identifier,
            schema=arrow_table.schema,
        )
        table.append(arrow_table)

    logger.info(
        f"Gold table successfully updated: {table_identifier} ({df.height} records)"
    )
    return table


def compute_market_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregates market indicators (medians, averages, days on market)

    grouped by administrative boundaries and property segments.
    """
    now_utc = datetime.now(UTC)

    # 1. Filter out records missing vital analytical metrics
    valid_df = df.filter(
        pl.col("price").is_not_null()
        & (pl.col("price") > 0)
        & pl.col("region").is_not_null()
        & pl.col("district").is_not_null()
    )

    if valid_df.is_empty():
        return pl.DataFrame()

    # 2. Compute Days on Market (DOM) metric per listing
    staged_df = valid_df.with_columns(
        (
            (pl.col("last_seen_at") - pl.col("first_seen_at"))
            .dt.total_days()
            .clip(lower_bound=0)
        )
        .cast(pl.Int32)
        .alias("days_on_market")
    )

    # 3. Vectorized aggregation across market segments
    # Cast count/sum metrics from UInt32 to signed Int32 to match Apache Iceberg IntegerType
    grouping_keys = [
        "region",
        "district",
        "transaction_type",
        "entity_type",
    ]

    metrics_df = (
        staged_df.group_by(grouping_keys)
        .agg(
            [
                pl.len().cast(pl.Int32).alias("total_listings"),
                pl.col("is_active").sum().cast(pl.Int32).alias("active_listings"),
                (~pl.col("is_active")).sum().cast(pl.Int32).alias("inactive_listings"),
                pl.col("price").median().round(0).alias("median_price"),
                pl.col("price").mean().round(0).alias("mean_price"),
                pl.col("price_per_m2").median().round(2).alias("median_price_per_m2"),
                pl.col("price_per_m2").mean().round(2).alias("mean_price_per_m2"),
                pl.col("usable_area_m2").median().round(1).alias("median_area_m2"),
                pl.col("days_on_market")
                .median()
                .round(0)
                .alias("median_days_on_market"),
            ]
        )
        .with_columns(pl.lit(now_utc).alias("calculated_at"))
        .sort(["region", "district", "total_listings"], descending=[False, False, True])
    )

    return metrics_df


def compute_investment_opportunities(
    df: pl.DataFrame,
    min_discount_pct: float = 20.0,
    min_segment_sample_size: int = 5,
) -> pl.DataFrame:
    """Identifies undervalued active listings by comparing price_per_m2

    against the local benchmark median using analytical window expressions.
    """
    # Analyze active building and land assets with measurable price and area
    active_candidates = df.filter(
        pl.col("is_active")
        & pl.col("price_per_m2").is_not_null()
        & (pl.col("price_per_m2") > 0)
        & pl.col("district").is_not_null()
    )

    if active_candidates.is_empty():
        return pl.DataFrame()

    segment_partition = ["district", "transaction_type", "entity_type"]

    # Calculate benchmark metrics per micro-segment using window expressions
    enriched = active_candidates.with_columns(
        pl.len().over(segment_partition).cast(pl.Int32).alias("segment_sample_size"),
        pl.col("price_per_m2")
        .median()
        .over(segment_partition)
        .alias("segment_median_price_m2"),
    )

    # Filter out segments with insufficient statistical confidence
    statistically_valid = enriched.filter(
        pl.col("segment_sample_size") >= min_segment_sample_size
    )

    # Calculate discount percentage relative to the segment median
    opportunities = (
        statistically_valid.with_columns(
            (
                (1.0 - (pl.col("price_per_m2") / pl.col("segment_median_price_m2")))
                * 100.0
            )
            .round(2)
            .alias("discount_pct")
        )
        .filter(pl.col("discount_pct") >= min_discount_pct)
        .select(
            [
                "id",
                "canonical_property_id",
                "source",
                "title",
                "url",
                "region",
                "district",
                "municipality",
                "transaction_type",
                "entity_type",
                "price",
                "usable_area_m2",
                "price_per_m2",
                "segment_median_price_m2",
                "discount_pct",
                "segment_sample_size",
                "first_seen_at",
                "last_seen_at",
            ]
        )
        .sort("discount_pct", descending=True)
    )

    return opportunities


def process_gold() -> tuple[Table | None, Table | None]:
    """Orchestrates Gold layer data processing.

    Reads unified Silver Iceberg table, computes market aggregates and
    investment opportunities, and atomically materializes them to Gold tables.
    """
    catalog = get_catalog()
    silver_identifier = f"{SILVER_NAMESPACE}.{SILVER_TABLE_NAME}"

    try:
        silver_table = catalog.load_table(silver_identifier)
        silver_df = pl.scan_iceberg(silver_table).collect()

        if silver_df.is_empty():
            logger.warning(
                f"Silver table {silver_identifier} is empty. Gold processing skipped."
            )
            return None, None

        # 1. Compute and materialize market metrics
        metrics_df = compute_market_metrics(silver_df)
        metrics_table = _save_gold_table(metrics_df, GOLD_METRICS_TABLE)

        # 2. Compute and materialize bargain investment opportunities
        opportunities_df = compute_investment_opportunities(silver_df)
        opportunities_table = _save_gold_table(
            opportunities_df, GOLD_OPPORTUNITIES_TABLE
        )

        return metrics_table, opportunities_table

    except NoSuchTableError, NoSuchNamespaceError:
        logger.warning(
            f"Silver table {silver_identifier} not found. Skipping Gold layer."
        )
        return None, None
    except Exception as e:
        logger.error(f"Unexpected fault processing Gold layer: {e}")
        raise
