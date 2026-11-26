from collections.abc import Sequence
from datetime import UTC, datetime

import polars as pl
from loguru import logger
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.table import Table

from src.contracts.base import Currency, EntityType
from src.storage.catalog import get_catalog_manager

# Filters out placeholder/symbolic prices (e.g., "1 Kč") that skew market benchmarks
DEFAULT_MIN_VALID_PRICE_CZK: float = 1_000.0


def _effective_area_expr(columns: Sequence[str]) -> pl.Expr:
    """Resolves effective area expression for both buildings and land."""
    building_cols = [
        pl.col(col) for col in ("usable_area_m2", "built_up_area_m2") if col in columns
    ]
    building_area = (
        pl.coalesce(building_cols) if building_cols else pl.lit(None, dtype=pl.Float64)
    )
    land_area = (
        pl.col("land_area_m2")
        if "land_area_m2" in columns
        else pl.lit(None, dtype=pl.Float64)
    )

    return (
        pl.when(pl.col("entity_type") == EntityType.LAND.value)
        .then(land_area)
        .otherwise(building_area)
    )


def _deduplicate_canonical(df: pl.DataFrame) -> pl.DataFrame:
    """Keeps one representative listing per physical property.

    Prefers active listings and the most recent observation.
    """
    if "canonical_property_id" not in df.columns or df.is_empty():
        return df

    return df.sort(["is_active", "last_seen_at"]).unique(
        subset=["canonical_property_id"],
        keep="last",
    )


def _save_gold_table(
    df: pl.DataFrame,
    table_identifier: str,
) -> Table | None:
    """Converts Polars DataFrame to Arrow and overwrites Iceberg Gold table."""
    if df.is_empty():
        logger.warning(f"DataFrame for {table_identifier} is empty. Skipped writing.")
        return None

    manager = get_catalog_manager()
    table = manager.overwrite_or_create_table(table_identifier, df.to_arrow())

    logger.info(
        f"Gold table successfully updated: {table_identifier} ({df.height} records)"
    )
    return table


def compute_market_metrics(
    df: pl.DataFrame,
    min_valid_price: float = DEFAULT_MIN_VALID_PRICE_CZK,
) -> pl.DataFrame:
    """Aggregates market indicators grouped by district and property segment."""
    now_utc = datetime.now(UTC)

    deduped_df = _deduplicate_canonical(df)

    valid_df = deduped_df.filter(
        pl.col("price").is_not_null()
        & (pl.col("price") >= min_valid_price)
        & (pl.col("currency").str.to_lowercase() == Currency.CZK.value)
        & pl.col("region").is_not_null()
        & pl.col("district").is_not_null()
    )

    if valid_df.is_empty():
        return pl.DataFrame()

    staged_df = valid_df.with_columns(
        (
            (pl.col("last_seen_at") - pl.col("first_seen_at"))
            .dt.total_days()
            .clip(lower_bound=0)
        )
        .cast(pl.Int32)
        .alias("days_on_market"),
        _effective_area_expr(valid_df.columns).alias("_effective_area_m2"),
    )

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
                pl.col("_effective_area_m2").median().round(1).alias("median_area_m2"),
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
    min_valid_price: float = DEFAULT_MIN_VALID_PRICE_CZK,
) -> pl.DataFrame:
    """Identifies undervalued active listings against local district medians."""
    deduped_df = _deduplicate_canonical(df)

    active_candidates = deduped_df.filter(
        pl.col("is_active")
        & pl.col("price").is_not_null()
        & (pl.col("price") >= min_valid_price)
        & (pl.col("currency").str.to_lowercase() == Currency.CZK.value)
        & pl.col("price_per_m2").is_not_null()
        & (pl.col("price_per_m2") > 0)
        & pl.col("district").is_not_null()
    )

    if active_candidates.is_empty():
        return pl.DataFrame()

    segment_partition = ["district", "transaction_type", "entity_type"]

    enriched = active_candidates.with_columns(
        _effective_area_expr(active_candidates.columns).alias("area_m2"),
        pl.len().over(segment_partition).cast(pl.Int32).alias("segment_sample_size"),
        pl.col("price_per_m2")
        .median()
        .over(segment_partition)
        .alias("segment_median_price_m2"),
    )

    statistically_valid = enriched.filter(
        pl.col("segment_sample_size") >= min_segment_sample_size
    )

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
                "area_m2",
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
    manager = get_catalog_manager()
    catalog = manager.get_catalog()
    silver_identifier = manager.silver_table_id

    try:
        silver_table = catalog.load_table(silver_identifier)
        silver_df = pl.scan_iceberg(silver_table).collect()

        if silver_df.is_empty():
            logger.warning(
                f"Silver table {silver_identifier} is empty. Gold processing skipped."
            )
            return None, None

        metrics_df = compute_market_metrics(silver_df)
        metrics_table = _save_gold_table(metrics_df, manager.gold_metrics_table_id)

        opportunities_df = compute_investment_opportunities(silver_df)
        opportunities_table = _save_gold_table(
            opportunities_df, manager.gold_opportunities_table_id
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
