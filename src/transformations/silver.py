import hashlib
import math
from collections.abc import Sequence

import polars as pl
import pyarrow as pa
from loguru import logger
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.table import Table

from src.contracts.base import EntityType, SourcePortal
from src.contracts.schemas import SILVER_PROPERTY_ARROW_SCHEMA
from src.storage.catalog import get_catalog_manager
from src.transformations.enrichment import load_geo_lookup_df


def _effective_area_expr(columns: Sequence[str]) -> pl.Expr:
    """Resolves effective area expression consistently for buildings and land."""
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

    if "entity_type" not in columns:
        return pl.coalesce([building_area, land_area])

    return (
        pl.when(pl.col("entity_type") == EntityType.LAND.value)
        .then(land_area)
        .otherwise(building_area)
    )


def haversine_distance_expr(
    lat1_col: str,
    lon1_col: str,
    lat2_col: str,
    lon2_col: str,
) -> pl.Expr:
    """Computes great-circle distance between two GPS coordinates in meters.

    Fully vectorized using native Polars trigonometric expressions.
    Earth mean radius is parameterized as 6,371,000 meters.
    """
    to_rad = math.pi / 180.0
    r_meters = 6_371_000.0

    phi1 = pl.col(lat1_col) * to_rad
    phi2 = pl.col(lat2_col) * to_rad
    delta_phi = (pl.col(lat2_col) - pl.col(lat1_col)) * to_rad
    delta_lambda = (pl.col(lon2_col) - pl.col(lon1_col)) * to_rad

    a = (delta_phi / 2.0).sin().pow(2) + phi1.cos() * phi2.cos() * (
        delta_lambda / 2.0
    ).sin().pow(2)

    c = 2.0 * a.clip(0.0, 1.0).sqrt().arcsin()
    return r_meters * c


def generate_blocking_key(df: pl.DataFrame) -> pl.DataFrame:
    """Generates coarse partition keys to narrow pairwise comparison space."""
    area_bucket = (
        (_effective_area_expr(df.columns).fill_null(0.0) / 10.0)
        .round(0)
        .cast(pl.Int32)
        .cast(pl.String)
    )

    clean_municipality = (
        pl.col("municipality").cast(pl.String).fill_null("unknown").str.to_lowercase()
    )

    return df.with_columns(
        pl.concat_str(
            [
                clean_municipality,
                pl.col("transaction_type"),
                pl.col("entity_type"),
                area_bucket,
            ],
            separator=":",
        ).alias("blocking_key")
    )

    clean_municipality = (
        pl.col("municipality").cast(pl.String).fill_null("unknown").str.to_lowercase()
    )

    return df.with_columns(
        pl.concat_str(
            [
                clean_municipality,
                pl.col("transaction_type"),
                pl.col("entity_type"),
                area_bucket,
            ],
            separator=":",
        ).alias("blocking_key")
    )


def match_cross_portal_duplicates(
    df: pl.DataFrame,
    max_distance_meters: float = 150.0,
    max_price_diff_ratio: float = 0.12,
    max_area_diff_m2: float = 4.0,
    max_no_gps_price_diff_ratio: float = 0.05,
    max_no_gps_area_diff_m2: float = 2.0,
) -> pl.DataFrame:
    """Identifies identical properties across portals and assigns canonical ID."""
    if df.is_empty():
        return df.with_columns(
            pl.lit(None).cast(pl.String).alias("canonical_property_id")
        )

    effective_area = _effective_area_expr(df.columns)
    staged = df.with_columns(
        effective_area.alias("_effective_area_m2"),
        (effective_area.fill_null(0.0) / 10.0).round(0).cast(pl.Int32).alias("_bucket"),
        pl.col("municipality")
        .cast(pl.String)
        .fill_null("unknown")
        .str.to_lowercase()
        .alias("_municipality_key"),
    )

    optional_cols = [
        col for col in ("postal_code", "currency") if col in staged.columns
    ]
    candidate_cols = [
        "id",
        "source",
        "municipality",
        "_municipality_key",
        "transaction_type",
        "entity_type",
        "_bucket",
        "price",
        "_effective_area_m2",
        "latitude",
        "longitude",
        *optional_cols,
    ]

    # Expand right-hand bucket to [b-1, b, b+1] to avoid 10 m² rounding boundary misses
    right_candidates = (
        staged.select(candidate_cols)
        .with_columns(
            pl.concat_list(
                [
                    pl.col("_bucket") - 1,
                    pl.col("_bucket"),
                    pl.col("_bucket") + 1,
                ]
            ).alias("_bucket")
        )
        .explode("_bucket", empty_as_null=False)
    )

    join_keys = ["_municipality_key", "transaction_type", "entity_type", "_bucket"]
    candidates = (
        staged.select(candidate_cols)
        .join(
            right_candidates,
            on=join_keys,
            how="inner",
            suffix="_match",
        )
        .filter(
            (pl.col("id") < pl.col("id_match"))
            & (pl.col("source") != pl.col("source_match"))
        )
    )

    temp_cols = ["_effective_area_m2", "_bucket", "_municipality_key"]

    if candidates.is_empty():
        return staged.with_columns(pl.col("id").alias("canonical_property_id")).drop(
            temp_cols
        )

    max_price = pl.max_horizontal("price", "price_match")
    price_diff_ratio = (pl.col("price") - pl.col("price_match")).abs() / max_price
    valid_prices = (
        pl.col("price").is_not_null()
        & pl.col("price_match").is_not_null()
        & (pl.col("price") > 0)
        & (pl.col("price_match") > 0)
    )

    area_delta = (
        pl.col("_effective_area_m2") - pl.col("_effective_area_m2_match")
    ).abs()
    valid_areas = (
        pl.col("_effective_area_m2").is_not_null()
        & pl.col("_effective_area_m2_match").is_not_null()
    )

    currency_condition = (
        (
            pl.col("currency").str.to_lowercase()
            == pl.col("currency_match").str.to_lowercase()
        )
        if "currency" in candidates.columns
        else pl.lit(True)
    )

    has_gps = (
        pl.col("latitude").is_not_null()
        & pl.col("longitude").is_not_null()
        & pl.col("latitude_match").is_not_null()
        & pl.col("longitude_match").is_not_null()
    )

    normalized_municipality = pl.col("municipality").cast(pl.String).str.to_lowercase()
    has_known_municipality = normalized_municipality.is_not_null() & (
        normalized_municipality != "unknown"
    )

    same_postal_code = (
        (
            pl.col("postal_code").cast(pl.String).str.replace_all(r"\s+", "")
            == pl.col("postal_code_match").cast(pl.String).str.replace_all(r"\s+", "")
        )
        & pl.col("postal_code").is_not_null()
        & (pl.col("postal_code").cast(pl.String).str.len_chars() > 0)
        if "postal_code" in candidates.columns
        else pl.lit(False)
    )

    distance_expr = haversine_distance_expr(
        "latitude", "longitude", "latitude_match", "longitude_match"
    )

    gps_match = (
        has_gps
        & (distance_expr <= max_distance_meters)
        & (price_diff_ratio <= max_price_diff_ratio)
        & (area_delta <= max_area_diff_m2)
    )

    # Without GPS coordinates, require exact postal code match and tighter tolerances
    # to avoid false positives across large municipalities (e.g., Prague or Brno).
    fallback_match = (
        (~has_gps)
        & has_known_municipality
        & same_postal_code
        & (price_diff_ratio <= max_no_gps_price_diff_ratio)
        & (area_delta <= max_no_gps_area_diff_m2)
    )

    matched_pairs = (
        candidates.filter(
            valid_prices
            & valid_areas
            & currency_condition
            & (gps_match | fallback_match)
        )
        .with_columns(
            pl.when(has_gps)
            .then(distance_expr)
            .otherwise(pl.lit(float("inf")))
            .alias("_match_distance"),
            price_diff_ratio.alias("_price_diff_ratio"),
        )
        .sort(["_match_distance", "_price_diff_ratio", "id"])
        .unique(subset=["id_match"], keep="first")
        .select(
            pl.col("id_match").alias("matched_id"),
            pl.col("id").alias("canonical_id"),
        )
    )

    return (
        staged.join(
            matched_pairs,
            left_on="id",
            right_on="matched_id",
            how="left",
        )
        .with_columns(
            pl.coalesce(["canonical_id", "id"]).alias("canonical_property_id")
        )
        .drop([*temp_cols, "canonical_id"])
    )


def extract_and_enrich_location(df: pl.DataFrame) -> pl.DataFrame:
    """Unnests location struct, extracts flat coordinates, and enriches geography."""
    if "location" in df.columns:
        df = df.unnest("location")

    if "gps" in df.columns:
        df = df.with_columns(
            pl.col("gps").struct.field("lat").alias("latitude"),
            pl.col("gps").struct.field("lon").alias("longitude"),
        ).drop("gps")

    geo_df = load_geo_lookup_df()

    if "postal_code" in df.columns and not geo_df.is_empty():
        df = df.with_columns(
            pl.col("postal_code")
            .cast(pl.String)
            .str.replace_all(r"\s+", "")
            .alias("postal_code")
        )

        df = df.join(geo_df, on="postal_code", how="left")
        df = df.with_columns(
            pl.coalesce(["enriched_region", "region"]).alias("region"),
            pl.coalesce(["enriched_district", "district"]).alias("district"),
            pl.coalesce(["municipality", "enriched_municipality"]).alias(
                "municipality"
            ),
        ).drop(["enriched_region", "enriched_district", "enriched_municipality"])

    return df


def clean_infrastructure(df: pl.DataFrame) -> pl.DataFrame:
    """Unnests infrastructure fields and fills baseline electricity defaults."""
    if "infrastructure" in df.columns:
        df = df.unnest("infrastructure")

    if "electricity" in df.columns and "entity_type" in df.columns:
        df = df.with_columns(
            pl.when(
                (pl.col("entity_type").is_in(["apartment", "house"]))
                & (pl.col("electricity").is_null())
            )
            .then(True)
            .otherwise(pl.col("electricity"))
            .alias("electricity")
        )
    return df


def clean_legal_context(df: pl.DataFrame) -> pl.DataFrame:
    """Unnests legal context struct and ensures ISO date parsing."""
    if "legal" in df.columns:
        df = df.unnest("legal")
        if "available_from" in df.columns:
            df = df.with_columns(pl.col("available_from").cast(pl.Date, strict=False))
    return df


def compute_derived_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """Computes analytical metrics such as price per square meter."""
    target_area = _effective_area_expr(df.columns)

    return df.with_columns(
        pl.when(
            (pl.col("price").is_not_null())
            & (target_area.is_not_null())
            & (target_area > 0)
        )
        .then((pl.col("price") / target_area).round(2))
        .otherwise(None)
        .alias("price_per_m2")
    )


def calculate_lifecycle_and_deduplicate(df: pl.DataFrame) -> pl.DataFrame:
    """Computes lifecycle timestamps, restores tombstones, and deduplicates."""
    excluded_cols = {
        "source",
        "source_id",
        "is_active",
        "scraped_at",
        "batch_id",
        "ingested_at",
        "first_seen_at",
        "last_seen_at",
    }
    fillable_cols = [col for col in df.columns if col not in excluded_cols]

    staged = df.sort(["source", "source_id", "scraped_at"]).with_columns(
        pl.col("scraped_at").min().over(["source", "source_id"]).alias("first_seen_at"),
        pl.col("scraped_at").max().over(["source", "source_id"]).alias("last_seen_at"),
    )

    if "title" in staged.columns:
        staged = staged.with_columns(
            pl.when(pl.col("title") == "[DEACTIVATED]")
            .then(None)
            .otherwise(pl.col("title"))
            .alias("title")
        )

    # Restore attributes from prior snapshots only onto inactive tombstone rows;
    # active updates keep their explicit values (including nullified fields).
    if fillable_cols:
        staged = staged.with_columns(
            [
                pl.when(pl.col("is_active"))
                .then(pl.col(col))
                .otherwise(pl.col(col).forward_fill().over(["source", "source_id"]))
                .alias(col)
                for col in fillable_cols
            ]
        )

    if "title" in staged.columns:
        staged = staged.with_columns(pl.col("title").fill_null("[DEACTIVATED]"))

    return staged.unique(
        subset=["source", "source_id"],
        keep="last",
        maintain_order=True,
    )


def compute_content_hash(df: pl.DataFrame) -> pl.DataFrame:
    """Computes deterministic SHA-256 hash over core attributes for change tracking."""
    payload_expr = pl.concat_str(
        [
            pl.col("title").fill_null(""),
            pl.col("price").fill_null(0.0).cast(pl.String),
            pl.col("usable_area_m2").fill_null(0.0).cast(pl.String),
            pl.col("land_area_m2").fill_null(0.0).cast(pl.String),
            pl.col("condition").fill_null(""),
            pl.col("description").fill_null(""),
        ],
        separator="|",
    )

    raw_payloads = df.select(payload_expr.alias("payload"))["payload"].to_list()
    hashes = [hashlib.sha256(item.encode("utf-8")).hexdigest() for item in raw_payloads]

    return df.with_columns(pl.Series("content_hash", hashes, dtype=pl.String))


def _align_to_silver_schema(df: pl.DataFrame) -> pa.Table:
    """Aligns Polars DataFrame to the exact Silver PyArrow schema."""
    missing_expressions = [
        pl.lit(None).alias(field.name)
        for field in SILVER_PROPERTY_ARROW_SCHEMA
        if field.name not in df.columns
    ]
    if missing_expressions:
        df = df.with_columns(missing_expressions)

    df = df.select([field.name for field in SILVER_PROPERTY_ARROW_SCHEMA])
    raw_arrow = df.to_arrow()

    arrays = [
        raw_arrow.column(field.name).cast(field.type)
        for field in SILVER_PROPERTY_ARROW_SCHEMA
    ]

    return pa.Table.from_arrays(arrays, schema=SILVER_PROPERTY_ARROW_SCHEMA)


def _load_and_clean_bronze(source: SourcePortal) -> pl.DataFrame | None:
    """Loads a Bronze Iceberg table and applies structural unnesting and cleaning."""
    manager = get_catalog_manager()
    catalog = manager.get_catalog()
    table_identifier = manager.bronze_table_id(source)

    try:
        bronze_table = catalog.load_table(table_identifier)
        df = pl.scan_iceberg(bronze_table).collect()

        if df.is_empty():
            logger.info(f"Bronze table {table_identifier} is empty.")
            return None

        # Apply structural unnesting, lifecycle restoration, and metric derivation
        df = extract_and_enrich_location(df)
        df = clean_infrastructure(df)
        df = clean_legal_context(df)
        df = calculate_lifecycle_and_deduplicate(df)
        df = compute_derived_metrics(df)
        df = compute_content_hash(df)

        return df

    except NoSuchTableError, NoSuchNamespaceError:
        logger.info(f"Bronze table {table_identifier} not found, skipping source.")
        return None


def process_silver(
    sources: list[SourcePortal] | None = None,
) -> Table | None:
    """Orchestrates unified Silver layer processing across multiple portal sources.

    1. Loads and cleans raw records from all specified Bronze tables.
    2. Unions datasets into a cohesive multi-portal DataFrame.
    3. Executes cross-portal Entity Resolution to discover identical properties.
    4. Enforces strict Silver PyArrow schema.
    5. Atomically overwrites the unified Iceberg table `silver.properties`.
    """
    target_sources = sources or [SourcePortal.BAZOS, SourcePortal.BEZREALITKY]
    manager = get_catalog_manager()
    silver_identifier = manager.silver_table_id

    cleaned_dfs: list[pl.DataFrame] = []
    for src in target_sources:
        src_df = _load_and_clean_bronze(src)
        if src_df is not None and not src_df.is_empty():
            cleaned_dfs.append(src_df)

    if not cleaned_dfs:
        logger.warning("No Bronze data found across any source. Silver skipped.")
        return None

    # Diagonal concatenation preserves disjoint portal attributes as nulls
    unified_df = pl.concat(cleaned_dfs, how="diagonal_relaxed")

    # Cross-portal Entity Resolution: matches identical properties across portals
    unified_df = match_cross_portal_duplicates(unified_df)

    arrow_table = _align_to_silver_schema(unified_df)
    table = manager.overwrite_or_create_table(silver_identifier, arrow_table)

    logger.info(
        f"Unified Silver layer updated: {silver_identifier} "
        f"({unified_df.height} records across {len(cleaned_dfs)} sources)"
    )
    return table
