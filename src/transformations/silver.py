import polars as pl
import pyarrow as pa
from loguru import logger
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.table import Table

from src.contracts.base import EntityType, SourcePortal
from src.contracts.schemas import SILVER_PROPERTY_ARROW_SCHEMA
from src.storage.catalog import get_catalog
from src.transformations.enrichment import load_geo_lookup_df

BRONZE_NAMESPACE = "bronze"
SILVER_NAMESPACE = "silver"


def _align_to_silver_schema(df: pl.DataFrame) -> pa.Table:
    """
    Ensures all Silver schema fields exist and converts DataFrame
    into a strictly typed PyArrow Table matching SILVER_PROPERTY_ARROW_SCHEMA.
    """
    for field in SILVER_PROPERTY_ARROW_SCHEMA:
        if field.name not in df.columns:
            df = df.with_columns(pl.lit(None).alias(field.name))

    df = df.select([f.name for f in SILVER_PROPERTY_ARROW_SCHEMA])
    raw_arrow = df.to_arrow()

    # Bezpečné přetypování sloupců na explicitní typy
    arrays = []
    for field in SILVER_PROPERTY_ARROW_SCHEMA:
        col = raw_arrow.column(field.name)
        if col.type == pa.null() and pa.types.is_nested(field.type):
            arrays.append(pa.nulls(len(raw_arrow), type=field.type))
        else:
            arrays.append(col.cast(field.type))

    return pa.Table.from_arrays(arrays, schema=SILVER_PROPERTY_ARROW_SCHEMA)


def _get_table_identifiers(source: SourcePortal | str) -> tuple[str, str]:
    source_str = source.value if isinstance(source, SourcePortal) else source
    return (
        f"{BRONZE_NAMESPACE}.{source_str}_properties",
        f"{SILVER_NAMESPACE}.{source_str}_properties",
    )


def extract_and_enrich_location(df: pl.DataFrame) -> pl.DataFrame:
    """Unnests location struct and enriches region/district via Polars join."""
    if "location" in df.columns:
        df = df.unnest("location")

    geo_df = load_geo_lookup_df()

    if "postal_code" in df.columns and not geo_df.is_empty():
        # Standardize postal code format to match lookup key
        df = df.with_columns(
            pl.col("postal_code")
            .cast(pl.String)
            .str.replace_all(r"\s+", "")
            .alias("postal_code")
        )

        df = df.join(geo_df, on="postal_code", how="left")
        df = df.with_columns(
            pl.coalesce(["region", "enriched_region"]).alias("region"),
            pl.coalesce(["district", "enriched_district"]).alias("district"),
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
    """
    Computes analytical metrics such as price per square meter.
    Strictly partitions area calculation by entity type:
    - LAND: strictly uses land_area_m2.
    - BUILDINGS (apartment, house, commercial, other): uses usable_area_m2,
      with built_up_area_m2 as a secondary fallback.
    """
    building_area = pl.coalesce(["usable_area_m2", "built_up_area_m2"])

    # Select appropriate area column based on physical nature of the property
    target_area = (
        pl.when(pl.col("entity_type") == EntityType.LAND.value)
        .then(pl.col("land_area_m2"))
        .otherwise(building_area)
    )

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


def process_silver(source: SourcePortal | str) -> Table | None:
    """
    Executes Silver transformation for a specific source.
    Returns the created/updated Table or None if Bronze is empty.
    """
    catalog = get_catalog()
    bronze_identifier, silver_identifier = _get_table_identifiers(source)

    try:
        bronze_table = catalog.load_table(bronze_identifier)
        df = pl.scan_iceberg(bronze_table).collect()

        if df.is_empty():
            logger.warning(
                f"Bronze table {bronze_identifier} is empty. Silver skipped."
            )
            return None

        # Deterministic deduplication
        df = df.sort("scraped_at").unique(subset=["source_id"], keep="last")

        df = extract_and_enrich_location(df)
        df = clean_infrastructure(df)
        df = clean_legal_context(df)
        df = compute_derived_metrics(df)

        arrow_table = _align_to_silver_schema(df)

        try:
            catalog.create_namespace(SILVER_NAMESPACE)
        except NamespaceAlreadyExistsError:
            pass

        try:
            table = catalog.load_table(silver_identifier)
            table.overwrite(arrow_table)
        except NoSuchTableError:
            table = catalog.create_table(
                identifier=silver_identifier,
                schema=pyarrow_to_schema(arrow_table.schema),
            )
            table.append(arrow_table)

        logger.info(f"Silver layer updated: {silver_identifier} ({df.height} records)")
        return table

    except NoSuchTableError, NoSuchNamespaceError:
        logger.warning(f"Bronze table {bronze_identifier} not found. Skipping Silver.")
        return None
    except Exception as e:
        logger.error(f"Failed to process Silver for {source}: {e}")
        raise
