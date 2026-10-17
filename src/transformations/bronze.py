import polars as pl
import pyarrow as pa
from loguru import logger
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.io.pyarrow import pyarrow_to_schema

from src.contracts.base import ActiveListingRef, SourcePortal
from src.contracts.portals.base import AnyProperty
from src.contracts.schemas import PROPERTY_ARROW_SCHEMA
from src.storage.catalog import get_catalog

NAMESPACE = "bronze"


def _get_table_identifier(source: SourcePortal | str) -> str:
    """Constructs the fully qualified table identifier for Iceberg."""
    source_str = source.value if isinstance(source, SourcePortal) else source
    return f"{NAMESPACE}.{source_str}_properties"


def get_known_source_ids(
    source: SourcePortal | str, transaction: str, entity_type: str
) -> dict[str, float | None]:
    """
    Loads existing Bronze Iceberg table and returns a mapping of source_id -> price.
    Used for incremental scraping to detect price changes or skip existing listings.
    """
    catalog = get_catalog()
    table_identifier = _get_table_identifier(source)

    try:
        table = catalog.load_table(table_identifier)
        df = (
            pl.scan_iceberg(table)
            .filter(
                (pl.col("transaction_type") == transaction)
                & (pl.col("entity_type") == entity_type)
            )
            .select(["source_id", "price"])
            .collect()
        )
        return dict(zip(df["source_id"].to_list(), df["price"].to_list()))
    except NoSuchTableError, NoSuchNamespaceError:
        return {}
    except Exception as e:
        logger.error(f"Failed to read known IDs from {table_identifier}: {e}")
        return {}


def save_to_iceberg(
    entities: list[AnyProperty | dict], source: SourcePortal | str
) -> None:
    """
    Directly materializes domain entities or tombstone dicts into an Arrow Table
    matching PROPERTY_ARROW_SCHEMA and appends to the Bronze Iceberg table.
    Bypasses Polars DataFrame creation to prevent struct casting errors on null values.
    """
    if not entities:
        return

    catalog = get_catalog()
    table_identifier = _get_table_identifier(source)

    records: list[dict] = []
    for item in entities:
        if isinstance(item, dict):
            row = item
        else:
            row = item.model_dump(mode="python")

        # Normalize non-Arrow serializable types
        if "url" in row and row["url"] is not None:
            row["url"] = str(row["url"])
        if "price" in row and row["price"] is not None:
            row["price"] = float(row["price"])

        records.append(row)

    # Directly build Arrow Table with enforced schema; handles missing nested fields cleanly
    arrow_table = pa.Table.from_pylist(records, schema=PROPERTY_ARROW_SCHEMA)

    try:
        try:
            catalog.create_namespace(NAMESPACE)
        except NamespaceAlreadyExistsError:
            pass

        try:
            table = catalog.load_table(table_identifier)
        except NoSuchTableError:
            table = catalog.create_table(
                identifier=table_identifier,
                schema=pyarrow_to_schema(PROPERTY_ARROW_SCHEMA),
            )

        table.append(arrow_table)
        logger.info(
            f"Bronze layer appended to {table_identifier} (Records: {len(entities)})"
        )
    except Exception as e:
        logger.error(f"Failed to append records to {table_identifier}: {e}")
        raise


def get_active_bronze_properties(
    source: SourcePortal | str, transaction: str, entity_type: str
) -> list[ActiveListingRef]:
    """
    Retrieves active listings projected to lightweight ActiveListingRef dataclasses.
    Bypasses domain entity deserialization to minimize memory footprint during reconciliation.
    """
    catalog = get_catalog()
    table_identifier = _get_table_identifier(source)

    try:
        table = catalog.load_table(table_identifier)

        # Select strictly required columns for state evaluation
        df = (
            pl.scan_iceberg(table)
            .filter(
                (pl.col("transaction_type") == transaction)
                & (pl.col("entity_type") == entity_type)
            )
            .select(["source_id", "url", "scraped_at", "is_active"])
            .collect()
        )

        if df.is_empty():
            return []

        # Deterministic state reconciliation: retain latest record per source_id
        active_df = (
            df.sort("scraped_at")
            .unique(subset=["source_id"], keep="last")
            .filter(pl.col("is_active") == True)
            .select(["source_id", "url"])
        )

        # Direct row iteration avoids intermediate dictionary allocations
        return [
            ActiveListingRef(source_id=row[0], url=row[1])
            for row in active_df.iter_rows()
        ]

    except NoSuchTableError, NoSuchNamespaceError:
        return []
    except Exception as e:
        logger.error(f"Failed to fetch active records from {table_identifier}: {e}")
        return []
