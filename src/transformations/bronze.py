import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timezone
from typing import Any

import polars as pl
import pyarrow as pa
from loguru import logger
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.io.pyarrow import pyarrow_to_schema

from src.contracts.base import ActiveListingRef, PropertyListing, SourcePortal
from src.contracts.schemas import BRONZE_PROPERTY_ARROW_SCHEMA
from src.storage.catalog import get_catalog

NAMESPACE = "bronze"


def _get_table_identifier(source: SourcePortal | str) -> str:
    """Constructs the fully qualified table identifier for Iceberg."""
    source_str = source.value if isinstance(source, SourcePortal) else source
    return f"{NAMESPACE}.{source_str}_properties"


def get_known_source_ids(
    source: SourcePortal | str, transaction: str, entity_type: str
) -> dict[str, float | None]:
    """Loads existing Bronze Iceberg table and returns a mapping of source_id -> price.

    Ensures deterministic results by taking the most recent price per source_id.
    """
    catalog = get_catalog()
    table_identifier = _get_table_identifier(source)

    try:
        table = catalog.load_table(table_identifier)
        # Scan Iceberg, sort chronologically, and retain strictly the latest record
        df = (
            pl.scan_iceberg(table)
            .filter(
                (pl.col("transaction_type") == transaction)
                & (pl.col("entity_type") == entity_type)
            )
            .sort("scraped_at")
            .unique(subset=["source_id"], keep="last")
            .select(["source_id", "price"])
            .collect()
        )
        return dict(zip(df["source_id"].to_list(), df["price"].to_list(), strict=False))

    except NoSuchTableError, NoSuchNamespaceError:
        # Table or namespace does not exist yet upon initial run
        return {}
    except Exception as e:
        logger.error(f"Failed to read known IDs from {table_identifier}: {e}")
        return {}


def save_to_iceberg(
    entities: Sequence[PropertyListing | dict[str, Any]],
    source: SourcePortal | str,
    batch_id: str | None = None,
) -> None:
    """Materializes domain entities or tombstone dicts into an Arrow Table.

    Accepts any covariant Sequence (list, tuple) of domain listings or raw dictionaries,
    applies Lakehouse audit metadata, and appends to the Bronze Iceberg table.
    """
    if not entities:
        return

    catalog = get_catalog()
    table_identifier = _get_table_identifier(source)

    effective_batch_id = batch_id or uuid.uuid4().hex
    ingested_at_utc = datetime.now(UTC)

    records: list[dict[str, Any]] = []
    for item in entities:
        if isinstance(item, dict):
            row = item.copy()
        else:
            row = item.model_dump(mode="python")

        row["batch_id"] = effective_batch_id
        row["ingested_at"] = ingested_at_utc

        if "url" in row and row["url"] is not None:
            row["url"] = str(row["url"])
        if "price" in row and row["price"] is not None:
            row["price"] = float(row["price"])

        records.append(row)

    arrow_table = pa.Table.from_pylist(records, schema=BRONZE_PROPERTY_ARROW_SCHEMA)

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
                schema=pyarrow_to_schema(BRONZE_PROPERTY_ARROW_SCHEMA),
            )

        table.append(arrow_table)
        logger.info(
            f"Bronze layer appended to {table_identifier} (Records: {len(entities)}, Batch: {effective_batch_id})"
        )
    except Exception as e:
        logger.error(f"Failed to append records to {table_identifier}: {e}")
        raise


def get_active_bronze_properties(
    source: SourcePortal | str, transaction: str, entity_type: str
) -> list[ActiveListingRef]:
    """Retrieves active listings projected to lightweight ActiveListingRef dataclasses.

    Evaluates the latest lifecycle state per source_id to identify currently active listings.
    """
    catalog = get_catalog()
    table_identifier = _get_table_identifier(source)

    try:
        table = catalog.load_table(table_identifier)

        # Select strictly required columns for lifecycle state evaluation
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

        # Deduplicate to find the latest known state of each property
        active_df = (
            df.sort("scraped_at")
            .unique(subset=["source_id"], keep="last")
            .filter(pl.col("is_active"))
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
