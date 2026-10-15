import polars as pl
import pyarrow as pa
from loguru import logger
from pydantic import TypeAdapter
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
    source_str = source.value if isinstance(source, SourcePortal) else source
    return f"{NAMESPACE}.{source_str}_properties"


def get_known_source_ids(
    source: SourcePortal | str, transaction: str, entity_type: str
) -> dict[str, str]:
    """Loads existing Iceberg table and returns source_id -> price mapping."""
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
    """
    if not entities:
        return

    catalog = get_catalog()
    table_identifier = _get_table_identifier(source)

    # Převod entit na serializovatelné slovníky pro PyArrow
    records = []
    for item in entities:
        if isinstance(item, dict):
            row = item
        else:
            row = item.model_dump(mode="python")

        # Úprava typů nekompatibilních s čistým from_pylist
        if "url" in row and row["url"] is not None:
            row["url"] = str(row["url"])
        if "price" in row and row["price"] is not None:
            row["price"] = float(row["price"])

        records.append(row)

    # Přímá konstrukce Arrow tabulky - bezpečně vytvoří struct null buffery i chybějící sloupce
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
