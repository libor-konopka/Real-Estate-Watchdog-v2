import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, cast

import polars as pl
import pyarrow as pa
from loguru import logger
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

from src.contracts.base import (
    ActiveListingRef,
    BuildingProperty,
    LandProperty,
    SourcePortal,
)
from src.contracts.schemas import BRONZE_PROPERTY_ARROW_SCHEMA
from src.storage.catalog import get_catalog_manager

BronzeInputRecord = BuildingProperty | LandProperty | dict[str, Any]


def _serialize_domain_entity(
    prop: BuildingProperty | LandProperty,
    batch_id: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    """Converts a Pydantic domain property into a nested dict matching BRONZE_PROPERTY_ARROW_SCHEMA."""
    location_struct = {
        "region": prop.location.region if prop.location else None,
        "district": prop.location.district if prop.location else None,
        "municipality": prop.location.municipality if prop.location else None,
        "postal_code": prop.location.postal_code if prop.location else None,
        "gps": (
            {
                "lat": prop.location.gps.lat,
                "lon": prop.location.gps.lon,
            }
            if prop.location and prop.location.gps
            else None
        ),
    }

    infra_struct = (
        {
            "water": prop.infrastructure.water.value
            if prop.infrastructure.water
            else None,
            "electricity": prop.infrastructure.electricity,
            "waste": prop.infrastructure.waste.value
            if prop.infrastructure.waste
            else None,
            "heating": prop.infrastructure.heating.value
            if prop.infrastructure.heating
            else None,
            "gas": prop.infrastructure.gas,
        }
        if prop.infrastructure
        else None
    )

    legal_struct = (
        {
            "ownership": prop.legal.ownership.value if prop.legal.ownership else None,
            "price_note": prop.legal.price_note,
            "available_from": prop.legal.available_from,
            "is_foreclosure": prop.legal.is_foreclosure,
            "is_insolvency": prop.legal.is_insolvency,
            "is_share": prop.legal.is_share,
        }
        if prop.legal
        else None
    )

    return {
        "batch_id": batch_id,
        "ingested_at": ingested_at,
        "id": prop.id,
        "title": prop.title,
        "is_active": prop.is_active,
        "source_id": prop.source_id,
        "url": str(prop.url),
        "source": prop.source.value,
        "transaction_type": prop.transaction_type.value,
        "entity_type": prop.entity_type.value,
        "price": float(prop.price) if prop.price is not None else None,
        "currency": prop.currency.value if prop.currency else None,
        "scraped_at": prop.scraped_at,
        "location": location_struct,
        "infrastructure": infra_struct,
        "legal": legal_struct,
        "land_area_m2": prop.land_area_m2,
        "usable_area_m2": prop.usable_area_m2,
        "built_up_area_m2": (
            prop.built_up_area_m2 if isinstance(prop, BuildingProperty) else None
        ),
        "material": (
            prop.material.value
            if isinstance(prop, BuildingProperty) and prop.material
            else None
        ),
        "condition": (
            prop.condition.value
            if isinstance(prop, BuildingProperty) and prop.condition
            else None
        ),
        "energy_class": (
            prop.energy_class.value
            if isinstance(prop, BuildingProperty) and prop.energy_class
            else None
        ),
        "land_category": (
            prop.land_category if isinstance(prop, LandProperty) else None
        ),
        "location_character": (
            prop.location_character if isinstance(prop, LandProperty) else None
        ),
        "access_road": (prop.access_road if isinstance(prop, LandProperty) else None),
        "transport": (prop.transport if isinstance(prop, LandProperty) else None),
        "description": prop.description,
    }


def _serialize_tombstone_dict(
    raw: dict[str, Any],
    batch_id: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    """Normalizes minimal tombstone dictionaries emitted by reconcilers to full Bronze schema."""
    return {
        "batch_id": batch_id,
        "ingested_at": ingested_at,
        "id": str(raw["id"]),
        "title": str(raw.get("title", "[DEACTIVATED]")),
        "is_active": bool(raw.get("is_active", False)),
        "source_id": str(raw["source_id"]),
        "url": str(raw["url"]),
        "source": str(raw["source"]),
        "transaction_type": str(raw["transaction_type"]),
        "entity_type": str(raw["entity_type"]),
        "price": float(raw["price"]) if raw.get("price") is not None else None,
        "currency": raw.get("currency"),
        "scraped_at": raw.get("scraped_at", ingested_at),
        "location": raw.get("location")
        or {
            "region": None,
            "district": None,
            "municipality": None,
            "postal_code": None,
            "gps": None,
        },
        "infrastructure": raw.get("infrastructure"),
        "legal": raw.get("legal"),
        "land_area_m2": raw.get("land_area_m2"),
        "usable_area_m2": raw.get("usable_area_m2"),
        "built_up_area_m2": raw.get("built_up_area_m2"),
        "material": raw.get("material"),
        "condition": raw.get("condition"),
        "energy_class": raw.get("energy_class"),
        "land_category": raw.get("land_category"),
        "location_character": raw.get("location_character"),
        "access_road": raw.get("access_road"),
        "transport": raw.get("transport"),
        "description": raw.get("description"),
    }


def properties_to_arrow_table(
    records: Sequence[BronzeInputRecord],
    ingestion_batch_id: str,
) -> pa.Table:
    """Converts domain property models or tombstone dicts into a strictly-typed PyArrow Table."""
    if not records:
        return pa.Table.from_batches([], schema=BRONZE_PROPERTY_ARROW_SCHEMA)

    current_utc = datetime.now(UTC)
    serialized: list[dict[str, Any]] = []

    for item in records:
        if isinstance(item, (BuildingProperty, LandProperty)):
            serialized.append(
                _serialize_domain_entity(item, ingestion_batch_id, current_utc)
            )
        else:
            serialized.append(
                _serialize_tombstone_dict(item, ingestion_batch_id, current_utc)
            )

    return pa.Table.from_pylist(serialized, schema=BRONZE_PROPERTY_ARROW_SCHEMA)


def save_to_iceberg(
    records: Sequence[BronzeInputRecord],
    source: SourcePortal,
    batch_id: str | None = None,
) -> int:
    """Appends a batch of raw listings or tombstones to the portal's Bronze Iceberg table."""
    if not records:
        return 0

    resolved_batch_id = batch_id or str(uuid.uuid4())
    arrow_table = properties_to_arrow_table(records, resolved_batch_id)

    manager = get_catalog_manager()
    table = manager.get_or_create_bronze_table(source)
    table.append(arrow_table)

    logger.debug(
        f"Appended {arrow_table.num_rows} rows to bronze.{source.value}_properties "
        f"(batch={resolved_batch_id})."
    )
    return arrow_table.num_rows


def _load_latest_bronze_slice(
    source: SourcePortal,
    transaction_type: str,
    entity_type: str,
) -> pl.DataFrame:
    """Reads the latest snapshot per source_id for a given portal, transaction, and entity."""
    manager = get_catalog_manager()
    catalog = manager.get_catalog()
    table_id = f"{manager.BRONZE_NAMESPACE}.{source.value}_properties"

    try:
        table = catalog.load_table(table_id)
    except NoSuchTableError, NoSuchNamespaceError:
        return pl.DataFrame()

    df = cast(
        pl.DataFrame,
        pl.scan_iceberg(table)
        .filter(
            (pl.col("transaction_type") == transaction_type)
            & (pl.col("entity_type") == entity_type)
        )
        .select(["source_id", "url", "price", "is_active", "scraped_at"])
        .collect(),
    )

    if df.is_empty():
        return df

    return df.sort("scraped_at").unique(subset=["source_id"], keep="last")


def get_known_source_ids(
    source: SourcePortal,
    transaction_type: str,
    entity_type: str,
) -> dict[str, float | None]:
    """Returns mapping of known active source_id -> latest price for incremental scraping."""
    latest_df = _load_latest_bronze_slice(source, transaction_type, entity_type)
    if latest_df.is_empty():
        return {}

    active_df = latest_df.filter(pl.col("is_active"))
    return {
        row["source_id"]: row["price"]
        for row in active_df.select(["source_id", "price"]).to_dicts()
    }


def get_active_bronze_properties(
    source: SourcePortal,
    transaction_type: str,
    entity_type: str,
) -> list[ActiveListingRef]:
    """Returns lightweight references of currently active listings for HTTP reconciliation."""
    latest_df = _load_latest_bronze_slice(source, transaction_type, entity_type)
    if latest_df.is_empty():
        return []

    active_df = latest_df.filter(pl.col("is_active"))
    return [
        ActiveListingRef(source_id=row["source_id"], url=row["url"])
        for row in active_df.select(["source_id", "url"]).to_dicts()
    ]
