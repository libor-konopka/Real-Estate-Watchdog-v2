import asyncio
import itertools
import socket
import uuid
from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, Protocol, cast

import aiohttp
import polars as pl
import pyarrow as pa
from loguru import logger
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

from src.contracts.base import (
    ActiveListingRef,
    BuildingProperty,
    EntityType,
    LandProperty,
    SourcePortal,
    TransactionType,
)
from src.contracts.portals.bazos import BazosInputModel
from src.contracts.portals.bezrealitky import BezrealitkyRawInput
from src.contracts.schemas import BRONZE_PROPERTY_ARROW_SCHEMA
from src.scrapers.bazos_scraper import BazosScraper
from src.scrapers.bezrealitky_scraper import BezrealitkyScraper
from src.scrapers.reconciliation import BazosReconciler, BezrealitkyReconciler
from src.storage.catalog import get_catalog_manager

BronzeInputRecord = BuildingProperty | LandProperty | dict[str, Any]

_REQUIRED_TOMBSTONE_KEYS: frozenset[str] = frozenset(
    {"id", "source_id", "url", "source", "transaction_type", "entity_type"}
)


class IngestionLogger(Protocol):
    """Structural logging protocol compatible with Loguru and Dagster loggers."""

    def info(self, msg: str) -> None: ...
    def error(self, msg: str) -> None: ...


def create_http_session_params() -> tuple[aiohttp.TCPConnector, aiohttp.ClientTimeout]:
    """Provides tuned TCPConnector and timeout settings for async scraping."""
    connector = aiohttp.TCPConnector(
        resolver=aiohttp.ThreadedResolver(),
        family=socket.AF_INET,
        use_dns_cache=True,
        limit=30,
        limit_per_host=10,
    )
    timeout = aiohttp.ClientTimeout(total=60.0, connect=10.0, sock_read=30.0)
    return connector, timeout


def _serialize_domain_entity(
    prop: BuildingProperty | LandProperty,
    batch_id: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    """Converts a Pydantic domain property into a dict matching Bronze schema."""
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
    """Normalizes tombstone dicts to the full Bronze Arrow schema."""
    missing = _REQUIRED_TOMBSTONE_KEYS - raw.keys()
    if missing:
        raise ValueError(f"Tombstone record missing required fields: {sorted(missing)}")

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
    """Converts domain property models or tombstone dicts into a PyArrow Table."""
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
    """Appends a batch of raw listings or tombstones to the Bronze Iceberg table."""
    if not records:
        return 0

    resolved_batch_id = batch_id or str(uuid.uuid4())
    arrow_table = properties_to_arrow_table(records, resolved_batch_id)

    manager = get_catalog_manager()
    table = manager.get_or_create_bronze_table(source)
    table.append(arrow_table)

    logger.debug(
        f"Appended {arrow_table.num_rows} rows to {manager.bronze_table_id(source)} "
        f"(batch={resolved_batch_id})."
    )
    return int(arrow_table.num_rows)


def _load_latest_bronze_slice(
    source: SourcePortal,
    transaction_type: str,
    entity_type: str,
) -> pl.DataFrame:
    """Reads the latest snapshot per source_id for a portal, transaction, and entity."""
    manager = get_catalog_manager()
    catalog = manager.get_catalog()
    table_id = manager.bronze_table_id(source)

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
        .select(["source_id", "url", "price", "is_active", "scraped_at", "ingested_at"])
        .collect(),
    )

    if df.is_empty():
        return df

    return df.sort(["scraped_at", "ingested_at"]).unique(
        subset=["source_id"], keep="last", maintain_order=True
    )


def _get_active_bronze_state(
    source: SourcePortal,
    transaction_type: str,
    entity_type: str,
) -> tuple[dict[str, float | None], list[ActiveListingRef]]:
    """Returns known active prices and reconciliation refs in a single Bronze scan."""
    latest_df = _load_latest_bronze_slice(source, transaction_type, entity_type)
    if latest_df.is_empty():
        return {}, []

    active_df = latest_df.filter(pl.col("is_active"))
    rows = active_df.select(["source_id", "url", "price"]).to_dicts()

    known_data = {row["source_id"]: row["price"] for row in rows}
    active_refs = [
        ActiveListingRef(source_id=row["source_id"], url=row["url"]) for row in rows
    ]
    return known_data, active_refs


def get_known_source_ids(
    source: SourcePortal,
    transaction_type: str,
    entity_type: str,
) -> dict[str, float | None]:
    """Returns mapping of known active source_id -> latest price for scraping."""
    known_data, _ = _get_active_bronze_state(source, transaction_type, entity_type)
    return known_data


def get_active_bronze_properties(
    source: SourcePortal,
    transaction_type: str,
    entity_type: str,
) -> list[ActiveListingRef]:
    """Returns lightweight references of active listings for HTTP reconciliation."""
    _, active_refs = _get_active_bronze_state(source, transaction_type, entity_type)
    return active_refs


async def ingest_bazos_portal(
    session: aiohttp.ClientSession,
    batch_id: str,
    log: IngestionLogger | None = None,
) -> tuple[int, int]:
    """Runs incremental ingestion and tombstone reconciliation for Bazos."""
    active_log = log or logger
    source = SourcePortal.BAZOS
    scraper = BazosScraper(session)
    reconciler = BazosReconciler(session)

    grouped_targets: dict[tuple[TransactionType, EntityType], list[tuple[str, str]]] = (
        defaultdict(list)
    )
    for transaction, entity in itertools.product(
        BazosInputModel.get_supported_transactions(),
        BazosInputModel.get_supported_entities(),
    ):
        tx_enum, ent_enum = BazosInputModel.resolve_enums(transaction, entity)
        grouped_targets[(tx_enum, ent_enum)].append((transaction, entity))

    scraped_count = 0
    deactivated_count = 0
    total_slugs = sum(len(slugs) for slugs in grouped_targets.values())
    failed_slugs = 0

    for (tx_enum, ent_enum), slug_pairs in grouped_targets.items():
        known_data, active_props = await asyncio.to_thread(
            _get_active_bronze_state, source, tx_enum.value, ent_enum.value
        )
        seen_ids: set[str] = set()
        group_had_errors = False

        for transaction, entity in slug_pairs:
            try:
                active_log.info(
                    f"[{source.value}] Scraping {transaction}/{entity} "
                    f"(known: {len(known_data)})"
                )

                entities = [
                    item
                    async for item in scraper.scrape_section(
                        transaction=transaction,
                        entity=entity,
                        known_data=known_data,
                        seen_ids=seen_ids,
                    )
                ]
                if entities:
                    await asyncio.to_thread(save_to_iceberg, entities, source, batch_id)
                    scraped_count += len(entities)
                    for item in entities:
                        known_data[item.source_id] = item.price
                    active_log.info(
                        f"[{source.value}] Appended {len(entities)} items "
                        f"for {transaction}/{entity} (batch={batch_id})"
                    )

            except (
                aiohttp.ClientError,
                ValueError,
                RuntimeError,
                OSError,
            ) as exc:
                group_had_errors = True
                failed_slugs += 1
                active_log.error(
                    f"[{source.value}] Error in {transaction}/{entity}: {exc}"
                )

            await asyncio.sleep(0.5)

        props_to_reconcile = [
            ref for ref in active_props if ref.source_id not in seen_ids
        ]
        if props_to_reconcile and not group_had_errors:
            tombstones = await reconciler.verify_and_deactivate(
                active_records=props_to_reconcile,
                transaction=tx_enum.value,
                entity_type=ent_enum.value,
            )
            if tombstones:
                await asyncio.to_thread(save_to_iceberg, tombstones, source, batch_id)
                deactivated_count += len(tombstones)
                active_log.info(
                    f"[{source.value}] Deactivated {len(tombstones)} listings "
                    f"for {tx_enum.value}/{ent_enum.value} (batch={batch_id})"
                )

    if total_slugs > 0 and failed_slugs == total_slugs:
        raise RuntimeError(
            f"[{source.value}] All {total_slugs} scraping targets failed."
        )

    return scraped_count, deactivated_count


async def ingest_bezrealitky_portal(
    session: aiohttp.ClientSession,
    batch_id: str,
    log: IngestionLogger | None = None,
) -> tuple[int, int]:
    """Runs incremental GraphQL API ingestion and reconciliation for Bezrealitky."""
    active_log = log or logger
    source = SourcePortal.BEZREALITKY
    scraper = BezrealitkyScraper(session)
    reconciler = BezrealitkyReconciler(session)

    grouped_targets: dict[tuple[TransactionType, EntityType], list[tuple[str, str]]] = (
        defaultdict(list)
    )
    for transaction, entity in itertools.product(
        BezrealitkyRawInput.get_supported_transactions(),
        BezrealitkyRawInput.get_supported_entities(),
    ):
        tx_enum, ent_enum = BezrealitkyRawInput.resolve_enums(transaction, entity)
        grouped_targets[(tx_enum, ent_enum)].append((transaction, entity))

    scraped_count = 0
    deactivated_count = 0
    total_slugs = sum(len(slugs) for slugs in grouped_targets.values())
    failed_slugs = 0

    for (tx_enum, ent_enum), slug_pairs in grouped_targets.items():
        known_data, active_props = await asyncio.to_thread(
            _get_active_bronze_state, source, tx_enum.value, ent_enum.value
        )
        seen_ids: set[str] = set()
        group_had_errors = False

        for transaction, entity in slug_pairs:
            try:
                active_log.info(
                    f"[{source.value}] Scraping {transaction}/{entity} "
                    f"(known: {len(known_data)})"
                )

                entities = [
                    item
                    async for item in scraper.scrape_section(
                        transaction=transaction,
                        entity=entity,
                        known_data=known_data,
                        seen_ids=seen_ids,
                    )
                ]
                if entities:
                    await asyncio.to_thread(save_to_iceberg, entities, source, batch_id)
                    scraped_count += len(entities)
                    for item in entities:
                        known_data[item.source_id] = item.price
                    active_log.info(
                        f"[{source.value}] Appended {len(entities)} items "
                        f"for {transaction}/{entity} (batch={batch_id})"
                    )

            except (
                aiohttp.ClientError,
                ValueError,
                RuntimeError,
                OSError,
            ) as exc:
                group_had_errors = True
                failed_slugs += 1
                active_log.error(
                    f"[{source.value}] Error in {transaction}/{entity}: {exc}"
                )

            await asyncio.sleep(0.5)

        props_to_reconcile = [
            ref for ref in active_props if ref.source_id not in seen_ids
        ]
        if props_to_reconcile and not group_had_errors:
            tombstones = await reconciler.verify_and_deactivate(
                active_records=props_to_reconcile,
                transaction=tx_enum.value,
                entity_type=ent_enum.value,
            )
            if tombstones:
                await asyncio.to_thread(save_to_iceberg, tombstones, source, batch_id)
                deactivated_count += len(tombstones)
                active_log.info(
                    f"[{source.value}] Deactivated {len(tombstones)} listings "
                    f"for {tx_enum.value}/{ent_enum.value} (batch={batch_id})"
                )

    if total_slugs > 0 and failed_slugs == total_slugs:
        raise RuntimeError(
            f"[{source.value}] All {total_slugs} scraping targets failed."
        )

    return scraped_count, deactivated_count
