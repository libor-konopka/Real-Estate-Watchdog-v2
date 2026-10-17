import asyncio
import itertools
import socket
from typing import Any

import aiohttp
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.contracts.base import SourcePortal
from src.contracts.portals.bazos import BazosInputModel
from src.contracts.portals.bezrealitky import BezrealitkyRawInput
from src.scrapers.bazos_scraper import BazosScraper
from src.scrapers.bezrealitky_scraper import BezrealitkyScraper
from src.scrapers.reconciliation import BazosReconciler
from src.transformations.bronze import (
    get_active_bronze_properties,
    get_known_source_ids,
    save_to_iceberg,
)

SOURCE_BAZOS = SourcePortal.BAZOS
SOURCE_BEZREALITKY = SourcePortal.BEZREALITKY


def _create_http_session() -> tuple[aiohttp.TCPConnector, aiohttp.ClientTimeout]:
    """Provides tuned connection settings for reliable async scraping."""
    connector = aiohttp.TCPConnector(
        resolver=aiohttp.ThreadedResolver(),
        family=socket.AF_INET,
        use_dns_cache=True,
        limit=20,
    )
    timeout = aiohttp.ClientTimeout(total=60.0, connect=10.0, sock_read=30.0)
    return connector, timeout


@asset(
    group_name="bronze_layer",
    description="Ingests raw listings from Bazos into Bronze Iceberg table and audits deleted records.",
    compute_kind="python",
)
async def bronze_bazos_listings(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Executes asynchronous scraping and reconciliation for all Bazos categories."""
    run_id = context.run_id
    transactions = BazosInputModel.get_supported_transactions()
    entities = BazosInputModel.get_supported_entities()
    targets = list(itertools.product(transactions, entities))

    connector, timeout = _create_http_session()
    total_scraped = 0
    total_deactivated = 0

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        scraper = BazosScraper(session)
        reconciler = BazosReconciler(session)

        for transaction, entity in targets:
            tx_enum = BazosInputModel._TRANSACTION_MAP[transaction].value
            ent_enum = BazosInputModel._ENTITY_MAP[entity].value

            try:
                known_data = get_known_source_ids(SOURCE_BAZOS, tx_enum, ent_enum)
                context.log.info(
                    f"Scraping {SOURCE_BAZOS.value}:{transaction}/{entity} "
                    f"(Known records: {len(known_data)})"
                )

                # 1. Scrape new and price-updated listings
                accumulated_entities = []
                async for property_entity in scraper.scrape_section(
                    transaction, entity, known_data
                ):
                    accumulated_entities.append(property_entity)

                if accumulated_entities:
                    save_to_iceberg(
                        accumulated_entities,
                        source=SOURCE_BAZOS,
                        batch_id=run_id,
                    )
                    total_scraped += len(accumulated_entities)
                    context.log.info(
                        f"Appended {len(accumulated_entities)} items to Bronze "
                        f"for {transaction}/{entity} (Batch: {run_id})"
                    )

                # 2. Reconcile active listings (Tombstones for deleted ads)
                active_properties = get_active_bronze_properties(
                    SOURCE_BAZOS, tx_enum, ent_enum
                )
                if active_properties:
                    tombstones: list[
                        dict[str, Any]
                    ] = await reconciler.verify_and_deactivate(
                        active_records=active_properties,
                        transaction=tx_enum,
                        entity_type=ent_enum,
                    )
                    if tombstones:
                        save_to_iceberg(
                            tombstones,
                            source=SOURCE_BAZOS,
                            batch_id=run_id,
                        )
                        total_deactivated += len(tombstones)
                        context.log.info(
                            f"Deactivated {len(tombstones)} listings "
                            f"for {transaction}/{entity} (Batch: {run_id})"
                        )

            except Exception as e:
                context.log.error(f"Failed category {transaction}/{entity}: {e}")
                continue

            await asyncio.sleep(1.0)

    return MaterializeResult(
        metadata={
            "source": MetadataValue.text(SOURCE_BAZOS.value),
            "batch_id": MetadataValue.text(run_id),
            "new_or_updated_records": MetadataValue.int(total_scraped),
            "deactivated_records": MetadataValue.int(total_deactivated),
            "target_table": MetadataValue.text(
                f"bronze.{SOURCE_BAZOS.value}_properties"
            ),
        }
    )


@asset(
    group_name="bronze_layer",
    description="Ingests raw listings from Bezrealitky GraphQL API into Bronze Iceberg table.",
    compute_kind="python",
)
async def bronze_bezrealitky_listings(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Executes asynchronous scraping for all supported Bezrealitky categories."""
    run_id = context.run_id
    transactions = BezrealitkyRawInput.get_supported_transactions()
    entities = BezrealitkyRawInput.get_supported_entities()
    targets = list(itertools.product(transactions, entities))

    connector, timeout = _create_http_session()
    total_scraped = 0

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        scraper = BezrealitkyScraper(session)

        for transaction, entity in targets:
            # Resolve canonical domain enum values from mapping dictionaries
            api_tx = BezrealitkyRawInput.API_TRANSACTION_MAP[transaction]
            tx_enum = BezrealitkyRawInput._TRANSACTION_MAP[api_tx].value

            api_ent = BezrealitkyRawInput.API_ENTITY_MAP[entity]
            ent_enum = BezrealitkyRawInput._ENTITY_MAP[api_ent].value

            try:
                known_data = get_known_source_ids(SOURCE_BEZREALITKY, tx_enum, ent_enum)
                context.log.info(
                    f"Scraping {SOURCE_BEZREALITKY.value}:{transaction}/{entity} "
                    f"(Known records: {len(known_data)})"
                )

                accumulated_entities = []
                async for property_entity in scraper.scrape_section(
                    transaction=transaction,
                    entity=entity,
                    known_data=known_data,
                ):
                    accumulated_entities.append(property_entity)

                if accumulated_entities:
                    save_to_iceberg(
                        accumulated_entities,
                        source=SOURCE_BEZREALITKY,
                        batch_id=run_id,
                    )
                    total_scraped += len(accumulated_entities)
                    context.log.info(
                        f"Appended {len(accumulated_entities)} items to Bronze "
                        f"for {transaction}/{entity} (Batch: {run_id})"
                    )

            except Exception as e:
                context.log.error(
                    f"Failed Bezrealitky category {transaction}/{entity}: {e}"
                )
                continue

            await asyncio.sleep(0.5)

    return MaterializeResult(
        metadata={
            "source": MetadataValue.text(SOURCE_BEZREALITKY.value),
            "batch_id": MetadataValue.text(run_id),
            "new_or_updated_records": MetadataValue.int(total_scraped),
            "target_table": MetadataValue.text(
                f"bronze.{SOURCE_BEZREALITKY.value}_properties"
            ),
        }
    )
