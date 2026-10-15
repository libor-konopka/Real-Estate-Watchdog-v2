import asyncio
import itertools
import socket

import aiohttp
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from src.contracts.base import SourcePortal
from src.contracts.portals.bazos import BazosInputModel
from src.scrapers.bazos_scraper import BazosScraper
from src.scrapers.reconciliation import BazosReconciler
from src.transformations.bronze import (
    get_active_bronze_properties,
    get_known_source_ids,
    save_to_iceberg,
)

SOURCE = SourcePortal.BAZOS


@asset(
    group_name="bronze_layer",
    description="Ingests raw listings from Bazos into Bronze Iceberg table and audits deleted records.",
    compute_kind="python",
)
async def bronze_bazos_listings(context: AssetExecutionContext) -> MaterializeResult:
    """Executes asynchronous scraping and reconciliation for all Bazos categories."""
    transactions = BazosInputModel.get_supported_transactions()
    entities = BazosInputModel.get_supported_entities()
    targets = list(itertools.product(transactions, entities))

    connector = aiohttp.TCPConnector(
        resolver=aiohttp.ThreadedResolver(),
        family=socket.AF_INET,
        use_dns_cache=True,
        limit=50,
    )

    total_scraped = 0
    total_deactivated = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        scraper = BazosScraper(session)
        reconciler = BazosReconciler(session)

        for transaction, entity in targets:
            tx_enum = BazosInputModel._TRANSACTION_MAP[transaction].value
            ent_enum = BazosInputModel._ENTITY_MAP[entity].value

            try:
                known_data = get_known_source_ids(SOURCE, tx_enum, ent_enum)
                context.log.info(
                    f"Scraping {SOURCE.value}:{transaction}/{entity} (Known records: {len(known_data)})"
                )

                # 1. Scrape new and updated listings
                accumulated_entities = []
                async for property_entity in scraper.scrape_section(
                    transaction, entity, known_data
                ):
                    accumulated_entities.append(property_entity)

                if accumulated_entities:
                    save_to_iceberg(accumulated_entities, source=SOURCE)
                    total_scraped += len(accumulated_entities)
                    context.log.info(
                        f"Appended {len(accumulated_entities)} items to Bronze for {transaction}/{entity}"
                    )

                # 2. Reconcile active listings (Tombstones)
                active_properties = get_active_bronze_properties(
                    SOURCE, tx_enum, ent_enum
                )
                if active_properties:
                    tombstones = await reconciler.verify_and_deactivate(
                        active_properties
                    )
                    if tombstones:
                        save_to_iceberg(tombstones, source=SOURCE)
                        total_deactivated += len(tombstones)
                        context.log.info(
                            f"Deactivated {len(tombstones)} listings for {transaction}/{entity}"
                        )

            except Exception as e:
                context.log.error(f"Failed category {transaction}/{entity}: {e}")
                continue

            await asyncio.sleep(1.0)

    # Return structured metadata directly into Dagster UI
    return MaterializeResult(
        metadata={
            "source": MetadataValue.text(SOURCE.value),
            "new_or_updated_records": MetadataValue.int(total_scraped),
            "deactivated_records": MetadataValue.int(total_deactivated),
            "target_table": MetadataValue.text(f"bronze.{SOURCE.value}_properties"),
        }
    )
