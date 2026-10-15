import asyncio
import itertools
import socket
import sys

import aiohttp
from loguru import logger

from src.contracts.base import SourcePortal
from src.contracts.portals.bazos import BazosInputModel
from src.scrapers.bazos_scraper import BazosScraper
from src.scrapers.reconciliation import BazosReconciler
from src.transformations.bronze import (
    get_active_bronze_properties,
    get_known_source_ids,
    save_to_iceberg,
)
from src.transformations.silver import process_silver

SOURCE = SourcePortal.BAZOS


def setup_logger() -> None:
    """
    Establishes two independent information streams.
    Terminal displays essential system status. File captures complete debug traces.
    """
    logger.remove()

    logger.add(
        sys.stdout,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>",
    )

    logger.add(
        "logs/scraper.log",
        level="DEBUG",
        retention="7 days",
        rotation="10 MB",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    )


async def run_pipeline() -> None:
    """
    Initiates the main ELT pipeline for Bazos.
    Maintains a persistent network session across all sections,
    extracts active listings, audits deleted entities, and updates Bronze & Silver layers.
    """
    setup_logger()
    logger.info("Starting real estate ingestion pipeline for Bazos...")

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

    # Single persistent session for the entire pipeline execution
    async with aiohttp.ClientSession(connector=connector) as session:
        scraper = BazosScraper(session)
        reconciler = BazosReconciler(session)

        for transaction, entity in targets:
            try:
                tx_enum = BazosInputModel._TRANSACTION_MAP[transaction].value
                ent_enum = BazosInputModel._ENTITY_MAP[entity].value

                known_data = get_known_source_ids(SOURCE, tx_enum, ent_enum)
                logger.info(
                    f"Scraping category {SOURCE.value}:{transaction}/{entity} "
                    f"(Known records: {len(known_data)})"
                )

                # 1. Scrape new and updated listings
                accumulated_entities = []
                async for property_entity in scraper.scrape_section(
                    transaction, entity, known_data
                ):
                    accumulated_entities.append(property_entity)
                    logger.debug(
                        f"Captured {property_entity.source_id} | "
                        f"Price: {property_entity.price} | {property_entity.url}"
                    )

                # Materialize raw listings into Bronze Iceberg table
                if accumulated_entities:
                    save_to_iceberg(accumulated_entities, source=SOURCE)
                    total_scraped += len(accumulated_entities)
                    logger.info(
                        f"Saved {len(accumulated_entities)} new/updated listings for {transaction}/{entity}"
                    )

                # 2. Reconcile active listings and flag deleted items (tombstones)
                active_properties = get_active_bronze_properties(
                    SOURCE, tx_enum, ent_enum
                )

                if active_properties:
                    logger.info(
                        f"Auditing {len(active_properties)} active records for {transaction}/{entity}..."
                    )
                    tombstones = await reconciler.verify_and_deactivate(
                        active_properties
                    )

                    if tombstones:
                        save_to_iceberg(tombstones, source=SOURCE)
                        total_deactivated += len(tombstones)
                        logger.info(
                            f"Deactivated {len(tombstones)} removed listings for {transaction}/{entity}"
                        )

            except Exception as e:
                logger.error(
                    f"Error processing category {transaction}/{entity}: {e}",
                    exc_info=True,
                )
                continue

            await asyncio.sleep(1.0)

    # 3. Silver Layer Transformation
    # Run once at the end of the entire extraction process
    if total_scraped > 0 or total_deactivated > 0:
        logger.info(
            f"Extraction complete (New/Updated: {total_scraped}, Deactivated: {total_deactivated}). "
            "Running Silver transformation..."
        )
        process_silver(source=SOURCE)
        logger.info("Silver layer processing completed successfully.")
    else:
        logger.info(
            "No updates detected across any section. Silver processing skipped."
        )


if __name__ == "__main__":
    try:
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        logger.warning("Pipeline execution interrupted by user.")
    except Exception as e:
        logger.critical(f"Fatal error in pipeline execution: {e}", exc_info=True)
        sys.exit(1)
