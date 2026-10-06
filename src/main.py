import asyncio
import itertools
import socket
import sys
from pathlib import Path

import aiohttp
from loguru import logger

from src.contracts.portals.bazos import BazosRawInput
from src.scrapers.bazos_scraper import BazosScraper
from src.transformations.bronze import get_known_source_ids, save_to_iceberg
from src.transformations.silver import process_silver


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
    Initiates the main data flow. Maintains a global network session and
    routes the extracted entities for further processing.
    """
    setup_logger()

    transactions = BazosRawInput.get_supported_transactions()
    entities = BazosRawInput.get_supported_entities()
    targets = list(itertools.product(transactions, entities))

    resolver = aiohttp.ThreadedResolver()
    connector = aiohttp.TCPConnector(
        resolver=resolver, family=socket.AF_INET, use_dns_cache=True, limit=50
    )

    project_root = Path(__file__).resolve().parent.parent

    async with aiohttp.ClientSession(connector=connector) as session:
        scraper = BazosScraper(session)

        for transaction, entity in targets:
            try:
                known_data = get_known_source_ids(transaction, entity)
                logger.info(
                    f"Data extraction initiated for Bazos: {transaction}/{entity} "
                    f"(Known records: {len(known_data)})"
                )

                accumulated_entities = []

                async for property_entity in scraper.scrape_section(
                    transaction, entity, known_data
                ):
                    accumulated_entities.append(property_entity)
                    logger.debug(
                        f"Entity: {property_entity.id} | Price: {property_entity.price} | URL: {property_entity.url}"
                    )

                # Bronze layer materialization
                save_to_iceberg(accumulated_entities, transaction, entity)

                # Silver layer transformations
                process_silver(transaction, entity)

                logger.info(
                    f"Successfully extracted and transformed {len(accumulated_entities)} NEW/UPDATED listings for: {transaction} {entity}"
                )

            except Exception as e:
                logger.error(
                    f"Pipeline failed for category {transaction}/{entity}: {e}"
                )
                continue


if __name__ == "__main__":
    try:
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        logger.info("The data flow was manually interrupted.")
    except Exception as e:
        # Fallback rescue from an uncontrolled application crash at the system level
        logger.critical(f"Fatal error in main data flow: {e}")
