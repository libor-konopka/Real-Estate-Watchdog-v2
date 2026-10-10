import asyncio
import http.client
import itertools
import logging
import socket
import sys
from pathlib import Path

import aiohttp
from loguru import logger

from src.contracts.portals.bazos import BazosRawInput
from src.scrapers.bazos_scraper import BazosScraper
from src.scrapers.reconciliation import BazosReconciler
from src.transformations.bronze import (
    get_active_bronze_properties,
    get_known_source_ids,
    save_to_iceberg,
)
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

    project_root = Path(__file__).resolve().parent.parent

    for transaction, entity in targets:
        try:
            resolver = aiohttp.ThreadedResolver()
            connector = aiohttp.TCPConnector(
                resolver=resolver, family=socket.AF_INET, use_dns_cache=True, limit=50
            )

            async with aiohttp.ClientSession(connector=connector) as session:
                scraper = BazosScraper(session)

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

                # ---------------------------------------------------------
                # NOVÝ KROK: AUDIT A DEAKTIVACE SMAZANÝCH INZERÁTŮ
                # ---------------------------------------------------------
                logger.info(
                    f"Zahajuji audit existence inzerátů na portálu pro: {transaction}/{entity}..."
                )

                # 1. Načteme všechny, o kterých si myslíme, že jsou aktivní
                active_properties = get_active_bronze_properties(transaction, entity)

                if active_properties:
                    # 2. Asynchronně je zkontrolujeme
                    reconciler = BazosReconciler(session)
                    tombstones = await reconciler.verify_and_deactivate(
                        active_properties
                    )

                    if tombstones:
                        # 3. Vytvořené neaktivní kopie přilepíme do historie (Bronze)
                        save_to_iceberg(tombstones, transaction, entity)

                        # 4. Spustíme znovu promítnutí do analytické vrstvy (Silver)
                        # Deduplikační krok keep="last" nahradí starý True stav novým False stavem
                        process_silver(transaction, entity)
                        logger.info(
                            f"Úspěšně deaktivováno {len(tombstones)} odstraněných inzerátů."
                        )
                # ---------------------------------------------------------

        except Exception as e:
            logger.error(f"Pipeline failed for category {transaction}/{entity}: {e}")
            continue

        # 2. Krátká pauza mezi kategoriemi (po uzavření session) pro snížení zátěže
        logger.info("Vyčkávám 2 sekundy před zahájením další kategorie...")
        await asyncio.sleep(2)


if __name__ == "__main__":
    try:
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        logger.info("The data flow was manually interrupted.")
    except Exception as e:
        logger.critical(f"Fatal error in main data flow: {e}")
