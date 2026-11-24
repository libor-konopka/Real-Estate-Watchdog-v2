import asyncio
import itertools
import socket
import sys
import uuid

import aiohttp
from loguru import logger
from pyiceberg.exceptions import RESTError

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
from src.transformations.gold import process_gold
from src.transformations.silver import process_silver


def setup_logger() -> None:
    """Establishes structured console and rotating file log sinks."""
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


async def _ingest_bazos(
    session: aiohttp.ClientSession, batch_id: str
) -> tuple[int, int]:
    """Runs incremental ingestion and tombstone reconciliation for Bazos."""
    source = SourcePortal.BAZOS
    scraper = BazosScraper(session)
    reconciler = BazosReconciler(session)

    targets = list(
        itertools.product(
            BazosInputModel.get_supported_transactions(),
            BazosInputModel.get_supported_entities(),
        )
    )

    scraped_count = 0
    deactivated_count = 0

    for transaction, entity in targets:
        tx_enum = BazosInputModel._TRANSACTION_MAP[transaction].value
        ent_enum = BazosInputModel._ENTITY_MAP[entity].value

        try:
            known_data = get_known_source_ids(source, tx_enum, ent_enum)
            logger.info(
                f"[{source.value}] Scraping {transaction}/{entity} (known: {len(known_data)})"
            )

            entities = [
                item
                async for item in scraper.scrape_section(
                    transaction, entity, known_data
                )
            ]
            if entities:
                save_to_iceberg(entities, source=source, batch_id=batch_id)
                scraped_count += len(entities)

            active_props = get_active_bronze_properties(source, tx_enum, ent_enum)
            if active_props:
                tombstones = await reconciler.verify_and_deactivate(
                    active_records=active_props,
                    transaction=tx_enum,
                    entity_type=ent_enum,
                )
                if tombstones:
                    save_to_iceberg(tombstones, source=source, batch_id=batch_id)
                    deactivated_count += len(tombstones)

        except (
            aiohttp.ClientError,
            RESTError,
            ValueError,
            RuntimeError,
            OSError,
        ) as exc:
            logger.error(f"[{source.value}] Error in {transaction}/{entity}: {exc}")

        await asyncio.sleep(0.5)

    return scraped_count, deactivated_count


async def _ingest_bezrealitky(session: aiohttp.ClientSession, batch_id: str) -> int:
    """Runs incremental GraphQL API ingestion for Bezrealitky."""
    source = SourcePortal.BEZREALITKY
    scraper = BezrealitkyScraper(session)

    targets = list(
        itertools.product(
            BezrealitkyRawInput.get_supported_transactions(),
            BezrealitkyRawInput.get_supported_entities(),
        )
    )

    scraped_count = 0

    for transaction, entity in targets:
        api_tx = BezrealitkyRawInput.API_TRANSACTION_MAP[transaction]
        tx_enum = BezrealitkyRawInput._TRANSACTION_MAP[api_tx].value
        api_ent = BezrealitkyRawInput.API_ENTITY_MAP[entity]
        ent_enum = BezrealitkyRawInput._ENTITY_MAP[api_ent].value

        try:
            known_data = get_known_source_ids(source, tx_enum, ent_enum)
            logger.info(
                f"[{source.value}] Scraping {transaction}/{entity} (known: {len(known_data)})"
            )

            entities = [
                item
                async for item in scraper.scrape_section(
                    transaction=transaction,
                    entity=entity,
                    known_source_ids=set(known_data.keys()),
                )
            ]
            if entities:
                save_to_iceberg(entities, source=source, batch_id=batch_id)
                scraped_count += len(entities)

        except (
            aiohttp.ClientError,
            RESTError,
            ValueError,
            RuntimeError,
            OSError,
        ) as exc:
            logger.error(f"[{source.value}] Error in {transaction}/{entity}: {exc}")

        await asyncio.sleep(0.5)

    return scraped_count


async def run_pipeline() -> None:
    """Executes full end-to-end Medallion ELT pipeline (Bronze -> Silver -> Gold)."""
    setup_logger()
    batch_id = str(uuid.uuid4())
    logger.info(f"Starting Real-Estate Watchdog ELT pipeline (batch={batch_id})...")

    connector = aiohttp.TCPConnector(
        resolver=aiohttp.ThreadedResolver(),
        family=socket.AF_INET,
        use_dns_cache=True,
        limit=30,
    )
    timeout = aiohttp.ClientTimeout(total=60.0, connect=10.0, sock_read=30.0)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        bazos_new, bazos_deactivated = await _ingest_bazos(session, batch_id)
        bezrealitky_new = await _ingest_bezrealitky(session, batch_id)

    logger.info(
        f"Bronze ingestion finished | Bazos: +{bazos_new} / -{bazos_deactivated} | "
        f"Bezrealitky: +{bezrealitky_new}"
    )

    logger.info("Materializing unified Silver layer (silver.properties)...")
    silver_table = process_silver()
    if silver_table is None:
        logger.warning("Silver layer returned empty dataset. Gold layer skipped.")
        return

    logger.info("Materializing Gold analytical marts...")
    process_gold()
    logger.success("End-to-end Medallion ELT pipeline completed.")


if __name__ == "__main__":
    try:
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        logger.warning("Pipeline execution interrupted by user.")
    except (
        aiohttp.ClientError,
        RESTError,
        ValueError,
        RuntimeError,
        OSError,
    ) as exc:
        logger.critical(f"Fatal error in pipeline execution: {exc}", exc_info=True)
        sys.exit(1)
