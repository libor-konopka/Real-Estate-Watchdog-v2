import asyncio
import sys
import uuid

import aiohttp
from loguru import logger
from pyiceberg.exceptions import RESTError

from src.transformations.bronze import (
    create_http_session_params,
    ingest_bazos_portal,
    ingest_bezrealitky_portal,
)
from src.transformations.gold import process_gold
from src.transformations.silver import process_silver

PIPELINE_ERRORS = (
    aiohttp.ClientError,
    RESTError,
    ValueError,
    RuntimeError,
    OSError,
)


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
        format=(
            "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
            "{name}:{function}:{line} - {message}"
        ),
    )


async def run_pipeline() -> None:
    """Executes full end-to-end Medallion ELT pipeline (Bronze -> Silver -> Gold)."""
    setup_logger()
    batch_id = str(uuid.uuid4())
    logger.info(f"Starting Real-Estate Watchdog ELT pipeline (batch={batch_id})...")

    connector, timeout = create_http_session_params()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        (
            (bazos_new, bazos_deactivated),
            (bezrealitky_new, bezrealitky_deactivated),
        ) = await asyncio.gather(
            ingest_bazos_portal(session, batch_id),
            ingest_bezrealitky_portal(session, batch_id),
        )

    logger.info(
        f"Bronze ingestion finished | Bazos: +{bazos_new} / -{bazos_deactivated} | "
        f"Bezrealitky: +{bezrealitky_new} / -{bezrealitky_deactivated}"
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
    except PIPELINE_ERRORS as exc:
        logger.opt(exception=True).critical(f"Fatal error in pipeline execution: {exc}")
        sys.exit(1)
