import aiohttp
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src.contracts.base import SourcePortal
from src.transformations.bronze import (
    create_http_session_params,
    ingest_bazos_portal,
    ingest_bezrealitky_portal,
)


@asset(
    group_name="bronze_layer",
    description=(
        "Ingests raw listings from Bazos into Bronze Iceberg table "
        "and audits deleted records."
    ),
    compute_kind="python",
)
async def bronze_bazos_listings(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Executes asynchronous scraping and reconciliation for all Bazos categories."""
    run_id = context.run_id
    connector, timeout = create_http_session_params()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        total_scraped, total_deactivated = await ingest_bazos_portal(
            session=session,
            batch_id=run_id,
            log=context.log,
        )

    return MaterializeResult(
        metadata={
            "source": MetadataValue.text(SourcePortal.BAZOS.value),
            "batch_id": MetadataValue.text(run_id),
            "new_or_updated_records": MetadataValue.int(total_scraped),
            "deactivated_records": MetadataValue.int(total_deactivated),
            "target_table": MetadataValue.text(
                f"bronze.{SourcePortal.BAZOS.value}_properties"
            ),
        }
    )


@asset(
    group_name="bronze_layer",
    description=(
        "Ingests raw listings from Bezrealitky GraphQL API into Bronze Iceberg table."
    ),
    compute_kind="python",
)
async def bronze_bezrealitky_listings(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Executes asynchronous scraping for all supported Bezrealitky categories."""
    run_id = context.run_id
    connector, timeout = create_http_session_params()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        total_scraped = await ingest_bezrealitky_portal(
            session=session,
            batch_id=run_id,
            log=context.log,
        )

    return MaterializeResult(
        metadata={
            "source": MetadataValue.text(SourcePortal.BEZREALITKY.value),
            "batch_id": MetadataValue.text(run_id),
            "new_or_updated_records": MetadataValue.int(total_scraped),
            "target_table": MetadataValue.text(
                f"bronze.{SourcePortal.BEZREALITKY.value}_properties"
            ),
        }
    )
