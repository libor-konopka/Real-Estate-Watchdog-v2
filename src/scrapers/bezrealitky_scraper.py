import asyncio
from collections.abc import AsyncIterator
from typing import Any, ClassVar

import aiohttp
from loguru import logger
from pydantic import ValidationError

from src.contracts.base import BuildingProperty, LandProperty
from src.contracts.portals.bezrealitky import BezrealitkyRawInput
from src.scrapers.bezrealitky_parser import BezrealitkyParser


class BezrealitkyScraper:
    """Orchestrator for the Bezrealitky portal using GraphQL API.

    Handles pagination via limit/offset, payload validation, incremental
    filtering against known Bronze records, and domain entity generation.
    """

    GRAPHQL_ENDPOINT: str = "https://api.bezrealitky.cz/graphql/"

    _HEADERS: ClassVar[dict[str, str]] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Origin": "https://www.bezrealitky.cz",
        "Referer": "https://www.bezrealitky.cz/",
    }

    GRAPHQL_QUERY: str = """
    query GetListAdverts(
        $offerType: [OfferType]
        $estateType: [EstateType]
        $limit: Int
        $offset: Int
        $locale: Locale = CS
    ) {
        listAdverts(
            offerType: $offerType
            estateType: $estateType
            limit: $limit
            offset: $offset
            locale: $locale
        ) {
            totalCount
            list {
                id
                uri
                title
                description
                price
                currency
                surface
                surfaceLand
                offerType
                estateType
                condition
                construction
                ownership
                penb
                city(locale: $locale)
                cityDistrict(locale: $locale)
                zip
                gps {
                    lat
                    lng
                }
            }
        }
    }
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        concurrency_limit: int = 2,
        request_delay: float = 0.5,
    ) -> None:
        self.session = session
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.request_delay = request_delay
        self.parser = BezrealitkyParser()

    async def _fetch_graphql_page(
        self,
        offer_type: str,
        estate_type: str,
        limit: int,
        offset: int,
        max_retries: int = 3,
    ) -> dict[str, Any] | None:
        """Executes paginated GraphQL POST query with exponential backoff."""
        # Inject explicit locale parameter alongside array filters
        variables: dict[str, Any] = {
            "offerType": [offer_type],
            "estateType": [estate_type],
            "limit": limit,
            "offset": offset,
            "locale": "CS",
        }
        payload: dict[str, Any] = {
            "query": self.GRAPHQL_QUERY,
            "variables": variables,
        }

        for attempt in range(1, max_retries + 1):
            async with self.semaphore:
                try:
                    await asyncio.sleep(self.request_delay)
                    async with self.session.post(
                        self.GRAPHQL_ENDPOINT,
                        json=payload,
                        headers=self._HEADERS,
                        timeout=aiohttp.ClientTimeout(total=15.0),
                    ) as response:
                        if response.status != 200:
                            logger.warning(
                                f"GraphQL query failed (status {response.status}), attempt {attempt}/{max_retries}"
                            )
                            if attempt < max_retries:
                                await asyncio.sleep(2.0**attempt)
                                continue
                            return None

                        result: dict[str, Any] = await response.json()

                        if "errors" in result:
                            logger.error(f"GraphQL returned errors: {result['errors']}")
                            return None

                        return result.get("data", {})

                except (TimeoutError, aiohttp.ClientError) as exc:
                    logger.warning(
                        f"Network fault on offset {offset} ({exc}), attempt {attempt}/{max_retries}"
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(2.0**attempt)
                        continue
                    return None

        return None

    async def scrape_section(
        self,
        transaction: str,
        entity: str,
        known_source_ids: set[str] | None = None,
        limit_per_page: int = 15,
        max_pages: int = 50,
        max_consecutive_known: int = 10,
    ) -> AsyncIterator[BuildingProperty | LandProperty]:
        """Iterates through paginated GraphQL results and yields validated domain properties."""
        known_ids = known_source_ids or set()

        # Map human-readable slugs to Bezrealitky GraphQL enums
        api_offer = BezrealitkyRawInput.API_TRANSACTION_MAP.get(transaction)
        api_estate = BezrealitkyRawInput.API_ENTITY_MAP.get(entity)

        if not api_offer or not api_estate:
            logger.error(f"Unsupported parameter combination: {transaction}/{entity}")
            return

        consecutive_known = 0
        current_offset = 0

        for page in range(max_pages):
            data = await self._fetch_graphql_page(
                offer_type=api_offer,
                estate_type=api_estate,
                limit=limit_per_page,
                offset=current_offset,
            )

            if not data:
                logger.info(
                    f"No response or error reached for {transaction}/{entity} at offset {current_offset}."
                )
                break

            envelope = data.get("listAdverts") or {}
            items: list[dict[str, Any]] = envelope.get("list") or []

            if not items:
                logger.info(
                    f"Reached end of listings for {transaction}/{entity} (total scanned: {current_offset})."
                )
                break

            page_yield_count = 0
            for raw_item in items:
                source_id = str(raw_item.get("id"))

                # Fast-path skipping: stop crawling if consecutive known listings threshold is reached
                if source_id in known_ids:
                    consecutive_known += 1
                    if consecutive_known >= max_consecutive_known:
                        logger.info(
                            f"Reached {consecutive_known} consecutive known listings. Fast-path stop."
                        )
                        return
                    continue

                consecutive_known = 0

                try:
                    domain_entity = self.parser.parse_advert(raw_item)
                    if domain_entity:
                        yield domain_entity
                        page_yield_count += 1
                except (ValidationError, ValueError, TypeError, KeyError) as exc:
                    logger.warning(
                        f"Failed to parse Bezrealitky advert {source_id}: {exc}"
                    )
                    continue

            logger.debug(
                f"Page {page + 1} (offset {current_offset}): extracted {page_yield_count}/{len(items)} listings."
            )

            # Move offset forward by page size
            current_offset += limit_per_page
            if len(items) < limit_per_page:
                break
