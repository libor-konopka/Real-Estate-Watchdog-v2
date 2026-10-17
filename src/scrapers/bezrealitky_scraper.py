import asyncio
from collections.abc import AsyncGenerator
from typing import Any

import aiohttp
from loguru import logger

from src.contracts.base import BuildingProperty, LandProperty
from src.contracts.portals.bezrealitky import BezrealitkyRawInput
from src.scrapers.bezrealitky_parser import BezrealitkyParser


class BezrealitkyScraper:
    """Orchestrator for the Bezrealitky portal using GraphQL API.

    Handles pagination via limit/offset, payload validation, incremental
    filtering against known Bronze records, and domain entity generation.
    """

    GRAPHQL_ENDPOINT: str = "https://api.bezrealitky.cz/graphql"

    _HEADERS: dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "WebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Origin": "https://www.bezrealitky.cz",
        "Referer": "https://www.bezrealitky.cz/",
    }

    # Declarative GraphQL query requesting exact fields needed for Bronze layer
    GRAPHQL_QUERY: str = """
    query GetListEstates(
        $offerType: [OfferType!],
        $estateType: [EstateType!],
        $limit: Int,
        $offset: Int
    ) {
        listEstates(
            offerType: $offerType,
            estateType: $estateType,
            limit: $limit,
            offset: $offset
        ) {
            items {
                id
                uri
                name
                offerType
                estateType
                price
                currency
                gps {
                    lat
                    lng
                }
                address {
                    city
                    zip
                    region
                    district
                }
                surface
                surfaceLand
                availableFrom
                tags
                condition
                construction
                ownership
                penb
                description
            }
            pageInfo {
                hasNextPage
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
        self._semaphore = asyncio.Semaphore(concurrency_limit)
        self._request_delay = request_delay

    async def scrape_section(
        self,
        transaction: str,
        entity: str,
        known_data: dict[str, float | None] | None = None,
        limit_per_page: int = 50,
        max_pages: int = 30,
        max_consecutive_known: int = 2,
    ) -> AsyncGenerator[BuildingProperty | LandProperty]:
        """Asynchronously pages through GraphQL results, yielding new or updated listings.

        Halts pagination early when encountering consecutive pages of already known data.
        """
        known_records = known_data or {}

        # Resolve internal keys to GraphQL API enums
        api_offer_type = BezrealitkyRawInput.API_TRANSACTION_MAP.get(transaction)
        api_estate_type = BezrealitkyRawInput.API_ENTITY_MAP.get(entity)

        if not api_offer_type or not api_estate_type:
            logger.error(
                f"Unsupported category parameters: transaction='{transaction}', entity='{entity}'"
            )
            return

        consecutive_known_pages = 0

        for page in range(max_pages):
            offset = page * limit_per_page

            payload = {
                "query": self.GRAPHQL_QUERY,
                "variables": {
                    "offerType": [api_offer_type],
                    "estateType": [api_estate_type],
                    "limit": limit_per_page,
                    "offset": offset,
                },
            }

            response_data = await self._fetch_graphql_page(payload)
            if not response_data:
                logger.info(
                    f"No response or error reached for {transaction}/{entity} at offset {offset}."
                )
                break

            items, has_next_page = self._extract_items_and_page_info(response_data)
            if not items:
                logger.info(
                    f"No more items returned for {transaction}/{entity} on page {page + 1}."
                )
                break

            # Fast-path incremental check
            items_to_process: list[dict[str, Any]] = []
            for item in items:
                source_id = str(item.get("id", ""))
                current_price = (
                    float(item["price"]) if item.get("price") is not None else None
                )

                if source_id in known_records:
                    previous_price = known_records[source_id]
                    if current_price == previous_price:
                        continue

                items_to_process.append(item)

            logger.debug(
                f"Page {page + 1} ({transaction}/{entity}): {len(items_to_process)}/{len(items)} "
                "listings are new or updated."
            )

            # Early stopping check
            if not items_to_process:
                consecutive_known_pages += 1
                if consecutive_known_pages >= max_consecutive_known:
                    logger.info(
                        f"Early stopping triggered for {transaction}/{entity} "
                        f"after {consecutive_known_pages} consecutive known pages."
                    )
                    break
            else:
                consecutive_known_pages = 0

                for raw_item in items_to_process:
                    property_entity = BezrealitkyParser.parse_advert(raw_item)
                    if property_entity is not None:
                        yield property_entity

            if not has_next_page:
                logger.info(f"Reached last available page for {transaction}/{entity}.")
                break

            await asyncio.sleep(self._request_delay)

    async def _fetch_graphql_page(
        self,
        payload: dict[str, Any],
        max_retries: int = 3,
    ) -> dict[str, Any] | None:
        """Executes a single GraphQL query with retries and exponential backoff."""
        async with self._semaphore:
            for attempt in range(1, max_retries + 1):
                try:
                    async with self.session.post(
                        self.GRAPHQL_ENDPOINT,
                        json=payload,
                        headers=self._HEADERS,
                    ) as response:
                        if response.status != 200:
                            logger.warning(
                                f"GraphQL query failed (status {response.status}), "
                                f"attempt {attempt}/{max_retries}"
                            )
                            if attempt == max_retries:
                                return None
                            await asyncio.sleep(2 * attempt)
                            continue

                        data: dict[str, Any] = await response.json()
                        if "errors" in data:
                            logger.error(f"GraphQL returned errors: {data['errors']}")
                            return None

                        return data

                except (TimeoutError, aiohttp.ClientError) as e:
                    if attempt == max_retries:
                        logger.error(f"Network error querying GraphQL: {e}")
                        return None
                    await asyncio.sleep(2 * attempt)

            return None

    @staticmethod
    def _extract_items_and_page_info(
        data: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], bool]:
        """Safely extracts items array and pagination boundary flag."""
        estates = data.get("data", {}).get("listEstates", {})
        items = estates.get("items") or []
        page_info = estates.get("pageInfo") or {}
        has_next_page = bool(page_info.get("hasNextPage", False))
        return items, has_next_page
