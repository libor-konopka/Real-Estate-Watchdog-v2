import asyncio
from collections.abc import AsyncGenerator

import aiohttp
from loguru import logger

from src.contracts.base import BuildingProperty, LandProperty
from src.contracts.portals.bazos import BazosInputModel
from src.scrapers.bazos_parser import BazosDOMParser, BazosListingCard


class BazosScraper:
    """Orchestrator for the Bazoš portal.

    Manages asynchronous HTTP requests, catalog pagination, fast-path
    skipping of existing listings, and transformation into domain entities.
    """

    _HEADERS: dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "cs,en-US;q=0.7,en;q=0.3",
        "Connection": "keep-alive",
    }

    # Bazoš displays 20 listings per pagination page
    PAGE_SIZE: int = 20

    def __init__(
        self,
        session: aiohttp.ClientSession,
        concurrency_limit: int = 3,
        request_delay: float = 0.5,
    ) -> None:
        self.session = session
        self.base_url = "https://reality.bazos.cz"
        self._semaphore = asyncio.Semaphore(concurrency_limit)
        self._request_delay = request_delay

    async def scrape_section(
        self,
        transaction: str,
        entity: str,
        known_data: dict[str, float | None] | None = None,
        max_pages: int = 25,
        max_consecutive_known: int = 2,
    ) -> AsyncGenerator[BuildingProperty | LandProperty]:
        """Asynchronously scrapes a section, yielding new or price-updated properties.

        Stops pagination early if consecutive pages yield no new or updated records.
        """
        known_records = known_data or {}
        consecutive_fully_known_pages = 0

        for page in range(max_pages):
            offset = page * self.PAGE_SIZE
            # First page uses canonical path, subsequent pages append the offset
            section_url = (
                f"{self.base_url}/{transaction}/{entity}/"
                if offset == 0
                else f"{self.base_url}/{transaction}/{entity}/{offset}/"
            )

            cards = await self._fetch_catalog_page(section_url)
            if not cards:
                logger.info(
                    f"No more listings found on page {page + 1} for {transaction}/{entity}."
                )
                break

            # Fast-path incremental check: identify listings that need detail fetching
            cards_to_fetch: list[BazosListingCard] = []
            for card in cards:
                if card.source_id in known_records:
                    previous_price = known_records[card.source_id]
                    # Skip if price is unchanged and listing is already recorded
                    if card.price == previous_price:
                        continue
                cards_to_fetch.append(card)

            logger.debug(
                f"Page {page + 1} ({transaction}/{entity}): {len(cards_to_fetch)}/{len(cards)} "
                "listings need extraction."
            )

            # Early stopping mechanism: if entire pages are already known, avoid scraping deeper
            if not cards_to_fetch:
                consecutive_fully_known_pages += 1
                if consecutive_fully_known_pages >= max_consecutive_known:
                    logger.info(
                        f"Early stopping triggered for {transaction}/{entity} "
                        f"after {consecutive_fully_known_pages} consecutive known pages."
                    )
                    break
            else:
                consecutive_fully_known_pages = 0

                # Fetch and parse details concurrently under the semaphore ceiling
                tasks = [
                    self._process_detail(card.url, transaction, entity)
                    for card in cards_to_fetch
                ]
                results = await asyncio.gather(*tasks)

                for property_entity in results:
                    if property_entity is not None:
                        yield property_entity

            # Courtesy delay between catalog pages to avoid triggering rate limits
            await asyncio.sleep(self._request_delay)

    async def _fetch_catalog_page(self, url: str) -> list[BazosListingCard]:
        """Fetches and parses a single catalog pagination page."""
        try:
            async with self.session.get(url, headers=self._HEADERS) as response:
                if response.status != 200:
                    logger.warning(
                        f"Catalog page returned status {response.status}: {url}"
                    )
                    return []
                html = await response.text(encoding="utf-8")
                return BazosDOMParser.parse_catalog_page(html, self.base_url)
        except (TimeoutError, aiohttp.ClientError) as e:
            logger.error(f"Network error while fetching catalog {url}: {e}")
            return []

    async def _process_detail(
        self,
        entity_url: str,
        raw_transaction: str,
        raw_entity: str,
        max_retries: int = 3,
    ) -> BuildingProperty | LandProperty | None:
        """Fetches listing detail, parses DOM, and returns a domain model."""
        async with self._semaphore:
            for attempt in range(1, max_retries + 1):
                try:
                    async with self.session.get(
                        entity_url, headers=self._HEADERS
                    ) as response:
                        if response.status != 200:
                            logger.warning(
                                f"Failed to fetch detail (status {response.status}): {entity_url}"
                            )
                            return None

                        html_content = await response.text(encoding="utf-8")

                    parser = BazosDOMParser(html_content, entity_url)

                    title = parser.extract_title()
                    description = parser.extract_description()
                    price, currency = parser.extract_price()
                    location = parser.extract_location()

                    combined_text = f"{title} {description}"

                    input_model = BazosInputModel(
                        url=entity_url,
                        raw_transaction=raw_transaction,
                        raw_entity=raw_entity,
                        title=title,
                        description=description,
                        price=price,
                        currency=currency,
                        location=location,
                        postal_code=location.postal_code,
                        infrastructure=parser.extract_infrastructure(description),
                        legal=parser.extract_legal_context(description, title),
                        usable_area_m2=parser.extract_usable_area(title, description),
                        land_area_m2=parser.extract_land_area(title, description),
                        built_up_area_m2=parser.extract_built_up_area(
                            title, description
                        ),
                        material=parser.extract_material(combined_text),
                        condition=parser.extract_condition(combined_text),
                        energy_class=parser.extract_energy_class(combined_text),
                    )

                    return input_model.materialize()

                except (TimeoutError, aiohttp.ClientError) as e:
                    if attempt == max_retries:
                        logger.error(f"Exceeded max retries for {entity_url}: {e}")
                        return None

                    wait_seconds = 2 * attempt
                    logger.debug(
                        f"Network issue on {entity_url}. Backoff {wait_seconds}s "
                        f"(attempt {attempt}/{max_retries})."
                    )
                    await asyncio.sleep(wait_seconds)

                except Exception as e:
                    logger.error(
                        f"Unrecoverable parsing/validation error for {entity_url}: {e}"
                    )
                    return None

            return None
