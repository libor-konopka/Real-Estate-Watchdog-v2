import asyncio
import re
from collections.abc import AsyncGenerator

import aiohttp
from bs4 import BeautifulSoup
from loguru import logger

from src.contracts.portals.base import AnyProperty
from src.contracts.portals.bazos import BazosInputModel
from src.scrapers.bazos_parser import BazosDOMParser


class BazosScraper:
    """
    Orchestrator for the Bazoš portal. Manages asynchronous HTTP communication,
    pagination, and the transformation of HTML documents into domain entities.
    """

    _HEADERS: dict[str, str] = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "cs,en-US;q=0.7,en;q=0.3",
        "Connection": "keep-alive",
    }

    def __init__(
        self, session: aiohttp.ClientSession, concurrency_limit: int = 3
    ) -> None:
        self.session = session
        self.base_url = "https://reality.bazos.cz"
        # Semaphore for throttling parallel detail scraping to avoid bans
        self._semaphore = asyncio.Semaphore(concurrency_limit)

    async def _process_detail(
        self,
        entity_url: str,
        raw_transaction: str,
        raw_entity: str,
        max_retries: int = 3,
    ) -> AnyProperty | None:
        """
        Fetches an individual listing page, parses DOM via selectolax,
        and returns a materialized domain property.
        """
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
                    location, postal_code = parser.extract_location()

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
                        postal_code=postal_code,
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
