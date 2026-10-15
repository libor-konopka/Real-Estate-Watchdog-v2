import asyncio
import re
from collections.abc import AsyncGenerator

import aiohttp
from loguru import logger
from selectolax.parser import HTMLParser

from src.contracts.portals.base import AnyProperty
from src.contracts.portals.bazos import BazosInputModel
from src.scrapers.bazos_parser import BazosDOMParser


class BazosScraper:
    """
    Orchestrator for the Bazoš portal.
    Manages asynchronous HTTP requests, pagination, and domain entity extraction.
    """

    _HEADERS: dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "cs,en;q=0.9",
        "Connection": "keep-alive",
    }

    def __init__(
        self, session: aiohttp.ClientSession, concurrency_limit: int = 3
    ) -> None:
        self.session = session
        self.base_url = "https://reality.bazos.cz"
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

    async def scrape_section(
        self,
        raw_transaction: str,
        raw_entity: str,
        known_data: dict[str, str] | None = None,
    ) -> AsyncGenerator[AnyProperty]:
        """
        Iterates through paginated listing views.
        Skips already known records with unchanged prices and yields materialized entities.
        """
        known_data = known_data or {}
        offset = 0
        consecutive_known_pages = 0

        while True:
            current_url = f"{self.base_url}/{raw_transaction}/{raw_entity}/"
            if offset > 0:
                current_url += f"{offset}/"

            try:
                async with self.session.get(
                    current_url, headers=self._HEADERS
                ) as response:
                    if response.status != 200:
                        logger.info(
                            f"Section boundary reached at {current_url} (status {response.status})."
                        )
                        break

                    list_html = await response.text(encoding="utf-8")

                tree = HTMLParser(list_html)
                listings = tree.css("div.inzeraty")

                if not listings:
                    logger.info(
                        f"No listings found at {current_url}. Ending pagination."
                    )
                    break

                new_detail_urls: list[str] = []

                for container in listings:
                    h2_node = container.css_first("h2.nadpis")
                    if not h2_node:
                        continue

                    a_tag = h2_node.css_first("a")
                    if not a_tag or "href" not in a_tag.attributes:
                        continue

                    detail_path = str(a_tag.attributes["href"])
                    match = re.search(r"/inzerat/(\d+)/", detail_path)
                    if not match:
                        continue

                    source_id = match.group(1)
                    price_node = container.css_first("div.inzeratycena")
                    current_price = (
                        re.sub(r"[^\d]", "", price_node.text(strip=True))
                        if price_node
                        else ""
                    )

                    # Incremental skipping: identical price on known ID
                    if source_id in known_data:
                        historical_price = re.sub(
                            r"[^\d]", "", str(known_data[source_id] or "")
                        )
                        if current_price == historical_price:
                            continue
                        logger.debug(
                            f"Price change detected on ID {source_id}: "
                            f"{historical_price} -> {current_price}"
                        )

                    full_url = (
                        f"{self.base_url}{detail_path}"
                        if detail_path.startswith("/")
                        else f"{self.base_url}/{detail_path}"
                    )
                    new_detail_urls.append(full_url)

                if not new_detail_urls:
                    consecutive_known_pages += 1
                    if consecutive_known_pages >= 3:
                        logger.info(
                            "Stop condition met: 3 consecutive pages contained "
                            "only unchanged known listings."
                        )
                        break

                    offset += 20
                    continue

                consecutive_known_pages = 0

                tasks = [
                    self._process_detail(url, raw_transaction, raw_entity)
                    for url in new_detail_urls
                ]
                results = await asyncio.gather(*tasks)

                for entity in results:
                    if entity is not None:
                        yield entity

                offset += 20

            except (TimeoutError, aiohttp.ClientError) as e:
                logger.warning(f"Connection failure on {current_url}: {e}")
                await asyncio.sleep(5.0)
                break
            except Exception as e:
                logger.error(f"Unexpected fault while scraping {current_url}: {e}")
                break
