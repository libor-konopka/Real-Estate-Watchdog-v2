import asyncio
import re
from collections.abc import AsyncGenerator

import aiohttp
from bs4 import BeautifulSoup
from loguru import logger

from src.contracts.portals.base import AnyProperty
from src.contracts.portals.bazos import BazosRawInput
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
        Fetches a single advertisement detail and materializes it into a domain entity.
        Includes a retry mechanism to overcome network instability and is protected
        by a semaphore against overloading the target server.
        """
        async with self._semaphore:
            for attempt in range(1, max_retries + 1):
                try:
                    async with self.session.get(
                        entity_url, headers=self._HEADERS
                    ) as response:
                        if response.status != 200:
                            logger.warning(
                                f"Network barrier. Status {response.status} for URL: {entity_url}"
                            )
                            return None

                        html_content = await response.text(encoding="utf-8")

                    parser = BazosDOMParser(html_content)
                    extracted_data = parser.parse_all()

                    additional_data: dict[str, str] = {
                        "raw_url": entity_url,
                        "raw_transaction": raw_transaction,
                        "raw_entity": raw_entity,
                    }
                    extracted_data |= additional_data

                    return BazosRawInput(**extracted_data).materialize()

                except (TimeoutError, aiohttp.ClientError) as e:
                    if attempt == max_retries:
                        logger.warning(
                            f"Connection with {entity_url} definitely failed: {e}"
                        )
                        return None

                    wait_time = 3 * attempt
                    logger.debug(
                        f"Barrier in the network. The system breathes {wait_time} s "
                        f"and repeats the attempt for the detail ({attempt}/{max_retries})..."
                    )
                    await asyncio.sleep(wait_time)

                except Exception as e:
                    # Irrecoverable errors (e.g., data parsing) are not retried
                    logger.warning(f"Error while processing {entity_url}: {e}")
                    return None

            return None

    async def scrape_section(
        self,
        raw_transaction: str,
        raw_entity: str,
        known_data: dict[str, str] | None = None,
    ) -> AsyncGenerator[AnyProperty]:
        """
        Asynchronous generator that iterates through the paginated list of advertisements.
        Filters out known unchanged listings and yields new or updated domain entities.
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
                            f"End of flow or barrier reached at {current_url} (status {response.status})."
                        )
                        break

                    list_html = await response.text(encoding="utf-8")

                soup = BeautifulSoup(list_html, "lxml")
                new_detail_urls: list[str] = []

                for container in soup.find_all("div", class_="inzeraty"):
                    if not (h2 := container.find("h2", class_="nadpis")):
                        continue

                    if (a_tag := h2.find("a")) and "href" in a_tag.attrs:
                        detail_path = str(a_tag["href"])

                        if match := re.search(r"/inzerat/(\d+)/", detail_path):
                            source_id = match.group(1)
                            current_price = ""

                            if price_tag := container.find(
                                "div", class_="inzeratycena"
                            ):
                                current_price = re.sub(
                                    r"[^\d]", "", price_tag.get_text()
                                )

                            if source_id in known_data:
                                historical_price = re.sub(
                                    r"[^\d]", "", str(known_data[source_id])
                                )

                                if current_price == historical_price:
                                    continue
                                else:
                                    logger.debug(
                                        f"Price change detected for ID {source_id}: "
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
                            "Historical bottom reached (3 consecutive pages of known listings). "
                            "Ending section."
                        )
                        break

                    offset += 20
                    continue

                consecutive_known_pages = 0

                # Safe execution of asynchronous tasks thanks to semaphore protection
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
                logger.warning(f"Network error while fetching URL {current_url}: {e}")
                await asyncio.sleep(5.0)
                break
            except Exception as e:
                logger.error(f"Unexpected error processing section {current_url}: {e}")
                break
