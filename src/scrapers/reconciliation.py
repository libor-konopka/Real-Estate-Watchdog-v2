import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
from loguru import logger

from src.contracts.base import ActiveListingRef, SourcePortal


class BazosReconciler:
    """
    Performs high-throughput, low-overhead HTTP verification of currently active listings.
    Emits tombstone records for listings that have been deleted or expired on the source portal.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        concurrency: int = 20,
        request_timeout_seconds: int = 10,
    ) -> None:
        self.session = session
        self._semaphore = asyncio.Semaphore(concurrency)
        self._timeout = aiohttp.ClientTimeout(total=request_timeout_seconds)
        self._headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "cs,en-US;q=0.7,en;q=0.3",
        }

    async def _check_listing_active(self, url: str) -> bool:
        """
        Executes a lightweight HEAD request with redirect resolution.
        Returns False if the resource is missing (404) or redirected away from the listing page.
        """
        async with self._semaphore:
            try:
                async with self.session.head(
                    url,
                    headers=self._headers,
                    allow_redirects=True,
                    timeout=self._timeout,
                ) as resp:
                    if resp.status in (404, 410):
                        return False

                    # Bazos redirects deleted/expired ads back to the parent category page
                    normalized_original = url.rstrip("/")
                    normalized_resolved = str(resp.url).rstrip("/")
                    if normalized_resolved != normalized_original:
                        return False

                    return resp.status == 200

            except (TimeoutError, aiohttp.ClientError) as exc:
                # Network flakiness should not trigger false deactivations
                logger.warning(
                    f"Transient network error probing listing {url}: {exc}. Preserving active state."
                )
                return True
            except Exception as exc:
                logger.error(f"Unexpected error probing listing {url}: {exc}")
                return True

    async def verify_and_deactivate(
        self,
        active_records: list[ActiveListingRef],
        transaction: str,
        entity_type: str,
    ) -> list[dict]:
        """
        Concurrently checks existing active listings.
        Returns minimal tombstone payloads ready for direct ingestion via save_to_iceberg.
        """
        if not active_records:
            return []

        logger.info(
            f"Starting reconciliation audit for {len(active_records)} active listings "
            f"({transaction}/{entity_type})..."
        )

        async def probe(ref: ActiveListingRef) -> dict | None:
            is_active = await self._check_listing_active(ref.url)
            if not is_active:
                logger.info(f"Listing deemed inactive on source: {ref.source_id}")
                return {
                    "id": f"{SourcePortal.BAZOS.value}_{ref.source_id}",
                    "title": "[DEACTIVATED]",
                    "is_active": False,
                    "source_id": ref.source_id,
                    "url": ref.url,
                    "source": SourcePortal.BAZOS.value,
                    "transaction_type": transaction,
                    "entity_type": entity_type,
                    "scraped_at": datetime.now(ZoneInfo("UTC")),
                }
            return None

        tasks = [probe(ref) for ref in active_records]
        results = await asyncio.gather(*tasks)

        tombstones = [res for res in results if res is not None]
        logger.info(
            f"Reconciliation completed: {len(tombstones)} inactive listings marked for deactivation."
        )
        return tombstones
