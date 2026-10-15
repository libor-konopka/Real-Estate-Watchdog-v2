import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
from loguru import logger

from src.contracts.base import ActiveListingRef, SourcePortal
from src.contracts.portals.base import AnyProperty


class BazosReconciler:
    """
    Audits the current existence of active listings on the Bazoš portal.
    Generates tombstone entities for records that were removed or redirected.
    """

    _HEADERS: dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Connection": "keep-alive",
    }

    def __init__(
        self, session: aiohttp.ClientSession, concurrency_limit: int = 5
    ) -> None:
        self.session = session
        self._semaphore = asyncio.Semaphore(concurrency_limit)

    async def _check_listing_active(self, url: str) -> bool:
        """
        Sends an HTTP HEAD/GET request without following redirects.
        Returns False ONLY if the server returns explicit redirection or 404/410.
        Returns True if status is 200 or in case of transient network errors.
        """
        async with self._semaphore:
            try:
                # Try HEAD first to avoid downloading body payload
                async with self.session.head(
                    url, headers=self._HEADERS, allow_redirects=False
                ) as response:
                    # Method Not Allowed: fallback to GET
                    if response.status == 405:
                        async with self.session.get(
                            url, headers=self._HEADERS, allow_redirects=False
                        ) as get_response:
                            status = get_response.status
                    else:
                        status = response.status

                # Explicit signs of listing removal: redirects or not found
                if status in (301, 302, 303, 307, 308, 404, 410):
                    return False

                if status == 200:
                    return True

                # Rate limiting or WAF block: assume listing is STILL active to protect data integrity
                logger.warning(
                    f"Unexpected status code {status} for {url}. "
                    "Preserving active state to avoid data corruption."
                )
                return True

            except (TimeoutError, aiohttp.ClientError) as e:
                logger.debug(f"Network error verifying {url}: {e}. Retaining status.")
                return True
            except Exception as e:
                logger.error(f"Unexpected error checking {url}: {e}")
                return True

    async def _audit_single_record(self, record: AnyProperty) -> AnyProperty | None:
        """Audits a single property and returns a tombstone if it was removed."""
        is_active = await self._check_listing_active(str(record.url))
        if not is_active:
            logger.debug(f"Listing {record.source_id} removed from portal.")
            return record.model_copy(
                update={
                    "is_active": False,
                    "scraped_at": datetime.now(ZoneInfo("UTC")),
                }
            )
        return None

    async def verify_and_deactivate(
        self, active_records: list[ActiveListingRef]
    ) -> list[dict]:
        """
        Concurrently checks active records and returns tombstone dictionaries
        ready for Bronze append.
        """
        if not active_records:
            return []

        async def audit(record: ActiveListingRef) -> dict | None:
            is_active = await self._check_listing_active(str(record.url))
            if not is_active:
                logger.debug(f"Listing {record.source_id} removed from portal.")
                return {
                    "id": record.id,
                    "title": "[DEACTIVATED]",
                    "is_active": False,
                    "source_id": record.source_id,
                    "url": str(record.url),
                    "source": SourcePortal.BAZOS.value,
                    "transaction_type": record.transaction_type.value,
                    "entity_type": record.entity_type.value,
                    "scraped_at": datetime.now(ZoneInfo("UTC")),
                }
            return None

        tasks = [audit(rec) for rec in active_records]
        results = await asyncio.gather(*tasks)

        return [res for res in results if res is not None]
