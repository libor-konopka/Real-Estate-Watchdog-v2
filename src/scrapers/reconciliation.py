import asyncio

import aiohttp
from loguru import logger

from src.contracts.portals.base import AnyProperty
from src.contracts.portals.bazos import BasePortalInput


class BazosReconciler:
    def __init__(self, session: aiohttp.ClientSession, concurrency_limit: int = 5):
        self.session = session
        # Semafor omezuje počet souběžných dotazů, abychom nedostali ban
        self._semaphore = asyncio.Semaphore(concurrency_limit)

    async def _check_exists(self, url: str) -> bool:
        """Pingne URL inzerátu. Pokud je přesměrován nebo nenalezen, považuje se za smazaný."""
        async with self._semaphore:
            try:
                # allow_redirects=False je klíčové, Bazoš smazané inzeráty přesměrovává (HTTP 301/302)
                async with self.session.get(url, allow_redirects=False) as response:
                    return response.status == 200
            except Exception as e:
                logger.warning(f"Chyba sítě při ověřování {url}: {e}")
                # Při chybě spojení raději vracíme True, abychom inzerát omylem nesmazali
                return True

    async def verify_and_deactivate(
        self, active_records: list[AnyProperty]
    ) -> list[AnyProperty]:
        """Projde Pydantic modely a pro ty smazané vygeneruje neaktivní kopie (Tombstones)."""
        tombstones = []

        async def process(record: AnyProperty):
            exists = await self._check_exists(str(record.url))
            if not exists:
                logger.debug(f"Inzerát {record.source_id} byl z portálu odstraněn.")

                # model_copy s parametrem update vytvoří novou instanci i u frozen=True
                tombstone = record.model_copy(
                    update={
                        "is_active": False,
                        "scraped_at": BasePortalInput._get_utc_now(),
                    }
                )
                tombstones.append(tombstone)

        # Spustíme kontroly asynchronně a souběžně
        tasks = [process(rec) for rec in active_records]
        if tasks:
            await asyncio.gather(*tasks)

        return tombstones
