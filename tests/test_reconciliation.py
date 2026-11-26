import asyncio
from types import TracebackType
from typing import Self

import aiohttp

from src.contracts.base import ActiveListingRef, SourcePortal
from src.contracts.portals.bazos import BazosInputModel
from src.scrapers.reconciliation import BazosReconciler


class _FakeResponse:
    def __init__(self, status: int, final_url: str) -> None:
        self.status = status
        self.url = final_url

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None


class _FakeSession:
    def __init__(self, responses: dict[str, tuple[int, str] | Exception]) -> None:
        self._responses = responses

    def head(
        self,
        url: str,
        headers: dict[str, str],
        allow_redirects: bool,
        timeout: aiohttp.ClientTimeout,
    ) -> _FakeResponse:
        outcome = self._responses[url]
        if isinstance(outcome, Exception):
            raise outcome
        status, final_url = outcome
        return _FakeResponse(status=status, final_url=final_url)


def test_reconciler_emits_consistent_sha256_tombstones() -> None:
    active_url = "https://reality.bazos.cz/inzerat/111111/aktivni-byt.php"
    deleted_url = "https://reality.bazos.cz/inzerat/222222/smazany-byt.php"
    redirected_url = "https://reality.bazos.cz/inzerat/333333/expirovany-byt.php"
    flaky_url = "https://reality.bazos.cz/inzerat/444444/docasny-vypadek.php"

    session = _FakeSession(
        {
            active_url: (200, active_url),
            deleted_url: (404, deleted_url),
            redirected_url: (200, "https://reality.bazos.cz/prodam/byt/"),
            flaky_url: aiohttp.ClientError("Transient timeout"),
        }
    )

    reconciler = BazosReconciler(session=session)  # type: ignore[arg-type]
    records = [
        ActiveListingRef(source_id="111111", url=active_url),
        ActiveListingRef(source_id="222222", url=deleted_url),
        ActiveListingRef(source_id="333333", url=redirected_url),
        ActiveListingRef(source_id="444444", url=flaky_url),
    ]

    tombstones = asyncio.run(
        reconciler.verify_and_deactivate(
            active_records=records,
            transaction="sale",
            entity_type="apartment",
        )
    )

    assert len(tombstones) == 2
    by_source_id = {item["source_id"]: item for item in tombstones}

    assert set(by_source_id.keys()) == {"222222", "333333"}
    assert by_source_id["222222"]["id"] == BazosInputModel.generate_entity_id(
        SourcePortal.BAZOS, "222222"
    )
    assert by_source_id["222222"]["is_active"] is False
    assert "location" in by_source_id["222222"]
