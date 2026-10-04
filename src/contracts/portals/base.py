import hashlib
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Annotated
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field

# Core entities import
from src.contracts.base import BuildingProperty, LandProperty

# Unified type for dynamic materialization
AnyProperty = Annotated[
    BuildingProperty | LandProperty, Field(discriminator="entity_type")
]


class BasePortalInput(BaseModel, ABC):
    """
    Abstract foundational membrane for all external data sources.
    Provides shared alchemical tools for identity and time anchoring.
    """

    @staticmethod
    def _generate_id(source_string: str) -> str:
        """
        Generates a deterministic SHA-256 hash to anchor the entity's identity.
        Ensures perfect deduplication across the Lakehouse.
        """
        return hashlib.sha256(source_string.encode("utf-8")).hexdigest()

    @staticmethod
    def _get_utc_now() -> datetime:
        """
        Captures the absolute current time in UTC, free from local illusions.
        """
        return datetime.now(ZoneInfo("UTC"))

    @abstractmethod
    def materialize(self) -> AnyProperty:
        """
        Alchemical process: converts the sanitized bronze mass into the strict core property.
        Must be implemented by every specific portal membrane.
        """
