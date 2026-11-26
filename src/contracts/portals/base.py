import hashlib
from abc import ABC, abstractmethod
from datetime import UTC, datetime

from pydantic import BaseModel

from src.contracts.base import PropertyListing, SourcePortal


class BasePortalInput(BaseModel, ABC):
    """Abstract base boundary model for external portal ingestion payloads.

    Provides deterministic SHA-256 identity generation and UTC timestamp helpers.
    """

    @staticmethod
    def _generate_id(source_string: str) -> str:
        """Generates a deterministic SHA-256 hex digest from an arbitrary string."""
        return hashlib.sha256(source_string.encode("utf-8")).hexdigest()

    @classmethod
    def generate_entity_id(cls, source: SourcePortal, source_id: str) -> str:
        """Generates the canonical Lakehouse entity ID for a portal listing."""
        return cls._generate_id(f"{source.value}_{source_id}")

    @staticmethod
    def _get_utc_now() -> datetime:
        """Returns the current timezone-aware UTC timestamp."""
        return datetime.now(UTC)

    @abstractmethod
    def materialize(self) -> PropertyListing:
        """Validates and converts raw portal input into an immutable domain model."""
