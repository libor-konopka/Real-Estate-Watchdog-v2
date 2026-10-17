import hashlib
import re
from datetime import UTC, datetime, timezone
from decimal import Decimal
from typing import ClassVar

from pydantic import HttpUrl

from src.contracts.base import (
    BuildingCondition,
    BuildingMaterial,
    BuildingProperty,
    Currency,
    EnergyClass,
    EntityType,
    Infrastructure,
    LandProperty,
    LegalContext,
    Location,
    SourcePortal,
    TransactionType,
)
from src.contracts.portals.base import AnyProperty, BasePortalInput


class BazosInputModel(BasePortalInput):
    """Validation boundary model for raw listings scraped from Bazos HTML pages."""

    # URL segment -> Domain TransactionType
    _TRANSACTION_MAP: ClassVar[dict[str, TransactionType]] = {
        "prodam": TransactionType.SALE,
        "pronajmu": TransactionType.RENT,
    }

    # URL segment -> Domain EntityType
    _ENTITY_MAP: ClassVar[dict[str, EntityType]] = {
        "byt": EntityType.APARTMENT,
        "dum": EntityType.HOUSE,
        "chalupa": EntityType.HOUSE,
        "pozemek": EntityType.LAND,
        "garaz": EntityType.OTHER,
        "kancelare": EntityType.COMMERCIAL,
    }

    # Raw input fields populated by BazosDOMParser / BazosScraper
    url: str
    raw_transaction: str
    raw_entity: str
    title: str
    description: str | None = None
    price: float | None = None
    currency: str = "czk"

    # Contextual domain objects and address fields
    location: Location | None = None
    postal_code: str | None = None
    infrastructure: Infrastructure | None = None
    legal: LegalContext | None = None

    # Physical dimensions in m2
    usable_area_m2: float | None = None
    land_area_m2: float | None = None
    built_up_area_m2: float | None = None

    # Technical structural parameters
    material: BuildingMaterial | None = None
    condition: BuildingCondition | None = None
    energy_class: EnergyClass | None = None

    @classmethod
    def get_supported_transactions(cls) -> list[str]:
        """Returns list of supported URL transaction slugs."""
        return list(cls._TRANSACTION_MAP.keys())

    @classmethod
    def get_supported_entities(cls) -> list[str]:
        """Returns list of supported URL entity slugs."""
        return list(cls._ENTITY_MAP.keys())

    def _extract_source_id(self) -> str:
        """Extracts numeric advertisement ID from Bazos URL path."""
        match = re.search(r"/inzerat/(\d+)/", self.url) or re.search(
            r"/(\d+)/", self.url
        )
        if match:
            return match.group(1)
        # Fallback to deterministic hash if numeric pattern is absent
        return hashlib.md5(self.url.encode()).hexdigest()[:10]

    def materialize(self) -> AnyProperty:
        """Converts raw validated input into domain BuildingProperty or LandProperty."""
        source_id = self._extract_source_id()

        # Deterministic Lakehouse entity ID: sha256(bazos_<source_id>)
        entity_id = hashlib.sha256(
            f"{SourcePortal.BAZOS.value}_{source_id}".encode()
        ).hexdigest()

        transaction_enum = self._TRANSACTION_MAP.get(
            self.raw_transaction, TransactionType.SALE
        )
        entity_enum = self._ENTITY_MAP.get(self.raw_entity, EntityType.APARTMENT)

        dec_price = Decimal(str(self.price)) if self.price is not None else None
        currency_enum = (
            Currency(self.currency.lower())
            if self.currency.lower() in Currency
            else Currency.CZK
        )

        # Ensure location object retains postal code if provided separately
        loc = self.location or Location()
        if self.postal_code and not loc.postal_code:
            loc = loc.model_copy(update={"postal_code": self.postal_code})

        base_params = {
            "id": entity_id,
            "title": self.title or "Bazoš inzerát",
            "is_active": True,
            "source_id": source_id,
            "url": HttpUrl(self.url),
            "source": SourcePortal.BAZOS,
            "transaction_type": transaction_enum,
            "price": dec_price,
            "currency": currency_enum,
            "scraped_at": datetime.now(UTC),
            "location": loc,
            "infrastructure": self.infrastructure,
            "legal": self.legal,
            "description": self.description,
            "land_area_m2": self.land_area_m2,
        }

        if entity_enum == EntityType.LAND:
            return LandProperty(
                **base_params,
                entity_type=entity_enum,
            )

        return BuildingProperty(
            **base_params,
            entity_type=entity_enum,
            usable_area_m2=self.usable_area_m2,
            built_up_area_m2=self.built_up_area_m2,
            material=self.material,
            condition=self.condition,
            energy_class=self.energy_class,
        )


# Alias ensuring backward compatibility across all modules
BazosRawInput = BazosInputModel
