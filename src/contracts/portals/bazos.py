import re
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
    PropertyListing,
    SourcePortal,
    TransactionType,
)
from src.contracts.portals.base import BasePortalInput


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

    @classmethod
    def resolve_enums(
        cls, transaction: str, entity: str
    ) -> tuple[TransactionType, EntityType]:
        """Resolves URL transaction and entity slugs into domain enums."""
        if transaction not in cls._TRANSACTION_MAP:
            raise ValueError(f"Unsupported Bazos transaction slug: {transaction}")
        if entity not in cls._ENTITY_MAP:
            raise ValueError(f"Unsupported Bazos entity slug: {entity}")
        return cls._TRANSACTION_MAP[transaction], cls._ENTITY_MAP[entity]

    def _extract_source_id(self) -> str:
        """Extracts numeric advertisement ID from Bazos URL path."""
        match = re.search(r"/inzerat/(\d+)/", self.url) or re.search(
            r"/(\d+)/", self.url
        )
        if match:
            return match.group(1)
        return self._generate_id(self.url)[:12]

    def materialize(self) -> PropertyListing:
        """Converts raw validated input into BuildingProperty or LandProperty."""
        source_id = self._extract_source_id()
        entity_id = self.generate_entity_id(SourcePortal.BAZOS, source_id)

        transaction_enum = self._TRANSACTION_MAP.get(
            self.raw_transaction, TransactionType.SALE
        )
        entity_enum = self._ENTITY_MAP.get(self.raw_entity, EntityType.APARTMENT)

        currency_enum = (
            Currency(self.currency.lower())
            if self.currency.lower() in Currency
            else Currency.CZK
        )

        loc = self.location or Location()
        if self.postal_code and not loc.postal_code:
            loc = loc.model_copy(update={"postal_code": self.postal_code})

        title = self.title or "Bazoš inzerát"
        listing_url = HttpUrl(self.url)
        scraped_at = self._get_utc_now()

        if entity_enum == EntityType.LAND:
            return LandProperty(
                id=entity_id,
                title=title,
                is_active=True,
                source_id=source_id,
                url=listing_url,
                source=SourcePortal.BAZOS,
                transaction_type=transaction_enum,
                entity_type=EntityType.LAND,
                price=self.price,
                currency=currency_enum,
                scraped_at=scraped_at,
                location=loc,
                infrastructure=self.infrastructure,
                legal=self.legal,
                description=self.description,
                land_area_m2=self.land_area_m2,
            )

        return BuildingProperty(
            id=entity_id,
            title=title,
            is_active=True,
            source_id=source_id,
            url=listing_url,
            source=SourcePortal.BAZOS,
            transaction_type=transaction_enum,
            entity_type=entity_enum,
            price=self.price,
            currency=currency_enum,
            scraped_at=scraped_at,
            location=loc,
            infrastructure=self.infrastructure,
            legal=self.legal,
            description=self.description,
            land_area_m2=self.land_area_m2,
            usable_area_m2=self.usable_area_m2,
            built_up_area_m2=self.built_up_area_m2,
            material=self.material,
            condition=self.condition,
            energy_class=self.energy_class,
        )


# Alias ensuring backward compatibility across all modules
BazosRawInput = BazosInputModel
