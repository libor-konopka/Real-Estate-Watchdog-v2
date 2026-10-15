import hashlib
import re
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
    """Clean validation boundary for data coming from Bazos DOM parser."""

    _TRANSACTION_MAP: ClassVar[dict[str, TransactionType]] = {
        "prodam": TransactionType.SALE,
        "pronajmu": TransactionType.RENT,
    }

    _ENTITY_MAP: ClassVar[dict[str, EntityType]] = {
        "byt": EntityType.APARTMENT,
        "dum": EntityType.HOUSE,
        "chata": EntityType.HOUSE,
        "pozemek": EntityType.LAND,
        "zahrada": EntityType.LAND,
        "kancelar": EntityType.COMMERCIAL,
        "sklad": EntityType.COMMERCIAL,
        "prostory": EntityType.COMMERCIAL,
        "restaurace": EntityType.COMMERCIAL,
        "garaz": EntityType.OTHER,
        "ostatni": EntityType.OTHER,
    }

    url: str
    raw_transaction: str
    raw_entity: str

    title: str
    description: str
    price: float | None = None
    currency: str = "czk"

    location: Location
    postal_code: str | None = None
    infrastructure: Infrastructure | None = None
    legal: LegalContext | None = None

    land_area_m2: float | None = None
    usable_area_m2: float | None = None
    built_up_area_m2: float | None = None

    material: BuildingMaterial | None = None
    condition: BuildingCondition | None = None
    energy_class: EnergyClass | None = None

    @classmethod
    def get_supported_transactions(cls) -> list[str]:
        return list(cls._TRANSACTION_MAP.keys())

    @classmethod
    def get_supported_entities(cls) -> list[str]:
        return list(cls._ENTITY_MAP.keys())

    def _extract_source_id(self) -> str:
        if match := re.search(r"/(\d+)/", self.url):
            return match.group(1)
        raise ValueError(f"Cannot extract listing ID from URL: {self.url}")

    def materialize(self) -> AnyProperty:
        if self.raw_transaction not in self._TRANSACTION_MAP:
            raise ValueError(f"Unknown transaction type: {self.raw_transaction}")
        if self.raw_entity not in self._ENTITY_MAP:
            raise ValueError(f"Unknown entity type: {self.raw_entity}")

        transaction_enum = self._TRANSACTION_MAP[self.raw_transaction]
        entity_enum = self._ENTITY_MAP[self.raw_entity]
        source_id = self._extract_source_id()

        # Stable deterministic ID derived from source + source_id
        entity_id = hashlib.sha256(
            f"{SourcePortal.BAZOS}_{source_id}".encode()
        ).hexdigest()

        dec_price = Decimal(str(self.price)) if self.price is not None else None
        currency_enum = Currency(self.currency.lower())

        base_params = {
            "id": entity_id,
            "title": self.title,
            "is_active": True,
            "source_id": source_id,
            "url": HttpUrl(self.url),
            "source": SourcePortal.BAZOS,
            "transaction_type": transaction_enum,
            "price": dec_price,
            "currency": currency_enum,
            "scraped_at": self._get_utc_now(),
            "location": self.location,
            "infrastructure": self.infrastructure,
            "legal": self.legal,
            "land_area_m2": self.land_area_m2,
            "description": self.description,
        }

        if entity_enum == EntityType.LAND:
            return LandProperty(
                **base_params,
                entity_type=entity_enum,
            )
        else:
            return BuildingProperty(
                **base_params,
                entity_type=entity_enum,
                usable_area_m2=self.usable_area_m2,
                built_up_area_m2=self.built_up_area_m2,
                material=self.material,
                condition=self.condition,
                energy_class=self.energy_class,
            )
