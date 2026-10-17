import hashlib
from datetime import UTC, date, datetime, timezone
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
    GPSCoordinates,
    Infrastructure,
    LandProperty,
    LegalContext,
    Location,
    OwnershipType,
    SourcePortal,
    TransactionType,
)
from src.contracts.portals.base import AnyProperty, BasePortalInput


class BezrealitkyRawInput(BasePortalInput):
    """Validation boundary for structured data payloads extracted from Bezrealitky GraphQL API."""

    # GraphQL API Bezrealitky -> Internal TransactionType
    _TRANSACTION_MAP: ClassVar[dict[str, TransactionType]] = {
        "PRODEJ": TransactionType.SALE,
        "PRONAJEM": TransactionType.RENT,
    }

    # GraphQL API Bezrealitky -> Internal EntityType
    _ENTITY_MAP: ClassVar[dict[str, EntityType]] = {
        "FLAT": EntityType.APARTMENT,
        "HOUSE": EntityType.HOUSE,
        "CHALET": EntityType.HOUSE,
        "LAND": EntityType.LAND,
        "COMMERCIAL": EntityType.COMMERCIAL,
        "GARAGE": EntityType.OTHER,
    }

    # API request mapping: CLI / Pipeline arguments -> GraphQL parameters
    API_TRANSACTION_MAP: ClassVar[dict[str, str]] = {
        "prodam": "PRODEJ",
        "pronajmu": "PRONAJEM",
    }

    API_ENTITY_MAP: ClassVar[dict[str, str]] = {
        "byt": "FLAT",
        "dum": "HOUSE",
        "chaty-a-chalupy": "CHALET",
        "pozemek": "LAND",
        "kancelar": "COMMERCIAL",
        "nebytovy-prostor": "COMMERCIAL",
        "garaz": "GARAGE",
    }

    # Mapping of Bezrealitky technical parameters to internal domain enums
    _CONDITION_MAP: ClassVar[dict[str, BuildingCondition]] = {
        "VERY_GOOD": BuildingCondition.GOOD,
        "GOOD": BuildingCondition.GOOD,
        "NEW": BuildingCondition.NEW,
        "BEFORE_RECONSTRUCTION": BuildingCondition.BEFORE_RECONSTRUCTION,
        "UNDER_CONSTRUCTION": BuildingCondition.UNDER_CONSTRUCTION,
        "RECONSTRUCTION": BuildingCondition.RENOVATED,
    }

    _MATERIAL_MAP: ClassVar[dict[str, BuildingMaterial]] = {
        "BRICK": BuildingMaterial.BRICK,
        "PANEL": BuildingMaterial.PANEL,
        "WOOD": BuildingMaterial.WOOD,
        "MIXED": BuildingMaterial.MIXED,
        "OTHER": BuildingMaterial.MIXED,
    }

    _OWNERSHIP_MAP: ClassVar[dict[str, OwnershipType]] = {
        "PERSONAL": OwnershipType.PERSONAL,
        "COOP": OwnershipType.COOP,
        "STATE": OwnershipType.MUNICIPAL,
    }

    # 1. Raw payload attributes received from GraphQL response
    source_id: str
    uri: str
    offer_type: str
    estate_type: str

    title: str
    description: str | None = None
    price: float | None = None
    currency: str = "czk"

    # 2. Spatial attributes and coordinates
    region: str | None = None
    district: str | None = None
    municipality: str | None = None
    postal_code: str | None = None
    latitude: float | None = None
    longitude: float | None = None

    # 3. Dimensional surface areas in m2
    surface: float | None = None
    surface_land: float | None = None

    # 4. Technical construction and legal parameters
    condition: str | None = None
    construction: str | None = None
    ownership: str | None = None
    penb: str | None = None
    available_from: date | None = None

    # 5. Infrastructure indicators
    has_water: bool | None = None
    has_electricity: bool | None = None
    has_gas: bool | None = None
    has_waste: bool | None = None

    @classmethod
    def get_supported_transactions(cls) -> list[str]:
        """Returns supported human-readable transaction keys."""
        return list(cls.API_TRANSACTION_MAP.keys())

    @classmethod
    def get_supported_entities(cls) -> list[str]:
        """Returns supported human-readable entity keys."""
        return list(cls.API_ENTITY_MAP.keys())

    def _build_location(self) -> Location:
        """Constructs a validated domain Location entity with optional GPS coordinates."""
        gps = None
        if self.latitude is not None and self.longitude is not None:
            gps = GPSCoordinates(lat=self.latitude, lon=self.longitude)

        clean_postal = self.postal_code.replace(" ", "") if self.postal_code else None

        return Location(
            region=self.region,
            district=self.district,
            municipality=self.municipality,
            postal_code=clean_postal,
            gps=gps,
        )

    def _build_infrastructure(self) -> Infrastructure | None:
        """Constructs an infrastructure context if utility indicators are present."""
        if not any(
            v is not None
            for v in (
                self.has_water,
                self.has_electricity,
                self.has_gas,
                self.has_waste,
            )
        ):
            return None

        return Infrastructure(
            electricity=self.has_electricity,
            gas=self.has_gas,
        )

    def _build_legal_context(self) -> LegalContext:
        """Translates raw ownership and availability into domain LegalContext."""
        ownership_enum = None
        if self.ownership and self.ownership in self._OWNERSHIP_MAP:
            ownership_enum = self._OWNERSHIP_MAP[self.ownership]

        return LegalContext(
            ownership=ownership_enum,
            available_from=self.available_from,
        )

    def materialize(self) -> AnyProperty:
        """Converts validated raw API payload into an immutable domain property model."""
        if self.offer_type not in self._TRANSACTION_MAP:
            raise ValueError(f"Unsupported transaction type: {self.offer_type}")
        if self.estate_type not in self._ENTITY_MAP:
            raise ValueError(f"Unsupported entity type: {self.estate_type}")

        transaction_enum = self._TRANSACTION_MAP[self.offer_type]
        entity_enum = self._ENTITY_MAP[self.estate_type]

        # Deterministic Lakehouse entity ID: sha256(BEZREALITKY_source_id)
        entity_id = hashlib.sha256(
            f"{SourcePortal.BEZREALITKY.value}_{self.source_id}".encode()
        ).hexdigest()

        full_url = (
            self.uri
            if self.uri.startswith("http")
            else f"https://www.bezrealitky.cz/nemovitosti-byty-domy/{self.uri.lstrip('/')}"
        )

        dec_price = Decimal(str(self.price)) if self.price is not None else None
        currency_enum = (
            Currency(self.currency.lower())
            if self.currency.lower() in Currency
            else Currency.CZK
        )

        base_params = {
            "id": entity_id,
            "title": self.title,
            "is_active": True,
            "source_id": self.source_id,
            "url": HttpUrl(full_url),
            "source": SourcePortal.BEZREALITKY,
            "transaction_type": transaction_enum,
            "price": dec_price,
            "currency": currency_enum,
            "scraped_at": datetime.now(UTC),
            "location": self._build_location(),
            "infrastructure": self._build_infrastructure(),
            "legal": self._build_legal_context(),
            "land_area_m2": self.surface_land,
            "usable_area_m2": self.surface,
            "description": self.description,
        }

        if entity_enum == EntityType.LAND:
            return LandProperty(
                **base_params,
                entity_type=entity_enum,
            )

        condition_enum = self._CONDITION_MAP.get(self.condition or "")
        material_enum = self._MATERIAL_MAP.get(self.construction or "")
        energy_enum = (
            EnergyClass(self.penb.upper())
            if self.penb and self.penb.upper() in EnergyClass
            else None
        )

        return BuildingProperty(
            **base_params,
            entity_type=entity_enum,
            material=material_enum,
            condition=condition_enum,
            energy_class=energy_enum,
        )
