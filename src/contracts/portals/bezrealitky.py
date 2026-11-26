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
    LandProperty,
    LegalContext,
    Location,
    OwnershipType,
    SourcePortal,
    TransactionType,
)
from src.contracts.portals.base import BasePortalInput


class BezrealitkyRawInput(BasePortalInput):
    """Validation boundary for payloads extracted from Bezrealitky GraphQL API."""

    # GraphQL API Bezrealitky -> Internal TransactionType
    _TRANSACTION_MAP: ClassVar[dict[str, TransactionType]] = {
        "PRODEJ": TransactionType.SALE,
        "PRONAJEM": TransactionType.RENT,
    }

    # Verified GraphQL API EstateType -> Internal EntityType
    _ENTITY_MAP: ClassVar[dict[str, EntityType]] = {
        "BYT": EntityType.APARTMENT,
        "DUM": EntityType.HOUSE,
        "REKREACNI_OBJEKT": EntityType.HOUSE,
        "POZEMEK": EntityType.LAND,
        "GARAZ": EntityType.OTHER,
        "KANCELAR": EntityType.COMMERCIAL,
        "NEBYTOVY_PROSTOR": EntityType.COMMERCIAL,
    }

    # API request mapping: CLI / Pipeline arguments -> GraphQL parameters
    API_TRANSACTION_MAP: ClassVar[dict[str, str]] = {
        "prodam": "PRODEJ",
        "pronajmu": "PRONAJEM",
    }

    # CLI / Pipeline arguments -> Verified GraphQL EstateType enum
    API_ENTITY_MAP: ClassVar[dict[str, str]] = {
        "byt": "BYT",
        "dum": "DUM",
        "chaty-a-chalupy": "REKREACNI_OBJEKT",
        "pozemek": "POZEMEK",
        "kancelar": "KANCELAR",
        "nebytovy-prostor": "NEBYTOVY_PROSTOR",
        "garaz": "GARAZ",
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

    # Raw payload attributes from Bezrealitky API
    source_id: str
    uri: str
    offer_type: str
    estate_type: str

    title: str
    description: str | None = None
    price: float | None = None
    currency: str = "czk"

    # Precise spatial attributes
    district: str | None = None
    municipality: str | None = None
    postal_code: str | None = None
    latitude: float | None = None
    longitude: float | None = None

    # Physical dimensions in m2
    surface: float | None = None
    surface_land: float | None = None

    # Technical parameters
    condition: str | None = None
    construction: str | None = None
    ownership: str | None = None
    penb: str | None = None

    @classmethod
    def get_supported_transactions(cls) -> list[str]:
        """Returns supported human-readable transaction keys."""
        return list(cls.API_TRANSACTION_MAP.keys())

    @classmethod
    def get_supported_entities(cls) -> list[str]:
        """Returns supported human-readable entity keys."""
        return list(cls.API_ENTITY_MAP.keys())

    @classmethod
    def resolve_enums(
        cls, transaction: str, entity: str
    ) -> tuple[TransactionType, EntityType]:
        """Resolves CLI/pipeline transaction and entity slugs into domain enums."""
        if transaction not in cls.API_TRANSACTION_MAP:
            raise ValueError(f"Unsupported Bezrealitky transaction: {transaction}")
        if entity not in cls.API_ENTITY_MAP:
            raise ValueError(f"Unsupported Bezrealitky entity: {entity}")

        api_tx = cls.API_TRANSACTION_MAP[transaction]
        api_ent = cls.API_ENTITY_MAP[entity]
        return cls._TRANSACTION_MAP[api_tx], cls._ENTITY_MAP[api_ent]

    def _build_location(self) -> Location:
        """Constructs a validated domain Location entity with optional coordinates."""
        gps = None
        if self.latitude is not None and self.longitude is not None:
            gps = GPSCoordinates(lat=self.latitude, lon=self.longitude)

        clean_postal = self.postal_code.replace(" ", "") if self.postal_code else None

        return Location(
            district=self.district,
            municipality=self.municipality,
            postal_code=clean_postal,
            gps=gps,
        )

    def _build_legal_context(self) -> LegalContext:
        """Translates raw ownership into domain LegalContext."""
        ownership_enum = (
            self._OWNERSHIP_MAP[self.ownership]
            if self.ownership and self.ownership in self._OWNERSHIP_MAP
            else None
        )

        return LegalContext(ownership=ownership_enum)

    def materialize(self) -> BuildingProperty | LandProperty:
        """Converts validated raw API payload into an immutable domain model."""
        if self.offer_type not in self._TRANSACTION_MAP:
            raise ValueError(f"Unsupported transaction type: {self.offer_type}")
        if self.estate_type not in self._ENTITY_MAP:
            raise ValueError(f"Unsupported entity type: {self.estate_type}")

        transaction_enum = self._TRANSACTION_MAP[self.offer_type]
        entity_enum = self._ENTITY_MAP[self.estate_type]
        entity_id = self.generate_entity_id(SourcePortal.BEZREALITKY, self.source_id)

        full_url = (
            self.uri
            if self.uri.startswith("http")
            else (
                "https://www.bezrealitky.cz/nemovitosti-byty-domy/"
                f"{self.uri.lstrip('/')}"
            )
        )

        currency_enum = (
            Currency(self.currency.lower())
            if self.currency.lower() in Currency
            else Currency.CZK
        )

        listing_url = HttpUrl(full_url)
        scraped_at = self._get_utc_now()
        location = self._build_location()
        legal = self._build_legal_context()

        if entity_enum == EntityType.LAND:
            return LandProperty(
                id=entity_id,
                title=self.title,
                is_active=True,
                source_id=self.source_id,
                url=listing_url,
                source=SourcePortal.BEZREALITKY,
                transaction_type=transaction_enum,
                entity_type=EntityType.LAND,
                price=self.price,
                currency=currency_enum,
                scraped_at=scraped_at,
                location=location,
                infrastructure=None,
                legal=legal,
                land_area_m2=self.surface_land,
                usable_area_m2=self.surface,
                description=self.description,
            )

        condition_enum = self._CONDITION_MAP.get(self.condition or "")
        material_enum = self._MATERIAL_MAP.get(self.construction or "")
        energy_enum = (
            EnergyClass(self.penb.upper())
            if self.penb and self.penb.upper() in EnergyClass
            else None
        )

        return BuildingProperty(
            id=entity_id,
            title=self.title,
            is_active=True,
            source_id=self.source_id,
            url=listing_url,
            source=SourcePortal.BEZREALITKY,
            transaction_type=transaction_enum,
            entity_type=entity_enum,
            price=self.price,
            currency=currency_enum,
            scraped_at=scraped_at,
            location=location,
            infrastructure=None,
            legal=legal,
            land_area_m2=self.surface_land,
            usable_area_m2=self.surface,
            description=self.description,
            material=material_enum,
            condition=condition_enum,
            energy_class=energy_enum,
        )
