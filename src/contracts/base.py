from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    PlainSerializer,
    TypeAdapter,
)

# Custom type: forces conversion of pydantic_core.Url to str during any model_dump()
# Prevents PyArrow/Polars from encountering unknown Python Url objects
SerializableHttpUrl = Annotated[
    HttpUrl,
    PlainSerializer(str, return_type=str, when_used="always"),
]


class SourcePortal(StrEnum):
    """Allowed source real estate portals."""

    SREALITY = "sreality"
    IDNES = "idnes"
    BEZREALITKY = "bezrealitky"
    BAZOS = "bazos"


class TransactionType(StrEnum):
    """Transaction category."""

    SALE = "sale"
    RENT = "rent"
    AUCTION = "auction"


class EntityType(StrEnum):
    """Property spatial archetype."""

    APARTMENT = "apartment"
    HOUSE = "house"
    LAND = "land"
    COMMERCIAL = "commercial"
    OTHER = "other"


class Currency(StrEnum):
    """Supported fiat currencies."""

    CZK = "czk"
    EUR = "eur"
    USD = "usd"


class GPSCoordinates(BaseModel):
    """Exact spatial point anchored by latitude and longitude."""

    lat: float
    lon: float


class Location(BaseModel):
    """Administrative area context and optional spatial coordinate."""

    region: str | None = None
    district: str | None = None
    municipality: str | None = None
    postal_code: str | None = None
    gps: GPSCoordinates | None = None


class WaterType(StrEnum):
    PUBLIC = "public_network"
    WELL = "well"
    BOREHOLE = "borehole"


class WasteType(StrEnum):
    SEWER = "public_sewer"
    SEPTIC = "septic_tank"
    CESSPOOL = "cesspool"
    TREATMENT_PLANT = "water_treatment_plant"


class HeatingType(StrEnum):
    GAS = "gas"
    ELECTRIC = "electric"
    SOLID_FUEL = "solid_fuel"
    HEAT_PUMP = "heat_pump"
    CENTRAL = "central"


class Infrastructure(BaseModel):
    """Utilities and infrastructure connections."""

    water: WaterType | None = None
    electricity: bool | None = None
    waste: WasteType | None = None
    heating: HeatingType | None = None
    gas: bool | None = None


class OwnershipType(StrEnum):
    PERSONAL = "personal"
    COOP = "coop"
    MUNICIPAL = "municipal"


class LegalContext(BaseModel):
    """Legal metadata and encumbrances."""

    ownership: OwnershipType | None = None
    price_note: str | None = None
    available_from: date | None = None
    is_foreclosure: bool = False
    is_insolvency: bool = False
    is_share: bool = False


class BuildingMaterial(StrEnum):
    BRICK = "brick"
    PANEL = "panel"
    WOOD = "wood"
    MIXED = "mixed"


class BuildingCondition(StrEnum):
    NEW = "new"
    RENOVATED = "renovated"
    BEFORE_RECONSTRUCTION = "before_reconstruction"
    GOOD = "good"
    UNDER_CONSTRUCTION = "under_construction"


class EnergyClass(StrEnum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"
    G = "G"


class BaseProperty(BaseModel):
    """Core domain contract immutable across all property types."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    # Identity
    id: str
    title: str
    is_active: bool
    source_id: str
    url: SerializableHttpUrl

    # Categorization
    source: SourcePortal
    transaction_type: TransactionType
    entity_type: EntityType

    # Financial details
    price: float | None = None
    currency: Currency | None = None

    # Time anchor (requires timezone-aware timestamp)
    scraped_at: AwareDatetime

    # Nested structures
    location: Location
    infrastructure: Infrastructure | None = None
    legal: LegalContext | None = None

    # Dimensions
    land_area_m2: float | None = None
    usable_area_m2: float | None = None
    description: str | None = None


class BuildingProperty(BaseProperty):
    """Structural property contract (apartments, houses, offices)."""

    entity_type: Literal[
        EntityType.APARTMENT,
        EntityType.HOUSE,
        EntityType.COMMERCIAL,
        EntityType.OTHER,
    ]

    built_up_area_m2: float | None = None
    material: BuildingMaterial | None = None
    condition: BuildingCondition | None = None
    energy_class: EnergyClass | None = None


class LandProperty(BaseProperty):
    """Raw land property contract."""

    entity_type: Literal[EntityType.LAND]

    land_category: str | None = None
    location_character: str | None = None
    access_road: str | None = None
    transport: str | None = None


# Polymorphic Discriminated Union representing any valid listing
PropertyListing = Annotated[
    BuildingProperty | LandProperty,
    Field(discriminator="entity_type"),
]

# High-performance TypeAdapter for validating single records or batches
property_listing_adapter: TypeAdapter[PropertyListing] = TypeAdapter(PropertyListing)
property_listing_list_adapter: TypeAdapter[list[PropertyListing]] = TypeAdapter(
    list[PropertyListing]
)


@dataclass(slots=True, frozen=True)
class ActiveListingRef:
    """Lightweight reference used strictly for HTTP reconciliation audits."""

    source_id: str
    url: str
