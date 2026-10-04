from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, HttpUrl


class SourcePortal(StrEnum):
    """
    Defines the allowed source portals for data extraction.
    Values are strictly lowercase ASCII to ensure frictionless
    integration with data lakes, Parquet files, and Polars.
    """

    SREALITY = "sreality"
    IDNES = "idnes"
    BEZREALITKY = "bezrealitky"
    BAZOS = "bazos"


class TransactionType(StrEnum):
    """Defines the nature of the financial transfer."""

    SALE = "sale"
    RENT = "rent"
    AUCTION = "auction"


class EntityType(StrEnum):
    """Defines the physical nature of the space."""

    APARTMENT = "apartment"
    HOUSE = "house"
    LAND = "land"
    COMMERCIAL = "commercial"
    OTHER = "other"


class Currency(StrEnum):
    """Defines the accepted fiat currency."""

    CZK = "czk"
    EUR = "eur"
    USD = "usd"


class GPSCoordinates(BaseModel):
    """
    Exact spatial point defined by latitude and longitude.
    These axes form an inseparable duality; both must be present to anchor the entity in reality.
    """

    lat: float
    lon: float


class Location(BaseModel):
    """
    Macro-space representation including administrative divisions.
    Embraces the void by allowing the exact spatial anchor (gps) to be absent
    without breaking the internal integrity of the system.
    """

    region: str | None = None
    district: str | None = None
    municipality: str | None = None
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
    """
    Represents the energy and material flows connected to the property.
    Embraces the reality of off-grid environments and permaculture systems
    where external dependencies may be entirely absent or unlisted.
    """

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
    """
    Anchors the property within the legal and social framework of human society.
    Captures the form of ownership and the underlying financial energy often
    hidden in price notes (e.g., commissions or VAT). Embraces the void of
    missing information by making all fields optional.
    """

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
    """
    The absolute core of the data contract.
    Unifies identity, space, energy, and time into a single, immutable entity.
    Acts as an impenetrable shield against external data chaos.
    """

    # Model immune system: absolute rejection of unknown anomalies and state mutations
    model_config = ConfigDict(extra="forbid", frozen=True)

    # Identity
    id: str
    title: str
    is_active: bool
    source_id: str
    url: HttpUrl

    # Categorization
    source: SourcePortal
    transaction_type: TransactionType
    entity_type: EntityType

    # Financial energy
    price: Decimal | None = None
    currency: Currency | None = None

    # Time anchor (requires timezone-aware object)
    scraped_at: AwareDatetime

    # Internal composition
    location: Location
    infrastructure: Infrastructure | None = None
    legal: LegalContext | None = None

    land_area_m2: float | None = None
    usable_area_m2: float | None = None
    description: str | None = None


class BuildingProperty(BaseProperty):
    """
    Physical manifestation of a structure anchored in space.
    Inherits the universal core and adds structural attributes.
    """

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
    """
    Raw, unformed earth energy representing the landscape.
    Inherits the universal core and adds terrain characteristics.
    """

    entity_type: Literal[EntityType.LAND]

    land_category: str | None = None
    location_character: str | None = None
    access_road: str | None = None
    transport: str | None = None
