import re
from datetime import datetime
from decimal import Decimal
from typing import ClassVar
from zoneinfo import ZoneInfo

from loguru import logger
from pydantic import field_validator

# Absolute imports from immutable core system
from src.contracts.base import (
    BuildingCondition,
    BuildingMaterial,
    BuildingProperty,
    Currency,
    EnergyClass,
    EntityType,
    HeatingType,
    Infrastructure,
    LandProperty,
    LegalContext,
    Location,
    OwnershipType,
    SourcePortal,
    TransactionType,
    WasteType,
    WaterType,
)
from src.contracts.validators import sanitize_czech_price
from src.transformations.enrichment import load_psc_lookup

# Relative import from shared membrane foundation in the same folder
from . import bazos_patterns as patterns
from .base import AnyProperty, BasePortalInput


class SrealityRawInput(BasePortalInput):
    """
    Boundary entity for absorbing chaotic data from the Sreality portal.
    """

    _ENTITY_MAP: ClassVar[dict[str, EntityType]] = {
        "byty": EntityType.APARTMENT,
        "domy": EntityType.HOUSE,
        "pozemky": EntityType.LAND,
        "komercni": EntityType.COMMERCIAL,
        "ostatni": EntityType.OTHER,
        # "projekty" úmyslně vynechány – scraper je díky tomu nebude vůbec stahovat
    }
