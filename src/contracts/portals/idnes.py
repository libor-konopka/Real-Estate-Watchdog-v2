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


class IdnesRawInput(BasePortalInput):
    _ENTITY_MAP: ClassVar[dict[str, EntityType]] = {
        "byt": EntityType.APARTMENT,
        "dum": EntityType.HOUSE,
        "pozemek": EntityType.LAND,
        "komercni": EntityType.COMMERCIAL,
        "garaze-nebo-ostatni": EntityType.OTHER,  # Zde záleží na přesném URL slugu iDNESu (např. 'ostatni')
    }
