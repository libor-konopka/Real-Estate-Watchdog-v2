from typing import ClassVar

from src.contracts.base import EntityType, TransactionType

from .base import BasePortalInput


class BezrealitkyRawInput(BasePortalInput):
    """
    Boundary entity for absorbing chaotic data from the Bezrealitky portal.
    """

    # Mapování pro GraphQL API Bezrealitek -> Váš interní systém (TransactionType)
    _TRANSACTION_MAP: ClassVar[dict[str, TransactionType]] = {
        "PRODEJ": TransactionType.SALE,
        "PRONAJEM": TransactionType.RENT,
    }

    # Mapování pro GraphQL API Bezrealitek -> Váš interní systém (EntityType)
    _ENTITY_MAP: ClassVar[dict[str, EntityType]] = {
        "FLAT": EntityType.APARTMENT,
        "HOUSE": EntityType.HOUSE,
        "CHALET": EntityType.HOUSE,
        "LAND": EntityType.LAND,
        "COMMERCIAL": EntityType.COMMERCIAL,
        "GARAGE": EntityType.OTHER,
    }

    # Zpětné slovníky: Váš vstupní parametr ze Streamlitu/main.py -> GraphQL parametr
    # Toto si bude volat scraper, aby věděl, co má poslat na server.
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
