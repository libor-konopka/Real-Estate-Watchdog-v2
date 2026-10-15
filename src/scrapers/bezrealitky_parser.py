from typing import Any

from loguru import logger

from src.contracts.base import (
    BuildingProperty,
    EntityType,
    LandProperty,
    Location,
    SourcePortal,
)
from src.contracts.portals.bezrealitky import BezrealitkyRawInput


def parse_bezrealitky_advert(
    advert: dict[str, Any],
) -> BuildingProperty | LandProperty | None:
    """
    Přetransformuje surový JSON záznam z GraphQL Bezrealitek do striktního Pydantic modelu.
    """
    advert_id = str(advert.get("id"))
    if not advert_id or advert_id == "None":
        return None

    try:
        # 1. Překlad kategorií pomocí kontraktu (Single Source of Truth)
        offer_type = advert.get("offerType")
        estate_type = advert.get("estateType")

        transaction_type = BezrealitkyRawInput._TRANSACTION_MAP.get(offer_type)
        entity_type = BezrealitkyRawInput._ENTITY_MAP.get(estate_type)

        if not transaction_type or not entity_type:
            logger.debug(
                f"Ignoruji inzerát {advert_id}: Nepodporovaná kategorie ({offer_type}/{estate_type})"
            )
            return None

        # 2. Sestavení základních atributů a URL
        uri = advert.get("uri", "")
        url = f"https://www.bezrealitky.cz/nemovitosti-byty-domy/{uri}" if uri else ""

        price = advert.get("price")
        # Někdy Bezrealitky posílají null (např. při "cena dohodou") - v takovém případě předáme None
        # a validator ve vašem Pydantic modelu si to přebere.

        # 3. Zpracování lokace (API vrací přímo GPS a textovou adresu)
        gps = advert.get("gps") or {}
        address_raw = advert.get("address", "")

        location = Location(address=address_raw, lat=gps.get("lat"), lon=gps.get("lng"))

        # 4. Zpracování výměry
        surface = advert.get("surface")
        surface_land = advert.get("surfaceLand")

        # 5. Inicializace příslušné doménové entity
        if entity_type == EntityType.LAND:
            return LandProperty(
                source_id=advert_id,
                source=SourcePortal.BEZREALITKY,
                url=url,
                transaction_type=transaction_type,
                entity_type=entity_type,
                price=price,
                location=location,
                # U pozemků API často posílá výměru v 'surface' místo 'surfaceLand'
                land_area_m2=surface_land or surface,
                is_active=True,
            )
        else:
            return BuildingProperty(
                source_id=advert_id,
                source=SourcePortal.BEZREALITKY,
                url=url,
                transaction_type=transaction_type,
                entity_type=entity_type,
                price=price,
                location=location,
                usable_area_m2=surface,
                land_area_m2=surface_land,
                is_active=True,
            )

    except Exception as e:
        logger.error(
            f"Chyba při parsování inzerátu z Bezrealitek (ID: {advert_id}): {e}"
        )
        return None
