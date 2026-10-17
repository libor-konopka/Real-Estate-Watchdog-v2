from datetime import date, datetime
from typing import Any

from loguru import logger

from src.contracts.base import BuildingProperty, LandProperty
from src.contracts.portals.bezrealitky import BezrealitkyRawInput


def _to_float(value: Any) -> float | None:
    """Safely converts incoming scalar to float, returning None on missing or invalid data."""
    if value is None:
        return None
    try:
        return float(value)
    except ValueError, TypeError:
        return None


class BezrealitkyParser:
    """Parser and normalizer for raw JSON payloads returned by Bezrealitky GraphQL API."""

    @staticmethod
    def parse_advert(
        advert: dict[str, Any],
    ) -> BuildingProperty | LandProperty | None:
        """Transforms a raw GraphQL estate dictionary into a strictly typed domain entity."""
        advert_id = str(advert.get("id", ""))
        if not advert_id or advert_id == "None":
            return None

        try:
            # 1. Resolve raw category values from GraphQL response
            raw_offer_type = str(advert.get("offerType") or "")
            raw_estate_type = str(advert.get("estateType") or "")

            # Check compatibility against input contract mappings
            if (
                raw_offer_type not in BezrealitkyRawInput._TRANSACTION_MAP
                or raw_estate_type not in BezrealitkyRawInput._ENTITY_MAP
            ):
                logger.debug(
                    f"Skipping listing {advert_id}: Unsupported category "
                    f"({raw_offer_type}/{raw_estate_type})"
                )
                return None

            # 2. Extract address components and GPS coordinates
            raw_gps = advert.get("gps")
            gps_node: dict[str, Any] = raw_gps if isinstance(raw_gps, dict) else {}

            latitude = _to_float(gps_node.get("lat"))
            longitude = _to_float(gps_node.get("lng"))

            # Bezrealitky GraphQL returns address as a string or structured dict
            address_val = advert.get("address")
            municipality: str | None = None
            postal_code: str | None = None
            region: str | None = None
            district: str | None = None

            if isinstance(address_val, dict):
                municipality = address_val.get("city")
                postal_code = address_val.get("zip")
                region = address_val.get("region")
                district = address_val.get("district")
            elif isinstance(address_val, str):
                municipality = address_val.strip()

            # 3. Parse availability date (handles YYYY-MM-DD or ISO datetime strings)
            available_from_raw = advert.get("availableFrom")
            available_date: date | None = None
            if available_from_raw:
                try:
                    date_str = str(available_from_raw).split("T")[0]
                    available_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError, TypeError:
                    logger.debug(f"Failed to parse availableFrom: {available_from_raw}")

            # 4. Extract infrastructure tags
            raw_tags = advert.get("tags")
            tags = raw_tags if isinstance(raw_tags, list) else []

            has_electricity = (
                True if "ELECTRICITY" in tags or advert.get("hasElectricity") else None
            )
            has_gas = True if "GAS" in tags or advert.get("hasGas") else None
            has_water = True if "WATER" in tags or advert.get("hasWater") else None
            has_waste = True if "SEWER" in tags or advert.get("hasWaste") else None

            # 5. Populate validation boundary model
            raw_input = BezrealitkyRawInput(
                source_id=advert_id,
                uri=str(advert.get("uri") or f"nemovitost/{advert_id}"),
                offer_type=raw_offer_type,
                estate_type=raw_estate_type,
                title=str(
                    advert.get("name") or advert.get("title") or "Bezrealitky listing"
                ),
                description=advert.get("description"),
                price=_to_float(advert.get("price")),
                currency=str(advert.get("currency") or "czk").lower(),
                region=region,
                district=district,
                municipality=municipality,
                postal_code=postal_code,
                latitude=latitude,
                longitude=longitude,
                surface=_to_float(advert.get("surface")),
                surface_land=_to_float(advert.get("surfaceLand")),
                condition=advert.get("condition"),
                construction=advert.get("construction"),
                ownership=advert.get("ownership"),
                penb=advert.get("penb"),
                available_from=available_date,
                has_water=has_water,
                has_electricity=has_electricity,
                has_gas=has_gas,
                has_waste=has_waste,
            )

            # 6. Materialize into strict domain model (BuildingProperty / LandProperty)
            return raw_input.materialize()

        except Exception as e:
            logger.error(f"Failed to parse Bezrealitky advert node {advert_id}: {e}")
            return None
