from typing import Any

from loguru import logger
from pydantic import ValidationError

from src.contracts.base import BuildingProperty, LandProperty
from src.contracts.portals.bezrealitky import BezrealitkyRawInput


class BezrealitkyParser:
    """Translates raw JSON objects from Bezrealitky GraphQL API into validated domain models."""

    @staticmethod
    def parse_advert(
        raw: dict[str, Any],
    ) -> BuildingProperty | LandProperty | None:
        """Parses a single Advert object from GraphQL list into a materialized domain property."""
        try:
            source_id = str(raw.get("id"))
            if not source_id:
                return None

            # Spatial coordinates extraction
            gps_data = raw.get("gps") or {}
            latitude = (
                float(gps_data["lat"]) if gps_data.get("lat") is not None else None
            )
            longitude = (
                float(gps_data["lng"]) if gps_data.get("lng") is not None else None
            )

            # Raw input validation boundary instantiation
            raw_input = BezrealitkyRawInput(
                source_id=source_id,
                uri=raw.get("uri") or f"inzerat/{source_id}",
                offer_type=raw.get("offerType") or "PRODEJ",
                estate_type=raw.get("estateType") or "BYT",
                title=raw.get("title") or "Bezrealitky inzerát",
                description=raw.get("description"),
                price=float(raw["price"]) if raw.get("price") is not None else None,
                currency=str(raw.get("currency") or "czk").lower(),
                municipality=raw.get("city"),
                district=raw.get("cityDistrict"),
                postal_code=raw.get("zip"),
                latitude=latitude,
                longitude=longitude,
                surface=float(raw["surface"])
                if raw.get("surface") is not None
                else None,
                surface_land=(
                    float(raw["surfaceLand"])
                    if raw.get("surfaceLand") is not None
                    else None
                ),
                condition=raw.get("condition"),
                construction=raw.get("construction"),
                ownership=raw.get("ownership"),
                penb=raw.get("penb"),
            )

            return raw_input.materialize()

        except (ValidationError, ValueError, TypeError, KeyError) as exc:
            logger.debug(
                f"Validation failed for Bezrealitky advert {raw.get('id')}: {exc}"
            )
            return None
