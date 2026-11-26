from typing import Any

from loguru import logger
from pydantic import ValidationError

from src.contracts.base import BuildingProperty, LandProperty
from src.contracts.portals.bezrealitky import BezrealitkyRawInput


class BezrealitkyParser:
    """Translates raw JSON objects from Bezrealitky GraphQL into domain models."""

    @staticmethod
    def parse_advert(
        raw: dict[str, Any],
    ) -> BuildingProperty | LandProperty | None:
        """Parses a single Advert object into a materialized domain property."""
        try:
            raw_id = raw.get("id")
            if raw_id is None or not str(raw_id).strip():
                return None
            source_id = str(raw_id).strip()

            gps_data = raw.get("gps") or {}
            latitude = (
                float(gps_data["lat"]) if gps_data.get("lat") is not None else None
            )
            longitude = (
                float(gps_data["lng"]) if gps_data.get("lng") is not None else None
            )
            if latitude == 0.0 and longitude == 0.0:
                latitude = None
                longitude = None

            # GraphQL cityDistrict holds municipal ward/borough (e.g. "Příbram VII"),
            # not the official administrative district (okres), which is resolved
            # deterministically from postal_code during Silver geo-enrichment.
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
                district=None,
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
