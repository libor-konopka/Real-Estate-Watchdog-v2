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


class BazosRawInput(BasePortalInput):
    """
    Boundary entity for absorbing chaotic data from the Bazos portal.
    """

    _TRANSACTION_MAP: ClassVar[dict[str, TransactionType]] = {
        "prodam": TransactionType.SALE,
        "pronajmu": TransactionType.RENT,
    }

    _ENTITY_MAP: ClassVar[dict[str, EntityType]] = {
        "byt": EntityType.APARTMENT,
        "dum": EntityType.HOUSE,
        "chata": EntityType.HOUSE,
        "pozemek": EntityType.LAND,
        "zahrada": EntityType.LAND,
        "kancelar": EntityType.COMMERCIAL,
        "sklad": EntityType.COMMERCIAL,
        "prostory": EntityType.COMMERCIAL,
        "restaurace": EntityType.COMMERCIAL,
        "garaz": EntityType.OTHER,
        "ostatni": EntityType.OTHER,
    }

    raw_transaction: str
    raw_entity: str

    raw_title: str
    raw_location: str
    raw_price: str | None = None
    raw_description: str
    raw_url: str

    @field_validator("raw_price", mode="before")
    @classmethod
    def validate_price(cls, value: str) -> str | None:
        return sanitize_czech_price(value)

    @classmethod
    def get_supported_transactions(cls) -> list[str]:
        return list(cls._TRANSACTION_MAP.keys())

    @classmethod
    def get_supported_entities(cls) -> list[str]:
        return list(cls._ENTITY_MAP.keys())

    def _extract_source_id(self) -> str:
        """Extracts the exact numeric ID from the Bazos URL path."""
        if match := re.search(r"/(\d+)/", self.raw_url):
            return match.group(1)
        raise ValueError(f"Cannot extract listing ID from URL: {self.raw_url}")

    def _parse_location(self) -> Location:
        """
        Extracts the postal code and municipality name from bronze text
        and enriches them with the region and district.
        """
        raw = self.raw_location.replace("\xa0", " ").strip()

        if (match := patterns.RE_LOCATION.match(raw)) and match.group(1):
            psc = match.group(1).replace(" ", "")
            municipality_raw = match.group(2).strip()

            lookup = load_psc_lookup()

            if psc in lookup:
                geo_data = lookup[psc]
                return Location(
                    region=geo_data["region"],
                    district=geo_data["district"],
                    municipality=geo_data["municipality"] or municipality_raw,
                )

        return Location(region=None, district=None, municipality=raw)

    def _parse_infrastructure(self) -> Infrastructure:
        """Scans the description text and maps patterns to infrastructure enums."""
        desc = self.raw_description
        infra = Infrastructure()

        has_all_networks = bool(patterns.RE_ALL_NETWORKS.search(desc))

        # Water
        if has_all_networks or patterns.RE_WATER_PUBLIC.search(desc):
            infra.water = WaterType.PUBLIC
        elif patterns.RE_WATER_BOREHOLE.search(desc):
            infra.water = WaterType.BOREHOLE
        elif patterns.RE_WATER_WELL.search(desc):
            infra.water = WaterType.WELL

        # Waste
        if has_all_networks or patterns.RE_WASTE_SEWER.search(desc):
            infra.waste = WasteType.SEWER
        elif patterns.RE_WASTE_SEPTIC.search(desc):
            infra.waste = WasteType.SEPTIC
        elif patterns.RE_WASTE_CESSPOOL.search(desc):
            infra.waste = WasteType.CESSPOOL

        # Electricity
        if patterns.RE_NO_ELEC.search(desc):
            infra.electricity = False
        elif has_all_networks or patterns.RE_ELEC.search(desc):
            infra.electricity = True
        else:
            infra.electricity = None

        # Gas
        infra.gas = True if (has_all_networks or patterns.RE_GAS.search(desc)) else None

        # Heating
        if patterns.RE_HEAT_PUMP.search(desc):
            infra.heating = HeatingType.HEAT_PUMP
        elif patterns.RE_HEAT_GAS.search(desc):
            infra.heating = HeatingType.GAS
        elif patterns.RE_HEAT_ELEC.search(desc):
            infra.heating = HeatingType.ELECTRIC
        elif patterns.RE_HEAT_SOLID.search(desc):
            infra.heating = HeatingType.SOLID_FUEL
        elif patterns.RE_HEAT_CENTRAL.search(desc):
            infra.heating = HeatingType.CENTRAL

        return infra

    def _parse_legal_context(self) -> LegalContext:
        desc = self.raw_description
        legal = LegalContext()

        # Ownership type
        if patterns.RE_OWN_PERSONAL.search(desc):
            legal.ownership = OwnershipType.PERSONAL
        elif patterns.RE_OWN_COOP.search(desc):
            legal.ownership = OwnershipType.COOP
        elif patterns.RE_OWN_MUNI.search(desc):
            legal.ownership = OwnershipType.MUNICIPAL

        # Boolean flags
        legal.is_foreclosure = bool(patterns.RE_FORECLOSURE.search(desc))
        legal.is_insolvency = bool(patterns.RE_INSOLVENCY.search(desc))
        legal.is_share = bool(patterns.RE_SHARE.search(desc))

        # Price note
        if price_match := patterns.RE_PRICE_NOTE.search(desc):
            legal.price_note = price_match.group(1).strip()

        # Availability date
        for text in (desc, self.raw_title):
            if not text:
                continue

            if match_avail := (
                patterns.RE_AVAILABLE_FWD.search(text)
                or patterns.RE_AVAILABLE_REV.search(text)
            ):
                val = match_avail.group(1)
                if val.lower() == "ihned":
                    legal.available_from = datetime.now(
                        tz=ZoneInfo("Europe/Prague")
                    ).date()
                else:
                    try:
                        clean_date = val.replace(" ", "")
                        legal.available_from = (
                            datetime.strptime(clean_date, "%d.%m.%Y")
                            .replace(tzinfo=ZoneInfo("Europe/Prague"))
                            .date()
                        )
                    except ValueError:
                        logger.warning(
                            f"Failed to parse availability date: '{val}' in listing {self.raw_url}"
                        )
                break

        return legal

    def _parse_land_area(self) -> float | None:
        """Extracts land area in square meters."""
        for text in (self.raw_description, self.raw_title):
            if not text:
                continue

            if match := (
                patterns.RE_LAND_AREA_FWD.search(text)
                or patterns.RE_LAND_AREA_REV.search(text)
                or patterns.RE_LAND_AREA_STRUCT.search(text)
            ):
                clean_number = re.sub(r"\s+", "", match.group(1)).replace(",", ".")
                try:
                    return float(clean_number)
                except ValueError:
                    logger.warning(
                        f"Failed to parse land area: '{match.group(1)}' in {self.raw_url}"
                    )

        return None

    def _parse_usable_area(self) -> float | None:
        """Extracts usable/floor area in square meters."""
        for text in (self.raw_title, self.raw_description):
            if not text:
                continue

            match = (
                patterns.RE_USABLE_AREA_FWD.search(text)
                or patterns.RE_USABLE_AREA_REV.search(text)
                or patterns.RE_USABLE_AREA_STRUCT.search(text)
            )

            if not match:
                for pattern in (
                    patterns.RE_USABLE_AREA_SIMPLE,
                    patterns.RE_USABLE_AREA_DISP,
                    patterns.RE_USABLE_AREA_START,
                ):
                    for m in pattern.finditer(text):
                        start_idx = max(0, m.start() - 35)
                        context = text[start_idx : m.start()].lower()

                        if not any(
                            kw in context
                            for kw in ("pozem", "zahrad", "parcel", "dvůr", "dvor")
                        ):
                            match = m
                            break
                    if match:
                        break

            if match:
                clean_number = re.sub(r"\s+", "", match.group(1)).replace(",", ".")
                try:
                    return float(clean_number)
                except ValueError:
                    logger.warning(
                        f"Failed to parse usable area: '{match.group(1)}' in {self.raw_url}"
                    )

        return None

    def _parse_built_up_area(self) -> float | None:
        """Extracts built-up area in square meters."""
        for text in (self.raw_title, self.raw_description):
            if not text:
                continue

            if match := (
                patterns.RE_BUILT_UP_AREA_FWD.search(text)
                or patterns.RE_BUILT_UP_AREA_REV.search(text)
                or patterns.RE_BUILT_UP_AREA_STRUCT.search(text)
            ):
                clean_number = re.sub(r"\s+", "", match.group(1)).replace(",", ".")
                try:
                    return float(clean_number)
                except ValueError:
                    logger.warning(
                        f"Failed to parse built-up area: '{match.group(1)}' in {self.raw_url}"
                    )

        return None

    def _parse_material(self) -> BuildingMaterial | None:
        """Extracts the primary building material."""
        for text in (self.raw_title, self.raw_description):
            if not text:
                continue

            if patterns.RE_MAT_BRICK.search(text):
                return BuildingMaterial.BRICK
            if patterns.RE_MAT_PANEL.search(text):
                return BuildingMaterial.PANEL
            if patterns.RE_MAT_WOOD.search(text):
                return BuildingMaterial.WOOD
            if patterns.RE_MAT_MIXED.search(text):
                return BuildingMaterial.MIXED

        return None

    def _parse_condition(self) -> BuildingCondition | None:
        """Extracts the condition of the building/apartment."""
        for text in (self.raw_title, self.raw_description):
            if not text:
                continue

            if patterns.RE_COND_NEW.search(text):
                return BuildingCondition.NEW
            if patterns.RE_COND_BEFORE_RECON.search(text):
                return BuildingCondition.BEFORE_RECONSTRUCTION
            if patterns.RE_COND_RENOVATED.search(text):
                return BuildingCondition.RENOVATED
            if patterns.RE_COND_CONSTRUCTION.search(text):
                return BuildingCondition.UNDER_CONSTRUCTION
            if patterns.RE_COND_GOOD.search(text):
                return BuildingCondition.GOOD

        return None

    def _parse_energy_class(self) -> EnergyClass | None:
        """Extracts the energy class (PENB)."""
        for text in (self.raw_title, self.raw_description):
            if not text:
                continue

            if match := patterns.RE_ENERGY_CLASS.search(text):
                letter = match.group(1).upper()
                try:
                    return EnergyClass(letter)
                except ValueError:
                    logger.warning(
                        f"Failed to parse energy class: '{letter}' in {self.raw_url}"
                    )

        return None

    def materialize(self) -> AnyProperty:
        """
        Transmutes the raw Bazoš context into a strictly typed core property model.
        Validates mapped entities and orchestrates the localized extraction pipeline.
        """
        if self.raw_transaction not in self._TRANSACTION_MAP:
            raise ValueError(f"Neznámý typ transakce: {self.raw_transaction}")
        transaction_essence = self._TRANSACTION_MAP[self.raw_transaction]

        if self.raw_entity not in self._ENTITY_MAP:
            raise ValueError(f"Neznámý typ entity: {self.raw_entity}")
        entity_essence = self._ENTITY_MAP[self.raw_entity]

        final_price = Decimal(self.raw_price) if self.raw_price else None
        entity_id = self._generate_id(self.raw_url)
        source_identifier = self._extract_source_id()
        absolute_time = self._get_utc_now()

        location = self._parse_location()
        infrastructure = self._parse_infrastructure()
        legal = self._parse_legal_context()
        land_area = self._parse_land_area()

        usable_area = None
        built_up_area = None
        material = None
        condition = None
        energy_class = None

        if entity_essence != EntityType.LAND:
            usable_area = self._parse_usable_area()
            material = self._parse_material()
            condition = self._parse_condition()
            energy_class = self._parse_energy_class()

        if entity_essence not in (EntityType.LAND, EntityType.APARTMENT):
            built_up_area = self._parse_built_up_area()

        if entity_essence == EntityType.LAND:
            return LandProperty(
                id=entity_id,
                title=self.raw_title,
                is_active=True,
                source_id=source_identifier,
                url=self.raw_url,
                source=SourcePortal.BAZOS,
                transaction_type=transaction_essence,
                entity_type=entity_essence,
                price=final_price,
                currency=Currency.CZK,
                scraped_at=absolute_time,
                location=location,
                infrastructure=infrastructure,
                legal=legal,
                land_area_m2=land_area,
                description=self.raw_description,
            )
        else:
            return BuildingProperty(
                id=entity_id,
                title=self.raw_title,
                is_active=True,
                source_id=source_identifier,
                url=self.raw_url,
                source=SourcePortal.BAZOS,
                transaction_type=transaction_essence,
                entity_type=entity_essence,
                price=final_price,
                currency=Currency.CZK,
                scraped_at=absolute_time,
                location=location,
                infrastructure=infrastructure,
                legal=legal,
                land_area_m2=land_area,
                usable_area_m2=usable_area,
                built_up_area_m2=built_up_area,
                material=material,
                condition=condition,
                energy_class=energy_class,
                description=self.raw_description,
            )
