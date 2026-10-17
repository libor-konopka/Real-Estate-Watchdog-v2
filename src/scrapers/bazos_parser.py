import re
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from selectolax.parser import HTMLParser

from src.contracts.base import (
    BuildingCondition,
    BuildingMaterial,
    EnergyClass,
    HeatingType,
    Infrastructure,
    LegalContext,
    Location,
    OwnershipType,
    WasteType,
    WaterType,
)
from src.contracts.portals import bazos_patterns as patterns
from src.contracts.validators import sanitize_czech_price


@dataclass(slots=True, frozen=True)
class BazosListingCard:
    """Lightweight projection of an advertisement card on a catalog page."""

    source_id: str
    url: str
    price: float | None


class BazosDOMParser:
    """High-performance parser for Bazos listing pages using selectolax."""

    def __init__(self, html_content: str, url: str) -> None:
        self.tree = HTMLParser(html_content)
        self.url = url
        self._table_data: dict[str, str] = {}
        self._extract_table_rows()

    @staticmethod
    def parse_catalog_page(
        html_content: str, base_url: str = "https://reality.bazos.cz"
    ) -> list[BazosListingCard]:
        """Extracts listing cards from a pagination catalog page in a single pass."""
        tree = HTMLParser(html_content)
        cards: list[BazosListingCard] = []

        # Iterate over all listing containers in the search result
        for container in tree.css("div.inzeraty"):
            link_node = container.css_first("h2.nadpis a, span.nadpis a")
            if not link_node:
                continue

            href = link_node.attributes.get("href")
            if not href:
                continue

            full_url = (
                href if href.startswith("http") else f"{base_url.rstrip('/')}{href}"
            )

            # Extract source_id: /inzerat/19837261/prodej-bytu.php -> 19837261
            match = re.search(r"/inzerat/(\d+)/", full_url) or re.search(
                r"/(\d+)/", full_url
            )
            if not match:
                continue
            source_id = match.group(1)

            # Extract card price to allow fast-path skipping without detail fetching
            price_val: float | None = None
            if price_node := container.css_first("div.inzeratycena"):
                raw_price = price_node.text(strip=True)
                dec_price, _ = sanitize_czech_price(raw_price)
                if dec_price is not None:
                    price_val = float(dec_price)

            cards.append(
                BazosListingCard(source_id=source_id, url=full_url, price=price_val)
            )

        return cards

    def _extract_table_rows(self) -> None:
        """Parses detail table into key-value map in a single pass."""
        for row in self.tree.css("table tr"):
            cells = row.css("td, th")
            if len(cells) >= 2:
                key = cells[0].text(strip=True).lower().rstrip(":")
                val = cells[1].text(strip=True)
                self._table_data[key] = val

    def extract_title(self) -> str:
        if node := self.tree.css_first("h1.nadpisdetail"):
            return node.text(strip=True)
        return ""

    def extract_description(self) -> str:
        if node := self.tree.css_first("div.popisdetail"):
            return node.text(strip=True)
        return ""

    def extract_price(self) -> tuple[float | None, str]:
        raw_price = self._table_data.get("cena", "")
        dec_price, currency = sanitize_czech_price(raw_price)
        return (
            float(dec_price) if dec_price is not None else None,
            currency.value,
        )

    def extract_location(self) -> Location:
        """Extracts location and correctly sets postal_code within the domain model."""
        raw_loc = self._table_data.get("lokalita", "").replace("\xa0", " ").strip()
        postal_code: str | None = None
        municipality: str | None = raw_loc

        if match := patterns.RE_LOCATION.match(raw_loc):
            if match.group(1):
                postal_code = match.group(1).replace(" ", "")
            if match.group(2):
                municipality = match.group(2).strip()

        return Location(
            region=None,
            district=None,
            municipality=municipality,
            postal_code=postal_code,
        )

    def extract_infrastructure(self, desc: str) -> Infrastructure:
        infra = Infrastructure()
        has_all = bool(patterns.RE_ALL_NETWORKS.search(desc))

        # Water
        if has_all or patterns.RE_WATER_PUBLIC.search(desc):
            infra.water = WaterType.PUBLIC
        elif patterns.RE_WATER_BOREHOLE.search(desc):
            infra.water = WaterType.BOREHOLE
        elif patterns.RE_WATER_WELL.search(desc):
            infra.water = WaterType.WELL

        # Waste
        if has_all or patterns.RE_WASTE_SEWER.search(desc):
            infra.waste = WasteType.SEWER
        elif patterns.RE_WASTE_SEPTIC.search(desc):
            infra.waste = WasteType.SEPTIC
        elif patterns.RE_WASTE_CESSPOOL.search(desc):
            infra.waste = WasteType.CESSPOOL

        # Electricity
        if patterns.RE_NO_ELEC.search(desc):
            infra.electricity = False
        elif has_all or patterns.RE_ELEC.search(desc):
            infra.electricity = True

        # Gas
        infra.gas = True if (has_all or patterns.RE_GAS.search(desc)) else None

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

    def extract_legal_context(self, desc: str, title: str) -> LegalContext:
        legal = LegalContext()

        if patterns.RE_OWN_PERSONAL.search(desc):
            legal.ownership = OwnershipType.PERSONAL
        elif patterns.RE_OWN_COOP.search(desc):
            legal.ownership = OwnershipType.COOP
        elif patterns.RE_OWN_MUNI.search(desc):
            legal.ownership = OwnershipType.MUNICIPAL

        legal.is_foreclosure = bool(patterns.RE_FORECLOSURE.search(desc))
        legal.is_insolvency = bool(patterns.RE_INSOLVENCY.search(desc))
        legal.is_share = bool(patterns.RE_SHARE.search(desc))

        if match := patterns.RE_PRICE_NOTE.search(desc):
            legal.price_note = match.group(1).strip()

        for text in (desc, title):
            if not text:
                continue
            if match_avail := (
                patterns.RE_AVAILABLE_FWD.search(text)
                or patterns.RE_AVAILABLE_REV.search(text)
            ):
                val = match_avail.group(1)
                if val.lower() == "ihned":
                    legal.available_from = datetime.now(
                        ZoneInfo("Europe/Prague")
                    ).date()
                else:
                    try:
                        clean_date = re.sub(r"\s+", "", val)
                        legal.available_from = datetime.strptime(
                            clean_date, "%d.%m.%Y"
                        ).date()
                    except ValueError:
                        pass
                break

        return legal

    def extract_usable_area(self, title: str, desc: str) -> float | None:
        for text in (title, desc):
            if not text:
                continue
            match = (
                patterns.RE_USABLE_AREA_FWD.search(text)
                or patterns.RE_USABLE_AREA_REV.search(text)
                or patterns.RE_USABLE_AREA_STRUCT.search(text)
            )
            if match:
                clean_num = re.sub(r"\s+", "", match.group(1)).replace(",", ".")
                try:
                    return float(clean_num)
                except ValueError:
                    pass
        return None

    def extract_land_area(self, title: str, desc: str) -> float | None:
        total = 0.0
        found = False
        for text in (desc, title):
            if not text:
                continue
            for pattern in (
                patterns.RE_LAND_AREA_FWD,
                patterns.RE_LAND_AREA_REV,
                patterns.RE_LAND_AREA_STRUCT,
            ):
                for match in pattern.finditer(text):
                    clean_num = re.sub(r"\s+", "", match.group(1)).replace(",", ".")
                    try:
                        total += float(clean_num)
                        found = True
                    except ValueError:
                        pass
                if found:
                    return total
        return None

    def extract_built_up_area(self, title: str, desc: str) -> float | None:
        for text in (title, desc):
            if not text:
                continue
            if match := (
                patterns.RE_BUILT_UP_AREA_FWD.search(text)
                or patterns.RE_BUILT_UP_AREA_REV.search(text)
                or patterns.RE_BUILT_UP_AREA_STRUCT.search(text)
            ):
                clean_num = re.sub(r"\s+", "", match.group(1)).replace(",", ".")
                try:
                    return float(clean_num)
                except ValueError:
                    pass
        return None

    def extract_material(self, text: str) -> BuildingMaterial | None:
        if patterns.RE_MAT_BRICK.search(text):
            return BuildingMaterial.BRICK
        if patterns.RE_MAT_PANEL.search(text):
            return BuildingMaterial.PANEL
        if patterns.RE_MAT_WOOD.search(text):
            return BuildingMaterial.WOOD
        if patterns.RE_MAT_MIXED.search(text):
            return BuildingMaterial.MIXED
        return None

    def extract_condition(self, text: str) -> BuildingCondition | None:
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

    def extract_energy_class(self, text: str) -> EnergyClass | None:
        if match := patterns.RE_ENERGY_CLASS.search(text):
            letter = match.group(1).upper()
            try:
                return EnergyClass(letter)
            except ValueError:
                pass
        return None
