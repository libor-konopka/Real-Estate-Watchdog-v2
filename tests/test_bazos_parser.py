import pytest

from src.contracts.base import (
    BuildingCondition,
    BuildingMaterial,
    EnergyClass,
    OwnershipType,
)
from src.scrapers.bazos_parser import BazosDOMParser

CATALOG_HTML = """
<div class="inzeraty inzeratyflex">
    <div class="inzeratynadpis">
        <h2 class="nadpis">
            <a href="/inzerat/19837261/prodej-bytu-3kk.php">Prodej bytu 3+kk 74 m²</a>
        </h2>
    </div>
    <div class="inzeratycena"><b>4 850 000 Kč</b></div>
</div>
<div class="inzeraty inzeratyflex">
    <div class="inzeratynadpis">
        <h2 class="nadpis">
            <a href="/inzerat/19837262/pronajem-garaze.php">Pronájem garáže</a>
        </h2>
    </div>
    <div class="inzeratycena"><b>Dohodou</b></div>
</div>
"""

DETAIL_HTML = """
<html>
<body>
    <h1 class="nadpisdetail">Prodej cihlového bytu 3+kk, 74 m², po rekonstrukci</h1>
    <div class="popisdetail">
        Nabízíme k prodeji slunný byt v osobním vlastnictví o užitné ploše 74 m².
        Dům prošel kompletní rekonstrukcí. Energetická třída B.
    </div>
    <table>
        <tr><td>Cena:</td><td>4 850 000 Kč</td></tr>
        <tr><td>Lokalita:</td><td>261 01 Příbram</td></tr>
    </table>
</body>
</html>
"""


def test_parse_catalog_page_extracts_cards_and_prices() -> None:
    cards = BazosDOMParser.parse_catalog_page(CATALOG_HTML)

    assert len(cards) == 2
    assert cards[0].source_id == "19837261"
    assert (
        cards[0].url == "https://reality.bazos.cz/inzerat/19837261/prodej-bytu-3kk.php"
    )
    assert cards[0].price == 4_850_000.0

    assert cards[1].source_id == "19837262"
    assert cards[1].price is None


def test_detail_parser_extracts_structured_attributes() -> None:
    url = "https://reality.bazos.cz/inzerat/19837261/prodej-bytu-3kk.php"
    parser = BazosDOMParser(DETAIL_HTML, url)

    title = parser.extract_title()
    desc = parser.extract_description()
    price, currency = parser.extract_price()
    location = parser.extract_location()
    legal = parser.extract_legal_context(desc, title)
    combined = f"{title} {desc}"

    assert title == "Prodej cihlového bytu 3+kk, 74 m², po rekonstrukci"
    assert price == 4_850_000.0
    assert currency == "czk"
    assert location.postal_code == "26101"
    assert location.municipality == "Příbram"
    assert parser.extract_usable_area(title, desc) == 74.0
    assert parser.extract_material(combined) == BuildingMaterial.BRICK
    assert parser.extract_condition(combined) == BuildingCondition.RENOVATED
    assert parser.extract_energy_class(combined) == EnergyClass.B
    assert legal.ownership == OwnershipType.PERSONAL
    assert legal.is_foreclosure is False


def test_extract_land_area_does_not_double_count_or_match_apartment_area() -> None:
    html = """
    <html>
    <body>
        <h1 class="nadpisdetail">Prodej stavebního pozemku 850 m²</h1>
        <div class="popisdetail">
            Nabízíme stavební pozemek 850 m² v klidné části obce.
            Plocha parcely: 850 m2.
        </div>
    </body>
    </html>
    """
    parser = BazosDOMParser(html, "https://reality.bazos.cz/inzerat/999/pozemek.php")

    land_area = parser.extract_land_area(
        parser.extract_title(), parser.extract_description()
    )
    assert land_area == 850.0

    empty_loc = parser.extract_location()
    assert empty_loc.municipality is None
    assert empty_loc.postal_code is None

    # Regression: apartment description using "o výměře" must not populate land_area_m2
    apt_land_area = parser.extract_land_area(
        "Prodej bytu 3+kk", "Byt o výměře 74 m² v 2. patře."
    )
    assert apt_land_area is None


def test_extract_infrastructure_avoids_false_positives_and_handles_negations() -> None:
    parser = BazosDOMParser("<html></html>", "https://reality.bazos.cz/inzerat/1/a.php")

    desc_false_pos = (
        "Klidná čtvrť ve čtvrtém patře, plynulý chod SVJ, bez plynu a bez elektřiny."
    )
    infra = parser.extract_infrastructure(desc_false_pos)

    assert infra.water is None
    assert infra.gas is False
    assert infra.electricity is False

    # Regression: "tel." must not trigger electricity=True
    infra_tel = parser.extract_infrastructure("Více info na tel. 777 123 456.")
    assert infra_tel.electricity is None

    desc_borehole = "Na pozemku je vlastní vrt a přípojka plynu."
    infra_ok = parser.extract_infrastructure(desc_borehole)

    assert infra_ok.water is not None
    assert infra_ok.water.value == "borehole"
    assert infra_ok.gas is True


def test_extract_legal_context_avoids_municipal_utility_false_positive() -> None:
    parser = BazosDOMParser("<html></html>", "https://reality.bazos.cz/inzerat/2/b.php")

    legal_utility = parser.extract_legal_context(
        "Dům napojen na obecní vodovod a obecní kanalizaci, městská doprava 200 m.",
        "Prodej rodinného domu",
    )
    assert legal_utility.ownership is None

    legal_muni = parser.extract_legal_context(
        "Převod nájmu obecního bytu v centru města.",
        "Obecní byt 2+1",
    )
    assert legal_muni.ownership == OwnershipType.MUNICIPAL


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("PENB: třída B", EnergyClass.B),
        ("Energetická náročnost budovy: C", EnergyClass.C),
        ("Třída energetické náročnosti - G", EnergyClass.G),
        ("PENB nebyl zatím dodán. Dům je cihlový.", None),
    ],
)
def test_extract_energy_class_handles_czech_phrases(
    text: str, expected: EnergyClass | None
) -> None:
    parser = BazosDOMParser("<html></html>", "https://reality.bazos.cz/inzerat/3/c.php")
    assert parser.extract_energy_class(text) == expected
