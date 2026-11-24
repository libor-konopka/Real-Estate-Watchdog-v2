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
        <h2 class="nadpis"><a href="/inzerat/19837261/prodej-bytu-3kk.php">Prodej bytu 3+kk 74 m²</a></h2>
    </div>
    <div class="inzeratycena"><b>4 850 000 Kč</b></div>
</div>
<div class="inzeraty inzeratyflex">
    <div class="inzeratynadpis">
        <h2 class="nadpis"><a href="/inzerat/19837262/pronajem-garaze.php">Pronájem garáže</a></h2>
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
