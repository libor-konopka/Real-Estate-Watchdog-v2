from src.contracts.base import (
    BuildingCondition,
    BuildingMaterial,
    BuildingProperty,
    EnergyClass,
    EntityType,
    LandProperty,
    OwnershipType,
    SourcePortal,
    TransactionType,
)
from src.scrapers.bezrealitky_parser import BezrealitkyParser


def test_parse_advert_materializes_building_property() -> None:
    raw_payload = {
        "id": "884422",
        "uri": "884422-nabidka-prodej-bytu-pribram",
        "offerType": "PRODEJ",
        "estateType": "BYT",
        "title": "Prodej bytu 2+kk 54 m²",
        "description": "Světlý byt v novostavbě.",
        "price": 3990000,
        "currency": "CZK",
        "city": "Příbram",
        "cityDistrict": "Příbram VII",
        "zip": "261 01",
        "gps": {"lat": 49.6883, "lng": 14.0104},
        "surface": 54,
        "surfaceLand": None,
        "condition": "NEW",
        "construction": "BRICK",
        "ownership": "PERSONAL",
        "penb": "A",
    }

    result = BezrealitkyParser.parse_advert(raw_payload)

    assert isinstance(result, BuildingProperty)
    assert result.source == SourcePortal.BEZREALITKY
    assert result.source_id == "884422"
    assert result.transaction_type == TransactionType.SALE
    assert result.entity_type == EntityType.APARTMENT
    assert result.price == 3_990_000.0
    assert result.usable_area_m2 == 54.0
    assert result.location.postal_code == "26101"
    assert result.location.municipality == "Příbram"
    assert result.location.gps is not None
    assert result.location.gps.lat == 49.6883
    assert result.condition == BuildingCondition.NEW
    assert result.material == BuildingMaterial.BRICK
    assert result.energy_class == EnergyClass.A
    assert result.legal is not None
    assert result.legal.ownership == OwnershipType.PERSONAL


def test_parse_advert_materializes_land_property() -> None:
    raw_payload = {
        "id": "991100",
        "uri": "991100-nabidka-prodej-pozemku",
        "offerType": "PRODEJ",
        "estateType": "POZEMEK",
        "title": "Stavební pozemek 1 200 m²",
        "price": 2400000,
        "currency": "CZK",
        "city": "Dobříš",
        "zip": "263 01",
        "surface": None,
        "surfaceLand": 1200,
    }

    result = BezrealitkyParser.parse_advert(raw_payload)

    assert isinstance(result, LandProperty)
    assert result.entity_type == EntityType.LAND
    assert result.land_area_m2 == 1200.0


def test_parse_advert_returns_none_on_invalid_payload() -> None:
    invalid_payload = {
        "id": "broken-1",
        "offerType": "UNSUPPORTED_TYPE",
        "estateType": "BYT",
    }

    assert BezrealitkyParser.parse_advert(invalid_payload) is None
