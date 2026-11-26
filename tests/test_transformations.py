from datetime import UTC, datetime, timedelta

import polars as pl
import pytest
from pydantic import HttpUrl

from src.contracts.base import (
    BuildingProperty,
    Currency,
    EntityType,
    Location,
    SourcePortal,
    TransactionType,
)
from src.contracts.schemas import BRONZE_PROPERTY_ARROW_SCHEMA
from src.transformations.bronze import properties_to_arrow_table
from src.transformations.gold import (
    compute_investment_opportunities,
    compute_market_metrics,
)
from src.transformations.silver import (
    calculate_lifecycle_and_deduplicate,
    compute_derived_metrics,
    match_cross_portal_duplicates,
)


def test_properties_to_arrow_table_serializes_domain_and_tombstone() -> None:
    now = datetime.now(UTC)
    prop = BuildingProperty(
        id="hash_prop",
        title="Byt 2+kk",
        is_active=True,
        source_id="1001",
        url=HttpUrl("https://reality.bazos.cz/inzerat/1001/byt.php"),
        source=SourcePortal.BAZOS,
        transaction_type=TransactionType.SALE,
        entity_type=EntityType.APARTMENT,
        price=3_500_000.0,
        currency=Currency.CZK,
        scraped_at=now,
        location=Location(municipality="Příbram", postal_code="26101"),
        usable_area_m2=55.0,
    )
    tombstone = {
        "id": "hash_dead",
        "source_id": "1002",
        "url": "https://reality.bazos.cz/inzerat/1002/byt.php",
        "source": "bazos",
        "transaction_type": "sale",
        "entity_type": "apartment",
        "is_active": False,
        "scraped_at": now,
    }

    arrow_table = properties_to_arrow_table([prop, tombstone], "batch-1")

    assert arrow_table.schema == BRONZE_PROPERTY_ARROW_SCHEMA
    assert arrow_table.num_rows == 2
    rows = arrow_table.to_pylist()
    assert rows[0]["is_active"] is True
    assert rows[0]["price"] == 3_500_000.0
    assert rows[1]["is_active"] is False
    assert rows[1]["title"] == "[DEACTIVATED]"


def test_properties_to_arrow_table_rejects_incomplete_tombstone() -> None:
    with pytest.raises(ValueError, match="missing required fields"):
        properties_to_arrow_table([{"id": "only_id"}], "batch-err")


def test_compute_derived_metrics_handles_buildings_and_land() -> None:
    df = pl.DataFrame(
        {
            "entity_type": ["apartment", "land", "house"],
            "price": [5_000_000.0, 2_000_000.0, None],
            "usable_area_m2": [50.0, None, 120.0],
            "built_up_area_m2": [None, None, None],
            "land_area_m2": [None, 1000.0, 500.0],
        }
    )

    res = compute_derived_metrics(df)
    prices_per_m2 = res["price_per_m2"].to_list()

    assert prices_per_m2[0] == 100_000.0
    assert prices_per_m2[1] == 2_000.0
    assert prices_per_m2[2] is None


def test_lifecycle_preserves_attributes_across_tombstones() -> None:
    t0 = datetime(2026, 11, 1, 10, 0, tzinfo=UTC)
    t1 = datetime(2026, 11, 5, 10, 0, tzinfo=UTC)

    df = pl.DataFrame(
        {
            "id": ["hash_1", "hash_1"],
            "source": ["bazos", "bazos"],
            "source_id": ["19837261", "19837261"],
            "title": ["Prodej bytu 3+kk", "[DEACTIVATED]"],
            "is_active": [True, False],
            "price": [4_850_000.0, None],
            "usable_area_m2": [74.0, None],
            "district": ["Příbram", None],
            "scraped_at": [t0, t1],
        }
    )

    deduped = calculate_lifecycle_and_deduplicate(df)

    assert deduped.height == 1
    row = deduped.to_dicts()[0]
    assert row["is_active"] is False
    assert row["title"] == "Prodej bytu 3+kk"
    assert row["price"] == 4_850_000.0
    assert row["usable_area_m2"] == 74.0
    assert row["district"] == "Příbram"
    assert row["first_seen_at"] == t0
    assert row["last_seen_at"] == t1


def test_lifecycle_keeps_explicit_null_on_active_update() -> None:
    t0 = datetime(2026, 11, 1, 10, 0, tzinfo=UTC)
    t1 = datetime(2026, 11, 3, 10, 0, tzinfo=UTC)

    df = pl.DataFrame(
        {
            "id": ["hash_2", "hash_2"],
            "source": ["bazos", "bazos"],
            "source_id": ["19837299", "19837299"],
            "title": ["Prodej domu", "Prodej domu"],
            "is_active": [True, True],
            "price": [6_500_000.0, None],
            "scraped_at": [t0, t1],
        }
    )

    deduped = calculate_lifecycle_and_deduplicate(df)

    assert deduped.height == 1
    row = deduped.to_dicts()[0]
    assert row["is_active"] is True
    assert row["price"] is None
    assert row["first_seen_at"] == t0
    assert row["last_seen_at"] == t1


def test_match_cross_portal_duplicates_links_identical_listing() -> None:
    df = pl.DataFrame(
        {
            "id": ["a_bazos", "b_bezrealitky", "c_distinct"],
            "source": ["bazos", "bezrealitky", "bezrealitky"],
            "municipality": ["Příbram", "Příbram", "Příbram"],
            "transaction_type": ["sale", "sale", "sale"],
            "entity_type": ["apartment", "apartment", "apartment"],
            "price": [4_500_000.0, 4_600_000.0, 7_200_000.0],
            "usable_area_m2": [68.0, 69.0, 95.0],
            "land_area_m2": [None, None, None],
            "latitude": [49.6880, 49.6884, 49.6950],
            "longitude": [14.0100, 14.0103, 14.0200],
        }
    )

    deduped = match_cross_portal_duplicates(df)
    canonical_map = dict(
        zip(
            deduped["id"].to_list(),
            deduped["canonical_property_id"].to_list(),
            strict=True,
        )
    )

    assert deduped.height == 3
    assert canonical_map["a_bazos"] == "a_bazos"
    assert canonical_map["b_bezrealitky"] == "a_bazos"
    assert canonical_map["c_distinct"] == "c_distinct"


def test_match_cross_portal_duplicates_skips_unknown_city_without_gps() -> None:
    df = pl.DataFrame(
        {
            "id": ["a_bazos", "b_bezrealitky"],
            "source": ["bazos", "bezrealitky"],
            "municipality": [None, None],
            "transaction_type": ["sale", "sale"],
            "entity_type": ["apartment", "apartment"],
            "price": [4_500_000.0, 4_550_000.0],
            "usable_area_m2": [68.0, 68.5],
            "land_area_m2": [None, None],
            "latitude": pl.Series([None, None], dtype=pl.Float64),
            "longitude": pl.Series([None, None], dtype=pl.Float64),
        }
    )

    deduped = match_cross_portal_duplicates(df)
    assert deduped["canonical_property_id"].to_list() == ["a_bazos", "b_bezrealitky"]


def test_match_cross_portal_duplicates_handles_land_and_zero_price() -> None:
    df = pl.DataFrame(
        {
            "id": ["a_land_bazos", "b_land_bez", "c_zero_bazos", "d_zero_bez"],
            "source": ["bazos", "bezrealitky", "bazos", "bezrealitky"],
            "municipality": ["Dobříš", "Dobříš", "Příbram", "Příbram"],
            "transaction_type": ["sale", "sale", "sale", "sale"],
            "entity_type": ["land", "land", "apartment", "apartment"],
            "price": [2_400_000.0, 2_450_000.0, 0.0, 0.0],
            "usable_area_m2": [None, None, 50.0, 50.0],
            "land_area_m2": [1000.0, 1002.0, None, None],
            "latitude": [49.7810, 49.7812, 49.6880, 49.6880],
            "longitude": [14.1670, 14.1671, 14.0100, 14.0100],
        }
    )

    deduped = match_cross_portal_duplicates(df)
    canonical_map = dict(
        zip(
            deduped["id"].to_list(),
            deduped["canonical_property_id"].to_list(),
            strict=True,
        )
    )

    assert canonical_map["b_land_bez"] == "a_land_bazos"
    assert canonical_map["c_zero_bazos"] == "c_zero_bazos"
    assert canonical_map["d_zero_bez"] == "d_zero_bez"


def test_gold_metrics_and_investment_opportunities() -> None:
    now = datetime.now(UTC)
    earlier = now - timedelta(days=10)

    # Includes 1 Kč placeholder, EUR listing, and cross-portal duplicate of id "1"
    silver_df = pl.DataFrame(
        {
            "id": [
                "1",
                "1_dup",
                "2",
                "3",
                "4",
                "5",
                "6_placeholder",
                "7_eur",
            ],
            "canonical_property_id": [
                "1",
                "1",
                "2",
                "3",
                "4",
                "5",
                "6_placeholder",
                "7_eur",
            ],
            "source": [
                "bazos",
                "bezrealitky",
                "bezrealitky",
                "bezrealitky",
                "bazos",
                "bezrealitky",
                "bazos",
                "bazos",
            ],
            "title": [
                "Byt 1",
                "Byt 1 (duplicita)",
                "Byt 2",
                "Byt 3",
                "Byt 4",
                "Výhodný byt 5",
                "Zástupná cena",
                "Byt v EUR",
            ],
            "url": [f"https://example.com/{i}" for i in range(1, 9)],
            "region": ["Středočeský"] * 8,
            "district": ["Příbram"] * 8,
            "municipality": ["Příbram"] * 8,
            "transaction_type": ["sale"] * 8,
            "entity_type": ["apartment"] * 8,
            "is_active": [True] * 8,
            "currency": [
                "czk",
                "czk",
                "czk",
                "czk",
                "czk",
                "czk",
                "czk",
                "eur",
            ],
            "price": [
                3_000_000.0,
                3_000_000.0,
                3_600_000.0,
                3_900_000.0,
                4_200_000.0,
                2_520_000.0,
                1.0,
                150_000.0,
            ],
            "usable_area_m2": [50.0, 50.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0],
            "land_area_m2": pl.Series([None] * 8, dtype=pl.Float64),
            "price_per_m2": [
                60_000.0,
                60_000.0,
                60_000.0,
                65_000.0,
                70_000.0,
                42_000.0,
                0.02,
                2_500.0,
            ],
            "first_seen_at": [earlier] * 8,
            "last_seen_at": [now] * 8,
        }
    )

    metrics = compute_market_metrics(silver_df)
    assert metrics.height == 1
    assert metrics["total_listings"][0] == 5
    assert metrics["active_listings"][0] == 5
    assert metrics["median_price_per_m2"][0] == 60_000.0
    assert metrics["median_area_m2"][0] == 60.0
    assert metrics["median_days_on_market"][0] == 10

    opps = compute_investment_opportunities(
        silver_df, min_discount_pct=20.0, min_segment_sample_size=5
    )
    assert opps.height == 1
    assert opps["id"][0] == "5"
    assert opps["area_m2"][0] == 60.0
    assert opps["discount_pct"][0] == 30.0


def test_gold_uses_land_area_for_land_listings() -> None:
    now = datetime.now(UTC)
    earlier = now - timedelta(days=5)

    land_df = pl.DataFrame(
        {
            "id": [f"land_{i}" for i in range(5)],
            "canonical_property_id": [f"land_{i}" for i in range(5)],
            "source": ["bazos"] * 5,
            "title": [f"Pozemek {i}" for i in range(5)],
            "url": [f"https://example.com/land/{i}" for i in range(5)],
            "region": ["Středočeský"] * 5,
            "district": ["Příbram"] * 5,
            "municipality": ["Dobříš"] * 5,
            "transaction_type": ["sale"] * 5,
            "entity_type": ["land"] * 5,
            "is_active": [True] * 5,
            "currency": ["czk"] * 5,
            "price": [
                2_000_000.0,
                2_000_000.0,
                2_000_000.0,
                2_200_000.0,
                1_200_000.0,
            ],
            "usable_area_m2": pl.Series([None] * 5, dtype=pl.Float64),
            "land_area_m2": [1000.0, 1000.0, 1000.0, 1000.0, 1000.0],
            "price_per_m2": [2_000.0, 2_000.0, 2_000.0, 2_200.0, 1_200.0],
            "first_seen_at": [earlier] * 5,
            "last_seen_at": [now] * 5,
        }
    )

    metrics = compute_market_metrics(land_df)
    assert metrics["median_area_m2"][0] == 1000.0

    opps = compute_investment_opportunities(
        land_df, min_discount_pct=20.0, min_segment_sample_size=5
    )
    assert opps.height == 1
    assert opps["id"][0] == "land_4"
    assert opps["area_m2"][0] == 1000.0
    assert opps["discount_pct"][0] == 40.0
