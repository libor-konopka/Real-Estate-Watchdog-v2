from datetime import UTC, datetime, timedelta

import polars as pl

from src.transformations.gold import (
    compute_investment_opportunities,
    compute_market_metrics,
)
from src.transformations.silver import (
    calculate_lifecycle_and_deduplicate,
    compute_derived_metrics,
    match_cross_portal_duplicates,
)


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


def test_gold_metrics_and_investment_opportunities() -> None:
    now = datetime.now(UTC)
    earlier = now - timedelta(days=10)

    # Includes a 1 Kč placeholder listing that must be ignored by Gold benchmarks
    silver_df = pl.DataFrame(
        {
            "id": ["1", "2", "3", "4", "5", "6_placeholder"],
            "canonical_property_id": ["1", "2", "3", "4", "5", "6_placeholder"],
            "source": [
                "bazos",
                "bezrealitky",
                "bezrealitky",
                "bazos",
                "bezrealitky",
                "bazos",
            ],
            "title": [
                "Byt 1",
                "Byt 2",
                "Byt 3",
                "Byt 4",
                "Výhodný byt 5",
                "Zástupná cena",
            ],
            "url": [f"https://example.com/{i}" for i in range(1, 7)],
            "region": ["Středočeský"] * 6,
            "district": ["Příbram"] * 6,
            "municipality": ["Příbram"] * 6,
            "transaction_type": ["sale"] * 6,
            "entity_type": ["apartment"] * 6,
            "is_active": [True] * 6,
            "price": [
                3_000_000.0,
                3_600_000.0,
                3_900_000.0,
                4_200_000.0,
                2_520_000.0,
                1.0,
            ],
            "usable_area_m2": [50.0, 60.0, 60.0, 60.0, 60.0, 60.0],
            "price_per_m2": [60_000.0, 60_000.0, 65_000.0, 70_000.0, 42_000.0, 0.02],
            "first_seen_at": [earlier] * 6,
            "last_seen_at": [now] * 6,
        }
    )

    metrics = compute_market_metrics(silver_df)
    assert metrics.height == 1
    assert metrics["total_listings"][0] == 5
    assert metrics["active_listings"][0] == 5
    assert metrics["median_price_per_m2"][0] == 60_000.0
    assert metrics["median_days_on_market"][0] == 10

    opps = compute_investment_opportunities(
        silver_df, min_discount_pct=20.0, min_segment_sample_size=5
    )
    assert opps.height == 1
    assert opps["id"][0] == "5"
    assert opps["discount_pct"][0] == 30.0
