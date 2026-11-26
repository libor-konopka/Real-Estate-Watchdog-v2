from typing import cast

import polars as pl
import streamlit as st
from pyiceberg.exceptions import (
    NoSuchNamespaceError,
    NoSuchTableError,
    RESTError,
)

from src.contracts.base import EntityType
from src.storage.catalog import get_catalog_manager


@st.cache_data(ttl=60)
def load_iceberg_table(table_identifier: str) -> pl.DataFrame:
    """Reads an Apache Iceberg table into a Polars DataFrame via Nessie REST catalog."""
    try:
        catalog = get_catalog_manager().get_catalog()
        table = catalog.load_table(table_identifier)
        df = cast(pl.DataFrame, pl.scan_iceberg(table).collect())

        if "available_from" in df.columns:
            df = df.with_columns(pl.col("available_from").cast(pl.Date))

        return df
    except NoSuchTableError, NoSuchNamespaceError:
        return pl.DataFrame()
    except (RESTError, OSError, ValueError) as exc:
        st.error(f"Chyba při načítání tabulky '{table_identifier}': {exc}")
        return pl.DataFrame()


def _safe_median(series: pl.Series) -> float | None:
    """Safely narrows a Polars median result to float | None."""
    val = series.median()
    if isinstance(val, (int, float)):
        return float(val)
    return None


def _weighted_price_per_m2(df: pl.DataFrame) -> float | None:
    """Computes listing-count-weighted average of district median prices per m²."""
    valid = df.filter(
        pl.col("median_price_per_m2").is_not_null() & (pl.col("total_listings") > 0)
    )
    if valid.is_empty():
        return None

    total_weight = float(valid["total_listings"].sum())
    if total_weight <= 0:
        return None

    weighted_sum = float((valid["median_price_per_m2"] * valid["total_listings"]).sum())
    return weighted_sum / total_weight


def _format_czk(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:,.0f} Kč".replace(",", " ")


def render_market_overview(metrics_df: pl.DataFrame) -> None:
    """Renders regional market benchmarks from gold.market_metrics."""
    st.subheader("Regionální tržní přehled (Gold Layer)")

    if metrics_df.is_empty():
        st.info(
            "Tabulka `gold.market_metrics` zatím neobsahuje žádná data. "
            "Spusťte nejprve ELT pipeline."
        )
        return

    col1, col2 = st.columns(2)
    tx_options = sorted(metrics_df["transaction_type"].drop_nulls().unique().to_list())
    ent_options = sorted(metrics_df["entity_type"].drop_nulls().unique().to_list())

    selected_tx = col1.selectbox("Typ transakce", tx_options, key="gold_tx")
    selected_ent = col2.selectbox("Typ nemovitosti", ent_options, key="gold_ent")

    filtered = metrics_df.filter(
        (pl.col("transaction_type") == selected_tx)
        & (pl.col("entity_type") == selected_ent)
    )

    if filtered.is_empty():
        st.warning("Pro zvolenou kombinaci nejsou k dispozici agregované metriky.")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Sledované okresy", filtered.height)
    c2.metric("Aktivní inzeráty celkem", int(filtered["active_listings"].sum()))
    c3.metric(
        "Vážený průměr mediánů / m²",
        _format_czk(_weighted_price_per_m2(filtered)),
    )

    st.dataframe(
        filtered,
        width="stretch",
        hide_index=True,
        column_order=[
            "region",
            "district",
            "active_listings",
            "inactive_listings",
            "median_price",
            "median_price_per_m2",
            "mean_price_per_m2",
            "median_area_m2",
            "median_days_on_market",
        ],
    )


def render_investment_opportunities(opps_df: pl.DataFrame) -> None:
    """Renders undervalued listings from gold.investment_opportunities."""
    st.subheader("Podhodnocené investiční příležitosti (Gold Layer)")

    if opps_df.is_empty():
        st.info(
            "Zatím nebyly detekovány žádné podhodnocené nabídky v "
            "`gold.investment_opportunities`."
        )
        return

    min_discount = st.slider(
        "Minimální sleva oproti mediánu okresu (%)",
        min_value=20,
        max_value=70,
        value=25,
        step=5,
    )

    filtered = opps_df.filter(pl.col("discount_pct") >= float(min_discount))

    st.metric("Nalezené příležitosti", filtered.height)

    st.dataframe(
        filtered,
        width="stretch",
        hide_index=True,
        column_order=[
            "discount_pct",
            "source",
            "title",
            "district",
            "municipality",
            "transaction_type",
            "entity_type",
            "price",
            "area_m2",
            "price_per_m2",
            "segment_median_price_m2",
            "url",
        ],
        column_config={
            "url": st.column_config.LinkColumn(display_text="Otevřít inzerát"),
            "discount_pct": st.column_config.NumberColumn(
                "Sleva (%)", format="%.1f %%"
            ),
        },
    )


def render_silver_explorer(silver_df: pl.DataFrame) -> None:
    """Renders searchable and deduplicated property explorer from silver.properties."""
    st.subheader("Průzkumník nemovitostí (Unified Silver Layer)")

    if silver_df.is_empty():
        st.warning("Tabulka `silver.properties` neobsahuje žádná data.")
        return

    with st.expander("Filtry průzkumníka", expanded=True):
        f1, f2, f3, f4 = st.columns(4)
        sources = sorted(silver_df["source"].drop_nulls().unique().to_list())
        selected_sources = f1.multiselect(
            "Zdrojový portál", sources, default=sources, key="silver_sources"
        )

        transactions = sorted(
            silver_df["transaction_type"].drop_nulls().unique().to_list()
        )
        selected_tx = f2.selectbox("Transakce", transactions, key="silver_tx")

        entities = sorted(silver_df["entity_type"].drop_nulls().unique().to_list())
        selected_ent = f3.selectbox("Kategorie", entities, key="silver_ent")

        regions = ["Všechny kraje"] + sorted(
            silver_df["region"].drop_nulls().unique().to_list()
        )
        selected_region = f4.selectbox("Kraj", regions, key="silver_region")

        c_opt1, c_opt2 = st.columns(2)
        show_inactive = c_opt1.checkbox(
            "Zobrazit i smazané inzeráty (historie)",
            value=False,
            key="silver_inactive",
        )
        deduplicate_cross_portal = c_opt2.checkbox(
            "Skrýt duplicity napříč portály",
            value=True,
            key="silver_dedup",
        )

    filtered = silver_df.filter(
        pl.col("source").is_in(selected_sources)
        & (pl.col("transaction_type") == selected_tx)
        & (pl.col("entity_type") == selected_ent)
    )

    if selected_region != "Všechny kraje":
        filtered = filtered.filter(pl.col("region") == selected_region)

    if not show_inactive:
        filtered = filtered.filter(pl.col("is_active"))

    if deduplicate_cross_portal and "canonical_property_id" in filtered.columns:
        filtered = filtered.sort(["is_active", "last_seen_at"]).unique(
            subset=["canonical_property_id"], keep="last"
        )

    if filtered.is_empty():
        st.info("Zvoleným filtrům neodpovídají žádné nemovitosti.")
        return

    effective_area_series = (
        filtered["land_area_m2"]
        if selected_ent == EntityType.LAND.value
        else filtered.select(
            pl.coalesce(["usable_area_m2", "built_up_area_m2"]).alias("area")
        )["area"]
    )
    median_area = _safe_median(effective_area_series)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Počet nemovitostí", filtered.height)
    col2.metric("Medián ceny", _format_czk(_safe_median(filtered["price"])))
    col3.metric("Medián ceny / m²", _format_czk(_safe_median(filtered["price_per_m2"])))
    col4.metric(
        "Medián plochy",
        f"{median_area:.1f} m²" if median_area is not None else "N/A",
    )

    st.dataframe(
        filtered,
        width="stretch",
        hide_index=True,
        height=700,
        column_order=[
            "is_active",
            "source",
            "title",
            "price",
            "price_per_m2",
            "currency",
            "region",
            "district",
            "municipality",
            "usable_area_m2",
            "built_up_area_m2",
            "land_area_m2",
            "condition",
            "material",
            "energy_class",
            "ownership",
            "first_seen_at",
            "last_seen_at",
            "url",
        ],
        column_config={"url": st.column_config.LinkColumn(display_text="Odkaz")},
    )


def main() -> None:
    """Main entry point for the Streamlit analytical dashboard."""
    st.set_page_config(page_title="Real-Estate Watchdog", layout="wide")
    st.title("Real-Estate Watchdog — Medallion Lakehouse Explorer")

    manager = get_catalog_manager()
    silver_df = load_iceberg_table(manager.silver_table_id)
    metrics_df = load_iceberg_table(manager.gold_metrics_table_id)
    opps_df = load_iceberg_table(manager.gold_opportunities_table_id)

    tab_silver, tab_opps, tab_market = st.tabs(
        [
            "Průzkumník nemovitostí (Silver)",
            "Investiční příležitosti (Gold)",
            "Tržní benchmarky (Gold)",
        ]
    )

    with tab_silver:
        render_silver_explorer(silver_df)

    with tab_opps:
        render_investment_opportunities(opps_df)

    with tab_market:
        render_market_overview(metrics_df)


if __name__ == "__main__":
    main()
