import os
from pathlib import Path

import polars as pl
import streamlit as st
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError


@st.cache_data(ttl=60)
def load_data(transaction: str, entity: str) -> pl.DataFrame:
    """Načítá data z Iceberg tabulky ve vrstvě Silver."""

    # Pověření pro podkladovou vrstvu s3fs (vyhne se odeslání do Nessie REST API)
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ["AWS_ALLOW_HTTP"] = "true"
    os.environ["AWS_REGION"] = "us-east-1"

    try:
        catalog = load_catalog(
            "default",
            **{
                "type": "rest",
                "uri": "http://localhost:19120/iceberg/",
                "s3.endpoint": "http://localhost:9000",
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            },
        )
        table = catalog.load_table(f"silver.bazos_{transaction}_{entity}")
        df = pl.scan_iceberg(table).collect()

        if "available_from" in df.columns:
            df = df.with_columns(pl.col("available_from").cast(pl.Date))

        return df
    except NoSuchTableError, NoSuchNamespaceError:
        return pl.DataFrame()
    except Exception as e:
        st.error(f"Failed to load data from Iceberg: {e}")
        return pl.DataFrame()


def main() -> None:
    """Main execution block for the Streamlit application."""
    st.set_page_config(page_title="Real-Estate Watchdog", layout="wide")
    st.title("Real-Estate Watchdog: Data Explorer")

    # Left sidebar
    st.sidebar.header("Filtrace segmentu")
    transaction = st.sidebar.selectbox("Transakce", ["prodam", "pronajmu"])
    entity = st.sidebar.selectbox(
        "Kategorie",
        [
            "byt",
            "dum",
            "chata",
            "pozemek",
            "zahrada",
            "kancelar",
            "sklad",
            "prostory",
            "restaurace",
            "garaz",
            "ostatni",
        ],
    )

    # Přidání přepínače do sidebaru
    st.sidebar.divider()
    show_inactive = st.sidebar.checkbox(
        "Zobrazit i smazané inzeráty (historie)", value=False
    )

    df = load_data(transaction, entity)

    if df.is_empty():
        st.warning(f"Žádná data pro kombinaci: {transaction} {entity}")
        return

    # Vyfiltrování smazaných inzerátů z výpočtů a tabulky
    if not show_inactive:
        df = df.filter(pl.col("is_active") == True)

    if df.is_empty():
        st.info("V této kategorii nejsou žádné aktivní inzeráty.")
        return

    # Aggregation
    total_listings = df.height
    avg_price = df["price"].mean()
    median_price = df["price"].median()

    # Dashboard layout
    col1, col2, col3 = st.columns(3)
    col1.metric("Aktivní inzeráty", total_listings)

    if avg_price is not None:
        # Format as standard Czech currency with spaces as thousands separators
        formatted_avg = f"{avg_price:,.0f}".replace(",", " ")
        formatted_median = f"{median_price:,.0f}".replace(",", " ")

        col2.metric("Průměrná cena (Kč)", formatted_avg)
        col3.metric("Medián ceny (Kč)", formatted_median)
    else:
        col2.metric("Průměrná cena (Kč)", "N/A")
        col3.metric("Medián ceny (Kč)", "N/A")

    st.divider()

    st.subheader("Konsolidovaný datový rámec")
    st.dataframe(
        df,
        width="stretch",
        hide_index=True,
        height=800,
        column_order=[
            "is_active",
            "source",
            "title",
            "price",
            "currency",
            "url",
            "scraped_at",
            "region",
            "district",
            "municipality",
            "water",
            "waste",
            "electricity",
            "gas",
            "heating",
            "ownership",
            "price_note",
            "available_from",
            "is_foreclosure",
            "is_insolvency",
            "is_share",
            "land_area_m2",
            "usable_area_m2",
            "description",
            "built_up_area_m2",
            "material",
            "condition",
            "energy_class",
        ],
        column_config={"url": st.column_config.LinkColumn(display_text="Odkaz")},
    )


if __name__ == "__main__":
    main()
