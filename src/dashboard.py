from pathlib import Path

import polars as pl
import streamlit as st


@st.cache_data
def load_data(transaction: str, entity: str) -> pl.DataFrame:
    """
    Loads data from a local Parquet file into a Polars DataFrame.
    Implements caching to prevent redundant disk I/O on UI interactions.
    """
    # Dynamic construction of the absolute path from the dashboard.py location
    current_dir = Path(__file__).resolve().parent
    project_root = current_dir.parent

    file_path = (
        project_root / "data" / "silver" / f"bazos_{transaction}_{entity}.parquet"
    )

    if not file_path.exists():
        return pl.DataFrame()

    try:
        df = pl.read_parquet(file_path)

        if "available_from" in df.columns:
            df = df.with_columns(pl.col("available_from").cast(pl.Date))

        return df
    except Exception as e:
        # Prevents app crash and displays a localized error in the UI
        st.error(f"Failed to load data from {file_path.name}: {e}")
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

    df = load_data(transaction, entity)

    if df.is_empty():
        st.warning(f"Žádná data pro kombinaci: {transaction} {entity}")
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
