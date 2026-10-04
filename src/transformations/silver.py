from pathlib import Path

import polars as pl
from loguru import logger


def clean_infrastructure(df: pl.DataFrame) -> pl.DataFrame:
    """Flattens infrastructure and imputes missing boolean values."""
    if "infrastructure" in df.columns:
        df = df.unnest("infrastructure")

    # Imputation of electricity for apartments and houses
    if "electricity" in df.columns and "entity_type" in df.columns:
        df = df.with_columns(
            pl.when(
                (pl.col("entity_type").is_in(["apartment", "house"]))
                & (pl.col("electricity").is_null())
            )
            .then(True)
            .otherwise(pl.col("electricity"))
            .alias("electricity")
        )

    return df


def clean_financials(df: pl.DataFrame) -> pl.DataFrame:
    """Standardizes the price column into a numerical format for aggregations."""
    if "price" in df.columns:
        # The price is already validated from Pydantic models
        df = df.with_columns(pl.col("price").cast(pl.Float64, strict=False))

    return df


def clean_legal_context(df: pl.DataFrame) -> pl.DataFrame:
    """Flattens the legal context and ensures a strict data type for the date."""
    if "legal" in df.columns:
        df = df.unnest("legal")

        if "available_from" in df.columns:
            # Pydantic in JSON mode exports the date as a string 'YYYY-MM-DD'.
            # Direct cast safely infers the type without manual parsing.
            df = df.with_columns(pl.col("available_from").cast(pl.Date, strict=False))

    return df


def process_silver(input_path: Path, output_path: Path) -> None:
    """Loads Bronze data, applies the cleaning pipeline, and saves it to the Silver layer."""
    if not input_path.exists():
        logger.warning(f"Source file does not exist: {input_path}")
        return

    try:
        df = pl.read_parquet(input_path)
    except Exception as e:
        logger.error(f"Failed to read bronze layer at {input_path}: {e}")
        return

    if df.is_empty():
        return

    # 1. Flattening the location
    if "location" in df.columns:
        df = df.unnest("location")

    # 2. Application of business and cleaning rules
    df = clean_infrastructure(df)
    df = clean_financials(df)
    df = clean_legal_context(df)

    # 3. Physical write to the Silver layer
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        df.write_parquet(output_path)
        logger.info(f"Silver layer saved: {output_path} (Records: {df.height})")
    except Exception as e:
        logger.error(f"Failed to save silver layer to {output_path}: {e}")
        raise
