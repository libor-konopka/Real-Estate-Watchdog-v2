import polars as pl
from loguru import logger
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

# Inicializace katalogu (stejná jako v bronze.py)
catalog = load_catalog(
    "default",
    **{
        "type": "rest",
        "uri": "http://localhost:19120/iceberg/",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    },
)


def clean_infrastructure(df: pl.DataFrame) -> pl.DataFrame:
    if "infrastructure" in df.columns:
        df = df.unnest("infrastructure")

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
    if "price" in df.columns:
        df = df.with_columns(pl.col("price").cast(pl.Float64, strict=False))
    return df


def clean_legal_context(df: pl.DataFrame) -> pl.DataFrame:
    if "legal" in df.columns:
        df = df.unnest("legal")
        if "available_from" in df.columns:
            df = df.with_columns(pl.col("available_from").cast(pl.Date, strict=False))
    return df


def process_silver(transaction: str, entity_type: str) -> None:
    """Načte data z Bronze Icebergu, deduplikuje, vyčistí a přepíše Silver Iceberg tabulku."""
    bronze_identifier = f"bronze.bazos_{transaction}_{entity_type}"
    silver_namespace = "silver"
    silver_table_name = f"bazos_{transaction}_{entity_type}"
    silver_identifier = f"{silver_namespace}.{silver_table_name}"

    try:
        # Načtení dat z Bronze
        bronze_table = catalog.load_table(bronze_identifier)
        df = pl.scan_iceberg(bronze_table).collect()

        if df.is_empty():
            return

        # Deduplikace - Bronze je append-only, Silver obsahuje vždy aktuální stav
        df = df.unique(subset=["source_id"], keep="last")

        if "location" in df.columns:
            df = df.unnest("location")

        df = clean_infrastructure(df)
        df = clean_financials(df)
        df = clean_legal_context(df)

        arrow_table = df.to_arrow()

        try:
            catalog.create_namespace(silver_namespace)
        except NamespaceAlreadyExistsError:
            pass

        try:
            table = catalog.load_table(silver_identifier)
            table.overwrite(arrow_table)
        except NoSuchTableError:
            table = catalog.create_table(
                identifier=silver_identifier,
                schema=arrow_table.schema,
                location=f"s3://warehouse/{silver_namespace}/{silver_table_name}",
            )
            table.append(arrow_table)

        logger.info(
            f"Silver layer updated in Iceberg: {silver_identifier} (Records: {df.height})"
        )

    except Exception as e:
        logger.error(
            f"Failed to process silver layer for {transaction}/{entity_type}: {e}"
        )
        raise
