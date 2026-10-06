import polars as pl
import pyarrow as pa
from loguru import logger
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
)

from src.contracts.portals.base import AnyProperty

# Inicializace Iceberg REST katalogu směrovaného na lokální Nessie a MinIO
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


def get_known_source_ids(transaction: str, entity_type: str) -> dict[str, str]:
    """Loads the existing Iceberg table and returns a dictionary mapping source IDs to their prices."""
    table_identifier = f"bronze.bazos_{transaction}_{entity_type}"

    try:
        table = catalog.load_table(table_identifier)
        df = pl.scan_iceberg(table).select(["source_id", "price"]).collect()
        return dict(zip(df["source_id"].to_list(), df["price"].to_list()))
    except NoSuchTableError, NoSuchNamespaceError:
        return {}
    except Exception as e:
        logger.error(f"Failed to read existing bronze data at {table_identifier}: {e}")
        return {}


def sanitize_null_types(field: pa.Field) -> pa.Field:
    """Rekurzivně přetypuje pa.null() na pa.string() v PyArrow schématu."""
    if pa.types.is_null(field.type):
        return field.with_type(pa.string())
    elif pa.types.is_struct(field.type):
        new_struct_fields = [sanitize_null_types(f) for f in field.type]
        return field.with_type(pa.struct(new_struct_fields))
    elif pa.types.is_list(field.type):
        return field.with_type(
            pa.list_(sanitize_null_types(field.type.value_field).type)
        )
    return field


def save_to_iceberg(
    entities: list[AnyProperty], transaction: str, entity_type: str
) -> None:
    """
    Materializes accumulated domain entities into an Iceberg table on MinIO S3.
    Uses append-only strategy for the Bronze layer.
    """
    if not entities:
        return

    raw_data = [entity.model_dump(mode="json") for entity in entities]
    new_df = pl.DataFrame(raw_data, infer_schema_length=10000)

    # Konverze do PyArrow
    arrow_table = new_df.to_arrow()

    # Dynamická oprava inferovaných Null typů (Iceberg vyžaduje konkrétní datový typ)
    sanitized_schema = pa.schema(
        [sanitize_null_types(field) for field in arrow_table.schema]
    )
    arrow_table = arrow_table.cast(sanitized_schema)

    namespace = "bronze"
    table_name = f"bazos_{transaction}_{entity_type}"
    table_identifier = f"{namespace}.{table_name}"

    try:
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass

        try:
            table = catalog.load_table(table_identifier)
            table.append(arrow_table)
        except NoSuchTableError:
            table = catalog.create_table(
                identifier=table_identifier,
                schema=arrow_table.schema,
            )
            table.append(arrow_table)

        logger.info(
            f"Bronze layer appended to Iceberg: {table_identifier} (Records: {new_df.height})"
        )
    except Exception as e:
        logger.error(
            f"Failed to save bronze layer to Iceberg table {table_identifier}: {e}"
        )
        raise
