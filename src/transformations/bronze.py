from pathlib import Path

import polars as pl
from loguru import logger

from src.contracts.portals.base import AnyProperty


def get_known_source_ids(transaction: str, entity_type: str) -> dict[str, str]:
    """Load the existing Parquet file and return a dictionary mapping source IDs to their prices."""
    # Dynamic construction of the absolute path from the bronze.py location
    project_root = Path(__file__).resolve().parents[2]

    file_path = (
        project_root / "data" / "bronze" / f"bazos_{transaction}_{entity_type}.parquet"
    )

    if not file_path.exists():
        return {}

    try:
        df = pl.read_parquet(file_path, columns=["source_id", "price"])
        return dict(zip(df["source_id"].to_list(), df["price"].to_list()))
    except Exception as e:
        logger.error(f"Failed to read existing bronze data at {file_path}: {e}")
        # Return an empty dictionary to force a complete download (self-healing).
        return {}


def save_to_parquet(
    entities: list[AnyProperty], transaction: str, entity_type: str
) -> None:
    """
    Materializes accumulated domain entities into the columnar Parquet format.
    Creates a persistent data store for analytical processing.
    """
    if not entities:
        return

    # Transforming Pydantic entities into bronze dictionaries (JSON-compatible)
    raw_data = [entity.model_dump(mode="json") for entity in entities]

    # Materialization into a vector DataFrame structure
    new_df = pl.DataFrame(raw_data, infer_schema_length=10000)

    # Definition and securing of physical space
    project_root = Path(__file__).resolve().parents[2]
    output_dir = project_root / "data" / "bronze"
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / f"bazos_{transaction}_{entity_type}.parquet"

    try:
        # Merging historical data with new data securely using diagonal concat
        if file_path.exists():
            existing_df = pl.read_parquet(file_path)
            final_df = pl.concat([existing_df, new_df], how="diagonal").unique(
                subset=["source_id"], keep="last"
            )
        else:
            final_df = new_df

        # Writing with compression
        final_df.write_parquet(file_path)
        logger.info(f"Bronze layer saved: {file_path} (Records: {final_df.height})")
    except Exception as e:
        logger.error(f"Failed to save bronze layer to {file_path}: {e}")
        # We need to throw an exception because a write failure means a loss of data from memory
        raise
