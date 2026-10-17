import pyarrow as pa

# -----------------------------------------------------------------------------
# BRONZE LAYER SCHEMA (Raw snapshots with ingestion audit metadata)
# Mirrors domain contracts while enforcing strict Parquet types for Lakehouse.
# -----------------------------------------------------------------------------
BRONZE_PROPERTY_ARROW_SCHEMA = pa.schema(
    [
        # Pipeline Lakehouse audit metadata
        pa.field("batch_id", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        # Identity & Source
        pa.field("id", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("url", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("transaction_type", pa.string(), nullable=False),
        pa.field("entity_type", pa.string(), nullable=False),
        # Financial
        pa.field("price", pa.float64(), nullable=True),
        pa.field("currency", pa.string(), nullable=True),
        # Extraction time anchor
        pa.field("scraped_at", pa.timestamp("us", tz="UTC"), nullable=False),
        # Location struct (preserves source hierarchy for Bronze auditing)
        pa.field(
            "location",
            pa.struct(
                [
                    pa.field("region", pa.string(), nullable=True),
                    pa.field("district", pa.string(), nullable=True),
                    pa.field("municipality", pa.string(), nullable=True),
                    pa.field("postal_code", pa.string(), nullable=True),
                    pa.field(
                        "gps",
                        pa.struct(
                            [
                                pa.field("lat", pa.float64(), nullable=True),
                                pa.field("lon", pa.float64(), nullable=True),
                            ]
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=False,
        ),
        # Infrastructure struct
        pa.field(
            "infrastructure",
            pa.struct(
                [
                    pa.field("water", pa.string(), nullable=True),
                    pa.field("electricity", pa.bool_(), nullable=True),
                    pa.field("waste", pa.string(), nullable=True),
                    pa.field("heating", pa.string(), nullable=True),
                    pa.field("gas", pa.bool_(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        # Legal context struct
        pa.field(
            "legal",
            pa.struct(
                [
                    pa.field("ownership", pa.string(), nullable=True),
                    pa.field("price_note", pa.string(), nullable=True),
                    pa.field("available_from", pa.date32(), nullable=True),
                    pa.field("is_foreclosure", pa.bool_(), nullable=True),
                    pa.field("is_insolvency", pa.bool_(), nullable=True),
                    pa.field("is_share", pa.bool_(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        # Physical dimensions and technical parameters
        pa.field("land_area_m2", pa.float64(), nullable=True),
        pa.field("usable_area_m2", pa.float64(), nullable=True),
        pa.field("built_up_area_m2", pa.float64(), nullable=True),
        pa.field("material", pa.string(), nullable=True),
        pa.field("condition", pa.string(), nullable=True),
        pa.field("energy_class", pa.string(), nullable=True),
        pa.field("land_category", pa.string(), nullable=True),
        pa.field("location_character", pa.string(), nullable=True),
        pa.field("access_road", pa.string(), nullable=True),
        pa.field("transport", pa.string(), nullable=True),
        pa.field("description", pa.string(), nullable=True),
    ]
)

# -----------------------------------------------------------------------------
# SILVER LAYER SCHEMA (Cleaned, flattened, deduplicated, and analytics-ready)
# -----------------------------------------------------------------------------
SILVER_PROPERTY_ARROW_SCHEMA = pa.schema(
    [
        # Identity
        pa.field("id", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("url", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("transaction_type", pa.string(), nullable=False),
        pa.field("entity_type", pa.string(), nullable=False),
        # Cleaned price metrics
        pa.field("price", pa.float64(), nullable=True),
        pa.field("currency", pa.string(), nullable=True),
        pa.field("price_per_m2", pa.float64(), nullable=True),
        # SCD / Tracking lifecycle timestamps
        pa.field("first_seen_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("last_seen_at", pa.timestamp("us", tz="UTC"), nullable=False),
        # Fully flattened location (high-performance columnar filtering)
        pa.field("region", pa.string(), nullable=True),
        pa.field("district", pa.string(), nullable=True),
        pa.field("municipality", pa.string(), nullable=True),
        pa.field("postal_code", pa.string(), nullable=True),
        pa.field("latitude", pa.float64(), nullable=True),
        pa.field("longitude", pa.float64(), nullable=True),
        # Fully flattened infrastructure
        pa.field("water", pa.string(), nullable=True),
        pa.field("electricity", pa.bool_(), nullable=True),
        pa.field("waste", pa.string(), nullable=True),
        pa.field("heating", pa.string(), nullable=True),
        pa.field("gas", pa.bool_(), nullable=True),
        # Fully flattened legal context
        pa.field("ownership", pa.string(), nullable=True),
        pa.field("price_note", pa.string(), nullable=True),
        pa.field("available_from", pa.date32(), nullable=True),
        pa.field("is_foreclosure", pa.bool_(), nullable=True),
        pa.field("is_insolvency", pa.bool_(), nullable=True),
        pa.field("is_share", pa.bool_(), nullable=True),
        # Areas and specifications
        pa.field("land_area_m2", pa.float64(), nullable=True),
        pa.field("usable_area_m2", pa.float64(), nullable=True),
        pa.field("built_up_area_m2", pa.float64(), nullable=True),
        pa.field("material", pa.string(), nullable=True),
        pa.field("condition", pa.string(), nullable=True),
        pa.field("energy_class", pa.string(), nullable=True),
        pa.field("land_category", pa.string(), nullable=True),
        pa.field("location_character", pa.string(), nullable=True),
        pa.field("access_road", pa.string(), nullable=True),
        pa.field("transport", pa.string(), nullable=True),
        pa.field("description", pa.string(), nullable=True),
        # Content hash for change detection (SCD Type 2 / diffing)
        pa.field("content_hash", pa.string(), nullable=False),
    ]
)
