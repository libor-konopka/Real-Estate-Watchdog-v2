import pyarrow as pa

# Explicit schema matching BaseProperty, BuildingProperty, and LandProperty contracts
PROPERTY_ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("url", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("transaction_type", pa.string(), nullable=False),
        pa.field("entity_type", pa.string(), nullable=False),
        pa.field("price", pa.float64(), nullable=True),
        pa.field("currency", pa.string(), nullable=True),
        pa.field("scraped_at", pa.timestamp("us", tz="UTC"), nullable=False),
        # Location struct
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
                                pa.field("lat", pa.float64(), nullable=False),
                                pa.field("lon", pa.float64(), nullable=False),
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
        # Legal struct
        pa.field(
            "legal",
            pa.struct(
                [
                    pa.field("ownership", pa.string(), nullable=True),
                    pa.field("price_note", pa.string(), nullable=True),
                    pa.field("available_from", pa.date32(), nullable=True),
                    pa.field("is_foreclosure", pa.bool_(), nullable=False),
                    pa.field("is_insolvency", pa.bool_(), nullable=False),
                    pa.field("is_share", pa.bool_(), nullable=False),
                ]
            ),
            nullable=True,
        ),
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

SILVER_PROPERTY_ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("url", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("transaction_type", pa.string(), nullable=False),
        pa.field("entity_type", pa.string(), nullable=False),
        pa.field("price", pa.float64(), nullable=True),
        pa.field("currency", pa.string(), nullable=True),
        pa.field("scraped_at", pa.timestamp("us", tz="UTC"), nullable=False),
        # Zploštělá lokace
        pa.field("region", pa.string(), nullable=True),
        pa.field("district", pa.string(), nullable=True),
        pa.field("municipality", pa.string(), nullable=True),
        pa.field("postal_code", pa.string(), nullable=True),
        pa.field(
            "gps",
            pa.struct(
                [
                    pa.field("lat", pa.float64(), nullable=False),
                    pa.field("lon", pa.float64(), nullable=False),
                ]
            ),
            nullable=True,
        ),
        # Zploštělá infrastruktura
        pa.field("water", pa.string(), nullable=True),
        pa.field("electricity", pa.bool_(), nullable=True),
        pa.field("waste", pa.string(), nullable=True),
        pa.field("heating", pa.string(), nullable=True),
        pa.field("gas", pa.bool_(), nullable=True),
        # Zploštělý právní kontext
        pa.field("ownership", pa.string(), nullable=True),
        pa.field("price_note", pa.string(), nullable=True),
        pa.field("available_from", pa.date32(), nullable=True),
        pa.field("is_foreclosure", pa.bool_(), nullable=True),
        pa.field("is_insolvency", pa.bool_(), nullable=True),
        pa.field("is_share", pa.bool_(), nullable=True),
        # Metrika ploch a parametrů
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
        # Odvozená analytická metrika
        pa.field("price_per_m2", pa.float64(), nullable=True),
    ]
)
