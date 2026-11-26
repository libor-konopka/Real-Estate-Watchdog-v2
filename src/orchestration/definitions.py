from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from src.orchestration.assets.bronze_assets import (
    bronze_bazos_listings,
    bronze_bezrealitky_listings,
)
from src.orchestration.assets.gold_assets import gold_market_analytics
from src.orchestration.assets.silver_assets import silver_properties

# Asset collection representing the full Medallion Lakehouse
all_assets = [
    bronze_bazos_listings,
    bronze_bezrealitky_listings,
    silver_properties,
    gold_market_analytics,
]

# Job orchestrating execution of the entire pipeline graph
lakehouse_etl_job = define_asset_job(
    name="real_estate_lakehouse_job",
    selection=AssetSelection.all(),
    description=(
        "Executes end-to-end extraction, normalization, entity resolution, "
        "and gold datamart materialization."
    ),
)

# Daily scheduled run at 04:00 UTC
daily_refresh_schedule = ScheduleDefinition(
    job=lakehouse_etl_job,
    cron_schedule="0 4 * * *",
    execution_timezone="UTC",
)

defs = Definitions(
    assets=all_assets,
    jobs=[lakehouse_etl_job],
    schedules=[daily_refresh_schedule],
)
