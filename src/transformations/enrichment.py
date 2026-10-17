from functools import lru_cache
from pathlib import Path

import polars as pl
from loguru import logger

DISTRICT_TO_REGION = {
    "Benešov": "Středočeský",
    "Beroun": "Středočeský",
    "Kladno": "Středočeský",
    "Kolín": "Středočeský",
    "Kutná Hora": "Středočeský",
    "Mělník": "Středočeský",
    "Mladá Boleslav": "Středočeský",
    "Nymburk": "Středočeský",
    "Praha-východ": "Středočeský",
    "Praha-západ": "Středočeský",
    "Příbram": "Středočeský",
    "Rakovník": "Středočeský",
    "České Budějovice": "Jihočeský",
    "Český Krumlov": "Jihočeský",
    "Jindřichův Hradec": "Jihočeský",
    "Písek": "Jihočeský",
    "Prachatice": "Jihočeský",
    "Strakonice": "Jihočeský",
    "Tábor": "Jihočeský",
    "Domažlice": "Plzeňský",
    "Klatovy": "Plzeňský",
    "Plzeň-jih": "Plzeňský",
    "Plzeň-město": "Plzeňský",
    "Plzeň-sever": "Plzeňský",
    "Rokycany": "Plzeňský",
    "Tachov": "Plzeňský",
    "Cheb": "Karlovarský",
    "Karlovy Vary": "Karlovarský",
    "Sokolov": "Karlovarský",
    "Děčín": "Ústecký",
    "Chomutov": "Ústecký",
    "Litoměřice": "Ústecký",
    "Louny": "Ústecký",
    "Most": "Ústecký",
    "Teplice": "Ústecký",
    "Ústí nad Labem": "Ústecký",
    "Česká Lípa": "Liberecký",
    "Jablonec nad Nisou": "Liberecký",
    "Liberec": "Liberecký",
    "Semily": "Liberecký",
    "Hradec Králové": "Královéhradecký",
    "Jičín": "Královéhradecký",
    "Náchod": "Královéhradecký",
    "Rychnov nad Kněžnou": "Královéhradecký",
    "Trutnov": "Královéhradecký",
    "Chrudim": "Pardubický",
    "Pardubice": "Pardubický",
    "Svitavy": "Pardubický",
    "Ústí nad Orlicí": "Pardubický",
    "Havlíčkův Brod": "Vysočina",
    "Jihlava": "Vysočina",
    "Pelhřimov": "Vysočina",
    "Třebíč": "Vysočina",
    "Žďár nad Sázavou": "Vysočina",
    "Blansko": "Jihomoravský",
    "Brno-město": "Jihomoravský",
    "Brno-venkov": "Jihomoravský",
    "Břeclav": "Jihomoravský",
    "Hodonín": "Jihomoravský",
    "Vyškov": "Jihomoravský",
    "Znojmo": "Jihomoravský",
    "Jeseník": "Olomoucký",
    "Olomouc": "Olomoucký",
    "Prostějov": "Olomoucký",
    "Přerov": "Olomoucký",
    "Šumperk": "Olomoucký",
    "Kroměříž": "Zlínský",
    "Uherské Hradiště": "Zlínský",
    "Vsetín": "Zlínský",
    "Zlín": "Zlínský",
    "Bruntál": "Moravskoslezský",
    "Frýdek-Místek": "Moravskoslezský",
    "Karviná": "Moravskoslezský",
    "Nový Jičín": "Moravskoslezský",
    "Opava": "Moravskoslezský",
    "Ostrava-město": "Moravskoslezský",
    "Hlavní město Praha": "Praha",
}


@lru_cache(maxsize=1)
def load_geo_lookup_df() -> pl.DataFrame:
    """Loads postal codes lookup and returns a Polars DataFrame with normalized columns."""
    current_dir = Path(__file__).resolve().parent
    project_root = current_dir.parent.parent
    file_path = project_root / "data" / "static" / "psc_kraje.csv"

    if not file_path.exists():
        logger.warning(f"Geo lookup dataset not found at {file_path}")
        return pl.DataFrame(
            schema={
                "postal_code": pl.String,
                "enriched_region": pl.String,
                "enriched_district": pl.String,
                "enriched_municipality": pl.String,
            }
        )

    df = pl.read_csv(file_path)

    # Normalize postal code (strip whitespace)
    df = df.with_columns(
        pl.col("zipcode").str.replace_all(r"\s+", "").alias("postal_code"),
        pl.col("district")
        .replace_strict(DISTRICT_TO_REGION, default="Unknown")
        .alias("enriched_region"),
        pl.col("district").alias("enriched_district"),
        pl.col("name").alias("enriched_municipality"),
    ).select(
        ["postal_code", "enriched_region", "enriched_district", "enriched_municipality"]
    )

    return df
