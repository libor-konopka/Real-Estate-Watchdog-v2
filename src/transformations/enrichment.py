import csv
from functools import lru_cache
from pathlib import Path

# Statická dimenzionální mapa pro odvození kraje z okresu
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
def load_psc_lookup() -> dict[str, dict[str, str]]:
    """
    Načte číselník obcí do paměti a dynamicky dopočítá kraj přes statickou mapu okresů.
    """
    lookup: dict[str, dict[str, str]] = {}

    current_dir = Path(__file__).resolve().parent
    project_root = current_dir.parent.parent
    file_path = project_root / "data" / "static" / "psc_kraje.csv"

    if not file_path.exists():
        return lookup

    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            psc = row["zipcode"].replace(" ", "")
            district = row["district"]

            # Párování kraje s fallbackem pro případ neznámého okresu
            region = DISTRICT_TO_REGION.get(district, "Unknown")

            lookup[psc] = {
                "region": region,
                "district": district,
                "municipality": row["name"],
            }

    return lookup
