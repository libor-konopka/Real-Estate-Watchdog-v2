import itertools
import os

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

# Pověření pro komunikaci s lokálním MinIO a Nessie
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["AWS_ALLOW_HTTP"] = "true"
os.environ["AWS_REGION"] = "us-east-1"

# Inicializace katalogu
catalog = load_catalog(
    "default",
    **{
        "type": "rest",
        "uri": "http://localhost:19120/iceberg/",
        "s3.endpoint": "http://localhost:9000",
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    },
)

# Definice transakcí a entit pro portál Bazos
transactions = ["prodam", "pronajmu"]
entities = [
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
]

# Vygenerování všech možných kombinací
targets = list(itertools.product(transactions, entities))

print("Zahajuji plošné mazání tabulek Bazoše z vrstev Bronze a Silver...")

for transaction, entity in targets:
    # Definice názvů tabulek pro Bronze i Silver vrstvu
    tables_to_delete = [
        f"bronze.bazos_{transaction}_{entity}",
        f"silver.bazos_{transaction}_{entity}",
    ]

    for table_identifier in tables_to_delete:
        try:
            catalog.drop_table(table_identifier)
            print(f"[ÚSPĚCH] Tabulka '{table_identifier}' byla smazána.")
        except NoSuchTableError:
            print(f"[SKIP] Tabulka '{table_identifier}' neexistuje.")
        except Exception as e:
            print(f"[CHYBA] Selhalo smazání tabulky '{table_identifier}': {e}")

print("Mazání dokončeno.")
