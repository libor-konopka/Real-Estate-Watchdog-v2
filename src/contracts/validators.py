import re

from src.contracts.base import Currency


def sanitize_czech_price(value: str | None) -> tuple[float | None, Currency]:
    """
    Parses complex price strings into a validated float and identifies currency.

    Handles:
    - '1 500 000 Kč', '1.500.000,-', '1.500.000,50 Kč'
    - '250 000 EUR', '1500.50 €', '$ 450,000'
    - Non-numeric strings: 'Dohodou', 'V textu', None -> (None, Currency.CZK)
    """
    if not value:
        return None, Currency.CZK

    raw = value.strip().lower()

    # 1. Detekce měny
    currency = Currency.CZK
    if "eur" in raw or "€" in raw:
        currency = Currency.EUR
    elif "usd" in raw or "$" in raw:
        currency = Currency.USD

    # 2. Odstranění měn, koncovek ', -' a všech variant mezer
    cleaned = (
        raw.replace("kč", "")
        .replace("czk", "")
        .replace("eur", "")
        .replace("€", "")
        .replace("usd", "")
        .replace("$", "")
        .replace(",-", "")
        .replace("\xa0", "")  # Non-breaking space
        .replace("\u202f", "")  # Narrow no-break space
    )
    cleaned = re.sub(r"\s+", "", cleaned)

    # Pokud řetězec neobsahuje žádné číslice (např. 'dohodou', 'vtextu')
    if not re.search(r"\d", cleaned):
        return None, currency

    # 3. Vyjmutí pouze číselného bloku s oddělovači (zahodí případná zbylá slova)
    match = re.search(r"[\d.,]+", cleaned)
    if not match:
        return None, currency

    num_str = match.group(0)

    # 4. Deterministické vyřešení oddělovačů
    has_dot = "." in num_str
    has_comma = "," in num_str

    if has_dot and has_comma:
        # Příklad: 1.500.000,50 vs 1,500,000.50
        if num_str.rfind(",") > num_str.rfind("."):
            # Kontinentální formát: tečky tisíce, čárka desetiny
            num_str = num_str.replace(".", "").replace(",", ".")
        else:
            # Anglosaský formát: čárky tisíce, tečka desetiny
            num_str = num_str.replace(",", "")
    elif has_comma:
        # Příklad: 15000,50 nebo 1,500,000
        if num_str.count(",") > 1:
            num_str = num_str.replace(",", "")
        else:
            parts = num_str.split(",")
            # Pokud jsou za čárkou přesně 3 nuly/číslice a před ní 1-3 číslice,
            # mohlo by jít o tisíce, ale v ČR se čárka používá primárně jako desetina
            num_str = f"{parts[0]}.{parts[1]}"
    elif has_dot:
        if num_str.count(".") > 1:
            # Příklad: 1.500.000 -> odstranit všechny tečky
            num_str = num_str.replace(".", "")
        else:
            # Příklad jedné tečky: 15.000 vs 1500.50
            parts = num_str.split(".")
            # Pokud jsou za tečkou přesně 3 číslice a jde o typickou tisícovku
            if len(parts[1]) == 3 and len(parts[0]) <= 3:
                num_str = f"{parts[0]}{parts[1]}"

    try:
        parsed_price = float(num_str)
        # Sanitizace záporných hodnot nebo zjevných anomálií
        return (parsed_price if parsed_price >= 0 else None), currency
    except ValueError:
        return None, currency
