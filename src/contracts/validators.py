import re

from src.contracts.base import Currency

# Matches the first contiguous numeric sequence including space/dot/comma separators
_FIRST_NUMBER_BLOCK_RE = re.compile(r"\d+(?:[\s\xa0\u202f.,]\d+)*")


def sanitize_czech_price(value: str | None) -> tuple[float | None, Currency]:
    """Parses complex price strings into a validated float and identifies currency.

    Handles:
    - '1 500 000 Kč', '1.500.000,-', '1.500.000,50 Kč'
    - '250 000 EUR', '1500.50 €', '$ 450,000'
    - Multiple amounts in text (extracts only the primary leading price)
    - Non-numeric strings: 'Dohodou', 'V textu', None -> (None, Currency.CZK)
    """
    if not value:
        return None, Currency.CZK

    raw = value.strip().lower()

    # 1. Currency detection
    currency = Currency.CZK
    if "eur" in raw or "€" in raw:
        currency = Currency.EUR
    elif "usd" in raw or "$" in raw:
        currency = Currency.USD

    # 2. Extract the first contiguous numeric block before stripping spaces
    # Prevents concatenating separate amounts (e.g., "15 000 Kč + 3 000 Kč fees")
    match = _FIRST_NUMBER_BLOCK_RE.search(raw)
    if not match:
        return None, currency

    num_str = re.sub(r"[\s\xa0\u202f]+", "", match.group(0)).strip(".,")
    if not num_str:
        return None, currency

    # 3. Deterministic separator resolution
    has_dot = "." in num_str
    has_comma = "," in num_str

    if has_dot and has_comma:
        if num_str.rfind(",") > num_str.rfind("."):
            # Continental format: dots for thousands, comma for decimals
            num_str = num_str.replace(".", "").replace(",", ".")
        else:
            # Anglo-Saxon format: commas for thousands, dot for decimals
            num_str = num_str.replace(",", "")
    elif has_comma:
        if num_str.count(",") > 1:
            num_str = num_str.replace(",", "")
        else:
            parts = num_str.split(",")
            # Treat single comma with 3 trailing digits
            # and <=3 leading digits as thousands
            if len(parts[1]) == 3 and len(parts[0]) <= 3:
                num_str = f"{parts[0]}{parts[1]}"
            else:
                num_str = f"{parts[0]}.{parts[1]}"
    elif has_dot:
        if num_str.count(".") > 1:
            num_str = num_str.replace(".", "")
        else:
            parts = num_str.split(".")
            if len(parts[1]) == 3 and len(parts[0]) <= 3:
                num_str = f"{parts[0]}{parts[1]}"

    try:
        parsed_price = float(num_str)
        return (parsed_price if parsed_price >= 0 else None), currency
    except ValueError:
        return None, currency
