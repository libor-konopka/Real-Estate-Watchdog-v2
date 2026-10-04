def sanitize_czech_price(value: str) -> str | None:
    """
    Purifies the financial string.
    Strips whitespace and currency symbols. If the remaining matter is not purely numeric, returns None.
    """
    # Fast removal of common unwanted characters
    for char in ("kč", "eur", "€", " "):
        value = value.lower().replace(char, "")

    # Handling non-breaking spaces (NBSP)
    cleaned = "".join(value.split())

    return cleaned if cleaned.isnumeric() else None
