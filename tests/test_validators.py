import pytest

from src.contracts.base import Currency
from src.contracts.validators import sanitize_czech_price


@pytest.mark.parametrize(
    ("raw_value", "expected_price", "expected_currency"),
    [
        ("4 850 000 Kč", 4_850_000.0, Currency.CZK),
        ("1.500.000,-", 1_500_000.0, Currency.CZK),
        ("1.500.000,50 Kč", 1_500_000.5, Currency.CZK),
        ("250 000 EUR", 250_000.0, Currency.EUR),
        ("1500.50 €", 1_500.5, Currency.EUR),
        ("$ 450,000", 450_000.0, Currency.USD),
        ("15 000 Kč + 3 000 Kč poplatky", 15_000.0, Currency.CZK),
        ("Dohodou", None, Currency.CZK),
        ("V textu", None, Currency.CZK),
        ("", None, Currency.CZK),
        (None, None, Currency.CZK),
    ],
)
def test_sanitize_czech_price_handles_formats_and_edge_cases(
    raw_value: str | None,
    expected_price: float | None,
    expected_currency: Currency,
) -> None:
    price, currency = sanitize_czech_price(raw_value)

    assert price == expected_price
    assert currency == expected_currency
