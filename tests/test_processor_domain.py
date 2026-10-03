from __future__ import annotations

import pytest

from lab_processor.domain import quote_order


def test_quote_is_deterministic_and_applies_known_coupon() -> None:
    quote = quote_order(
        "DROP-SNEAKER-RED",
        2,
        "drop10",
        quoted_at="2026-08-30T10:00:00.000Z",
    )

    assert quote.currency == "USD"
    assert quote.unit_price_cents == 18_900
    assert quote.discount_cents == 3_780
    assert quote.total_cents == 34_020
    assert quote.price_version == "flashdrop-2026-08"
    assert quote.quoted_at == "2026-08-30T10:00:00.000Z"


def test_unknown_coupon_is_not_a_pricing_error() -> None:
    quote = quote_order("DROP-CAP-LIME", 1, "NO-SUCH-COUPON")
    assert quote.discount_cents == 0
    assert quote.total_cents == 4_900


@pytest.mark.parametrize(
    ("sku", "quantity", "message"),
    [
        ("UNKNOWN", 1, "unsupported sku"),
        ("DROP-CAP-LIME", 0, "between 1 and 5"),
        ("DROP-CAP-LIME", 6, "between 1 and 5"),
    ],
)
def test_invalid_catalog_input_is_rejected(
    sku: str, quantity: int, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        quote_order(sku, quantity)
