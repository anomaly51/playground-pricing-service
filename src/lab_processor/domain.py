from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

SKU_PRICES_CENTS = {
    "DROP-SNEAKER-RED": 18_900,
    "DROP-HOODIE-BLACK": 12_900,
    "DROP-CAP-LIME": 4_900,
}

COUPON_DISCOUNTS_PERCENT = {
    "DROP10": 10,
    "VIP20": 20,
}


@dataclass(frozen=True, slots=True)
class Quote:
    currency: str
    unit_price_cents: int
    discount_cents: int
    total_cents: int
    price_version: str
    quoted_at: str


def quote_order(
    sku: str,
    quantity: int,
    coupon_code: str | None = None,
    *,
    quoted_at: str | None = None,
) -> Quote:
    """Return a deterministic FlashDrop quote for the supplied catalog inputs."""

    try:
        unit_price_cents = SKU_PRICES_CENTS[sku]
    except KeyError as error:
        raise ValueError(f"unsupported sku: {sku}") from error
    if not 1 <= quantity <= 5:
        raise ValueError("quantity must be between 1 and 5")

    gross_cents = unit_price_cents * quantity
    normalized_coupon = (coupon_code or "").strip().upper()
    discount_percent = COUPON_DISCOUNTS_PERCENT.get(normalized_coupon, 0)
    discount_cents = gross_cents * discount_percent // 100
    timestamp = quoted_at or (
        datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )
    return Quote(
        currency="USD",
        unit_price_cents=unit_price_cents,
        discount_cents=discount_cents,
        total_cents=gross_cents - discount_cents,
        price_version="flashdrop-2026-08",
        quoted_at=timestamp,
    )
