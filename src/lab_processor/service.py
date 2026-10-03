from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

import ulid
from aiohttp import web

from .config import Settings
from .domain import quote_order
from .events import Status, trace_event
from .metrics import HTTP_DURATION, HTTP_REQUESTS
from .publisher import TracePublisher

LOGGER = logging.getLogger(__name__)
LAB_SCENARIOS = {
    "normal",
    "pricing-timeout",
    "pricing-error",
    "inventory-retry",
    "inventory-dlq",
}


def parse_quote_request(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("request body must be a JSON object")
    allowed = {"traceId", "customerId", "sku", "quantity", "couponCode", "scenario", "runId"}
    if value.keys() - allowed:
        raise ValueError("request contains unsupported fields")
    for name in ("traceId", "customerId", "sku"):
        if not isinstance(value.get(name), str) or not value[name].strip():
            raise ValueError(f"{name} must be a non-empty string")
    result = dict(value)
    result["traceId"] = value["traceId"].strip()
    try:
        ulid.from_str(result["traceId"])
    except ValueError:
        raise ValueError("traceId must be a ULID") from None
    if not re.fullmatch(r"[A-Za-z0-9_-]{3,64}", value["customerId"]):
        raise ValueError("customerId must be a valid identifier")
    if type(value.get("quantity")) is not int or not 1 <= value["quantity"] <= 5:
        raise ValueError("quantity must be an integer between 1 and 5")
    coupon = value.get("couponCode")
    if coupon is not None and (
        not isinstance(coupon, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", coupon)
    ):
        raise ValueError("couponCode must be a valid coupon")
    scenario = value.get("scenario", "normal")
    if not isinstance(scenario, str) or scenario not in LAB_SCENARIOS:
        raise ValueError("unsupported scenario")
    result["scenario"] = scenario
    run_id = value.get("runId")
    if run_id is not None and (
        not isinstance(run_id, str) or not 1 <= len(run_id.strip()) <= 128
    ):
        raise ValueError("runId must be a non-empty string of at most 128 characters")
    if run_id is not None:
        result["runId"] = run_id.strip()
    return result


class PricingService:
    def __init__(self, settings: Settings, publisher: TracePublisher) -> None:
        self._settings = settings
        self._publisher = publisher
        self._in_flight = 0
        self._total = 0

    def runtime(self) -> dict[str, int]:
        return {"inFlight": self._in_flight, "total": self._total}

    async def quote(self, request: web.Request) -> web.Response:
        started_at = time.monotonic()
        self._in_flight += 1
        payload: dict[str, Any] | None = None
        outcome = "failed"
        try:
            if request.content_type != "application/json":
                raise web.HTTPUnsupportedMediaType(reason="Content-Type must be application/json")
            try:
                body = await request.json()
            except ValueError:
                raise ValueError("request body must contain valid JSON") from None
            payload = parse_quote_request(body)
            self._trace(payload, "started", "PricingService received a quote request")
            async with asyncio.timeout(self._settings.quote_timeout_seconds):
                if payload["scenario"] == "pricing-timeout":
                    await asyncio.sleep(self._settings.pricing_timeout_delay_seconds)
                    raise TimeoutError("injected pricing timeout")
                if payload["scenario"] == "pricing-error":
                    raise RuntimeError("injected pricing error")
                result = await asyncio.to_thread(
                    quote_order,
                    payload["sku"],
                    payload["quantity"],
                    payload.get("couponCode"),
                )
            response = {
                "currency": result.currency,
                "unitPriceCents": result.unit_price_cents,
                "discountCents": result.discount_cents,
                "totalCents": result.total_cents,
                "priceVersion": result.price_version,
                "quotedAt": result.quoted_at,
            }
            self._trace(payload, "succeeded", "PricingService returned a FlashDrop quote", response)
            outcome = "succeeded"
            LOGGER.info(
                "Order quoted",
                extra={"traceId": payload["traceId"], "sku": payload["sku"]},
            )
            return web.json_response(response, headers={"x-correlation-id": payload["traceId"]})
        except web.HTTPException as error:
            return web.json_response(
                {"error": "invalid_request", "message": error.reason}, status=error.status
            )
        except ValueError as error:
            self._trace(payload, "failed", str(error))
            return web.json_response(
                {"error": "invalid_request", "message": str(error)}, status=400
            )
        except TimeoutError:
            self._trace(payload, "failed", "pricing quote deadline exceeded")
            return web.json_response(
                {"error": "pricing_timeout", "message": "Pricing quote deadline exceeded"},
                status=504,
            )
        except asyncio.CancelledError:
            self._trace(payload, "failed", "pricing request cancelled")
            raise
        except Exception:
            LOGGER.exception("Unhandled pricing error")
            self._trace(payload, "failed", "internal pricing error")
            return web.json_response(
                {"error": "pricing_error", "message": "Internal pricing error"}, status=500
            )
        finally:
            self._in_flight -= 1
            self._total += 1
            HTTP_REQUESTS.labels(status=outcome).inc()
            HTTP_DURATION.observe(time.monotonic() - started_at)

    def _trace(
        self,
        payload: dict[str, Any] | None,
        status: Status,
        summary: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        if payload is None:
            return
        self._publisher.publish_best_effort(
            trace_event(
                trace_id=payload["traceId"],
                source="gateway" if status == "started" else "pricing-service",
                target="pricing-service" if status == "started" else "gateway",
                transport="http",
                stage="pricing.quote",
                status=status,
                summary=summary,
                payload={
                    "sku": payload["sku"],
                    "quantity": payload["quantity"],
                    "scenario": payload["scenario"],
                    **(result or {}),
                },
                run_id=payload.get("runId"),
            )
        )
