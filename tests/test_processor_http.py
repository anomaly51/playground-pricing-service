from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from typing import Any

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from lab_processor import service as service_module
from lab_processor.config import Settings
from lab_processor.server import create_app

REQUEST = {
    "traceId": "01K3TQ1YW4H3G2VJ8ZA0Z5X8RM",
    "customerId": "customer-42",
    "sku": "DROP-SNEAKER-RED",
    "quantity": 2,
    "couponCode": "DROP10",
    "scenario": "normal",
    "runId": "test-run-42",
}


class FakePublisher:
    ready = False

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.stopped = False
        self.started = asyncio.Event()
        self.finished = asyncio.Event()

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self.stopped = True

    def publish_best_effort(self, event: dict[str, Any]) -> None:
        self.events.append(event)
        if event["status"] == "started":
            self.started.set()
        else:
            self.finished.set()


@pytest_asyncio.fixture
async def api():
    settings = replace(
        Settings.from_env(), kafka_enabled=False,
        quote_timeout_seconds=0.5, pricing_timeout_delay_seconds=1.0,
        max_request_bytes=1024,
    )
    publisher = FakePublisher()
    app = create_app(settings, publisher)
    async with TestClient(TestServer(app, handler_cancellation=True)) as client:
        yield client, publisher
    assert publisher.stopped


async def test_quote_returns_json_and_http_traces_without_kafka(api) -> None:
    client, publisher = api
    ready = await client.get("/readyz")
    assert ready.status == 200
    assert await ready.json() == {"status": "ready", "http": True, "kafkaTelemetry": False}
    response = await client.post("/api/v1/quotes", json=REQUEST)
    assert response.status == 200
    assert response.headers["x-correlation-id"] == REQUEST["traceId"]
    body = await response.json()
    assert body == {
        "currency": "USD", "unitPriceCents": 18900, "discountCents": 3780,
        "totalCents": 34020, "priceVersion": "flashdrop-2026-08",
        "quotedAt": body["quotedAt"],
    }
    assert body["quotedAt"].endswith("Z")
    assert [event["status"] for event in publisher.events] == ["started", "succeeded"]
    assert all(event["transport"] == "http" for event in publisher.events)
    assert all(event["traceId"] == REQUEST["traceId"] for event in publisher.events)
    assert all(event["runId"] == REQUEST["runId"] for event in publisher.events)
    snapshot = await (await client.get("/operations/snapshot")).json()
    assert snapshot["nodes"]["pricing-service"] == {"inFlight": 0, "total": 1, "healthy": True}
    metrics = await (await client.get("/metrics")).text()
    assert "lab_processor_http_requests_total" in metrics


@pytest.mark.parametrize("changes", [
    {"traceId": "invalid"}, {"traceId": 12}, {"customerId": "bad id"},
    {"quantity": True}, {"quantity": "2"}, {"quantity": 2.5}, {"quantity": 0},
    {"quantity": 6}, {"sku": "UNKNOWN"}, {"scenario": "unknown"},
    {"scenario": None}, {"runId": []}, {"couponCode": {}}, {"extra": "field"},
])
async def test_quote_rejects_invalid_fields(api, changes) -> None:
    client, _ = api
    response = await client.post("/api/v1/quotes", json={**REQUEST, **changes})
    assert response.status == 400
    assert (await response.json())["error"] == "invalid_request"


@pytest.mark.parametrize("body", [[], "string", None, {"quantity": 1}])
async def test_quote_requires_an_object_with_required_fields(api, body) -> None:
    client, _ = api
    response = await client.post(
        "/api/v1/quotes", data=json.dumps(body), headers={"Content-Type": "application/json"}
    )
    assert response.status == 400


async def test_quote_handles_bad_json_content_type_and_size(api) -> None:
    client, _ = api
    response = await client.post(
        "/api/v1/quotes", data="{", headers={"Content-Type": "application/json"}
    )
    assert response.status == 400
    response = await client.post("/api/v1/quotes", data="not json")
    assert response.status == 415
    response = await client.post("/api/v1/quotes", json={**REQUEST, "padding": "x" * 2048})
    assert response.status == 413


@pytest.mark.parametrize(("scenario", "status", "error"), [
    ("pricing-error", 500, "pricing_error"),
    ("pricing-timeout", 504, "pricing_timeout"),
])
async def test_fault_scenarios_return_http_errors_and_failure_traces(api, scenario, status, error):
    client, publisher = api
    response = await client.post("/api/v1/quotes", json={**REQUEST, "scenario": scenario})
    assert response.status == status
    assert (await response.json())["error"] == error
    assert [event["status"] for event in publisher.events] == ["started", "failed"]
    snapshot = await (await client.get("/operations/snapshot")).json()
    assert snapshot["nodes"]["pricing-service"]["inFlight"] == 0


async def test_quote_hides_unexpected_errors(api, monkeypatch) -> None:
    client, _ = api

    def crash(*_args):
        raise RuntimeError("private internal details")

    monkeypatch.setattr(service_module, "quote_order", crash)
    response = await client.post("/api/v1/quotes", json=REQUEST)
    assert response.status == 500
    assert "private" not in await response.text()


async def test_disconnect_cancels_pricing_and_clears_in_flight(api) -> None:
    client, publisher = api
    pending = asyncio.create_task(client.post(
        "/api/v1/quotes", json={**REQUEST, "scenario": "pricing-timeout"}
    ))
    await asyncio.wait_for(publisher.started.wait(), 1)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    await asyncio.wait_for(publisher.finished.wait(), 1)
    assert publisher.events[-1]["status"] == "failed"
    assert publisher.events[-1]["summary"] == "pricing request cancelled"
    snapshot = await (await client.get("/operations/snapshot")).json()
    assert snapshot["nodes"]["pricing-service"]["inFlight"] == 0
