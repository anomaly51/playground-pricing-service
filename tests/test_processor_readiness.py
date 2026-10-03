from __future__ import annotations

import asyncio
from collections import deque
from types import SimpleNamespace
from typing import Any

import pytest

from lab_processor import publisher as publisher_module
from lab_processor.config import Settings
from lab_processor.ops import ServiceStatus
from lab_processor.publisher import TracePublisher


class FakeProducer:
    def __init__(self, outcomes: list[Any], client: Any | None = None) -> None:
        self.outcomes = deque(outcomes)
        self.client = client or AlwaysHealthyClient()
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def send_and_wait(self, *_: Any, **__: Any) -> Any:
        outcome = self.outcomes.popleft()
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def stop(self) -> None:
        self.stopped = True


class AlwaysHealthyClient:
    async def fetch_all_metadata(self) -> object:
        return object()


class RecoveringProbeClient:
    def __init__(self) -> None:
        self.calls = 0
        self.failure_observed = asyncio.Event()
        self.recovery_gate = asyncio.Event()
        self.recovery_observed = asyncio.Event()

    async def fetch_all_metadata(self) -> object:
        self.calls += 1
        if self.calls == 1:
            self.failure_observed.set()
            raise RuntimeError("metadata unavailable")
        await self.recovery_gate.wait()
        self.recovery_observed.set()
        return object()


def settings(probe_interval_seconds: float = 60.0) -> Settings:
    return Settings(
        http_host="127.0.0.1",
        http_port=8001,
        kafka_enabled=True,
        kafka_bootstrap_servers="kafka:29092",
        kafka_trace_topic="flashdrop.traces.v1",
        kafka_client_id="test-pricing-service",
        quote_timeout_seconds=1.0,
        pricing_timeout_delay_seconds=2.0,
        kafka_publish_timeout_seconds=1.0,
        kafka_probe_interval_seconds=probe_interval_seconds,
        kafka_probe_timeout_seconds=0.1,
        shutdown_grace_seconds=1.0,
        max_request_bytes=1_048_576,
    )


@pytest.mark.asyncio
async def test_pricing_readiness_does_not_depend_on_kafka_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer = FakeProducer(
        [
            RuntimeError("broker unavailable"),
            SimpleNamespace(partition=1, offset=42),
        ]
    )
    monkeypatch.setattr(publisher_module, "AIOKafkaProducer", lambda **_: producer)
    publisher = TracePublisher(settings())
    status = ServiceStatus(http_started=True, kafka_ready_check=lambda: publisher.ready)
    event = {
        "id": "01K3TQ1YW4H3G2VJ8ZA0Z5X8RM",
        "traceId": "01K3TQ1YW4H3G2VJ8ZA0Z5X8RN",
    }

    assert publisher.ready is False
    assert status.ready is True

    await publisher.start()
    assert producer.started is True
    assert publisher.ready is True
    assert status.ready is True

    assert await publisher.publish(event) is False
    assert publisher.ready is False
    assert status.ready is True

    assert await publisher.publish(event) is True
    assert publisher.ready is True
    assert status.ready is True

    await publisher.stop()
    assert producer.stopped is True
    assert publisher.ready is False
    assert status.ready is True


@pytest.mark.asyncio
async def test_periodic_probe_detects_outage_and_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = RecoveringProbeClient()
    producer = FakeProducer([], client=client)
    monkeypatch.setattr(publisher_module, "AIOKafkaProducer", lambda **_: producer)
    publisher = TracePublisher(settings(probe_interval_seconds=0.01))

    await publisher.start()
    probe_task = publisher._probe_task
    assert probe_task is not None
    assert publisher.ready is True

    await asyncio.wait_for(client.failure_observed.wait(), 1.0)
    assert publisher.ready is False

    client.recovery_gate.set()
    await asyncio.wait_for(client.recovery_observed.wait(), 1.0)
    assert publisher.ready is True

    await publisher.stop()
    assert probe_task.done()
    assert publisher.ready is False
