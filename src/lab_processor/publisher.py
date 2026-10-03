from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from aiokafka import AIOKafkaProducer

from .config import Settings
from .events import encode_event
from .metrics import KAFKA_PUBLISHED, KAFKA_READY

LOGGER = logging.getLogger(__name__)


class TracePublisher:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None
        self._probe_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._ready = False
        self._pending: set[asyncio.Task[bool]] = set()

    @property
    def ready(self) -> bool:
        if self._stopping.is_set():
            return False
        if not self._settings.kafka_enabled:
            return self._ready
        task = self._probe_task
        return (
            self._ready
            and self._producer is not None
            and task is not None
            and not task.done()
        )

    async def start(self) -> None:
        self._stopping.clear()
        self._set_ready(False)
        if not self._settings.kafka_enabled:
            self._set_ready(True)
            LOGGER.warning("Kafka publishing is disabled")
            return

        try:
            await self._connect()
        except Exception:
            self._set_ready(False)
            LOGGER.exception(
                "Kafka trace producer unavailable; pricing remains available"
            )
        self._probe_task = asyncio.create_task(
            self._probe_loop(), name="pricing-kafka-telemetry-supervisor"
        )
        self._probe_task.add_done_callback(self._probe_task_done)

    async def _connect(self) -> None:
        if self._producer is not None:
            return
        producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            client_id=self._settings.kafka_client_id,
            acks="all",
            enable_idempotence=True,
            request_timeout_ms=int(self._settings.kafka_publish_timeout_seconds * 1000),
        )
        try:
            async with asyncio.timeout(self._settings.kafka_publish_timeout_seconds):
                await producer.start()
        except Exception:
            with contextlib.suppress(Exception):
                await producer.stop()
            self._set_ready(False)
            raise
        self._producer = producer
        self._set_ready(True)
        LOGGER.info(
            "Kafka trace producer started",
            extra={"topic": self._settings.kafka_trace_topic},
        )

    async def publish(self, event: dict[str, Any]) -> bool:
        if not self._settings.kafka_enabled:
            return True
        producer = self._producer
        if producer is None:
            KAFKA_PUBLISHED.labels(status="failed").inc()
            self._set_ready(False)
            return False

        try:
            async with asyncio.timeout(self._settings.kafka_publish_timeout_seconds):
                metadata = await producer.send_and_wait(
                    self._settings.kafka_trace_topic,
                    value=encode_event(event),
                    key=event["traceId"].encode("utf-8"),
                )
            KAFKA_PUBLISHED.labels(status="succeeded").inc()
            self._set_ready(True)
            LOGGER.debug(
                "Trace event published",
                extra={
                    "traceId": event["traceId"],
                    "eventId": event["id"],
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                },
            )
            return True
        except Exception:
            KAFKA_PUBLISHED.labels(status="failed").inc()
            self._set_ready(False)
            LOGGER.exception(
                "Unable to publish trace event",
                extra={
                    "traceId": event.get("traceId"),
                    "eventId": event.get("id"),
                },
            )
            return False

    def publish_best_effort(self, event: dict[str, Any]) -> None:
        """Schedule bounded telemetry without delaying the pricing request."""
        if len(self._pending) >= 1_000:
            KAFKA_PUBLISHED.labels(status="dropped").inc()
            return
        task = asyncio.create_task(self.publish(event))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def stop(self) -> None:
        self._stopping.set()
        self._set_ready(False)
        task, self._probe_task = self._probe_task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        pending = tuple(self._pending)
        self._pending.clear()
        for publication in pending:
            publication.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        producer, self._producer = self._producer, None
        if producer is not None:
            await producer.stop()
            LOGGER.info("Kafka trace producer stopped")

    async def _probe_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(
                    self._stopping.wait(),
                    timeout=self._settings.kafka_probe_interval_seconds,
                )
                return
            except TimeoutError:
                if self._producer is None:
                    try:
                        await self._connect()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self._set_ready(False)
                else:
                    await self._probe_connectivity()

    async def _probe_connectivity(self) -> None:
        was_ready = self._ready
        try:
            producer = self._producer
            if producer is None:
                raise RuntimeError("Kafka producer is not started")
            async with asyncio.timeout(self._settings.kafka_probe_timeout_seconds):
                await producer.client.fetch_all_metadata()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._set_ready(False)
            if was_ready:
                LOGGER.warning(
                    "Kafka connectivity probe failed",
                    extra={"errorType": type(error).__name__},
                )
        else:
            self._set_ready(True)
            if not was_ready:
                LOGGER.info("Kafka connectivity probe recovered")

    def _probe_task_done(self, task: asyncio.Task[None]) -> None:
        error: BaseException | None = None
        if not task.cancelled():
            error = task.exception()
        if not self._stopping.is_set():
            self._set_ready(False)
            LOGGER.error(
                "Kafka connectivity probe stopped unexpectedly",
                extra={"error": repr(error)},
            )

    def _set_ready(self, ready: bool) -> None:
        self._ready = ready
        KAFKA_READY.set(1 if self.ready else 0)
