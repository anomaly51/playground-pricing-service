from __future__ import annotations

import os
from dataclasses import dataclass

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def _boolean(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"{name} must be a boolean value")


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    value = default if raw is None else int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    value = default if raw is None else float(raw)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    http_host: str
    http_port: int
    kafka_enabled: bool
    kafka_bootstrap_servers: str
    kafka_trace_topic: str
    kafka_client_id: str
    quote_timeout_seconds: float
    pricing_timeout_delay_seconds: float
    kafka_publish_timeout_seconds: float
    kafka_probe_interval_seconds: float
    kafka_probe_timeout_seconds: float
    shutdown_grace_seconds: float
    max_request_bytes: int

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            http_host=os.getenv("LAB_PROCESSOR_HTTP_HOST", "0.0.0.0"),
            http_port=_positive_int("LAB_PROCESSOR_HTTP_PORT", 8001),
            kafka_enabled=_boolean("LAB_KAFKA_ENABLED", True),
            kafka_bootstrap_servers=os.getenv(
                "LAB_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
            ),
            kafka_trace_topic=os.getenv(
                "FLASHDROP_KAFKA_TRACE_TOPIC",
                os.getenv("LAB_KAFKA_TRACE_TOPIC", "flashdrop.traces.v1"),
            ),
            kafka_client_id=os.getenv(
                "FLASHDROP_KAFKA_CLIENT_ID",
                os.getenv("LAB_KAFKA_CLIENT_ID", "flashdrop-pricing-service"),
            ),
            quote_timeout_seconds=_positive_float(
                "FLASHDROP_PRICING_QUOTE_TIMEOUT_SECONDS", 2.0
            ),
            pricing_timeout_delay_seconds=_positive_float(
                "FLASHDROP_PRICING_TIMEOUT_DELAY_SECONDS", 3.0
            ),
            kafka_publish_timeout_seconds=_positive_float(
                "LAB_KAFKA_PUBLISH_TIMEOUT_SECONDS", 2.0
            ),
            kafka_probe_interval_seconds=_positive_float(
                "LAB_KAFKA_PROBE_INTERVAL_SECONDS", 2.0
            ),
            kafka_probe_timeout_seconds=_positive_float(
                "LAB_KAFKA_PROBE_TIMEOUT_SECONDS", 1.0
            ),
            shutdown_grace_seconds=_positive_float(
                "LAB_PROCESSOR_SHUTDOWN_GRACE_SECONDS", 10.0
            ),
            max_request_bytes=_positive_int("LAB_PROCESSOR_MAX_REQUEST_BYTES", 1_048_576),
        )
