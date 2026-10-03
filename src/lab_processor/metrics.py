from prometheus_client import Counter, Gauge, Histogram

HTTP_REQUESTS = Counter(
    "lab_processor_http_requests_total",
    "HTTP pricing requests by outcome.",
    ("status",),
)
HTTP_DURATION = Histogram(
    "lab_processor_http_request_duration_seconds",
    "Time spent serving pricing requests.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
KAFKA_PUBLISHED = Counter(
    "lab_processor_kafka_trace_events_total",
    "Trace event publish attempts by outcome.",
    ("status",),
)
KAFKA_READY = Gauge(
    "lab_processor_kafka_ready",
    "Whether the Kafka producer is ready (or disabled intentionally).",
)
