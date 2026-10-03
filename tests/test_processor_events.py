from __future__ import annotations

import json
import re

from lab_processor.events import encode_event, trace_event

ULID_PATTERN = re.compile(r"^[0-7][0-9A-HJKMNP-TV-Z]{25}$")


def test_trace_event_uses_shared_camel_case_envelope() -> None:
    event = trace_event(
        trace_id="trace-1",
        source="processor",
        target="gateway",
        transport="http",
        stage="pricing.quote",
        status="succeeded",
        summary="done",
        payload={"wordCount": 2},
        timestamp="2026-08-28T10:00:00.000Z",
    )

    assert ULID_PATTERN.fullmatch(event["id"])
    assert event | {"id": "<ulid>"} == {
        "id": "<ulid>",
        "traceId": "trace-1",
        "timestamp": "2026-08-28T10:00:00.000Z",
        "source": "processor",
        "target": "gateway",
        "transport": "http",
        "stage": "pricing.quote",
        "status": "succeeded",
        "summary": "done",
        "payload": {"wordCount": 2},
    }
    assert json.loads(encode_event(event)) == event
