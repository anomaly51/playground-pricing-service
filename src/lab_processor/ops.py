from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest


@dataclass(slots=True)
class ServiceStatus:
    http_started: bool = False
    shutting_down: bool = False
    kafka_ready_check: Callable[[], bool] = field(default=lambda: False, repr=False)
    runtime_check: Callable[[], dict[str, int]] = field(
        default=lambda: {"inFlight": 0, "total": 0}, repr=False
    )

    @property
    def kafka_ready(self) -> bool:
        return self.kafka_ready_check()

    @property
    def ready(self) -> bool:
        return self.http_started and not self.shutting_down


def create_ops_app(
    status: ServiceStatus, *, client_max_size: int = 1_048_576
) -> web.Application:
    app = web.Application(client_max_size=client_max_size)

    async def healthz(_: web.Request) -> web.Response:
        code = 503 if status.shutting_down else 200
        return web.json_response(
            {
                "status": "stopping" if status.shutting_down else "ok",
                "preview": "preview-e2e-20261004",
            },
            status=code,
        )

    async def readyz(_: web.Request) -> web.Response:
        return web.json_response(
            {
                "status": "ready" if status.ready else "not-ready",
                "http": status.http_started,
                "kafkaTelemetry": status.kafka_ready,
            },
            status=200 if status.ready else 503,
        )

    async def metrics(_: web.Request) -> web.Response:
        return web.Response(
            body=generate_latest(REGISTRY),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    async def operations_snapshot(_: web.Request) -> web.Response:
        runtime = status.runtime_check()
        return web.json_response(
            {
                "nodes": {
                    "pricing-service": {
                        "inFlight": runtime["inFlight"],
                        "total": runtime["total"],
                        "healthy": status.ready,
                    }
                }
            }
        )

    app.router.add_get("/healthz", healthz)
    app.router.add_get("/readyz", readyz)
    app.router.add_get("/metrics", metrics)
    app.router.add_get("/operations/snapshot", operations_snapshot)
    return app
