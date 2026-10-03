from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import AsyncIterator

from aiohttp import web

from .config import Settings
from .log import configure_logging
from .ops import ServiceStatus, create_ops_app
from .publisher import TracePublisher
from .service import PricingService

LOGGER = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    publisher: TracePublisher | None = None,
) -> web.Application:
    settings = settings or Settings.from_env()
    publisher = publisher or TracePublisher(settings)
    pricing_service = PricingService(settings, publisher)
    status = ServiceStatus(
        kafka_ready_check=lambda: publisher.ready,
        runtime_check=pricing_service.runtime,
    )
    app = create_ops_app(status, client_max_size=settings.max_request_bytes)
    app.router.add_post("/api/v1/quotes", pricing_service.quote)

    async def lifespan(_: web.Application) -> AsyncIterator[None]:
        try:
            await publisher.start()
            status.http_started = True
            yield
        finally:
            status.http_started = False
            await publisher.stop()

    async def shutdown(_: web.Application) -> None:
        status.shutting_down = True

    app.cleanup_ctx.append(lifespan)
    app.on_shutdown.append(shutdown)
    return app


async def serve(settings: Settings | None = None) -> None:
    settings = settings or Settings.from_env()
    runner = web.AppRunner(
        create_app(settings),
        access_log=None,
        handler_cancellation=True,
        shutdown_timeout=settings.shutdown_grace_seconds,
    )
    await runner.setup()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals = []
    try:
        await web.TCPSite(runner, settings.http_host, settings.http_port).start()
        for handled_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(handled_signal, stop_event.set)
                registered_signals.append(handled_signal)
            except NotImplementedError:
                pass
        LOGGER.info(
            "FlashDrop PricingService started",
            extra={"httpPort": settings.http_port, "kafkaTopic": settings.kafka_trace_topic},
        )
        await stop_event.wait()
    finally:
        for handled_signal in registered_signals:
            loop.remove_signal_handler(handled_signal)
        await runner.cleanup()
        LOGGER.info("FlashDrop PricingService stopped")


def main() -> None:
    configure_logging()
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
