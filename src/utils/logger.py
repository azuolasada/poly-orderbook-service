"""Structlog-based logging: pretty console output in dev, JSON lines in prod."""

import logging
import sys
from typing import Literal

import structlog
from structlog.typing import Processor

# Third-party loggers that are too chatty below WARNING (e.g. websockets logs every frame at DEBUG).
_NOISY_LOGGERS = ("botocore", "boto3", "s3transfer", "urllib3", "httpx", "httpcore", "websockets", "asyncio")

logger: structlog.stdlib.BoundLogger = structlog.stdlib.get_logger("poly_orderbook")


def configure_logging(env: Literal["dev", "prod"]) -> None:
    """Configures structlog and the stdlib root logger. Call once at startup, before anything logs.

    Both structlog calls and stdlib records from third-party libraries go through the same
    renderer, so all output is consistently formatted: colored console in dev, JSON in prod.

    Args:
        env: Deployment environment. ``dev`` logs at DEBUG with a console renderer,
            ``prod`` logs at INFO as one JSON object per line.
    """
    is_dev = env == "dev"

    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),  # supports logger.info("x %s", y)
        structlog.processors.TimeStamper(fmt="iso", utc=not is_dev),
        structlog.processors.StackInfoRenderer(),
    ]

    structlog.configure(
        processors=[*shared_processors, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    renderer: Processor
    final_processors: list[Processor] = [structlog.stdlib.ProcessorFormatter.remove_processors_meta]
    if is_dev:
        renderer = structlog.dev.ConsoleRenderer()
    else:
        # Structured tracebacks, without frame locals (they could leak credentials or payloads).
        final_processors.append(
            structlog.processors.ExceptionRenderer(structlog.tracebacks.ExceptionDictTransformer(show_locals=False))
        )
        renderer = structlog.processors.JSONRenderer()
    final_processors.append(renderer)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(foreign_pre_chain=shared_processors, processors=final_processors)
    )

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.DEBUG if is_dev else logging.INFO)

    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
