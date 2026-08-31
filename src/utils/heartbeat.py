"""Module for a periodic liveness heartbeat, probed by the Dockerfile's HEALTHCHECK."""

import asyncio
from pathlib import Path

DEFAULT_HEARTBEAT_PATH = Path("/tmp/heartbeat")


async def run_heartbeat(path: Path = DEFAULT_HEARTBEAT_PATH, interval_seconds: float = 30) -> None:
    """
    Periodically touches a file to prove the event loop is alive and responsive.

    HEALTHCHECK checks this file's mtime; if the event loop deadlocks or a task
    hangs, this coroutine stops being scheduled and the file goes stale, marking
    the container unhealthy even when the process hasn't crashed.

    Args:
        path (Path, optional): File to touch on each heartbeat.
        interval_seconds (float, optional): Seconds between heartbeats.
    """
    while True:
        path.touch()
        await asyncio.sleep(interval_seconds)
