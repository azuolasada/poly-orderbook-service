"""Periodic logging of the process's resident memory (RSS), read from /proc."""

import asyncio
import os

from src.utils.logger import logger

_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")


def _read_rss_bytes() -> int:
    """Returns the current process's resident set size in bytes (Linux only)."""
    with open("/proc/self/statm") as f:
        resident_pages = int(f.read().split()[1])
    return resident_pages * _PAGE_SIZE


async def monitor_memory(interval_seconds: float = 300) -> None:
    """
    Periodically logs the process's resident memory (RSS).

    Args:
        interval_seconds (float, optional): Seconds between samples.
    """
    while True:
        logger.info(f"Memory usage: {round(_read_rss_bytes() / 1024 / 1024, 2)} MB")
        await asyncio.sleep(interval_seconds)
