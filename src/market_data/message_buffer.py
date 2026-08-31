"""Module for buffering and flushing market data messages to S3 storage."""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import zstandard as zstd

from src.s3_storage import S3Storage
from src.utils.logger import logger


class MessageBuffer:
    """
    In-memory buffer that batches messages and flushes them as zstd-compressed JSONL to S3.

    On a failed flush, the batch is spilled to disk (under spill_dir) instead of staying in
    memory, bounding memory usage during a sustained S3 outage. Spilled batches are retried
    opportunistically whenever a flush succeeds or the buffer is otherwise idle. If spilled
    data exceeds max_spill_bytes, the oldest spilled batches are dropped as a last resort.

    Attributes:
        flush_interval_seconds (int): Max time between flushes, in seconds.
        flush_count_threshold (int): Max buffered message count before flushing.
        spill_dir (Path): Directory batches are written to when a flush fails.
        max_spill_bytes (int): Hard cap on total spilled bytes on disk.
        buffer (list[Any]): The currently buffered messages.
        last_flush_time (datetime): Timestamp of the last successful flush.
        s3_storage (S3Storage): The storage backend flushes are uploaded to.
    """

    def __init__(self,
                 flush_interval_seconds: int = 60,
                 flush_count_threshold: int = 100,
                 spill_dir: str = "./data/spill",
                 max_spill_bytes: int = 2 * 1024 * 1024 * 1024) -> None:
        """
        Initializes the MessageBuffer and ensures the target S3 bucket exists.

        Args:
            flush_interval_seconds (int, optional): Max time between flushes, in seconds.
            flush_count_threshold (int, optional): Max buffered message count before flushing.
            spill_dir (str, optional): Directory to write failed batches to for later retry.
            max_spill_bytes (int, optional): Hard cap on total spilled bytes on disk. Defaults to 2 GiB.
        """
        self.flush_interval_seconds = flush_interval_seconds
        self.flush_count_threshold = flush_count_threshold
        self.spill_dir = Path(spill_dir)
        self.spill_dir.mkdir(parents=True, exist_ok=True)
        self.max_spill_bytes = max_spill_bytes

        self.buffer: list[Any] = []
        self.last_flush_time = datetime.now()
        self._lock = asyncio.Lock()

        self.s3_storage = S3Storage()
        self.s3_storage.ensure_bucket_exists()

        self._compressor = zstd.ZstdCompressor(level=3)

    async def add_message(self, message: Any) -> None:
        """
        Adds a message to the buffer, flushing if a threshold is reached.

        Args:
            message (Any): The JSON-serializable message to buffer.
        """
        async with self._lock:
            self.buffer.append(message)

            time_since_last_flush = (datetime.now() - self.last_flush_time).total_seconds()

            if len(self.buffer) >= self.flush_count_threshold:
                logger.info(f"Buffer count threshold reached ({len(self.buffer)}). Flushing...")
                await self._flush_internal()
            elif time_since_last_flush >= self.flush_interval_seconds:
                logger.info(f"Buffer time threshold reached ({time_since_last_flush:.2f}s). Flushing...")
                await self._flush_internal()

    async def flush(self) -> None:
        """Public method to manually trigger a flush, including any previously spilled batches."""
        async with self._lock:
            if self.buffer:
                await self._flush_internal()
            else:
                await self._drain_spill()

    async def _flush_internal(self) -> None:
        """Internal flush logic, assumes lock is held."""
        if not self.buffer:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"messages_{timestamp}.jsonl.zst"

        content = "\n".join(json.dumps(msg) for msg in self.buffer) + "\n"
        compressed = self._compressor.compress(content.encode("utf-8"))

        try:
            await self.s3_storage.upload_bytes(data=compressed, key=filename)

            logger.info(f"Successfully flushed {len(self.buffer)} messages to {self.s3_storage.bucket}")
            self.buffer = []
            self.last_flush_time = datetime.now()
            await self._drain_spill()
        except Exception as e:
            logger.error(f"Failed to flush {len(self.buffer)} messages, spilling to disk: {e}")
            self._spill_to_disk(filename, compressed)
            self.buffer = []
            self.last_flush_time = datetime.now()

    def _spill_to_disk(self, filename: str, compressed: bytes) -> None:
        """
        Writes a batch that failed to upload to disk for later retry.

        Assumes the lock is held. Enforces max_spill_bytes by dropping the oldest spilled
        batches first if needed.

        Args:
            filename (str): Name to store the batch under.
            compressed (bytes): The zstd-compressed batch content.
        """
        self._enforce_spill_cap(incoming_size=len(compressed))
        path = self.spill_dir / filename
        path.write_bytes(compressed)
        logger.error(f"Spilled batch to {path} ({len(compressed)} bytes) pending retry.")

    def _enforce_spill_cap(self, incoming_size: int) -> None:
        """
        Drops the oldest spilled batches until there is room for incoming_size bytes.

        Assumes the lock is held. This is a last-resort data-loss path for when a flush
        outage outlasts max_spill_bytes of disk space.

        Args:
            incoming_size (int): Size in bytes of the batch about to be spilled.
        """
        files = sorted(self.spill_dir.glob("*.jsonl.zst"))
        total = sum(f.stat().st_size for f in files) + incoming_size

        while total > self.max_spill_bytes and files:
            oldest = files.pop(0)
            size = oldest.stat().st_size
            oldest.unlink()
            total -= size
            logger.error(
                f"Spill directory exceeded max_spill_bytes ({self.max_spill_bytes}); "
                f"dropped oldest spilled batch {oldest.name} ({size} bytes)."
            )

    async def _drain_spill(self) -> None:
        """
        Attempts to upload any previously spilled batches now that S3 appears reachable.

        Assumes the lock is held. Stops at the first failure rather than retrying every
        remaining file, since a failure likely means S3 is still unreachable.
        """
        for path in sorted(self.spill_dir.glob("*.jsonl.zst")):
            try:
                data = path.read_bytes()
                await self.s3_storage.upload_bytes(data=data, key=path.name)
                path.unlink()
                logger.info(f"Successfully uploaded spilled batch {path.name} from disk.")
            except Exception as e:
                logger.error(f"Failed to upload spilled batch {path.name}, will retry later: {e}")
                break

    async def start_periodic_flush(self) -> None:
        """Background task to ensure periodic flushing and spill retries even if no messages are arriving."""
        while True:
            await asyncio.sleep(self.flush_interval_seconds)
            async with self._lock:
                if self.buffer:
                    time_since_last_flush = (datetime.now() - self.last_flush_time).total_seconds()
                    if time_since_last_flush >= self.flush_interval_seconds:
                        logger.info(f"Periodic flush triggered (time since last flush: {time_since_last_flush:.2f}s).")
                        await self._flush_internal()
                else:
                    await self._drain_spill()
