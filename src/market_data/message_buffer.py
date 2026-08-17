import asyncio
import json
from datetime import datetime
from typing import Any

import zstandard as zstd

from src.s3_storage import S3Storage
from src.utils.logger import logger


class MessageBuffer:
    def __init__(self,
                 flush_interval_seconds: int = 60,
                 flush_count_threshold: int = 100) -> None:
        self.flush_interval_seconds = flush_interval_seconds
        self.flush_count_threshold = flush_count_threshold
        
        self.buffer: list[Any] = []
        self.last_flush_time = datetime.now()
        self._lock = asyncio.Lock()

        self.s3_storage = S3Storage()
        self.s3_storage.ensure_bucket_exists()

        self._compressor = zstd.ZstdCompressor(level=3)

    async def add_message(self, message: Any) -> None:
        """Adds a message to the buffer and checks if flush is needed."""
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
        """Public method to manually trigger a flush."""
        async with self._lock:
            if self.buffer:
                await self._flush_internal()

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
        except Exception as e:
            logger.error(f"Failed to flush messages: {e}")

    async def start_periodic_flush(self) -> None:
        """Background task to ensure periodic flushing even if no messages are arriving."""
        while True:
            await asyncio.sleep(self.flush_interval_seconds)
            async with self._lock:
                if self.buffer:
                    time_since_last_flush = (datetime.now() - self.last_flush_time).total_seconds()
                    if time_since_last_flush >= self.flush_interval_seconds:
                        logger.info(f"Periodic flush triggered (time since last flush: {time_since_last_flush:.2f}s).")
                        await self._flush_internal()
