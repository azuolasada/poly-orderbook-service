import time
from types import TracebackType
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from src.utils.logger import logger


def _is_retryable(exception: BaseException) -> bool:
    """Retry on connection-level errors, 5xx responses, and 429 (rate limited); not other 4xx client errors."""
    if isinstance(exception, httpx.HTTPStatusError):
        status_code = exception.response.status_code
        return status_code >= 500 or status_code == 429
    return isinstance(exception, httpx.RequestError)


class ApiClient:
    def __init__(self, base_url: str = "https://gamma-api.polymarket.com/") -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)

    async def __aenter__(self) -> ApiClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(_is_retryable),
        reraise=True
    )
    async def _get(self, endpoint: str) -> dict[str, Any]:
        logger.debug(f"Fetching {endpoint}")
        try:
            start = time.perf_counter()
            response = await self._client.get(endpoint)
            elapsed_ms = (time.perf_counter() - start) * 1000

            response.raise_for_status()
            logger.debug(f"GET {endpoint} -> {response.status_code} in {elapsed_ms:.2f}ms")
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred while fetching {endpoint}: {e}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error occurred while fetching {endpoint}: {e}")
            raise
        
        return response.json()

    async def get_series_by_id(self, series_id: int) -> dict[str, Any]:
        return await self._get(f"series/{series_id}")

    async def get_event_by_id(self, event_id: int | str) -> dict[str, Any]:
        return await self._get(f"events/{event_id}")