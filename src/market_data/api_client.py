"""Module for the Polymarket Gamma REST API client."""

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
    """
    An async client for the Polymarket Gamma REST API.

    Attributes:
        base_url (str): The base URL of the Gamma API.
    """

    def __init__(self, base_url: str = "https://gamma-api.polymarket.com/") -> None:
        """
        Initializes the ApiClient with a base URL and underlying HTTP client.

        Args:
            base_url (str, optional): The base URL of the Gamma API.
        """
        self.base_url = base_url.rstrip("/") + "/"
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)

    async def __aenter__(self) -> ApiClient:
        """Enters the async context manager, returning this client."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exits the async context manager, closing the underlying HTTP client."""
        await self.close()

    async def close(self) -> None:
        """Closes the underlying HTTP client."""
        await self._client.aclose()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(_is_retryable),
        reraise=True
    )
    async def _get(self, endpoint: str) -> dict[str, Any]:
        """
        Performs a GET request against the Gamma API, retrying on transient errors.

        Args:
            endpoint (str): The API endpoint to request, relative to the base URL.

        Returns:
            dict[str, Any]: The parsed JSON response body.

        Raises:
            httpx.HTTPStatusError: If the response has a non-2xx status code.
            httpx.RequestError: If a connection-level error occurs.
        """
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
        """
        Fetches a series by its ID.

        Args:
            series_id (int): The Polymarket series ID.

        Returns:
            dict[str, Any]: The series data, including its events.
        """
        return await self._get(f"series/{series_id}")

    async def get_event_by_id(self, event_id: int | str) -> dict[str, Any]:
        """
        Fetches an event by its ID.

        Args:
            event_id (int | str): The Polymarket event ID.

        Returns:
            dict[str, Any]: The event data, including its markets.
        """
        return await self._get(f"events/{event_id}")