"""Module for the Polymarket CLOB WebSocket client with auto-reconnect."""

import asyncio
import json
from collections.abc import AsyncIterator
from types import TracebackType
from typing import Any

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from src.utils.logger import logger


class WebSocketClient:
    """
    An auto-reconnecting client for the Polymarket CLOB market-data WebSocket.

    Attributes:
        url (str): The WebSocket endpoint URL.
        connection (ClientConnection | None): The active connection, if any.
    """

    def __init__(self, url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market") -> None:
        """
        Initializes the WebSocketClient.

        Args:
            url (str, optional): The WebSocket endpoint URL.
        """
        self.url = url
        self.connection: ClientConnection | None = None
        self._subscribed_assets: list[str] = []
        self._connection_lock = asyncio.Lock()

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((WebSocketException, ConnectionRefusedError, OSError)),
        reraise=True
    )
    async def _connect_locked(self) -> ClientConnection:
        """Connects to the WebSocket, retrying with backoff. Assumes the connection lock is held."""
        logger.info(f"Connecting to WebSocket at {self.url}")
        self.connection = await connect(self.url, ping_interval=20)
        logger.info("Successfully connected to WebSocket")
        return self.connection

    async def connect(self) -> ClientConnection:
        """Connects to the WebSocket, retrying with backoff.

        Returns:
            ClientConnection: The established connection.
        """
        async with self._connection_lock:
            return await self._connect_locked()

    async def disconnect(self) -> None:
        """Closes the WebSocket connection, if one is open."""
        async with self._connection_lock:
            if self.connection:
                logger.info("Disconnecting from WebSocket")
                await self.connection.close()
                self.connection = None

    async def subscribe(self, asset_ids: list[str]) -> None:
        """
        Subscribes to market data for the given asset IDs, replacing any prior subscription.

        Args:
            asset_ids (list[str]): The CLOB token/asset IDs to subscribe to.
        """
        async with self._connection_lock:
            await self._subscribe_locked(asset_ids)

    async def _subscribe_locked(self, asset_ids: list[str]) -> None:
        """Sends the subscription message. Assumes the connection lock is held.

        Args:
            asset_ids (list[str]): The CLOB token/asset IDs to subscribe to.

        Raises:
            RuntimeError: If called before connect().
        """
        if not self.connection:
            raise RuntimeError("WebSocket is not connected. Call connect() first.")

        logger.info(f"Subscribing to assets: {asset_ids}")
        self._subscribed_assets = asset_ids
        subscription_msg = {
            "assets_ids": asset_ids,
            "type": "market",
        }
        await self.connection.send(json.dumps(subscription_msg))

    async def listen(self) -> AsyncIterator[Any]:
        """
        Yields decoded messages from the WebSocket, reconnecting and re-subscribing on drops.

        Yields:
            Any: The JSON-decoded message payload.
        """
        while True:
            if not self.connection:
                logger.warning("WebSocket connection is missing. Attempting to connect...")
                async with self._connection_lock:
                    if not self.connection:
                        await self._connect_locked()
                        if self._subscribed_assets:
                            await self._subscribe_locked(self._subscribed_assets)

            connection = self.connection
            if connection is None:
                continue

            try:
                async for message in connection:
                    try:
                        data = json.loads(message)
                        yield data
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode WebSocket message: {e}")
            except (ConnectionClosed, WebSocketException) as e:
                logger.warning(f"WebSocket connection closed/error: {e}. Reconnecting...")
                async with self._connection_lock:
                    self.connection = None
                await asyncio.sleep(1)  # Brief pause before reconnecting
            except Exception as e:
                logger.error(f"Unexpected error in WebSocket listen loop: {e}")
                async with self._connection_lock:
                    self.connection = None
                raise

    async def __aenter__(self) -> WebSocketClient:
        """Enters the async context manager, connecting and returning this client."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exits the async context manager, disconnecting the WebSocket."""
        await self.disconnect()

    @property
    def subscribed_assets(self) -> list[str]:
        """list[str]: The asset IDs currently subscribed to."""
        return self._subscribed_assets