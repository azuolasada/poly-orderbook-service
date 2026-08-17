import json
import asyncio
from types import TracebackType
from typing import Any, AsyncIterator
from websockets.asyncio.client import connect, ClientConnection
from websockets.exceptions import ConnectionClosed, WebSocketException
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.utils.logger import logger

class WebSocketClient:
    def __init__(self, url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market") -> None:
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
        logger.info(f"Connecting to WebSocket at {self.url}")
        self.connection = await connect(self.url, ping_interval=20)
        logger.info("Successfully connected to WebSocket")
        return self.connection

    async def connect(self) -> ClientConnection:
        async with self._connection_lock:
            return await self._connect_locked()

    async def disconnect(self) -> None:
        async with self._connection_lock:
            if self.connection:
                logger.info("Disconnecting from WebSocket")
                await self.connection.close()
                self.connection = None

    async def subscribe(self, asset_ids: list[str]) -> None:
        async with self._connection_lock:
            await self._subscribe_locked(asset_ids)

    async def _subscribe_locked(self, asset_ids: list[str]) -> None:
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
        while True:
            if not self.connection:
                logger.warning("WebSocket connection is missing. Attempting to connect...")
                async with self._connection_lock:
                    if not self.connection:
                        await self._connect_locked()
                        if self._subscribed_assets:
                            await self._subscribe_locked(self._subscribed_assets)

            try:
                async for message in self.connection:
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

    async def __aenter__(self) -> "WebSocketClient":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.disconnect()

    @property
    def subscribed_assets(self) -> list[str]:
        return self._subscribed_assets