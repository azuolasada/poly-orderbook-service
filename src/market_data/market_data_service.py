import asyncio
import json

from src.market_data.api_client import ApiClient
from src.market_data.message_buffer import MessageBuffer
from src.market_data.web_socket_client import WebSocketClient
from src.utils.logger import logger

class MarketDataService:
    def __init__(self) -> None:
        self.api_client = ApiClient()
        logger.info("Initializing MarketDataService")

    async def get_tokens_for_events(self, event_ids: list[str], only_moneyline: bool = True) -> list[str]:
        """Fetches tokens for a list of event IDs."""
        if not event_ids:
            return []

        tasks = [self.api_client.get_event_by_id(event_id=eid) for eid in event_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        club_tokens = []
        for res in results:
            if isinstance(res, Exception):
                logger.warning(f"Failed to fetch an event: {res}")
                continue

            for market in res.get("markets", []):
                if only_moneyline and market.get("sportsMarketType") != "moneyline":
                    continue

                token_ids = market.get("clobTokenIds", "[]")
                if isinstance(token_ids, str):
                    try:
                        token_ids = json.loads(token_ids)
                    except json.JSONDecodeError:
                        logger.error(f"Failed to decode clobTokenIds: {token_ids}")
                        continue

                club_tokens.extend(token_ids)
        return club_tokens

    async def get_series_tokens(self,
                               series_id: int,
                               only_moneyline: bool = True) -> list[str]:
        series = await self.api_client.get_series_by_id(series_id=series_id)
        event_ids = [e["id"] for e in series.get("events", [])]
        return await self.get_tokens_for_events(event_ids, only_moneyline)

    async def subscribe_to_series(self, series_id: int, only_moneyline: bool = True) -> WebSocketClient:
        """
        Subscribes to websocket based on series_id
        """
        tokens = await self.get_series_tokens(series_id, only_moneyline)
        if not tokens:
            logger.warning(f"No tokens found for series_id: {series_id}")
            raise ValueError(f"No tokens found for series_id: {series_id}")

        ws_client = WebSocketClient()
        await ws_client.connect()
        await ws_client.subscribe(tokens)
        return ws_client

    async def watch_series(self,
                           series_id: int,
                           ws_client: WebSocketClient,
                           message_buffer: MessageBuffer | None = None,
                           interval_seconds: int = 300,
                           only_moneyline: bool = True) -> None:
        """
        Background task to monitor changes in events for a series and update subscriptions.
        """
        logger.info(f"Starting watcher for series_id: {series_id}")
        known_event_ids = set()

        try:
            series = await self.api_client.get_series_by_id(series_id)
            known_event_ids = {e["id"] for e in series.get("events", [])}
            if message_buffer:
                await message_buffer.add_message({"type": "series_info", "data": series})
        except Exception as e:
            logger.error(f"Error fetching initial series state: {e}")

        while True:
            await asyncio.sleep(interval_seconds)
            try:
                series = await self.api_client.get_series_by_id(series_id)
                current_events = series.get("events", [])
                current_event_ids = {e["id"] for e in current_events}

                if current_event_ids != known_event_ids:
                    new_tokens = await self.get_tokens_for_events(list(current_event_ids), only_moneyline)
                    logger.info(f"Detected change in events for series {series_id}. New token count: {len(new_tokens)} Updating subscriptions.")
                    await ws_client.subscribe(new_tokens)
                    
                    if message_buffer:
                        await message_buffer.add_message({"type": "series_update", "data": series})

                    known_event_ids = current_event_ids
                else:
                    logger.debug(f"No changes detected for series {series_id}")

            except asyncio.CancelledError:
                logger.info(f"Watcher for series {series_id} cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in watch_series loop: {e}")