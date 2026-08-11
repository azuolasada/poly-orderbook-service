import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from src.market_data.market_data_service import MarketDataService

@pytest.mark.asyncio
async def test_get_series_tokens():
    with patch("src.market_data.market_data_service.ApiClient", autospec=True) as mock_api_class:
        mock_api = mock_api_class.return_value
        mock_api.get_series_by_id = AsyncMock(return_value={
            "events": [{"id": "event1"}, {"id": "event2"}]
        })
        mock_api.get_event_by_id = AsyncMock()
        mock_api.get_event_by_id.side_effect = [
            {"markets": [{"sportsMarketType": "moneyline", "clobTokenIds": '["token1", "token2"]'}]},
            {"markets": [{"sportsMarketType": "spread", "clobTokenIds": '["token3"]'}, 
                        {"sportsMarketType": "moneyline", "clobTokenIds": '["token4"]'}]}
        ]
        
        md_service = MarketDataService()
        tokens = await md_service.get_series_tokens(series_id=123, only_moneyline=True)
        
        assert tokens == ["token1", "token2", "token4"]
        assert mock_api.get_series_by_id.call_count == 1
        assert mock_api.get_event_by_id.call_count == 2

@pytest.mark.asyncio
async def test_get_tokens_for_events():
    with patch("src.market_data.market_data_service.ApiClient", autospec=True) as mock_api_class:
        mock_api = mock_api_class.return_value
        mock_api.get_event_by_id = AsyncMock(return_value={
            "markets": [{"sportsMarketType": "moneyline", "clobTokenIds": '["token1"]'}]
        })
        
        md_service = MarketDataService()
        tokens = await md_service.get_tokens_for_events(["event1"])
        assert tokens == ["token1"]
        mock_api.get_event_by_id.assert_called_once_with(event_id="event1")

@pytest.mark.asyncio
async def test_watch_series_updates_subscription():
    with patch("src.market_data.market_data_service.ApiClient", autospec=True) as mock_api_class:
        mock_api = mock_api_class.return_value
        
        # Initial state: event1
        mock_api.get_series_by_id = AsyncMock()
        mock_api.get_series_by_id.side_effect = [
            {"events": [{"id": "event1"}]}, # Initial state check in watch_series
            {"events": [{"id": "event1"}, {"id": "event2"}]}, # First loop iteration
            asyncio.CancelledError() # To stop the loop
        ]
        
        md_service = MarketDataService()
        
        # Mock get_tokens_for_events
        md_service.get_tokens_for_events = AsyncMock()
        md_service.get_tokens_for_events.side_effect = [
            ["token1", "token2"] # When event2 is added
        ]
        
        mock_ws = MagicMock()
        mock_ws._subscribed_assets = ["token0"] # Initial tokens
        mock_ws.subscribe = AsyncMock()
        
        # We need to run watch_series but it has a while True and sleep
        with patch("asyncio.sleep", AsyncMock()):
            try:
                await md_service.watch_series(series_id=123, ws_client=mock_ws, interval_seconds=0.1)
            except asyncio.CancelledError:
                pass
        
        # Verify get_tokens_for_events was called for the new event list
        md_service.get_tokens_for_events.assert_called_with(["event1", "event2"], True)
        # Verify ws_client.subscribe was called
        mock_ws.subscribe.assert_called_with(["token1", "token2"])

@pytest.mark.asyncio
async def test_subscribe_to_series():
    with patch("src.market_data.market_data_service.MarketDataService.get_series_tokens", new_callable=AsyncMock) as mock_get_tokens:
        with patch("src.market_data.market_data_service.WebSocketClient", autospec=True) as mock_ws_class:
            mock_ws = mock_ws_class.return_value
            mock_ws.connect = AsyncMock()
            mock_ws.subscribe = AsyncMock()
            
            mock_get_tokens.return_value = ["token1", "token2"]
            
            md_service = MarketDataService()
            ws_client = await md_service.subscribe_to_series(series_id=123)
            
            assert ws_client == mock_ws
            mock_get_tokens.assert_called_once_with(123, True)
            mock_ws.connect.assert_called_once()
            mock_ws.subscribe.assert_called_once_with(["token1", "token2"])

@pytest.mark.asyncio
async def test_subscribe_to_series_no_tokens():
    with patch("src.market_data.market_data_service.MarketDataService.get_series_tokens", new_callable=AsyncMock) as mock_get_tokens:
        mock_get_tokens.return_value = []
        
        md_service = MarketDataService()
        with pytest.raises(ValueError, match="No tokens found for series_id: 123"):
            await md_service.subscribe_to_series(series_id=123)
