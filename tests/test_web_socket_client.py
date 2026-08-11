import asyncio
import pytest
import json
from unittest.mock import AsyncMock, patch
from websockets.exceptions import ConnectionClosed

from src.market_data.web_socket_client import WebSocketClient

@pytest.mark.asyncio
async def test_ws_connect_disconnect():
    with patch("src.market_data.web_socket_client.connect", new_callable=AsyncMock) as mock_connect:
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn
        
        ws_client = WebSocketClient()
        await ws_client.connect()
        
        assert ws_client.connection == mock_conn
        mock_connect.assert_called_once_with(ws_client.url, ping_interval=20)
        
        await ws_client.disconnect()
        assert ws_client.connection is None
        mock_conn.close.assert_called_once()

@pytest.mark.asyncio
async def test_ws_subscribe():
    with patch("src.market_data.web_socket_client.connect", new_callable=AsyncMock) as mock_connect:
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn
        
        ws_client = WebSocketClient()
        await ws_client.connect()
        
        asset_ids = ["token1", "token2"]
        await ws_client.subscribe(asset_ids)
        
        expected_msg = json.dumps({
            "assets_ids": asset_ids,
            "type": "market",
        })
        mock_conn.send.assert_called_once_with(expected_msg)

@pytest.mark.asyncio
async def test_ws_listen():
    with patch("src.market_data.web_socket_client.connect", new_callable=AsyncMock) as mock_connect:
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn
        
        messages = [
            json.dumps({"event_type": "book", "asset_id": "token1"}),
            json.dumps({"event_type": "price", "asset_id": "token2"})
        ]
        
        # mock_conn is an AsyncMock, but for __aiter__ we need something that returns an async iterator
        mock_conn.__aiter__.return_value = messages
        
        ws_client = WebSocketClient()
        await ws_client.connect()
        
        received = []
        async for msg in ws_client.listen():
            received.append(msg)
            if len(received) == 2:
                break
                
        assert len(received) == 2
        assert received[0]["event_type"] == "book"
        assert received[1]["event_type"] == "price"


@pytest.mark.asyncio
async def test_ws_reconnection_on_close():
    with patch("src.market_data.web_socket_client.connect", new_callable=AsyncMock) as mock_connect:
        # First connection
        class MockConn:
            def __init__(self, messages):
                self.messages = messages
                self.send = AsyncMock()
                self.close = AsyncMock()

            async def __aiter__(self):
                for m in self.messages:
                    yield m
                raise ConnectionClosed(None, None)

        mock_conn1 = MockConn([json.dumps({"msg": "1"})])

        # Second connection
        class MockConn2:
            def __init__(self, messages):
                self.messages = messages
                self.send = AsyncMock()
                self.close = AsyncMock()

            async def __aiter__(self):
                for m in self.messages:
                    yield m
                # keep it open
                while True:
                    await asyncio.sleep(0.1)

        mock_conn2 = MockConn2([json.dumps({"msg": "2"})])

        mock_connect.side_effect = [mock_conn1, mock_conn2]

        ws_client = WebSocketClient()
        ws_client._subscribed_assets = ["asset1"]

        received = []
        gen = ws_client.listen()

        # Get first message
        msg1 = await anext(gen)
        received.append(msg1)

        # Get second message (should trigger reconnection)
        msg2 = await anext(gen)
        received.append(msg2)

        assert len(received) == 2
        assert received[0]["msg"] == "1"
        assert received[1]["msg"] == "2"

        assert mock_connect.call_count == 2
        assert mock_conn2.send.called