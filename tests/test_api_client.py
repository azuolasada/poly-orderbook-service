import pytest
import httpx
from src.market_data.api_client import ApiClient

@pytest.mark.asyncio
async def test_get_series_by_id(respx_mock):
    api_client = ApiClient()
    series_id = 123
    mock_response = {"id": series_id, "name": "Test Series", "events": []}
    
    respx_mock.get(f"https://gamma-api.polymarket.com/series/{series_id}").mock(
        return_value=httpx.Response(200, json=mock_response)
    )
    
    result = await api_client.get_series_by_id(series_id)
    assert result == mock_response
    await api_client.close()

@pytest.mark.asyncio
async def test_get_event_by_id(respx_mock):
    api_client = ApiClient()
    event_id = 456
    mock_response = {"id": event_id, "name": "Test Event", "markets": []}
    
    respx_mock.get(f"https://gamma-api.polymarket.com/events/{event_id}").mock(
        return_value=httpx.Response(200, json=mock_response)
    )
    
    result = await api_client.get_event_by_id(event_id)
    assert result == mock_response
    await api_client.close()

@pytest.mark.asyncio
async def test_api_client_retry(respx_mock):
    api_client = ApiClient()
    event_id = 789

    route = respx_mock.get(f"https://gamma-api.polymarket.com/events/{event_id}")
    route.side_effect = [
        httpx.Response(500),
        httpx.Response(200, json={"id": event_id})
    ]

    result = await api_client.get_event_by_id(event_id)
    assert result == {"id": event_id}
    assert route.call_count == 2
    await api_client.close()

@pytest.mark.asyncio
async def test_api_client_does_not_retry_on_4xx(respx_mock):
    api_client = ApiClient()
    event_id = 999

    route = respx_mock.get(f"https://gamma-api.polymarket.com/events/{event_id}")
    route.mock(return_value=httpx.Response(404))

    with pytest.raises(httpx.HTTPStatusError):
        await api_client.get_event_by_id(event_id)

    assert route.call_count == 1
    await api_client.close()

@pytest.mark.asyncio
async def test_api_client_retries_on_429(respx_mock):
    api_client = ApiClient()
    event_id = 111

    route = respx_mock.get(f"https://gamma-api.polymarket.com/events/{event_id}")
    route.side_effect = [
        httpx.Response(429),
        httpx.Response(200, json={"id": event_id})
    ]

    result = await api_client.get_event_by_id(event_id)
    assert result == {"id": event_id}
    assert route.call_count == 2
    await api_client.close()
