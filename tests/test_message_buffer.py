import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import zstandard as zstd
from botocore.exceptions import ClientError

from src.market_data.message_buffer import MessageBuffer
from src.s3_storage import S3Storage


@pytest.fixture
def mock_s3():
    with patch("src.market_data.message_buffer.S3Storage") as mock:
        instance = mock.return_value
        instance.upload_bytes = AsyncMock()
        instance.ensure_bucket_exists = MagicMock()
        yield instance


@pytest.mark.asyncio
async def test_buffer_count_threshold(mock_s3):
    buffer = MessageBuffer(flush_count_threshold=5)
    
    for i in range(4):
        await buffer.add_message({"id": i})
    
    # Should not have flushed yet
    assert len(buffer.buffer) == 4
    assert mock_s3.upload_bytes.call_count == 0
    
    # Add 5th message
    await buffer.add_message({"id": 4})
    
    # Should have flushed
    assert len(buffer.buffer) == 0
    assert mock_s3.upload_bytes.call_count == 1

    # Verify content of upload
    args, kwargs = mock_s3.upload_bytes.call_args
    compressed_data = kwargs.get('data') or args[0]
    
    dctx = zstd.ZstdDecompressor()
    content = dctx.decompress(compressed_data).decode('utf-8')
    lines = content.splitlines()

    assert len(lines) == 5
    assert json.loads(lines[0]) == {"id": 0}

@pytest.mark.asyncio
async def test_buffer_manual_flush(mock_s3):
    buffer = MessageBuffer(flush_count_threshold=100)
    await buffer.add_message({"id": 1})
    
    assert len(buffer.buffer) == 1
    await buffer.flush()
    assert len(buffer.buffer) == 0
    assert mock_s3.upload_bytes.call_count == 1

@pytest.mark.asyncio
async def test_buffer_time_threshold(mock_s3):
    # Set a very short interval for testing
    buffer = MessageBuffer(flush_interval_seconds=1, flush_count_threshold=100)
    
    await buffer.add_message({"id": 1})
    assert len(buffer.buffer) == 1

    # Wait for interval to pass
    await asyncio.sleep(1.1)
    
    # Next message should trigger flush due to time
    await buffer.add_message({"id": 2})
    
    assert len(buffer.buffer) == 0
    assert mock_s3.upload_bytes.call_count == 1


@pytest.mark.asyncio
async def test_s3_upload_retries():
    # Mock boto3 client
    mock_client = MagicMock()

    # Simulate 2 failures then 1 success
    error_response = {'Error': {'Code': '500', 'Message': 'Internal Server Error'}}
    mock_client.put_object.side_effect = [
        ClientError(error_response, 'PutObject'),
        ClientError(error_response, 'PutObject'),
        {'ResponseMetadata': {'HTTPStatusCode': 200}}
    ]

    with patch("boto3.client", return_value=mock_client):
        # We need to speed up the retry wait time for tests
        with patch("src.s3_storage.wait_exponential", return_value=lambda x: 0.1):
            storage = S3Storage(bucket="test-bucket")
            await storage.upload_bytes(b"data", "key")

            assert mock_client.put_object.call_count == 3