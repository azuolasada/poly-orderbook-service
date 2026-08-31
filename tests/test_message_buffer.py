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
        instance.bucket = "test-bucket"
        yield instance


@pytest.mark.asyncio
async def test_buffer_count_threshold(mock_s3, tmp_path):
    buffer = MessageBuffer(flush_count_threshold=5, spill_dir=str(tmp_path))

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
async def test_buffer_manual_flush(mock_s3, tmp_path):
    buffer = MessageBuffer(flush_count_threshold=100, spill_dir=str(tmp_path))
    await buffer.add_message({"id": 1})

    assert len(buffer.buffer) == 1
    await buffer.flush()
    assert len(buffer.buffer) == 0
    assert mock_s3.upload_bytes.call_count == 1

@pytest.mark.asyncio
async def test_buffer_time_threshold(mock_s3, tmp_path):
    # Set a very short interval for testing
    buffer = MessageBuffer(flush_interval_seconds=1, flush_count_threshold=100, spill_dir=str(tmp_path))

    await buffer.add_message({"id": 1})
    assert len(buffer.buffer) == 1

    # Wait for interval to pass
    await asyncio.sleep(1.1)

    # Next message should trigger flush due to time
    await buffer.add_message({"id": 2})

    assert len(buffer.buffer) == 0
    assert mock_s3.upload_bytes.call_count == 1


@pytest.mark.asyncio
async def test_buffer_spills_to_disk_on_flush_failure(mock_s3, tmp_path):
    mock_s3.upload_bytes.side_effect = Exception("S3 unreachable")
    buffer = MessageBuffer(flush_count_threshold=1, spill_dir=str(tmp_path))

    await buffer.add_message({"id": 1})

    # Buffer is cleared (moved to disk), not held in memory
    assert len(buffer.buffer) == 0
    spilled = list(tmp_path.glob("*.jsonl.zst"))
    assert len(spilled) == 1

    dctx = zstd.ZstdDecompressor()
    content = dctx.decompress(spilled[0].read_bytes()).decode("utf-8")
    assert json.loads(content.strip()) == {"id": 1}


@pytest.mark.asyncio
async def test_buffer_drains_spill_once_s3_recovers(mock_s3, tmp_path):
    mock_s3.upload_bytes.side_effect = Exception("S3 unreachable")
    buffer = MessageBuffer(flush_count_threshold=1, spill_dir=str(tmp_path))

    await buffer.add_message({"id": 1})
    assert len(list(tmp_path.glob("*.jsonl.zst"))) == 1

    # S3 recovers
    mock_s3.upload_bytes.side_effect = None
    mock_s3.upload_bytes.reset_mock()

    await buffer.add_message({"id": 2})

    # New message flushed, and the previously spilled batch was drained too
    assert len(list(tmp_path.glob("*.jsonl.zst"))) == 0
    assert mock_s3.upload_bytes.call_count == 2


@pytest.mark.asyncio
async def test_spill_cap_drops_oldest_batch(mock_s3, tmp_path):
    mock_s3.upload_bytes.side_effect = Exception("S3 unreachable")
    # Cap small enough that only one ~tiny batch fits at a time
    buffer = MessageBuffer(flush_count_threshold=1, spill_dir=str(tmp_path), max_spill_bytes=1)

    await buffer.add_message({"id": 1})
    first_spill = list(tmp_path.glob("*.jsonl.zst"))
    assert len(first_spill) == 1

    await buffer.add_message({"id": 2})
    second_spill = list(tmp_path.glob("*.jsonl.zst"))

    # The oldest spilled batch was dropped to make room for the newest one
    assert len(second_spill) == 1
    assert second_spill != first_spill


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
