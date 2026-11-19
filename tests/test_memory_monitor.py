import asyncio
from unittest.mock import MagicMock, mock_open, patch

import pytest

from src.utils import memory_monitor
from src.utils.memory_monitor import _read_rss_bytes, monitor_memory


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rss_bytes", "expected_message"),
    [
        (50 * 1024 * 1024, "Memory usage: 50.0 MB"),
        (1_234_567, "Memory usage: 1.18 MB"),  # 1.1773... MB, rounded to 2 decimals
    ],
)
async def test_monitor_memory_logs_rss_in_mb(rss_bytes, expected_message):
    mock_logger = MagicMock()
    with (
        patch.object(memory_monitor, "_read_rss_bytes", return_value=rss_bytes),
        patch.object(memory_monitor, "logger", mock_logger),
    ):
        task = asyncio.create_task(monitor_memory(interval_seconds=60))
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    mock_logger.info.assert_called_once_with(expected_message)


def test_read_rss_bytes_uses_resident_field_times_page_size():
    # statm fields: size resident shared text lib data dt (in pages)
    statm = "1000 250 30 5 0 400 0\n"
    with (
        patch.object(memory_monitor, "open", mock_open(read_data=statm), create=True) as mocked_open,
        patch.object(memory_monitor, "_PAGE_SIZE", 4096),
    ):
        result = _read_rss_bytes()

    mocked_open.assert_called_once_with("/proc/self/statm")
    assert result == 250 * 4096
