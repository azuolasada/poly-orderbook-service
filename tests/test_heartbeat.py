import asyncio

import pytest

from src.utils.heartbeat import run_heartbeat


@pytest.mark.asyncio
async def test_heartbeat_touches_file_immediately(tmp_path):
    path = tmp_path / "heartbeat"
    task = asyncio.create_task(run_heartbeat(path, interval_seconds=60))

    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert path.exists()


@pytest.mark.asyncio
async def test_heartbeat_touches_file_periodically(tmp_path):
    path = tmp_path / "heartbeat"
    task = asyncio.create_task(run_heartbeat(path, interval_seconds=0.05))

    await asyncio.sleep(0.02)
    first_mtime = path.stat().st_mtime_ns

    await asyncio.sleep(0.1)
    second_mtime = path.stat().st_mtime_ns

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert second_mtime > first_mtime
