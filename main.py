import asyncio
import signal
import sys

from src.config import settings
from src.market_data.market_data_service import MarketDataService
from src.market_data.message_buffer import MessageBuffer
from src.utils.logger import logger


async def main() -> None:
    md_service = MarketDataService()
    series_id = settings.POLYMARKET_SERIES_ID

    # Initialize message buffer
    buffer = MessageBuffer(flush_interval_seconds=300, flush_count_threshold=10_000)

    # Start periodic flush task
    flush_task = asyncio.create_task(buffer.start_periodic_flush())

    # Handle SIGTERM (docker stop) and SIGINT (Ctrl+C)
    main_task = asyncio.current_task()
    assert main_task is not None
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, main_task.cancel)

    try:
        ws_client = await md_service.subscribe_to_series(series_id=series_id)
    except ValueError as e:
        logger.error(f"Error: {e}")
        flush_task.cancel()
        await asyncio.gather(flush_task, return_exceptions=True)
        sys.exit(1)

    # Start watcher for series updates
    watcher_task = asyncio.create_task(md_service.watch_series(series_id, ws_client, message_buffer=buffer))

    logger.info("Connected and subscribed. Listening for events...")
    
    try:
        async for data in ws_client.listen():
            events = data if isinstance(data, list) else [data]
            for event in events:
                # Add message to buffer
                await buffer.add_message(event)

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}")
        sys.exit(1)
    finally:
        logger.info("Shutting down... flushing remaining messages.")
        watcher_task.cancel()
        await asyncio.gather(watcher_task, return_exceptions=True)
        await buffer.flush()
        flush_task.cancel()
        await asyncio.gather(flush_task, return_exceptions=True)
        await ws_client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())