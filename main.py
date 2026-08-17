import asyncio
import signal
from src.market_data.market_data_service import MarketDataService
from src.market_data.message_buffer import MessageBuffer


async def main() -> None:
    md_service = MarketDataService()
    series_id = 10365 # ATP tennis

    # Initialize message buffer
    buffer = MessageBuffer(flush_interval_seconds=300, flush_count_threshold=10_000)

    # Start periodic flush task
    flush_task = asyncio.create_task(buffer.start_periodic_flush())

    # Handle SIGTERM (docker stop) and SIGINT (Ctrl+C)
    main_task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, main_task.cancel)

    try:
        ws_client = await md_service.subscribe_to_series(series_id=series_id)
    except ValueError as e:
        print(f"Error: {e}")
        flush_task.cancel()
        await asyncio.gather(flush_task, return_exceptions=True)
        return

    # Start watcher for series updates
    watcher_task = asyncio.create_task(md_service.watch_series(series_id, ws_client, message_buffer=buffer))

    print("Connected and subscribed. Listening for events...")
    
    try:
        async for data in ws_client.listen():
            events = data if isinstance(data, list) else [data]
            for event in events:
                # Add message to buffer
                await buffer.add_message(event)

    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"Unexpected error in main loop: {e}")
    finally:
        print("Shutting down... flushing remaining messages.")
        watcher_task.cancel()
        await asyncio.gather(watcher_task, return_exceptions=True)
        await buffer.flush()
        flush_task.cancel()
        await asyncio.gather(flush_task, return_exceptions=True)
        await ws_client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())