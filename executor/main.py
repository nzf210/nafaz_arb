import asyncio
import logging
import signal
import sys
from executor.config import ExecutorConfig
from executor.exchange_client import create_exchange_client, create_ccxt_client
from executor.engine import ArbitrageEngine
from executor.supabase_repository import SupabaseRepository

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("executor.main")


async def main():
    config = ExecutorConfig.load("config.json")
    mode_str = "MOCK / PAPER TRADING" if config.mock_mode else "LIVE REAL TRADING"
    logger.info(
        f"Starting Nafaz Executor [{mode_str}] with Local={config.local_exchange_name}, "
        f"Global={config.global_exchange_name}, Redis={config.redis_url}"
    )

    client = create_exchange_client(config)
    engine = ArbitrageEngine(config, client)
    if config.supabase.enabled:
        try:
            engine.supabase_repository = SupabaseRepository.from_config(config.supabase)
            logger.info("Supabase operational persistence enabled")
        except Exception as exc:
            # Database persistence is not allowed to block hedge, rollback or close.
            logger.warning("Supabase persistence unavailable; Redis remains active: %s", exc)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    await engine.start()
    await stop_event.wait()
    await engine.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Executor process exited")
        raise
