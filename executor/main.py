import asyncio
import logging
import signal
import sys
from executor.config import ExecutorConfig
from executor.exchange_client import create_exchange_client, create_ccxt_client
from executor.engine import ArbitrageEngine
from executor.exposure_reconciliation import ExposureReconciliationWorker

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
    reconciliation_task = None
    if not config.mock_mode:
        worker = ExposureReconciliationWorker(engine)
        reconciliation_task = asyncio.create_task(worker.run())
        logger.info(
            "Read-only exchange reconciliation enabled "
            "(open_orders_interval=%ss, balance_position_interval=%ss)",
            worker.open_orders_interval,
            worker.interval,
        )
    try:
        await stop_event.wait()
    finally:
        if reconciliation_task is not None:
            reconciliation_task.cancel()
            await asyncio.gather(reconciliation_task, return_exceptions=True)
        await engine.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Executor process exited")
        raise
