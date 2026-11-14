import asyncio
import logging
import os
import signal
import sys

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine
from executor.exchange_client import create_exchange_client
from executor.v2_control_runtime import V2ControlPlaneRuntime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("executor.main")


def _v2_control_enabled() -> bool:
    return str(os.environ.get("NAFAZ_V2_CONTROL_ENABLED", "")).strip().lower() in {
        "1", "true", "yes", "on"
    }


async def main():
    config = ExecutorConfig.load("config.json")
    v2_control = None
    if _v2_control_enabled():
        try:
            v2_control = await V2ControlPlaneRuntime.create(config)
        except Exception as exc:
            # Process liveness is independent from trading authorization. Keep
            # monitoring/recovery alive, but the missing V2 gate blocks LIVE
            # entries and no legacy fallback is permitted.
            logger.error(
                "V2 control-plane bootstrap failed; LIVE entries remain blocked error_type=%s",
                type(exc).__name__,
            )

    mode_str = "MOCK / PAPER TRADING" if config.mock_mode else "LIVE (V2 entry gate required)"
    logger.info(
        "Starting Nafaz Executor [%s] with Local=%s, Global=%s, Redis=%s, V2Control=%s",
        mode_str,
        config.local_exchange_name,
        config.global_exchange_name,
        config.redis_url,
        "enabled" if v2_control else "unavailable",
    )

    client = create_exchange_client(config)
    engine = ArbitrageEngine(config, client)
    if v2_control is not None:
        engine.v2_admission_gate = v2_control.admission_gate

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

    try:
        # Start the V2 lease/invalidation heartbeat before the engine can drain
        # its durable signal queue. The durable operator state is STOPPED.
        if v2_control is not None:
            await v2_control.start()
        # engine.start() completes startup recovery before signal processing.
        await engine.start()
        await stop_event.wait()
    finally:
        await engine.stop()
        if v2_control is not None:
            await v2_control.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Executor process exited")
        raise
