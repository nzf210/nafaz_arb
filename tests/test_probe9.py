import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine


def test_probe9(mock_client):
    cfg = ExecutorConfig()
    eng = ArbitrageEngine(config=cfg, client=mock_client, redis_client=None)
    assert eng.config is cfg
