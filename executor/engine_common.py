"""Shared dependencies for engine orchestration mixins."""

import asyncio
from contextvars import ContextVar
from decimal import Decimal, ROUND_DOWN
import json
import logging
import math
import time
from typing import Optional, Dict, Any

import redis.asyncio as aioredis
from executor.models import PositionState, ExecutorPayload, ActivePosition, OrderRecord, OrderRole
from executor.exchange_client import ExchangeClient
from executor.fee_engine import FeeRates, round_trip_fees_usdt
from executor.pair_registry import ExchangePairKey, PairClientRegistry
from executor.config import ExecutorConfig
from executor.private_stream import PrivateStreamCache
from executor.quantity import quantize_base_quantity
from executor.helpers import (
    ARB_TRADE_HISTORY_KEY, ARB_TRADE_HISTORY_MAX_LEN, BALANCES_KEY,
    PENDING_ENTRY_KEY_PREFIX, HedgeSlippageExceeded, RetryExhausted,
    build_client_order_id, ceil_to_tick, clean_pair_key, floor_to_tick,
    retry_with_backoff, round_to_tick,
)
from executor import helpers

logger = logging.getLogger("executor.engine")
