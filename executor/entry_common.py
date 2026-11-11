"""Shared imports and logging for the entry lifecycle mixins."""

import asyncio
import json
import logging
import math
import time
from typing import Any, Dict, Optional

from executor.models import ActivePosition, OrderRole, PairInfo, PositionState
from executor import helpers
from executor.quantity import AmountSpec
from executor.helpers import BALANCES_KEY, HedgeSlippageExceeded, build_client_order_id
from executor.private_stream import fetch_order_preferring_stream

logger = logging.getLogger("executor.engine")
