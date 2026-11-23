from decimal import Decimal

import pytest

from executor.hedge_attempt_ledger import (
    _ACTIVE,
    _client_order_id,
    _decimal,
)


def test_attempt_client_order_ids_are_stable_and_distinct_per_attempt():
    first = _client_order_id("indodax:spot-123:100000000", 1)
    replay = _client_order_id("indodax:spot-123:100000000", 1)
    second = _client_order_id("indodax:spot-123:100000000", 2)

    assert first == replay
    assert first != second
    assert first.startswith("hda")
    assert len(first) <= 32


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1", "0", None, "not-a-number"])
def test_attempt_quantities_reject_non_positive_or_non_finite_values(value):
    with pytest.raises(ValueError):
        _decimal(value, "quantity")


def test_attempt_fill_fee_allows_zero_but_quantity_does_not():
    assert _decimal("0", "fee", allow_zero=True) == Decimal("0")
    assert _decimal("0.001", "quantity") == Decimal("0.001")


def test_unresolved_attempt_states_block_a_new_attempt():
    assert {
        "PREPARED", "SUBMITTING", "UNKNOWN", "ACKNOWLEDGED", "OPEN",
        "PARTIALLY_FILLED", "RECONCILIATION_REQUIRED",
    } == set(_ACTIVE)
    assert "CANCELED" not in _ACTIVE
    assert "REJECTED" not in _ACTIVE
