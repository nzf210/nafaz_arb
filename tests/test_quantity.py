import pytest

from executor.quantity import (
    AmountSpec,
    base_to_order_amount,
    order_amount_to_base,
    quantize_base_quantity,
)


def test_base_quantity_is_quantized_to_common_spot_and_contract_increment():
    spot = AmountSpec("BASE", 0.0001, 0.0001, 1.0)
    futures = AmountSpec("CONTRACT", 1.0, 1.0, 0.001)

    quantity = quantize_base_quantity(0.0049, spot, futures)

    assert quantity == pytest.approx(0.004)


def test_base_quantity_obeys_contract_market_minimum():
    spot = AmountSpec("BASE", 0.0001, 0.0001, 1.0)
    futures = AmountSpec("CONTRACT", 1.0, 2.0, 0.001)

    with pytest.raises(ValueError, match="below CONTRACT minimum"):
        quantize_base_quantity(0.0019, spot, futures)


def test_contract_amount_conversion_preserves_base_quantity_contract():
    futures = AmountSpec("CONTRACT", 1.0, 1.0, 0.001)

    contracts = base_to_order_amount(0.003, futures)

    assert contracts == 3
    assert order_amount_to_base(contracts, futures) == pytest.approx(0.003)


def test_amount_spec_rejects_non_base_spot_multiplier():
    with pytest.raises(ValueError, match="invalid amount spec"):
        AmountSpec("BASE", 0.01, 0.01, 0.1).validate()
