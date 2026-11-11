# Multi-strategy analysis

The analysis package is intentionally independent from order execution and
exchange-specific clients.

```text
CEX adapters -> normalized market snapshots -> scanners
                                      |
                                      v
                            strategy analysis engine
                                      |
                         ranked research signals
                                      |
                           portfolio/risk gate
                                      |
                         execution strategy router
                                      |
                            CEX execution adapter
```

The scanner discovers and validates data. The analysis engine runs independent
strategies against the same normalized snapshot. A strategy emits a research
signal (instrument, venue, side, confidence and risk references); it does not
place orders. A portfolio/risk gate must check exposure, liquidity, fees,
funding/borrow costs, leverage, drawdown, correlation and venue capabilities
before any signal can be routed to an executor.

Candidate strategy families (not yet implemented by this contract):

- Spot momentum / trend following and breakout.
- Spot mean reversion and statistical pairs.
- Long/short perpetual basis and funding strategies.
- Cross-exchange cash-and-carry and price dislocation.
- Triangular spot arbitrage.
- Grid and market-making strategies.
- Options volatility strategies, only when an options-capable venue adapter
  and option-specific risk model exist.

Do not treat these as interchangeable execution modes. Each needs its own
entry/exit model, fees, funding or borrow cost, position sizing, stop logic and
backtest. The existing spot-long/perpetual-short scanner remains a legacy
specialized path until a parity-tested migration is complete.

The engine rejects malformed signals, sorts valid results by confidence and
returns per-strategy failures separately. Callers must not execute results when
the required analysis or risk checks failed.
