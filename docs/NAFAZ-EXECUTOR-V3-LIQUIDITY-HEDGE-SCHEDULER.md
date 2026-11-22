# NAFAZ Executor V3 — Liquidity Evaluator and Bounded Hedge Scheduler

Status: PR-06 implementation, layered on durable obligations/reservations.
Scope: pure liquidity authorization plus durable scheduling evidence; no exchange order mutation.

## Market-data contract

The evaluator accepts a typed immutable order-book snapshot with:
- bid/ask levels in native venue order units
- exchange observation timestamp
- sequence validity and checksum validity
- quote-to-USD FX rate, observation timestamp, and source

The evaluator does not repair gaps, infer missing levels, or silently substitute stale cache data. Invalid sequence, checksum, crossed/empty book, stale book, future timestamp, or stale FX never authorizes a submit.

## Decimal execution economics

All prices, quantities, VWAP, notional, FX, fees, and slippage use Decimal. Native contract quantity is converted to base exposure through the pinned `base_per_unit`. Quantity is rounded down to the venue step; the evaluator never rounds a hedge upward to meet a minimum.

For SHORT entry, executable depth is swept from bids; for LONG entry, from asks. It calculates:
- executable native and base-equivalent quantity
- executable VWAP and quote/USD notional
- slippage versus the best executable price
- estimated taker fee plus safety buffer
- all-in execution cost ceiling
- internal USD minimum and venue quote-notional minimum

A full depth sweep returns SUBMIT. A valid executable portion above minimum returns PARTIAL_SUBMIT. Insufficient depth, stale market data, FX, adverse movement, minimum notional, or cost-limit failure returns DEFER.

## Bounded wait and de-risk

Every obligation has durable next-evaluation time, first defer time, max-wait deadline, last book sequence/time, defer count, and de-risk flag.

- If the unhedged notional exceeds the configured risk ceiling, the evaluator returns DE_RISK_REQUIRED immediately.
- If book/FX/cost/depth remains unsuitable, it returns DEFER with a bounded next-evaluation timestamp.
- If the maximum unhedged wait expires, it returns DE_RISK_REQUIRED.
- DE_RISK_REQUIRED blocks further hedge reservations until an explicit recovery/de-risk workflow resolves the obligation.

The scheduler stores every decision in an append-only event table and updates the obligation under the session coordinator lease/fencing generation. Duplicate event keys are idempotent; changed decisions under the same key are conflicts.

## Safety boundary

The scheduler does not submit, cancel, or replace orders. SUBMIT/PARTIAL_SUBMIT means only that current book evidence satisfies the configured gate; a later stage must create a durable order intent and pass the global mutation gateway. The reservation service remains the sole authority for preventing duplicate hedge quantity.

No scanner/V2 runtime changes, no live exchange calls, no production migration, and no VPS deployment.
