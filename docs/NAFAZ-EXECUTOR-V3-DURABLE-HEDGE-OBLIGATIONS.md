# NAFAZ Executor V3 — Durable Hedge Obligations

Status: PR-05 implementation, built on PR-04 durable fills.
Scope: one execution session at a time; no exchange mutation calls.

## Core invariant

Every confirmed Spot entry fill creates one durable hedge obligation. The obligation target is:

`spot_fill_native_quantity × spot_leg.base_per_unit × immutable_hedge_ratio`

The target is expressed in base-equivalent quantity. Futures contracts are never compared directly with Spot base quantity; conversion uses the paired Futures leg's pinned `base_per_unit`.

The default hedge ratio is 1.0. Ratios above 1.0 are rejected by this service unless a separately approved over-hedge policy is implemented. Replaying an existing source fill also compares the complete Spot/Futures leg, account, pair route, venue, market, native symbol, contract, quantity-unit, and conversion identity.

## Durable states and quantities

Each obligation tracks:
- target base quantity
- confirmed Futures base quantity
- reserved in-flight base quantity
- remaining unhedged base quantity (= target − confirmed)

Reserved quantity is not treated as confirmed coverage. Multiple partial Spot fills create independent obligations, which may be reserved together within the same session when the full Futures route/contract identity matches.

## Reservation contract

A reservation is created transactionally by locking compatible OPEN/PARTIALLY_HEDGED obligations in FIFO order, computing available quantity (target − confirmed − reserved), and reserving only the amount that can actually be backed by those obligations. If the backed amount is below the supplied minimum base quantity, no reservation is created.

The route identity pins account, Futures venue, market type, native symbol, base asset, position side, contract type/size, quantity unit, and base-per-unit conversion. PostgreSQL composite foreign keys enforce that obligations, reservation items, and allocations cannot reference legs, fills, reservations, or obligations belonging to another session. This prevents aggregation across incompatible contract definitions.

Reservation lifecycle:
- RESERVED: durable intent exists; no submit is claimed.
- SUBMITTED: caller has bound the reservation to a deterministic client order ID before invoking the mutation gateway.
- UNKNOWN: submit outcome is ambiguous; reserved quantity remains unavailable to other orders.
- PARTIALLY_FILLED / CONSUMED: confirmed Futures fills have been allocated.
- RELEASED: only proven-unfilled remainder has been returned to obligations.

UNKNOWN/SUBMITTED/PARTIALLY_FILLED reservations cannot be released without both terminal-order verification and fill reconciliation. Both proof flags must be actual booleans; truthy strings are rejected. Release also requires non-empty terminal-order and fill-reconciliation evidence records (source, evidence ID, timestamps, the exact reserved client-order identity, terminal status, and an explicit fills-checked assertion). The evidence is persisted on the reservation and copied into the session event journal. Unknown submissions remain locked until evidence is supplied.

## Allocation contract

A confirmed Futures fill is checked against the reservation's exact route and position side. Its base-equivalent quantity is `fill.quantity × futures_leg.base_per_unit`. The service rejects any fill that exceeds remaining reserved quantity.

Allocation is FIFO across reservation items and writes, in one transaction:
- append-only allocation rows
- reservation-item allocated quantity
- obligation confirmed/reserved/remaining quantities and status
- reservation aggregate fill state
- session aggregate version and event journal

Duplicate Futures fill allocation is a no-op. Reusing a fill identity for another reservation or quantity is a conflict.

## Session lifecycle

The first confirmed Spot-fill obligation advances ENTRY_IN_PROGRESS to HEDGE_PENDING. Confirmed allocation advances HEDGE_PENDING to PARTIALLY_HEDGED. This service does not mark a session HEDGED or COMPLETED; those states require the later exposure reconciler to verify both legs, orders, and residual obligations.

A new Spot fill after the session was already HEDGED or terminal reopens RECONCILIATION_REQUIRED rather than silently increasing exposure.

## Scope limitations

- Aggregation is within a single session across multiple partial Spot fills. Cross-session pooled Futures batches are deferred until the data model has a batch-level fill identity independent of a single session/leg.
- No Futures order is submitted here. A caller may pass the reservation to the future mutation gateway in a later lifecycle stage.
- No live exchange, scanner, V2 runtime, production DB, or VPS changes are made.
