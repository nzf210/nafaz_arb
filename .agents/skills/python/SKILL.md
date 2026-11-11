---
name: python
description: Python engineering standards for Nafazbot executor, exchange adapters, risk, order lifecycle, and tests.
---

# Python skill — Nafazbot

- Follow PEP 8, type hints for public interfaces, explicit return types, and focused modules.
- Separate pure calculations from I/O and orchestration; inject exchange clients, clocks, and persistence boundaries.
- Keep order lifecycle, risk decisions, exchange transport, and persistence responsibilities distinct.
- Use Decimal or the repository's established fixed-point representation for all monetary, fee, price, and quantity calculations; never use float for financial decisions.
- Validate external API responses and configuration at trust boundaries. Treat missing or stale exchange state as unknown, not zero.
- Preserve idempotency, client order IDs, retry limits, cancellation semantics, partial-fill handling, and recovery guarantees.
- Catch only expected exceptions at the layer that can handle them; preserve exception context and never swallow execution or persistence failures.
- Avoid hidden module-level mutable state. Make time and network dependencies injectable in tests.
- Test success, rejection, timeout, partial fill, duplicate event, stale data, and recovery paths with deterministic fakes.
- Keep every Python source file <= 500 lines. Split cohesive responsibilities without creating pass-through modules.
- Run pytest -q and the relevant focused tests after each slice.
