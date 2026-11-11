# Nafazbot V2 — Engineering Rules

## Mission
Refactor incrementally toward small, cohesive modules while preserving observable behavior. This repository handles market data and automated execution; correctness and fail-safe behavior outrank stylistic cleanup.

## Hard constraints
- New or modified Go, Python, and SQL source files must remain at or below 500 lines. Split by responsibility, not arbitrary line count.
- Keep functions cohesive and small; avoid god objects, duplicate business rules, hidden side effects, and unnecessary abstractions.
- Preserve public APIs, persisted data contracts, exchange semantics, order lifecycle, fee calculations, and recovery behavior unless a change is explicitly requested.
- Never use binary floating point for monetary/quantity calculations. Follow existing decimal/fixed-point conventions.
- Fail closed when exchange state, balances, order status, or reconciliation data is unknown.
- Do not log, commit, or expose API keys, tokens, service-role credentials, account identifiers, or live trading secrets.
- Every non-trivial behavior change needs focused tests, including edge/failure cases.
- Do not perform live exchange actions in tests. Use mocks, paper clients, and deterministic fixtures.
- SQL migrations are additive and forward-only. Preserve RLS, grants, idempotency, constraints, and migration replay safety.

## Architecture direction
- Go: keep domain rules independent of transport and persistence; adapters implement small interfaces.
- Python: separate orchestration from exchange adapters, order lifecycle, risk, persistence, and pure calculations.
- Supabase: keep schema changes in versioned migrations; keep SQL readable and narrowly scoped.
- Prefer dependency injection at system boundaries; avoid service-locator patterns and speculative layers.
- Keep compatibility facades only where callers require them; migrate callers and tests before deleting old entry points.

## Required workflow
1. Read the target module, its callers, tests, fixtures, configuration, and relevant docs.
2. Identify invariants and failure modes before moving code.
3. Make one cohesive refactor slice; avoid mixing unrelated behavior changes.
4. Keep each touched source file <= 500 lines.
5. Run focused tests, then the relevant Go/Python/SQL checks.
6. Review the diff for behavior changes, secrets, duplicated logic, dead code, and SonarQube issues.
7. Report checks actually run; never claim SonarQube is clean without a successful server scan.

## Skills
Load the relevant project skill before substantial changes:
- `.agents/skills/ponytail/SKILL.md`
- `.agents/skills/golang/SKILL.md`
- `.agents/skills/python/SKILL.md`
- `.agents/skills/supabase-sql/SKILL.md`
