---
name: golang
description: Go engineering standards for Nafazbot scanner, domain, exchange adapters, concurrency, and tests.
---

# Go skill — Nafazbot

- Follow gofmt, idiomatic Go naming, small interfaces, explicit errors, and standard-library-first design.
- Keep domain packages independent from HTTP, Telegram, Redis, Supabase, and exchange SDK details.
- Define interfaces at the consumer boundary; avoid broad interfaces and unnecessary factories.
- Pass context.Context as the first parameter for cancellable I/O; propagate deadlines and cancellation.
- Do not spawn unbounded goroutines. Make ownership, shutdown, synchronization, and error propagation explicit.
- Protect shared state with clear ownership and synchronization; run race tests for concurrency changes.
- Wrap errors with context using %w; never silently discard errors from persistence, order submission, or reconciliation.
- Keep money, price, fee, and quantity calculations in decimal/fixed-point types. Do not convert through float64.
- Validate exchange payloads and market metadata at adapter boundaries; fail closed on unknown or ambiguous values.
- Use table-driven tests where they improve clarity; test timeouts, partial failures, cancellation, and malformed responses.
- Keep every Go source file <= 500 lines. Split by cohesive responsibility and keep package APIs stable.
- Run gofmt, go test ./..., go test -race ./..., and go vet ./... for relevant changes.
