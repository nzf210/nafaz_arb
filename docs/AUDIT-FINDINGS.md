# Audit Findings: Scanner & Executor
**Tanggal Audit:** 2026-11-03  
**Auditor:** Copilot AI (Static Code Inspection)  
**Status:** NOT READY FOR LIVE TRADING

> **Update 2026-11-03:** Seluruh bug P0 (BUG-01…BUG-06) sudah diperbaiki di Fase 0.
> **Update 2026-11-03 (Fase 1):** BUG-05, BUG-07, BUG-09, dan BUG-10 sudah diperbaiki.
> **Update 2026-11-03 (Fase 2):** BUG-08 sudah diperbaiki (P2-01) bersama P2-02..P2-06.
> **Update 2026-11-03 (Fase 3):** P3-01..P3-05 selesai (semua opt-in, default off).
> Seluruh bug audit (BUG-01..BUG-10) tertutup dan tidak ada temuan baru yang terbuka.
> Lihat [IMPROVEMENT-PLAN.md](./IMPROVEMENT-PLAN.md#progres) untuk detail implementasi.

## Ringkasan Temuan

| Kategori | Jumlah |
|---|---|
| Bug Kritis (P0) | 5 |
| Bug High (P1) | 5 |
| Fitur Hilang | 22 |
| Implementasi Parsial | 14 |
| Race Condition | 4 |
| Recovery Issue | 6 |

## Bug Kritis

| ID | Deskripsi | File | Baris | Status |
|---|---|---|---|---|
| BUG-01 | Short entry price selalu salah di mock mode — `open_global_short_market` tidak return `price` field, fallback ke `price_idr / usdt_rate` (harga spot, bukan futures) | `executor/exchange_client.py`, `executor/engine.py:_record_hedged_position` | ~L97, ~L512 | ✅ Fixed (P0-04) |
| BUG-02 | Unhedged spot exposure jika global futures gagal — tidak ada rollback/emergency sell spot | `executor/engine.py:_execute_entry_hedge` | ~L470 | ✅ Fixed (P0-01) |
| BUG-03 | Restart kehilangan semua active positions — tidak ada startup recovery dari Redis | `executor/engine.py:initialize` | ~L72 | ✅ Fixed (P0-03) |
| BUG-04 | Force close tidak melikuidasi spot leg — hanya close futures short | `executor/engine.py:handle_force_close` | ~L1060 | ✅ Fixed (P0-05) |
| BUG-05 | Race condition duplicate order pada signal burst | `executor/engine.py:handle_signal_notification` | ~L174 | ✅ P1-01 |
| BUG-06 | `NameError: name 'funding' is not defined` di force_close (crash!) | `executor/engine.py:handle_force_close` | ~L1142 | ✅ Verified (P0-02) |
| BUG-07 | Signal TTL tidak di-set di Redis — signal bisa stale selamanya | `internal/redishandler/client.go:PublishSignal` | - | ✅ P1-02 |
| BUG-08 | State `ENTRY_PARTIAL` tidak pernah di-set di engine | `executor/engine.py`, `executor/models.py` | - | ✅ Fixed (P2-01) |
| BUG-09 | Stale orderbook dikirim ke Executor — tidak ada max age validation | `internal/scanner/service.go:fetchMarketData` | ~L570 | ✅ P1-05 |
| BUG-10 | `short_exit_usdt` dari snapshot, bukan actual fill price | `executor/engine.py:_execute_exit_fill` | ~L870 | ✅ BUG-10 (Fase 1) |

### Defect tambahan yang ditemukan saat implementasi

Keduanya ditemukan di luar daftar audit asli, dan keduanya **blocking** untuk fase
yang sedang dikerjakan:

| ID | Deskripsi | File | Status |
|---|---|---|---|
| BUG-11 | `const ARB_TRADE_EVENTS = "arb:trade_events"` — sintaks Go di dalam class Python. Modul `executor/engine.py` **tidak bisa di-import sama sekali** sejak commit `86584ef`, sehingga seluruh suite Fase 1 tidak pernah benar-benar berjalan. Keempat call-site memakai nama yang tidak ada di scope. | `executor/engine.py` | ✅ Fixed (prasyarat Fase 2, jadi `self.ARB_TRADE_EVENTS` di `__init__`) |
| BUG-12 | `fetchMarketData` membungkus error dengan `fmt.Errorf(... , %v)`, yang **memutus rantai error**. Akibatnya `errors.Is(err, ErrExchangeUnavailable)` tidak pernah cocok dan deteksi maintenance P2-05 tidak berfungsi. | `internal/scanner/service.go` | ✅ Fixed (`errors.Join` + `%w`) |


---

## V2 review additions — 2026-11-13

V2 bootstrap/configuration/STOP work is tracked separately. The detailed review
contract, including CEX rate-limit versus confirmed IP-block behavior, is in
[ V2-BOOTSTRAP-CONFIG-STOP-FENCING-CHECKLIST.md ](./V2-BOOTSTRAP-CONFIG-STOP-FENCING-CHECKLIST.md).
These are open requirements, not completed remediations.

| ID | Current finding | Area | Required remediation / verification | Status |
|---|---|---|---|---|
| R-19 | A CEX may block the executor's shared egress IP after rate-limit abuse; current per-venue cooldown alone does not establish a durable cross-instance stop. A generic HTTP 429 is not proof of an IP ban. | Exchange adapter / global STOP / distributed admission | Classify venue-specific explicit IP-ban evidence (e.g. Binance 418), persist idempotent `CEX_IP_BLOCK` STOP in PostgreSQL, advance fencing epoch for all instances sharing egress, close new-entry admission, back off requests, preserve unresolved exposure for reconciliation, and require verified unblock + operator acknowledgement before resume. Add 429/418/403 and multi-instance regression tests. | PARTIAL — PostgreSQL STOP singleton + durable submit ledger/fence, exact-ID startup recovery, spot/futures IP-ban STOP, HMAC-signed operator resolution, and real PostgreSQL fault-injection test are implemented; latest CI must verify the new integration job, and live venue contract probes remain OPEN |

## Post-refactor review — 2026-11-07

The previous "all findings closed" status is historical and does not mean the current
main branch is production-ready. A fresh static review identified additional gaps
after the executor was split into mixins.

| ID | Current finding | Area | Remediation in this branch |
|---|---|---|---|
| R-01 | Entry TTL/adverse/emergency cancellation released state without verifying final venue status | Entry | Cancel + fetch + terminal-state verification; reconcile fills before releasing funds |
| R-02 | Startup recovery deleted the Redis pending key after re-adopting an open order | Recovery | Keep pending record durable; terminal partial fills are hedged before finalization; unknown status blocks pair |
| R-03 | Force close trusted cancel acknowledgement without checking the order | Cancellation | Verify final venue order state and reconcile raced exit/entry fills; abort force close if unresolved |
| R-04 | Force close treated missing futures close response as success | Force close | Require authoritative futures position query to confirm flat |
| R-05 | Force close treated any positive spot fill as full liquidation | Force close | Require filled quantity to cover requested quantity; partial fill remains reconciliation-required |
| R-06 | Entry hedge recorded requested quantity without checking actual futures fill | Entry hedge | Require a verifiable full futures fill before recording a hedged position |
| R-07 | Exit flow reduced position state without verifying futures close fill | Exit | Require full futures close fill; otherwise retain position and block automatic retries |
| R-08 | Funding accrual used estimated notional × rate and assumed an 8-hour interval | Funding | Use exchange funding history amounts and next-settlement schedule; never credit estimates |
| R-09 | Balance read failure skipped the mismatch guard | Risk | Halt new entries on both venues when balance data is unavailable |
| R-10 | Scanner HTTP API was unauthenticated | API | Go API requires Bearer token in LIVE mode; health remains public |
| R-11 | Startup restored Redis active positions without checking actual futures exposure | Recovery | Live startup compares exchange short quantity with restored base-asset quantity; mismatch/unknown state is blocked
| R-12 | LIVE API accepted arbitrarily short bearer tokens | API | LIVE config now requires a minimum 32-character token
| R-13 | Malformed futures position payload could be filtered into an empty list and mistaken for a flat account | Exchange adapter / recovery | `fetch_positions` now rejects non-list, malformed, or symbol-less position payloads; only a valid empty list means flat
| R-14 | Executor container ran as root and copied `config.json` into an image layer | Deployment | Executor now runs as a dedicated non-root user and receives config only at runtime; CI builds both images
| R-15 | Live CCXT client had no balance adapter, so fail-closed checks would halt every LIVE entry; sizing also requires IDR-to-USDT normalization | Risk / exchange adapter | Added async live balance reads for spot and futures, FX normalization from the fresh scanner snapshot, and fail-closed handling when FX/balance data is unavailable
| R-16 | Docker build context could include local `config.json` and `.env` secrets even when the image did not copy them | Deployment | Added `.dockerignore` to exclude secrets, credentials, runtime data, and repository metadata from build context
| R-17 | Scanner API returned raw Redis/scanner error details to HTTP clients, including status/latest result payloads | API | Return generic 500 responses, sanitize status/latest scan errors, log internal details server-side, and add regression tests
| R-18 | Repeating force close after futures closed but spot liquidation failed could submit a duplicate futures close and fail to retry spot | Force close | Retry verifies actual futures exposure first; if already flat, skips duplicate close and retries spot; if a short remains, blocks resubmission pending manual reconciliation

These changes are static-code remediations with regression tests added. They have not
been executed in this environment; CI must pass before the branch is approved.


| R-10 | Scanner HTTP API was unauthenticated | API | Go API now requires a Bearer token in LIVE mode; health remains public |

## V2 order-path audit update — 2026-11-13

| ID | Finding | Area | Remediation / current status |
|---|---|---|---|
| R-20 | Durable submit startup recovery used fetch_local_order_by_client_id for every attempt, including futures hedge rows marked market_type=perpetual. A real accepted futures order could remain unresolved because the wrong market adapter was queried. | Futures entry hedge / startup recovery | FIXED in commit a8eaee3: route perpetual/future/futures/swap to fetch_global_order_by_client_id; spot/local to local lookup; unknown market type fails closed. Regression test added. |
| R-21 | PREPARED client-order-ID reuse compared core fields but omitted amount_spec, even though adapter conversion can change the actual contract/order amount. | GlobalStop durable ledger / idempotency | FIXED in commit 1183d4e: include JSONB amount_spec in the intent equality check. Regression test rejects a changed contract specification. |

### Order-path inventory status

- Exposure opening: spot limit BUY and futures market SELL hedge are wired through durable PREPARED/SUBMITTING and the shared PostgreSQL session advisory fence.
- Risk reduction: spot limit SELL exit, spot market SELL rollback/force-close, and futures market BUY with reduceOnly=True remain outside the entry STOP fence by design. Their close/exit idempotency and reconciliation still need separate audit; they must not be blocked without explicit risk-reduction analysis.
- Adapter surface: Indodax TAPIv2 order methods funnel through _create_order; current facade invokes them through the spot wrapper. Modular executor/venues/* files showed no direct create-order invocation in the inspected source, but runtime wiring and all optional venue configurations remain to be proven.
- Reconciliation: exact client-ID not-found and lookup failures continue to block entry; they are not proof of rejection because venue history may be eventually consistent. No automatic resubmit is allowed.

Remaining blockers: complete runtime call-site/call-graph audit across all venue configurations and retry/recovery paths; authenticated operator manual resolution; and real PostgreSQL multi-process fault injection proving that a connection loss releases the session lock while the durable SUBMITTING row blocks another process even if the fake venue later accepts the original request. CI status must be checked after the latest commits. The branch remains NOT READY FOR LIVE TRADING.


| R-22 | Futures entry-hedge previously persisted UNKNOWN/REJECTED on submit errors but did not escalate explicit venue IP-ban evidence into canonical Global STOP. | Futures entry hedge / CEX IP-block policy | FIXED in commit f6da668: write CEX_IP_BLOCK inside the active submit fence; local halt immediately; notify Redis after the fence; if durable STOP persistence fails, keep the attempt unresolved rather than marking it REJECTED. Generic 429 remains non-ban. |

| R-23 | Spot entry could run exact-client-ID fallback after an explicit IP restriction when the in-transaction canonical STOP write failed; for some IP-restriction signatures the definitive-rejection classifier did not short-circuit lookup, so a lookup hit could ACK and retire the unresolved fence without canonical Global STOP. | Spot entry / CEX IP-block policy | FIXED in commit 9a0a269: explicit STOP-persistence-failed flag skips lookup, best-effort marks UNKNOWN, and preserves local ambiguity. Regression test supplies a positive fake lookup result and asserts lookup is not called. |

| R-24 | Generic retry_with_backoff retried every exception for rollback and force-close spot market sells. A timeout, connection reset, or HTTP 503 can occur after the venue accepted the first market SELL, so repeated attempts could oversell or duplicate liquidation even with a stable client ID on venues without enforced client-ID idempotency. | Risk-reducing rollback / force-close retry | FIXED in commits 8c146f4, 8e2c67d, be54874: add explicit retry predicate; only confirmed HTTP 429 rejections are retried. Ambiguous errors and HTTP 503 stop after one attempt. Tests cover 429 retry and 503/timeout no-retry. |

| R-25 | Dynamic spot exit limit SELL logged ambiguous submit failures but left the position eligible for the next repricing cycle, which generated a new client ID and could duplicate a venue-accepted order whose ACK was lost. | Spot exit / cancel-replace lifecycle | FIXED in commit a698308: exact-client-ID lookup adopts a found order; lookup not-found/failure marks pair RECONCILIATION_REQUIRED and blocks new repricing submits. Regression test covers inconclusive lookup. Exit remains outside entry STOP fencing. |

| R-26 | Force-close futures reduceOnly BUY previously could submit a new client ID after a stale position read. | Force-close futures de-risking | FIXED FAIL-CLOSED. LIVE now derives a deterministic position-scoped ID, requires durable registry lookup, and exact-ID reconciles any prior intent. Missing/pending/partial/inconclusive outcomes block resubmission; residual close is manual reconciliation. Per-venue pending-close probes remain OPEN. |


## R-27 — Operator-authenticated resolution for ambiguous submit attempts

**Status: IMPLEMENTED; PostgreSQL integration CI verified on 2026-11-14 (workflow 43570939910). Operational closure remains OPEN.**

- Added migration `20261113000300_add_submit_resolution_audit.sql` for an append-only audit table with unique nonce, operator ID, decision, two distinct evidence references, rationale and signed-payload hash.
- Added `GlobalStopService.resolve_submit_attempt_signed`: HMAC-SHA256 signature verification, bounded timestamp window, nonce replay protection through a unique DB constraint, shared fencing lock, row lock, and atomic transition from SUBMITTING/UNKNOWN to ACKNOWLEDGED/REJECTED/ABORTED.
- Added `executor/operator_resolution.py` CLI. It reads a signed envelope and requires `POSTGRES_DSN` plus `OPERATOR_RESOLUTION_HMAC_SECRET` from the runtime secret manager.
- `FOUND` requires the exact venue order ID. `DEFINITIVE_REJECTION` and `PROVEN_NOT_SUBMITTED` require two distinct evidence references and an operator-signed rationale. A plain NOT_FOUND lookup is never accepted as evidence.
- This is HMAC operator authentication, not an integrated corporate SSO/OIDC identity provider. Secret issuance, rotation and custody remain deployment responsibilities; do not expose the shared secret to the executor's general runtime environment.

## R-28 — PostgreSQL multi-process disconnect/fault injection

**Status: TEST PASSED against disposable PostgreSQL in CI on 2026-11-14 (workflow 43570939910). Production-topology review and repeated fault-boundary validation remain OPEN.**

- Added `tests/test_postgres_submit_fencing_integration.py`: independent executor process commits SUBMITTING and enters a fake venue request; parent kills the executor process while a separate fake venue process remains active; a peer executor process must be rejected by the durable unresolved-attempt gate.
- The same integration file verifies signed operator resolution against PostgreSQL and confirms the audit row is persisted.
- CI now provisions PostgreSQL and runs this test after applying all migrations. Initial CI attempts exposed a missing dependency install in the schema job; workflow was corrected to install the repository requirements under Python 3.12. The 2026-11-14 workflow ran this integration file with 2 passed tests, including the multi-process disconnect/fake-venue boundary and signed operator-resolution/audit case.

## R-29 — Venue recovery and pending-close contract coverage

**Status: PARTIAL.**

- Added fake endpoint delayed-visibility and pending reduce-only close fixtures for Indodax, Tokocrypto, MEXC and OKX in `tests/test_venue_recovery_contracts.py`.
- Added `docs/ORDER-RUNTIME-VENUE-RECOVERY-MATRIX.md` with active adapter/call-site and retry/recovery inventory.
- Fake fixtures prove executor behavior under delayed visibility; they do not establish live venue eventual-consistency SLAs or each venue's pending-close semantics. Those still require controlled per-venue sandbox/paper probes.
- R-26 duplicate-new-client-ID retry is fixed fail-closed with deterministic ID reuse and exact-ID lookup. Pending/partial/not-visible outcomes remain reconciliation-required; live per-venue pending-close semantics are still unverified.


## R-26 update — Force-close reduceOnly duplicate submit blocked

**Status: FIXED FAIL-CLOSED (2026-11-13).**

- LIVE force-close now derives a deterministic close client ID from the persisted position `opened_at_ms` and requires the durable order registry.
- If the intent already exists, the executor performs exact client-ID lookup. A lookup miss/error or non-terminal order blocks a new submit; it does not mint another client ID.
- A previously found terminal full-fill order is adopted and futures position flatness is verified before continuing spot liquidation.
- Partial terminal close, missing stable position timestamp, missing registry, or stale/non-flat position remains a reconciliation/manual-intervention condition. This is intentionally fail-closed; it does not auto-submit a residual close with a new ID.
- Regression: `test_force_close_prior_reduce_only_intent_not_found_never_resubmits`.


## V2 full create-order call-site audit — 2026-11-14

Audit scope: branch \`feat/v2-bootstrap-config-stop-fencing\`, head
\`97819da0d068ca67f8c51f74cb2f9129f150a270\` before this audit patch.

### Proven create-order inventory and classification

| Path | Call chain / adapter endpoint | Exposure effect | STOP ledger + shared session fence |
|---|---|---|---|
| Spot entry | \`EngineEntrySubmissionMixin._submit_entry_order\` → \`client.place_local_limit_buy\` → \`ExchangeOrdersMixin.place_local_limit_buy\` → CCXT \`create_limit_buy_order\`; Indodax TAPIv2 adapter funnels through \`IndodaxTAPIv2._create_order\` | Opens spot exposure | REQUIRED; implemented on the active LIVE path |
| Futures hedge entry | \`EntryHedgeMixin._submit_entry_hedge_order\` → \`client.open_global_short_market\` → \`ExchangeFuturesMixin.open_global_short_market\` → CCXT \`create_market_sell_order\` with \`reduceOnly=False\` and short position-side params | Opens/extends short exposure | REQUIRED; implemented on the active LIVE path |
| Generic gateway create | \`MutationGateway.execute_create_order\` → \`create_order_direct\` or CCXT \`create_order\` | Potentially opens/extends exposure depending on arguments | REQUIRED. This method had an unfenced \`global_stop=None/disabled\` branch, did not persist ACK, and treated all non-timeout exceptions as rejection. It had no production create caller in the inspected branch; the Telegram command processor calls only \`execute_cancel_order\`. This latent bypass is now fail-closed and ledger transitions are explicit. |

No direct create-order calls were found in \`executor/venues/{indodax,mexc,okx,tokocrypto}.py\`; those modules provide venue formatting/read fallbacks. The Indodax adapter's public create methods funnel through its single \`_create_order\` implementation. Runtime alternative-venue certification is still open.

### Risk-reducing paths deliberately excluded from entry STOP fence

- Spot limit SELL: \`exit_orders\` → \`place_local_limit_sell\` → \`create_limit_sell_order\`; exit only.
- Spot market SELL: \`rollback\` and \`force_close_legs\` → \`place_local_market_sell\` → \`create_market_sell_order\`; rollback/force-close only.
- Futures market BUY: \`exit_execution\` and \`force_close_legs\` → \`close_global_short_market\` → \`create_market_buy_order\` with \`reduceOnly=True\`; closes short exposure.
- Cancel: \`CommandProcessor\` → \`MutationGateway.execute_cancel_order\`; cancellation is not a create and is not routed through entry admission.

These actions remain outside the entry STOP fence intentionally. Their own stable-ID, cancel/replace, retry, reduceOnly and reconciliation controls remain separately audited; a global STOP must not suppress a verified risk-reducing action.

### R-30 / R-31 — latent unfenced entry paths

| ID | Finding | Status after this patch |
|---|---|---|
| R-30 | Generic \`MutationGateway.execute_create_order\` could call a venue without an initialized GlobalStop; ACK was not durably written and generic transport errors were mislabeled REJECTED. | FIXED FAIL-CLOSED: requires initialized service, writes PREPARED, enters shared fence, persists ACK, classifies only explicit rejections as REJECTED, and retains UNKNOWN/SUBMITTING on ambiguity or persistence failure. |
| R-31 | Spot and futures entry mixins retained \`global_stop is None\` compatibility branches. LIVE startup already rejects missing \`POSTGRES_DSN\`, but direct invocation could bypass the startup gate. | FIXED DEFENSE-IN-DEPTH: both submit boundaries reject missing GlobalStop in LIVE mode; mock compatibility remains available. |

Regression coverage added for missing service, connection reset, explicit rejection, missing venue order ID, ACK persistence failure, and direct LIVE spot/futures invocation without GlobalStop.

### Remaining blockers — NOT LIVE-CERTIFIED

1. Operator-authenticated resolution is implemented as HMAC-signed envelope + nonce/evidence audit, but operator identity provisioning, secret custody/rotation, approval workflow and operational runbook have not been independently closed.
2. The real PostgreSQL multi-process disconnect/fault-injection test must pass on the latest CI revision and its failure boundary must be reviewed; fake venue behavior is not a real CEX contract probe.
3. Real per-venue sandbox/paper probes remain open for exact client-ID visibility, pending/partial market hedge semantics, cancel/fill races and reduceOnly close behavior.
4. Runtime adapter configuration coverage, all account/position modes, and deployment-wide shared PostgreSQL DSN consistency still require verification.

No merge, deployment, production migration, or cutover was performed.


## R-32 — PREPARED intent reuse omitted recovery context

**Status: FIXED; regression and full CI verified on 2026-11-14 (workflows 43571991627 and 43572108728).**

- `GlobalStopService.prepare_submit_attempt` already compared exchange, market type, symbol, side, order type, quantity, price, pair key and `amount_spec` when the same client order ID was presented again while PREPARED.
- It did not compare JSONB `recovery_context`. For a futures hedge, this context binds the submit to its parent spot exchange/order/client ID. A stale PREPARED ID could therefore be accepted with a different parent identity even though the actual venue parameters were unchanged.
- Commit `01f412a09edcc5ec23781d1b0eedbfa4b061a942` adds consistent JSONB mapping and compares `recovery_context` with the rest of the durable intent.
- Commit `d8eb70d647f452744c894a3f92c5b8fb3f1dde6c` adds a regression test: identical PREPARED context is reusable; changing the parent spot client ID is rejected.
- This does not permit a second venue submit from SUBMITTING/UNKNOWN/ACKNOWLEDGED rows; those states were already non-reusable. The patch tightens intent integrity rather than changing order admission policy.

## V2 audit continuation — 2026-11-14

The current source review traced the active exposure-increasing calls:
1. Spot BUY entry → `place_local_limit_buy` → CCXT `create_limit_buy_order`; Indodax uses TAPIv2 `_create_order` and one signed `POST /api/v2/order`.
2. Futures SELL hedge → `open_global_short_market` → CCXT `create_market_sell_order` with `reduceOnly=False` and short-side params.
3. Generic `MutationGateway.execute_create_order` is potentially exposure-increasing but has no production create caller in the reviewed branch; the command processor uses only the cancel method. The generic create API itself is fail-closed and uses the durable ledger/fence.

Risk-reducing paths remain outside the entry STOP fence by design: spot limit SELL exit, spot market SELL rollback/force-close, futures market BUY with `reduceOnly=True`, and cancel. Their independent retry, exact-ID and reconciliation controls remain required; a generic STOP must not block de-risking.

The checked GlobalStop signatures match the actual spot, futures and generic-gateway callers. Startup reconciliation routes exact client-ID lookup by market type. Found orders are ACKNOWLEDGED; NOT_FOUND and lookup errors remain SUBMITTING/UNKNOWN and block new entries because venue history may be eventually consistent. Ambiguous request errors are not retried; ACK persistence failures do not authorize a new client ID or new submit. Indodax's raw POST boundary has no generic POST retry. No direct create calls were found in the modular `executor/venues/*` adapters.

### CI evidence and remaining risk

- Source patch commit `d8eb70d`: workflow [43571991627](https://github.com/nzf210/nafaz_arb/actions/runs/43571991627) passed all four jobs. The PostgreSQL integration file reported 2 passed; full Python suite reported 559 passed, 2 skipped. The new recovery-context regression is included.
- Final documentation head `d8c1f207`: workflow [43572108728](https://github.com/nzf210/nafaz_arb/actions/runs/43572108728) also passed all four jobs, with the same Python and PostgreSQL integration counts. The two Python skips are in the general job; the dedicated PostgreSQL integration job ran both tests. Two non-failing pytest unknown-marker warnings remain.
- Operator resolution has a passing HMAC + PostgreSQL audit integration test. Deployment identity provisioning, secret custody/rotation, approval controls, audit retrieval and runbook remain OPEN; this is shared-secret HMAC, not SSO/OIDC.
- The multi-process PostgreSQL backend-disconnect test passed on disposable CI PostgreSQL on the source patch and final documentation head. Repeated stress/fault-boundary review and production topology validation remain OPEN.
- Real per-venue sandbox/paper probes remain OPEN for exact client-ID visibility, eventual consistency, pending/partial hedge market orders, cancel/fill races and reduceOnly close semantics.
- Latest partial-fill sizing may intentionally enlarge the futures short beyond actual spot fill to meet venue minimum notional/contract rules. Delta imbalance is logged; the hard-drift path currently logs rather than automatically unwinding. This is a strategy/risk-policy review gate, not a STOP-fence bypass; it was not changed in this audit.

**Overall status remains PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration or cutover was performed.
