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

**Status: FIXED; corrected SQL projection regression and full CI verified on 2026-11-14 (workflow 43572374744).**

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

- Corrected source/test head `df41a7c`: workflow [43572374744](https://github.com/nzf210/nafaz_arb/actions/runs/43572374744) passed all four jobs. PostgreSQL integration reported 2 passed; full Python suite reported 559 passed, 2 skipped. Go tests, race tests, vet, Docker builds, migration ordering/idempotency, RLS/grants/triggers and shell syntax all passed.
- The two Python skips are in the general job; the dedicated PostgreSQL integration job ran both tests against disposable PostgreSQL. Two non-failing pytest unknown-marker warnings remain.
- Operator resolution has a passing HMAC + PostgreSQL audit integration test. Deployment identity provisioning, secret custody/rotation, approval controls, audit retrieval and runbook remain OPEN; this is shared-secret HMAC, not SSO/OIDC.
- The multi-process PostgreSQL backend-disconnect test passed on disposable CI PostgreSQL in workflow 43572374744. Repeated stress/fault-boundary review and production topology validation remain OPEN.
- Real per-venue sandbox/paper probes remain OPEN for exact client-ID visibility, eventual consistency, pending/partial hedge market orders, cancel/fill races and reduceOnly close semantics.
- Latest partial-fill sizing may intentionally enlarge the futures short beyond actual spot fill to meet venue minimum notional/contract rules. Delta imbalance is logged; the hard-drift path currently logs rather than automatically unwinding. This is a strategy/risk-policy review gate, not a STOP-fence bypass; it was not changed in this audit.

**Overall status remains PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration or cutover was performed.


## R-33 — Recovery-context regression fake masked a missing SQL projection

**Status: FIXED AND VERIFIED IN CI (2026-11-14).**

- After adding the recovery-context equality check, the conflict-path SQL still selected `amount_spec` but not `recovery_context`.
- The first fake PostgreSQL connection returned the entire stored row regardless of the SELECT list. This let the test pass even though a real asyncpg Record would not contain the missing field and identical PREPARED reuse would be rejected.
- Commit `be30fc2710b3b24c325ac6f275a7ad5a5981eda9` adds `recovery_context` to the SELECT projection.
- Commit `df41a7c2d9e4234d85be42b0f16276aef9ca3a7f` makes the fake connection project only the columns actually selected, so the regression now detects a missing projection.
- Workflow `43572374744` passed: 559 Python tests passed (2 skipped), and the disposable PostgreSQL integration job reported 2 passed. This confirms the corrected fake/unit path and the real PostgreSQL fencing tests; it is not a production VPS or exchange test.


## V2 audit continuation — 2026-11-15 (source baseline `ca8f2e7095b7`)

### Complete mutation inventory from executor wrappers

The production Python order-mutation boundaries found in this pass are:

| Boundary | Raw venue call | Exposure classification | Durable entry ledger + shared session fence |
|---|---|---|---|
| `ExchangeOrdersMixin.place_local_limit_buy` | `create_limit_buy_order` | Spot BUY; opens exposure | REQUIRED; active LIVE caller `_submit_entry_order` prepares and fences |
| `ExchangeOrdersMixin.place_local_limit_sell` | `create_limit_sell_order` | Spot SELL; reduces inventory | Not entry-fenced; exact-ID ambiguity recovery and cancel/replace controls apply |
| `ExchangeOrdersMixin.place_local_market_sell` | `create_market_sell_order` | Spot SELL; rollback/force-close/sniper de-risking | Not entry-fenced; retry/ambiguity controls apply |
| `ExchangeFuturesMixin.open_global_short_market` | `create_market_sell_order`, `reduceOnly=False` | Futures SELL; opens/adds short | REQUIRED; active LIVE caller `_submit_entry_hedge_order` prepares and fences |
| `ExchangeFuturesMixin.close_global_short_market` | `create_market_buy_order`, `reduceOnly=True` | Futures BUY; closes short | Not entry-fenced; close lifecycle has independent exact-ID/reconciliation controls |
| `IndodaxTAPIv2._create_order` | signed `POST /api/v2/order` | Adapter raw boundary used by the above spot BUY/SELL wrappers | Inherits caller classification; no generic POST retry |
| `MutationGateway.execute_create_order` | `create_order_direct` or CCXT `create_order` | Generic, potentially exposure-increasing | REQUIRED; method itself fails closed and persists PREPARED/SUBMITTING/ACK/rejection/unknown. No active production create caller found; engine imports no create call and command processor uses cancel only |

The modular `executor/venues/{indodax,mexc,okx,tokocrypto}.py` modules contain client-ID formatting/venue parameter helpers, not raw create-order calls. Paper exchange methods are simulation-only and do not call a CEX. No additional direct create-order boundary was found in the reviewed executor orchestration, recovery, rollback, force-close, or Go market-data adapter files.

### R-34 — Taker-sniper spot SELL ambiguous ACK could fall through to another SELL

**Status: FIX IMPLEMENTED; first regression CI passed, fill-validation follow-up CI pending at time of this entry.**

- The taker-sniper path returned `False` for every market SELL exception. Its caller could then continue into smart-maker exit and submit a second SELL with a new client ID even when the first request might have reached the venue.
- It now classifies explicit venue rejection separately. For ambiguous timeout/transport outcomes it performs exact client-ID lookup; a match is adopted, while not-found/lookup failure marks the pair RECONCILIATION_REQUIRED and returns a handled result so no fallback SELL is sent.
- A venue ACK without an exchange order ID also becomes reconciliation-required.
- Regression tests cover lost ACK with exact-ID match and not-found. Workflow [43691790303](https://github.com/nzf210/nafaz_arb/actions/runs/43691790303) passed all four jobs on test head `17c4f67446d0`.

### R-35 — Taker-sniper futures close used requested amount instead of confirmed spot fill

**Status: FIX IMPLEMENTED; latest CI pending.**

- The prior code used `filled or amount or position_qty`. An acknowledged market SELL with `filled=0` could therefore close the entire futures short even though no spot quantity had sold; a partial fill could also over-close the short.
- The path now requires a present, finite, non-negative cumulative `filled` value no larger than the position quantity (within tolerance). Missing or invalid fill evidence becomes reconciliation-required.
- A zero-fill open order is retained as the same pending exit order; no futures close and no fallback SELL occurs. Explicit terminal no-fill rejection/cancel/expiry can be retired and the caller may continue with a replacement exit.
- Futures `reduceOnly=True` close is sent only for the confirmed spot-filled quantity. New fake-venue regressions cover zero-fill and half-fill behavior.
- These changes do not put exits behind Global STOP. They prevent a risk-reducing path from creating a naked spot-long delta by closing more futures than spot actually sold.

### Other exposure and STOP findings

- The two active LIVE exposure-opening call chains remain spot BUY and futures short SELL. Both commit PREPARED before the venue call and hold the shared PostgreSQL session advisory fence while the venue request is active.
- Generic gateway create is fail-closed but no active production create caller was found.
- GlobalStop signatures and callers were rechecked: `prepare_submit_attempt(...)`, `order_submit_fence(client_order_id=...)`, `request_stop(reason, source=..., metadata=...)`, and `request_stop_in_transaction(conn, reason, source=..., metadata=...)`. The entry fence uses the session-level advisory lock; STOP takes the conflicting transaction-level lock.
- Intent equality includes exchange, market type, symbol, side, order type, quantity, price, pair key, amount specification and recovery context. PREPARED reuse is only allowed for identical intent; SUBMITTING/UNKNOWN and terminal states cannot be reused.
- Startup recovery routes exact client-ID lookup by spot/perpetual market type. Found orders are ACKNOWLEDGED. NOT_FOUND and lookup failure remain unresolved; eventual consistency is not treated as definitive rejection. ACK persistence failure leaves the durable submit unresolved and does not authorize a new ID or submit.
- The entry `scale_to_min` policy can intentionally make the futures short larger than the actual spot fill to satisfy futures minimum notional/contract rules. This is fenced but remains a material strategy exposure decision; delta imbalance is recorded, and hard-drift does not yet automatically unwind the excess. Keep this as an explicit strategy approval gate.
- Risk-reducing spot SELL, futures `reduceOnly=True` BUY close, and cancel remain outside entry STOP by design. Their own exact-ID, retry, fill-quantity and reconciliation protections are required; do not apply a generic STOP to these operations.

### Verification and release state

- Source baseline CI before the new exit fixes: workflow [43677081780](https://github.com/nzf210/nafaz_arb/actions/runs/43677081780) passed all four jobs (Python, Go/race/vet, PostgreSQL schema/integration, Docker builds).
- R-34 exact-ID recovery regression head: workflow [43691790303](https://github.com/nzf210/nafaz_arb/actions/runs/43691790303) passed all four jobs.
- Source/test head `ee1c2db2707c` (including R-34/R-35 and all four new fake-venue regression tests) completed workflow [43691977186](https://github.com/nzf210/nafaz_arb/actions/runs/43691977186) with all four jobs successful: Python executor tests, Go tests/race/vet, PostgreSQL schema/integration, and Docker builds. The PostgreSQL job's multi-process backend-disconnect/fake-venue step also passed. The final documentation head `19c8b3aeebdc` completed workflow [43692065378](https://github.com/nzf210/nafaz_arb/actions/runs/43692065378) with all four jobs successful. CI uses disposable PostgreSQL and fake venue behavior; it does not validate production VPS topology or real CEX contracts.
- Operator HMAC resolution remains a shared-secret mechanism, not enterprise identity/SSO. Provisioning, key custody/rotation, dual approval, audit retrieval and operational runbook remain open.
- PostgreSQL multi-process disconnect/fake-venue integration has passed in prior CI, but repeated stress/fault-boundary review and production VPS topology validation remain open. It is not a real CEX contract probe.
- Real per-venue sandbox/paper probes remain open for client-ID visibility, eventual consistency, pending/partial hedge semantics, cancel/fill races and reduceOnly close behavior.

**Overall status: PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.


## R-36 — OKX client-order-ID wire format was not applied to submit/recovery

**Status: FIX IMPLEMENTED; source/test CI passed on 2026-11-15.**

- The logical ID builder can produce up to 36 characters and includes dashes. OKX v5 requires `clOrdId` to be alphanumeric and no more than 32 characters ([official OKX order API](https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order)). Current CCXT OKX maps the unified `clientOrderId` parameter to `clOrdId`.
- `OKXVenue.format_client_order_id` existed but `venue_client_order_id` did not call it. Futures open/close passed the logical ID directly, and exact-ID recovery did not derive an OKX wire ID before an ACK alias had been learned.
- OKX now preserves already-valid short alphanumeric IDs; otherwise it maps the full logical ID to a deterministic 32-character SHA-256 hex prefix. It does not truncate the suffix, so distinct logical IDs do not collapse merely because their leading characters match.
- The same mapping is used by spot wrappers, futures short open, futures reduceOnly close, and generic MutationGateway create. Recovery's accepted-ID set includes the deterministic wire ID before ACK, and direct recovery lookup tries both logical and wire IDs. The OKX permission probe's raw order POST was separately removed; see R-37.
- Regression coverage verifies 32-character alphanumeric output, determinism, distinct logical IDs, accepted recovery aliases, and the actual futures short create call's wire parameter.
- Source/test CI for the OKX mapping and gateway commits passed all four jobs: workflows [43692450259](https://github.com/nzf210/nafaz_arb/actions/runs/43692450259), [43692452829](https://github.com/nzf210/nafaz_arb/actions/runs/43692452829), [43692456060](https://github.com/nzf210/nafaz_arb/actions/runs/43692456060), [43692515692](https://github.com/nzf210/nafaz_arb/actions/runs/43692515692), [43692520066](https://github.com/nzf210/nafaz_arb/actions/runs/43692520066), and [43692523689](https://github.com/nzf210/nafaz_arb/actions/runs/43692523689). These include Python executor tests, PostgreSQL schema/integration, Go/race/vet, and Docker builds.
- This is a source/CCXT contract fix; a real OKX sandbox/live contract probe is still required before relying on the mapping operationally. Other venue wire formats remain subject to per-venue probes.

**Overall status remains PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.


## R-37 — Unfenced OKX create-endpoint permission probe removed

**Status: FIX IMPLEMENTED AND VERIFIED IN CI (2026-11-15).**

- A final scan found one additional direct call to OKX `privatePostTradeOrder` inside `ExchangeFuturesMixin.probe_short_tradability`. It submitted an intentionally invalid zero-size/zero-price payload to infer trade permission.
- No production caller of this method was found, but the call still crossed a private create-order endpoint without the durable submit ledger or shared STOP fence. Relying on current venue validation to guarantee that a zero-size payload can never mutate state is not an acceptable safety boundary.
- The direct POST has been removed. For OKX, the method now returns a fail-closed “non-mutating probe unavailable” result and sends no order. A real permission probe must be a separately approved sandbox procedure or use a venue-supported read-only permission endpoint.
- A fake-venue regression asserts the method returns non-tradable and never invokes `privatePostTradeOrder`.
- This does not change any active spot/futures entry or risk-reducing close path; the method had no production caller in the reviewed branch.

Source head [b9716184e23c](https://github.com/nzf210/nafaz_arb/commit/b9716184e23ce1152d6e4ece1a1220f16748a10c) and regression-test head [5dcad7dc8cde](https://github.com/nzf210/nafaz_arb/commit/5dcad7dc8cde710c248bf7f7b8eb19ad738211f5) both passed all four CI jobs: [43692777060](https://github.com/nzf210/nafaz_arb/actions/runs/43692777060) and [43692785951](https://github.com/nzf210/nafaz_arb/actions/runs/43692785951). Python executor tests, PostgreSQL schema/integration, Go/race/vet and Docker builds all succeeded. Previous OKX wire-ID mapping and direct-recovery tests also passed workflows [43692697374](https://github.com/nzf210/nafaz_arb/actions/runs/43692697374) and [43692706776](https://github.com/nzf210/nafaz_arb/actions/runs/43692706776).

**Overall status remains PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.


## Order-mutation and exposure-path audit — 2026-11-16

**Audited source baseline:** `ea44caf051de4a435c5e40a8e36e288438c86063`  
**Remediation/test head:** `838aac5baefd0f3f96f1763ec59293445078b32e`  
**Scope:** all 100 `executor/**/*.py` source files and all 69 Go source files at the audited baseline; focused review of the entry/hedge, adapter, recovery, STOP, rollback, exit, and force-close call chains.

### Call-site inventory and classification

| Call-site / wrapper | Lifecycle | STOP fencing classification |
|---|---|---|
| `ExchangeOrderMixin.place_local_limit_buy` → CCXT `create_limit_buy_order` | Spot BUY entry; opens exposure | Durable PREPARED/SUBMITTING + shared PostgreSQL session fence required |
| `ExchangeFuturesMixin.open_global_short_market` → CCXT `create_market_sell_order` (`reduceOnly=False`) | Futures short hedge; opens/adds exposure | Durable PREPARED/SUBMITTING + shared PostgreSQL session fence required |
| `MutationGateway.execute_create_order` → `create_order_direct` or generic `create_order` | Generic private create | Requires durable ledger + shared fence; no production caller was found in the audited tree |
| `place_local_limit_sell` → `create_limit_sell_order` | Spot exit | Risk-reducing; uses de-risk fence, not entry PREPARED/SUBMITTING |
| `place_local_market_sell` → `create_market_sell_order` | Rollback, taker exit, force-close | Risk-reducing; uses de-risk fence, not entry PREPARED/SUBMITTING |
| `close_global_short_market` → `create_market_buy_order` (`reduceOnly=True`) | Futures close | Risk-reducing; reduceOnly is mandatory; uses de-risk fence |
| Indodax `create_limit_buy_order` / `create_limit_sell_order` / `create_market_sell_order` → `_create_order` → `POST /api/v2/order` | Venue raw HTTP boundary | Classification is inherited from the caller; no independent production caller bypass was found |
| `cancel_local_order` / venue `cancel_order` | Cancel existing order | Not a create-order exposure path; must remain available under STOP and reconcile final fills/status |

The two live exposure-increasing call chains are spot BUY and futures short SELL. Both refuse LIVE operation without an initialized `GlobalStopService`, persist PREPARED before the venue boundary, and enter the shared session advisory fence before the create call. The direct no-service branches are compatibility/mock-only after the explicit LIVE guards. The Go scanner/exchange packages contain no CEX private create-order call; their POST requests found in this scan are scanner/Supabase/Telegram APIs. The modular `executor/venues/*` adapters do not expose another independent create-order call-site.

Risk-reducing spot SELL and futures reduceOnly BUY calls do not use the entry ledger. They use stable client IDs/order-intent registry and exact-ID recovery in their owning lifecycle. They take the shared PostgreSQL lock when available, but Global STOP itself does not reject them. An unresolved entry is advisory for de-risking rather than a global close blocker.

### Findings fixed in this execution

| ID | Finding | Remediation | Status |
|---|---|---|---|
| R-38 | Futures `hedge_submit_started` was set before durable PREPARED and before entering the submit fence. A DB/STOP failure before the venue POST could be misclassified as an ambiguous submitted hedge, suppressing the normal spot rollback path. | Reset the flag at function entry; set it only immediately before the futures venue create call (including mock-only direct path). | FIXED; regression test verifies PREPARED failure makes zero venue/lookup calls and leaves the flag false |
| R-39 | Rollback retry predicate treated generic `timeout`, `internal error`, `system busy`, and `service unavailable` text as transient settlement failures. Those can be ambiguous after POST and could resubmit a spot SELL. | Keep retries only for explicit venue business rejections (Tokocrypto 2206/989012 and matching-engine balance rejection) plus explicit HTTP 429. Generic transport/server errors no longer qualify. | FIXED; fake venue timeout test verifies exactly one POST |
| R-40 | `GlobalStopService.order_submit_fence` and `derisk_submit_fence` demoted the PostgreSQL pool for connection-shaped exceptions raised by the yielded venue request, because the exception handler enclosed the yield. A venue timeout/reset could therefore be mistaken for a PostgreSQL disconnect and prevent ACK/UNKNOWN persistence on the still-healthy pool. | Track whether control was yielded. Only pre-yield DB errors and failed advisory unlock demote the pool; a venue exception is propagated without falsely demoting PostgreSQL. | FIXED; fake boundary tests distinguish venue timeout from PostgreSQL unlock disconnect |
| R-41 | De-risk submissions were blocked when a configured PostgreSQL service could not reconnect, even though STOP must not prevent removal of existing exposure. | De-risk fence now falls back unfenced only for recognized PostgreSQL connectivity/authority-unavailable failures, logs CRITICAL, and still requires a stable client ID. LIVE entry remains fail-closed. Invalid IDs and non-connectivity integrity errors remain blocked. | FIXED; regression tests cover STOP-active/unresolved pass-through and DB-unavailable de-risk fallback |

### Lifecycle invariants rechecked

- PREPARED intent equality includes exchange, market type, symbol, side, order type, quantity, price, pair key, `amount_spec`, and `recovery_context`.
- Entry fence commits PREPARED → SUBMITTING before yielding to the venue request. A duplicate/reused non-PREPARED client ID is rejected.
- Venue ACK persistence failure leaves SUBMITTING/UNKNOWN durable; it does not authorize a new client ID.
- Definitive venue rejection is distinguished from timeout/reset/5xx ambiguity. The rollback retry predicate is now restricted to explicit business rejection and explicit 429 evidence.
- Startup exact-client-ID reconciliation routes spot versus perpetual lookup by market type. A NOT_FOUND result or unavailable lookup remains unresolved; eventual consistency is not treated as proof of rejection.
- STOP and entry submit use the same advisory lock key. The current `request_stop` and `request_stop_in_transaction` signatures match the spot/futures IP-ban call sites.
- HMAC manual resolution requires a valid signature, bounded timestamp, nonce, two distinct evidence references, and append-only audit persistence. Operational identity provisioning, key custody/rotation, dual control, and runbook are not complete.

### Verification and remaining release gates

GitHub Actions run [43825126487](https://github.com/nzf210/nafaz_arb/actions/runs/43825126487) for source/test head `f113e79` completed successfully: Python executor suite, Go tests/race/vet, PostgreSQL schema/idempotency/RLS checks, real disposable-PostgreSQL multi-process submit-disconnect integration, and Docker scanner/executor builds all passed. The new regression file is included in the passing Python job. No local VPS or CEX sandbox test was performed.

Still OPEN:
- Venue contract validation for each configured CEX: client-ID idempotency, exact-ID visibility, lookup eventual consistency, partial/pending hedge, cancel/fill race, and reduceOnly semantics.
- Operator resolution operational controls: verified operator identity, secret custody/rotation, dual approval, audit retrieval, and runbook.
- Repeated PostgreSQL chaos/fault injection and validation against the actual VPS topology. CI disposable PostgreSQL evidence is not a production-topology sign-off.
- `scale_to_min` may intentionally create a futures short larger than the spot fill to satisfy venue minimum notional; hard-drift still needs an approved automatic/manual remediation policy.
- De-risking remains blocked on non-connectivity integrity failures such as missing Global STOP control row/schema/permission errors; operator repair is required rather than silently bypassing a corrupt authority.

**Status remains PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.


### Additional status-transition hardening — 2026-11-16

| ID | Finding | Remediation | Status |
|---|---|---|---|
| R-42 | `mark_submit_aborted` used the generic transition predicate and could technically change SUBMITTING/UNKNOWN to ABORTED, despite its contract meaning “venue boundary was never crossed.” Current callers were guarded, but the service API itself did not enforce the invariant. | Restrict ABORTED SQL transition to `status='PREPARED'` only. SUBMITTING/UNKNOWN can no longer be cleared by a pre-submit abort path; a lost boundary/fence race remains unresolved and requires reconciliation. | FIXED; regression verifies UPDATE 0 is rejected for an unresolved attempt |

Latest source/test CI [43825126487](https://github.com/nzf210/nafaz_arb/actions/runs/43825126487) completed successfully for `f113e79`; all four jobs passed, including the full Python executor job and disposable PostgreSQL multi-process disconnect integration.


### Force-close and order-registry hardening — 2026-11-16

| ID | Finding | Remediation | Status |
|---|---|---|---|
| R-43 | LIVE force-close spot liquidation generated a new sequence-based client ID on each invocation and recorded its pre-submit intent as optional. After a lost acknowledgement, a later force-close retry could therefore create a second SELL with a new ID. | Bind the force-close spot ID deterministically to pair + position `opened_at_ms`; require the durable order registry for LIVE; if a prior intent exists, perform exact client-ID lookup and adopt only a terminal verified full fill. NOT_FOUND, lookup failure, partial, or non-terminal results never create a new ID. | FIXED; fake venue test proves one POST across a lost ACK and retry |
| R-44 | `_register_order_intent` treated `config.postgres_dsn` as proof that the universal order registry was durable, although that DSN is used by GlobalStop and does not enable the Supabase order-record writer. A required LIVE intent could be Redis-only while the code believed it was durable. | Required LIVE order intents now require `supabase.enabled`; a local GlobalStop DSN alone is not accepted as the order-registry writer. Optional de-risk intents remain allowed so the general exit/rollback path is not blocked solely by registry outage. LIVE force-close spot now uses a required intent. | FIXED; regression covers local PostgreSQL DSN + disabled Supabase |

Force-close cancellation remains deliberately conservative: cancel acknowledgement alone is not terminal proof. The executor fetches final venue status, reconciles any entry fill and hedges any exit fill before allowing force-close legs to proceed. If a pending order remains non-terminal or its final status cannot be verified, force-close waits for reconciliation rather than risk a late entry fill reopening exposure.

**Residual de-risk trade-off:** ordinary optional exit/taker intents may still be best-effort/Redis-backed when Supabase is disabled, because blocking all exits on the order-registry writer would strand exposure. Force-close spot is stricter because it can be invoked repeatedly after a lost ACK and now requires durable intent ownership. This asymmetry and venue client-ID contract tests remain release-review items.

Latest source/test CI [43825962830](https://github.com/nzf210/nafaz_arb/actions/runs/43825962830) passed all four jobs for `838aac5`: Python, Go/race/vet, PostgreSQL schema plus multi-process disconnect integration, and Docker builds.

## Exposure/recovery audit continuation — 2026-11-17

**Audited branch baseline:** `b699ab279524a1fdecaa6325ef771e2d51c4195a`  
**Current remediation commits:** `c2dfdd1`, `3d32b4a`, `685bc8f`, `dc3971c`, `214bd3b`  
**Scope:** revalidation of spot BUY, futures short hedge, Indodax raw order adapter, generic mutation gateway, de-risk/exit/force-close/rollback wrappers, hedge-claim lifecycle, Global STOP signatures, and the latest cancel/recovery changes.

### Call-site classification revalidated

| Boundary | Classification | Required control |
|---|---|---|
| `place_local_limit_buy` → venue `create_limit_buy_order` | Spot entry BUY; opens exposure | Durable PREPARED/SUBMITTING + shared PostgreSQL session fence |
| `open_global_short_market` → venue `create_market_sell_order`, `reduceOnly=False` | Futures hedge SELL; opens/adds short | Durable PREPARED/SUBMITTING + shared PostgreSQL session fence |
| `MutationGateway.execute_create_order` → `create_order_direct/create_order` | Generic create boundary | Durable PREPARED/SUBMITTING + shared fence; no active production caller found |
| `place_local_limit_sell` → `create_limit_sell_order` | Spot exit | De-risk fence; not entry STOP admission |
| `place_local_market_sell` → `create_market_sell_order` | Rollback, taker exit, force-close | De-risk fence; exact-ID ambiguity handling |
| `close_global_short_market` → `create_market_buy_order`, `reduceOnly=True` | Futures close | De-risk fence; mandatory reduceOnly |
| Indodax create wrappers → `_create_order` → `POST /api/v2/order` | Raw venue boundary | Inherits the calling lifecycle classification |
| Cancel wrappers | Cancel existing order | Not create exposure; retain final-fill reconciliation |

No additional private CEX create-order endpoint was found in the modular `executor/venues/*` adapters or Go scanner/exchange code during this continuation. The direct order wrappers remain concentrated in the exchange facade and the Indodax TAPIv2 adapter. Paper-exchange create methods are simulation-only. The generic mutation gateway is initialized but has no active production call-site in the inspected executor orchestration.

### New finding

| ID | Finding | Remediation | Status |
|---|---|---|---|
| R-45 | Startup recovery could retire an older terminal spot-entry record before its lifecycle fill was reconciled. If a newer pending entry for the same pair already existed, the guard in `_finalize_terminal_entry` returned success and skipped the older order's cumulative fill. A late partial/full spot fill could therefore be dropped from hedge reconciliation. | Do not retire ENTRY records until lifecycle finalization succeeds. When a terminal entry conflicts with a newer pending entry, or fill finalization cannot complete, explicitly reopen the record as `UNVERIFIED`, keep it in the recovery index, record the pending-recovery reason, and halt new exposure for that pair. A dedicated registry transition bypasses terminal monotonic merge only for this explicit unresolved-recovery case. | FIXED; R-45 regression and current-head CI passed |

### Latest non-bypass changes reviewed

- Cancel reconciliation now performs up to three short exact-order fetches after cancel, stopping only on a terminal status. A still-open, malformed, unavailable, or not-found result keeps the pending state; it does not authorize a replacement order.
- Balance-snapshot cleanup removes stale snapshots on sizing/risk rejection paths. This is state hygiene, not an order-submit path.
- Hedge claim transitions remain bound to the exact hedge client ID. Terminal zero-fill rotates the claim's venue ID; stale callbacks for an older ID cannot abandon or confirm a newer retry.
- Futures hedge PREPARED/fence failure is distinct from the actual venue boundary via `hedge_submit_started`; LIVE no-service branches remain blocked by the explicit GlobalStop requirement.
- Global STOP method signatures were rechecked against spot, futures, and gateway callers. NOT_FOUND and lookup exceptions leave submit attempts unresolved; they are not treated as definitive rejection.
- De-risk paths use the separate de-risk fence and remain allowed under STOP. Rollback retry predicates are limited to explicit HTTP 429 and explicit venue settlement/balance rejections; timeout/reset/5xx are not retryable.

### Verification

GitHub Actions run [44044178958](https://github.com/nzf210/nafaz_arb/actions/runs/44044178958) completed **SUCCESS** for R-45 head `a3b4e937`:
- Python executor suite: **711 passed, 4 skipped, 111 warnings**.
- Disposable PostgreSQL submit-fencing integration: **4 passed**.
- VPS migration runner, schema/idempotency/RLS checks, Go tests/race/vet, and Docker builds: all five jobs passed.
- The `integration` pytest marker remains unregistered and emits warnings; it did not fail the jobs.

These results verify the R-45 commits through `a3b4e937`, not the subsequent R-46 remediation. No local VPS or live CEX test was performed.

### Remaining release gates

- Real per-venue sandbox/paper contracts: client-ID idempotency and exact-ID visibility, eventual consistency, pending/partial hedge, cancel/fill race, and reduceOnly semantics.
- Operator-authenticated resolution operational controls: verified operator identity, HMAC secret custody/rotation, dual approval, audit retrieval, and runbook.
- Repeated real PostgreSQL multi-process disconnect/fault-injection and validation against the actual VPS topology; disposable CI PostgreSQL is not VPS production sign-off.
- Explicit approval/remediation for any futures minimum-notional overhedge policy and hard-drift handling.
- Optional de-risk intents can remain Redis/best-effort when the Supabase order-registry writer is disabled; this is a documented restart-recovery trade-off.

**Status: PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.

### R-46 — Missing order registry must not ACK a recovered venue order

| ID | Finding | Remediation | Status |
|---|---|---|---|
| R-46 | `_persist_reconciled_submit_order` returned successfully when `order_registry` was `None`. Its caller then marked the durable Global STOP submit attempt `ACKNOWLEDGED`, even though the exact-client-ID venue order had not been attached to a durable lifecycle record. This contradicted the required persist-before-ACK recovery ordering. | Missing registry now raises. The existing recovery exception path records the lookup evidence but leaves the submit attempt in `SUBMITTING/UNKNOWN`; startup remains globally fenced. The successful recovery regression now supplies a fake registry, and a new fake-venue test proves that a missing registry cannot ACK the attempt. | FIXED; targeted regression and full CI passed |

**R-46 verification:** source/test head `77906cf1f650dc084ec01ab4a3ea65b6909994f8` passed GitHub Actions [44046589327](https://github.com/nzf210/nafaz_arb/actions/runs/44046589327). Python: **712 passed, 4 skipped, 111 warnings**; disposable PostgreSQL submit-fencing integration: **4 passed**; VPS migration runner, schema/idempotency/RLS, Go/race/vet, and Docker jobs all passed (five jobs total).

The first R-46 CI attempts failed because two positive recovery fixtures (including the futures lookup case) still omitted the now-required fake order registry. The fixtures were corrected to assert registry persistence before ACK; the final run above is green. The `integration` pytest marker and existing deprecation warnings remain non-fatal.
### R-47 — Recovery lookup errors were mislabeled as NOT_FOUND

| ID | Finding | Remediation | Status |
|---|---|---|---|
| R-47 | `_reconcile_durable_submit_attempts` routed every exception through `reconcile_submit_attempt_not_found`. A timeout/history endpoint failure, malformed lookup payload, missing order intent, or registry write failure could therefore be recorded as `last_lookup_result=NOT_FOUND` even though the venue lookup had not proven absence (and in some cases had actually found the order). The attempt stayed unresolved, so this was an evidence-integrity defect rather than an entry-fence release, but it could mislead manual resolution. | Only a successful exact-ID lookup returning `None` records `NOT_FOUND`. Lookup exceptions, malformed results, missing registry/intent, registry persistence failure, and ACK persistence failure are recorded as `UNKNOWN`; the durable `SUBMITTING/UNKNOWN` attempt remains globally blocking. Added fake-venue timeout and registry-write-failure regressions; retained a separate exact-NOT_FOUND test. | FIXED; full CI passed |

**R-47 verification:** source/test head `8f2d309c7d85128c30fa041bc5efc4da261eec35` passed GitHub Actions [44047469831](https://github.com/nzf210/nafaz_arb/actions/runs/44047469831). Python: **714 passed, 4 skipped, 111 warnings**; disposable PostgreSQL submit-fencing integration: **4 passed**; all five CI jobs (Go/race/vet, VPS migration runner, schema/idempotency/RLS, Docker builds, Python) passed.

The first R-47 CI attempt exposed two test/implementation follow-ups: the fake submit ledger needed an UNKNOWN transition method, and the recovery refactor had accidentally omitted `_group_recovered_positions`. Both were corrected; the final run above is green. The `integration` pytest marker and existing deprecation warnings remain non-fatal.

## Audit execution 2026-11-19 — R-50 stale spot-slot retirement during an in-flight submit

**Audited baseline:** `d3e5af62cafa03574ec46c2b631ee48543fa78e7`  
**Source fixes:** `409c730d414bddded57b65de1f8d6c3d0e4036fd`, `57d1c2cf1d783cd9e4fd7fd2b8d9f296d6bb40a9`  
**Regression tests:** `705e3751fcd8633daf81cace18be0330c432216c`, `e8b967dfa1bc9c225502c6676b1a2c9097cb0670`, `868f568d8890a133a64c6ef72993b1f844a6f455`

### Finding

- [x] R-50: `OrderSlotManager.reconcile_active_slots` released stale `RESERVED` rows unconditionally before considering active client IDs or the durable submit ledger. A spot slot remains `RESERVED` from admission until the venue acknowledgement is persisted and `activate_slot` runs.
- [x] The exchange-reconciliation loop runs independently of the entry request. If a venue POST remains in flight beyond the reservation age threshold (default 60 seconds), the sweeper could mark its reservation `RELEASED`. `activate_slot` updates only `RESERVED`, so a later acknowledgement could not restore the released slot. A subsequent entry could then exceed the configured venue/quote slot cap.
- [x] The old order also reserved a slot before committing the Global STOP `PREPARED` row, leaving a pre-submit interval in which the sweeper had no durable evidence that the reservation belonged to an admitted submit.

### Remediation

- [x] Spot entry now commits durable `PREPARED` before creating its slot reservation. If reservation/admission fails before the venue boundary, the existing exception path marks the attempt `ABORTED` and releases the reservation.
- [x] The database sweeper now releases an old `RESERVED`/`ACTIVE` reservation only when its client ID is absent from the caller's active set and no durable submit attempt for that ID is in `PREPARED`, `SUBMITTING`, or `UNKNOWN`.
- [x] A successful explicit `ABORTED`/definitive `REJECTED` transition permits stale reservation cleanup. Ambiguous attempts retain their slot.
- [x] The in-memory sweeper continues to preserve caller-supplied active client IDs; LIVE uses the PostgreSQL ledger-backed branch.

### Verification

- [x] Fake-venue regression asserts ordering: PREPARED → slot reservation → shared fence → venue create; definitive rejection releases the slot.
- [x] Disposable PostgreSQL integration ages reservations and verifies PREPARED and SUBMITTING rows remain reserved; after PREPARED is explicitly ABORTED, only that reservation is released.
- [x] Source/test head `868f568d8890a133a64c6ef72993b1f844a6f455` completed GitHub Actions workflow [44492169205](https://github.com/nzf210/nafaz_arb/actions/runs/44492169205): all five jobs successful. Python suite: **746 passed, 10 skipped, 117 warnings**. PostgreSQL schema/integration job: **11 passed**. Go tests/race/vet, migration runner, and Docker builds passed.
- [x] The branch head immediately before this execution, `d3e5af62`, also had all five CI jobs successful in workflow [44489672994](https://github.com/nzf210/nafaz_arb/actions/runs/44489672994).

### Revalidated surrounding paths

- [x] Spot BUY and futures short SELL remain the only active production entry create paths found in the previously enumerated executor/venue inventory; both require durable PREPARED/SUBMITTING and the shared PostgreSQL session advisory fence.
- [x] Indodax TAPIv2 raw `POST /api/v2/order` remains a wrapper whose exposure classification follows its caller. Generic MutationGateway create remains fenced; no active production caller was found in the inspected orchestration.
- [x] Spot SELL, futures `reduceOnly=True` BUY, rollback, force-close, and cancel remain de-risk/cancel paths. The separate de-risk lane still proceeds during Global STOP; no new entry fence was added to those operations.
- [x] The 33 commits since baseline were reviewed for execution-path changes: execution-module and hedge-venue allowlists, shared module/venue/quote slot caps, durable-intent-bound scoped fence, compatibility entry routing, and independent de-risk lock lane. No new direct CEX create-order bypass was found in those diffs.
- [x] `OrderSyncWorker.sync_once` currently has one production caller in `_exchange_reconciliation_loop`, which runs under `_exchange_reconciliation_lock`. Its `run()` worker is not scheduled by `start()`, and `maybe_sync()` has no production caller in the inspected engine. No concurrent OrderSyncWorker caller/race was established in this execution.

### Remaining release gates

- [ ] Finish a fresh, exhaustive branch-head call-site inventory across every Python/Go adapter and indirect wrapper; this execution revalidated the prior inventory and all changed execution paths, not a new local clone-wide grep (GitHub clone/DNS was unavailable).
- [ ] Complete operator identity provisioning, HMAC secret custody/rotation, dual approval, audit retrieval, and manual-resolution runbook.
- [ ] Repeat PostgreSQL multi-process disconnect/fault injection under stress and validate the actual VPS topology; disposable CI PostgreSQL is not VPS sign-off.
- [ ] Complete real per-venue sandbox/paper contracts for exact client-ID visibility/idempotency, eventual consistency, pending/partial hedge, cancel/fill race, and reduceOnly behavior.
- [ ] Approve the futures minimum-notional overhedge/delta-drift policy and close the Redis-only/best-effort de-risk persistence gap.

**Overall status: PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.


### R-50 follow-up — safe retirement of stale PREPARED intents

**Additional source fix:** `513d7f8eee066892ab665620f36121316e78d18b`  
**Final regression head:** `4e169fb3f50e9ad9693da99d07081d203402b915`

- [x] The new PREPARED-before-slot ordering exposed a second lifecycle requirement: a process can crash after PREPARED and slot reservation but before acquiring the submit fence. Such a row is provably pre-venue, but preserving PREPARED forever would strand its slot.
- [x] The slot sweeper now attempts `pg_try_advisory_xact_lock(FENCE_LOCK_KEY)` before retiring stale PREPARED attempts. This is the same advisory key used by the session-level entry fence.
- [x] If another entry currently owns the session fence, cleanup does not retire PREPARED; its slot remains reserved. If the fence is free and the PREPARED attempt is older than the slot cutoff, it is transitioned to ABORTED with a reason and resolution timestamp. A paused submitter that later resumes cannot cross the venue boundary because `order_submit_fence` requires status PREPARED.
- [x] SUBMITTING and UNKNOWN are never automatically aborted by this cleanup. They remain durable and continue to protect their slots.
- [x] PostgreSQL regression holds the shared session lock from a separate connection and verifies stale PREPARED stays reserved; after releasing the lock, stale PREPARED becomes ABORTED/released, SUBMITTING remains reserved, and an active caller ID remains reserved until it is no longer active.

**CI history:** an initial run for the extended fixture failed because the test aged reservation rows but not the corresponding submit-attempt `created_at`; the implementation correctly uses the durable attempt age. The fixture was corrected to age both records. Final head `4e169fb3` passed workflow [44493239020](https://github.com/nzf210/nafaz_arb/actions/runs/44493239020): all five jobs successful; Python **746 passed, 10 skipped, 117 warnings**; PostgreSQL integration **11 passed**; Go tests/race/vet, VPS migration runner, and Docker builds passed.

**R-50 status remains FIXED in source and regression tests.** Production VPS and real CEX validation remain open.


## Audit execution 2026-11-20 — R-51 ambiguous submit retirement and R-52 market identity

**Audited branch head before documentation:** `43163e1580cd8c48a585ddeed94e81feb0202770`

### R-51 — aged unacknowledged orders must not be auto-retired

- [x] Removed the recovery shortcut that marked a `submitting`/`pending` order `canceled/RECONCILED`, retired its registry record, and cleared pending recovery when the parent position was absent or the order was older than 180 seconds.
- [x] A successful exact-client-ID lookup returning `None` remains ambiguous. Venue history retention, propagation delay, or eventual consistency can hide an accepted order; age and missing parent state are not proof of rejection.
- [x] Recovery now retains the record in the pending-recovery index and halts the affected exposure scope. Only authoritative venue evidence or authorized operator resolution may release it.
- [x] Added fake-venue regressions for both independent conditions: old intent with parent position restored, and recent intent with parent position absent.

### R-52 — contradictory local market metadata must fail closed

The single-flight market gate previously trusted `PairInfo.base_asset` before parsing the actual local symbol. A stale signal such as `ETH_IDR` with `base_asset=BTC` could be classified as BTC/IDR, allowing the same-market check to inspect the wrong market.

- [x] Added `resolve_local_market_identity`, requiring parsed symbol base/quote to agree with explicit metadata when both are present.
- [x] Signal admission and the in-fence pre-submit guard reject contradictory or incomplete market identity.
- [x] Existing active/pending records with invalid identity on the same venue block new exposure rather than being silently skipped.
- [x] Updated copied test fixtures to keep local symbol, base asset, and quote asset consistent; added base/quote mismatch regressions.

### Revalidated order-create inventory

| Boundary | Call-site | Classification |
|---|---|---|
| `executor/exchange_orders.py` | `create_limit_buy_order` | Spot entry BUY — durable PREPARED/SUBMITTING + shared PostgreSQL session fence |
| `executor/exchange_futures.py` | `create_market_sell_order` with `reduceOnly=False` | Futures short entry/hedge — durable PREPARED/SUBMITTING + shared fence |
| `executor/exchange_orders.py` | `create_limit_sell_order` | Spot exit — de-risk lane |
| `executor/exchange_orders.py` | `create_market_sell_order` | Spot rollback/force-close — de-risk lane |
| `executor/exchange_futures.py` | `create_market_buy_order` with `reduceOnly=True` | Futures close — de-risk lane |
| `executor/indodax_v2.py` | `_create_order` → `POST /api/v2/order` | Raw venue wrapper; public BUY/SELL methods inherit caller classification |
| `executor/mutation_gateway.py` | dynamic `create_order_direct` / `create_order` | Generic create is fenced and rejects when PostgreSQL fencing is unavailable; no active production caller found in inspected orchestration |
| `executor/venues/*` | modular adapters | No direct create-order call-site found in the inspected adapters |

Spot and futures LIVE entry methods explicitly reject a missing `GlobalStopService`; engine startup also refuses LIVE mode without `POSTGRES_DSN`. Mock mode is the only compatibility path without the PostgreSQL fence. Cancel and de-risk operations remain outside the entry STOP fence.

### Verification

- Source/test head: `43163e1580cd8c48a585ddeed94e81feb0202770`.
- GitHub Actions workflow [44698742920](https://github.com/nzf210/nafaz_arb/actions/runs/44698742920): all five jobs passed.
- Python suite: **754 passed, 10 skipped, 117 warnings**.
- Disposable PostgreSQL schema/integration job: **11 passed**, including the existing multi-process submit-disconnect tests.
- Go tests, race tests, vet, VPS migration-runner tests, and scanner/executor Docker builds passed.
- Earlier intermediate runs exposed three stale test fixtures whose copied ETH/SOL symbols still carried BTC metadata. The fixtures were corrected and the final source/test head passed. This was not treated as a production test pass until the corrected head completed CI.

**Remaining release gates:** operator identity and HMAC secret governance/rotation, dual approval and manual-resolution runbook; repeated PostgreSQL multi-process disconnect/fault injection against the actual VPS topology; real per-venue sandbox/paper contracts for exact client-ID visibility and eventual consistency; approved minimum-notional overhedge/delta-drift policy; and durable de-risk persistence when Redis/registry writers are unavailable.

**Overall status: PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.


## 2026-11-21 follow-up — R-53 and R-54

### R-53 — durable submit recovery must resolve the exact venue market

EngineRecoveryMixin._reconcile_durable_submit_attempts previously called for_venue(exchange) without passing the durable attempt's market_type. This is incorrect for a venue such as MEXC that exposes both spot and futures clients. The base venue key can point to only one of those markets; a spot attempt could therefore query the configured/default client rather than the MEXC spot account (and vice versa). A wrong-client lookup could be recorded as NOT_FOUND. The attempt remained unresolved and the global entry gate stayed closed, so this was a recovery-routing defect rather than a demonstrated duplicate-entry bypass.

Fix: normalize the durable market type before resolving the client (spot/local → spot; future/futures/perpetual/swap → futures), pass that market to for_venue(exchange, market), and fail closed as UNKNOWN if a market-aware resolver or exact client is unavailable. There is no fallback to the ambiguous one-argument resolver.

### R-54 — startup spot-inventory verification undercounted shared asset requirements

_verify_recovered_spot_legs claimed to aggregate by (local venue, base asset) but its key also included pair_tag. Consequently, two active positions on the same venue and base asset were checked independently against the same total venue balance. Example: two restored routes each require 1 BTC while the venue holds 1.5 BTC; each 1 BTC check passed even though aggregate backing is short by 0.5 BTC.

Fix: aggregate required quantity by (local_exchange, base_asset), fetch the venue balance once per group, and apply a deficit to every affected global symbol. Non-finite/negative recovered quantities and non-finite/negative venue balances now fail closed.

### Explicit de-risk-only script call-sites

scripts/resolve_ai_position.py contains direct spot create_market_sell_order and futures create_market_buy_order calls. The former sells an existing spot holding; the latter sends reduceOnly=True to close an existing short. They are manual de-risk operations, not exposure-opening entry paths, and are intentionally not routed through the entry Global STOP fence. This script is not an executor production caller. Its static client IDs and lack of exact-client-ID recovery after an ambiguous submit remain a manual-operation limitation; reruns after a timeout must not be treated as safe/idempotent until venue behavior and an operator procedure are validated.

### Verification

- Source/test head: 817eec2bafac85f2b0f242b3681cc4e06574f46b.
- Workflow [44881614430](https://github.com/nzf210/nafaz_arb/actions/runs/44881614430): all five jobs succeeded, including Python executor tests, disposable PostgreSQL integration/multi-process submit-disconnect, Go tests/race/vet, migration-runner tests, and scanner/executor image builds.
- The two preceding intermediate commits had failed CI; the final source/test head above is the verified passing head.
- This does not validate the production VPS PostgreSQL topology or live CEX contracts.

Remaining gates: clone-wide/runtime-instrumented create-order inventory, operator identity and HMAC secret custody/rotation, dual approval and manual-resolution runbook, repeated PostgreSQL multi-process fault injection on the actual VPS topology, per-venue sandbox/paper exact-ID/eventual-consistency tests, minimum-notional overhedge policy, and durable de-risk persistence. Status remains PARTIAL / NOT LIVE-CERTIFIED.


## Follow-up order-path audit — 2026-11-22

Reviewed target branch head before this follow-up: `8167cefe153e5c950e4732c9120bf2c8a0f56fa8`. The follow-up source/test commits are listed below. This is a focused audit of the new generic MutationGateway path and the LIVE order-intent durability gate, not a claim that the entire repository has completed a runtime-instrumented call-site audit.

| ID | Finding | Status / evidence |
|---|---|---|
| R-55 | `EngineOrderIntentMixin._register_order_intent` treated an enabled `GlobalStopService` / `POSTGRES_DSN` as proof that the separate universal order registry was durable. When the Supabase/outbox writer was disabled, `OrderRegistryStorage._persist` could succeed from Redis alone, allowing a required LIVE order intent to pass despite the lifecycle registry not being durable. The code comment already stated that the STOP database is not the order-registry writer, but the condition contradicted it. | **FIXED** in `203d74480ee6b4c3e984ccab1a48ebc7b9b843b4`: required LIVE intents now require `supabase.enabled`; a GlobalStop connection alone no longer satisfies the registry gate. Regression test added in `1e68e26f3462a4550c69e8532e9e6bc139931124` proves Redis-only registry mode is rejected before `_register_order` is called. |
| R-56 | `MutationGateway.execute_create_order` previously submitted through the caller-supplied client without verifying that the client belonged to the exchange and market recorded in the durable intent. A mismatched client could therefore send to venue B while the ledger claimed venue A. The same entry method also accepted risk-reducing spot SELL / futures `reduceOnly` mutations, which would put exits behind the entry STOP lane. | **FIXED** in `b69d82cfe8b354c7fc71506a74661b0c23cad6e1`: resolve `for_venue(exchange, market)` when available, require the resolved client's exact `.id`, and fail before PREPARED/slot reservation on mismatch. The generic create method is now explicitly exposure-increasing only: spot SELL and futures reduceOnly are rejected from this lane and must continue through the independent de-risk path. Fake-client regressions are in `b10ae10a910eb9f7851ca26c88847496d2698802` and `e83fa8ec1ed7fa6c057e740919732455a528ee74`. |
| R-57 | The generic gateway and spot-entry pre_submit_guard previously received no durable intent scope. A no-op callback could not demonstrate which exchange/market/symbol/pair was admitted. | **FIXED (2026-11-26)** — order_submit_fence now passes immutable SubmitAdmissionContext derived from the durable PREPARED row, including a fingerprint over amount specification and recovery context; it revalidates the same context under FOR UPDATE before SUBMITTING. The live spot guard checks all scope fields. Legacy no-argument callbacks fail closed. No production generic-gateway create caller was found. |
| R-58 | The current spot single-market admission guard reads process-local state plus shared Redis active/pending keys. It does not query a canonical PostgreSQL active-market reservation. If Redis returns an empty keyspace after eviction/flush while another executor still owns an active position, a peer with empty local memory has no durable active-market proof. The global submit lock serializes requests but does not by itself reject a previously ACKNOWLEDGED order/position. | **OPEN / RESILIENCE NOT PROVEN** — add a durable cross-instance market-exposure reservation or an equivalent transactionally checked canonical gate before claiming Redis-loss safety. Do not solve this by blocking de-risking. |

### Current order-path classification

- Spot BUY and futures SELL with `reduceOnly=False` remain exposure-increasing and require durable PREPARED/SUBMITTING plus the shared PostgreSQL session advisory fence.
- Futures hedge submission remains tied to the parent spot fill and durable hedge-generation claim; it must not be treated as an unrelated new market entry or blocked by a duplicate-position guard for the parent pair.
- Spot SELL exits/rollback/force-close and futures BUY `reduceOnly=True` remain de-risking operations outside the entry STOP fence. This follow-up did not change those production paths.
- `MutationGateway.execute_create_order` has no production caller in the inspected branch paths and is not yet the sole mutation boundary. The architecture's intended gateway centralization must not be represented as completed wiring.
- Indodax TAPIv2 continues to funnel its raw order POST through `_create_order`; production entry still reaches it through the spot wrapper. No new adapter create-order caller was introduced by this follow-up.

### Verification status for this follow-up

- Source fixes: `203d7448` (LIVE registry gate) and `b69d82cf` (generic venue/risk-lane binding).
- Regression tests: `1e68e26f`, `b10ae10a`, and `e83fa8ec`.
- GitHub combined status for the earlier branch head `8167cefe` returned no status entries, and the available commit-workflow lookup returned no PR-triggered runs. Therefore no CI result is claimed for these follow-up commits until a check run is observed on the resulting branch head.
- A local clone attempt failed with `Could not resolve host: github.com`; therefore no local test execution was possible in this run.
- No merge, deployment, production migration, cutover, or live order was performed.

Remaining gates: clone-wide/runtime-instrumented create-order inventory; scoped durable active-market admission resilient to Redis loss; production wiring and lifecycle registration for MutationGateway; operator identity and HMAC secret custody/rotation; dual approval and manual-resolution runbook; repeated PostgreSQL multi-process disconnect/fault injection on the actual VPS topology; and per-venue sandbox/paper exact-ID/eventual-consistency tests. Status remains PARTIAL / NOT LIVE-CERTIFIED.


## V2 order-path follow-up — 2026-11-23

Reviewed branch source/test head: `8fffeda582f9f4b09765b22c77ffe0ecd3969f3f`.

### Order call-site inventory evidence

The recursive Git tree contains 276 files, including 102 `executor/*.py` files and 151 Python files repository-wide. The tree was enumerated and the order-bearing lifecycle, adapter, recovery, script, and test files were inspected. This is **not** a claim that every Python file has received a full local grep: a local clone remains unavailable because DNS resolution for github.com fails.

Inspected create-order surfaces and their risk classification:

- `executor/exchange_orders.py`: `place_local_limit_buy` -> spot BUY (exposure-increasing); `place_local_limit_sell` and `place_local_market_sell` -> exit/rollback/force-close (de-risking).
- `executor/exchange_futures.py`: `open_global_short_market` -> futures market SELL with `reduceOnly=False` (exposure-increasing hedge); `close_global_short_market` -> futures market BUY with `reduceOnly=True` (de-risking).
- `executor/indodax_v2.py`: limit BUY, limit SELL, and market SELL wrappers all funnel through raw `_create_order` / `POST /api/v2/order`; production entry reaches the BUY wrapper through the fenced spot lifecycle.
- `executor/mutation_gateway.py`: dynamic `create_order_direct` / `create_order` generic entry boundary. No production create caller was found in the inspected branch paths; it is not yet the sole mutation boundary.
- `scripts/resolve_ai_position.py`: direct Tokocrypto market SELL and OKX market BUY with `reduceOnly=True`; both are explicitly de-risk-only and were not moved behind the entry STOP fence.
- `executor/paper_exchange_client.py`: simulated orders only.
- The inspected `executor/venues/*` modular adapters and recovery/order-sync modules showed no direct venue create-order side effects; recovery paths inspected are lookup/adoption/reconciliation paths.

Spot BUY and futures short SELL remain required to use durable PREPARED/SUBMITTING plus the shared PostgreSQL session advisory fence. Exit, close, rollback, force-close, and cancel remain separate risk-reducing paths.

### R-58 — Historical Redis-loss admission gap (pre-reservation fix; recovery gate remains OPEN)

The code evidence confirms the previously recorded risk:

1. `EngineSignalMixin._has_active_or_pending_market` checks process memory and Redis active/pending keys; it does not read a canonical PostgreSQL active-market reservation.
2. `SupabasePositionMixin.position_opened` enqueues the position row into the local SQLite outbox. Delivery to Supabase is asynchronous and the outbox is local to each executor process/volume.
3. The inspected Supabase query API exposes durable open-order registry reads, but no canonical active-position read used by entry admission.
4. Startup restores active positions from Redis. `GlobalStopService.order_submit_fence` blocks unresolved SUBMITTING/UNKNOWN attempts, but an ACKNOWLEDGED attempt is not a durable representation of an open position.

Therefore, after Redis active/pending state is lost, a peer with empty local memory can see an empty Redis keyspace and cannot distinguish it from a genuinely flat market. If the position outbox has not yet reached Supabase, the peer has no durable active-position evidence and the current submit fence alone does not prevent a second market entry. This is a **confirmed resilience gap**, not a claim that an ordinary healthy Redis path is currently bypassing the fence.

This paragraph records the pre-fix assessment and is superseded by the 2026-11-24 durable-reservation implementation below. The branch now has a PostgreSQL local-market reservation created atomically with spot BUY SUBMITTING under the shared session advisory fence, with startup backfill from PostgreSQL positions, unresolved spot BUY attempts, open spot BUY registry rows, and restored Redis state. The remaining R-58 gate is proof of complete backfill/recovery when Redis keys and/or the process-local SQLite outbox volume are lost, plus repeated testing on the actual VPS topology. De-risking remains outside entry STOP admission.

### R-57 — generic admission callback scope FIXED (2026-11-26)

The generic gateway and active spot entry now receive an immutable SubmitAdmissionContext built from the durable PREPARED row. The callback is bound to client order ID, exchange, market type, symbol, side, order type, quantity, price, pair key and an intent fingerprint covering amount specification and recovery context. The service re-reads the intent under FOR UPDATE before SUBMITTING; a legacy no-argument callback fails closed. PostgreSQL market reservation remains the canonical cross-process uniqueness authority. No production create caller for the generic gateway was found, so this closes the API contract rather than changing the active mutation boundary.

### CI correction and evidence

- Run `45000129249` on `9ba44b1a` failed: 7 tests failed, 782 passed, 10 skipped. The failing MutationGateway tests used bare `AsyncMock` objects; dynamic attribute access made `for_venue` appear callable and return an un-awaited mock coroutine, so the strict venue resolver correctly rejected the fake before reaching the lifecycle assertions.
- Test-only correction: commit `8fffeda582f9f4b09765b22c77ffe0ecd3969f3f` introduced a strict fake venue object with an explicit `.id` and only the intended async create method.
- Workflow `45132361737` on `8fffeda`: **SUCCESS, 5/5 jobs**. Python: **789 passed, 10 skipped, 117 warnings**. PostgreSQL schema/integration: **11 passed**. Go race tests and `go vet`: passed. VPS migration-runner validation and scanner/executor Docker builds: passed.
- The PostgreSQL tests used disposable CI PostgreSQL, not the actual VPS topology. Local tests were not run because `git clone` failed with `Could not resolve host: github.com`.

No source-code behavior changed in this follow-up; only the test fake was corrected. No merge, deployment, production migration, cutover, or live order was performed. Overall status remains **PARTIAL / NOT LIVE-CERTIFIED**.


## R-59 — exact-venue recovery fallback bypass CONFIRMED / FIXED IN SOURCE

A follow-up call-chain check found that `RecoveryDiscoveryMixin._client_for_exchange()` correctly returns `None` for an unavailable/unconfigured venue, but `RecoveryRegistryMixin._fetch_order_by_client_id()` and `_fetch_order_by_role()` then fell back to `self.client` (the configured base pair facade). This undid the exact venue/market selection at the final lookup boundary. A MEXC spot/futures record with no matching exact client could therefore be queried against the default Indodax/OKX facade, producing a false NOT_FOUND or a misleading symbol error. It did not create a new order, but it violated exact-client-ID reconciliation and could delay or misclassify an already accepted order.

The minimal source fix removes both fallbacks and raises before any venue lookup when the exact exchange/market client cannot be resolved. Mock mode retains the universal paper-client fallback only; LIVE mode raises before lookup when the exact venue/market client is unavailable. A regression test covers both spot and futures paths and asserts that none of the default facade lookup methods are called. Verified by workflow `45133454526` on source/test head `cf8edbbb`: 5/5 jobs succeeded; Python 790 passed/10 skipped/117 warnings; PostgreSQL schema/integration 11 passed; Go race/vet, Docker builds, and migration-runner validation passed.


## 2026-11-23 clone-wide runtime order-call scan and R-59 verification

The production source scan is now complete for the branch tree: all 102 `executor/*.py` files were inspected with create-order patterns, all 43 non-test Go files were inspected, and the only Python utility under `scripts/` was inspected. The second utility is a shell migration runner. Tests were not counted as runtime call-sites.

Confirmed runtime create surfaces:

1. `executor/exchange_orders.py`: spot BUY limit; spot SELL limit; spot SELL market. BUY is entry; SELLs are exit/rollback/force-close.
2. `executor/exchange_futures.py`: futures SELL market with `reduceOnly=False` opens the short hedge; futures BUY market with `reduceOnly=True` closes it.
3. `executor/indodax_v2.py`: public limit BUY, limit SELL, and market SELL wrappers converge on raw `_create_order` / `POST /api/v2/order`.
4. `executor/mutation_gateway.py`: generic dynamic `create_order_direct` / `create_order` boundary. No production create caller was found in the inspected call graph; it is not yet the only mutation boundary.
5. `scripts/resolve_ai_position.py`: direct Tokocrypto market SELL and OKX market BUY with `reduceOnly=True`; both are explicitly de-risk-only.
6. `executor/paper_exchange_client.py`: simulated orders only. `executor/venues/*` contains no direct create-order implementation. `exchange_common.py` uses private open-order listing endpoints only. Recovery, adoption, order-sync, and reconciliation modules contain no create-order side effects.
7. The 43 production Go files contain no create-order call-site; the Go component is the scanner/API/Telegram side, not the CEX executor.

All exposure-increasing CEX call-sites are therefore the canonical spot BUY and futures short SELL wrappers plus the currently uncalled generic gateway. Spot/futures de-risk, cancel, and the one-off manual unwind remain outside the entry STOP fence.

R-59 final behavior: LIVE recovery now raises if exact (exchange, market) resolution returns no client and never queries the default pair client. Only mock/paper mode may use the venue-neutral simulator fallback. The first attempt (workflow `45133192762`) exposed three legacy paper-mode fixtures that relied on that simulator fallback; commit `cf8edbbb` restricted the fallback to `mock_mode` and kept LIVE fail-closed. Final workflow `45133454526` succeeded 5/5.

Remaining open: R-58 Redis/outbox-loss recovery and reservation backfill completeness; operator-authenticated manual resolution governance; real VPS PostgreSQL multi-process disconnect/fault injection; and venue sandbox contract verification. Full runtime call-site source inventory is now closed for this tree, but this does not certify venue behavior or live readiness.


## Durable market exposure reservation update — 2026-11-24

| ID | Finding | Remediation / evidence | Status |
|---|---|---|---|
| R-58 | Redis-only active/pending market admission could miss an existing local spot exposure after key loss or process restart. The Global STOP submit fence serialized requests but did not retain market ownership after ACK. | Added the PostgreSQL executor_market_exposure_reservations table with a unique active (local_exchange, base_asset, quote_asset) claim. Spot BUY creates the claim in the same PostgreSQL transaction as PREPARED -> SUBMITTING while the shared session advisory fence is held. Startup backfill scans active PostgreSQL positions, unresolved spot BUY attempts, open spot BUY registry rows, plus Redis-restored positions/pending entries. Verified exit/force-close releases only after both legs are venue-verified flat; definitive rejection releases its own claim. Ambiguous submit, ACK persistence failure, Redis loss, and database disconnect retain the claim. | IMPLEMENTED AND CI-VERIFIED on baseline `bfc69454`; disposable PostgreSQL multi-process submit-disconnect passed in workflow `45568229845`. Actual VPS topology and legacy/outbox-loss proof remain OPEN. |

**Safety boundary:** this reservation applies only to exposure-increasing spot BUY. Spot SELL, futures reduceOnly=true, cancel, rollback, force-close, and other de-risking operations remain outside the entry STOP fence. Reservation release is post-close metadata work; a release failure leaves the market blocked and does not undo a completed de-risk action.

The branch remains NOT LIVE-CERTIFIED. Operator-authenticated manual resolution, real VPS PostgreSQL disconnect/fault injection, and venue contract verification remain outstanding.


## 2026-11-25 follow-up — R-60 generic futures admission and R-61 atomic rejection cleanup

**Audited branch head:** `bfc69454c729e57b403a8d8e48e5845c0b47d23b`  
**Source/test remediation head:** `6d211b0d21bd3074c5a32d84add4054ccae610d7`

### R-60 — generic gateway could open futures exposure outside the hedge-generation lifecycle

The generic `MutationGateway.execute_create_order` accepted futures SELL and futures BUY when `reduceOnly` was false. It persisted PREPARED/SUBMITTING and used the global fence, but it did not own the durable parent spot-fill hedge-generation claim enforced by `EntryHedgeMixin`. A future caller could therefore create a naked futures short or long through the generic path. No production create caller was found; this was an exposed API boundary, not an observed live caller bypass.

**Fix:** generic futures exposure-increasing creates now fail closed before PREPARED, slot reservation, or venue I/O. Futures reduceOnly remains rejected to the independent de-risk path. The only enabled LIVE futures entry is `EntryHedgeMixin._submit_entry_hedge_order`, which binds the order to the parent spot fill and durable hedge-generation claim. Generic spot BUY remains protected by the PostgreSQL local-market reservation created atomically with SUBMITTING. The intent-scoped SubmitAdmissionContext guard is now verified; the PostgreSQL market reservation remains the canonical market uniqueness control.

### R-61 — definitive rejection and market-reservation release were not atomic

`mark_submit_rejected` previously committed the submit-attempt transition and released the reservation in a second database operation. A process crash between them could leave a REJECTED intent with an ACTIVE reservation indefinitely. This was a stale-claim/liveness defect, not an exposure-opening bypass.

**Fix:** submit status transition and reservation release now execute in one PostgreSQL transaction. Callers already inside the submit fence reuse its yielded connection; post-boundary recovery callers acquire one connection for both writes. If reservation release fails, the REJECTED transition rolls back too. A fault-injection regression covers rollback and successful paired commit.

### Verification and remaining gates

- Baseline workflow [45568229845](https://github.com/nzf210/nafaz_arb/actions/runs/45568229845) on `bfc69454` succeeded 5/5 jobs. Python: **809 passed, 11 skipped**. Disposable PostgreSQL integration: **12 passed**, including backend termination during a fake venue request and peer blocking with durable `SUBMITTING` / `ACTIVE` reservation.
- R-60/R-61 source/test head `6d211b0d` completed workflow [45571240202](https://github.com/nzf210/nafaz_arb/actions/runs/45571240202) **SUCCESS, 5/5 jobs**: Python **811 passed, 11 skipped, 11 warnings**; disposable PostgreSQL integration **12 passed**; Go tests/race/vet, Docker builds, and migration-runner checks passed. Documentation head `45910f22` also completed [45571374728](https://github.com/nzf210/nafaz_arb/actions/runs/45571374728) **SUCCESS, 5/5 jobs**.
- R-57 FIXED: the generic gateway and spot-entry guard now use durable-intent-scoped SubmitAdmissionContext; legacy no-argument callbacks fail closed. No production generic-gateway create caller was found.
- Operator resolution has HMAC signature, nonce/replay protection, two distinct evidence references, state precondition, and append-only audit persistence. Operator identity/provisioning, secret custody/rotation, dual approval, audit retrieval, and runbook remain OPEN.
- Disposable PostgreSQL multi-process disconnect/fault injection is CI-verified, but repeated testing against the actual VPS PostgreSQL topology and legacy/outbox-loss recovery remain OPEN.
- Per-venue sandbox/paper exact-client-ID idempotency/visibility, eventual consistency, pending/partial hedge, cancel/fill race, and reduceOnly semantics remain OPEN.
- Minimum-notional overhedge/delta-drift policy and durable de-risk persistence during Redis/registry-writer outage remain OPEN.

**Overall status: PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, cutover, or live order was performed.


### R-62 — manual de-risk utility retry is not exact-client-ID reconciled

`scripts/resolve_ai_position.py` is deliberately outside the entry STOP fence: it submits a Tokocrypto spot market SELL and an OKX futures market BUY with `reduceOnly=True`. It uses fixed client IDs (`resolveaispotv1`, `resolveaifuturesv1`) and, before a new POST, does not perform an exact-client-ID lookup to adopt a prior order. It fetches by exchange order ID only after receiving an acknowledgement that contains an ID. A timeout/lost acknowledgement followed by rerunning the utility can therefore repeat the same client-ID create request; whether each configured venue treats that as idempotent has not been proven by sandbox contract tests.

**No source change was made:** these are de-risk-only operations and must not be moved behind the entry STOP fence. Until venue idempotency/exact-ID semantics are verified and the utility has a guarded retry/resolution flow, operators must inspect the venue before rerunning after any ambiguous result. Spot SELL remains bounded by the current free balance; futures BUY remains `reduceOnly=True`, so this is not an entry-lane bypass, but duplicate close semantics remain unverified.

### Manual STOP/operator authentication boundary

`resolve_submit_attempt_signed` verifies HMAC, timestamp, nonce, two distinct evidence references, unresolved state, and writes an audit row. The signed payload's `operator_id` is still a claimed string; there is no identity-provider binding, dual approval, or secret custody/rotation workflow in this repository. `acknowledge_resume` accepts an operator string and expected epoch but has no signature check and no production command caller was found. Do not expose either API as an operator-authenticated workflow until those controls are implemented.

## 2026-11-26 follow-up — R-57 intent-scoped admission contract

**Finding:** the optional pre-submit callback was invoked with no arguments before the durable intent was loaded. This allowed a callback to return True without receiving any verifiable order scope. The PostgreSQL reservation was already the canonical uniqueness control, so this was an admission-contract/auditability defect rather than a proven production reservation bypass.

**Fix:**
- SubmitAdmissionContext is built from the durable PREPARED row while the shared session advisory lock is held.
- The context includes client order ID, exchange, market type, symbol, side, order type, quantity, price, pair key, and a SHA-256 fingerprint covering amount_spec and recovery_context.
- The callback receives that context. The row is then re-read FOR UPDATE; any field/fingerprint change rejects before SUBMITTING.
- The active spot-entry guard compares the full context with the entry being submitted before consulting active/pending pair state.
- A legacy no-argument callback fails closed and leaves PREPARED; fake-gateway and disposable PostgreSQL integration fixtures now exercise the scoped contract.

**Scope:** no de-risk path was changed. Generic futures create remains disabled; no production create caller for the generic gateway was found.

**Remaining release gates:** R-58 Redis/outbox-loss recovery and reservation backfill completeness; operator identity/authentication, dual approval and secret custody; repeated fault injection on the actual VPS PostgreSQL topology; per-venue sandbox idempotency/visibility/eventual-consistency contracts; manual de-risk exact-client-ID retry resolution.

**Overall status: PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, cutover, or live order was performed.

### Verification — R-57 scoped admission source/test head

Source/test commit `a5d7c05f8a217c565a91dc007874dcb1e83c4f79` passed GitHub Actions workflow [45777304013](https://github.com/nzf210/nafaz_arb/actions/runs/45777304013), **5/5 jobs SUCCESS**. Python executor suite: **812 passed, 11 skipped, 11 warnings**. Disposable PostgreSQL schema/integration job: **12 passed**, including the real PostgreSQL multi-process submit-disconnect scenario. Go tests, Go race tests, Go vet, scanner/executor Docker builds, and isolated VPS migration-runner validation all passed. The pytest warnings are unregistered `integration` marks; no test failed. This is CI evidence, not repeated testing against the actual VPS PostgreSQL topology or real CEX sandbox certification.
