<!-- Dokumentasi proyek ini ditulis dalam Bahasa Indonesia. Nama berkas, simbol kode, nama fungsi, nama API, dan istilah teknis baku dipertahankan agar dapat dicari di repositori. -->

# Baseline Arsitektur — Nafazbot

- Repositori: `nzf210/nafaz_arb`
- Cabang acuan: `feat/unified-dynamic-fee-engine`
- Komit acuan: `9cc3dcd11d0999e828309053ba4cffce98a28675`
- Cabang audit: `audit/architecture-baseline-regression-contract`
- Tanggal audit: 2026-11-13
- Cakupan: pemeriksaan kode sumber hanya-baca; dokumentasi saja. Tidak ada kode trading, konfigurasi, migrasi, atau sumber daya produksi yang diubah.

## 1. Tujuan dan batas pengamanan

This document freezes the current architectural baseline before implementation of shared exchange infrastructure. It distinguishes:
- **PERTAHANKAN** — pertahankan behavior and interfaces; changes require explicit regression proof.
- **UBAH** — improve implementation while preserving the externally observable contract.
- **BARU** — add a separate capability behind an adapter/port; do not replace the old path until parity is demonstrated.
- **TUNDA** — not part of this architecture work.
- **VERIFIKASI** — source evidence is incomplete or venue/version dependent; do not claim support until verified.

Aturan yang tidak boleh dilanggar:
1. Tidak ada rewrite of the strategy/execution state machine as part of gateway or market-data work.
2. Never dual-submit a create-order operation through old and new paths.
3. A create-order timeout is **SUBMISSION_UNKNOWN**, not proof of rejection; reconcile by venue/ID klien before any retry.
4. A missing order from an incomplete/unsupported history endpoint is not proof of absence.
5. Unknown wallet, order, or position state is not healthy/flat.
6. Pertahankan independent Spot dan Futures leg accounting and pemulihan.
7. Keep Supabase/Postgres as durable ledger; Redis is coordination/cache/notification, not the only order record.
8. Roll out hanya-baca first, then cancel/pemulihan, then create-order only after idempotency and ambiguity tests.
9. Existing REST fallback remains until a per-venue/per-symbol WS feed is proven LIVE and fresh. Stale/gapped books cannot create signals.
10. Tidak ada production mutation from this audit branch.

## 2. Potret repositori

Cabang diperiksa dari GitHub at the commit above. Recursive tree contained **213 tracked paths**, with no truncation. Ini adalah inventaris kode sumber, bukan pernyataan bahwa seluruh 213 berkas telah diaudit baris demi baris. Critical runtime paths are inspected; secondary files are classified by responsibility and marked for deeper verification in the implementation plan.

Pesan komit kepala cabang adalah:
`fix(executor): support USDT spot quotes and resolve queue stall bottlenecks`.

Fakta penting kode saat ini:
- Python executor builds CCXT async clients with `enableRateLimit=True`; this is per CCXT instance, not a shared cross-process limiter.
- Indodax Spot uses a custom TAPIv2 adapter with its own `aiohttp.ClientSession` and HMAC-SHA256 request signing.
- Balance-only client inventory includes Tokocrypto Spot, Indodax Spot, OKX Futures, MEXC Futures, Binance Futures, and Bybit Futures.
- The Go scanner's exchange adapter exposes public market discovery, books, ticker/FX, and funding data; no authenticated create/cancel/order/balance/position path was found in the inspected Go exchange adapter. Go exchange API traffic in this baseline is public market-data traffic.
- Executor private stream cache exists, but the compose comment says no publisher/subscriber is wired to populate it yet. Jangan treat it as an operational private WS feed.
- Redis Pub/Sub is used for emergency cancel/force close notifications; Pub/Sub alone is not durable delivery.
- Signal priority queue uses Redis sorted-set pop; it is an ephemeral opportunity queue, not an execution command journal.
- Pair serialization in the Python engine is process-local; pemulihan has a distributed Redis lock. They are different protections.

## 3. Baseline arsitektur saat berjalan

```text
Public exchange data pasar
  CEX REST + public WebSocket
        |
        v
Go scanner / market discovery / local books / funding
        |
        v
Redis opportunity payload + priority ZSET + notify Pub/Sub
        |
        v
Python executor (signal validation, risk, sizing, maker/hedge, exits)
        |
        +--> ExchangeClient facade
        |      +--> CCXT async clients (venue-specific)
        |      +--> Indodax TAPIv2 direct HTTP adapter
        |      +--> extra hanya-baca balance clients
        |
        +--> Universal Order Registry / Supabase durable records
        +--> Redis operational state, locks, queues, Pub/Sub
        +--> pemulihan / exposure / fill rekonsiliasi pekerjas
        |
        v
Telegram Go API reads Redis/Supabase state and sends operator commands
```

The architecture is currently hybrid: Go owns scanner/data pasar publik and Telegram service; Python owns execution, private exchange access, pemulihan, risk, and order lifecycle.

## 4. Matriks pertahankan / ubah / bangun

| Component | Decision | Baseline contract |
|---|---|---|
| Go scanner and opportunity evaluation | PERTAHANKAN + UBAH | Pertahankan signal payload semantics, profitability/fee checks, idempotency, stale-data rejection. Add WS coverage without changing strategy formulas. |
| Exchange pair/market normalization | PERTAHANKAN + UBAH | Persisted scanner venue tags and native IDs wajib continue resolving to exact venue market; ambiguous mapping fails closed. |
| Python execution engine orchestration | PERTAHANKAN | Pertahankan entry, hedge, maker, exit, rollback, force-close, and pemulihan state transitions. |
| ExchangeClient facade | UBAH | Keep current call interface during penjadwal migration; make every private request pass through a shared policy boundary. |
| CCXT clients | PERTAHANKAN initially | Keep adapter behavior; per-instance limiter is not sufficient as global control. |
| Indodax TAPIv2 | PERTAHANKAN | Pertahankan signature, endpoints, decimal formatting, status normalization, post-only/TIF behavior, and error classification. Add penjadwal transport injection, not a rewrite. |
| Venue modules | PERTAHANKAN + UBAH | Retain Tokocrypto client-ID mapping, Indodax history pagination, OKX position-side params, MEXC swap balance params. Add capability metadata. |
| Universal Order Registry | PERTAHANKAN | Durable order identity and lifecycle remain canonical. |
| Durable order intent | PERTAHANKAN | Persist intent before LIVE create order. Persistence failure blocks submission. |
| Two-leg execution/pemulihan | PERTAHANKAN | Spot dan Futures progress remain independent. |
| Risk state/circuit breaker | PERTAHANKAN | Unknown state blocks new exposure; durable restore remains mandatory. |
| Trade fill rekonsiliasi | PERTAHANKAN + UBAH | Immutable venue fill IDs and cumulative-fill cross-check remain required; route reads through penjadwal. |
| Exposure rekonsiliasi | PERTAHANKAN + UBAH | Keep actual balance/position verification and fail-closed mismatch handling. |
| Adaptive Maker Pegging | PERTAHANKAN | Pertahankan strategy and Indodax-specific behavior; regression tests required for every exchange transport change. |
| Fee engine | PERTAHANKAN | Pertahankan fee source/verification semantics and net-profit calculation. |
| Telegram command UX | PERTAHANKAN | Pertahankan commands and operator semantics; route all private reads/commands through executor penjadwal/journal. |
| Redis opportunity queue | PERTAHANKAN | Ephemeral signals remain TTL-bounded; do not repurpose as durable execution queue. |
| Emergency Pub/Sub | UBAH | Keep as wake-up only; add durable command journal and acknowledgement. |
| PrivateStreamCache | PERTAHANKAN + BARU producer | Pertahankan cache API/freshness fallback; add actual venue private-stream ingestion separately. |
| Shared API privat penjadwal | BARU | Cross-pekerja/cross-process request policy, rate buckets, cooldown, priority, bounded retries, telemetry. |
| Unified market-data subscription manager | BARU incrementally | Per-symbol lifecycle, sequence/freshness/resync, feed coverage and REST budget. Existing venue managers adapt into contract. |
| Distributed execution lease | BARU/UBAH | Extend beyond pemulihan lease; pair/account ownership, fencing token, atomic order-slot reservation. |
| Durable emergency command journal | BARU | Persist, claim, acknowledge, retry, idempotent command IDs. |
| Full Go migration of Python executor | TUNDA | Tidak ada justification until staged gateway proves value and parity. |

## 5. Baseline kemampuan bursa

Kemampuan dipisahkan menjadi **scanner public market-data support**, **executor private trading route**, and **auxiliary balance client registration**. Ketiganya tidak dapat saling menggantikan.

| CEX | Go scanner public markets/books | Python executor private route evidenced | Auxiliary balance registry | Status |
|---|---|---|---|---|
| Tokocrypto | Spot market discovery, Spot REST order book; WS order-book coverage not established | Spot create limit buy/sell, market sell, cancel, order lookup, open-order listing, client-ID pemulihan, private trades, balance, account fee | Spot | Partial full-trade path; verifikasi all CCXT endpoint variants and WS |
| Indodax | Spot market discovery, Spot REST order book, dedicated public book WS and USDT/IDR ticker WS | TAPIv2 Spot create/cancel/order lookup/client-ID lookup/order terbuka/terminal history/myTrades/account balance; post-only limit support | Spot | Strong custom adapter; verifikasi live endpoint semantics and WS |
| OKX | Perpetual SWAP discovery, order book REST + dedicated public WS, funding rate | Futures market sell short, reduceOnly buy close, positions, order lookup/order terbuka/client-ID pemulihan, private trades, balance, funding history/rate, ticker | Futures | Executor path evidenced; verifikasi account position mode and CCXT version |
| MEXC | Spot and perpetual discovery, Spot/Futures REST order book and funding | Futures market sell/buy, positions, order lookup/order terbuka/client-ID pemulihan, private trades, balance using `type=swap`, funding methods when supported | Futures | Executor path evidenced; verifikasi swap-specific endpoint/capability |
| Binance | Tidak ada scanner market route established in this audit | Tidak ada dedicated active pair route established; generic CCXT path may be selected by config | Futures | Auxiliary balance client only evidenced; trading support NOT certified |
| Bybit | Tidak ada scanner market route established in this audit | Tidak ada dedicated active pair route established; generic CCXT path may be selected by config | Futures | Auxiliary balance client only evidenced; trading support NOT certified |

**Catatan penting:** `BALANCE_MARKETS` proves auxiliary balance-client registration, not end-to-end order execution/pemulihan support. Generic CCXT method availability does not prove the account, market type, client-ID, position mode, pagination, or endpoint is production-ready. Tidak ada CEX sebaiknya be marked LIVE-supported until capability tests pass.

### Scanner public data coverage
- Tokocrypto Spot: REST order-book fetch path.
- Indodax Spot: dedicated `IndodaxBookManager` with dynamic pair subscriptions.
- OKX Futures: dedicated `OKXBookManager` with instrument subscriptions/snapshot pemulihan.
- MEXC Spot/Futures: REST book paths; a generic WS stream exists but complete per-symbol book lifecycle was not established.
- Tidak ada private order/account API is identified in the Go scanner exchange adapters examined.

## 6. Batas rancangan penjadwal permintaan bersama

The penjadwal wajib not become a new trading engine. It owns transport admission/control only:
- identity scope: `venue + credential/account + market/API category + endpoint bucket`;
- request classes: emergency write, pemulihan read, order monitor, position/balance risk read, order write, analytics/fill history, fee/metadata;
- venue-specific weight/cost and concurrency;
- shared cooldown, Retry-After, exponential backoff with jitter, circuit state;
- bounded queues and deadlines;
- read deduplication only for semantically identical safe reads;
- telemetry and trace/request IDs;
- no blind retries of non-idempotent create order;
- no request is silently dropped; caller receives typed timeout/rejection/deferred result.

Transport adapters:
- CCXT: wrap authenticated methods at the ExchangeClient/transport boundary; do not monkey-patch undocumented internals without tests.
- Indodax: inject a penjadwal-aware request executor into TAPIv2 `_request`.
- Go public scanner: separate public data limiter; do not share private credential bucket with executor.

## 7. Batas pengelola data pasar

Normalized book contract wajib include venue, market type, native symbol, bid/ask levels, exchange timestamp, local receive timestamp, sequence/update ID where available, source, state, and freshness.
State machine: `UNSUBSCRIBED -> SYNCING -> LIVE`; any stale timestamp, sequence gap, disconnect, checksum failure, or malformed payload transitions to `STALE/GAP/FAILED` and blocks signal generation until resync.
REST budget is explicit per venue and purpose: initial snapshot, resync, metadata, and controlled fallback. A generic socket-connected flag is not proof that every symbol is live.

## 8. Batas kepemilikan eksekusi

Add a distributed lease for normal execution separate from pemulihan lock:
- key scope includes account/credential and canonical pair;
- lease owner and monotonically increasing fencing token;
- lease heartbeat and bounded TTL;
- every state-changing command validates current fencing token;
- pending/open Spot order slot reservation is atomic per CEX/account;
- lock loss blocks new order writes and triggers safe rekonsiliasi;
- pertahankan existing local asyncio lock as an optimization, never as distributed correctness.

## 9. Risiko yang diketahui / hal yang masih perlu diverifikasi

1. Exact authenticated endpoint weights and burst limits wajib be verified against current official docs for each enabled CEX and account tier.
2. CCXT version and generated endpoint names wajib be pinned and capability-tested.
3. Tokocrypto private fallback uses generated CCXT methods `privateGetOpenV1Orders` and `privateGetOpenV1AccountSpot`; verifikasi exact response shape and order type semantics against the installed CCXT version.
4. Indodax TAPIv2 order/history/myTrades endpoint behavior, timestamp windows, and post-only semantics require live-safe or sandbox/recorded-fixture verification.
5. OKX `posSide`, `reduceOnly`, position mode, and ID klien behavior wajib be tested against actual account mode.
6. MEXC `defaultType=swap`, `type=swap`, order symbol resolution, and position normalization require venue-specific tests.
7. Binance/Bybit are not certified for trading by balance registry presence.
8. Private WS credentials, login, subscription, reconnect, deduplication, and event-to-registry mapping are not yet evidenced as complete.
9. Multi-executor normal pair ownership is not proven distributed.
10. Recovery sweep pagination and account-wide endpoint semantics require per-venue tests.
11. Exact API privat call coverage wajib be re-scanned whenever new direct exchange method or raw HTTP use is added.

## 10. Kebijakan perubahan

Every future PR touching exchange access wajib:
- update `PRIVATE_API_CALL_MAP.md` if a private endpoint/callsite changes;
- update `REGRESSION_CONTRACT.md` if behavior or test obligations change;
- demonstrate no bypass around penjadwal;
- include mock/fixture tests and venue capability test;
- pertahankan old path behind explicit rollout flag until parity is demonstrated;
- include rollback procedure and operational metrics.
