<!-- Dokumentasi proyek ini ditulis dalam Bahasa Indonesia. Nama berkas, simbol kode, nama fungsi, nama API, dan istilah teknis baku dipertahankan agar dapat dicari di repositori. -->

# File-Level Architecture Inventory

- Repositori: `nzf210/nafaz_arb`
- Base: `feat/unified-dynamic-fee-engine` @ `9cc3dcd11d0999e828309053ba4cffce98a28675`
- Cabang audit: `audit/architecture-baseline-regression-contract`
- Pohon repositori rekursif: 213 berkas terlacak, not truncated.
- Purpose: file-by-file change boundary for the architecture work.

## How to read this inventory

- **Kode sumber telah diperiksa** means the file was fetched from the exact baseline commit and examined for its architectural responsibility. It does not imply a runtime test was run.
- **Inventaris / cakupan saja** means the file exists in the baseline tree and is assigned a preliminary preservation/change policy from its role/name. It wajib receive detailed review before implementation touches it.
- **PERTAHANKAN** protects current behavior; it does not mean the file is bug-free.
- **UBAH** means pertahankan behavior and public contracts while changing implementation.
- Tidak ada source file was modified by this audit.

## Complete tracked-file matrix

| File | Decision | Review depth | Contract / reason |
|---|---|---|---|
| `.dockerignore` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Deployment/feature flags/CI wajib support staged rollout |
| `.github/workflows/test.yml` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Deployment/feature flags/CI wajib support staged rollout |
| `.gitignore` | PERTAHANKAN | Inventaris / cakupan saja | Baseline dokumentasi/perkakas |
| `Dockerfile` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Deployment/feature flags/CI wajib support staged rollout |
| `Dockerfile.executor` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Deployment/feature flags/CI wajib support staged rollout |
| `LICENSE` | PERTAHANKAN | Inventaris / cakupan saja | Baseline dokumentasi/perkakas |
| `PODMAN.md` | PERTAHANKAN | Inventaris / cakupan saja | Baseline dokumentasi/perkakas |
| `README.md` | PERTAHANKAN | Inventaris / cakupan saja | Baseline dokumentasi/perkakas |
| `cache/indodax_spot_markets.json` | PERTAHANKAN / VERIFIKASI | Inventaris / cakupan saja | Periksa sebelum mengubah |
| `cmd/nafaz-scanner-outbox/main.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan opportunity contract; improve WS coverage/telemetry |
| `cmd/nafaz-scanner/main.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan opportunity contract; improve WS coverage/telemetry |
| `config.example.json` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Deployment/feature flags/CI wajib support staged rollout |
| `data/.gitkeep` | PERTAHANKAN / VERIFIKASI | Inventaris / cakupan saja | Periksa sebelum mengubah |
| `docker-compose.yml` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Deployment/feature flags/CI wajib support staged rollout |
| `docs/AUDIT-FINDINGS.md` | REFERENSI | Inventaris / cakupan saja | Perbarui jika kontrak arsitektur berubah |
| `docs/EXPOSURE-RECONCILIATION.md` | REFERENSI | Inventaris / cakupan saja | Perbarui jika kontrak arsitektur berubah |
| `docs/GAP-01-03-IMPLEMENTATION.md` | REFERENSI | Inventaris / cakupan saja | Perbarui jika kontrak arsitektur berubah |
| `docs/IMPROVEMENT-PLAN.md` | REFERENSI | Inventaris / cakupan saja | Perbarui jika kontrak arsitektur berubah |
| `docs/RISK-STATE-PERSISTENCE.md` | REFERENSI | Inventaris / cakupan saja | Perbarui jika kontrak arsitektur berubah |
| `docs/UNIFIED-FEE-ENGINE.md` | REFERENSI | Inventaris / cakupan saja | Perbarui jika kontrak arsitektur berubah |
| `executor/__init__.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/adaptive_maker.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/cancel.py` | PERTAHANKAN | Kode sumber telah diperiksa | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/config.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/engine.py` | PERTAHANKAN | Kode sumber telah diperiksa | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/engine_common.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/engine_entry_flow.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/engine_entry_sizing.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/engine_entry_submission.py` | PERTAHANKAN | Kode sumber telah diperiksa | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/engine_fee_preflight.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/engine_order_intents.py` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan state contract; add durable command/coordination or penjadwal integration |
| `executor/engine_pemulihan.py` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan state contract; add durable command/coordination or penjadwal integration |
| `executor/engine_runtime.py` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan state contract; add durable command/coordination or penjadwal integration |
| `executor/engine_signal.py` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan state contract; add durable command/coordination or penjadwal integration |
| `executor/engine_status.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/entry.py` | PERTAHANKAN | Kode sumber telah diperiksa | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/entry_common.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/entry_hedge.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/entry_monitoring.py` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan state contract; add durable command/coordination or penjadwal integration |
| `executor/entry_orders.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/exchange_client.py` | UBAH | Kode sumber telah diperiksa | Private API boundary; penjadwal integration without changing lifecycle semantics |
| `executor/exchange_common.py` | UBAH | Kode sumber telah diperiksa | Private API boundary; penjadwal integration without changing lifecycle semantics |
| `executor/exchange_fee_balance.py` | UBAH | Kode sumber telah diperiksa | Private API boundary; penjadwal integration without changing lifecycle semantics |
| `executor/exchange_futures.py` | UBAH | Kode sumber telah diperiksa | Private API boundary; penjadwal integration without changing lifecycle semantics |
| `executor/exchange_orders.py` | UBAH | Kode sumber telah diperiksa | Private API boundary; penjadwal integration without changing lifecycle semantics |
| `executor/exchange_trades.py` | UBAH | Kode sumber telah diperiksa | Private API boundary; penjadwal integration without changing lifecycle semantics |
| `executor/exit_execution.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/exit_orders.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/exit_pricing.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/exits.py` | PERTAHANKAN | Kode sumber telah diperiksa | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/exposure_rekonsiliasi.py` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan state contract; add durable command/coordination or penjadwal integration |
| `executor/fee_engine.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/force_close.py` | PERTAHANKAN | Kode sumber telah diperiksa | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/force_close_legs.py` | PERTAHANKAN | Kode sumber telah diperiksa | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/force_close_lifecycle.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/helpers.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/indodax_v2.py` | UBAH | Kode sumber telah diperiksa | Private API boundary; penjadwal integration without changing lifecycle semantics |
| `executor/main.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/market_models.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/models.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/order_models.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/order_pemulihan.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/order_registry.py` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan state contract; add durable command/coordination or penjadwal integration |
| `executor/order_registry_common.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/order_registry_pemulihan.py` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan state contract; add durable command/coordination or penjadwal integration |
| `executor/order_registry_storage.py` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan state contract; add durable command/coordination or penjadwal integration |
| `executor/pair_registry.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/paper_exchange_client.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/position_models.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/private_stream.py` | PERTAHANKAN | Kode sumber telah diperiksa | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/quantity.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/pemulihan_adoption.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/pemulihan_discovery.py` | UBAH | Kode sumber telah diperiksa | Private API boundary; penjadwal integration without changing lifecycle semantics |
| `executor/pemulihan_orphans.py` | PERTAHANKAN | Kode sumber telah diperiksa | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/pemulihan_registry.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/pemulihan_utils.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/risk.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/risk_exposure.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/risk_funding.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/risk_state.py` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan state contract; add durable command/coordination or penjadwal integration |
| `executor/rollback.py` | PERTAHANKAN | Kode sumber telah diperiksa | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/simulator.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/supabase.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/supabase_common.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/supabase_orders.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/supabase_outbox.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/supabase_positions.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/supabase_queries.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/supabase_risk.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/supabase_trades.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/supabase_pekerja.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/trade_fill_rekonsiliasi.py` | UBAH | Kode sumber telah diperiksa | Private API boundary; penjadwal integration without changing lifecycle semantics |
| `executor/trade_models.py` | PERTAHANKAN | Inventaris / cakupan saja | Trading/pemulihan feature; regression protected, no rewrite |
| `executor/venues/__init__.py` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Retain venue quirks; add capability/penjadwal contract |
| `executor/venues/base.py` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Retain venue quirks; add capability/penjadwal contract |
| `executor/venues/indodax.py` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Retain venue quirks; add capability/penjadwal contract |
| `executor/venues/mexc.py` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Retain venue quirks; add capability/penjadwal contract |
| `executor/venues/okx.py` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Retain venue quirks; add capability/penjadwal contract |
| `executor/venues/registry.py` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Retain venue quirks; add capability/penjadwal contract |
| `executor/venues/tokocrypto.py` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Retain venue quirks; add capability/penjadwal contract |
| `go.mod` | PERTAHANKAN + VERIFIKASI | Inventaris / cakupan saja | Dependency pinning and CCXT API surface wajib be verified |
| `go.sum` | PERTAHANKAN + VERIFIKASI | Inventaris / cakupan saja | Dependency pinning and CCXT API surface wajib be verified |
| `internal/api/server.go` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan API/ledger contracts; additive changes only |
| `internal/api/server_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/config/config.go` | PERTAHANKAN / VERIFIKASI | Inventaris / cakupan saja | Periksa sebelum mengubah |
| `internal/config/config_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/domain/domain.go` | PERTAHANKAN / VERIFIKASI | Inventaris / cakupan saja | Periksa sebelum mengubah |
| `internal/exchange/adapter.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/adapter_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/exchange/funding_pekerja.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/funding_pekerja_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/exchange/health.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/health_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/exchange/indodax_limiter.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/indodax_resilience_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/exchange/indodax_ws.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live_books.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live_dns.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live_funding.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live_perp.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live_spot.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/live_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/exchange/mock.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/okx_books_ws.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/okx_books_ws_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/exchange/okx_limiter.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/okx_limiter_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/exchange/venue_indodax.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/venue_mexc.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/venue_okx.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/venue_tokocrypto.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/venues.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/venues_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/exchange/ws_book.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/ws_book_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/exchange/ws_client.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/ws_decode.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/exchange/ws_streams.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan venue adapter; unify market-data lifecycle and public request policy |
| `internal/history/store.go` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan API/ledger contracts; additive changes only |
| `internal/history/store_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/redishandler/client.go` | UBAH | Kode sumber telah diperiksa | Pertahankan signal semantics; add durable command path separately |
| `internal/redishandler/client_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/scanner/evaluate.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan opportunity contract; improve WS coverage/telemetry |
| `internal/scanner/exchange_health_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/scanner/hybrid_filter_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/scanner/liquidity.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan opportunity contract; improve WS coverage/telemetry |
| `internal/scanner/liquidity_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/scanner/service.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan opportunity contract; improve WS coverage/telemetry |
| `internal/scanner/service_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/scanner/validation.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan opportunity contract; improve WS coverage/telemetry |
| `internal/scanner/validation_freshness_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/supabase/client.go` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan API/ledger contracts; additive changes only |
| `internal/supabase/client_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/supabase/outbox.go` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan API/ledger contracts; additive changes only |
| `internal/supabase/outbox_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/telegram/balances_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/telegram/bot.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan pengalaman operator; emergency commands become durable |
| `internal/telegram/bot_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/telegram/clean_orphan_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/telegram/formatters.go` | PERTAHANKAN + UBAH | Inventaris / cakupan saja | Pertahankan pengalaman operator; emergency commands become durable |
| `internal/telegram/handlers.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan pengalaman operator; emergency commands become durable |
| `internal/telegram/monitor.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan pengalaman operator; emergency commands become durable |
| `internal/telegram/monitor_test.go` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `internal/telegram/positions.go` | PERTAHANKAN + UBAH | Kode sumber telah diperiksa | Pertahankan pengalaman operator; emergency commands become durable |
| `pytest.ini` | PERTAHANKAN | Inventaris / cakupan saja | Baseline dokumentasi/perkakas |
| `requirements.txt` | PERTAHANKAN + VERIFIKASI | Inventaris / cakupan saja | Dependency pinning and CCXT API surface wajib be verified |
| `session-files/20261108-okx-ratelimit-note.md` | REFERENSI | Inventaris / cakupan saja | Perbarui jika kontrak arsitektur berubah |
| `session.db` | PERTAHANKAN / VERIFIKASI | Inventaris / cakupan saja | Periksa sebelum mengubah |
| `supabase/README.md` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan kontrak skema/migrasi tahan lama; hanya migrasi tambahan |
| `supabase/config.toml` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan kontrak skema/migrasi tahan lama; hanya migrasi tambahan |
| `supabase/migrations/20261107000100_create_nafaz_core_schema.sql` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan kontrak skema/migrasi tahan lama; hanya migrasi tambahan |
| `supabase/migrations/20261109000100_add_scanner_opportunity_idempotency.sql` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan kontrak skema/migrasi tahan lama; hanya migrasi tambahan |
| `supabase/migrations/20261111000100_add_live_order_fill_rekonsiliasi.sql` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan kontrak skema/migrasi tahan lama; hanya migrasi tambahan |
| `supabase/migrations/20261111000200_add_durable_risk_state.sql` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan kontrak skema/migrasi tahan lama; hanya migrasi tambahan |
| `supabase/migrations/20261111000300_add_idempotent_risk_pnl_events.sql` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan kontrak skema/migrasi tahan lama; hanya migrasi tambahan |
| `supabase/migrations/20261111000400_add_rekonsiliasi_audit_trail.sql` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan kontrak skema/migrasi tahan lama; hanya migrasi tambahan |
| `supabase/migrations/20261111000500_add_universal_order_registry.sql` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan kontrak skema/migrasi tahan lama; hanya migrasi tambahan |
| `supabase/migrations/20261112000100_add_venue_client_order_id.sql` | PERTAHANKAN | Inventaris / cakupan saja | Pertahankan kontrak skema/migrasi tahan lama; hanya migrasi tambahan |
| `supabase/tests/schema_smoke.sql` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/conftest.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_adaptive_maker.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_adaptive_maker_paper_validation.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_clients.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_config.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_distributed_pemulihan_lock.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_entry.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_exchange_fee_provider.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_exchange_market_resolution_pemulihan.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_exchange_order_id_normalization.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_exits.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_exposure_rekonsiliasi.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_fee_engine.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_force_clean_orphan.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_force_close.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_indodax_v2.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_mexc_futures_balance.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_order_persistence_guarantees.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_order_pemulihan.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_pair_registry.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_pnl.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_probe9.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_production_failure_injection.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_quantity.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_rate_limit_resilience.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_pemulihan.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_risk.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_signal_priority.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_stream_cache.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_supabase_writer.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_trade_fill_rekonsiliasi.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_two_leg_execution_pemulihan.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |
| `tests/test_venues_modular.py` | PERTAHANKAN | Inventaris / cakupan saja | Bukti pengujian regresi yang sudah ada; perluas, jangan mengganti |

## Aturan peninjauan wajib

Sebelum mengubah baris mana pun yang ditandai **Inventaris / cakupan saja**, periksa seluruh isi berkas serta pemanggil dan pengujian langsungnya pada komit target yang tepat. Update this inventory with the reviewed status and any refined decision in the same PR. Untuk setiap berkas kode sumber baru, tambahkan satu baris. Untuk setiap berkas yang dihapus atau diganti nama, cantumkan pemetaan nama lama ke nama baru dalam deskripsi PR.

## Urutan ketergantungan kritis

1. `executor/exchange_client.py`, `executor/exchange_orders.py`, `executor/exchange_futures.py`, `executor/exchange_trades.py`, `executor/exchange_fee_balance.py`, `executor/exchange_common.py`, `executor/indodax_v2.py` — batas API privat bursa.
2. `executor/engine_order_intents.py`, `executor/order_registry.py`, `executor/order_registry_pemulihan.py`, `executor/order_registry_storage.py` — identitas order tahan lama.
3. `executor/engine_pemulihan.py`, `executor/pemulihan_discovery.py`, `executor/pemulihan_orphans.py`, `executor/trade_fill_rekonsiliasi.py`, `executor/exposure_rekonsiliasi.py` — pemulihan and verification.
4. `executor/engine_runtime.py`, `executor/engine_signal.py`, `executor/entry_monitoring.py`, `executor/entry_orders.py`, `executor/exit_orders.py`, `executor/rollback.py`, `executor/force_close*.py` — order orchestration and emergency handling.
5. `internal/exchange/*.go`, `internal/scanner/*.go`, `cmd/nafaz-scanner/main.go` — public market-data path.
6. `internal/redishandler/client.go`, `internal/telegram/*.go`, `internal/api/*.go` — operator/control plane.
7. `supabase/migrations/*.sql`, `internal/supabase/*.go`, `executor/supabase*.py` — durable ledger and outbox.

## Hal yang secara tegas bukan tujuan

- Tidak ada strategy rewrite.
- Tidak ada fee formula rewrite.
- Tidak ada change to maker/taker selection or adaptive maker algorithm.
- Tidak ada removal of the current pemulihan or registry implementation.
- Tidak ada switch of durable source of truth from Supabase/Postgres to Redis.
- Tidak ada LIVE enablement or production credential changes.
