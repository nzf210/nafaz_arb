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
