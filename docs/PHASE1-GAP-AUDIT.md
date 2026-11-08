# NAFAZ ARB — Audit Gap Tahap 1

**Tanggal:** 2026-11-08  
**Basis audit:** `main`, feature branch `audit/phase1-gap-hardening-20261108`, dan PR #6 `feat/local-spot-global-perpetual-six-cex`.  
**Batasan:** audit statis + tes repository; tidak mengirim order live, tidak membaca/mengubah secrets, dan tidak mengubah konfigurasi produksi.

## Kesimpulan

**Belum READY untuk live trading.** Banyak fondasi Tahap 1 sudah tersedia: kalkulasi opportunity net, VWAP depth, pemeriksaan saldo, state machine dua leg, recovery, dan notifikasi dasar. Gap yang masih menghalangi live adalah pembacaan open orders/positions akun secara authenticated, validasi market metadata dan order constraints tepat sebelum submit, integrasi exchange enam venue ke `main`, serta rekonsiliasi fee/fill aktual.

Perubahan pada branch audit ini sengaja fail-closed bila data FX atau exposure tidak dapat diverifikasi. Ini dapat menghentikan sinyal LIVE; itu disengaja sampai prasyarat keselamatan tersedia.

## Matriks rencana vs kondisi aktual

| Rencana Tahap 1 | Status | Bukti kode / tes | Catatan |
|---|---|---|---|
| 1. Gross, estimated net, maker/taker fee, slippage, FX | **Ada tetapi parsial/bermasalah** | Scanner: `internal/scanner/evaluate.go:calculate`, `internal/scanner/liquidity.go`, `internal/domain/domain.go:Opportunity`. Executor: `executor/config.py:trade_fee_rates/estimate_trade_fees_usdt`, `executor/exits.py`, `executor/force_close.py`; tests `tests/test_pnl.py`, `tests/test_exits.py`, `tests/test_force_close.py`. | Scanner menghitung estimasi net setelah VWAP/slippage, fee dan FX. Executor sekarang memisahkan gross, estimated fees dan estimated net berdasarkan profil fee per venue; status PnL ditandai `estimated` atau `unverified`. **Realized PnL aktual belum didukung** karena komisi aktual tiap fill belum direkonsiliasi dari exchange. Profil fee masih input operator, bukan fee tier authenticated. |
| 2. Order-book depth dan VWAP sesuai ukuran | **Sudah ada dan berfungsi pada level unit test** | `internal/scanner/liquidity.go`; `internal/scanner/evaluate.go`; `internal/scanner/liquidity_test.go` (thin/deep books, required notional, VWAP/imbalance). | Ukuran disesuaikan dengan depth yang tersedia dan slippage dihitung dari level buku. Belum membuktikan kualitas depth live pada semua venue. |
| 3. Saldo Spot dan margin Futures | **Sudah ada dan berfungsi pada level adapter/unit test** | `executor/risk.py`; `executor/exchange_client.py:get_balances`; `tests/test_risk.py`. | Membaca saldo tersedia, menghentikan entry bila data saldo gagal, dan menerapkan fee buffer. |
| 3. Fee buffer per venue | **Sudah ada pada branch audit; perlu konfigurasi akun** | `executor/config.py:estimate_round_trip_fee_fraction`; `executor/risk.py`; `tests/test_pnl.py`. | Profil per venue meng-override default scanner. Nilai harus diisi sesuai fee tier akun yang sebenarnya. |
| 3. Open orders dan posisi eksternal/manual | **Belum ada; sekarang fail-closed** | `internal/exchange/live_funding.go:LiveAdapter.OpenExposures`; `internal/scanner/service.go:loadExposures/pairPositionReason`; tests `internal/exchange/live_test.go:TestLiveAdapterOpenExposuresFailsClosed` dan `internal/scanner/service_test.go:TestScanMarksEveryCandidateIneligibleWhenExposureStateIsUnknown`. | Adapter LIVE sebelumnya mengembalikan slice kosong walau tidak membaca akun. Branch audit mengubahnya menjadi error agar tidak menyatakan akun flat secara palsu. Dengan `check_open_positions=true`, scanner LIVE akan memblokir kandidat sampai authenticated reader tersedia. |
| 3. Minimum notional, lot size, tick size, contract multiplier | **Ada tetapi parsial** | Scanner market metadata: `internal/domain/domain.go:Market`, adapter discovery di `internal/exchange/live_spot.go` dan `live_perp.go`; executor memakai `local_lot_size`/`local_price_tick`. PR #6: `executor/exchange_client.py:_contract_size/_to_contract_amount`. | Contract multiplier dikonversi pada jalur futures PR #6. Pemeriksaan min-notional, amount/price precision dan market-active tepat sebelum submit belum lengkap/terverifikasi untuk semua CCXT venue. |
| 4. Revalidasi harga sebelum order | **Ada tetapi parsial; diperkuat pada branch audit** | `executor/engine.py:_validate_entry_spread`; `executor/exchange_client.py:fetch_local_ticker/fetch_global_ticker`; `tests/test_entry.py`. | Branch audit mewajibkan ticker Spot dan Futures bertimestamp, maksimum umur 2 detik, FX valid, spread minimum (bila diaktifkan), dan menolak limit buy yang menyentuh ask. Ini masih belum menggantikan market-status/order-constraint preflight. |
| 4. Freshness dan timestamp skew order book | **Sudah ada, dengan keterbatasan sumber timestamp** | `internal/scanner/validation.go:validateOrderBookAge/validateCrossExchangeTimestampAlignment`; `internal/scanner/service.go:normalizeOrderBookCurrency`; `internal/scanner/validation_freshness_test.go`. | Default age/skew dikonfigurasi 5s/3s. Indodax/Tokocrypto depth API tidak memberi timestamp sumber yang dapat dipercaya; receive time hanya membuktikan respons baru diterima, bukan bahwa server tidak menyajikan cache lama. Metadata receive time harus dipertahankan saat normalisasi FX. |
| 4. Status market/maintenance sebelum submit | **Ada tetapi parsial** | `internal/exchange/health.go`, `live_spot.go`, `live_perp.go`; `internal/scanner/evaluate.go`. | Scanner memeriksa venue health dan metadata market saat discovery. Executor belum memverifikasi ulang status market aktif pada saat submit. Wallet/account status adapter juga belum menjadi pemeriksaan authenticated yang otoritatif. |
| 5. Two-leg state machine, partial fill, actual fill sizing | **Sudah ada dan berfungsi pada level unit test** | `executor/engine.py`, `executor/entry.py`, `executor/exits.py`, `executor/models.py`; tests `tests/test_entry.py`, `tests/test_exits.py`. | Fill Spot kumulatif dihedge berdasarkan delta fill; order/position state dipersist ke Redis. |
| 5. Rollback, rekonsiliasi, recovery | **Sudah ada dan berfungsi pada level unit test** | `executor/rollback.py`, `executor/force_close.py`, `executor/engine.py:_recover_pending_entry_orders/_verify_recovered_active_positions`; tests `tests/test_recovery.py`, `tests/test_force_close.py`. | Status unknown/partial dipertahankan sebagai `RECONCILIATION_REQUIRED`; futures flat diverifikasi sebelum posisi dihapus. Belum diuji dengan akun live tiap venue. |
| 6. Telegram: API/order failure, saldo, unhedged, partial fill | **Ada tetapi perlu verifikasi end-to-end** | `executor/engine.py:_publish_execution_failure`, `executor/risk.py`, `internal/telegram/handlers.go:broadcastTradeEvent`; `internal/telegram/bot_test.go`. | Event dikirim lewat Redis Pub/Sub; akses tetap bergantung pada bot token/chat allowlist dan konektivitas Telegram. |
| 6. Telegram ringkasan scan | **Parsial / opt-in** | `config.example.json:telegram.alert_on_scan_summary`; `internal/telegram` scan summary handler. | Default `false`; operator harus mengaktifkannya. |
| 7. Tes sukses, timeout, reject, partial fill, saldo kurang, recovery | **Sudah ada pada level unit/integration mock** | `tests/test_entry.py`, `tests/test_risk.py`, `tests/test_recovery.py`, `tests/test_force_close.py`, `internal/scanner/*_test.go`. | Belum ada bukti E2E live untuk seluruh kombinasi enam venue, fee tier, precision, market maintenance, dan partial fills aktual. |
| Supabase schema dan runtime persistence | **Schema ada; runtime parsial/belum terintegrasi** | `supabase/migrations/20261107000100_create_nafaz_core_schema.sql`, `supabase/tests/schema_smoke.sql`, `supabase/tests/rls_smoke.sql`; migration baru `20261108000100_add_explicit_pnl_status.sql`. | RLS/grants/trigger diuji di CI. Redis tetap menjadi koordinasi live dan history scanner masih file-backed; runtime writes ke Supabase belum terintegrasi. Kolom realized PnL disediakan nullable, tetapi tidak diisi sebelum rekonsiliasi actual fill/fee. |

## Strategi exchange yang dipertahankan

- Local Spot: **Indodax dan Tokocrypto**.
- Global Perpetual target: **MEXC, OKX, Binance, Bybit**.
- **MEXC Spot tidak digunakan**.
- Futures quote assets yang ditargetkan: **USDT dan USDC**. USDC masih memakai asumsi 1:1 terhadap USDT untuk sizing/normalisasi; depeg risk belum dihilangkan.
- Spot IDR/BIDR tidak boleh diproses tanpa live USDT/IDR. Branch audit juga menolak FX last-known untuk local Spot non-IDR karena executor mengirim harga IDR-normalized.
- Pada `main` saat audit, discovery Perpetual aktif baru OKX/MEXC. Binance/Bybit berada di PR #6, bukan pada `main`.

## Perubahan gap yang dikerjakan pada branch audit

1. Mempertahankan `ReceivedAt` dan `Source` saat normalisasi FX order book; membandingkan receive timestamp Spot dan Perp dari masing-masing buku.
2. Menolak penggunaan FX last-known untuk harga order Spot yang dinormalisasi ke IDR.
3. Membuat live exposure inspection gagal-terbuka menjadi fail-closed; kandidat tidak eligible ketika status akun tidak diketahui.
4. Memuat fee profiles per venue pada executor, menghitung Spot maker/taker + Perp taker + surcharge, dan memisahkan gross/estimated fee/estimated net.
5. Menghapus static FX fallback dari normal exit PnL. Posisi tetap dapat ditutup/dihedge, tetapi PnL ditandai unverified dan tidak dimasukkan ke total PnL bila FX tidak tersedia.
6. Menambah validasi ticker Spot dan Futures bertimestamp sebelum entry; zero profit-floor tidak menonaktifkan freshness gate.
7. Menambah kolom Supabase gross/estimated/reconciled PnL dan smoke test.
8. Memperjelas notifikasi Telegram untuk PnL estimated vs unverified dan memperbaiki dokumentasi role exchange.

## Risiko dan prasyarat yang masih terbuka

1. Implementasikan authenticated open-orders + positions readers untuk setiap venue yang akan diaktifkan. Jangan menonaktifkan `check_open_positions` sebagai workaround.
2. Tambahkan preflight market active, min notional, amount/price precision, lot/tick dan contract multiplier di jalur executor untuk setiap venue.
3. Rebase/review PR #6 terhadap `main`; jangan mengaktifkan Binance/Bybit sebelum CI dan payload live diverifikasi. PR #6 saat audit tertinggal dari `main` dan tidak mergeable.
4. Isi fee profile sesuai fee tier akun sebenarnya. Fee aktual per fill masih belum direkonsiliasi; semua angka PnL executor harus tetap berlabel estimate.
5. Uji Supabase migration pada project non-production dan bangun idempotent runtime persistence/outbox; jangan menjadikan DB wajib bagi hedge/rollback.
6. Jalankan paper scenarios lengkap dan kemudian supervised low-balance smoke test hanya setelah prasyarat di atas lulus. Tidak ada order live yang dikirim dalam audit ini.

## Hasil tes

Hasil tes final untuk head branch ini akan dicatat setelah GitHub Actions selesai. Kehadiran test source tidak dianggap sebagai bukti tes lulus. Minimal CI yang wajib hijau: Python suite, `go test ./...`, `go vet ./...`, Supabase migration/RLS smoke, dan Docker build.
