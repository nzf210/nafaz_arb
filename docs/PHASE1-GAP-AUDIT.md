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
| 1. Gross, estimated net, maker/taker fee, slippage, FX | **Ada tetapi parsial/bermasalah** | Scanner: `internal/scanner/evaluate.go:calculate`, `internal/scanner/liquidity.go`, `internal/domain/domain.go:Opportunity`. Executor: `executor/config.py:trade_fee_rates/estimate_trade_fees_usdt`, `executor/exits.py`, `executor/force_close.py`; tests `tests/test_pnl.py`, `tests/test_exits.py`, `tests/test_force_close.py`. | Scanner menghitung gross dan estimated net setelah VWAP/slippage, fee maker/taker, funding dan normalisasi FX. FX IDR kini memakai midpoint bid/ask live Indodax; last trade/cached FX tidak dipakai. **Biaya spread/konversi FX eksplisit belum dimodelkan**. Executor memisahkan gross, estimated fees dan estimated net; realized PnL aktual belum didukung karena fee aktual per fill belum direkonsiliasi. Fee profile tetap input operator, bukan authenticated account tier. |
| 2. Order-book depth dan VWAP sesuai ukuran | **Sudah ada dan berfungsi pada level unit test** | `internal/scanner/liquidity.go`; `internal/scanner/evaluate.go`; `internal/scanner/liquidity_test.go` (thin/deep books, required notional, VWAP/imbalance). | Ukuran disesuaikan dengan depth yang tersedia dan slippage dihitung dari level buku. Belum membuktikan kualitas depth live pada semua venue. |
| 3. Saldo Spot dan margin Futures | **Sudah ada dan berfungsi pada level adapter/unit test** | `executor/risk.py`; `executor/exchange_client.py:get_balances`; `tests/test_risk.py`. | Membaca saldo tersedia, menghentikan entry bila data saldo gagal, dan menerapkan fee buffer. |
| 3. Fee buffer per venue | **Sudah ada pada branch audit; perlu konfigurasi akun** | `executor/config.py:estimate_round_trip_fee_fraction`; `executor/risk.py`; `tests/test_pnl.py`. | Profil per venue meng-override default scanner. Nilai harus diisi sesuai fee tier akun yang sebenarnya. |
| 3. Open orders dan posisi eksternal/manual | **Parsial; scanner tetap fail-closed** | Scanner: `internal/exchange/live_funding.go:LiveAdapter.OpenExposures`, `internal/scanner/service.go:loadExposures/pairPositionReason`; executor: `executor/exchange_client.py:fetch_positions`; tests `internal/exchange/live_test.go:TestLiveAdapterOpenExposuresFailsClosed`, `internal/scanner/service_test.go:TestScanMarksEveryCandidateIneligibleWhenExposureStateIsUnknown`, `tests/test_recovery.py`. | Executor kini dapat membaca posisi Futures per symbol melalui CCXT dan menormalkan contract units ke base units. Namun belum ada reader account-wide yang menggabungkan open orders Spot/Futures dan posisi manual/eksternal lintas venue. Scanner `OpenExposures` tetap mengembalikan error agar tidak menyatakan akun flat secara palsu; dengan `check_open_positions=true`, kandidat LIVE diblokir sampai reader account-wide tersedia. |
| 3. Minimum notional, lot size, tick size, contract multiplier | **Ada tetapi parsial; preflight fail-closed pada branch audit** | `executor/exchange_client.py:validate_entry_order`, `_resolve_symbol`, `_to_contract_amount`, `_to_spot_amount`, `fetch_positions`; `executor/engine.py` memanggil preflight sebelum Spot submit; tests `tests/test_clients.py` dan `tests/test_entry.py`. | CCXT market catalog di-refresh; preflight mensyaratkan market aktif, Spot/linear Perp, quote Futures USDT/USDC, min amount/notional, amount/price precision, dan jumlah Spot yang tepat merupakan kelipatan contract size. Order/fill/position Futures dikonversi ke/dari base units. Jika metadata/limit tidak tersedia atau ukuran tidak dapat dihedge tepat, entry diblokir. **Belum diuji dengan metadata live akun di semua venue**; market yang tidak menyediakan `limits.cost.min` akan fail-closed. |
| 4. Revalidasi harga sebelum order | **Ada tetapi parsial; diperkuat pada branch audit** | `executor/engine.py:_validate_entry_spread`; `executor/exchange_client.py:fetch_local_ticker/fetch_global_ticker`; `tests/test_entry.py`. | Branch audit mewajibkan ticker Spot dan Futures bertimestamp, maksimum umur 2 detik, FX valid, spread minimum (bila diaktifkan), dan menolak limit buy yang menyentuh ask. Ini masih belum menggantikan market-status/order-constraint preflight. |
| 4. Freshness dan timestamp skew order book | **Sudah ada, dengan keterbatasan sumber timestamp** | `internal/scanner/validation.go:validateOrderBookAge/validateCrossExchangeTimestampAlignment`; `internal/scanner/service.go:normalizeOrderBookCurrency`; `internal/scanner/validation_freshness_test.go`. | Default age/skew dikonfigurasi 5s/3s. Indodax/Tokocrypto depth API tidak memberi timestamp sumber yang dapat dipercaya; receive time hanya membuktikan respons baru diterima, bukan bahwa server tidak menyajikan cache lama. Metadata receive time harus dipertahankan saat normalisasi FX. |
| 4. Status market/maintenance sebelum submit | **Ada tetapi parsial; diperkuat pada branch audit** | `executor/exchange_client.py:validate_entry_order/_refresh_markets`; `internal/exchange/live_spot.go`, `live_perp.go`, `internal/exchange/health.go`. | Executor me-refresh CCXT market catalog dan menolak market yang tidak dapat dikonfirmasi aktif sebelum Spot order. Discovery scanner masih memakai cache market hingga 1 jam dan Indodax pair endpoint tidak menyertakan status trading per pair; preflight executor menjadi gerbang terakhir. Wallet/account status authenticated dan maintenance E2E belum terverifikasi. |
| 5. Two-leg state machine, partial fill, actual fill sizing | **Sudah ada dan berfungsi pada level unit test** | `executor/engine.py`, `executor/entry.py`, `executor/exits.py`, `executor/models.py`; tests `tests/test_entry.py`, `tests/test_exits.py`. | Fill Spot kumulatif dihedge berdasarkan delta fill; order/position state dipersist ke Redis. |
| 5. Rollback, rekonsiliasi, recovery | **Sudah ada dan berfungsi pada level unit test** | `executor/rollback.py`, `executor/force_close.py`, `executor/engine.py:_recover_pending_entry_orders/_verify_recovered_active_positions`; tests `tests/test_recovery.py`, `tests/test_force_close.py`. | Status unknown/partial dipertahankan sebagai `RECONCILIATION_REQUIRED`; futures flat diverifikasi sebelum posisi dihapus. Belum diuji dengan akun live tiap venue. |
| 6. Telegram: API/order failure, saldo, unhedged, partial fill | **Ada tetapi perlu verifikasi end-to-end** | `executor/engine.py:_publish_execution_failure`, `executor/risk.py`, `internal/telegram/handlers.go:broadcastTradeEvent`; `internal/telegram/bot_test.go`. | Event dikirim lewat Redis Pub/Sub; akses tetap bergantung pada bot token/chat allowlist dan konektivitas Telegram. |
| 6. Telegram ringkasan scan | **Parsial / opt-in** | `config.example.json:telegram.alert_on_scan_summary`; `internal/telegram` scan summary handler. | Default `false`; operator harus mengaktifkannya. |
| 7. Tes sukses, timeout, reject, partial fill, saldo kurang, recovery | **Sudah ada pada level unit/integration mock** | `tests/test_entry.py`, `tests/test_risk.py`, `tests/test_recovery.py`, `tests/test_force_close.py`, `internal/scanner/*_test.go`. | Belum ada bukti E2E live untuk seluruh kombinasi enam venue, fee tier, precision, market maintenance, dan partial fills aktual. |
| Supabase schema dan runtime persistence | **Schema ada; runtime masih PR terpisah** | Migrations `supabase/migrations/20261107000100_create_nafaz_core_schema.sql` dan `20261108000100_add_explicit_pnl_status.sql`; smoke tests `supabase/tests/schema_smoke.sql`/`rls_smoke.sql`; runtime repository di PR #11 (`executor/supabase_repository.py`, `internal/supabase/client.go`). | Schema/RLS/grants/trigger diuji di CI. PR #11 runtime persistence terbuka dan CI-nya lulus, tetapi belum terintegrasi ke `main`/branch audit. Redis tetap koordinasi live; jangan menjadikan Supabase wajib bagi hedge/rollback. Kolom realized PnL hanya boleh diisi setelah fill/fee direkonsiliasi. |

## Strategi exchange yang dipertahankan

- Local Spot: **Indodax dan Tokocrypto**.
- Global Perpetual target: **MEXC, OKX, Binance, Bybit**.
- **MEXC Spot tidak digunakan**.
- Futures quote assets yang ditargetkan: **USDT dan USDC**. USDC masih memakai asumsi 1:1 terhadap USDT untuk sizing/normalisasi; depeg risk belum dihilangkan.
- Spot IDR/BIDR tidak boleh diproses tanpa live USDT/IDR. Branch audit juga menolak FX last-known untuk local Spot non-IDR karena executor mengirim harga IDR-normalized.
- Pada `main` saat audit, discovery Perpetual aktif baru OKX/MEXC. Binance/Bybit berada di PR #6, bukan pada `main`. PR #6 saat ini `diverged`, 80 commits ahead/54 behind dan `mergeable=false`; tidak diintegrasikan otomatis. Executor pada branch audit mengonfigurasi satu local + satu global client per proses, belum empat global venue sekaligus.

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
2. Preflight generic CCXT untuk market active, min notional, amount/price precision, lot/tick dan contract multiplier sudah ditambahkan pada branch audit. Sebelum venue diaktifkan, verifikasi bahwa CCXT mengembalikan metadata/limits yang benar untuk akun dan symbol aktual; jika tidak, entry tetap diblokir.
3. Rebase/review PR #6 terhadap `main`; jangan mengaktifkan Binance/Bybit sebelum CI dan payload live diverifikasi. PR #6 saat audit tertinggal dari `main` dan tidak mergeable.
4. Isi fee profile sesuai fee tier akun sebenarnya. Fee aktual per fill masih belum direkonsiliasi; semua angka PnL executor harus tetap berlabel estimate.
5. Uji Supabase migration pada project non-production dan bangun idempotent runtime persistence/outbox; jangan menjadikan DB wajib bagi hedge/rollback.
6. Jalankan paper scenarios lengkap dan kemudian supervised low-balance smoke test hanya setelah prasyarat di atas lulus. Tidak ada order live yang dikirim dalam audit ini.

## Hasil tes

Hasil tes final untuk head branch ini akan dicatat setelah GitHub Actions selesai. Kehadiran test source tidak dianggap sebagai bukti tes lulus. Minimal CI yang wajib hijau: Python suite, `go test ./...`, `go vet ./...`, Supabase migration/RLS smoke, dan Docker build.
## Audit follow-up — 2026-11-08

### Gap yang berhasil ditutup pada branch audit
- **FX benar-benar fresh:** `LiveAdapter.USDTIDRRate` tidak lagi memakai cache last-known dan kini memakai midpoint bid/ask Indodax. Jika salah satu sisi bid/ask hilang, rate ditolak. `MockAdapter` menyediakan rate deterministik hanya untuk MOCK/test.
- **CCXT symbol/contract handling:** raw exchange IDs dipetakan ke unified CCXT symbols; Futures quantity dikonversi dari base asset ke contract units dan order fills/positions dinormalisasi kembali ke base units. `open_global_short_market` menerima reference `price` yang dipakai entry mixin.
- **Entry order preflight:** LIVE entry wajib memuat ulang market catalog dan memverifikasi market aktif, tipe Spot/linear perpetual, quote USDT/USDC, minimum amount/notional kedua leg, price/amount precision, dan exact contract multiple. Jika metadata tidak lengkap, ukuran tidak kompatibel, atau adapter tidak memiliki preflight, Spot order tidak dikirim.
- **Regression tests:** mencakup live FX bid/ask vs last trade, no-cache fallback, metadata market hilang/inactive, contract multiplier, symbol resolution, normalized position quantities, dan LIVE entry diblokir saat preflight gagal.

### Gap yang sengaja belum diubah
- **Authenticated account-wide open orders/positions:** `LiveAdapter.OpenExposures` tetap fail-closed. Tidak ada reader authenticated yang menyatukan open orders dan posisi manual/eksternal di Indodax, Tokocrypto, MEXC, OKX, Binance, dan Bybit. Jangan menonaktifkan `check_open_positions` untuk melewati blocker ini.
- **Enam venue sekaligus:** PR #6 masih terbuka dan diverged dari `main`; jangan cherry-pick/merge massal tanpa review konflik. Pada `main` scanner hanya discovery OKX/MEXC; executor saat ini satu local + satu global client per proses.
- **Actual realized PnL:** fee aktual per fill, rebate, fee asset conversion dan funding settlement lintas venue belum direkonsiliasi menyeluruh. Nilai PnL tetap `estimated`/`unverified`.
- **Market metadata live:** preflight hanya sebaik metadata CCXT. Belum ada bukti payload market/limits aktual dari akun produksi untuk seluruh venue. Missing limits sengaja memblokir order.
- **FX/USDC basis:** FX IDR menggunakan midpoint USDT/IDR; spread/biaya konversi tidak dimodelkan. USDC masih diasumsikan 1:1 terhadap USDT untuk normalisasi.
- **Live end-to-end:** tidak ada order live, secrets tidak disentuh, dan belum ada E2E akun real. Unit/mocks/CI bukan bukti fill aktual atau latensi venue.

### Branch/PR
- Audit: `audit/phase1-gap-hardening-20261108`, PR #10 ke `main`; **tidak di-merge**.
- Six CEX: PR #6 masih open, diverged (80 ahead / 54 behind), tidak mergeable.
- Supabase runtime: PR #11 masih open, CI lulus pada head yang diaudit, belum di-merge.
