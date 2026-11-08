# Rencana Perbaikan: Scanner & Executor
**Dibuat:** 2026-11-03  
**Berdasarkan:** [AUDIT-FINDINGS.md](./AUDIT-FINDINGS.md)  
**Status historis:** Fase 0 (P0), Fase 1 (P1), Fase 2 (P2) & Fase 3 (P3) telah dikerjakan pada audit 2026-11-03. **Status terkini: NOT READY FOR LIVE TRADING**; audit ulang Tahap 1 pada 2026-11-08 menemukan blocker baru/pasca-refactor. Lihat [PHASE1-GAP-AUDIT.md](./PHASE1-GAP-AUDIT.md) untuk status aktual.

---

## Progres

| Fase | Progres |
|---|---|
| Fase 0 — Critical Bugfix | ✅ 5/5 selesai |
| Fase 1 — High Priority | ✅ 5/5 (+ BUG-10) |
| Fase 2 — Medium Priority | ✅ 6/6 selesai |
| Fase 3 — Low Priority | ✅ 5/5 selesai |

---

## Filosofi Perbaikan

1. **Urutan prioritas ketat** — P0 harus selesai sebelum P1, P1 sebelum P2.
2. **Tidak ada breaking change tanpa test** — setiap perubahan disertai unit/integration test.
3. **Mock mode harus tetap berjalan** — perbaikan live tidak boleh merusak paper trading.
4. **Tidak ada live trading** hingga semua P0 dan P1 selesai dan semua tes hijau.

---

## FASE 0 — Critical Bugfix (Wajib sebelum live)

> Target: Sistem tidak crash dan tidak menyebabkan loss of funds.
>
> **Status: ✅ SELESAI** — semua item diverifikasi lewat `tests/test_executor.py` (seluruh suite: 31 tes hijau).

### P0-01 — Rollback/emergency sell spot jika global short hedge gagal

**Status:** ✅ Selesai

**File:** `executor/engine.py:_execute_entry_hedge`, `_rollback_hedge_failure`

**Implementasi:**
- `_rollback_hedge_failure()` menjual balik spot leg lewat `place_local_market_sell()` (taker market sell)
- Retry hingga **5 kali** dengan exponential backoff (0.5s → 8s) sebelum menyerah
- Jika semua percobaan gagal: log `CRITICAL` dengan instruksi "MANUAL INTERVENTION REQUIRED" (qty + simbol yang masih unhedged)
- Event `HEDGE_FAILED` dipublish ke `arb:trade_events` dengan field `rollback_succeeded` dan `rollback_error`
- State pair di-set ke `CLOSED` (sesuai keputusan: reuse `CLOSED`, tidak menambah state baru)
- Dana yang sudah di-lock (`spot_cost_usdt`) di-unlock lewat `unlock_funds()`
- Tidak memakai `place_local_limit_sell` seperti rencana awal, karena limit sell tidak menjamin exit — untuk de-risking harus taker.

---

### P0-02 — Fix NameError `funding` vs `funding_usdt` di force_close

**Status:** ✅ Selesai (sudah benar sebelum audit dijalankan ulang)

**File:** `executor/engine.py:handle_force_close`

**Verifikasi:** Tidak ada lagi referensi variabel `funding` yang tidak terdefinisi — seluruh pemakaian adalah `funding_usdt`. Dikonfirmasi lewat pencarian `\bfunding\b` (hanya menyisa di komentar dan string log).

---

### P0-03 — Startup recovery: load active positions dari Redis

**Status:** ✅ Selesai

**File:** `executor/engine.py:initialize`, `_recover_active_positions`

**Implementasi:**
- `initialize()` memanggil `_recover_active_positions()` setelah koneksi Redis siap
- Scan `arb:active_pos:*`, parse menjadi `ActivePosition.from_dict()`, isi `self.active_positions`
- Setiap posisi yang di-recover di-set ke `HEDGED_ACTIVE` sehingga `_monitoring_loop` langsung mengelolanya (exit pegging, funding rollover, force close)
- Posisi dengan `qty <= 0` dibuang dan key-nya dihapus dari Redis (mencegah zombie position)
- Jumlah posisi yang dipulihkan di-log sebagai `WARNING` agar operator sadar ada state yang dibawa dari sesi sebelumnya

---

### P0-04 — Fix short entry price di mock client

**Status:** ✅ Selesai

**File:** `executor/exchange_client.py`, `executor/engine.py:_execute_entry_hedge`, `_record_hedged_position`

**Implementasi:**
- `open_global_short_market(symbol, amount, price=0.0)` — kedua client (live & paper) kini menerima harga referensi
- Engine meneruskan `global_ask_usdt` dari `PricingSnapshot` (disimpan di `order_info` saat order entry dibuat) sebagai `price`
- Mock client mengembalikan `price`, `average`, dan `expected_price` yang konsisten
- `_resolve_short_entry_usdt()` mengambil harga fill dari `average` → `price` → `fillPrice` → `expected_price`
- **Tidak ada lagi fallback ke harga spot.** Jika harga tidak tersedia, posisi ditolak dengan `ValueError` — lebih baik gagal keras daripada mencatat basis PnL yang salah.

---

### P0-05 — Sell spot leg pada force close

**Status:** ✅ Selesai

**File:** `executor/engine.py:handle_force_close`

**Implementasi:**
- Setelah futures short ditutup (langkah 3), spot leg dijual lewat `place_local_market_sell()` (langkah 3b)
- Retry hingga 5 kali dengan exponential backoff
- `exit_price_idr` memakai **harga fill nyata** dari order spot; snapshot live `local_best_bid` hanya dipakai jika sell gagal
- Jika likuidasi spot gagal total: log `CRITICAL` "MANUAL INTERVENTION REQUIRED" dan set `spot_liquidation_failed: true` pada record trade history agar operator bisa menyaringnya
- Kedua legs tercatat dalam closed trade record (`spot_pnl_idr` + `futures_pnl_usdt` + funding)

---

## FASE 1 — High Priority (Stability & Safety)

> Target: Bot stabil untuk paper trading yang akurat dan siap live trading terkontrol.
>
> **Status: ✅ SELESAI** — 31 tes Python + seluruh paket Go hijau (`go build ./...`, `go vet ./...`, `go test ./...`).

### P1-01 — Per-pair asyncio.Lock untuk prevent duplicate order

**File:** `executor/engine.py`  
**Masalah:** Tidak ada locking antara `redis.exists()` dan `place_local_limit_buy()`. Dua signal bersamaan bisa menghasilkan dua order.

**Status:** ✅ Selesai

**Implementasi:**
- `self._pair_locks: Dict[str, asyncio.Lock]` di `__init__`, diakses lewat `_pair_lock(pair_tag)` yang membuat lock saat pertama dipakai
- `handle_signal_notification()` hanya membungkus `async with self._pair_lock(pair_tag)` lalu memanggil `_handle_signal_locked()` — jadi seluruh rantai `redis.get` → cek `exists` → `place_local_limit_buy` berjalan serial per pair
- Lock **per pair**, bukan lock global: pair berbeda tetap bisa entry paralel
- Lock juga dipakai oleh pengecekan circuit breaker, sehingga flag halt dibaca konsisten

**Test:** `test_p1_01_concurrent_signals_place_only_one_entry_order` — 3 signal bersamaan pada pair yang sama, hanya **1** entry order yang terpasang (client men-`await` di tengah `place_local_limit_buy` agar race benar-benar tereproduksi tanpa lock).

---

### P1-02 — Set TTL pada Redis signal key

**File:** `internal/redishandler/client.go:PublishSignal`  
**Masalah:** `arb:signal:{pair_tag}` tidak punya TTL. Signal bisa dieksekusi saat data sudah sangat outdated.

**Status:** ✅ Selesai

**Implementasi:**
- Konstanta baru `SignalTTL = 3 * time.Second` di `internal/redishandler/client.go`
- `PublishSignal()` memakai `SignalTTL` untuk `arb:signal:*`, sementara `arb:live_price:*` tetap `DefaultTTL` (60s) karena harga live memang berguna lebih lama
- `ExecuteBatchUpdates()` — jalur batch yang dipakai scanner untuk publish massal — juga memakai `SignalTTL`, jadi kedua jalur konsisten
- TTL 3 detik dipilih karena jauh di bawah `max_stale_latency_ms` (500ms) × beberapa siklus scan (30s): signal yang tidak sempat dieksekusi berarti datanya sudah tidak relevan

**Test:** `TestRedisHandler_PublishSignalAndLivePrice` — memverifikasi signal TTL ≤ `SignalTTL` dan live price TTL > `SignalTTL` serta ≤ `DefaultTTL`.

---

### P1-03 — Daily loss limit / circuit breaker

**File:** `executor/engine.py`  
**Masalah:** Tidak ada mekanisme stop trading jika kerugian harian melebihi batas.

**Status:** ✅ Selesai (alert Telegram dikirim lewat event `arb:trade_events` dan status API, bukan panggilan langsung)

**Implementasi:**
- Config baru `max_daily_loss_usdt` (default **50 USDT**, `0` = nonaktif), bisa di-override dari `config.json` (`executor.max_daily_loss_usdt`) maupun env `MAX_DAILY_LOSS_USDT`
- `record_realized_pnl(net_profit_usdt)` dipanggil dari **dua** jalur penutupan posisi: exit normal (`_execute_exit_fill`) dan force close (`handle_force_close`)
- Hanya PnL negatif yang menambah `_daily_realized_loss_usdt`
- `_roll_loss_day_if_needed()` mereset akumulator pada pergantian hari **UTC**; jika halt disebabkan limit harian, trading otomatis dibuka kembali di hari baru
- Saat melewati limit: `_trading_halted = True`, `_halt_reason = "DAILY_LOSS_LIMIT"`, log `CRITICAL`, dan semua signal entry baru ditolak di awal `handle_signal_notification`
- **Posisi yang sudah terbuka tetap dikelola** (exit pegging/funding/force close) — menghentikan manajemen posisi akan meninggalkan posisi tanpa hedge, lebih berbahaya daripada kerugian yang hendak dicegah
- `resume_trading(reason)` untuk membuka halt secara manual setelah operator meninjau
- Status halt terlihat di `arb:executor:status` → `trading_halted`, `halt_reason`, `daily_realized_loss_usdt`, `loss_day_utc`

**Test:** `test_p1_03_daily_loss_limit_halts_new_entries` dan `test_p1_03_profits_do_not_halt_and_zero_limit_disables`.

**Catatan:** alert Telegram proaktif saat halt belum ada — perlu ditambahkan ketika bot Telegram mulai mengonsumsi `arb:executor:status`.

---

### P1-04 — Implement state FAILED dan error tracking

**File:** `executor/engine.py`, `executor/models.py`  
**Masalah:** Tidak ada state `FAILED`. Bot tidak tahu pair mana yang dalam kondisi error.

**Status:** ✅ Selesai

**Implementasi:**
- `PositionState.FAILED` ditambahkan ke `executor/models.py`
- `_mark_failed(pair_tag, reason, message, detail)` mencatat alasan di `self._failure_reasons` + log `CRITICAL`; `clear_failure(pair_tag)` mengembalikan state ke `IDLE`
- State `FAILED` di-set hanya untuk kegagalan yang benar-benar butuh intervensi:
  - `HEDGE_FAILED` — hedge gagal **dan** rollback spot juga gagal (koin tidak ter-hedge)
  - `SPOT_LIQUIDATION_FAILED` — force close menutup futures tapi gagal menjual spot
- Rollback yang **berhasil** tetap berakhir di `CLOSED` (posisi benar-benar flat) — keputusan desain Fase 0 dipertahankan
- `status_snapshot()` mengembalikan status lengkap (posisi, pending, failures, circuit breaker) dan `publish_status()` menulisnya ke Redis `arb:executor:status` dengan TTL 30 detik; loop monitoring mempublikasikan maksimal 1×/detik
- Endpoint Go baru `GET /api/v1/executor/status` di `internal/api/server.go` membaca key tersebut dan meneruskannya apa adanya; mengembalikan `{"status":"DISABLED"}` bila Redis nonaktif dan `{"status":"OFFLINE"}` bila executor belum pernah publish

**Test:** `test_p1_04_status_snapshot_reports_failures` (Python) dan `TestExecutorStatusEndpoint` (Go: DISABLED / OFFLINE / RUNNING).

---

### P1-05 — Stale orderbook timestamp validation di Scanner

**File:** `internal/scanner/service.go:fetchMarketData`  
**Masalah:** Scanner tidak cek apakah orderbook yang diterima dari exchange sudah terlalu lama.

**Status:** ✅ Selesai (guard aktif tapi default nonaktif — lihat catatan)

**Implementasi:**
- Config baru `max_order_book_age_seconds` di `internal/config/config.go` (`MaxOrderBookAgeSeconds`)
- `validateOrderBookAge(sb, pb, now, maxAgeSeconds)` dipanggil dari `calculate()` tepat setelah `validateOrderBooks()`, sehingga pair yang stale di-skip dengan error yang sama seperti empty book
- Book tanpa `Timestamp` (zero value) dilewati, bukan dianggap stale
- Timestamp di masa depan diperlakukan sebagai clock skew, bukan stale

**Catatan penting:** default-nya `0` (nonaktif) karena adapter live saat ini men-stamp orderbook dengan `time.Now()` **saat data diterima**, bukan timestamp dari bursa. Mengaktifkan guard dengan default 5s hanya akan mengukur latensi HTTP sendiri dan tidak pernah mendeteksi orderbook basi dari bursa. Guard ini harus diaktifkan **setelah** `internal/exchange/live.go` meneruskan timestamp asli bursa (Indodax tidak menyertakan timestamp di endpoint depth; OKX/MEXC perlu dipetakan dari field respons).

**Test:** `TestValidateOrderBookAge` (disabled / fresh / stale spot / stale perp / unstamped).

---

## FASE 2 — Medium Priority (Reliability)

### P2-01 — Set state ENTRY_PARTIAL saat partial fill terdeteksi

**File:** `executor/engine.py:_handle_entry_fills`  
**Rencana:** Sebelum hedge, jika `0 < filled < total_qty`, set state ke `ENTRY_PARTIAL`.

---

### P2-02 — Slippage guard untuk market orders

**File:** `executor/engine.py:_execute_entry_hedge`  
**Rencana:**
- Tambahkan config `max_entry_slippage_pct` (default: 0.3%)
- Fetch ticker sebelum market order
- Jika ask price lebih dari `expected * (1 + max_slippage)`, abort hedge

---

### P2-03 — Exponential backoff pada retry

**File:** `executor/engine.py`  
**Rencana:** Implementasikan helper `async def retry_with_backoff(fn, max_retries=3, base_delay=0.5)`.

---

### P2-04 — Max exposure per exchange

**File:** `executor/engine.py`  
**Rencana:**
- Tambahkan config `max_exposure_per_exchange_usdt`
- Hitung total exposure yang sedang aktif sebelum membuka posisi baru

---

### P2-05 — Exchange maintenance detection di Scanner

**File:** `internal/exchange/live.go`  
**Rencana:**
- Check HTTP status code dan response error code per exchange
- Jika error rate tinggi dalam window 5 menit, mark exchange sebagai `unavailable`
- Skip pair dari exchange yang unavailable

---

### P2-06 — Fee buffer sebelum order

**File:** `executor/engine.py:handle_signal_notification`  
**Rencana:** Kurangi `max_pos_usdt` dengan estimasi fee sebelum locking dan ordering.

---

### Implementasi Fase 2 (catatan)

**P2-01 — `ENTRY_PARTIAL` (menutup BUG-08).** Saat fill parsial masih di bawah
`min_partial_fill_usd` dan order maker masih `open`, pair ditandai
`PositionState.ENTRY_PARTIAL` (sebelumnya tetap `ENTRY_PENDING`, sehingga spot
yang sudah terisi tapi belum di-hedge tidak terlihat). Marker hanya dibersihkan
ke `HEDGED_ACTIVE` **setelah** hedge benar-benar berhasil, jadi kegagalan hedge
tetap terlihat sebagai `FAILED`/`CLOSED`.

**P2-02 — Slippage guard.** Config `max_entry_slippage_pct` (default `0.003` = 0.3%).
Sebelum market short, `fetch_global_ticker()` dibaca dan hedge dibatalkan bila
ask > `expected * (1 + pct)`; kegagalan dialirkan ke `_rollback_hedge_failure()`
yang sudah ada sehingga spot leg langsung dijual kembali. Guard **fail-open**:
ticker yang tidak terbaca tidak memblokir hedge — menolak hedge justru lebih
berisiko daripada hedge pada harga yang belum terverifikasi. `0` menonaktifkan.

**P2-03 — Retry helper.** `retry_with_backoff(fn, max_retries=3, base_delay=0.5)`
mengembalikan hasil atau `RetryExhausted` (membawa `attempts` + `last_error`).
Kedua loop inline (`_rollback_hedge_failure`, likuidasi spot di force close)
sekarang memakai helper ini dengan `max_retries=4` → tetap 5 percobaan total.

**P2-04 — Max exposure per exchange.** Config `max_exposure_per_exchange_usdt`
(default `0` = nonaktif). Sebelum locking, `_exposure_allows()` menjumlahkan
`spot_cost_usdt` + `futures_margin_usdt` dari posisi aktif **dan** order entry
yang masih pending, dihitung terpisah per exchange lokal dan global.

**P2-05 — Exchange maintenance detection.** `internal/exchange/health.go`
menyimpan outcome per-exchange dalam rolling window. Bila `failed/total >= threshold`
(dengan minimal `min_requests`), exchange dianggap `unavailable`: adapter
langsung mengembalikan `ErrExchangeUnavailable` tanpa memanggil HTTP, dan
scanner melewati pair yang menyentuh exchange tersebut **tanpa** menandai seluruh
scan sebagai gagal. Status 502/503/504 diklasifikasikan sebagai maintenance.
Config: `exchange_error_window_seconds` (300), `exchange_error_rate_threshold`
(0.5), `exchange_error_min_requests` (5). Set `threshold=0` atau `window=0` untuk
menonaktifkan.

> **Catatan P2-05:** `fetchMarketData()` kini memakai `errors.Join` (bukan `%v`)
> agar rantai error tetap utuh — ini prasyarat agar `errors.Is` bisa membedakan
> outage venue dari kegagalan data per-pair.

**P2-06 — Fee buffer.** Config `estimated_round_trip_fee_pct` (default `0.001`).
`max_pos_usdt` dikurangi `max_pos_usdt * pct` sebelum `lock_funds()`, sehingga
exit tetap bisa menutup fee both legs. Bila buffer membuat ukuran turun di bawah
`min_allocated_capital`, entry ditolak (bukan silently undersized). `0` menonaktifkan.

## FASE 3 — Low Priority (Enhancement)

> **Status: ✅ SELESAI** — 5/5. Semua fitur Fase 3 bersifat **opt-in** (default
> tidak aktif), sehingga perilaku default sistem identik dengan akhir Fase 2.

### P3-01 — WebSocket realtime feed (menggantikan REST polling)

**Status:** ✅ Selesai

**File:** `internal/exchange/ws_book.go` (baru), `ws_client.go` (baru),
`ws_streams.go` (baru), `live.go`, `internal/config/config.go`,
`cmd/nafaz-scanner/main.go`

**Implementasi:**
- `WSBookCache` menyimpan book terakhir per `(exchange, symbol)` beserta cap
  waktu terima. `Get()` hanya mengembalikan book yang umurnya ≤ `maxAge`.
- `WSStream` adalah deskripsi satu subscription (URL, frame subscribe, decoder),
  dan `WSClient` menjalankan supervisor satu goroutine per stream dengan
  reconnect **exponential backoff** (1s → 30s).
- Decoder murni dan terpisah: `decodeIndodaxDepth`, `decodeOKXBook` (qty dikali
  contract size, timestamp venue dipakai bila ada), `decodeMEXCSpotDepth`.
- Integrasi dilakukan di **satu choke point**, `trackedBookForSymbol()`: bila ada
  book WS yang fresh, REST dilewati (terhitung WS hit); bila tidak, fetch REST
  berjalan seperti biasa. Karena semua lima entry point order book
  (tokocrypto/indodax/mexc spot, okx/mexc perp) lewat fungsi itu, tidak ada
  duplikasi logika dan **coverage tidak pernah berkurang** saat feed diaktifkan.
- Pemeriksaan maintenance (P2-05) tetap berjalan sebelum cache dilihat, sehingga
  venue yang sedang down tetap di-skip walau ada book lama di cache.

**Aktivasi:** `enable_websocket_feed: true` (env `ENABLE_WEBSOCKET_FEED`),
`websocket_book_max_age_seconds` (default `2`, env
`WEBSOCKET_BOOK_MAX_AGE_SECONDS`). Default **off**.

> **Catatan jujur:** feed Indodax dan MEXC spot memerlukan subscribe per simbol,
> sedangkan scanner tidak mengekstrak `subscribe()` dari `WSStream` sebagai
> mekanisme pilihan per simbol — supervisor hanya mengirim frame dari field
> `Subscribe` yang statis. Saat ini hanya stream OKX (`books5`) yang tercakup
> penuh; venue lain otomatis jatuh ke REST. Payload venue juga **belum
> diverifikasi terhadap endpoint live**, hanya terhadap fixture di unit test,
> karena itu fitur ini default off.

`WebSocketStats()` mengembalikan `(hits, misses)` sehingga rasio WS-vs-REST
bisa dipantau tanpa debugger.

---

### P3-02 — Client Order ID untuk idempotency

**Status:** ✅ Selesai

**File:** `executor/engine.py`, `executor/exchange_client.py`

**Implementasi:**
- `build_client_order_id(pair_tag, leg, seq)` menghasilkan id deterministik
  `{pair}-{leg}-{seq}` (lowercase, alfanumerik + dash, maksimal 36 karakter).
  Dipakai lima venue-safe karakter set karena Binance/OKX/Indodax punya aturan
  yang saling tumpang tindih.
- `ExchangeClient` meneruskan `{"clientOrderId": ...}` sebagai params ccxt.
  Helper `_client_order_params()` **menghilangkan argumen params sepenuhnya**
  bila id kosong, supaya ccxt tidak menerima dict kosong.
- Setiap leg punya prefix berbeda: `entry`, `hedge`, `exit`, `unwind`,
  `rollback`, `force-close`, `force-liq`.
- Dua loop retry (`retry_with_backoff`) memakai **satu id untuk seluruh
  percobaan**, karena percobaan ulang adalah order logis yang sama — venue dapat
  menolak duplikat daripada menjual dua kali.
- `PaperTradingExchangeClient` mengembalikan order lama bila `client_order_id`
  yang sama dipakai ulang, sehingga mock mode benar-benar menguji jalur idempotensi.

---

### P3-03 — Volume/velocity filter di Scanner

**Status:** ✅ Selesai

**File:** `internal/scanner/liquidity.go` (baru), `internal/scanner/service.go`,
`internal/config/config.go`

**Implementasi:**
- `validateLiquidityVelocity()` menolak pair yang **top-of-book**-nya terlalu
  tipis untuk menyerap satu posisi. Ambang diambil dari
  `min_top_of_book_quote_usdt` (env `MIN_TOP_OF_BOOK_QUOTE_USDT`), lalu
  di-*cap* oleh `requiredNotionalUsdt()` — budget per pair
  (`capital_total * allocation_per_pair`) dengan lantai `min_allocated_capital`.
  Tanpa cap ini, akun kecil akan ditolak karena buku tidak punya $10k likuiditas
  yang sebenarnya tidak dibutuhkan.
- Filter direkayasa **fail-safe terhadap buku kosong**: buku satu sisi/ kosong
  selalu ditolak ketika filter aktif.
- Default **0 = nonaktif**, sehingga tidak ada pair yang tiba-tiba hilang bagi
  deployment yang sudah berjalan.

> **Batasan desain:** audit meminta filter berbasis *24h volume* dan *trade
> count*. Kedua metrik itu tidak tersedia di `domain.Market` saat ini (hanya
> `MinOrder`/`LotSize`), dan menambah request ticker 24h per pair akan
> menggandakan beban REST scan. Yang diimplementasikan adalah proxy yang paling
> dekat dan bebas biaya tambahan: likuiditas top-of-book pada saat scan.

---

### P3-04 — Order Book Imbalance (OBI) calculation

**Status:** ✅ Selesai

**File:** `internal/scanner/liquidity.go`, `internal/scanner/service.go`,
`internal/domain/domain.go`

**Implementasi:**
- `orderBookImbalance()` menghitung
  `OBI = (bid_depth - ask_depth) / (bid_depth + ask_depth)` atas notional yang
  berada dalam **±1% dari mid price** — kira-kira rentang yang dilewati market
  order seukuran posisi kita. Hasil ∈ [-1, 1].
- Buku satu sisi dikembalikan sebagai `-1` (bids kosong = tekanan jual) atau
  `1` (asks kosong); buku kosong = `0`. Nilai selalu total, aman untuk dipublikasikan.
- Dipublikasikan sebagai `spot_obi` dan `perp_obi` pada `Opportunity`.

> **Keputusan:** OBI **tidak** dijadikan gate kelayakan (`TradingEligible`).
> Belum ada bukti empiris soal arah yang menguntungkan untuk strategi ini, dan
> menambahkan gate berbasis heuristik berisiko membuang peluang yang benar-benar
> profitable. OBI dilaporkan sebagai sinyal advisory untuk operator.

---

### P3-05 — Private WebSocket untuk balance & order updates

**Status:** ✅ Selesai

**File:** `executor/private_stream.py` (baru), `executor/engine.py`,
`executor/config.py`

**Implementasi:**
- `PrivateStreamCache` menyimpan update order/balance dari feed privat beserta
  cap waktu terima **lokal** (timestamp venue tidak konsisten antar-exchange dan
  yang kita butuhkan hanya "sudah berapa lama kita memegang nilai ini").
- `fetch_order_preferring_stream()` mengembalikan nilai cache jika masih fresh
  dan jatuh ke REST bila tidak. Hasil REST ditulis balik ke cache supaya
  beberapa pengecekan dalam tick yang sama tidak menghantam venue berulang kali.
- Dua titik polling order (`_process_pending_entry`, `_check_exit_orders`)
  dialihkan melalui helper ini. `forget_order()` membuang order terminal agar
  cache tidak tumbuh tanpa batas.

**Aktivasi:** `private_stream_enabled` (default **false**, env
`PRIVATE_STREAM_ENABLED`), `private_stream_max_age_seconds` (default `5.0`,
env `PRIVATE_STREAM_MAX_AGE_SECONDS`).

> **Batasan jujur:** modul ini adalah **cache + jalur fallback**, bukan klien
> websocket venue. Implementasi P3-05 di audit menyebut "ganti polling dengan
> WebSocket event stream"; transport autentikasi per venue (listenKey Binance,
> login OKX, dsb.) belum dibangun. Yang diselesaikan adalah bagian yang aman dan
> bisa diuji sekarang: struktur data, kebijakan staleness, dan sakelar fallback
> yang menjamin socket mati tidak pernah membuat executor buta. Karena itu fitur
> ini default off.

---

## Dependency Antar Task

```
P0-02 (NameError fix) ──────────────────────── independent   ✅
P0-04 (mock price fix) ─────────────────────── independent   ✅
P0-01 (rollback hedge) ──── requires P0-04                   ✅
P0-03 (startup recovery) ── requires P0-04                   ✅
P0-05 (force close spot) ── requires P0-02                   ✅
P1-01 (per-pair lock) ──────────────────────── independent   ✅
P1-02 (signal TTL) ─────────────────────────── independent   ✅
P1-03 (daily loss limit) ── requires P1-04                   ✅
P1-04 (FAILED state) ──────────────────────── requires P0-01 ✅
P1-05 (stale OB) ───────────────────────────── independent   ✅
BUG-10 (short exit fill) ───────────────────── independent   ✅
P2-03 (retry helper) ───────────────────────── independent   ✅
P2-01 (ENTRY_PARTIAL) ──────────────────────── independent   ✅
P2-02 (slippage guard) ─── requires P2-03                    ✅
P2-06 (fee buffer) ─────────────────────────── independent   ✅
P2-04 (exposure cap) ───────────────────────── independent   ✅
P2-05 (exchange health) ────────────────────── independent   ✅
P3-01 (websocket feed) ── requires P2-05 (health)            ✅
P3-02 (client order id) ─────────────────────── independent   ✅
P3-03 (liquidity filter) ────────────────────── independent   ✅
P3-04 (order book OBI) ──────────────────────── independent   ✅
P3-05 (private stream) ──────────────────────── independent   ✅
```

> **Catatan P1-04:** Rollback yang **berhasil** tetap berakhir di `CLOSED` karena posisi
> memang flat. State `FAILED` hanya untuk kasus yang butuh intervensi operator
> (`HEDGE_FAILED` dengan rollback gagal, `SPOT_LIQUIDATION_FAILED`). State ini dapat
> di-query lewat `GET /api/v1/executor/status`.

## Test Coverage Fase 0

Test baru di `tests/test_executor.py`:

| Test | Memverifikasi |
|---|---|
| `test_p0_04_short_entry_uses_futures_ask_not_spot_price` | Short entry = futures ask (65_000), bukan harga spot (62_500) |
| `test_p0_01_hedge_failure_rolls_back_spot_leg` | Hedge gagal → spot leg di-market-sell, event `HEDGE_FAILED` terbit |
| `test_p0_01_hedge_failure_with_failed_rollback_retries_and_alerts` | Rollback gagal → 5 retry + alert `rollback_succeeded=false` |
| `test_p0_03_startup_recovery_restores_active_positions` | `initialize()` memulihkan posisi dari `arb:active_pos:*` |
| `test_p0_03_recovery_drops_zero_qty_position` | Posisi `qty=0` dibuang, bukan dibangkitkan |
| `test_p0_05_force_close_liquidates_spot_leg` | Force close menjual spot leg + mencatat harga fill nyata |
| `test_p0_05_force_close_records_failed_spot_liquidation` | Kegagalan likuidasi spot ditandai di trade history + state `FAILED` |

## Test Coverage Fase 1

| Test | Memverifikasi |
|---|---|
| `test_p1_01_concurrent_signals_place_only_one_entry_order` | 3 signal bersamaan → hanya 1 limit buy (per-pair lock) |
| `test_p1_03_daily_loss_limit_halts_new_entries` | Loss kumulatif ≥ limit → halt; `resume_trading()` membuka halt |
| `test_p1_03_profits_do_not_halt_and_zero_limit_disables` | Limit `0` = nonaktif; profit tidak menambah akumulator loss |
| `test_p1_04_status_snapshot_reports_failures` | Snapshot + Redis `arb:executor:status` memuat failure & state `FAILED` |
| `test_bug10_short_exit_uses_actual_close_fill` | Futures PnL memakai fill close (65_200), bukan pegged ask (65_000) |
| `test_bug10_paper_client_close_reports_fill_price` | Paper client melaporkan harga fill saat close short |
| `TestValidateOrderBookAge` (Go) | Guard stale: disabled / fresh / stale spot / stale perp / tanpa timestamp |
| `TestRedisHandler_PublishSignalAndLivePrice` (Go) | Signal TTL ≤ `SignalTTL`, live price TTL lebih panjang |
| `TestExecutorStatusEndpoint` (Go) | Endpoint `/api/v1/executor/status`: DISABLED / OFFLINE / RUNNING |

## Test Coverage Fase 2

Python (`tests/test_executor.py`):

| Test | Memverifikasi |
|---|---|
| `test_p2_01_partial_fill_below_threshold_sets_entry_partial` | Fill parsial di bawah threshold → state `ENTRY_PARTIAL`, tidak ada hedge |
| `test_p2_01_partial_fill_above_threshold_hedges_and_activates` | Fill di atas threshold → langsung `HEDGED_ACTIVE`, tidak singgah di `ENTRY_PARTIAL` |
| `test_p2_02_excessive_slippage_aborts_and_rolls_back` | Ask +2% (> 0.3%) → hedge dibatalkan, spot leg dijual kembali |
| `test_p2_02_acceptable_slippage_still_hedges` | Ask +0.15% (di dalam toleransi) → hedge tetap jalan |
| `test_p2_02_missing_live_ask_fails_open` | Ticker tanpa ask → guard fail-open, hedge tetap jalan |
| `test_p2_02_zero_slippage_disables_guard` | `max_entry_slippage_pct=0` menonaktifkan guard |
| `test_p2_03_retry_with_backoff_retries_until_success` | Berhenti setelah sukses; jumlah panggilan sesuai |
| `test_p2_03_retry_with_backoff_raises_after_exhaustion` | `RetryExhausted` membawa `attempts` + `last_error` |
| `test_p2_03_force_close_uses_backoff_helper` | Force close benar-benar memakai helper bersama |
| `test_p2_04_exposure_cap_rejects_second_position` | Posisi $50 + order $50 vs cap $60 → ditolak |
| `test_p2_04_exposure_cap_counts_active_positions` | Posisi **aktif** (bukan hanya pending) ikut dihitung |
| `test_p2_04_exposure_cap_allows_within_budget` | $50 + $50 vs cap $120 → diizinkan |
| `test_p2_04_zero_cap_disables_check` | Cap `0` menonaktifkan pemeriksaan |
| `test_p2_06_fee_buffer_shrinks_locked_capital` | Fee 1% → locked 49.5 dari budget 50 |
| `test_p2_06_zero_fee_pct_disables_buffer` | Fee `0` → locked penuh 50 |
| `test_p2_06_fee_buffer_can_drop_below_min_capital` | Buffer menurunkan size < `min_allocated_capital` → entry ditolak |

Go (`internal/exchange/health_test.go`, `internal/scanner/exchange_health_test.go`,
`internal/config/config_test.go`):

| Test | Memverifikasi |
|---|---|
| `TestAvailabilityTripsAfterErrorThreshold` | Breaker tidak trip sebelum `MinRequests`, lalu trip di atas threshold |
| `TestAvailabilityIgnoresOldOutcomes` | Outcome lama keluar dari window → exchange kembali `available` |
| `TestAvailabilityDisabledPolicyAlwaysAvailable` | Policy nol = deteksi nonaktif |
| `TestAvailabilityResetClearsHistory` | `reset()` mengosongkan riwayat |
| `TestClassifyHTTPError` | 502/503/504 = maintenance; 200/404/429 bukan |
| `TestTrackedBookReturnsUnavailableWhenTripped` | Exchange unavailable tidak dipanggil HTTP lagi |
| `TestLiveAdapterFlagsServiceUnavailableAsMaintenance` | Response 503 → `ErrExchangeUnavailable` |
| `TestLiveAdapterPolicyDisabledNeverSkips` | Adapter default selalu melaporkan available |
| `TestScanSkipsPairsOnUnavailableExchange` | Pair di exchange down tidak di-fetch dan dilaporkan sebagai skip |
| `TestScanDoesNotFailCompletelyWhenExchangeGoesDownMidScan` | Outage satu venue tidak menandai seluruh scan gagal |
| `TestAvailabilityReporterIsOptional` | Adapter tanpa `ExchangeAvailable` tetap didukung |
| `TestExchangeHealthDefaultsAndValidation` | Default 300s/0.5/5 + validasi range |

## Test Coverage Fase 3

Python (`tests/test_executor.py`, total suite **64 tes hijau**):

| Test | Memverifikasi |
|---|---|
| `test_p3_02_client_order_ids_are_unique_and_venue_safe` | Id unik, lowercase, alfanumerik + dash |
| `test_p3_02_client_order_id_retry_is_stable` | Id sama untuk retry order yang sama |
| `test_p3_02_client_order_id_is_bounded` | Id ≤ 36 karakter walau pair panjang |
| `test_p3_02_entry_order_carries_client_order_id` | Entry maker buy membawa id ber-prefix `entry` |
| `test_p3_02_hedge_carries_client_order_id` | Hedge futures membawa id ber-prefix `hedge` |
| `test_p3_02_live_client_sends_params_only_when_id_present` | Tanpa id, argumen params dihilangkan total |
| `test_p3_04_sanitize_clamps_out_of_range_values` | Nilai di luar range di-clamp ke batas |
| `test_p3_04_sanitize_replaces_nan_and_garbage` | NaN dan string invalid → default |
| `test_p3_04_sanitize_keeps_valid_defaults_quiet` | Config default tidak memunculkan warning |
| `test_p3_04_sanitize_announces_disabled_safety_nets` | `0` yang menonaktifkan proteksi dilaporkan |
| `test_p3_05_cache_serves_fresh_orders` | Order fresh dilayani dari cache |
| `test_p3_05_cache_rejects_stale_orders` | Order basi → miss (jatuh ke REST) |
| `test_p3_05_cache_ignores_orders_without_id` | Order tanpa id tidak di-cache |
| `test_p3_05_cache_forget_order_frees_the_slot` | Order terminal dibuang dari cache |
| `test_p3_05_fetch_prefers_cache_then_falls_back_to_rest` | Cache → REST → cache basi → REST |
| `test_p3_05_disabled_by_default_keeps_rest_polling` | Default off: `private_stream is None` |
| `test_p3_05_enabled_engine_uses_stream_cache` | Data feed fresh menghindari polling REST |

Go (`internal/scanner/liquidity_test.go`, `internal/exchange/ws_book_test.go`,
`internal/config/config_test.go`):

| Test | Memverifikasi |
|---|---|
| `TestDecimalOrderingSanity` | Sanity check ordering `decimal` yang jadi dasar semua asersi likuiditas |
| `TestLiquidityFilterDisabledByDefault` | Ambang `0` = filter nonaktif |
| `TestLiquidityFilterRejectsThinSpotBook` | Buku spot tipis ditolak |
| `TestLiquidityFilterRejectsThinPerpBook` | Buku perp tipis ditolak |
| `TestLiquidityFilterPassesDeepBook` | Buku dalam lolos |
| `TestLiquidityThresholdCappedAtRequiredNotional` | Ambang tidak melebihi ukuran posisi |
| `TestLiquidityFilterHandlesEmptyBook` | Buku kosong ditolak |
| `TestRequiredNotionalIsAllocationBudget` | Budget = `capital_total * allocation_per_pair` |
| `TestRequiredNotionalNeverBelowMinCapital` | Lantai `min_allocated_capital` |
| `TestOrderBookImbalanceSymmetricIsZero` | Buku simetris → OBI 0 |
| `TestOrderBookImbalanceBidHeavyIsPositive` | Bid-heavy → OBI ∈ (0,1) |
| `TestOrderBookImbalanceAskHeavyIsNegative` | Ask-heavy → OBI ∈ (-1,0) |
| `TestOrderBookImbalanceIgnoresDistantLevels` | Level > 1% dari mid diabaikan |
| `TestOrderBookImbalanceOneSidedAndEmpty` | Buku satu sisi → ±1; kosong → 0 |
| `TestWSBookCacheServesFreshBooks` | Book fresh disajikan + hit count |
| `TestWSBookCacheRejectsStaleBooks` | Book basi → miss |
| `TestWSBookCacheUnknownSymbolMisses` | Simbol tak dikenal → miss |
| `TestWSBookCacheIgnoresOneSidedBooks` | Buku satu sisi tidak di-cache |
| `TestDecodeOKXBook` | Contract size mengalikan qty; timestamp venue dipakai |
| `TestDecodeOKXBookRejectsEmptyData` | Frame ack tanpa data → error |
| `TestDecodeIndodaxDepth` | Decode bids/asks Indodax |
| `TestDecodeIndodaxDepthRejectsOneSided` | Buku satu sisi ditolak |
| `TestDecodeMEXCSpotDepth` | Decode + timestamp venue |
| `TestDecodeMEXCSpotDepthUnknownTimestampFallsBackToNow` | Tanpa ts → jam lokal |
| `TestTrackedBookForSymbolPrefersCacheThenFallsBack` | WS hit melewati REST; simbol lain tetap REST |
| `TestTrackedBookForSymbolStillHonoursMaintenance` | Venue down tetap di-skip walau cache ada |
| `TestWebSocketStatsWithoutFeed` | Tanpa feed, statistik nol |
| `TestWSClientStopIsIdempotent` | `Stop()` dua kali aman |
| `TestTopOfBookLiquidityConfig` | Default 0, override, dan validasi negatif |
| `TestWebSocketFeedConfig` | Default off, max age 2s, override, validasi negatif |


---

## Post-refactor safety and Supabase work — 2026-11-07

This section supersedes the older blanket "all phases complete" status for current
release readiness. The new review found regressions/gaps after the executor refactor.

### Implemented in branch `feat/executor-safety-supabase-persistence`

- Entry TTL, adverse-selection, and emergency cancellation now verify terminal
  exchange status before clearing pending state or releasing funds.
- Cancellation races reconcile final cumulative spot fills before entry finalization.
- Startup recovery retains Redis pending records for adopted open orders and processes
  terminal partial fills before clearing them. Unknown order status blocks the pair.
- Force-close cancellation verifies each pending order and reconciles fills; force
  close aborts when a pending order cannot be proven terminal.
- Futures force close requires a position query confirming zero residual exposure.
- Spot force close requires the full requested quantity to be filled.
- Entry hedge and normal exit paths verify exchange-reported fill quantities.
- Unverified/partial exit hedge transitions to `RECONCILIATION_REQUIRED`, preventing
  blind retries after the spot leg has already sold.
- Funding PnL is taken from actual exchange funding history; no 8-hour assumption or
  estimated funding credit is used.
- Balance API failures halt new entries on both venues; existing positions remain
  under the monitoring/force-close paths.
- Added Supabase migration for durable scanner/executor data and documentation.
- Added GitHub Actions jobs for Python and Go test suites.

### Supabase runtime boundary

The database schema is versioned under `supabase/migrations`. Runtime writes to
Supabase are **not yet integrated**: Redis remains the live coordination store and
scanner history remains file-backed. The next database integration must implement an
idempotent persistence/outbox adapter and ensure database outages never block a
required hedge, rollback, or force close.

### Release gate

- [ ] Python executor test suite passes.
- [ ] Go tests and vet pass.
- [ ] Supabase migration is reviewed and applied to a non-production project.
- [ ] Paper-mode scenarios pass, including restart, partial fill, cancellation timeout,
      partial futures hedge/close, funding settlement replay, and force close.
- [ ] Live mode remains disabled until exchange-specific order/funding/position
      payloads are verified against the actual target venues.

---

## Comprehensive Phase 1 gap audit — 2026-11-08

The previous phase checklists describe historical implementation work and are not a live-readiness certification. The current repository/PR review, evidence matrix, changes made, and remaining blockers are documented in [PHASE1-GAP-AUDIT.md](./PHASE1-GAP-AUDIT.md).

**Current release status: NOT READY FOR LIVE TRADING.** In particular, authenticated open-order/position inspection and exchange-specific order-constraint preflight remain incomplete; PR #6 for Binance/Bybit is not yet integrated into `main`.
