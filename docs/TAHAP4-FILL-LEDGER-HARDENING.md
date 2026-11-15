# Tahap 4 — Immutable Fill Ledger, Expected-vs-Realized Analytics & V2 Hardening

Scope: P2/P3. Catatan implementasi, batasan, dan bukti verifikasi.

## 4.1 Immutable Fill Ledger

### Kenapa PostgreSQL langsung, bukan Supabase REST

Di VPS produksi `SUPABASE_URL` dan `SUPABASE_KEY` kosong, sementara `POSTGRES_DSN` terisi. Artinya seluruh `self.supabase.*` adalah no-op: sebelum perubahan ini `public.orders`, `public.order_fills`, dan `public.closed_trades` berisi **0 baris**. Ledger yang hanya menulis lewat Supabase akan membuang setiap fill tanpa terlihat.

Karena itu `executor/fill_ledger.py` memakai **asyncpg** dan berbagi pool milik `GlobalStopService` (kanonik authority yang sudah wajib ada pada mode LIVE).

Bukti sebelum perubahan (VPS, 2026-11-15):

```
orders | 0
order_fills | 0
closed_trades | 0
position_legs | 0
execution_sessions | 0
pair_pnl_snapshots | 0
```

### Schema

Migration: `supabase/migrations/20261115000100_add_execution_fill_ledger.sql`

`public.execution_fill_ledger` — satu baris per eksekusi venue per kaki:

| Kolom | Isi |
|---|---|
| `leg` | `spot_entry` / `futures_entry` / `spot_exit` / `futures_exit` |
| `qty`, `actual_vwap_price` | eksekusi aktual, bukan harga yang diminta |
| `expected_vwap_price` | harga yang dimodelkan scanner/entry |
| `actual_fee_paid`, `fee_currency`, `fee_verified` | fee aktual dari payload venue |
| `fx_rate_applied`, `fx_rate_source` | kurs yang benar-benar dipakai + asalnya |
| `realized_funding_usd` | funding yang terealisasi pada kaki tersebut |
| `exchange_fill_id` | identitas trade venue |

Invariant:

- **Append-only.** Tidak ada `UPDATE`/`DELETE`. Koreksi = baris adjustment (`adjustment_of` + `adjustment_reason`).
- **Idempoten.** Unique index `(exchange_name, market_type, exchange_fill_id)` membuat replay trade yang sama menjadi no-op, bukan duplikat.
- **Fail-soft.** Kegagalan tulis di-log dan dihitung (`ledger.dropped`), tidak pernah melempar exception ke jalur trading. Utang akuntansi bisa dipulihkan; posisi tanpa manajemen tidak.

`public.pair_pnl_deviations` — atribusi sekali per trade yang tertutup penuh:

```
net_pnl_deviation_usd = realized - expected
fee_deviation_usd     = expected_fees - realized_fees
slippage_deviation_usd = net_pnl_deviation - fee_deviation
```

Tanda pada `slippage` penting: seluruh selisih bersih yang **tidak** dijelaskan fee adalah slippage eksekusi. Implementasi awal memakai `+` dan membalik atribusi (terbukti oleh test: −4.0 vs −2.0 yang benar).

### Sumber nilai "aktual"

- `actual_vwap_price` diambil dari `order["average"]`, dan **ditolak** bila tidak ada harga eksekusi (`<= 0`). Harga permintaan tidak pernah dipakai sebagai pengganti fill.
- Bila venue hanya memberi snapshot kumulatif (tanpa daftar trade), payload ditandai `fee_source = ORDER_CUMULATIVE_AVERAGE` di `raw_payload`, sehingga rekonsiliasi bisa membedakan rata-rata kumulatif dari trade individual.
- Identitas fill sintetis memakai `"{order_id}:{cumulative_filled}"`, bukan `order_id` saja. Maker order yang terisi bertahap dipoll berulang dengan id sama dan kuantitas kumulatif yang naik; keying pada order id saja akan membuat setiap chunk berikutnya dianggap replay chunk pertama dan dibuang. Watermark yang sama tetap idempoten, increment baru tetap tercatat. Diverifikasi pada DB VPS: 3 poll (0.002 / 0.002 / 0.005) menghasilkan 2 baris.

### Titik pencatatan

| Kaki | Lokasi |
|---|---|
| `spot_entry` | `entry_hedge.py::_handle_entry_fills`, sebelum hedge (agar kegagalan hedge tetap menyisakan bukti fill spot) |
| `futures_entry` | `entry_hedge.py::_persist_entry_hedge_success` |
| `spot_exit` | `exit_execution.py`, sebelum `_persist_closed_trade` |
| `futures_exit` | `exit_execution.py`, sebelum `_persist_closed_trade` |
| deviation | `exit_execution.py::_record_expected_vs_realized`, hanya saat `pos.qty == 0` |

`expected_net_profit_usdt`, `expected_fees_usdt`, dan `expected_hold_hours` diambil dari directive scanner saat entry dan disimpan pada `ActivePosition` sehingga tetap tersedia setelah restart.

## 4.2 V2 Production Hardening

### STOP fencing untuk semua jalur submit

Sebelumnya hanya `spot_entry` dan hedge futures yang melewati fence. Exit, unwind, rollback, dan force-close tidak. Dua context manager sekarang tersedia di `GlobalStopService`:

- `order_submit_fence(...)` — membuka exposure. **Fail-closed** saat STOP aktif atau ada submit `SUBMITTING`/`UNKNOWN` yang belum terselesaikan.
- `derisk_submit_fence(...)` — mengurangi exposure (exit/unwind/rollback/force-close). Tetap mengambil advisory lock serialisasi yang sama, tetapi **tidak** diblokir STOP.

Alasannya: sistem yang berhenti dan tidak bisa menutup posisi lebih berbahaya daripada sistem yang bisa. Keputusan ini disengaja dan diuji (`test_derisk_fence_allows_exit_while_stopped`).

Catatan desain: `derisk_submit_fence` **tidak** menolak de-risking hanya karena ada entry lain yang ambigu. Entry ambigu adalah alasan untuk berhenti membuka posisi baru, bukan alasan membiarkan posisi lama terbuka. Kondisi itu di-log sebagai warning, dan serialisasi tetap menjaga keduanya tidak saling menyela.

### Pool resilience

- `ensure_pool()` menyambung ulang pool setelah koneksi putus. `order_submit_fence` memanggilnya sebelum `_require_pool()`, sehingga reconnect benar-benar diefektifkan (bug urutan awal: `_require_pool()` lebih dulu membuat pool yang drop tidak pernah pulih).
- `_demote_broken_pool()` hanya menurunkan pool untuk error koneksi (`ConnectionResetError`, `InterfaceError`, `OSError`, `TimeoutError`, dst). Error statement biasa tidak boleh membuang pool.
- Loop rekonsiliasi memanggil `ensure_pool()` sebelum restore readiness: koneksi fence yang hilang berarti semua keputusan admission sebelumnya sudah basi.

### Startup recovery memverifikasi kedua kaki

`_verify_recovered_active_positions()` sekarang memanggil `_verify_recovered_spot_legs()` selain verifikasi short futures.

Sebelumnya hanya kaki futures yang diperiksa. Short futures tanpa inventory spot bukan hedge: exit spot tidak bisa menjual apa yang tidak dipegang venue. Kedua kaki harus cocok sebelum entry dibuka.

Detail:

- Read-only. Tidak pernah menjual atau menyusun order; operator harus melihat mismatch sebelum trading dilanjutkan.
- Bila API saldo spot tidak tersedia, semua posisi ditandai `RECONCILIATION_REQUIRED` (fail-closed), bukan diasumsikan cocok.
- Base asset diturunkan dari `pos.base_asset`, lalu `_pair_infos`, lalu nama simbol (`btc_idr` → `BTC`).
- Toleransi relatif `1e-4`, sama dengan verifikasi futures.

## Verifikasi

| Pemeriksaan | Hasil |
|---|---|
| `python3 -m pytest tests/ -q` | **632 passed**, 2 skipped |
| `go build ./...` + `go test ./...` | semua paket ok |
| Migration pada PostgreSQL VPS | tabel + 3 index + constraint terpasang |
| Smoke test ledger ke DB VPS | 4 kaki tertulis, replay idempoten (`duplicate_fill_id: None`), `dropped: 0` |
| Deviation math pada DB VPS | net −3.0, fee −1.0, slippage −2.0 (benar) |
| Chunked fill watermark pada DB VPS | 3 poll (0.002/0.002/0.005) → 2 baris; replay deduped, increment tercatat |

## Batasan yang masih terbuka

- Ledger menulis satu baris per **order**, bukan per trade individual, ketika venue hanya menyediakan rata-rata kumulatif. Fill individual tetap masuk saat `trade_fill_reconciliation` berjalan — tetapi worker itu masih bergantung pada Supabase writer yang nonaktif di VPS, sehingga saat ini tidak pernah jalan. Ini pekerjaan lanjutan: memindahkan jalur rekonsiliasi trade ke asyncpg.
- `realized_funding_usd` diisi dari `accumulated_funding_usdt` posisi saat exit, belum dari `funding_payments` venue per settlement.
- `pair_pnl_deviations.funding_deviation_usd` masih `0.0` karena expected funding belum dimodelkan saat entry.
- Jalur submit di `venues/` (adapter tingkat bawah) tidak difence satu per satu; fence dipasang pada pemanggil (`exit_orders`, `exit_execution`, `force_close_legs`, `rollback`). `MutationGateway` sudah mem-fence untuk jalur yang memakainya.
