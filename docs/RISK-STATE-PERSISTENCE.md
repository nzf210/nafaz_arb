# Pemulihan status risiko persisten

## 1. Inventaris status

| Status runtime | Jenis data persisten | Cakupan | Perilaku pemulihan |
|---|---|---|---|
| `_trading_halted`, `_halt_reason` | `trading_halt` | Global | Dipulihkan sebelum worker/listener berjalan; kill switch manual tidak dihapus saat pergantian tanggal UTC |
| Pengakuan resume manual | `trading_resume` | Global | Mencegah penghentian rugi harian yang sudah diakui muncul kembali setelah restart pada hari UTC yang sama |
| `_daily_realized_loss_usdt`, `_loss_day` | `daily_loss` + `risk_pnl_events` | Global | Dibangun ulang dari peristiwa PnL terverifikasi yang tidak dapat diubah untuk tanggal UTC berjalan |
| `_exchange_halted` | `exchange_halt` | Bursa | Dipulihkan per venue; masa cooldown rate limit tetap dipertahankan |
| `_consecutive_order_failures` | `consecutive_order_failures` | Bursa | Dipulihkan per venue dan diselesaikan setelah keberhasilan bersih atau resume manual |
| `_recovery_scope_halts` | `reconciliation_halt` | Global/bursa/pasangan | Hanya dipulihkan pada cakupan eksposur yang terdampak |
| `_failure_reasons` dan status posisi perlu rekonsiliasi | `position_failure` | Posisi | Dipulihkan sebagai status keselamatan posisi dan gerbang entri pasangan |
| Eksposur tanpa hedge/rekonsiliasi parsial | `position_failure` + `reconciliation_halt` | Posisi/pasangan | Tetap diblokir sampai rekonsiliasi penyebabnya diselesaikan secara eksplisit |

## 2. Penyimpanan dan urutan pemulihan

- `risk_state` menyimpan `state_id`, `state_type`, `scope`, `exchange`, `pair_key`, `reason`, `value`, `created_at`, `updated_at`, dan `resolved_at`.
- `risk_pnl_events` adalah ledger peristiwa append-only dengan ID unik. Hanya PnL transaksi terealisasi yang telah diverifikasi yang boleh dimasukkan melalui jalur penutupan.
- Outbox SQLite adalah jurnal lokal sinkron. Saat startup, status risiko lokal yang masih menunggu pengiriman ditumpangkan di atas data Supabase agar crash sebelum pengiriman asinkron tidak menghapus status penghentian yang baru dicatat.
- Jika penyimpanan risiko persisten gagal dipulihkan atau gagal dicatat dalam mode LIVE, entri baru diblokir. Pemantauan posisi dan pengurangan risiko tetap tersedia.
- Redis tidak digunakan sebagai satu-satunya sumber kebenaran untuk status penghentian risiko.

## 3. Akuntansi rugi harian UTC dan pencegahan duplikasi

Rugi harian merupakan jumlah nilai negatif `pnl_usdt` pada `risk_pnl_events` dengan `loss_day_utc` sama dengan tanggal UTC saat ini. ID peristiwa immutable hanya dimasukkan sekali melalui outbox persisten; ID duplikat tidak menambah akumulator. Profit tidak mengurangi akumulator rugi terealisasi bruto, sesuai perilaku strategi yang dipertahankan.

Saat tanggal UTC berganti, hanya penghentian karena rugi harian yang memenuhi syarat untuk dihapus. Kill switch operator tetap aktif. Resume manual dicatat secara persisten untuk hari UTC berjalan; peristiwa PnL terealisasi baru akan mengaktifkan kembali evaluasi batas rugi harian.

## 4. Migrasi

Terapkan secara berurutan:

- `20261111000200_add_durable_risk_state.sql`
- `20261111000300_add_idempotent_risk_pnl_events.sql`

Kedua tabel hanya dapat diakses oleh service-role dan dilindungi RLS.
