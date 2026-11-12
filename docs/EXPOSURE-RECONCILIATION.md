# Rekonsiliasi eksposur bursa LIVE

Pada mode LIVE, executor menjalankan worker rekonsiliasi eksposur baca-saja. Worker membandingkan kuantitas aset dasar spot yang dikelola dan short futures dengan hasil pembacaan bursa terautentikasi. Worker tidak pernah mengirim, mengubah, atau membatalkan order.

## Pemeriksaan

- **Spot:** jumlahkan kuantitas aset dasar yang dikelola berdasarkan bursa lokal dan aset dasar. Total saldo aset dasar di bursa harus minimal sama dengan kuantitas yang dikelola. Saldo lebih besar bukan ketidaksesuaian karena akun dapat memiliki inventaris manual atau aset lain yang tidak terkait.
- **Perpetual:** jumlahkan kuantitas short terkelola berdasarkan bursa global dan simbol kontrak, lalu bandingkan dengan kuantitas short aktual di bursa. Posisi long yang tidak diharapkan, data bertanda/berformat salah, atau data posisi yang tidak tersedia tidak boleh dianggap sebagai posisi nol.
- Snapshot saldo dicatat ke tabel Supabase `balance_snapshots` melalui outbox lokal executor yang persisten.

Ketidaksesuaian yang terkonfirmasi menahan entri baru pada venue terdampak dan menerbitkan peristiwa kegagalan eksekusi untuk Telegram/operasi. Status posisi aktif tidak diubah dan pemantauan/penutupan posisi yang sudah terbuka tetap berjalan. Setelah ketidaksesuaian tertentu hilang, penghentian rekonsiliasi untuk masalah tersebut dilepas kecuali masih ada masalah lain pada venue yang sama.

Jika pemetaan bursa untuk posisi aktif yang dipulihkan tidak tersedia, worker melewati pemetaan spot/kontrak tersebut dan tidak menebak simbol. Pemeriksaan pemulihan futures saat startup tetap dijalankan.

## Konfigurasi

- `EXPOSURE_RECONCILIATION_INTERVAL_SECONDS`: bawaan `300`, minimum `60`.
- `EXPOSURE_RECONCILIATION_REL_TOLERANCE`: bawaan `0.005` (0,5%), maksimum `0.05` (5%).
- `EXPOSURE_RECONCILIATION_ABS_TOLERANCE`: bawaan `1e-8` unit aset dasar.

Worker dinonaktifkan pada mode MOCK/PAPER. Worker memerlukan metode baca-saja adaptor `fetch_balance_report` dan `fetch_positions`.

**Batasan cakupan saat dokumen ini ditulis:** adaptor executor dikonfigurasi untuk satu venue spot lokal dan satu venue perpetual global per instans executor. Worker ini tidak membuat klien bursa tambahan untuk venue lain. Posisi yang dibuat versi ini menyimpan aset dasar dan simbol futures dalam catatan posisi aktif Redis. Catatan lama tanpa metadata tersebut ditandai sebagai pemetaan tidak tersedia dan menahan entri baru pada venue yang diketahui sampai pemetaan dipulihkan atau diselesaikan manual.

Worker ini tidak merekonsiliasi riwayat order/fill individual, biaya, transfer, deposit, penarikan, atau settlement funding; hal tersebut membutuhkan rekonsiliasi ledger terpisah.
