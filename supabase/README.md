# Basis Data Supabase NAFAZ

Supabase menyimpan riwayat operasional NAFAZ secara persisten. Redis tetap menjadi lapisan koordinasi berlatensi rendah antara scanner Go dan executor Python. Outbox lokal menjaga catatan audit agar tidak hilang ketika proses restart atau Supabase sementara tidak tersedia.

## 1. Skema

Migrasi awal dan migrasi tambahan membentuk tabel utama berikut:

- `exchange_accounts`: metadata akun bursa dan slot kredensial terenkripsi. Kolom kredensial hanya boleh berisi ciphertext.
- `strategy_configs` dan `risk_limits`: parameter strategi dan batas risiko yang dapat diberi versi.
- `market_pairs`: pemetaan pasar spot lokal dengan perpetual global.
- `scanner_opportunities`: snapshot peluang yang dievaluasi scanner.
- `positions` dan `position_legs`: siklus hidup posisi dan masing-masing kaki.
- `orders`, `order_fills`, dan `order_registry`: siklus order, fill individual, serta registri order universal untuk pemulihan setelah restart.
- `funding_payments`: settlement funding bursa, dengan pemisahan jelas antara estimasi dan catatan terkonfirmasi.
- `closed_trades`: snapshot PnL transaksi yang telah ditutup.
- `balance_snapshots`: observasi saldo historis.
- `risk_events` dan `executor_events`: jurnal peristiwa risiko dan executor.
- `executor_status_snapshots`: snapshot status untuk pemantauan dan analisis insiden.

Seluruh nilai finansial menggunakan PostgreSQL `NUMERIC`, bukan floating point. Tabel dilindungi RLS dan tidak memberikan akses kepada peran `anon` maupun `authenticated`. Komponen server memakai kredensial service-role saja. Jangan pernah menaruh kunci tersebut pada Vue, Telegram, atau klien lain.

## 2. Status persistensi runtime

Supabase merupakan tujuan audit/riwayat yang bersifat best-effort. Supabase bukan sumber kebenaran eksposur LIVE dan tidak berada pada jalur kritis pengiriman order.

### Penulisan yang sudah tersedia

- Scanner Go menulis snapshot peluang ke `scanner_opportunities`. Kunci idempotensi memakai `scan_id + pair_key`; identitas rute mencakup venue dan simbol spot maupun perpetual sehingga rute paralel tidak saling menimpa.
- Executor Python menulis `positions`, `orders`, `order_fills`, `closed_trades`, `executor_events`, dan `risk_events`.
- Setiap order registry memastikan baris induk per kaki pada tabel `orders`.
- Fill individual diambil dari riwayat transaksi bursa yang terautentikasi dan menggunakan ID trade bursa yang tidak berubah. Kuantitas, harga, nilai kuotasi, jumlah/jenis aset biaya, dan waktu eksekusi disimpan.
- `fee_verified` membedakan detail biaya yang lengkap dari payload biaya yang belum lengkap. Fill historis berbasis snapshot kumulatif diberi label `legacy_cumulative`, bukan dicampur dengan fill bursa yang telah diverifikasi.
- `position_legs` diperbarui secara terpisah untuk kaki spot dan perpetual.
- Settlement funding yang dikonfirmasi bursa ditulis ke `funding_payments` dengan ID deterministik.
- Snapshot saldo spot dan perpetual ditambahkan ke `balance_snapshots`.
- `executor_status_snapshots` masih sebatas skema dan belum menjadi penulis runtime.

### Sumber kebenaran

Redis dipakai untuk posisi aktif, lock, harga langsung, saldo cache, Pub/Sub, dan koordinasi cepat. API bursa tetap menjadi sumber kebenaran saldo aktual, status order, fill, serta eksposur posisi. Supabase hanya menjadi jurnal audit dan analitik.

Worker rekonsiliasi eksposur membandingkan kecukupan aset dasar spot dengan kuantitas yang dikelola dan membandingkan posisi short perpetual aktual dengan posisi yang tercatat. Ketidaksesuaian terkonfirmasi atau kegagalan pembacaan pada venue yang memiliki eksposur terkelola akan menahan entri baru pada venue tersebut, sementara pengelolaan posisi terbuka tetap berjalan. Saldo spot dipakai sebagai pemeriksaan cakupan minimum; kelebihan saldo manual tidak dianggap sebagai milik bot.

Worker rekonsiliasi fill membaca riwayat trade privat spot dan perpetual saat startup serta setiap 120 detik secara default. Indodax menggunakan endpoint TAPI v2 `/api/v2/myTrades` dalam jendela maksimal tujuh hari; venue CCXT yang didukung memakai `fetch_my_trades`. Riwayat yang tidak tersedia atau tidak lengkap tidak boleh dianggap sebagai nol fill. Pasangan terkait tetap memerlukan rekonsiliasi sebelum entri baru diizinkan.

## 3. Jaminan outbox dan pemulihan

- Outbox executor menggunakan SQLite dengan WAL dan `synchronous=FULL` pada `SUPABASE_OUTBOX_PATH`, default `/app/data/executor/supabase-outbox.sqlite3`. Berkas wajib berada pada volume persisten lokal.
- Worker mengirim ulang data ke PostgREST memakai exponential backoff terbatas. Baris baru dihapus setelah upsert idempoten berhasil. Kegagalan skema/perizinan tetap berada di antrean sampai diperbaiki.
- Fill individual dan penanda idempotensinya disimpan dalam satu transaksi SQLite. Total fill trade unik dibandingkan dengan kuantitas kumulatif order. Riwayat yang hilang, endpoint privat yang tidak didukung, atau jumlah yang tidak cocok memicu status perlu rekonsiliasi; sistem tidak membuat fill sintetis dari harga rata-rata order.
- Antrean memori hanya menjadi sinyal bangun worker. Antrean penuh tidak membuang catatan yang sudah di-commit ke disk. Outbox executor memiliki batas 50.000 baris; kapasitas atau kegagalan disk harus dipantau sebagai kondisi kritis.
- Outbox scanner berupa jurnal JSONL append-only pada `SUPABASE_SCANNER_OUTBOX_PATH`, default `/app/data/scanner/supabase-outbox.jsonl`. Setiap append di-fsync sebelum fungsi penyimpanan mengembalikan hasil. Batas bawaan jurnal adalah 128 MiB.
- Scanner mengakui pengiriman hanya setelah upsert PostgREST berhasil. Kegagalan jaringan/HTTP tetap tersimpan dengan jadwal retry. Catatan committed yang rusak membuat pemulihan gagal secara aman; append terakhir yang belum lengkap dapat dipotong saat pemulihan.
- Kapasitas penuh, disk penuh, atau izin tulis yang salah harus ditangani operator. Jangan menghapus catatan committed otomatis hanya untuk menyediakan ruang.
- Kedua jurnal bersifat lokal untuk satu proses/host dan tidak boleh dipakai bersama oleh beberapa replika. Gunakan volume persisten dan buat cadangan sebelum mengganti host atau volume.

Worker rekonsiliasi scanner hanya mengirim ulang snapshot peluang. Rekonsiliasi eksposur bursa dilakukan terpisah oleh executor Python dan bersifat baca-saja; worker ini tidak mengirim atau membatalkan order.

## 4. Konfigurasi runtime

Masukkan variabel berikut ke lingkungan server dan teruskan ke container scanner serta executor:

- `SUPABASE_URL=https://<project-ref>.supabase.co`
- `SUPABASE_KEY=<kunci service_role khusus server>`
- `SUPABASE_OUTBOX_PATH=/app/data/executor/supabase-outbox.sqlite3` (executor)
- `SUPABASE_SCANNER_OUTBOX_PATH=/app/data/scanner/supabase-outbox.jsonl` (scanner)

Nama `SUPABASE_KEY` dipertahankan untuk kompatibilitas. Nilainya wajib berupa secret `service_role`, bukan kunci anon/publishable. Scanner menolak URL remote non-HTTPS; HTTP hanya diizinkan untuk pengujian loopback. Jika URL dan kunci sama-sama tidak tersedia, persistensi dinonaktifkan. Jika hanya salah satu tersedia, scanner memberi peringatan dan berjalan tanpa persistensi.

Kedua lokasi outbox harus berada pada penyimpanan persisten lokal dan dapat ditulis oleh UID container. Compose memasang `./data` ke `/app/data`. Jangan memasukkan kredensial ke Git, `config.example.json`, image, frontend, atau Telegram. Gunakan pengelola secret host atau environment container/systemd yang terlindungi. Berkas `.env` yang diabaikan Git tetap tidak aman jika dibagikan sebagai teks biasa.

## 5. Menerapkan migrasi

1. Pasang Supabase CLI dan lakukan autentikasi.
2. Hubungkan repositori ke proyek yang benar: `supabase link --project-ref <project-ref>`.
3. Periksa riwayat remote: `supabase migration list --remote`.
4. Tinjau SQL dan skema remote sebelum menerapkan: `supabase db push --remote`.

CI menerapkan seluruh migrasi berurutan secara leksikografis, menjalankan ulang migrasi untuk memeriksa idempotensi, lalu menjalankan `supabase/tests/schema_smoke.sql` terhadap PostgreSQL 16. Migrasi baseline awal tidak boleh diubah setelah digunakan. Jangan menerapkan migrasi ke proyek produksi sebelum riwayat migrasi dan skema yang sudah ada diperiksa.
