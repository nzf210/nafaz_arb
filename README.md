# NAFAZ V3 — Sistem Arbitrase Spot Indonesia dan Perpetual Global

NAFAZ adalah sistem pemantauan dan eksekusi arbitrase dua kaki: membeli aset pada pasar spot lokal Indonesia dan membuka posisi short pada pasar perpetual global. Sistem menggunakan Go untuk pemindaian pasar, Python untuk eksekusi, Redis untuk koordinasi waktu nyata, serta Supabase/PostgreSQL untuk jejak audit dan riwayat.

> **Status:** V3 adalah jalur pengembangan menuju produksi. Status LIVE harus dibuktikan melalui pengujian, pemeriksaan akun bursa, rekonsiliasi, dan verifikasi deployment. Keberhasilan kompilasi atau CI saja bukan bukti bahwa transaksi nyata aman atau menguntungkan.

## Prinsip yang tidak boleh dilanggar

1. Spot lokal hanya membeli dan menjual aset spot; perpetual global hanya membuka/menutup short sesuai strategi.
2. Tidak ada pemindahan koin antar-bursa sebagai bagian dari alur arbitrase.
3. Identitas pasangan memuat bursa, simbol asli, jenis pasar, aset dasar, dan aset kuotasi untuk kedua kaki. Nama simbol yang mirip tidak boleh dianggap identik.
4. Kuantitas kedua kaki dinormalisasi menggunakan aturan lot, minimum order, dan ukuran kontrak resmi bursa.
5. Biaya, pendanaan, spread, kedalaman order book, slippage, dan kurs IDR/USDT harus berasal dari data segar atau konfigurasi konservatif yang diberi label sumbernya.
6. AI boleh memilih dan mengoptimalkan keputusan dalam batas yang ditetapkan, tetapi tidak boleh mengubah batas risiko keras, mematikan kill switch, atau menarik dana.
7. Kegagalan membaca saldo, posisi, order, harga, atau status rekonsiliasi harus menahan entri baru pada bursa/pasangan terdampak. Posisi terbuka tetap harus dikelola dan dipulihkan.

## Arsitektur

- `cmd/nafaz-scanner`: proses pemindaian dan API.
- `internal/config`: konfigurasi dan validasi.
- `internal/domain`: kontrak data pasar dan peluang.
- `internal/exchange`: adaptor data pasar bursa.
- `internal/scanner`: penemuan pasangan, evaluasi, kalkulasi, dan pemeringkatan.
- `internal/redishandler`: sinyal, harga langsung, dan koordinasi.
- `internal/history`: riwayat peluang.
- `internal/telegram`: operasi dan notifikasi Telegram.
- `executor/`: mesin eksekusi Python, pemulihan order, pengelolaan posisi, dan pengendalian risiko.
- `supabase/`: migrasi skema, pengujian skema, dan dokumentasi persistensi.

Rancangan pengembangan V3 dijelaskan pada [Arsitektur dan rencana V3](docs/ARSITEKTUR-V3.md). Matriks cakupan bursa dan tahapan integrasinya ada di [Matriks CEX V3](docs/MATRIKS-CEX-V3.md).

## Menjalankan pengujian

Prasyarat mengikuti versi Go pada `go.mod` dan Python 3.12 pada CI.

```bash
go test ./...
go test -race ./...
go vet ./...
python -m pip install -r requirements.txt
pytest -q
```

CI juga menjalankan pemeriksaan migrasi Supabase dan pembangunan kedua image kontainer. Jalankan seluruh pemeriksaan sebelum membuat rilis.

## Menjalankan mode pengembangan

```bash
cp config.example.json config.json
# Pastikan mode tetap MOCK selama pengembangan dan pengujian.
go run ./cmd/nafaz-scanner -config config.json
```

Jangan menaruh kunci API, token Telegram, atau kunci Supabase ke Git. Gunakan secret environment atau berkas konfigurasi yang hanya tersedia pada host.

## Produksi dan keamanan

- Gunakan kunci API dengan izin minimum yang diperlukan; **jangan pernah mengaktifkan izin penarikan dana**.
- Pisahkan mode scanner LIVE dan executor LIVE. Aktifkan secara bertahap setelah pengujian paper dan verifikasi perilaku setiap venue.
- Pastikan Redis, jurnal outbox, basis data, dan direktori data berada pada penyimpanan persisten serta memiliki pemantauan kapasitas.
- Terapkan migrasi Supabase secara berurutan setelah meninjau skema produksi dan status migrasi.
- Pastikan pemulihan order/posisi dari API bursa berjalan sebelum membuka entri otomatis.
- Sediakan prosedur kill switch, penghentian entri, rekonsiliasi manual, pemulihan cadangan, dan rollback rilis.
- Jangan menyatakan PnL sebagai hasil bersih final sebelum biaya aktual setiap fill dan pendanaan direkonsiliasi dari ledger bursa.

Panduan deployment yang sudah ada tetap tersedia di [PODMAN.md](PODMAN.md), sedangkan rincian skema dan persistensi ada di [supabase/README.md](supabase/README.md).

## Bahasa dan dokumentasi

Dokumentasi baru V3, panduan operasional, komentar kode yang ditambahkan, pesan kesalahan baru, serta penjelasan konfigurasi ditulis dalam Bahasa Indonesia. Istilah API, nama protokol, nama field, nama fungsi, dan identifier teknis dipertahankan bila diperlukan agar kontrak perangkat lunak tidak berubah.
