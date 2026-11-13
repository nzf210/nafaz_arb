# Checklist Audit Manual Terperinci — Nafazbot

- Repositori: `nzf210/nafaz_arb`
- Cabang audit: `audit/architecture-baseline-regression-contract`
- Cabang acuan kode: `feat/unified-dynamic-fee-engine`
- Komit acuan: `9cc3dcd11d0999e828309053ba4cffce98a28675`
- Tujuan: panduan pemeriksaan manual sebelum perubahan arsitektur atau implementasi.
- Batasan: checklist ini adalah dokumen audit. Mencentang item tidak mengubah kode, konfigurasi, basis data, atau sistem produksi.

## Cara menggunakan checklist

Untuk setiap butir:
1. Buka berkas dan fungsi yang disebutkan pada cabang acuan.
2. Ikuti pemanggil fungsi sampai ke lapisan API, penyimpanan, pekerja latar, dan perintah Telegram yang terkait.
3. Cari bukti berupa kode, pengujian, catatan log, atau hasil pemeriksaan endpoint resmi. Jangan menandai selesai hanya karena nama fungsi tersedia.
4. Catat jalur berkas, nama fungsi, nomor baris/komit, bukti pengujian, dan temuan.
5. Pilih satu status: **LULUS**, **GAGAL**, **BELUM DIPERIKSA**, atau **TIDAK BERLAKU**.
6. Jika bukti belum lengkap, gunakan **BELUM DIPERIKSA**; jangan menganggapnya lulus.

### Lembar hasil pemeriksaan

| ID | Status | Bukti (berkas/fungsi/baris/hasil uji) | Temuan dan tindakan |
|---|---|---|---|
| | BELUM DIPERIKSA | | |

---

## A. Pengamanan audit dan batas perubahan

- [ ] A-01 — Pastikan cabang aktif adalah `audit/architecture-baseline-regression-contract`.
  - Bukti: nama cabang dan komit kepala cabang.
  - Kriteria lulus: pemeriksaan dan perubahan dokumen hanya dilakukan pada cabang audit ini.
- [ ] A-02 — Pastikan cabang acuan tetap `feat/unified-dynamic-fee-engine` dan komit acuannya tercatat.
  - Kriteria lulus: tidak ada perubahan kode sumber pada cabang acuan akibat pekerjaan dokumentasi.
- [ ] A-03 — Pastikan tidak ada perubahan langsung ke sistem produksi, kredensial, konfigurasi aktif, atau migrasi basis data.
  - Kriteria lulus: pekerjaan audit hanya menghasilkan dokumentasi.
- [ ] A-04 — Setiap klaim dukungan bursa harus memiliki bukti implementasi dan pengujian, bukan hanya konfigurasi atau keberadaan pustaka.
- [ ] A-05 — Semua temuan yang belum dibuktikan ditandai BELUM DIPERIKSA atau PERLU VERIFIKASI, bukan LULUS.

## B. Peta arsitektur dan batas tanggung jawab

- [ ] B-01 — Petakan proses Go yang menjalankan pemindaian peluang dan data pasar publik.
  - Periksa: `cmd/nafaz-scanner/main.go`, `internal/scanner/`, `internal/exchange/`.
  - Lulus bila: alur penemuan pasar, buku pesanan, ticker, kurs, dan pendanaan dapat ditelusuri sampai sumber data.
- [ ] B-02 — Petakan proses Python yang menjalankan strategi, risiko, eksekusi, order, dan pemulihan.
  - Periksa: `executor/engine*.py`, `executor/exchange*.py`, `executor/*recovery*.py`.
  - Lulus bila: pemilik setiap perubahan status order dan posisi diketahui.
- [ ] B-03 — Petakan Redis sebagai antrean sementara, cache, kunci koordinasi, dan notifikasi.
  - Lulus bila: tidak ada asumsi bahwa Redis Pub/Sub atau antrean peluang merupakan catatan transaksi permanen.
- [ ] B-04 — Petakan Supabase/Postgres sebagai penyimpanan tahan lama untuk identitas order, status, risiko, dan catatan audit.
  - Lulus bila: jalur tulis/baca dan penanganan kegagalan penyimpanan diketahui.
- [ ] B-05 — Petakan Telegram sebagai antarmuka operator, bukan sumber kebenaran status order.
  - Lulus bila: perintah operator diteruskan ke layanan yang berwenang dan hasilnya dapat diaudit.
- [ ] B-06 — Pastikan batas data publik dan API privat terpisah.
  - Lulus bila: permintaan data pasar publik tidak memakai kredensial privat dan semua operasi akun/order melewati batas API privat yang tercatat.
- [ ] B-07 — Identifikasi semua pekerja latar, jadwal, proses, dan layanan yang aktif pada konfigurasi penerapan.
  - Lulus bila: setiap pekerja memiliki pemilik, masukan, keluaran, mekanisme berhenti, dan kebijakan percobaan ulang yang terdokumentasi.

## C. Inventaris seluruh pemanggilan API privat

Periksa sekurangnya: `executor/exchange_client.py`, `executor/exchange_orders.py`, `executor/exchange_futures.py`, `executor/exchange_trades.py`, `executor/exchange_fee_balance.py`, `executor/exchange_common.py`, `executor/indodax_v2.py`, seluruh `executor/venues/*.py`, pekerja pemulihan, rekonsiliasi, saldo, dan pemantauan.

- [ ] C-01 — Cari seluruh pemanggilan CCXT terautentikasi: buat order, batalkan order, ambil order, order terbuka, riwayat, transaksi, saldo, posisi, biaya, dan pendanaan privat.
  - Lulus bila: setiap pemanggilan memiliki lokasi, bursa, jenis pasar, pemanggil, tujuan, dan kelas prioritas.
- [ ] C-02 — Cari metode CCXT mentah/hasil generasi otomatis, termasuk metode `privateGet...`, `privatePost...`, `privateDelete...`, dan variasinya.
  - Lulus bila: tidak ada jalur privat tersembunyi di luar peta API.
- [ ] C-03 — Cari permintaan HTTP langsung melalui `aiohttp`, `requests`, `httpx`, atau klien lain.
  - Lulus bila: setiap permintaan yang memakai kunci/ tanda tangan akun masuk dalam inventaris.
- [ ] C-04 — Untuk setiap API, catat metode HTTP, jalur endpoint, parameter wajib, jenis autentikasi, format tanda tangan, dan bentuk respons.
- [ ] C-05 — Catat batas permintaan, bobot endpoint, batas serentak, jeda pemulihan, dan header `Retry-After` berdasarkan dokumentasi resmi versi yang berlaku.
  - Lulus bila: nilai tidak ditebak dan menyertakan tautan/sumber serta tanggal verifikasi.
- [ ] C-06 — Catat batas halaman, urutan hasil, rentang waktu, ukuran halaman, dan cara mengambil seluruh riwayat.
  - Lulus bila: pemulihan tidak menyimpulkan order tidak ada hanya karena satu halaman kosong.
- [ ] C-07 — Catat batas waktu koneksi/baca, perilaku pembatalan, dan klasifikasi kesalahan untuk setiap jalur.
- [ ] C-08 — Pastikan seluruh permintaan privat mempunyai identitas permintaan, pencatatan metrik, dan penanganan kegagalan.
- [ ] C-09 — Pastikan tidak ada percobaan ulang otomatis untuk pengiriman order yang hasilnya belum diketahui.
- [ ] C-10 — Pastikan tidak ada pemanggilan privat langsung yang melewati pembatas permintaan bersama setelah rancangan pembatas tersebut diterapkan.
- [ ] C-11 — Verifikasi versi CCXT yang benar-benar terkunci pada dependensi dan cocokkan nama metode dengan versi itu.
- [ ] C-12 — Pisahkan bukti kode, bukti pengujian, dan verifikasi langsung/sandbox. Ketiganya tidak boleh dianggap saling menggantikan.

## D. Pemeriksaan per bursa

### Tokocrypto — Spot
- [ ] D-TK-01 — Verifikasi pembuatan beli/jual limit, jual pasar, pembatalan, status order, daftar order terbuka, riwayat, transaksi pribadi, saldo, dan biaya.
- [ ] D-TK-02 — Verifikasi pemetaan ID order bursa dan ID klien pada seluruh jalur pemulihan.
- [ ] D-TK-03 — Verifikasi metode mentah `privateGetOpenV1Orders` dan bentuk respons menurut versi CCXT terpasang.
- [ ] D-TK-04 — Verifikasi `privateGetOpenV1AccountSpot` dan apakah data yang dikembalikan benar-benar biaya perdagangan yang dipakai.
- [ ] D-TK-05 — Verifikasi paginasi, batas waktu, batas permintaan, presisi harga/jumlah, minimum order, dan status terminal.
- [ ] D-TK-06 — Verifikasi dukungan buku pesanan WebSocket per simbol; jika belum terbukti, tandai belum didukung dan pastikan fallback REST terkendali.
- [ ] D-TK-07 — Pastikan konfigurasi saldo Tokocrypto tidak disalahartikan sebagai bukti bahwa semua fitur trading telah siap.

### Indodax — Spot TAPIv2
- [ ] D-ID-01 — Verifikasi penandatanganan TAPIv2: nonce, payload, HMAC-SHA256, header, dan sinkronisasi waktu.
- [ ] D-ID-02 — Verifikasi `POST /api/v2/order`, termasuk format simbol, tipe order, jumlah, harga, dan respons.
- [ ] D-ID-03 — Verifikasi `DELETE /api/v2/order` dan konfirmasi hasil pembatalan melalui pembacaan status.
- [ ] D-ID-04 — Verifikasi `GET /api/v2/order` berdasarkan ID order dan ID klien bila didukung.
- [ ] D-ID-05 — Verifikasi `GET /api/v2/openOrders`, `GET /api/v2/order/histories`, `GET /api/v2/myTrades`, dan `GET /api/v2/account`.
- [ ] D-ID-06 — Verifikasi seluruh paginasi, filter simbol, rentang waktu, dan batas jumlah hasil.
- [ ] D-ID-07 — Verifikasi perilaku post-only, maker-only, time-in-force, dan penolakan order; jangan menyimpulkan dukungan hanya dari parameter lokal.
- [ ] D-ID-08 — Verifikasi format desimal, presisi, minimum order, status order, dan pemetaan jumlah terisi.
- [ ] D-ID-09 — Verifikasi batas permintaan dan perilaku pembatas khusus Indodax.
- [ ] D-ID-10 — Verifikasi WebSocket buku pesanan publik, pembaruan langganan simbol, snapshot awal, celah urutan, reconnect, dan data kedaluwarsa.

### OKX — Futures/SWAP
- [ ] D-OK-01 — Verifikasi pembuatan posisi short dan penutupan reduce-only.
- [ ] D-OK-02 — Verifikasi `posSide`, `reduceOnly`, mode posisi akun, margin mode, dan parameter kontrak.
- [ ] D-OK-03 — Verifikasi pengenal klien, pencarian order setelah hasil pengiriman tidak diketahui, dan normalisasi ID.
- [ ] D-OK-04 — Verifikasi pembacaan posisi, order terbuka, riwayat order, transaksi pribadi, saldo, dan riwayat pendanaan.
- [ ] D-OK-05 — Verifikasi paginasi, cursor, batas permintaan per endpoint, dan respons rate limit.
- [ ] D-OK-06 — Verifikasi snapshot dan pembaruan WebSocket buku pesanan, sequence/checksum, reconnect, serta resinkronisasi.
- [ ] D-OK-07 — Pastikan parameter kontrak dan ukuran lot tidak tertukar dengan kuantitas aset Spot.

### MEXC — Futures/SWAP
- [ ] D-MX-01 — Verifikasi pembuatan short dan penutupan posisi dengan parameter khusus kontrak.
- [ ] D-MX-02 — Verifikasi `defaultType=swap`, parameter `type=swap`, simbol kontrak, dan pemetaan pasar.
- [ ] D-MX-03 — Verifikasi pembacaan posisi, order, order terbuka, transaksi pribadi, saldo, dan pendanaan.
- [ ] D-MX-04 — Verifikasi ID klien, pemulihan order ambigu, paginasi, presisi, ukuran kontrak, dan minimum order.
- [ ] D-MX-05 — Verifikasi batas permintaan dan penanganan kesalahan spesifik MEXC.
- [ ] D-MX-06 — Verifikasi apakah WebSocket benar-benar memelihara buku per simbol; koneksi socket generik saja bukan bukti.

### Binance dan Bybit — klien saldo tambahan
- [ ] D-BB-01 — Pastikan penggunaan yang terbukti hanya pembacaan saldo atau catat bukti tambahan jika ada.
- [ ] D-BB-02 — Jangan tandai siap trading sebelum alur order, pembatalan, posisi, transaksi, pemulihan, dan pengujian khusus bursa tersedia.
- [ ] D-BB-03 — Verifikasi jenis akun/pasar, mode posisi, simbol, batas permintaan, dan izin kunci API.
- [ ] D-BB-04 — Pastikan tidak ada pemilihan bursa aktif secara tidak sengaja hanya karena kredensial tersedia.

## E. Siklus hidup order dan ketahanan terhadap kegagalan

- [ ] E-01 — Niat order disimpan secara tahan lama sebelum permintaan buat order LIVE dikirim.
- [ ] E-02 — Kegagalan penyimpanan niat order menghentikan pengiriman order.
- [ ] E-03 — Setiap order memiliki identitas internal, identitas bursa, ID klien, akun, bursa, pasar, simbol, dan pasangan strategi.
- [ ] E-04 — Hasil pengiriman yang timeout/putus koneksi menjadi status **SUBMISSION_UNKNOWN**, bukan otomatis gagal.
- [ ] E-05 — Sebelum mencoba ulang order ambigu, sistem mencari berdasarkan ID klien, ID bursa, order terbuka, riwayat, dan transaksi.
- [ ] E-06 — Pembatalan dianggap permintaan; status akhir harus dikonfirmasi lewat pembacaan bursa.
- [ ] E-07 — Perlombaan antara fill dan cancel ditangani tanpa kehilangan jumlah terisi.
- [ ] E-08 — Fill parsial, fill berulang, dan event duplikat tidak menggandakan posisi atau PnL.
- [ ] E-09 — Identitas transaksi bursa dipakai untuk deduplikasi fill.
- [ ] E-10 — Jumlah fill kumulatif order dicocokkan dengan total transaksi.
- [ ] E-11 — Order yang tidak dikenal bot dikarantina sebagai order asing/orphan; jangan menghapus atau menganggapnya aman.
- [ ] E-12 — Order terbuka tetap dapat dikenali setelah proses atau server dimulai ulang.
- [ ] E-13 — Status terminal tidak ditimpa oleh pembaruan lama yang datang terlambat.
- [ ] E-14 — Semua perubahan status penting mempunyai jejak audit dan waktu kejadian.

## F. Eksekusi dua kaki Spot dan Futures

- [ ] F-01 — Status kaki Spot dan Futures disimpan dan dipulihkan secara independen.
- [ ] F-02 — Fill Spot tidak dianggap bukti bahwa kaki Futures sudah berhasil, dan sebaliknya.
- [ ] F-03 — Kegagalan salah satu kaki memicu tindakan kompensasi sesuai jumlah yang benar-benar terisi.
- [ ] F-04 — Kuantitas hedge dihitung dari eksposur aktual, bukan hanya jumlah order awal.
- [ ] F-05 — Restart di antara pengiriman kedua kaki tidak menyebabkan pengiriman ulang tanpa rekonsiliasi.
- [ ] F-06 — Penutupan, rollback, force-close, dan pemulihan mempertahankan pemisahan status kedua kaki.
- [ ] F-07 — Ketidaksesuaian posisi aktual dan catatan internal memblokir penambahan eksposur.

## G. Risiko, saldo, posisi, dan rekonsiliasi

- [ ] G-01 — Status risiko penting dipersistenkan dan dipulihkan setelah restart.
- [ ] G-02 — Status tidak diketahui tidak diperlakukan sebagai saldo nol, posisi kosong, atau kondisi sehat.
- [ ] G-03 — Circuit breaker memiliki alasan, waktu, sumber, dan mekanisme pemulihan yang tercatat.
- [ ] G-04 — Saldo bebas, saldo total, saldo terkunci, dan nilai posisi dibedakan.
- [ ] G-05 — Cache saldo/posisi memiliki waktu pembaruan dan batas kedaluwarsa.
- [ ] G-06 — Kegagalan pembacaan saldo/posisi tidak berubah menjadi nilai nol yang tampak valid.
- [ ] G-07 — Rekonsiliasi membandingkan catatan internal dengan order, fill, saldo, dan posisi bursa.
- [ ] G-08 — Selisih yang belum terselesaikan menghasilkan status peringatan/blokir yang eksplisit.
- [ ] G-09 — Riwayat pendanaan dan biaya tidak dihitung dua kali.
- [ ] G-10 — Perhitungan keuntungan bersih tetap memakai biaya yang tervalidasi dan tidak menganggap biaya yang hilang sebagai nol.

## H. Batas order Spot dan koordinasi lintas proses

- [ ] H-01 — Batas maksimum order terbuka dan order tertunda dihitung per bursa, akun, dan pasar yang benar.
- [ ] H-02 — Reservasi slot dilakukan atomik sebelum order dikirim.
- [ ] H-03 — Dua pekerja/proses tidak dapat melewati batas dengan membaca slot kosong secara bersamaan.
- [ ] H-04 — Slot dilepas hanya setelah status order terminal dikonfirmasi.
- [ ] H-05 — Order yang statusnya tidak diketahui tetap menggunakan slot sampai direkonsiliasi.
- [ ] H-06 — Kunci eksekusi memiliki pemilik, masa berlaku, heartbeat, dan token fencing.
- [ ] H-07 — Hilangnya kepemilikan kunci memblokir pengiriman order baru.
- [ ] H-08 — Kunci lokal asyncio tidak dianggap sebagai pengaman lintas server/proses.

## I. WebSocket dan data pasar

- [ ] I-01 — Status langganan dicatat per bursa, pasar, dan simbol.
- [ ] I-02 — Snapshot awal diterima sebelum pembaruan inkremental dipakai.
- [ ] I-03 — Sequence/update ID/checksum diperiksa jika disediakan bursa.
- [ ] I-04 — Celah urutan, pesan rusak, disconnect, atau data basi mengubah status menjadi tidak layak dipakai.
- [ ] I-05 — Data basi atau buku tidak sinkron tidak boleh menghasilkan sinyal.
- [ ] I-06 — Reconnect melakukan snapshot/resinkronisasi sebelum status LIVE dipulihkan.
- [ ] I-07 — Umur data dihitung dari waktu bursa dan waktu penerimaan lokal.
- [ ] I-08 — REST hanya dipakai untuk snapshot, resinkronisasi, metadata, atau fallback terkendali dengan batas anggaran.
- [ ] I-09 — Koneksi WebSocket yang hidup tidak dianggap bukti semua simbol aktif dan segar.
- [ ] I-10 — Telemetri mencatat cakupan simbol, jeda, disconnect, gap, dan jumlah fallback REST.

## J. Antrean, perintah darurat, dan Telegram

- [ ] J-01 — Antrean peluang dibedakan dari jurnal perintah eksekusi yang tahan lama.
- [ ] J-02 — Perintah cancel/force-close disimpan secara tahan lama sebelum notifikasi dikirim.
- [ ] J-03 — Redis Pub/Sub hanya menjadi pemicu/pemberitahuan, bukan satu-satunya salinan perintah.
- [ ] J-04 — Perintah mempunyai ID unik, status, pemilik, percobaan, batas waktu, dan tanda selesai.
- [ ] J-05 — Perintah yang gagal saat pekerja mati dapat diklaim ulang tanpa menggandakan aksi.
- [ ] J-06 — Telegram menampilkan status yang berasal dari catatan order/posisi yang otoritatif.
- [ ] J-07 — Perintah operator divalidasi otorisasi, parameter, cakupan akun, dan dampak risikonya.
- [ ] J-08 — Hasil perintah darurat, kegagalan, dan pengakuan penyelesaian tercatat dan dapat ditelusuri.

## K. Pengujian regresi wajib

- [ ] K-01 — Jalankan seluruh pengujian yang sudah ada sebelum perubahan dan simpan hasilnya sebagai pembanding.
- [ ] K-02 — Tambahkan pengujian untuk setiap perilaku yang disentuh; jangan menghapus atau melemahkan pengujian lama.
- [ ] K-03 — Uji timeout setelah bursa menerima order tetapi sebelum respons diterima.
- [ ] K-04 — Uji restart setelah niat tersimpan, setelah pengiriman, setelah fill parsial, dan saat cancel.
- [ ] K-05 — Uji fill/cancel bersamaan, event duplikat, event terlambat, dan paginasi terpotong.
- [ ] K-06 — Uji kegagalan Redis, Supabase, WebSocket, dan API bursa secara terpisah.
- [ ] K-07 — Uji dua pekerja bersamaan untuk batas order, kunci, dan fencing.
- [ ] K-08 — Uji setiap bursa dengan fixture respons khusus venue serta pengujian sandbox/aman bila tersedia.
- [ ] K-09 — Uji strategi Adaptive Maker, biaya, sizing, rollback, force-close, dan eksekusi dua kaki sebagai uji regresi.
- [ ] K-10 — Pastikan pengujian tidak membutuhkan kredensial LIVE atau mengirim order sungguhan.
- [ ] K-11 — Jalankan pemeriksaan format, lint, tipe, keamanan, dan CI sesuai pipeline repositori.
- [ ] K-12 — Catat perintah, komit, hasil, durasi, dan kegagalan; jangan menyatakan hijau tanpa bukti.

## L. Kriteria keputusan sebelum implementasi

- [ ] L-01 — Semua API privat sudah dipetakan sampai pemanggil dan pekerja latar.
- [ ] L-02 — Semua bursa yang akan diaktifkan memiliki matriks kemampuan yang dibuktikan.
- [ ] L-03 — Semua butir kritis order, pemulihan, risiko, dan dua kaki berstatus LULUS.
- [ ] L-04 — Butir BELUM DIPERIKSA/GAGAL memiliki pemilik, bukti yang kurang, dan rencana penyelesaian.
- [ ] L-05 — Kontrak regresi disetujui sebelum perubahan antarmuka atau transport.
- [ ] L-06 — Migrasi dilakukan bertahap: baca-saja, lalu pembatalan/pemulihan, lalu pembuatan order setelah uji idempotensi.
- [ ] L-07 — Ada sakelar peluncuran, metrik, prosedur rollback, dan batas penghentian.
- [ ] L-08 — Tidak ada perubahan strategi, rumus biaya, atau perilaku Adaptive Maker yang terselip dalam pekerjaan infrastruktur.

## M. Formulir temuan per butir

Salin formulir ini untuk setiap temuan:

| Kolom | Isi |
|---|---|
| ID checklist | |
| Status | LULUS / GAGAL / BELUM DIPERIKSA / TIDAK BERLAKU |
| Cabang dan komit yang diperiksa | |
| Berkas dan fungsi | |
| Baris atau bagian kode | |
| Bukti pengujian/log/dokumen resmi | |
| Perilaku aktual | |
| Perilaku yang diharapkan | |
| Risiko bila dibiarkan | |
| Tindakan yang disarankan | |
| Pemeriksa dan tanggal | |

## Aturan penutupan

Audit belum boleh dinyatakan selesai jika masih ada butir kritis berstatus GAGAL atau BELUM DIPERIKSA. Checklist ini tidak menyatakan bahwa semua fitur saat ini sudah memenuhi kriteria; checklist ini menetapkan cara membuktikannya.
