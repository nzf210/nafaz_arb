# Temuan Audit: Scanner dan Executor

**Tanggal audit:** 2026-11-03  
**Auditor:** Copilot AI (pemeriksaan kode statis)  
**Status pada audit awal:** BELUM SIAP UNTUK PERDAGANGAN LIVE

> **Catatan historis:** Status di atas berasal dari audit awal 3 November 2026, bukan pernyataan status akhir branch V3. Pembaruan pada 3 November menyatakan bug P0 (BUG-01 sampai BUG-06) diperbaiki pada Fase 0; BUG-05, BUG-07, BUG-09, dan BUG-10 pada Fase 1; BUG-08 dan P2-01 sampai P2-06 pada Fase 2; serta P3-01 sampai P3-05 pada Fase 3. Semua bug audit awal BUG-01 sampai BUG-10 ditandai tertutup pada saat pembaruan tersebut.
>
> Status historis ini tidak membuktikan bahwa seluruh perubahan berikutnya telah lolos CI atau siap produksi. Lihat [Rencana Perbaikan](./IMPROVEMENT-PLAN.md#progres) dan temuan pascarefaktor di bawah.

## Ringkasan temuan audit awal

| Kategori | Jumlah |
|---|---:|
| Bug kritis (P0) | 5 |
| Bug prioritas tinggi (P1) | 5 |
| Fitur belum tersedia | 22 |
| Implementasi parsial | 14 |
| Kondisi balapan | 4 |
| Masalah pemulihan | 6 |

## Bug kritis audit awal

| ID | Deskripsi | Berkas/lokasi | Status historis |
|---|---|---|---|
| BUG-01 | Harga masuk short selalu salah pada mode mock: `open_global_short_market` tidak mengembalikan field `price`, lalu memakai harga spot dari `price_idr / usdt_rate` sebagai pengganti harga futures. | `executor/exchange_client.py`, `executor/engine.py:_record_hedged_position` | Diperbaiki (P0-04) |
| BUG-02 | Eksposur spot tanpa hedge jika futures global gagal karena tidak ada rollback/penjualan spot darurat. | `executor/engine.py:_execute_entry_hedge` | Diperbaiki (P0-01) |
| BUG-03 | Restart menghilangkan seluruh posisi aktif karena tidak ada pemulihan awal dari Redis. | `executor/engine.py:initialize` | Diperbaiki (P0-03) |
| BUG-04 | Force close tidak melikuidasi kaki spot dan hanya menutup short futures. | `executor/engine.py:handle_force_close` | Diperbaiki (P0-05) |
| BUG-05 | Kondisi balapan dapat mengirim order duplikat ketika sinyal datang bersamaan. | `executor/engine.py:handle_signal_notification` | Diperbaiki (P1-01) |
| BUG-06 | `NameError: name 'funding' is not defined` pada force close menyebabkan proses crash. | `executor/engine.py:handle_force_close` | Diverifikasi diperbaiki (P0-02) |
| BUG-07 | TTL sinyal tidak dipasang di Redis sehingga sinyal dapat kedaluwarsa tanpa batas. | `internal/redishandler/client.go:PublishSignal` | Diperbaiki (P1-02) |
| BUG-08 | Status `ENTRY_PARTIAL` tidak pernah dipasang pada engine. | `executor/engine.py`, `executor/models.py` | Diperbaiki (P2-01) |
| BUG-09 | Order book usang dikirim ke executor tanpa validasi umur maksimum. | `internal/scanner/service.go:fetchMarketData` | Diperbaiki (P1-05) |
| BUG-10 | Nilai `short_exit_usdt` berasal dari snapshot, bukan harga fill aktual. | `executor/engine.py:_execute_exit_fill` | Diperbaiki pada Fase 1 |

## Cacat tambahan saat implementasi

Dua cacat berikut ditemukan di luar daftar audit awal dan menghambat fase terkait:

| ID | Deskripsi | Berkas | Status historis |
|---|---|---|---|
| BUG-11 | Deklarasi `const ARB_TRADE_EVENTS = "arb:trade_events"` memakai sintaks Go di dalam class Python. Modul `executor/engine.py` tidak dapat diimpor sejak commit `86584ef`; empat pemanggil memakai nama yang tidak tersedia. | `executor/engine.py` | Diperbaiki dengan `self.ARB_TRADE_EVENTS` di `__init__` |
| BUG-12 | `fetchMarketData` membungkus error dengan `fmt.Errorf(..., %v)` sehingga rantai error terputus dan `errors.Is(err, ErrExchangeUnavailable)` tidak cocok. Deteksi pemeliharaan P2-05 pun tidak bekerja. | `internal/scanner/service.go` | Diperbaiki dengan `errors.Join` dan `%w` |

## Tinjauan pascarefaktor — 2026-11-07

Pernyataan sebelumnya bahwa semua temuan tertutup adalah catatan historis, bukan bukti bahwa branch saat ini siap produksi. Pemeriksaan statis baru setelah executor dipecah menjadi mixin menemukan celah berikut.

| ID | Temuan | Area | Perbaikan yang tercatat pada branch sumber |
|---|---|---|---|
| R-01 | Pembatalan TTL/adverse/emergency melepaskan status tanpa memverifikasi status akhir di bursa. | Entri | Batalkan, ambil ulang status order, verifikasi status terminal, dan rekonsiliasi fill sebelum melepas dana. |
| R-02 | Pemulihan startup menghapus kunci Redis pending setelah mengadopsi order terbuka. | Pemulihan | Pertahankan catatan pending; hedge fill parsial terminal sebelum finalisasi; status tidak diketahui memblokir pasangan. |
| R-03 | Force close mempercayai pengakuan cancel tanpa memeriksa order. | Pembatalan | Verifikasi status akhir order dan rekonsiliasi fill yang terjadi bersamaan; batalkan force close jika belum jelas. |
| R-04 | Force close menganggap respons penutupan futures yang hilang sebagai keberhasilan. | Force close | Wajibkan query posisi futures otoritatif untuk memastikan posisi benar-benar nol. |
| R-05 | Force close menganggap fill spot positif berapa pun sebagai likuidasi penuh. | Force close | Kuantitas fill harus menutup seluruh kuantitas permintaan; fill parsial tetap perlu rekonsiliasi. |
| R-06 | Hedge entri mencatat kuantitas yang diminta tanpa memeriksa fill futures aktual. | Hedge entri | Wajibkan fill futures penuh yang dapat diverifikasi sebelum mencatat posisi ter-hedge. |
| R-07 | Alur keluar mengurangi status posisi tanpa memverifikasi fill penutupan futures. | Keluar | Wajibkan fill penutupan futures penuh; jika tidak, pertahankan posisi dan blokir retry otomatis. |
| R-08 | Akumulasi funding memakai nosional estimasi × tarif dan mengasumsikan interval delapan jam. | Funding | Gunakan jumlah dari riwayat funding bursa dan jadwal settlement berikutnya; jangan mengkreditkan estimasi. |
| R-09 | Kegagalan membaca saldo melewati pemeriksaan ketidaksesuaian. | Risiko | Tahan entri baru pada kedua venue saat data saldo tidak tersedia. |
| R-10 | API HTTP scanner tidak terautentikasi. | API | API Go mewajibkan Bearer token pada mode LIVE; endpoint health tetap publik. |
| R-11 | Startup memulihkan posisi aktif Redis tanpa memeriksa eksposur futures aktual. | Pemulihan | Startup LIVE membandingkan kuantitas short bursa dengan kuantitas aset dasar yang dipulihkan; ketidaksesuaian/status tak diketahui diblokir. |
| R-12 | API LIVE menerima token Bearer dengan panjang sembarang, termasuk terlalu pendek. | API | Konfigurasi LIVE mewajibkan token minimal 32 karakter. |
| R-13 | Payload posisi futures yang rusak dapat disaring menjadi daftar kosong dan dianggap sebagai akun tanpa posisi. | Adaptor/pemulihan | `fetch_positions` menolak payload non-list, rusak, atau tanpa simbol; hanya daftar kosong yang valid berarti posisi nol. |
| R-14 | Container executor berjalan sebagai root dan menyalin `config.json` ke lapisan image. | Deployment | Executor berjalan sebagai pengguna non-root khusus dan menerima konfigurasi saat runtime; CI membangun kedua image. |
| R-15 | Klien CCXT LIVE tidak memiliki adaptor saldo sehingga pemeriksaan fail-closed dapat menghentikan semua entri; sizing juga membutuhkan normalisasi IDR ke USDT. | Risiko/adaptor | Ditambahkan pembacaan saldo spot/futures asinkron, normalisasi FX dari snapshot scanner segar, dan penanganan fail-closed jika FX/saldo tidak tersedia. |
| R-16 | Konteks build Docker dapat menyertakan `config.json` dan secret `.env` lokal meskipun image tidak menyalinnya. | Deployment | Ditambahkan `.dockerignore` untuk mengecualikan secret, kredensial, data runtime, dan metadata repositori. |
| R-17 | API scanner mengembalikan detail error Redis/scanner mentah ke klien HTTP. | API | Respons 500 dibuat generik, error status/hasil scan disanitasi, detail internal dicatat di server, dan tes regresi ditambahkan. |
| R-18 | Force close ulang setelah futures tertutup tetapi likuidasi spot gagal dapat mengirim penutupan futures duplikat dan tidak mencoba ulang penjualan spot. | Force close | Retry memeriksa eksposur futures aktual terlebih dahulu; jika sudah nol, lewati penutupan futures duplikat dan coba likuidasi spot; jika short masih ada, blokir pengiriman ulang sampai rekonsiliasi manual. |

Perbaikan tersebut tercatat sebagai perbaikan hasil pemeriksaan kode statis dengan tes regresi. Dokumen sumber menyatakan perbaikan belum dijalankan pada lingkungan saat audit dilakukan. Status CI dan pengujian branch V3 harus diperiksa secara terpisah sebelum persetujuan rilis.
