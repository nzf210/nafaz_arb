# Panduan Dokumentasi Audit Arsitektur Nafazbot

Seluruh penjelasan dan panduan audit pada direktori ini menggunakan Bahasa Indonesia. Nama berkas kode, nama fungsi, nama endpoint, istilah protokol, dan istilah teknis baku dipertahankan sesuai sumber aslinya agar mudah dicari di repositori.

## Urutan membaca dan memeriksa

1. [Checklist Audit Manual Terperinci](CHECKLIST-AUDIT-MANUAL.md) — lembar pemeriksaan utama yang dapat dicentang satu per satu, lengkap dengan bukti dan kriteria lulus.
2. [Baseline Arsitektur](ARCHITECTURE-BASELINE.md) — gambaran komponen, tanggung jawab, batas perubahan, dan kemampuan tiap bursa.
3. [Peta Pemanggilan API Privat](PRIVATE-API-CALL-MAP.md) — daftar operasi privat, endpoint, bursa, pemanggil, dan risiko.
4. [Kontrak Pengujian Regresi](REGRESSION-CONTRACT.md) — perilaku yang wajib dipertahankan dan pengujian yang harus lulus.
5. [Inventaris Arsitektur per Berkas](FILE-LEVEL-ARCHITECTURE-INVENTORY.md) — daftar berkas dan arahan peninjauan per berkas.

6. [Desain Teknis Executor V2](EXECUTOR-ARCHITECTURE-V2.md) — rancangan coordinator, ranking peluang, eksekusi dua kaki, Pair PnL, exit, reconciliation, schema, pengujian, dan rollout. Status: untuk review; belum diimplementasikan.

## Aturan status

- **LULUS** — bukti memenuhi kriteria yang ditetapkan.
- **GAGAL** — ditemukan perilaku yang tidak sesuai.
- **BELUM DIPERIKSA** — bukti belum dikumpulkan atau belum cukup.
- **TIDAK BERLAKU** — butir tidak relevan, disertai alasan.

Jangan menandai butir sebagai LULUS hanya karena fungsi atau konfigurasi terlihat tersedia. Harus ada bukti implementasi dan, bila relevan, bukti pengujian atau verifikasi endpoint.

## Batas audit

Dokumen ini tidak menyatakan bahwa seluruh 213 berkas sudah diperiksa baris demi baris atau bahwa seluruh bursa sudah tersertifikasi untuk trading LIVE. Tingkat pemeriksaan setiap berkas mengikuti keterangan pada inventaris. Berkas yang belum diperiksa penuh wajib ditinjau sebelum diubah.

Pekerjaan pada cabang audit ini bersifat dokumentasi. Tidak ada perubahan kode trading, strategi, konfigurasi produksi, kredensial, atau migrasi basis data.
