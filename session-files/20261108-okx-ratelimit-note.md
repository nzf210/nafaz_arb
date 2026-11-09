# Catatan: OKX Rate Limit Memotong Coverage Scan

**Dibuat:** 2026-11-08 16:10 WIB
**Status:** open — cek kembali jika dalam 2 hari (sampai ~10 Nov 2026) tidak ada kemajuan

## Kondisi saat ini (terverifikasi di VPS 163.52.135.3)

- Semua container UP stabil: `nafaz-redis`, `nafaz-scanner` (restart=0), `nafaz-executor`.
- Fix clock-skew (`d3256fc`) sudah masuk di image scanner build 16:05 — error "in the future" = 0.
- Fix data/opportunities.json refactor (`9680f81`) juga sudah aktif (`/app/data` tidak ada lagi).
- Supabase persistence hidup: scanner_opportunities 50 baris, executor_status_snapshots 27, balance_snapshots 18.

## Masalah tersisa

Scan masih `incomplete` — OKX rate limit 50011 (44 kejadian dalam 2 menit).

Akar: worker pool scanner hard-coded 15 worker paralel di
internal/scanner/evaluate.go:374 (tidak bisa diatur via config), menghantam
endpoint depth OKX sampai di-throttle — banyak pair gagal fetch perp book,
scan dinyatakan incomplete.

## Pilihan perbaikan

1. Jadikan workerCount field config (scan_worker_count), turunkan default
   ke 3-5, plus throttle antar request OKX. Perlu edit kode di local, lalu
   user deploy + rebuild scanner.
2. Kurangi jumlah pair tokocrypto-vs-okx di config.json VPS (tanpa edit kode).
3. Biarkan — scan interval 180s tetap jalan, sebagian pair lolos per putaran.

## Rekap deploy VPS (untuk nanti)

```
cd /home/deploy/nafaz-scanner
~/.venvs/podman-compose/bin/podman-compose up -d --build nafaz-scanner
# Kalau gagal "name already in use":
podman rm -f nafaz-scanner nafaz-executor nafaz-redis
# lalu ulangi up -d --build
```

Image executor (08:13) tidak wajib rebuild kecuali ada perubahan kode executor.
