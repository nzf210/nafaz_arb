# Operasional Atomic Spot Capacity dan Capital Reservation

Status: implementasi bertahap pada branch `feat/unified-dynamic-fee-engine`; belum merupakan persetujuan LIVE.

## Aturan yang diterapkan

- Batas Spot pending/open per account dan CEX disimpan di Postgres, default **2**.
- Telegram `/spotmax <exchange> <limit>` menunggu acknowledgement dari Executor. Perubahan baru dianggap berhasil setelah Executor memvalidasi coordinator lease dan menulis konfigurasi ke Postgres.
- Redis hanya cache untuk tampilan Telegram; saat startup, konfigurasi LIVE dibaca dari Postgres.
- Reservasi dibuat atomik sebelum order Spot dikirim. Satu pasangan menahan satu slot dan jumlah modal pasangan (Spot notional + margin Futures 1x).
- Reservasi yang sama bersifat idempotent. Order ambigu, pending, partial fill, atau posisi aktif tetap menahan reservasi.
- Reservasi hanya dilepas setelah entry terbukti terminal tanpa fill, rollback terbukti selesai, atau kedua leg pasangan terbukti flat.
- Menurunkan limit tidak membatalkan order/posisi yang sudah aktif.

## Konfigurasi wajib sebelum LIVE

1. Set `account_scope` pada setiap exchange credential di `config.json`. Nilainya adalah identitas akun stabil yang tidak berubah saat API key dirotasi. Jika kosong, kode menggunakan fingerprint API key; rotasi key kemudian dapat membuat reservasi lama tidak terhitung pada scope baru.
2. Set `max_exposure_per_exchange_usdt` ke nilai positif yang sesuai dengan anggaran modal pasangan. Nilai 0 berarti belum ada batas modal dan RPC akan menolak reservasi LIVE dengan `capital_limit_not_configured`.
3. Jangan memakai account scope yang sama untuk dua akun exchange yang berbeda. Setiap exchange/account perlu identitas scope yang stabil.

Contoh:

```json
{
  "exchanges": {
    "indodax": {
      "account_scope": "indodax-main-account"
    },
    "okx": {
      "account_scope": "okx-main-account"
    }
  },
  "max_exposure_per_exchange_usdt": 1000
}
```

Sesuaikan dengan konfigurasi credential yang sebenarnya; jangan menaruh API key atau secret di dokumentasi.

## Operasional dan recovery

- RPC Postgres mengunci transaksi per account-scope + exchange sebelum menghitung slot/modal dan menyisipkan reservasi.
- Kegagalan RPC, database, intent persistence, atau coordinator fencing harus memblokir order baru.
- Jika release RPC gagal, reservasi dibiarkan aktif (konservatif); operator harus merekonsiliasi order/posisi sebelum melakukan release manual.
- Jangan menghapus reservasi aktif langsung dari tabel hanya untuk membuka slot. Verifikasi order dan kedua leg terlebih dahulu.
- Schema smoke memverifikasi fencing-token takeover, stale-token rejection, idempotensi, slot cap, capital cap, dan release. Concurrency test Postgres lintas koneksi tetap menjadi acceptance test terpisah sebelum READY FOR LIVE TEST.
