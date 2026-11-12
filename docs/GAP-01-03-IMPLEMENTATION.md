# Catatan Implementasi GAP-01 sampai GAP-03

**Branch sumber:** `fix/gap-01-03-fx-freshness-fees`

## GAP-01 — Kurs FX IDR fail-closed

- Executor menolak sinyal entri IDR/BIDR jika payload scanner tidak memiliki kurs USDT/IDR yang finite dan positif, termasuk pada mode mock.
- Kurs yang sudah divalidasi beserta waktu scan disimpan pada setiap entri pending.
- Rekonsiliasi fill tidak lagi mengganti kurs dengan nilai tetap. Order lama atau rusak yang sudah terisi tetapi tidak memiliki kurs tersimpan ditandai `RECONCILIATION_REQUIRED`.
- Perhitungan eksposur pending memakai reservasi ekuivalen USDT yang disimpan saat order dikirim, disesuaikan dengan kuantitas tersisa.

**Catatan operasional:** catatan order pending lama yang dibuat sebelum perubahan ini mungkin tidak memiliki kurs FX tersimpan. Jika order seperti itu sudah memiliki fill, executor sengaja meminta rekonsiliasi manual alih-alih menebak kurs.

## GAP-02 — Kesegaran order book

- Umur maksimum order book bawaan adalah 5 detik.
- Selisih waktu penerimaan antar-bursa bawaan adalah 3 detik.
- Order book Indodax dan Tokocrypto mencatat `ReceivedAt` dan `Source=receive_time` secara eksplisit.
- Order book perpetual OKX dan MEXC mempertahankan timestamp bursa dan mencatat waktu penerimaan lokal.
- Validasi kesegaran menolak timestamp yang hilang, data usang, dan timestamp lebih dari satu detik di masa depan.
- Penyelarasan antar-bursa membandingkan waktu penerimaan lokal jika tersedia pada kedua sisi, bukan membandingkan jam dari dua bursa secara langsung.

**Batasan:** endpoint kedalaman Indodax/Tokocrypto pada adaptor saat ini tidak menyediakan timestamp order book dari sisi bursa yang dapat diandalkan. Validasi waktu penerimaan membuktikan respons baru diterima, bukan bahwa bursa tidak menyajikan kedalaman cache. Ini merupakan keterbatasan API/venue.

## GAP-03 — Profil biaya

- Ditambahkan penggantian tarif per bursa yang bersifat opsional melalui `fee_profiles`.
- Tarif maker/taker spot diambil dari profil venue spot lokal; tarif taker perpetual diambil dari profil venue perpetual global.
- Maker fee nol yang ditetapkan secara eksplisit tetap dipertahankan; nilai yang tidak disediakan memakai konfigurasi biaya global yang sudah ada.
- Persentase tidak valid di luar rentang 0–100 ditolak.

**Batasan penting:** profil ini merupakan nilai konfigurasi yang dimasukkan operator, bukan tingkat biaya akun yang diambil secara dinamis. Scanner belum memanggil endpoint biaya terautentikasi setiap bursa. Sebelum perdagangan LIVE, isi profil berdasarkan jadwal biaya akun yang sebenarnya. Penemuan biaya dinamis adalah pekerjaan lanjutan dan tidak boleh dianggap selesai oleh perubahan ini.

Contoh berikut hanya ilustrasi. Ganti dengan tarif akun yang telah diverifikasi; jangan menyalinnya sebagai nilai produksi.

```json
{
  "fee_profiles": {
    "indodax": {
      "spot_maker_fee_pct": 0.0,
      "spot_taker_fee_pct": 0.3
    },
    "tokocrypto": {
      "spot_maker_fee_pct": 0.1,
      "spot_taker_fee_pct": 0.1
    },
    "okx": {
      "perp_taker_fee_pct": 0.05
    },
    "mexc": {
      "perp_taker_fee_pct": 0.0
    }
  }
}
```
