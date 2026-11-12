# Matriks CEX NAFAZ V3

Dokumen ini memisahkan **terdaftar sebagai kandidat**, **tersedia di library**, dan **benar-benar siap LIVE**. Ketiganya bukan status yang sama.

## 1. Spot lokal Indonesia

Prioritas awal tetap venue yang sudah ada di jalur kode saat ini:

| Venue | Peran | Status pada baseline V3 | Syarat sebelum LIVE |
|---|---|---|---|
| Indodax | Spot IDR | Adaptor scanner dan jalur private TAPI v2 tersedia | Uji saldo, order, fill, fee, cancel, trade history, minimum, dan pair IDR/USDT |
| Tokocrypto | Spot IDR/USDT | Adaptor scanner dan jalur executor tersedia | Uji broker/client ID, fee akun, saldo, order, fill, dan pemulihan |
| Pintu | Kandidat spot lokal | Belum dianggap terintegrasi | API perdagangan resmi, izin/akses akun, order lifecycle dan fee harus diverifikasi |
| Reku | Kandidat spot lokal | Belum dianggap terintegrasi | API perdagangan resmi, order lifecycle, saldo, fill, fee, dan batas rate |
| Bittime | Kandidat spot lokal | Belum dianggap terintegrasi | API resmi dan kelengkapan endpoint private diverifikasi |
| Upbit Indonesia | Kandidat spot lokal | Belum dianggap terintegrasi | API lokal, pair IDR, aturan autentikasi, dan endpoint private diverifikasi |
| Triv | Kandidat spot lokal | Belum dianggap terintegrasi | API resmi dan akses perdagangan otomatis diverifikasi |
| Ajaib Kripto | Kandidat spot lokal | Belum dianggap terintegrasi | API resmi untuk akun dan order; jangan mengasumsikan API publik berarti API trading |
| Pluang | Kandidat spot lokal | Belum dianggap terintegrasi | Pastikan instrumen spot dapat diperdagangkan melalui API resmi |
| Luno Indonesia | Kandidat spot lokal | Belum dianggap terintegrasi | Verifikasi ketersediaan pair IDR dan private trading API |
| BTSE Indonesia | Kandidat spot lokal | Belum dianggap terintegrasi | Verifikasi produk, yurisdiksi akun, dan API private |
| Nanovest / Nobi / Bitwyre / lainnya | Kandidat evaluasi | Belum dianggap terintegrasi | Masuk registry hanya setelah API dan kapabilitas memenuhi kontrak |

Daftar penyelenggara berizin berubah. Status legal harus diperiksa ulang pada daftar resmi OJK saat integrasi dilakukan. Status PAKD tidak otomatis berarti tersedia API perdagangan otomatis, pasar IDR yang sesuai, atau izin produk derivatif.

## 2. Perpetual global

| Venue | Produk target | Status baseline V3 | Prioritas |
|---|---|---|---|
| OKX | USDT linear Swap | Adaptor scanner dan executor sudah ada | P0 |
| MEXC | USDT-M Contract | Adaptor scanner dan executor sudah ada | P0 |
| Binance | USDⓈ-M Futures | Fee/executor disebut dalam dokumentasi; cakupan scanner harus diverifikasi | P1 |
| Bybit | Linear Perpetual | Fee/executor disebut dalam dokumentasi; cakupan scanner harus diverifikasi | P1 |
| Bitget | USDT-Futures | Belum dinyatakan terintegrasi | P2 |
| Gate.io | USDT Perpetual | Belum dinyatakan terintegrasi | P2 |
| KuCoin Futures | USDT Perpetual | Belum dinyatakan terintegrasi | P3 |
| BingX | Perpetual | Belum dinyatakan terintegrasi | P3 |

Perluasan tidak dilakukan hanya karena CCXT mengenali nama venue. Setiap adaptor wajib membuktikan dukungan instrumen linear, posisi one-way/hedge mode, reduce-only, presisi kontrak, funding history, fee akun, order idempotency, private fills, dan pemulihan posisi.

## 3. Aturan aktivasi venue

Sebuah venue hanya boleh berstatus `LIVE_READY` jika seluruh pemeriksaan berikut lulus:

1. Dokumen API resmi dan yurisdiksi akun telah diperiksa.
2. Penemuan instrumen menghasilkan pasar aktif yang benar.
3. Metadata lot, minimum, tick, dan ukuran kontrak lengkap.
4. Harga dan order book memiliki timestamp serta batas kesegaran.
5. Autentikasi private API, saldo, dan posisi dapat diverifikasi.
6. Submit, fetch, cancel, partial fill, dan trade history lolos uji.
7. Fee aktual dan aset komisi dapat direkonsiliasi.
8. Rate limit, retry, circuit breaker, dan pemulihan setelah restart diuji.
9. Uji paper dan uji integrasi dengan ukuran minimum lulus.
10. Tidak ada izin penarikan dana pada API key.

Status yang disarankan: `PLANNED`, `PUBLIC_DATA_READY`, `PRIVATE_READ_READY`, `PAPER_READY`, `LIVE_READY`, `DISABLED`, `DEGRADED`.

## 4. Prinsip implementasi

- Registry menyimpan ID venue, peran pasar yang diizinkan, kapabilitas, status kesehatan, dan versi adaptor.
- Strategi tidak boleh memanggil SDK bursa secara langsung.
- Adaptor mengubah format venue menjadi model internal yang sama.
- Kegagalan satu venue tidak boleh menghentikan pemindaian venue lain yang sehat.
- Rute selalu venue-bound: `spot_exchange + spot_symbol + perp_exchange + perp_symbol`.
- Tidak boleh menggunakan fallback fee nol, kurs tetap, atau ukuran kontrak tebakan untuk LIVE.
- Penambahan venue dilakukan bertahap dan diuji tanpa mengubah aturan strategi yang sudah berjalan.

## 5. Rujukan regulasi dan teknis

- OJK — [Perizinan aset keuangan digital dan aset kripto](https://ojk.go.id/id/Fungsi-Utama/ITSK/Perizinan-ITSK-Aset-Keuangan-Digital-Aset-Kripto/).
- OJK — [Daftar penyelenggara perdagangan aset keuangan digital posisi 22 September 2026](https://ojk.go.id/id/Fungsi-Utama/ITSK/Perizinan-ITSK-Aset-Keuangan-Digital-Aset-Kripto/Pages/Daftar-Penyelenggara-Perdagangan-Aset-Keuangan-Digital-Posisi-22-September-2026.aspx).
- CCXT — [Daftar bursa yang didukung](https://docs.ccxt.com/docs/exchange-markets).

Rujukan tersebut dipakai sebagai titik awal verifikasi, bukan bukti bahwa endpoint tertentu telah diimplementasikan atau diuji pada NAFAZ.
