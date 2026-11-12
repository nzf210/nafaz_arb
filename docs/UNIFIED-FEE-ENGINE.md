# Mesin Biaya Terpadu

## Model biaya

Semua tarif memakai satuan persentase: `0.10` berarti `0.10%`. Scanner dan executor memakai model empat fill untuk satu siklus penuh:

- Masuk spot: maker pada mode `MAKER_TAKER`, taker pada mode `TAKER_TAKER`.
- Keluar spot: taker.
- Masuk perpetual: taker.
- Keluar perpetual: taker.

Total estimasi biaya dihitung dari nilai nosional setiap fill yang dapat dieksekusi, bukan dengan mengalikan satu tarif rata-rata terhadap gabungan dua kaki. Biaya tambahan aset fee diterapkan pada keempat nosional di scanner dan executor. Executor memakai rumus yang sama untuk estimasi PnL berjalan, batas minimum harga jual spot yang menguntungkan, dan estimasi PnL penutupan. Batas harga jual spot sudah memperhitungkan biaya saat keluar.

## Sumber biaya akun

Sebelum entri LIVE, executor meminta tarif akun terautentikasi untuk pasangan yang tepat:

- Tokocrypto Spot: permintaan bertanda tangan `GET /open/v1/account/spot`; `makerCommission` dan `takerCommission` dikonversi dari basis poin ke persentase.
- OKX, MEXC, Binance USD-M, dan Bybit linear: CCXT `fetch_trading_fee(symbol)`, jika didukung adaptor CCXT yang terpasang.
- Indodax Spot: adaptor CCXT saat ini belum mengimplementasikan `fetchTradingFee`. Sementara itu, tarif memakai `fee_profiles.indodax` atau konfigurasi global. Sumber ini ditandai sebagai konfigurasi/parsial, bukan tarif dinamis terverifikasi.

Endpoint yang gagal atau tidak didukung tidak boleh dianggap memiliki biaya nol. Scanner memakai tarif konfigurasi konservatif dan menandai sumbernya sebagai `CONFIG` atau `PARTIAL_EXCHANGE_API`.

## Cache dan distribusi

- Executor menyimpan cache tarif akun per klien dan simbol selama lima menit.
- Snapshot akun yang masih segar diterbitkan ke Redis dengan kunci `arb:fee_profile:{exchange}:{market}:{symbol}` dan TTL lima menit.
- Scanner hanya menerima snapshot `EXCHANGE_API` yang masih segar dan terverifikasi. Tarif spot dan perpetual diselesaikan terpisah; bila salah satu kaki memakai konfigurasi, sumber keseluruhan ditandai parsial.
- Siklus scan berikutnya memakai tarif akun dari cache jika tersedia.
- Sebelum mengirim order baru, executor menghitung ulang estimasi bersih memakai tarif terautentikasi. Jika biaya aktual menghapus keuntungan bersih, entri ditolak dengan alasan `NET_EDGE_ERODED_BY_ACTUAL_FEES`.

## Snapshot posisi dan keterbatasan

Tarif dan sumbernya disimpan sebagai snapshot tetap pada posisi agar pembaruan tingkat biaya akun tidak mengubah estimasi historis posisi yang sudah terbuka. Posisi lama tanpa snapshot memakai tarif konservatif sampai posisi ditutup.

**Keterbatasan penting:** mesin ini masih menghitung estimasi biaya dari tarif akun. Komisi aktual setiap fill, termasuk diskon atau aset komisi non-kuotasi, belum seluruhnya direkonsiliasi ke ledger biaya fill. Karena itu PnL penutupan belum boleh disebut PnL bersih final yang tepat sampai rekonsiliasi komisi aktual bursa selesai.

## Cakupan pengujian regresi

`tests/test_fee_engine.py` mencakup delapan kombinasi spot Indodax/Tokocrypto dengan perpetual OKX/MEXC/Binance/Bybit, kedua mode eksekusi, maker fee nol yang disengaja, tarif tidak valid, dan aljabar batas harga keluar. `tests/test_exchange_fee_provider.py` menguji konversi komisi akun Tokocrypto dan fallback parsial Indodax. Pengujian Redis Go mencakup kesegaran data dan penyaringan sumber tarif yang terverifikasi.
