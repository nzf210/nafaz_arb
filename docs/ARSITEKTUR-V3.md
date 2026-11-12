# Arsitektur NAFAZ V3 dan rencana implementasi

## 1. Sasaran

Membangun sistem arbitrase spot lokal Indonesia dan perpetual global yang modular, dapat dipulihkan setelah gangguan, dapat diaudit, dan bisa menambah atau menonaktifkan venue tanpa mengubah mesin strategi inti.

Target bisnis bukan janji profit. Profitabilitas harus dibuktikan dengan data fill nyata setelah biaya, pendanaan, slippage, kurs, dan risiko operasional.

## 2. Batas strategi

- Kaki lokal: transaksi spot beli saat masuk dan jual saat keluar.
- Kaki global: kontrak perpetual linear, short saat masuk dan buy-reduce-only saat keluar.
- Leverage awal strategi tetap 1x; jangan menaikkan leverage sebagai bagian dari pekerjaan otomatis.
- Tidak ada transfer aset antar-bursa.
- Kuantitas posisi harus sama secara ekonomi setelah konversi ukuran kontrak.
- Identitas rute wajib mencakup venue dan simbol masing-masing kaki; jangan menggabungkan hanya berdasarkan ticker aset.
- Semua biaya dihitung per fill dan per kaki. Biaya yang tidak terverifikasi tidak boleh dianggap nol.
- Kurs IDR/USDT harus memiliki sumber, waktu observasi, spread, dan batas kesegaran. Kegagalan kurs tidak boleh memakai angka tetap untuk keputusan LIVE.

## 3. Batas layanan dan modul

### Go — scanner dan data pasar

Pertahankan kontrak dan paket yang sudah ada. Pecah adaptor bursa berdasarkan tanggung jawab:

- penemuan instrumen;
- metadata lot, minimum, tick, dan ukuran kontrak;
- order book dan ticker;
- kurs konversi;
- funding dan jadwal settlement;
- status instrumen serta kesehatan koneksi.

Setiap adaptor mengumumkan kapabilitas eksplisit. Jangan menganggap suatu venue mendukung fitur hanya karena nama venue tercantum dalam konfigurasi.

### Python — executor

Executor tetap menjadi satu-satunya komponen yang mengirim order. Pisahkan adaptor private API dari logika strategi. Setiap venue harus memenuhi kontrak saldo, order, fill, posisi perpetual, biaya akun, pembatalan, dan riwayat transaksi yang diperlukan.

Mesin eksekusi wajib mempertahankan idempotensi client order ID, lock per rute, TTL adaptif yang dibatasi, pemulihan order terbuka, rekonsiliasi fill, serta penanganan eksposur yatim.

### Redis — koordinasi waktu nyata

Redis digunakan untuk sinyal, harga terbaru, lock, status posisi aktif, status venue, dan peristiwa eksekusi. Data yang dipakai untuk keputusan harus membawa timestamp dan identitas sumber. Data kedaluwarsa berarti tidak boleh membuka entri baru.

### Supabase dan outbox — audit tahan gangguan

Outbox lokal harus melakukan commit sebelum pengiriman asinkron. Redis dan API bursa tetap menjadi sumber kebenaran untuk status waktu nyata; Supabase adalah jurnal historis dan analitik, bukan pengganti verifikasi bursa.

Siklus data harus dapat ditelusuri secara konsisten:
`observasi pasar → peluang → keputusan → order → fill → posisi/kaki → funding → penutupan → PnL aktual`.

Setiap hubungan memakai ID deterministik atau foreign key yang stabil. Pair key saja tidak cukup untuk mengidentifikasi satu percobaan eksekusi.

## 4. Kontrak adaptor CEX

Adaptor harus mengimplementasikan hanya kapabilitas yang benar-benar didukung dan diuji:

- `discover_markets`: pasar aktif dan metadata instrumen.
- `fetch_order_book`: bid/ask dan kedalaman dengan waktu sumber.
- `fetch_balance`: saldo tersedia dan terkunci.
- `create_order`, `fetch_order`, `cancel_order`: siklus order.
- `fetch_my_trades`: fill individual dan komisi aktual.
- `fetch_positions`: eksposur kontrak.
- `fetch_funding_rate` dan `fetch_funding_history`: funding saat ini dan settlement.
- `fetch_trading_fee`: biaya akun untuk instrumen yang tepat.
- `normalize_amount`: normalisasi jumlah berdasarkan lot dan ukuran kontrak.

Kapabilitas yang belum tersedia harus dinyatakan `UNSUPPORTED`, bukan diberi implementasi kosong atau nilai default yang terlihat sukses.

## 5. Lapisan AI dan kewenangan otonom

Urutan pengembangan AI:

1. **Intelijen pasar:** likuiditas, volatilitas, umur peluang, stabilitas spread, dan kondisi venue.
2. **Prediksi peluang:** probabilitas fill dan peluang profit bersih. Mulai dari model terukur seperti LightGBM/XGBoost setelah dataset memadai.
3. **Pengoptimal eksekusi:** pilihan maker/taker, harga limit, TTL adaptif, dan ukuran order berdasarkan kedalaman yang bisa dieksekusi.
4. **Alokasi modal:** distribusi dana berdasarkan saldo terverifikasi, eksposur, likuiditas, dan hasil aktual.
5. **Deteksi kesehatan CEX:** keterlambatan, rate limit, data usang, penolakan order, dan ketidaksesuaian saldo/posisi.
6. **Riset strategi:** analisis hasil dan eksperimen dengan validasi walk-forward.
7. **Pengambil keputusan:** menyusun keputusan terstruktur dan alasan yang dapat diaudit.

AI numerik tidak boleh memakai LLM pada jalur pengiriman order. LLM hanya membantu analisis, penjelasan, dan riset non-kritis.

### Otoritas yang boleh diberikan

AI dapat memilih rute yang telah diaktifkan, menurunkan ukuran, memilih mode eksekusi yang diizinkan, mengubah TTL dalam rentang yang ditentukan, menahan entri, dan mengurangi eksposur melalui prosedur executor yang sudah diuji.

AI tidak boleh menaikkan leverage atau batas rugi, mengubah aturan risiko keras, mematikan kill switch, menarik dana, mengubah secret, atau melakukan deployment LIVE tanpa persetujuan manusia.

### Mesin risiko independen

Mesin risiko deterministik selalu memvalidasi keputusan AI. Batas minimum: saldo tersedia, eksposur per venue, kerugian harian, ukuran posisi, slippage maksimum, umur data, status rekonsiliasi, jumlah order terbuka, serta status kill switch. Kegagalan membaca salah satu data wajib menghasilkan penolakan entri.

## 6. Tahapan rilis

### Tahap 0 — Baseline dan keselamatan

- Kunci baseline dari branch pengembangan fee engine yang sudah ada.
- Audit seluruh migrasi, konfigurasi, dan alur order.
- Pastikan CI berjalan untuk branch V3.
- Inventarisasi setiap venue dan kapabilitas aktual.
- Tetapkan kontrak data dan kebijakan risiko.

### Tahap 1 — Integritas data

- Telusuri ID peluang sampai order, fill, posisi, funding, dan closed trade.
- Pisahkan fill terverifikasi dari snapshot kumulatif lama.
- Catat komisi aktual per fill dan aset biaya.
- Tambahkan pengujian pemulihan setelah restart dan pengiriman ulang outbox.

### Tahap 2 — Registri venue

- Daftarkan venue melalui konfigurasi dan registry, bukan percabangan nama venue yang tersebar.
- Uji metadata, saldo, order, pembatalan, fill, posisi, fee, funding, dan pemulihan secara terpisah.
- Venue yang gagal uji tetap nonaktif untuk LIVE.

### Tahap 3 — Kualitas eksekusi

- Validasi TTL adaptif, repricing, partial fill, hedge, rollback, exit, dan force close.
- Ukur conversion rate sinyal ke fill dan fill ke posisi hedge lengkap.
- Bandingkan maker-taker dan taker-taker menggunakan biaya aktual; jangan memilih berdasarkan asumsi biaya saja.

### Tahap 4 — AI dalam shadow mode

- Bangun dataset dari seluruh observasi, termasuk peluang yang tidak dieksekusi.
- Cegah kebocoran data masa depan dan gunakan walk-forward validation.
- Jalankan inferensi tanpa mengendalikan order, bandingkan prediksi dengan hasil aktual.
- Terapkan model hanya setelah hasil out-of-sample lebih baik dan stabil.

### Tahap 5 — Otonomi terbatas

- Aktifkan pemilihan rute dan optimasi eksekusi dalam batas risiko.
- Audit setiap keputusan, input, versi model, alasan, dan hasilnya.
- Sediakan rollback model dan fallback deterministik.

### Tahap 6 — Produksi bertahap

- Mulai dari paper, lalu akun live dengan ukuran minimum dan satu rute.
- Verifikasi saldo, fill, biaya, hedge, exit, PnL, dan pemulihan.
- Tingkatkan cakupan hanya setelah periode observasi dan kriteria stabilitas terpenuhi.

## 7. Kriteria kelulusan produksi

- Seluruh pengujian Go, Python, race, vet, migrasi SQL, dan pembangunan image lulus.
- Tidak ada order duplikat setelah retry atau restart.
- Tidak ada posisi terbuka tanpa status rekonsiliasi yang jelas.
- Fill dan biaya aktual cocok dengan riwayat bursa.
- Data stale, saldo tidak terbaca, atau status posisi tidak pasti memblokir entri baru.
- Kill switch menghentikan entri baru tanpa menghentikan pengelolaan posisi yang sudah terbuka.
- Outbox pulih setelah restart dan tidak kehilangan catatan yang sudah di-commit.
- Batas risiko tidak dapat ditimpa AI atau konfigurasi venue.
- Deployment diuji pada host target; status VPS tidak boleh disimpulkan dari CI GitHub.

## 8. Standar Bahasa dan kode

Dokumentasi, komentar baru, pesan log baru, pesan validasi, dan petunjuk operasi ditulis dalam Bahasa Indonesia. Nama identifier, field JSON, nama endpoint, simbol protokol, dan kontrak eksternal tidak diterjemahkan karena dapat merusak kompatibilitas. Komentar lama diterjemahkan secara bertahap saat file disentuh dan setelah tes terkait tersedia.
