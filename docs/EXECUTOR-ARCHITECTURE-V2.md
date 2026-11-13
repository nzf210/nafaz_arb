<!-- Dokumen desain ini ditulis dalam Bahasa Indonesia. Nama modul, state, tabel, field, dan istilah teknis dipertahankan agar dapat dicari di repositori. -->

# Desain Teknis Executor V2 — Nafazbot

- Repositori: nzf210/nafaz_arb
- Cabang implementasi acuan: feat/unified-dynamic-fee-engine
- Cabang dokumentasi: audit/architecture-baseline-regression-contract
- Baseline kode: 9cc3dcd11d0999e828309053ba4cffce98a28675
- Status: RANCANGAN UNTUK REVIEW — BELUM DIIMPLEMENTASIKAN
- Ruang lingkup: desain ulang internal executor, lifecycle order, pengelolaan dua kaki, PnL, exit, dan pemulihan.
- Batas tegas: tidak ada perubahan kode trading, migrasi database, konfigurasi live, atau sumber daya produksi melalui dokumen ini.

## 0. Ringkasan keputusan yang telah disetujui

| Area | Keputusan |
|---|---|
| Model coordinator | Satu logical Execution Coordinator pusat |
| Deployment coordinator | Satu proses aktif + satu standby |
| State canonical | Supabase/PostgreSQL |
| Recovery | Event-driven sebagai jalur utama, ditambah startup recovery dan periodic safety sweep |
| Sumber event | Private WebSocket + REST fallback per CEX |
| Client order ID | Wajib digunakan jika didukung exchange; local intent ID selalu wajib |
| SUBMISSION_UNKNOWN | Recovery window dan kebijakan timeout per CEX |
| Exchange Gateway | Mutation Gateway khusus create/cancel; seluruh private API tetap melalui Exchange Adapter terstandardisasi |
| Kegagalan Supabase | Stop new exposure; tindakan protektif hanya jika state exchange dapat diverifikasi |
| Scope refactor | Rebuild internal executor; strategi dan formula ekonomi tetap dipertahankan |
| Opportunity ranking | Ranking kandidat berdasarkan estimasi profit bersih dan kualitas eksekusi, sebelum coordinator menerima kandidat |
| Two-leg | Spot dan Futures memiliki lifecycle independen; setiap Spot fill yang menambah exposure wajib dipertimbangkan untuk hedge Futures |
| Exit | Keputusan berdasarkan estimasi net PnL seluruh pasangan dan risiko exposure, bukan profit satu kaki secara terpisah |

Keputusan penting: satu coordinator pusat tidak berarti semua order harus diproses serial global. Coordinator adalah satu pusat otoritas; session berbeda boleh berjalan paralel dengan ownership, fencing, dan batas concurrency yang eksplisit.

## 1. Tujuan desain

Executor V2 harus menjadi pengendali lifecycle ekonomi dari kandidat peluang sampai seluruh exposure ditutup dan direkonsiliasi.

Sistem harus mampu:
1. Mengurutkan peluang berdasarkan potensi profit bersih yang realistis, bukan spread mentah.
2. Memvalidasi ulang peluang sebelum order dikirim karena harga, fee, saldo, dan likuiditas dapat berubah.
3. Membentuk dan mengelola Spot serta Futures secara independen tetapi dalam satu execution session.
4. Menjamin setiap Spot fill yang menciptakan exposure mendapatkan keputusan hedge yang eksplisit dan tepat waktu.
5. Menghitung PnL gabungan dua kaki, fee, funding, slippage, konversi mata uang, realized dan unrealized PnL.
6. Memilih exit berdasarkan net liquidation value yang dapat dieksekusi sekarang, target profit, biaya keluar, risiko, dan umur posisi.
7. Memulihkan order dan posisi setelah timeout, event hilang, WebSocket putus, atau proses restart.
8. Mencegah duplicate order, double-counted fill, ownership ganda, dan penutupan posisi yang salah.
9. Mempertahankan strategi yang sudah ada: Adaptive Maker Pegging, aggressive execution, dynamic profit target, fee engine, sizing, risk limits, dan aturan per CEX.

Non-goal:
- Tidak mengganti scanner Go menjadi Python atau sebaliknya.
- Tidak mengubah formula strategi secara diam-diam.
- Tidak mengasumsikan semua CEX mendukung kemampuan private WebSocket atau client order ID yang sama.
- Tidak menjadikan Redis sebagai sumber kebenaran order/posisi.
- Tidak mengaktifkan trading LIVE sebelum seluruh capability dan acceptance gate lulus.

## 2. Arsitektur tingkat tinggi

Opportunity Scanner / Evaluator
  -> Opportunity Normalizer
  -> Net Profit Estimator
  -> Opportunity Ranker + Priority Queue
  -> Central Execution Coordinator
       -> Risk & Admission Gate
       -> Execution Session Manager
       -> Spot Leg Manager
       -> Futures Leg Manager
       -> Pair PnL / Exposure Engine
       -> Exit Decision Engine
       -> Mutation Gateway (create/cancel)
       -> Exchange Adapter (private read + normalized venue contract)
       -> Reconciliation Engine
       -> Durable Command Processor

Supabase/PostgreSQL menyimpan canonical state:
- opportunity decision snapshot;
- execution sessions dan legs;
- order intents dan order registry;
- order events dan fills;
- balances/positions snapshots beserta freshness;
- PnL snapshots dan realized PnL ledger;
- risk state;
- leases/fencing tokens;
- command journal;
- transactional outbox.

Redis hanya untuk:
- ephemeral opportunity notification/queue acceleration;
- cache yang dapat dibangun ulang;
- wake-up signal;
- koordinasi non-canonical yang tidak menggantikan fencing database;
- telemetry/cache sementara.

### 2.1 Batas tanggung jawab

| Komponen | Tanggung jawab | Tidak boleh |
|---|---|---|
| Scanner | Mengumpulkan public market data dan membentuk kandidat | Mengirim private order |
| Opportunity Evaluator | Menghitung estimasi ekonomi dan kelayakan kandidat | Menganggap estimasi sebagai realized profit |
| Opportunity Ranker | Mengurutkan kandidat yang lolos hard filters | Mengubah risk limits atau mengabaikan freshness |
| Execution Coordinator | Satu otoritas untuk admission, session, ownership, lifecycle orchestration | Menanamkan kode khusus CEX atau formula strategi |
| Spot Leg Manager | Lifecycle order/fill/exposure Spot | Mengasumsikan Futures leg otomatis sama statusnya |
| Futures Leg Manager | Lifecycle order/fill/position Futures | Menutup posisi tanpa validasi side/position mode |
| Pair PnL Engine | Menghitung nilai ekonomi gabungan pasangan | Menjumlahkan persentase dua kaki secara naif |
| Exit Decision Engine | Memilih tindakan exit/hold/reduce/repair dari data tervalidasi | Menutup satu kaki hanya karena kaki itu sendiri hijau |
| Exchange Adapter | Private read dan normalisasi capability/response CEX | Mengirim create/cancel di luar gateway |
| Mutation Gateway | Semua create/cancel order; idempotency, timeout, rate policy | Blind retry pada hasil ambigu |
| Reconciliation Engine | Membandingkan durable state dengan bukti exchange | Menganggap event stream lengkap/berurutan |
| Telegram | Query dan membuat durable operator command | Memanggil private exchange langsung |

## 3. Opportunity Evaluation dan Ranking

### 3.1 Penempatan ranking

Ranking terjadi setelah kandidat dinormalisasi dan hard filters diperiksa, tetapi sebelum kandidat diserahkan ke Execution Coordinator.

Alur:
1. Scanner memperoleh book/ticker/funding dan status freshness.
2. Normalizer memastikan venue, market type, symbol, quote currency, contract size, tick/lot size, dan timestamp konsisten.
3. Hard filters menolak kandidat yang tidak valid: book stale/gap, symbol tidak cocok, market tidak didukung, minimum notional gagal, saldo tidak cukup, fee tidak terverifikasi, atau risk circuit terbuka.
4. Net Profit Estimator menghitung estimasi profit dua kaki pada ukuran kandidat yang realistis.
5. Ranker mengurutkan kandidat berdasarkan expected net return, expected net profit dalam mata uang dasar, kualitas fill, risiko, dan umur quote.
6. Kandidat dikirim ke priority queue dengan ranking snapshot dan expiry.
7. Coordinator mengambil kandidat dan melakukan final revalidation menggunakan data terbaru sebelum membuat intent.

Ranking tidak boleh menjadi otoritas untuk melewati risk gate. Kandidat nomor satu tetap dapat ditolak coordinator jika harga, saldo, fee, atau exposure berubah.

### 3.2 Hard filters dan ranking score

Hard filters dijalankan sebelum scoring. Contoh:
- kedua market/leg capability berstatus ENABLED dan telah tersertifikasi;
- book kedua kaki masih fresh dan tidak crossed/malformed;
- fee source memiliki status valid;
- order size memenuhi min amount, min notional, precision dan contract size;
- saldo tersedia dan tidak sedang reserved;
- funding/position mode dapat dibaca bila dibutuhkan;
- tidak ada unresolved orphan order atau RECONCILIATION_REQUIRED pada scope terkait;
- tidak melampaui max pending/open Spot order per CEX;
- estimated net profit melewati minimum strategy threshold.

Setelah hard filters, ranking score dapat menggunakan faktor:
- expected net profit dalam mata uang dasar;
- expected net ROI atas modal yang benar-benar terikat;
- estimated fill probability;
- spread depth/slippage pada ukuran order;
- quote age dan volatilitas;
- fee confidence dan funding uncertainty;
- execution latency dan venue health;
- imbalance/unwind risk.

Rekomendasi: gunakan lexicographic gates + normalized score, bukan satu rumus campuran yang membuat hard risk dapat dikalahkan oleh profit tinggi. Urutan prioritas:
1. Validity/safety gate — wajib lolos.
2. Minimum net-profit gate — wajib lolos.
3. Ranking score — mengurutkan kandidat yang lolos.
4. Coordinator final admission — pemeriksaan terakhir sebelum create.

Skor awal yang dapat diuji (bukan formula final produksi):
- komponen utama: expected net profit / expected capital;
- penyesuaian positif: fill probability dan depth;
- penalti: slippage, quote staleness, volatility, funding uncertainty, API degradation, dan unwind cost.
Bobot tidak boleh ditetapkan tanpa backtest/replay terhadap data historis dan paper/shadow validation. Jangan menyortir hanya berdasarkan spread persen.

### 3.3 Candidate snapshot dan freshness

Setiap kandidat harus menyimpan snapshot:
- opportunity_id dan candidate version;
- venue/account scope dan canonical pair;
- native market symbols;
- bid/ask depth kedua kaki;
- exchange timestamp dan receive timestamp;
- estimated order quantity/notional;
- expected entry price dan expected exit assumptions;
- maker/taker fee assumptions dan sumbernya;
- funding assumptions;
- conversion rates untuk IDR/USDT/USDC atau mata uang dasar;
- estimated gross/net PnL, net ROI, slippage dan confidence;
- ranking score dan alasan ranking;
- expiry/deadline.

Saat coordinator menerima kandidat, semua input yang dapat berubah harus divalidasi ulang. Candidate yang expired atau snapshot-nya stale tidak boleh dieksekusi.

### 3.4 Queue semantics

Redis priority queue boleh mempercepat dispatch, tetapi tidak menjadi satu-satunya catatan kandidat atau execution intent. Jika kandidat hilang dari queue, tidak boleh ada order yang sudah terkirim tanpa durable intent. Jika diperlukan delivery yang dapat dipulihkan, gunakan durable opportunity dispatch/outbox di Postgres dengan Redis sebagai wake-up.

Ranking harus stabil untuk kandidat dengan snapshot yang sama; tie-breaker deterministik: score, expected net profit, timestamp, lalu opportunity_id. Hindari starvation: kandidat bernilai lebih kecil tidak boleh tertahan tanpa batas jika tetap valid dan belum kedaluwarsa.

## 4. Ekonomi dua kaki dan Pair PnL Engine

### 4.1 Prinsip wajib

Semua keputusan profit/exit menggunakan hasil gabungan dua kaki dalam satu unit mata uang dasar yang disepakati untuk strategy/account, setelah memasukkan biaya relevan.

Jangan:
- menjumlahkan persentase profit Spot dan Futures secara langsung;
- menganggap unrealized PnL Futures sama dengan keuntungan yang sudah dapat direalisasikan;
- mengabaikan fee entry/exit, funding, borrow/interest jika berlaku, conversion spread, dan slippage;
- memakai last price/mark price sebagai bukti bahwa posisi dapat ditutup pada harga tersebut;
- menilai Spot untung secara terpisah lalu menutupnya tanpa menghitung exposure Futures yang tersisa.

### 4.2 Tiga tampilan profit yang berbeda

A. Estimated entry PnL:
- estimasi sebelum order, berdasarkan book dan ukuran kandidat;
- mencakup expected entry/exit fee, slippage, funding assumptions, conversion dan depth;
- hanya untuk ranking/admission.

B. Actual realized/unrealized PnL:
- berdasarkan fills aktual dan posisi aktual;
- realized PnL dihitung dari fill close yang terkonfirmasi;
- unrealized PnL memakai valuation yang sesuai dengan instrument dan side;
- fee aktual dan funding aktual dicatat terpisah agar tidak dihitung dua kali.

C. Net liquidation PnL (jika ditutup sekarang):
- nilai yang diperkirakan dapat direalisasikan jika kedua kaki ditutup sekarang;
- menggunakan bid/ask depth yang dapat dieksekusi untuk ukuran aktual;
- memasukkan expected close fee, slippage, funding accrued/expected sampai close, dan conversion cost;
- menjadi input utama Exit Decision Engine.

### 4.3 Rumus konseptual

Untuk Spot:
- Spot net liquidation value = proceeds jual berdasarkan bid depth yang dapat dieksekusi - expected exit fee - slippage/impact - conversion cost.
- Spot economic PnL = net liquidation value - allocated entry cost - entry fee - biaya terkait lainnya.
- Jika ada beberapa entry fills, gunakan lot/cost-basis policy yang eksplisit dan konsisten (misalnya weighted average cost atau metode yang sudah berlaku). Jangan mengubah metode tanpa migration/reconciliation plan.

Untuk Futures:
- Unrealized PnL dihitung sesuai contract type, contract size, side, entry average, mark/valuation policy, dan venue.
- Short linear contract secara konseptual: (average entry price - executable close buy price) × base exposure/contract multiplier, setelah penyesuaian contract denomination.
- Short inverse/contract khusus harus memakai formula venue/instrument; jangan menggunakan formula linear generik.
- Kurangi close fee, slippage/impact, funding accrued dan biaya relevan.
- Close price harus memakai ask depth untuk menutup short dengan BUY, bukan ticker last.

Pair net liquidation PnL:
- Pair net liquidation PnL = Spot net liquidation PnL + Futures net liquidation PnL + realized PnL yang belum masuk pair ledger - biaya pair yang belum dialokasikan.
- Jika realized PnL sudah dimasukkan ke cost basis/snapshot, jangan ditambahkan lagi.
- Seluruh komponen dikonversi ke mata uang dasar menggunakan rate yang timestamp/freshness-nya tervalidasi.

Pair net return:
- Pair net ROI = pair net liquidation PnL / capital basis yang didefinisikan secara konsisten.
- Capital basis harus menjelaskan apakah memakai gross notional, margin committed, atau total capital at risk. Jangan mencampur denominator antar kandidat.

### 4.4 Fee, funding dan konversi

Setiap fee record menyimpan:
- venue, account, market, order/trade ID;
- maker/taker classification jika tersedia;
- fee amount, fee currency, rate/source, confidence;
- actual vs estimated flag;
- timestamp dan fill association.

Fee engine harus:
- mempertahankan existing maker/taker source dan verification state;
- memakai fee aktual dari trade bila tersedia;
- tidak menggandakan fee aktual dengan estimasi;
- menandai fee yang belum diketahui sebagai uncertainty, bukan nol;
- memasukkan fee dalam token lain melalui conversion rate yang valid.

Funding:
- catat funding payment aktual dari exchange bila tersedia;
- estimasi funding berikutnya hanya untuk keputusan hold/exit, dengan uncertainty;
- bedakan realized funding dari projected funding;
- jangan menganggap funding rate positif/negatif memberi hasil yang sama untuk semua side.

FX/conversion:
- setiap conversion memakai source, timestamp, spread/fee dan freshness;
- jika quote currencies berbeda, semua legs dinormalisasi ke base currency yang sama;
- jika conversion rate stale/tidak tersedia, tandai PnL confidence rendah dan tahan entry baru sesuai risk policy.

### 4.5 PnL reconciliation

PnL ledger dibangun dari fill, fee, funding, conversion dan adjustment yang memiliki identitas sumber. Snapshot PnL adalah hasil turunan yang dapat dibangun ulang. Setiap update harus idempotent. PnL engine harus menyediakan breakdown per:
- Spot;
- Futures;
- realized;
- unrealized;
- fee entry;
- fee exit estimate;
- funding realized/projected;
- slippage/impact;
- conversion;
- total pair net liquidation;
- confidence/freshness.

## 5. Two-Leg Execution Controller dan kewajiban hedge

### 5.1 Makna "wajib open order saat Spot terbuka"

Desain membedakan tiga kondisi:
1. Spot order masih OPEN tetapi belum ada fill: belum ada Spot exposure dari order tersebut. Sistem harus memiliki kebijakan hedge readiness sebelum membiarkan order maker tetap aktif, tetapi tidak boleh menciptakan Futures quantity fiktif.
2. Spot order PARTIALLY_FILLED atau FILLED: setiap tambahan Spot fill yang meningkatkan exposure wajib menghasilkan hedge decision untuk delta yang belum terlindungi. Secara default strategi pasangan ini harus mengirim/menjaga Futures hedge order sesuai target hedge ratio dan aturan existing.
3. Spot order cancel/reject/unknown: tetap rekonsiliasi kemungkinan fill/cancel race sebelum menyimpulkan exposure nol.

Jadi, aturan yang tepat bukan "selalu kirim Futures order sebesar total Spot order saat Spot pending". Aturannya adalah **setiap Spot fill yang terkonfirmasi dan belum terlindungi harus segera diproses oleh hedge controller**. Ini menghindari short berlebih sebelum Spot benar-benar terisi, tetapi tetap melindungi exposure begitu fill terjadi.

### 5.2 Hedge accounting

Untuk setiap leg simpan:
- target quantity;
- submitted quantity;
- cumulative filled quantity;
- canceled/rejected quantity;
- verified net position quantity;
- hedge target ratio;
- hedged quantity;
- unhedged quantity;
- in-flight hedge quantity;
- tolerance/rounding remainder;
- exposure age.

Unhedged quantity:
- Spot exposure terverifikasi dikurangi Futures hedge exposure terverifikasi yang ekuivalen setelah contract-size conversion.
- In-flight Futures order tidak boleh dianggap sebagai hedge terisi.
- Futures position yang belum diverifikasi tidak boleh dianggap melindungi Spot.
- Rounding/lot-size remainder harus terlihat dan dibatasi oleh risk policy.

### 5.3 Urutan eksekusi

Urutan maker/aggressive harus tetap mengikuti strategy yang sudah ada. Controller mengelola lifecycle dan delta:
1. Coordinator menyimpan session dan kedua leg.
2. Spot Leg Manager mengirim order sesuai strategi.
3. Fill event Spot masuk ledger dan dideduplikasi.
4. Controller menghitung delta Spot baru yang belum terlindungi.
5. Futures Leg Manager mengirim hedge untuk delta yang diizinkan, dengan side/position mode/contract size benar.
6. Reconciliation memastikan Futures fill/position benar.
7. Controller memperbarui residual exposure.
8. Jika hedge gagal/terlambat, risk policy menjalankan tindakan protektif yang sudah disetujui: retry terbatas setelah ambiguity resolved, cancel remaining Spot order, reduce/rollback, atau force-close. Tidak boleh mengirim create ulang saat hasil sebelumnya unknown.

### 5.4 Hedge deadline dan fail-safe

Setiap CEX/pair mempunyai hedge latency budget yang dapat dikonfigurasi. Jika terlampaui:
- tandai session DEGRADED/UNHEDGED;
- stop menambah Spot exposure;
- batalkan sisa Spot entry jika aman dan sesuai state;
- verifikasi order/position Futures;
- jalankan existing rollback/force-close policy berdasarkan exposure aktual;
- eskalasi Telegram/incident bila hedge tidak dapat dipastikan.

Tidak ada satu angka global sebelum latency, API behavior dan market liquidity diukur.

## 6. Exit Decision Engine — kapan waktu keluar paling tepat

### 6.1 Tujuan

Exit bukan sekadar "profit salah satu kaki sudah positif". Exit Engine memilih tindakan yang memaksimalkan expected net outcome sambil menjaga risiko pair dan memastikan kedua kaki ditutup/diimbangkan dengan benar.

Input wajib:
- actual Spot fills dan cost basis;
- verified Futures position, entry average, contract size dan side;
- open/pending orders kedua kaki;
- pair net liquidation PnL;
- actual fee dan estimated close fee;
- funding realized/projected;
- bid/ask depth dan executable close VWAP;
- spread convergence/divergence;
- volatility dan quote freshness;
- remaining unhedged exposure;
- target profit dinamis;
- stop loss/risk limits;
- time-in-trade dan opportunity cost;
- exchange health, rate-limit state, private stream freshness;
- exit strategy/maker/aggressive configuration.

### 6.2 Exit conditions

Engine mengevaluasi:
1. Target profit: pair net liquidation PnL mencapai dynamic net target setelah biaya.
2. Stop loss: pair net loss atau risk metric melewati batas.
3. Spread convergence: expected edge tersisa tidak cukup membayar biaya/risiko hold.
4. Spread reversal: kondisi pasar bergerak melawan pasangan.
5. Funding/holding cost: biaya hold yang diproyeksikan menghapus expected edge.
6. Time stop: posisi terlalu lama atau opportunity cost melampaui manfaat menunggu.
7. Liquidity deterioration: depth tidak cukup untuk keluar dengan slippage yang dapat diterima.
8. Risk/emergency: posisi, exchange, data, atau account state tidak lagi aman.
9. Partial exit: hanya bila strategy/risk policy mengizinkan dan residual pair exposure dihitung ulang.

### 6.3 Menilai Spot yang masih untung

Jika Spot masih profit tetapi Futures mengalami loss, jangan melihat Spot secara terpisah. Hitung:
- Spot net liquidation PnL jika dijual sekarang;
- Futures net liquidation PnL jika short ditutup sekarang;
- total pair net liquidation PnL;
- fee dan slippage untuk kedua tindakan;
- exposure yang tersisa jika hanya satu kaki ditutup.

Contoh konseptual:
- Spot net liquidation PnL: +12 unit mata uang dasar.
- Futures net liquidation PnL: -7 unit.
- Estimasi close fees/slippage tambahan: -2 unit.
- Pair net liquidation PnL: +3 unit.

Walaupun Futures sedang rugi, pasangan masih berpotensi net profit jika ditutup sekarang. Sebaliknya, Spot yang untung +12 tidak berarti exit menguntungkan jika Futures loss -15 dan biaya close -2. Keputusan dibuat pada angka pair + biaya close.

Jika satu kaki ditutup lebih dahulu karena keterbatasan likuiditas, sistem wajib menandai leg lain sebagai residual directional exposure, memperbarui risk limits, dan segera membuat rencana penyelesaian. Jangan menganggap session selesai hanya karena salah satu leg sudah closed.

### 6.4 Exit action planning

Exit Engine mengeluarkan keputusan terstruktur:
- HOLD;
- CANCEL_REMAINING_ENTRY;
- REDUCE_SPOT;
- CLOSE_FUTURES;
- CLOSE_SPOT;
- CLOSE_BOTH;
- REPAIR_HEDGE;
- EMERGENCY_UNWIND;
- WAIT_FOR_RECONCILIATION.

Keputusan harus berisi alasan, input snapshot, estimated net PnL before/after, expected slippage, exposure after action, deadline, dan confidence. Coordinator memvalidasi risk/ownership sebelum menjalankan action.

Urutan close tidak boleh hard-coded sama untuk semua CEX. Pilihan close Spot dahulu atau Futures dahulu mempertimbangkan:
- mana yang paling likuid/cepat;
- risiko market bergerak saat kaki lain belum ditutup;
- margin/liquidation risk Futures;
- kemampuan reduceOnly;
- actual exposure dan hedge ratio;
- fee dan slippage;
- order state/unknown state.

### 6.5 Anti-churn dan hysteresis

Untuk mencegah order cancel/replace/exit bolak-balik:
- gunakan threshold hysteresis;
- minimum dwell/cooldown yang sesuai strategy;
- evaluasi perubahan PnL setelah biaya, bukan noise tick;
- pertahankan Adaptive Maker Pegging dan dynamic target semantics;
- emergency/risk exit boleh mengesampingkan dwell time;
- setiap perubahan target/exit decision dicatat dengan reason code.

Parameter threshold tidak ditetapkan di dokumen desain; harus diambil dari konfigurasi strategi saat ini atau ditinjau terpisah dengan data replay.

## 7. State machine

### 7.1 Order lifecycle

State yang diusulkan:
- INTENT_CREATED
- SUBMISSION_PENDING
- SUBMISSION_UNKNOWN
- OPEN
- PARTIALLY_FILLED
- CANCEL_PENDING
- FILLED
- CANCELED
- REJECTED
- RECONCILIATION_REQUIRED

Aturan:
- intent durable sebelum write;
- timeout create -> SUBMISSION_UNKNOWN;
- timeout cancel -> tetap CANCEL_PENDING/RECONCILIATION_REQUIRED sampai diverifikasi;
- cancel ack bukan bukti tidak ada fill;
- state tidak boleh mundur akibat event lama;
- FILLED/CANCELED tidak menghapus kemungkinan fill yang belum direkonsiliasi;
- status terminal hanya final setelah order/trade evidence cukup.

### 7.2 Leg lifecycle

CREATED -> ENTRY_WORKING -> PARTIALLY_EXPOSED -> HEDGING/PROTECTED -> OPEN_PROTECTED -> EXIT_WORKING -> CLOSED_VERIFIED

Status pengecualian:
- UNHEDGED
- DEGRADED
- RECOVERY_REQUIRED
- RECONCILIATION_REQUIRED
- ABORTING

### 7.3 Execution session lifecycle

CREATED -> VALIDATING -> EXECUTING -> MANAGING -> EXITING -> RECONCILING -> COMPLETED

Status pengecualian:
- REJECTED
- DEGRADED
- RECOVERY_REQUIRED
- ABORTING
- FAILED

Session hanya COMPLETED jika:
- tidak ada order aktif/pending yang dimiliki session;
- Spot exposure verified sesuai target terminal (biasanya nol);
- Futures position verified flat atau residual exposure secara eksplisit dialihkan ke incident/manual ownership;
- fills, fees, funding dan PnL telah direkonsiliasi sesuai toleransi;
- tidak ada command kritis yang belum selesai.

## 8. Reconciliation dan event architecture

### 8.1 Jalur event utama

Private WebSocket event -> Event Normalizer -> Durable Event Inbox/Outbox -> Deduplication -> Order/Fill/Position Reconciliation -> State transition -> PnL/Exposure update -> Coordinator notification.

Jika event source tidak tersedia atau tidak tersertifikasi untuk suatu CEX:
- gunakan REST fallback terjadwal dan terkontrol;
- tandai source mode REST_ONLY;
- jangan mengklaim event-driven live coverage untuk venue tersebut.

### 8.2 Event contract

Minimal:
- event_id;
- venue/account/market scope;
- event_type;
- exchange_order_id;
- client_order_id bila ada;
- native symbol;
- trade_id/fill_id;
- cumulative filled quantity;
- last fill quantity/price/fee bila tersedia;
- exchange timestamp;
- received timestamp;
- source (private_ws, rest_reconcile, internal);
- sequence/update version bila tersedia;
- raw payload reference/hash untuk audit tanpa membocorkan credential.

Event duplikat harus idempotent. Event tidak berurutan tidak boleh menurunkan cumulative fill. Jika exchange tidak menyediakan sequence, gunakan trade IDs, timestamps, cumulative quantities dan REST verification.

### 8.3 Tiga lapisan recovery

1. Event-driven reconciliation: jalur normal tercepat.
2. Startup recovery: wajib sebelum coordinator READY dan sebelum new exposure.
3. Periodic safety sweep: menemukan event yang hilang, stream gap, atau state mismatch.

Periodic sweep menggunakan budget API per CEX; interval dan pagination harus capability-specific. Jika inventory tidak lengkap, scope tetap UNKNOWN/DEGRADED.

### 8.4 WebSocket disconnect dan stale data

- Private stream putus: private event freshness menjadi STALE.
- REST fallback membaca open orders, order detail, trades/fills, balances dan positions sesuai kebutuhan.
- Setelah reconnect, lakukan resync sebelum menganggap stream sehat.
- Event gap/unknown order mapping masuk reconciliation queue.
- New exposure dibatasi jika status order/position tidak dapat diverifikasi.
- Public market-data freshness dan private order-event freshness adalah dua health dimension berbeda.

## 9. Coordinator, standby, ownership dan concurrency

Deployment:
- satu active coordinator;
- satu standby coordinator yang tidak mengirim mutasi sebelum memperoleh lease dan fencing token baru;
- health check dan heartbeat;
- failover hanya setelah lease expired/ownership transfer tervalidasi.

Database:
- lease scope minimal account + canonical pair/session ownership;
- fencing token monotonik;
- semua state-changing operation memvalidasi token;
- stale coordinator tidak dapat menulis state atau mengirim mutasi;
- transaction/constraint mencegah duplicate claim.

Parallelism:
- session berbeda dapat berjalan paralel;
- satu session hanya punya satu logical owner;
- mutation scheduling dibatasi per account/venue/endpoint;
- per-CEX Spot max pending/open order slot direservasi atomik;
- default cap 2 tetap dipertahankan kecuali konfigurasi operator mengubahnya;
- Redis lock bukan satu-satunya bukti ownership.

Standby tidak boleh aktif hanya karena proses utama gagal health check lokal. Ia harus memastikan lease/fencing dari Postgres. Jika Postgres tidak tersedia, standby tidak boleh mengambil alih hak mutasi.

## 10. Exchange Adapter dan Mutation Gateway

### 10.1 Exchange Adapter

Semua private API harus melalui adapter contract:
- create/cancel hanya dapat dipanggil oleh Mutation Gateway;
- read order/open orders/history/trades/balance/positions dilakukan melalui adapter;
- capability metadata per venue/account/market;
- normalized order/fill/position/error types;
- native response/raw metadata disimpan untuk audit terbatas.

### 10.2 Mutation Gateway

Mengatur:
- create/cancel;
- client order ID generation/mapping;
- idempotency key;
- shared rate limit/cooldown;
- request deadline;
- typed result: confirmed, rejected, unknown, deferred;
- request tracing dan audit;
- bounded concurrency dan priority.

Tidak ada blind retry create. Retry hanya setelah reconciliation membuktikan order pertama tidak diterima atau idempotency venue benar-benar menjamin safe replay. Client order ID wajib jika didukung; local intent ID tetap wajib di semua venue.

### 10.3 Capability registry

Per CEX/account/market type harus menyatakan:
- create/cancel;
- client order ID support dan lookup support;
- private WS order/trade stream;
- REST open orders/order history/trades;
- pagination completeness;
- partial fill semantics;
- reduceOnly/position side/margin mode;
- post-only/TIF;
- precision/contract size;
- fee/funding endpoints;
- balance/position read;
- rate-limit weights;
- ambiguity recovery method.

Status capability:
- VERIFIED;
- VERIFIED_WITH_LIMITS;
- REST_ONLY;
- UNSUPPORTED;
- UNKNOWN.

UNKNOWN tidak boleh dianggap VERIFIED.

### 10.4 Batas CEX baseline

Mengikuti baseline audit:
- Tokocrypto: Spot private route evidenced; client-ID mapping dan endpoint shape harus dites.
- Indodax: Spot TAPIv2 custom adapter; pertahankan signature, history, myTrades, post-only/TIF semantics.
- OKX: Futures private route evidenced; verifikasi posSide, reduceOnly, position mode, contract sizing.
- MEXC: Futures private route evidenced; verifikasi swap params, contract sizing, history dan position semantics.
- Binance/Bybit: baseline hanya membuktikan auxiliary balance clients; jangan aktifkan trading sampai end-to-end capability test lulus.

Private WebSocket availability tidak boleh diasumsikan dari adanya public WebSocket manager.

## 11. Supabase/Postgres schema proposal

Nama tabel berikut usulan konseptual; harus dibandingkan dengan schema existing sebelum migration. Jangan membuat tabel duplikat jika fungsi serupa sudah ada.

| Tabel/konsep | Data utama | Constraint penting |
|---|---|---|
| execution_sessions | session_id, opportunity_id, strategy snapshot, status, owner/fencing, timestamps | idempotent session key |
| execution_legs | leg_id, session_id, leg_type, venue/account/market, target/filled/exposure/hedge quantities | unique session + leg_type |
| opportunity_decisions | candidate snapshot, rank, score, assumptions, rejection reason | candidate version/idempotency |
| order_intents | intent_id, leg_id, client_order_id, requested side/type/qty/price, status | unique intent; unique client ID per venue/account where applicable |
| order_registry | local/exchange order identity, latest status, cumulative fill, last verified time | unique venue/account/exchange order ID |
| order_events | append-only event history | unique event dedupe key |
| order_fills | trade ID, order ID, qty, price, fee, fee currency, timestamps | unique venue/account/trade ID |
| position_snapshots | venue/account/instrument, side, qty, entry price, margin, observed time | versioned snapshots |
| balance_snapshots | asset, free/used/total, observed time | freshness tracking |
| pair_pnl_snapshots | Spot/Futures/realized/unrealized/fee/funding/FX/net liquidation breakdown | source version and computed_at |
| pnl_ledger | immutable realized/fee/funding/adjustment entries | unique source transaction |
| execution_leases | scope, owner, token, expires_at, heartbeat | atomic ownership/fencing |
| order_slot_reservations | venue/account/slot key/session/order/status | atomic cap enforcement |
| command_journal | command_id, type, scope, payload, state, attempts, ack | idempotent command ID |
| outbox_events | event_id, aggregate_id, type, payload, published_at | transactional insert with state mutation |
| event_inbox | event_id, source, payload reference, processing status | unique dedupe key |

Schema rules:
- use NUMERIC/decimal-safe representation for price, quantity, fee and PnL; avoid binary float as persisted financial truth;
- timestamps store exchange time and receive time separately;
- all account/venue scoped records have explicit account identity (credential secret never stored in event payload);
- foreign keys and unique constraints enforce identity;
- indexes cover nonterminal orders/sessions, account+venue+symbol, event dedupe and command pending scans;
- append-only event/ledger rows are not silently overwritten;
- migrations must be additive and idempotent within the repository's migration runner conventions;
- inspect existing migrations and schema_migrations before adding any migration; never create duplicate version IDs.

### 11.1 Transactional outbox/inbox

A database state transition and its event publication intent must commit in one transaction. Publisher retries outbox delivery; consumers deduplicate by event ID. Redis Pub/Sub is only a wake-up mechanism.

Inbound exchange events are stored or durably checkpointed before being acknowledged as processed, subject to venue delivery guarantees. If durable event ingestion cannot be guaranteed, REST reconciliation remains required.

## 12. Durable command journal dan Telegram

Commands:
- cancel one order;
- cancel all within explicit scope;
- force close a session/pair/account;
- pause/resume new entries;
- run reconciliation;
- operator acknowledge/resolve incident.

Flow:
1. Telegram validates authorization and arguments.
2. Create command_id and persist command.
3. Emit outbox/wakeup.
4. Coordinator claims command under lease.
5. Validate current exchange state and risk.
6. Execute allowed action through adapter/gateway.
7. Reconcile actual outcome.
8. Persist terminal result and operator-visible details.

Emergency commands must survive Telegram/executor restart. Cancel-all and force-close must have explicit scope and preview/confirmation rules. A command cannot claim success based solely on request acknowledgement.

## 13. Supabase unavailable policy

Default:
- block new exposure and new entry intents;
- do not create orders without durable intent;
- do not promote Redis cache to canonical state;
- continue safe observation/read-only work only if rate policy and account scope permit;
- protective cancel/close is allowed only when actual order/position state can be verified sufficiently and a pre-approved emergency policy permits it;
- if protective write result is ambiguous, stop repeat writes and reconcile when service returns;
- persist local incident logs where possible, but local logs are not canonical proof;
- on database recovery, reconcile all actions/events before READY.

A protection exception must be narrowly scoped and tested. It must never become a general bypass for order creation.

## 14. Failure matrix

| Failure | Required behavior |
|---|---|
| Create timeout after exchange acceptance | SUBMISSION_UNKNOWN; lookup by client ID/order history/trades; no blind create |
| Create response success but DB update fails | durable intent remains; startup/event reconciliation resolves exchange order |
| Cancel timeout | CANCEL_PENDING/UNKNOWN; fetch order and fills |
| Cancel acknowledgement but late fill | ingest fill, recalc exposure/PnL, do not assume zero |
| Spot partial fill, Futures hedge fails | stop additional Spot exposure; reconcile hedge; execute bounded protective policy |
| Futures order filled but position read stale | hedge not considered fully verified until position/fill evidence reconciles |
| Private WS disconnect | mark stream stale; REST fallback and resync |
| Event duplicate/out of order | dedupe and monotonic cumulative-fill reconciliation |
| Unknown orphan order | record orphan, block relevant scope until resolved |
| Unknown futures position | never assume flat; block conflicting action/new exposure |
| Coordinator crash | standby waits for valid lease/fencing; recover nonterminal sessions |
| Old coordinator resumes after failover | fencing rejects stale writes; mutation gate checks ownership |
| Supabase unavailable | stop new exposure; verified protection only |
| Redis unavailable | canonical DB state remains; polling/wakeup fallback |
| Fee unavailable/unverified | mark PnL uncertain; block new entry if minimum net edge cannot be proven |
| FX conversion stale | PnL confidence degraded; do not rank as fully valid |
| Spot/Futures contract size mismatch | reject candidate or halt session; never infer quantity equivalence |
| Exit one leg succeeds, other fails | session remains open/degraded with residual directional exposure and recovery action |

## 15. Test strategy dan acceptance criteria

### 15.1 Opportunity ranking
- same snapshot produces deterministic rank;
- ranking uses net profit after fees and expected slippage;
- hard filters cannot be overridden by high score;
- stale/gapped book is rejected;
- queue expiry is respected;
- coordinator final revalidation rejects deteriorated opportunity;
- FX/fee uncertainty lowers confidence or rejects candidate;
- no starvation under sustained candidate load.

### 15.2 Pair PnL
- Spot and Futures PnL individually match venue contract fixtures;
- combined pair PnL equals sum of normalized components exactly once;
- maker/taker entry and exit fees are included correctly;
- fee in non-quote currency is converted using correct timestamped rate;
- funding actual vs projected is not double counted;
- partial fills produce correct weighted cost basis;
- close VWAP uses correct side/depth;
- inverse and linear contracts use different validated formulas;
- stale price/FX data marks PnL untrusted;
- PnL ledger rebuild yields same snapshot.

### 15.3 Two-leg and hedge
- no hedge for unfilled Spot quantity;
- every confirmed Spot fill delta creates exactly one hedge decision;
- in-flight Futures order is not treated as filled hedge;
- duplicate Spot fill event does not duplicate hedge;
- partial Futures fill leaves accurate residual unhedged quantity;
- contract rounding remainder is bounded;
- hedge timeout is ambiguous, not failed;
- Spot remainder is canceled/stopped after hedge deadline according to policy;
- restart between Spot fill and Futures submission recovers delta once.

### 15.4 Exit
- exit target uses pair net liquidation PnL after close costs;
- profitable Spot does not trigger isolated close if pair economics/risk say otherwise;
- a profitable pair is not reported when exit fees turn it negative;
- stop-loss/emergency overrides target/dwell;
- partial exit recalculates remaining pair exposure;
- close order side/position mode/reduceOnly are correct;
- one-leg close failure leaves session nonterminal;
- realized PnL and remaining unrealized PnL are not double counted.

### 15.5 Lifecycle/recovery
- create/cancel timeout, cancel/fill race, out-of-order events, duplicate fills;
- crash at every boundary before/after DB commit and exchange request;
- startup recovery before READY;
- periodic sweep catches dropped private WS events;
- lease expiry and fencing prevent stale coordinator writes;
- per-CEX Spot order cap remains atomic;
- command journal survives process downtime;
- Supabase outage blocks new exposure.

### 15.6 Existing regression suite

Existing tests listed in docs/REGRESSION-CONTRACT.md remain mandatory starting points, including adaptive maker, entry, exits, two-leg execution recovery, order persistence, recovery, force close, exposure/fill reconciliation, fee engine, signal priority, venue adapters, and production failure injection. Expand tests; do not replace the old suite with only new tests.

Go tests, Python tests/static analysis, SQL smoke/migration ordering, adapter capability tests, and CI must all pass for the touched scope. Paper/shadow validation is required before any LIVE write enablement.

## 16. Phased implementation plan

| Phase | Scope | Exit gate |
|---|---|---|
| 0. Source audit | Re-fetch latest branch; trace every caller, direct private API, state owner, current schema and tests | Complete file/function map and no unknown write path |
| 1. Contracts | Define typed domain contracts, state transitions, money/quantity types, capability schema | Contract tests pass without live behavior change |
| 2. Persistence | Additive schema, unique constraints, inbox/outbox, PnL ledger | Migration smoke and crash consistency pass |
| 3. Coordinator | Active/standby lease, fencing, session admission and ownership | Split-brain/failover tests pass |
| 4. Adapter/gateway | Central create/cancel path; read adapter contract; one writer per venue operation | No bypass and ambiguity tests pass |
| 5. Leg managers | Independent Spot/Futures lifecycle and hedge delta controller | Partial-fill/restart matrix passes |
| 6. PnL and exit | Pair PnL, net liquidation, exit decisions with strategy parity | Historical replay and regression parity pass |
| 7. Reconciliation | Private WS where verified, REST fallback, startup and periodic sweep | Event gap/drop/reorder tests pass |
| 8. Telegram/commands | Durable command journal and monitoring | Offline/restart command tests pass |
| 9. Shadow/paper | Run new decision path without order writes; compare old/new decisions | Explainable parity and no invariant breach |
| 10. Controlled LIVE rollout | Per-CEX flags, one writer, operator approval, rollback plan | Explicit readiness sign-off |

Jangan menggabungkan seluruh phase menjadi satu rewrite besar. Setiap phase harus memiliki commit, tests, CI evidence, and rollback boundary. Do not enable old and new create paths simultaneously.

## 17. KEEP / MODIFY / NEW / DEFER

| Area/file family | Decision | Arahan |
|---|---|---|
| Go scanner/public market data | KEEP + MODIFY | Pertahankan feed dan signal contract; tambahkan ranking snapshot/score tanpa private order access |
| Opportunity evaluation/ranking | MODIFY | Pisahkan hard filters, net profit estimation, rank score, final coordinator revalidation |
| executor/engine.py | MODIFY | Coordinator facade/orchestration; kurangi tanggung jawab lifecycle yang bercampur |
| Spot execution modules | MODIFY | Spot Leg Manager dan exposure accounting |
| Futures execution modules | MODIFY | Futures Leg Manager, position-side/reduceOnly correctness |
| executor/venues/* | KEEP + MODIFY | Adapter contract dan capability metadata; pertahankan venue quirks |
| Indodax TAPIv2 | KEEP + ADAPT | Pertahankan signing/endpoints/post-only; integrasikan gateway tanpa rewrite protokol |
| order registry/persistence | KEEP + MODIFY | Tambahkan event ledger, intent, constraints dan reconciliation version |
| Supabase modules/migrations | MODIFY | Audit schema existing dahulu; additive migration only |
| trade fill/exposure reconciliation | KEEP + MODIFY | Jadikan bagian dari reconciliation engine |
| fee engine | KEEP | Tidak mengubah sumber dan formula tanpa persetujuan |
| Adaptive Maker Pegging | KEEP | Strategi tetap; hanya lifecycle integration |
| dynamic profit target | KEEP + ADAPT | Exit engine mengonsumsi target yang sudah dihitung; tidak mengubah formula diam-diam |
| risk/circuit breaker | KEEP + MODIFY | Durable restore, coordinator admission, exposure-aware fail-safe |
| Redis queue/cache/PubSub | KEEP + MODIFY | Wake-up/cache only; durable dispatch/commands di Postgres |
| Telegram | KEEP + MODIFY | Query + durable command interface |
| private WebSocket ingestion | NEW / VERIFY | Implement per CEX hanya setelah capability evidence |
| Pair PnL Engine | NEW or consolidate existing | Audit existing PnL terlebih dahulu; jangan duplicate calculator |
| Exit Decision Engine | MODIFY / consolidate | Satu sumber keputusan pair-level; hindari exit logic ganda |
| Full event sourcing | DEFER | Relational canonical state + append-only ledger cukup untuk tahap ini |
| Full Go rewrite | DEFER | Tidak ada alasan untuk menggabungkan dengan refactor executor |
| Binance/Bybit live trading | DEFER | Sampai end-to-end capability certification |

Nama file/fungsi final belum dikunci. Sebelum implementasi, semua kandidat file harus diperiksa pada HEAD terbaru dan seluruh caller/test ditelusuri. Inventaris awal menyatakan beberapa file hanya diperiksa pada tingkat cakupan; status itu tidak boleh dianggap sebagai audit penuh.

## 18. Rollout, observability dan rollback

Feature flags harus memiliki scope per venue/account/market:
- coordinator_v2_enabled;
- order_mutation_gateway_enabled;
- reconciliation_v2_enabled;
- pair_pnl_v2_shadow;
- exit_engine_v2_shadow;
- private_ws_enabled per venue;
- live_mutation_enabled hanya setelah explicit operator approval.

Shadow mode:
- menghitung ranking, pair PnL, hedge decisions dan exit recommendations tanpa mengirim order;
- membandingkan dengan keputusan existing;
- menyimpan perbedaan dan alasan;
- tidak boleh mengirim create/cancel.

Metrics:
- opportunity rank distribution dan rejection reason;
- estimated vs realized net PnL;
- pair net liquidation PnL freshness;
- fee/funding/FX uncertainty;
- hedge latency, unhedged quantity dan exposure age;
- order submission unknown count/age per CEX;
- private WS freshness/gap/reconnect;
- REST fallback volume/rate-limit;
- reconciliation mismatch/orphan count;
- active coordinator/standby/lease/fencing rejects;
- pending/open Spot slot usage per CEX;
- exit reason, expected vs realized close PnL;
- command journal age/ack/retry.

Rollback:
- stop new entries first if ambiguity or invariant breach;
- preserve current durable order/position state;
- reconcile all open orders and positions;
- switch to last proven single writer only after old/new ownership is unambiguous;
- never activate two create-order writers as fallback;
- do not rollback by deleting ledger/migration data;
- keep protective operations available only under verified-state policy.

## 19. Pertanyaan yang masih harus dijawab sebelum implementasi

1. Mata uang dasar untuk ranking dan pair PnL: USD/USDT, IDR, atau configurable per strategy? Rekomendasi: configurable, dengan satu canonical reporting currency per strategy dan FX snapshot eksplisit.
2. Cost basis Spot: apakah existing weighted-average policy dipertahankan? Rekomendasi: pertahankan metode saat ini setelah diaudit dan diuji.
3. Hedge ratio: apakah wajib 1:1 setelah konversi contract size, atau ada toleransi strategi? Rekomendasi: target sesuai strategi sekarang; sisa rounding/tolerance dibatasi risk config.
4. Exit target: apakah target dynamic existing tetap menjadi target utama dan Exit Engine hanya mengoptimalkan kapan/cara mengeksekusinya? Rekomendasi: ya.
5. Partial exit: apakah strategi saat ini mengizinkan close sebagian? Jika belum, jangan menambahkan partial exit behavior pada refactor pertama tanpa persetujuan.
6. Urutan close: pertahankan aturan existing per venue/strategy; jangan globalisasi sebelum replay dan test membuktikan aman.

## 20. Status review

Dokumen ini merupakan desain teknis untuk review. Belum ada implementasi, migration, test execution, perubahan branch implementasi, atau deployment yang dilakukan sebagai bagian dari dokumen ini.

Desain dianggap siap menjadi implementation specification setelah:
- pemilik proyek menyetujui model ranking dan mata uang dasar;
- formula Pair PnL dan definisi capital basis disepakati;
- aturan hedge delta dan deadline ditentukan;
- exit behavior dan partial-close policy disetujui;
- audit source terbaru menyelesaikan pemetaan file/fungsi dan caller;
- capability matrix per CEX diverifikasi.

