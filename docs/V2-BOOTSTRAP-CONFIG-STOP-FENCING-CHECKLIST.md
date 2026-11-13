# V2 Bootstrap, Runtime Configuration, STOP Fencing & CEX IP-Block Review

**Status:** Partial implementation on this branch — remaining checklist items are OPEN until regression/integration evidence exists  
**Branch target:** `feat/v2-bootstrap-config-stop-fencing`  
**Baseline:** `audit/architecture-baseline-regression-contract`

Dokumen ini adalah kontrak review untuk pekerjaan V2. Item yang belum mempunyai implementasi, migration, dan regression test harus tetap dianggap OPEN. Jangan menandai LIVE-ready hanya karena dokumen atau schema sudah dibuat.

## 1. Non-negotiable invariants

1. Risk limits, sizing, leverage, symbol/venue mapping, dan pengaturan venue tidak berubah akibat migrasi.
2. PostgreSQL menjadi sumber konfigurasi runtime sebelum cutover. `config.json` dan environment lama hanya dipakai pada fase import/shadow dan untuk bootstrap secrets/infrastructure setelah cutover.
3. Admission order harus fail-closed ketika database, lease, snapshot, heartbeat, atau STOP state tidak dapat diverifikasi.
4. STOP dan submit order harus diserialisasi pada satu otoritas PostgreSQL lintas-instance; Redis Pub/Sub hanya notifikasi, bukan sumber kebenaran.
5. STOP memblokir order pembuka baru. Manajemen posisi yang sudah ada dan de-risking hanya boleh berjalan melalui jalur yang aman dan tervalidasi; jangan mematikan monitoring posisi aktif.
6. Setiap keputusan STOP, resume, fencing, dan CEX ban harus dapat diaudit tanpa menyimpan API key, secret, passphrase, atau credential.

## 2. Startup bootstrap V2

Urutan yang wajib diverifikasi:

- [ ] Load konfigurasi aktif dari PostgreSQL, validasi schema/version, required keys, dan checksum.
- [ ] PostgreSQL connection pool siap dan transaksi sederhana berhasil.
- [ ] Redis siap; consumer belum boleh membuka admission.
- [ ] Acquire instance lease dan fencing generation/token secara atomik.
- [ ] Load canonical runtime snapshot, open positions, pending order intents, order registry, risk/circuit-breaker state, dan active STOP state.
- [ ] Reconcile snapshot/registry dengan venue exposure dan open orders sebelum READY.
- [ ] Start Redis consumer, durable command consumer, dan STOP listener.
- [ ] Start heartbeat; heartbeat harus memperbarui lease dengan compare-and-set terhadap token/generation yang dimiliki.
- [ ] Admission gate dibuka hanya jika semua dependency dan reconciliation lulus.
- [ ] Shutdown/restart melepaskan lease secara aman; expired lease tidak boleh membuat token lama valid kembali.

**Fail-closed checks:** PostgreSQL unavailable, config missing/invalid, migration version mismatch, lease conflict, heartbeat lost, snapshot corrupt, unclassified order, unknown venue exposure, atau STOP active harus menahan READY/admission.

## 3. PostgreSQL runtime configuration migration

### 3.1 Mapping dan provenance

- [ ] Inventarisasi seluruh key `config.json`, nested `executor`, `redis`, `supabase`, `exchanges`, serta seluruh environment variable yang dibaca oleh `ExecutorConfig._apply_env()` dan adapter venue.
- [ ] Petakan setiap key ke kolom/JSON path PostgreSQL dengan type, default, validation, owner, secrecy classification, dan source provenance.
- [ ] Pisahkan runtime config non-secret dari secret references. Credential tidak boleh disimpan plaintext di tabel config atau log.
- [ ] Catat precedence lama secara eksplisit; jangan mengasumsikan urutan JSON/env. Baseline saat ini memanggil file config lalu environment override.
- [ ] Buat deterministic import/dry-run yang menghasilkan diff tanpa menulis atau mengubah nilai.
- [ ] Bandingkan effective config lama dan hasil PostgreSQL secara semantik (angka, boolean, null/default, case venue, symbol mapping).
- [ ] Risk limits dan venue settings harus identik; setiap diff pada field terlindungi menggagalkan cutover sampai disetujui secara eksplisit.
- [ ] Shadow-read PostgreSQL dan log diff redacted sebelum mengaktifkan PostgreSQL sebagai authority.
- [ ] Setelah cutover, jangan fallback diam-diam ke config.json/env untuk runtime trading config. Database/config unavailable = no new admission.
- [ ] Sediakan rollback cutover yang mengembalikan sumber config tanpa mengubah data risk/venue dan tetap mempertahankan audit trail.

### 3.2 Protected fields

Minimal perlakukan field berikut sebagai protected dan lakukan exact semantic comparison:
- risk limits, max loss, per-pair allocation, max exposure, order caps, sizing, slippage, fee assumptions;
- execution mode, mock/live mode, leverage/margin mode bila ada;
- local/global exchange, account binding, market type, symbol/pair mapping, quote/base asset;
- venue order parameters, precision/lot/tick constraints, testnet/live endpoint selection.

Tidak boleh melakukan normalisasi yang mengubah nilai efektif (mis. pembulatan numeric, mengubah 0 menjadi default nonzero, atau menghapus venue override).

## 4. Distributed lease, heartbeat, and STOP fencing

- [ ] PostgreSQL menyimpan canonical executor ownership, lease expiry, monotonic fencing generation, STOP state, STOP epoch, reason, actor/source, timestamps, dan audit event.
- [ ] Lease acquisition/renewal memakai transaksi dan conditional update; instance dengan token usang tidak boleh memperbarui lease atau mengirim order.
- [ ] Order intent dibuat durable sebelum network submit dengan idempotency key/client order id yang stabil.
- [x] Jalur spot entry `_submit_entry_order` dan STOP request memakai advisory transaction lock PostgreSQL yang sama; transaksi menahan lock selama create-order HTTP call.
- [ ] Audit dan integrasikan seluruh jalur create-order lain yang membuka exposure; tambahkan concurrent multi-instance integration test.
- [ ] STOP commit harus mendahului setiap submit yang belum melewati titik serialisasi; submit yang telah memperoleh hak serialisasi lebih dahulu dicatat sebagai in-flight dan diselesaikan/reconcile secara eksplisit.
- [ ] Jangan menganggap pemeriksaan STOP lalu melepas transaksi sebelum HTTP request sebagai fencing yang cukup.
- [ ] Redis Pub/Sub hanya mempercepat propagasi STOP. Setiap instance wajib membaca ulang state PostgreSQL sebelum order admission dan pada jalur submit.
- [x] STOP berulang idempotent terhadap epoch/reason pada singleton control row.
- [ ] Audit trail append-only dan audit actor/authorization belum tersedia.
- [x] Primitive resume mensyaratkan operator string dan expected epoch, serta menaikkan epoch.
- [ ] Endpoint authorization, health/ban verification, dan reconciliation gate sebelum resume belum diintegrasikan.
- [ ] Kehilangan koneksi database saat submit harus gagal tertutup untuk submit baru; status order yang sudah dikirim menjadi unknown dan wajib direconcile memakai client order id.

## 5. CEX rate limit vs IP block: STOP request policy

**Requirement baru — wajib diimplementasikan dan diuji.** Jika CEX secara eksplisit memblokir IP egress executor akibat rate limit, executor harus mengirim **global STOP request** ke otoritas PostgreSQL. STOP tersebut harus terlihat oleh semua instance yang menggunakan egress IP/NAT yang sama.

### 5.1 Classification (jangan menyamakan semua 429 dengan IP ban)

- [ ] Classifier per venue dari HTTP status, exchange error code, response headers, dan structured CCXT exception; implementasi awal baru mengenali signature teks eksplisit (418/IP banned) dan belum merupakan adapter classifier lengkap.
- [ ] Kategori `RATE_LIMITED`: HTTP 429 atau exchange-specific throttle yang berarti request dibatasi tetapi tidak membuktikan IP diban. Terapkan venue-scoped cooldown, exponential backoff + jitter, dan hentikan polling yang tidak perlu. Jangan otomatis mengklaim IP ban.
- [x] Signature eksplisit HTTP 418 / IP banned pada jalur spot entry memicu `CEX_IP_BLOCK`; STOP ditulis di transaksi submit yang sama sebelum advisory lock dilepas bila PostgreSQL tersedia.
- [ ] Tambahkan signature tervalidasi per venue dan response/header parser; jangan perluas ke HTTP 403 generik.
- [ ] HTTP 403/Cloudflare/WAF tidak otomatis dianggap IP ban kecuali adapter dapat membuktikan blok IP; selain itu klasifikasikan sebagai `VENUE_ACCESS_BLOCKED_UNKNOWN`, fail-closed untuk venue tersebut dan minta review operator.
- [ ] Error jaringan, timeout, 5xx, invalid key, insufficient balance, dan order rejection biasa tidak boleh salah diklasifikasikan sebagai IP ban.
- [ ] Simpan raw response secara redacted dan bounded: status, venue, error code, response headers allowlist, endpoint category, timestamp, request weight bila tersedia. Jangan simpan credential, signature, atau seluruh header sensitif.

### 5.2 STOP behavior on confirmed IP ban

Ketika `IP_BANNED` terkonfirmasi:

1. [ ] Instance yang mendeteksi segera menulis durable STOP request ke PostgreSQL melalui jalur yang atomik dan idempotent; jangan hanya publish Redis event.
2. [ ] STOP reason wajib `CEX_IP_BLOCK`; simpan venue, egress/NAT identity reference (bukan IP publik jika tidak diperlukan), exchange error code, ban-until bila disediakan, detector instance, dan evidence metadata redacted.
3. [ ] Naikkan STOP epoch/fencing generation secara monotonik agar semua instance lama kehilangan hak admission.
4. [ ] STOP berlaku global untuk semua instance yang berbagi egress IP, dan secara default menutup seluruh new-entry admission bot. Bila deployment memakai egress IP berbeda per venue, policy boleh scope lebih sempit hanya jika pemetaan egress sudah tervalidasi.
5. [ ] Broadcast event ke Redis untuk latensi rendah, tetapi consumer wajib membaca canonical STOP state dari PostgreSQL.
6. [ ] Jangan melakukan retry storm, health-check loop rapat, atau cancel-all berulang ke IP/venue yang sedang memblokir. Hormati ban-until/Retry-After jika ada dan gunakan backoff.
7. [ ] Existing positions tetap tercatat dan dipantau melalui jalur yang tidak memperparah ban. Order cancel/close pada CEX yang memblokir IP mungkin juga gagal; jangan menandai posisi flat atau order canceled tanpa konfirmasi venue.
8. [ ] Jika salah satu leg tidak dapat direconcile karena IP block, tandai exposure/order sebagai `RECONCILIATION_REQUIRED`, alert operator, dan jangan membuat hedge/close duplikat secara buta.
9. [ ] Jalur reduce-only/emergency action hanya boleh dipakai setelah venue mengizinkan request lagi atau melalui jalur akses alternatif yang telah disetujui dan aman. IP ban bukan alasan untuk mengirim request berulang.
10. [ ] STOP tidak boleh otomatis terhapus hanya karena cooldown lokal selesai atau satu request berhasil. Resume membutuhkan ban expiry/unblock terverifikasi, venue probe dengan rate aman, reconciliation posisi/order, dan operator acknowledgement.
11. [ ] Jika STOP store/PostgreSQL tidak tersedia saat IP ban terdeteksi, tutup local admission seketika, retry penyimpanan STOP secara bounded, dan tetap fail-closed; laporkan kegagalan persistensi sebagai CRITICAL.

### 5.3 Required observability and operator alert

- [ ] Status API memuat `trading_halted`, `halt_reason=CEX_IP_BLOCK`, `stop_epoch`, venue, detected_at, ban_until (jika ada), egress identity reference, dan resume eligibility.
- [ ] Alert high/critical dikirim satu kali per STOP epoch, dengan deduplication; alert berulang hanya untuk perubahan state/escalation.
- [ ] Dashboard/log harus membedakan `RATE_LIMITED`, `IP_BANNED`, `VENUE_ACCESS_BLOCKED_UNKNOWN`, dan `STOP_PERSISTENCE_FAILED`.
- [ ] Jangan memasukkan API key, secret, passphrase, signed URL, authorization header, atau credential ke alert.

## 6. Regression and integration test contract

### Bootstrap/config
- [ ] PostgreSQL unavailable / pool exhausted -> READY false, no entry.
- [ ] Invalid/missing config version or protected-field diff -> cutover rejected.
- [ ] Legacy config + env import -> effective values match exactly, including all risk and venue fields.
- [ ] Redis consumer failure, invalid snapshot, unknown exposure, or lease conflict -> admission closed.
- [ ] Heartbeat lost / lease stolen -> stale instance cannot submit.
- [ ] Restart recovers active STOP, lease epoch, order intents, and exposure before READY.

### STOP concurrency
- [ ] Submit wins serialization first -> STOP waits/records in-flight; no later submit can pass STOP commit.
- [ ] STOP wins first -> all subsequent submits from all instances are rejected.
- [ ] Two concurrent STOP requests -> one canonical active STOP epoch/reason with idempotent audit.
- [ ] Old fencing token after restart/lease expiry -> rejected.
- [ ] PostgreSQL disconnect at admission/submit boundary -> no new order submission.
- [ ] Redis notification loss -> periodic/call-path PostgreSQL check still enforces STOP.

### CEX IP-block cases
- [ ] Venue explicit IP-ban response (e.g. Binance 418 fixture) -> durable global STOP request with `CEX_IP_BLOCK`.
- [ ] Same egress IP on two executor instances -> both close admission after canonical STOP.
- [ ] Different egress IP mapping -> only explicitly configured scope may be stopped; default remains global.
- [ ] HTTP 429 throttle without ban evidence -> venue cooldown/backoff, no false IP-ban reason.
- [ ] HTTP 403/WAF ambiguous -> venue fail-closed + operator review, not false confirmed IP-ban.
- [ ] Invalid API key, insufficient balance, timeout, 5xx -> no false IP-ban STOP.
- [ ] Retry-After/ban-until -> no request storm before expiry.
- [ ] Existing open orders/positions during ban -> no false canceled/flat state; unresolved state is reconciliation-required.
- [ ] STOP persistence failure -> local admission closed and CRITICAL signal.
- [ ] Resume before ban expiry or without operator acknowledgement -> rejected.
- [ ] Resume after verified unblock + reconciliation + acknowledgement -> new epoch and admission may reopen.

## 7. Required documentation review

Sebelum merge/cutover, reviewer wajib membuka dan mencocokkan:
- [ ] `docs/AUDIT-FINDINGS.md` — R-19 dan status aktual CEX IP-block STOP.
- [ ] `docs/IMPROVEMENT-PLAN.md` — urutan dependency dan release gate V2.
- [ ] `docs/EXECUTOR-ARCHITECTURE-V2.md` — lifecycle, config authority, lease, STOP linearization.
- [ ] Migration SQL — constraints, indexes, RLS/grants, transaction functions, monotonic epochs.
- [ ] `executor/config.py` — legacy mapping, protected fields, secret handling, precedence.
- [ ] `executor/main.py` dan `executor/engine.py` — bootstrap ordering and admission gate.
- [ ] Exchange adapters — venue-specific 429/418/403/IP-ban classifiers and Retry-After parsing.
- [ ] Entry/exit/order submission mixins — every create-order path fenced; emergency path explicitly documented.
- [ ] Status/alert API — reason, epoch, venue, evidence redaction, deduplication.
- [ ] Python + PostgreSQL integration + multi-instance concurrency tests and CI output.

## 8. Release gate

- [ ] All required tests pass in CI and PostgreSQL integration tests run against a disposable non-production database.
- [ ] Migration reviewed and applied to staging; rollback plan rehearsed.
- [ ] Config shadow comparison has zero unexplained diffs, especially protected risk and venue fields.
- [ ] Multi-instance STOP-vs-submit race test passes repeatedly under injected latency and connection loss.
- [ ] CEX 429/418/403 fixtures validated per target venue using official venue semantics.
- [ ] Paper mode passes restart, recovery, throttling, confirmed IP-ban, STOP, and resume scenarios.
- [ ] No production/live cutover until all above are checked and an operator explicitly approves it.

**Safety note:** This document is a mixed implementation/review checklist. A partial PostgreSQL STOP/fencing implementation, durable in-flight submit ledger, startup exact-client-ID reconciliation, and unit-test files now exist on this branch. The Global STOP migration and earlier schema smoke job passed against CI's disposable PostgreSQL service; the durable-ledger migration passed CI schema validation, and Python suite passed (488 tests) on commit `01b38c07`. No staging/production database or real multi-instance fault-injection test has been exercised. All unchecked items remain OPEN; this branch is not LIVE-certified.


## 9. Implementasi yang sudah ada di branch (2026-11-13)

| Komponen | Implementasi saat ini | Batas yang masih OPEN |
|---|---|---|
| `executor/global_stop.py` | Pool `asyncpg`, STOP epoch, session advisory lock, durable submit-attempt ledger (`PREPARED` → `SUBMITTING` → `ACKNOWLEDGED`/`REJECTED`/`UNKNOWN`), unresolved-attempt gate, exact-ID lookup resolution, Redis notification best-effort | Audit journal, authenticated operator API, heartbeat/lease generation, operator resolution workflow, retry/observability hardening |
| `executor/engine.py` / `engine_runtime.py` | LIVE startup membutuhkan `POSTGRES_DSN`; global STOP dibaca sebelum recovery; pool ditutup saat shutdown | Full bootstrap snapshot/config migration belum ada |
| `executor/engine_entry_submission.py` | Spot entry menulis durable `PREPARED`, commit `SUBMITTING` sebelum HTTP di bawah session advisory lock, menyimpan ACK/rejection, dan mempertahankan unresolved state pada timeout/DB loss | Seluruh create-order path belum diaudit; classifier masih signature-based |
| `supabase/migrations/20261113000100_add_global_stop_fencing.sql` | Singleton canonical control row + epoch | Deployment migration dan live DB integration test belum dijalankan |
| `supabase/migrations/20261113000200_add_durable_submit_attempts.sql` | Durable submit attempt table, unresolved index, status constraints, RLS/grants | Deployment migration dan DB disconnect/fault-injection test belum dijalankan |
| `tests/test_global_stop_fencing.py` | Unit tests untuk durable `SUBMITTING` survival setelah simulated connection loss, second-entry blocking, STOP update, dan classifier 429 vs IP-ban | Python suite passed (488 tests) in CI on commit `01b38c07`; belum ada race test multi-process/PostgreSQL nyata |

**Catatan operasi:** `POSTGRES_DSN` harus diberikan ke setiap instance LIVE melalui secret/environment manager dan migration harus diterapkan terlebih dahulu. Tanpa DSN, LIVE startup sengaja gagal; mode MOCK tetap dapat berjalan tanpa PostgreSQL. Jangan menganggap branch ini LIVE-certified sebelum checklist tersisa dan integration tests lulus.


## 10. Durable in-flight submit acceptance gates

- [x] Create-order intent is committed as \`PREPARED\` before the venue request.
- [x] Under the shared session advisory lock, status changes to \`SUBMITTING\` and commits before the HTTP call.
- [x] A disconnected session releases the PostgreSQL session lock but leaves \`SUBMITTING\` durable.
- [x] A later entry checks for any other \`SUBMITTING\`/\`UNKNOWN\` row and fails closed.
- [x] An acknowledged order is reconciled by exact client order ID at startup; found order becomes \`ACKNOWLEDGED\`.
- [x] A negative/failed lookup remains unresolved; it is not treated as proof the venue rejected the request.
- [ ] Add operator-authenticated/manual resolution requiring independent venue evidence and audit event.
- [ ] Add disposable PostgreSQL multi-process test: disconnect connection while fake venue still accepts order; prove second instance cannot submit.
- [ ] Add tests for crash before HTTP boundary (\`PREPARED\`), after request (\`SUBMITTING\`), after venue ack before ledger ack, and delayed exact-ID visibility.
- [~] Spot-entry and futures entry-hedge submissions now prepare durable attempts and hold the shared session fence across the venue call. Remaining exposure-opening paths and alternate adapters still require a complete call-site audit and integration.
- [ ] Define retention/archival policy for terminal attempt rows without deleting unresolved rows.

## 11. Audit eksekusi call-site order — 2026-11-13

Audit statis pada branch ini menelusuri facade, wrapper venue, serta lifecycle caller berikut. Ini adalah inventaris dari source yang diperiksa, bukan bukti bahwa semua kombinasi runtime venue sudah diuji.

| Jalur | Implementasi/caller | Klasifikasi STOP | Status durable ledger/fence |
|---|---|---|---|
| Spot entry BUY | engine_entry_submission.py → ExchangeOrderMixin.place_local_limit_buy → local.create_limit_buy_order | Membuka eksposur; wajib PREPARED/SUBMITTING + shared session fence | Terintegrasi; LIVE startup mensyaratkan PostgreSQL |
| Futures entry hedge SELL | entry_hedge.py → ExchangeFuturesMixin.open_global_short_market → global_fut.create_market_sell_order | Menambah short exposure; wajib PREPARED/SUBMITTING + shared session fence | Terintegrasi; exact-ID recovery sebelumnya salah memilih lookup spot, diperbaiki pada commit a8eaee3 |
| Spot exit SELL LIMIT | exit_orders.py → ExchangeOrderMixin.place_local_limit_sell → local.create_limit_sell_order | Mengurangi/menutup spot exposure; jangan otomatis diblokir oleh entry STOP | Tidak memakai entry ledger/fence; perlu audit terpisah untuk idempotensi, cancel/fill race, dan exit recovery |
| Spot rollback/emergency SELL MARKET | rollback.py dan force_close_legs.py → place_local_market_sell → local.create_market_sell_order | De-risking/rollback; jangan otomatis diblokir oleh entry STOP | Tidak memakai entry ledger/fence; tetap perlu verifikasi hasil venue dan larangan blind retry |
| Futures close BUY | exit_execution.py / force_close_legs.py → close_global_short_market → global_fut.create_market_buy_order dengan reduceOnly=True | Reduce-only close; bukan pembuka short/long dan tidak boleh diperlakukan sebagai entry | Tidak memakai entry ledger/fence; perlu audit idempotensi/reconciliation khusus close |

### Adapter dan wrapper

- ExchangeClient mengikat spot/futures client berdasarkan venue. for_venue() memilih client sesuai registry market; for_pair() memilih pasangan spot/futures.
- IndodaxTAPIv2 menyediakan create_limit_buy_order, create_limit_sell_order, dan create_market_sell_order yang semuanya melewati _create_order. Jalur pembuka spot BUY melewati wrapper spot entry; SELL digunakan oleh exit/de-risking.
- Adapter modular di executor/venues/{indodax,mexc,okx,tokocrypto}.py tidak menunjukkan direct create-order invocation pada source yang diperiksa; belum ada bukti bahwa modul-modul tersebut terpasang sebagai jalur submit aktif pada facade ini.
- Pencarian juga memeriksa create_order, create_market_order, create_limit_order, market/limit buy/sell, serta wrapper place_local_* dan open/close_global_*. Hasil statis tidak menggantikan runtime instrumentation untuk plugin/CCXT internal.

### Temuan dan perbaikan

1. R-20 — market-aware durable submit recovery. Sebelumnya semua unresolved attempt diarahkan ke fetch_local_order_by_client_id, termasuk market_type=perpetual. Startup dapat gagal mengadopsi futures hedge yang sebenarnya sudah diterima. Recovery sekarang memilih fetch_global_order_by_client_id untuk perpetual/future/futures/swap, dan lookup lokal untuk spot/local; market type tidak dikenal tetap fail-closed. Regression test menguji futures exact-client-ID lookup.
2. R-21 — amount specification adalah bagian dari idempotency intent. Reuse client order ID dengan quantity/price sama tetapi amount_spec berbeda dapat mengubah konversi jumlah order di adapter. Pemeriksaan reuse PREPARED sekarang membandingkan amount_spec JSONB juga; regression test menolak perubahan spesifikasi.
3. Hasil lookup not-found atau exception tetap unresolved dan menahan global entry gate. Tidak ada automatic resubmit; eventual consistency venue tetap dianggap mungkin.
4. ACK persistence failure / DB disconnect setelah request tidak boleh diubah menjadi REJECTED berdasarkan dugaan. Row SUBMITTING tetap durable dan harus direconcile; fake connection test yang ada menguji ledger survival, tetapi belum menguji proses PostgreSQL multi-process nyata saat fake venue masih menerima request.

Sisa OPEN: full runtime call-site/call-graph audit lintas seluruh adapter/konfigurasi, authenticated operator manual resolution, durable fencing untuk seluruh order-open path yang mungkin ditemukan berikutnya, real PostgreSQL multi-process disconnect/fault injection, serta uji venue contract dan eventual-consistency tertunda. Exit/close/cancel/de-risking sengaja tidak dimasukkan ke entry STOP fence tanpa policy khusus yang membuktikan aman untuk risk reduction.


### 11.1 Futures hedge IP-ban escalation

Audit lanjutan menemukan bahwa futures entry-hedge sudah memakai durable fence, tetapi explicit IP-ban response dari venue futures belum menulis canonical CEX_IP_BLOCK STOP. Commit f6da668 menambahkan request_stop_in_transaction di dalam session fence yang sama, local global-admission halt, dan Redis notification best-effort setelah transaksi. Jika persistensi STOP gagal, attempt dipertahankan UNKNOWN (atau SUBMITTING bila koneksi DB sudah putus), bukan ditandai REJECTED, agar instance peer tetap terblokir. Generic 429 tetap tidak dianggap IP ban.
