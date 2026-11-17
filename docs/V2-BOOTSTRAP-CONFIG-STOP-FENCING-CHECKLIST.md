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
  - Sebagian: jalur de-risking (`exit_orders`, `exit_execution`, `force_close_legs`, `rollback`) sekarang melewati advisory lock serialisasi yang sama via `derisk_submit_fence`. Entry tetap fail-closed. Concurrent multi-instance integration test belum ada.
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

**Safety note:** PostgreSQL STOP/fencing, durable submit ledger, startup exact-client-ID recovery, signed operator resolution, and a disposable PostgreSQL multi-process backend-disconnect test are now implemented on this branch. The latest workflow must pass before the integration test is marked verified. No staging/production database or real per-venue sandbox/paper contract probes have been exercised. Remaining unchecked items are release gates; this branch is not LIVE-certified.


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

### 11.2 Spot entry IP-ban persistence failure

Audit lanjutan menemukan celah serupa di spot entry: saat venue mengembalikan signature IP restriction yang eksplisit tetapi penulisan canonical Global STOP gagal, jalur fallback exact-client-ID lookup masih dapat berjalan untuk signature yang tidak masuk classifier definitive-rejection. Jika lookup menemukan order, ACK dapat dipersist dan unresolved guard terangkat tanpa Global STOP. Commit 9a0a269 menambahkan guard eksplisit: attempt tetap UNKNOWN/SUBMITTING, lookup tidak dijalankan, dan order ditandai ambiguous secara lokal. Regression test menggunakan fake venue yang sengaja mengembalikan order dari lookup untuk membuktikan lookup tidak dipanggil setelah STOP write failure.

### 11.3 Retry/recovery audit untuk de-risking

Ditemukan bahwa generic retry_with_backoff mengulang setiap exception. Dua pemanggil order yang diperiksa adalah spot rollback setelah hedge gagal dan spot market liquidation saat force-close. Walaupun keduanya mengurangi eksposur dan sengaja tidak masuk entry STOP fence, retry setelah timeout/connection reset/HTTP 503 tetap dapat menggandakan market SELL jika venue menerima request pertama tetapi ACK hilang. Perubahan commit 8c146f4, 8e2c67d, dan be54874 menambahkan retry predicate: kedua jalur hanya retry explicit HTTP 429 rejection; timeout, connection reset, HTTP 503, dan exception ambigu berhenti setelah satu percobaan. Client order ID tetap stabil untuk retry 429 yang eksplisit ditolak. Regression test memverifikasi 429 retry dan 503/timeout tidak diulang.

### 11.4 Spot exit limit SELL ambiguous acknowledgement

Exit limit SELL is intentionally not subject to the entry Global STOP fence. However, the previous exception handler only logged an ambiguous submit and left the position eligible for another repricing cycle with a new client ID. That could create duplicate sell orders if the first order was accepted but its ACK was lost. Commit a698308 now performs exact-client-ID lookup; if a verifiable order is found, normal exit registration continues. If not found or lookup fails, the pair is marked RECONCILIATION_REQUIRED and is not automatically repriced/submitted again. Regression test verifies lookup-not-found prevents a second exit attempt. This is a pair-level reconciliation gate, not a block on emergency risk-reducing actions.

### 11.5 Sisa audit khusus futures reduceOnly force-close

- Exit lifecycle futures BUY reduceOnly setelah spot exit fill masuk RECONCILIATION_REQUIRED jika hasil close tidak terminal/terverifikasi; tidak ada retry otomatis.
- Force-close futures BUY reduceOnly sekarang memakai client ID deterministik berbasis `opened_at_ms` dan memeriksa durable order registry sebelum submit ulang. Intent yang sudah ada hanya boleh diadopsi lewat exact-client-ID lookup; lookup miss/error, order pending, atau fill parsial memblokir submit baru dan memerlukan reconciliation.
- Karena reduceOnly=True dan fungsi utamanya mengurangi short, jalur ini tidak dimasukkan ke entry STOP fence. Tetap OPEN untuk audit idempotensi/venue pending-order semantics dan penyimpanan client ID close yang bisa direkonsiliasi. Jangan mengubahnya menjadi entry fence tanpa analisis risiko yang eksplisit.
- Cancel/replace spot exit sudah menunggu order lama mencapai status terminal dan menerapkan fill sebelum membuat replacement; jika cancel/status lookup inconclusive, replacement diblokir.


## 12. Latest hardening status — 2026-11-13

- [x] HMAC-signed operator resolution implemented with bounded timestamp, unique nonce, exact attempt-state precondition, two distinct evidence references, atomic row transition and append-only audit.
- [x] CLI entry point added: `python -m executor.operator_resolution --envelope <signed-envelope.json>`. Requires `POSTGRES_DSN` and `OPERATOR_RESOLUTION_HMAC_SECRET` supplied from a secret manager.
- [x] Real PostgreSQL multi-process test added: executor killed during a fake venue request; peer must remain blocked by durable SUBMITTING.
- [x] Fake delayed-visibility and pending reduce-only fixtures added for Indodax, Tokocrypto, MEXC and OKX.
- [ ] Wait for CI run after the dependency/Python-version correction to pass the new PostgreSQL integration job.
- [ ] Real per-venue sandbox/paper contract probes remain required; fake fixtures do not prove venue-side eventual consistency.
- [x] R-26 duplicate force-close reduceOnly submit is blocked using deterministic client-ID reuse and exact-ID recovery. Pending/partial/not-visible states remain reconciliation-required; per-venue pending-close probes are still open.
- [ ] HMAC is a shared-secret operator authentication mechanism, not SSO/OIDC. Secret rotation/custody and operator provisioning must be completed by deployment owners.


### R-26 closeout — futures reduceOnly force-close

- [x] Stable force-close client ID derived from persisted position `opened_at_ms`.
- [x] Require durable order registry in LIVE before futures force-close.
- [x] Existing intent triggers exact client-ID lookup; missing/error/pending result never triggers a new client ID.
- [x] Adopt only a terminal full-fill order and verify futures position flat before continuing.
- [x] Regression test confirms delayed lookup cannot cause duplicate reduceOnly close.
- [ ] Venue-specific pending-close timing/status probes remain part of the per-venue sandbox/paper release gate.


## 30. Full create-order call-site audit — 2026-11-14

Audited branch head before this patch: \`97819da0d068ca67f8c51f74cb2f9129f150a270\`.

### Exposure-increasing mutations

- [x] Spot entry chain identified: \`_submit_entry_order\` → \`place_local_limit_buy\` → CCXT \`create_limit_buy_order\` (Indodax routes through \`IndodaxTAPIv2._create_order\`). LIVE path prepares the durable intent and holds the PostgreSQL session advisory fence across the venue request.
- [x] Futures hedge chain identified: \`_submit_entry_hedge_order\` → \`open_global_short_market\` → CCXT \`create_market_sell_order\`, with \`reduceOnly=False\`. LIVE path prepares the durable intent and holds the same fence across the venue request.
- [x] Generic \`MutationGateway.execute_create_order\` inspected. It had an unfenced optional-service branch, no ACK persistence, and unsafe generic-error rejection. There was no active runtime create caller in this branch; \`CommandProcessor\` uses only the cancel method. The create method now fails closed without initialized PostgreSQL fencing and persists PREPARED/SUBMITTING/ACKNOWLEDGED/REJECTED/UNKNOWN outcomes.
- [x] LIVE spot/futures mixin boundaries now independently reject missing GlobalStop; the unfenced compatibility path remains mock-only.
- [x] Venue-specific modules inspected: no direct create calls in modular \`executor/venues/*\`; Indodax TAPIv2 funnels create methods through \`_create_order\`.

### Risk reduction — intentionally not fenced as entry

- Spot limit SELL exit, spot market SELL rollback/force-close, futures market BUY \`reduceOnly=True\` close, and cancel remain outside the entry STOP fence.
- These actions are not changed by this audit. A global STOP must not disable them generically; their separate retry, stable client-ID, terminal-state and reconciliation guards remain mandatory.

### Verification contract

- [x] Regression tests added for unfenced gateway denial, direct LIVE spot/futures missing-GlobalStop denial, connection-reset ambiguity, explicit rejection, missing order ID and ACK-persistence failure.
- [x] Focused Python regressions and full relevant CI passed: source/test workflow [43691977186](https://github.com/nzf210/nafaz_arb/actions/runs/43691977186); final documentation head workflow [43692065378](https://github.com/nzf210/nafaz_arb/actions/runs/43692065378).
- [ ] Review operator-authenticated resolution end-to-end (HMAC key custody/rotation, operator identity, dual evidence, approval, audit retrieval and runbook).
- [ ] Close real PostgreSQL multi-process disconnect/fault-injection review on the latest CI revision.
- [ ] Run real per-venue sandbox/paper probes for client-ID exact lookup, delayed visibility, pending/partial hedge market orders, and reduceOnly close semantics.

**Status remains PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.


## 31. Audit continuation — 2026-11-14

**Source baseline reviewed:** branch head `5956c23071b61ff075393cecfa5ed3de75a07420`.  
**Audit patch commits:** `01f412a09edcc5ec23781d1b0eedbfa4b061a942` and `d8eb70d647f452744c894a3f92c5b8fb3f1dde6c`.  
The recovery-context regression added by these commits is awaiting its own CI run; do not attribute the previous green workflow to this new test.

### 31.1 Call-site inventory and exposure classification

| Path / call site | Concrete call chain | Classification | Durable PREPARED/SUBMITTING + shared fence |
|---|---|---|---|
| Spot entry | `EngineEntrySubmissionMixin._submit_entry_order` → `ExchangeOrderMixin.place_local_limit_buy` → CCXT `create_limit_buy_order`; Indodax uses `IndodaxTAPIv2.create_limit_buy_order` → `_create_order` → signed `POST /api/v2/order` | Opens spot exposure | REQUIRED; LIVE path prepares and fences before HTTP |
| Futures hedge | `EntryHedgeMixin._submit_entry_hedge_order` → `ExchangeFuturesMixin.open_global_short_market` → CCXT `create_market_sell_order`, `reduceOnly=False`, short position-side parameters | Opens/adds short exposure; latest sizing may intentionally scale above the spot fill to meet venue minimums | REQUIRED; LIVE path prepares and fences before HTTP |
| Generic create gateway | `MutationGateway.execute_create_order` → `create_order_direct` or `create_order` | Potentially exposure-increasing | REQUIRED; service must be initialized, PREPARED is committed, fence changes to SUBMITTING, ACK/rejection/unknown is persisted. No production create caller found in the reviewed call graph |
| Spot exit | `ExitOrdersMixin` → `place_local_limit_sell` → CCXT `create_limit_sell_order` | Reduces spot inventory | Deliberately outside entry STOP fence; separate exact-ID and cancel/replace reconciliation |
| Spot rollback / force-close | `RollbackMixin` / `ForceCloseLegMixin` → `place_local_market_sell` → CCXT `create_market_sell_order` | Reduces spot inventory | Deliberately outside entry STOP fence; retries only explicit HTTP 429; ambiguous transport/503 is not retried |
| Futures exit / force-close | `ExitExecutionMixin` / `ForceCloseLegMixin` → `close_global_short_market` → CCXT `create_market_buy_order`, `reduceOnly=True` | Reduces short exposure | Deliberately outside entry STOP fence; deterministic force-close ID and exact-ID recovery; unresolved/pending/partial result blocks duplicate close |
| Cancel | `CommandProcessor` → `MutationGateway.execute_cancel_order` | Cancels an existing order; not a create | Not subject to entry admission |

Additional findings:
- The reviewed `executor/venues/{indodax,mexc,okx,tokocrypto}.py` modules do not call create-order APIs directly; they provide venue formatting/lookup support. Runtime venue/account-mode certification is still open.
- `IndodaxTAPIv2._create_order` is the custom raw HTTP create boundary. It makes one request and does not wrap POST in generic retry.
- `MutationGateway.execute_create_order` has no production caller in the inspected branch. The Telegram command processor calls only `execute_cancel_order`.
- No other live exposure-increasing create call site was found in the inspected executor lifecycle. This is source-level inventory, not live venue certification.

### 31.2 Durable intent and GlobalStop API review

- Actual API signatures are `prepare_submit_attempt(...)`, `order_submit_fence(client_order_id=...)`, `request_stop(reason, source=..., metadata=...)`, and `request_stop_in_transaction(conn, reason, source=..., metadata=...)`. Spot, futures hedge and generic gateway call sites use these signatures correctly.
- The shared key is a PostgreSQL **session-level** advisory lock for the full venue request. STOP uses a conflicting transaction-level advisory lock. The committed `SUBMITTING` row survives loss of the DB session and blocks a peer even after the session lock is released.
- Intent equality already included exchange, market type, symbol, side, order type, quantity, price, pair key and amount specification. Audit found it omitted `recovery_context`; a PREPARED client ID could therefore be reused with a different parent spot-order identity. Commit `01f412a` now compares JSONB `recovery_context` as well. Commit `d8eb70d` adds a regression test for same-context reuse and changed-parent rejection.
- Non-definitive transport failures, timeouts, connection resets, malformed ACKs and ACK-persistence failures remain UNKNOWN/SUBMITTING; they are not blind-retried. Explicit venue rejection may be marked REJECTED. IP-ban STOP persistence failure retains the unresolved attempt.
- Startup uses exact client-order-ID lookup routed by market type (spot/local vs perpetual/futures). Found orders become ACKNOWLEDGED. Not-found or lookup failure is recorded as evidence only and remains globally unresolved; eventual consistency is not treated as rejection.

### 31.3 Verified test evidence after the source patch

- A first recovery-context regression run passed, but a subsequent SQL review found the production SELECT projection still omitted `recovery_context`; the original fake connection returned the full row and masked that omission. That earlier pass is not treated as proof of the database read path.
- Commit `be30fc2` adds `recovery_context` to the existing-attempt SELECT projection. Commit `df41a7c` makes the fake connection return only selected columns, so the regression fails if the production query omits the field.
- Workflow [43572374744](https://github.com/nzf210/nafaz_arb/actions/runs/43572374744), on source/test head `df41a7c`, completed successfully: all four jobs (Go, PostgreSQL schema/integration, Docker image builds, Python) passed.
- The full Python suite reported **559 passed, 2 skipped**. The dedicated PostgreSQL integration file reported **2 passed**, including the multi-process backend-disconnect/fake-venue boundary and signed operator-resolution/audit test.
- Go tests, Go race tests, Go vet, both Docker builds, migration ordering/idempotency, RLS/grants/triggers and VPS migration runner shell syntax all passed.
- The two skips are in the general Python job; the separate PostgreSQL integration job explicitly ran both integration tests against disposable PostgreSQL. Pytest reports two unknown `integration` marker warnings; these are non-failing configuration warnings.

### 31.4 Remaining release gates

- [x] Recovery-context intent equality regression, including the actual SQL SELECT projection and a projection-aware fake, passed in workflow 43572374744.
- [x] HMAC operator-resolution signature, nonce/evidence checks and audit persistence passed the disposable PostgreSQL integration test. Identity provisioning, secret custody/rotation, dual-control approval, audit retrieval and runbook remain operationally OPEN; this is shared-secret HMAC, not SSO/OIDC.
- [x] PostgreSQL multi-process backend-disconnect/fake-venue boundary passed on the source patch and final documentation head. Repeated stress/fault-boundary review and production topology validation remain OPEN; CI does not validate a real CEX.
- [ ] Run per-venue sandbox/paper probes for exact client-ID lookup visibility, delayed history consistency, pending/partial market hedge behavior, cancel/fill races and reduceOnly close semantics.
- [ ] Review the latest `scale_to_min` partial-fill policy: it can deliberately open a futures short larger than the filled spot quantity to satisfy minimum notional/contract rules. Current code logs delta imbalance; the hard-drift branch logs but does not itself close the excess. This is a strategy/risk-policy approval gate, not a STOP-fence bypass, and was not changed in this audit.
- [ ] Verify every LIVE executor instance shares the same PostgreSQL authority/DSN and the required migrations before any deployment.

**Status: PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.


## V2 audit continuation — 2026-11-15 (source baseline `ca8f2e7095b7`)

### Complete mutation inventory from executor wrappers

The production Python order-mutation boundaries found in this pass are:

| Boundary | Raw venue call | Exposure classification | Durable entry ledger + shared session fence |
|---|---|---|---|
| `ExchangeOrdersMixin.place_local_limit_buy` | `create_limit_buy_order` | Spot BUY; opens exposure | REQUIRED; active LIVE caller `_submit_entry_order` prepares and fences |
| `ExchangeOrdersMixin.place_local_limit_sell` | `create_limit_sell_order` | Spot SELL; reduces inventory | Not entry-fenced; exact-ID ambiguity recovery and cancel/replace controls apply |
| `ExchangeOrdersMixin.place_local_market_sell` | `create_market_sell_order` | Spot SELL; rollback/force-close/sniper de-risking | Not entry-fenced; retry/ambiguity controls apply |
| `ExchangeFuturesMixin.open_global_short_market` | `create_market_sell_order`, `reduceOnly=False` | Futures SELL; opens/adds short | REQUIRED; active LIVE caller `_submit_entry_hedge_order` prepares and fences |
| `ExchangeFuturesMixin.close_global_short_market` | `create_market_buy_order`, `reduceOnly=True` | Futures BUY; closes short | Not entry-fenced; close lifecycle has independent exact-ID/reconciliation controls |
| `IndodaxTAPIv2._create_order` | signed `POST /api/v2/order` | Adapter raw boundary used by the above spot BUY/SELL wrappers | Inherits caller classification; no generic POST retry |
| `MutationGateway.execute_create_order` | `create_order_direct` or CCXT `create_order` | Generic, potentially exposure-increasing | REQUIRED; method itself fails closed and persists PREPARED/SUBMITTING/ACK/rejection/unknown. No active production create caller found; engine imports no create call and command processor uses cancel only |

The modular `executor/venues/{indodax,mexc,okx,tokocrypto}.py` modules contain client-ID formatting/venue parameter helpers, not raw create-order calls. Paper exchange methods are simulation-only and do not call a CEX. No additional direct create-order boundary was found in the reviewed executor orchestration, recovery, rollback, force-close, or Go market-data adapter files.

### R-34 — Taker-sniper spot SELL ambiguous ACK could fall through to another SELL

**Status: FIX IMPLEMENTED; first regression CI passed, fill-validation follow-up CI pending at time of this entry.**

- The taker-sniper path returned `False` for every market SELL exception. Its caller could then continue into smart-maker exit and submit a second SELL with a new client ID even when the first request might have reached the venue.
- It now classifies explicit venue rejection separately. For ambiguous timeout/transport outcomes it performs exact client-ID lookup; a match is adopted, while not-found/lookup failure marks the pair RECONCILIATION_REQUIRED and returns a handled result so no fallback SELL is sent.
- A venue ACK without an exchange order ID also becomes reconciliation-required.
- Regression tests cover lost ACK with exact-ID match and not-found. Workflow [43691790303](https://github.com/nzf210/nafaz_arb/actions/runs/43691790303) passed all four jobs on test head `17c4f67446d0`.

### R-35 — Taker-sniper futures close used requested amount instead of confirmed spot fill

**Status: FIX IMPLEMENTED; latest CI pending.**

- The prior code used `filled or amount or position_qty`. An acknowledged market SELL with `filled=0` could therefore close the entire futures short even though no spot quantity had sold; a partial fill could also over-close the short.
- The path now requires a present, finite, non-negative cumulative `filled` value no larger than the position quantity (within tolerance). Missing or invalid fill evidence becomes reconciliation-required.
- A zero-fill open order is retained as the same pending exit order; no futures close and no fallback SELL occurs. Explicit terminal no-fill rejection/cancel/expiry can be retired and the caller may continue with a replacement exit.
- Futures `reduceOnly=True` close is sent only for the confirmed spot-filled quantity. New fake-venue regressions cover zero-fill and half-fill behavior.
- These changes do not put exits behind Global STOP. They prevent a risk-reducing path from creating a naked spot-long delta by closing more futures than spot actually sold.

### Other exposure and STOP findings

- The two active LIVE exposure-opening call chains remain spot BUY and futures short SELL. Both commit PREPARED before the venue call and hold the shared PostgreSQL session advisory fence while the venue request is active.
- Generic gateway create is fail-closed but no active production create caller was found.
- GlobalStop signatures and callers were rechecked: `prepare_submit_attempt(...)`, `order_submit_fence(client_order_id=...)`, `request_stop(reason, source=..., metadata=...)`, and `request_stop_in_transaction(conn, reason, source=..., metadata=...)`. The entry fence uses the session-level advisory lock; STOP takes the conflicting transaction-level lock.
- Intent equality includes exchange, market type, symbol, side, order type, quantity, price, pair key, amount specification and recovery context. PREPARED reuse is only allowed for identical intent; SUBMITTING/UNKNOWN and terminal states cannot be reused.
- Startup recovery routes exact client-ID lookup by spot/perpetual market type. Found orders are ACKNOWLEDGED. NOT_FOUND and lookup failure remain unresolved; eventual consistency is not treated as definitive rejection. ACK persistence failure leaves the durable submit unresolved and does not authorize a new ID or submit.
- The entry `scale_to_min` policy can intentionally make the futures short larger than the actual spot fill to satisfy futures minimum notional/contract rules. This is fenced but remains a material strategy exposure decision; delta imbalance is recorded, and hard-drift does not yet automatically unwind the excess. Keep this as an explicit strategy approval gate.
- Risk-reducing spot SELL, futures `reduceOnly=True` BUY close, and cancel remain outside entry STOP by design. Their own exact-ID, retry, fill-quantity and reconciliation protections are required; do not apply a generic STOP to these operations.

### Verification and release state

- Source baseline CI before the new exit fixes: workflow [43677081780](https://github.com/nzf210/nafaz_arb/actions/runs/43677081780) passed all four jobs (Python, Go/race/vet, PostgreSQL schema/integration, Docker builds).
- R-34 exact-ID recovery regression head: workflow [43691790303](https://github.com/nzf210/nafaz_arb/actions/runs/43691790303) passed all four jobs.
- Source/test head `ee1c2db2707c` (including R-34/R-35 and all four new fake-venue regression tests) completed workflow [43691977186](https://github.com/nzf210/nafaz_arb/actions/runs/43691977186) with all four jobs successful: Python executor tests, Go tests/race/vet, PostgreSQL schema/integration, and Docker builds. The PostgreSQL job's multi-process backend-disconnect/fake-venue step also passed. The final documentation head `19c8b3aeebdc` completed workflow [43692065378](https://github.com/nzf210/nafaz_arb/actions/runs/43692065378) with all four jobs successful. CI uses disposable PostgreSQL and fake venue behavior; it does not validate production VPS topology or real CEX contracts.
- Operator HMAC resolution remains a shared-secret mechanism, not enterprise identity/SSO. Provisioning, key custody/rotation, dual approval, audit retrieval and operational runbook remain open.
- PostgreSQL multi-process disconnect/fake-venue integration has passed in prior CI, but repeated stress/fault-boundary review and production VPS topology validation remain open. It is not a real CEX contract probe.
- Real per-venue sandbox/paper probes remain open for client-ID visibility, eventual consistency, pending/partial hedge semantics, cancel/fill races and reduceOnly close behavior.

**Overall status: PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.


## R-36 — OKX client-order-ID wire format was not applied to submit/recovery

**Status: FIX IMPLEMENTED; source/test CI passed on 2026-11-15.**

- The logical ID builder can produce up to 36 characters and includes dashes. OKX v5 requires `clOrdId` to be alphanumeric and no more than 32 characters ([official OKX order API](https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order)). Current CCXT OKX maps the unified `clientOrderId` parameter to `clOrdId`.
- `OKXVenue.format_client_order_id` existed but `venue_client_order_id` did not call it. Futures open/close passed the logical ID directly, and exact-ID recovery did not derive an OKX wire ID before an ACK alias had been learned.
- OKX now preserves already-valid short alphanumeric IDs; otherwise it maps the full logical ID to a deterministic 32-character SHA-256 hex prefix. It does not truncate the suffix, so distinct logical IDs do not collapse merely because their leading characters match.
- The same mapping is used by spot wrappers, futures short open, futures reduceOnly close, and generic MutationGateway create. Recovery's accepted-ID set includes the deterministic wire ID before ACK, and direct recovery lookup tries both logical and wire IDs. The OKX permission probe's raw order POST was separately removed; see R-37.
- Regression coverage verifies 32-character alphanumeric output, determinism, distinct logical IDs, accepted recovery aliases, and the actual futures short create call's wire parameter.
- Source/test CI for the OKX mapping and gateway commits passed all four jobs: workflows [43692450259](https://github.com/nzf210/nafaz_arb/actions/runs/43692450259), [43692452829](https://github.com/nzf210/nafaz_arb/actions/runs/43692452829), [43692456060](https://github.com/nzf210/nafaz_arb/actions/runs/43692456060), [43692515692](https://github.com/nzf210/nafaz_arb/actions/runs/43692515692), [43692520066](https://github.com/nzf210/nafaz_arb/actions/runs/43692520066), and [43692523689](https://github.com/nzf210/nafaz_arb/actions/runs/43692523689). These include Python executor tests, PostgreSQL schema/integration, Go/race/vet, and Docker builds.
- This is a source/CCXT contract fix; a real OKX sandbox/live contract probe is still required before relying on the mapping operationally. Other venue wire formats remain subject to per-venue probes.

**Overall status remains PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.


## R-37 — Unfenced OKX create-endpoint permission probe removed

**Status: FIX IMPLEMENTED AND VERIFIED IN CI (2026-11-15).**

- A final scan found one additional direct call to OKX `privatePostTradeOrder` inside `ExchangeFuturesMixin.probe_short_tradability`. It submitted an intentionally invalid zero-size/zero-price payload to infer trade permission.
- No production caller of this method was found, but the call still crossed a private create-order endpoint without the durable submit ledger or shared STOP fence. Relying on current venue validation to guarantee that a zero-size payload can never mutate state is not an acceptable safety boundary.
- The direct POST has been removed. For OKX, the method now returns a fail-closed “non-mutating probe unavailable” result and sends no order. A real permission probe must be a separately approved sandbox procedure or use a venue-supported read-only permission endpoint.
- A fake-venue regression asserts the method returns non-tradable and never invokes `privatePostTradeOrder`.
- This does not change any active spot/futures entry or risk-reducing close path; the method had no production caller in the reviewed branch.

Source head [b9716184e23c](https://github.com/nzf210/nafaz_arb/commit/b9716184e23ce1152d6e4ece1a1220f16748a10c) and regression-test head [5dcad7dc8cde](https://github.com/nzf210/nafaz_arb/commit/5dcad7dc8cde710c248bf7f7b8eb19ad738211f5) both passed all four CI jobs: [43692777060](https://github.com/nzf210/nafaz_arb/actions/runs/43692777060) and [43692785951](https://github.com/nzf210/nafaz_arb/actions/runs/43692785951). Python executor tests, PostgreSQL schema/integration, Go/race/vet and Docker builds all succeeded. Previous OKX wire-ID mapping and direct-recovery tests also passed workflows [43692697374](https://github.com/nzf210/nafaz_arb/actions/runs/43692697374) and [43692706776](https://github.com/nzf210/nafaz_arb/actions/runs/43692706776).

**Overall status remains PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.


## 10. Exposure-path audit execution — 2026-11-16

**Audited baseline:** `ea44caf051de4a435c5e40a8e36e288438c86063`  
**Source/test remediation head:** `838aac5baefd0f3f96f1763ec59293445078b32e`

### 10.1 Call-site coverage

- [x] Enumerated all 100 Python executor source files for direct and indirect create-order boundaries.
- [x] Enumerated all 69 Go source files; no CEX private create-order path was found in the scanner/exchange code.
- [x] Traced spot BUY through `_submit_entry_order` → `place_local_limit_buy` → `create_limit_buy_order`.
- [x] Traced futures short SELL through `_submit_entry_hedge_order` → `open_global_short_market` → `create_market_sell_order`.
- [x] Traced generic `MutationGateway.execute_create_order` dynamic `create_order_direct/create_order`; no active production caller was found.
- [x] Traced Indodax TAPIv2 create wrappers to `_create_order` → `POST /api/v2/order`.
- [x] Classified spot limit/market SELL and futures reduceOnly BUY as de-risking; cancellation is not a create-order path.

### 10.2 Findings fixed and regression evidence

- [x] Futures `hedge_submit_started` now flips only immediately before the venue create call. PREPARED/fence failure remains pre-submit and does not trigger false exact-ID ambiguity.
- [x] Rollback no longer retries timeout, connection reset, generic internal error, system busy, or service-unavailable responses. Only explicit venue settlement rejection and explicit HTTP 429 qualify for retry.
- [x] PostgreSQL fence no longer demotes a healthy pool solely because a venue request raised TimeoutError/ConnectionResetError inside the yielded submit scope. Failed session unlock still demotes the broken pool while durable SUBMITTING remains.
- [x] De-risking proceeds under active Global STOP and treats unresolved entry as advisory. If PostgreSQL connectivity is unavailable before the de-risk POST, it can proceed without shared serialization with a CRITICAL log; LIVE entry remains fail-closed.
- [x] Fake-venue/fault-boundary regressions cover rollback timeout single-submit, futures PREPARED failure, STOP-active de-risk, PostgreSQL-unavailable de-risk, venue timeout versus DB disconnect, and durable SUBMITTING preservation.

Source/test CI: [GitHub Actions run 43825962830](https://github.com/nzf210/nafaz_arb/actions/runs/43825962830) completed successfully for `838aac5`. All four jobs passed. Python, Go/race/vet, disposable PostgreSQL schema/idempotency/RLS and multi-process disconnect integration, and Docker image jobs passed.

### 10.3 Still-open release gates

- [ ] Run venue-specific sandbox/paper contract tests for client-ID idempotency, exact-ID visibility, eventual consistency, partial/pending hedge, cancel/fill race, and reduceOnly semantics on every configured CEX.
- [ ] Complete operator identity provisioning, HMAC secret custody/rotation, dual approval, audit retrieval, and signed-resolution runbook. The current HMAC/two-evidence code tests do not close operational governance.
- [ ] Repeat PostgreSQL disconnect/fault-injection under stress and validate the actual VPS topology. Disposable CI PostgreSQL success is not VPS production sign-off.
- [ ] Approve and implement a policy for `scale_to_min` futures over-hedge/delta imbalance, including hard-drift remediation.
- [ ] Resolve schema/permission/control-row integrity failures before allowing de-risking through the fence; only recognized connectivity loss has the explicit unfenced emergency fallback.

**Release state: PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.

- [x] Restrict `ABORTED` transition to PREPARED only; SUBMITTING/UNKNOWN cannot be retired by a pre-submit abort path. Regression verifies the database rejects UPDATE 0 for an unresolved attempt.

- [x] LIVE force-close spot uses a position-bound deterministic client ID, durable-required intent, and exact-ID adoption; a prior NOT_FOUND/partial/non-terminal order cannot be resubmitted with a new ID.
- [x] Required LIVE order intents require the actual Supabase/outbox registry writer; local `postgres_dsn` alone is not treated as durable order-registry storage.
- [x] Force-close cancellation requires final terminal venue status and reconciles racing entry/exit fills before force-close legs continue.
- [ ] Decide/validate the operational policy for optional best-effort de-risk intents when Supabase is disabled and Redis is the only remaining registry store; do not claim restart-safe recovery in that configuration.

## Audit execution 2026-11-17 — R-45 terminal entry recovery

**Audited baseline:** `b699ab279524a1fdecaa6325ef771e2d51c4195a`  
**Source/test remediation:** `c2dfdd1`, `685bc8f`, `dc3971c`, `214bd3b`; regression test `3d32b4a`  
**Documentation commits:** `5477201`, `bb10958`, this checklist update  
**R-45 CI:** GitHub Actions [44044178958](https://github.com/nzf210/nafaz_arb/actions/runs/44044178958) SUCCESS for `a3b4e937`; Python 711 passed / 4 skipped, disposable PostgreSQL submit-fencing integration 4 passed, and migration/schema, Go/race/vet, Docker jobs passed.

### Revalidated order boundary inventory

- [x] Spot entry BUY: `_submit_entry_order` → `place_local_limit_buy` → CCXT `create_limit_buy_order`; LIVE requires durable PREPARED/SUBMITTING and shared PostgreSQL session advisory fence.
- [x] Futures hedge SELL: `_submit_entry_hedge_order` → `open_global_short_market` → CCXT `create_market_sell_order` with `reduceOnly=False`; LIVE requires the same durable ledger/fence plus the parent-spot hedge-generation claim.
- [x] Generic mutation gateway: dynamic `create_order_direct` / `create_order`; create is blocked without Global STOP and fenced when called; no active production caller found in inspected engine orchestration.
- [x] Indodax TAPIv2: `create_limit_buy_order`, `create_limit_sell_order`, `create_market_sell_order` → `_create_order` → `POST /api/v2/order`; classification follows the caller lifecycle.
- [x] Spot limit SELL / market SELL, futures reduceOnly BUY, rollback and force-close are de-risk operations; they use the separate de-risk fence and are not rejected merely because Global STOP is active.
- [x] Cancel is not a create-order path. After cancel, the executor polls exact order state up to three times; non-terminal/not-found/unavailable results retain pending recovery and do not authorize a replacement order.
- [x] Modular venue adapters and Go scanner/exchange code were rechecked; no additional private CEX create-order call was found in the inspected current tree.

### Finding and remediation

- [x] R-45: terminal ENTRY recovery could retire an older spot order before its lifecycle fill was reconciled when a newer pending order for the same pair existed.
- [x] ENTRY records are retired only after lifecycle finalization succeeds.
- [x] A mismatched older terminal entry, missing pair context, or failed fill reconciliation is explicitly retained as `open` + `UNVERIFIED` in the recovery index via `retain_for_reconciliation`; the reason is recorded and the pair is halted in `RECONCILIATION_REQUIRED`.
- [x] The newer pending entry context is not overwritten and no hedge is submitted from the mismatched old-order context.
- [x] Fake-venue regression covers older order `501` with a partial terminal fill while newer pending order `902` exists.

### Test and CI evidence

- Earlier head `b699ab2`: GitHub Actions [43986593318](https://github.com/nzf210/nafaz_arb/actions/runs/43986593318) SUCCESS; Python 710 passed / 4 skipped; disposable PostgreSQL submit-fencing integration 4 passed; migration runner, schema/idempotency/RLS, Go/race/vet and Docker jobs passed.
- R-45 targeted regression, full Python suite, disposable PostgreSQL integration, and all five CI jobs were subsequently verified by run `44044178958`.
- No VPS PostgreSQL or real CEX sandbox contract test was performed.

### Release gates still open

- [ ] Per-venue sandbox/paper contract verification: client-ID idempotency/visibility, eventual consistency, pending/partial hedge, cancel/fill race, reduceOnly semantics.
- [ ] Operator-authenticated manual resolution governance: operator identity, HMAC key custody/rotation, dual approval, audit retrieval, runbook.
- [ ] Repeated real PostgreSQL multi-process disconnect/fault-injection and validation against the actual VPS topology; CI disposable PostgreSQL is not production-topology sign-off.
- [ ] Approve the minimum-notional overhedge/delta-drift remediation policy.
- [ ] Review the residual best-effort/Redis-only de-risk intent mode when Supabase order-registry persistence is disabled.

**Overall release state: PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.
## Audit execution 2026-11-17 — R-46 recovery ACK requires order registry

**Audited baseline:** `a3b4e9374288237b31a5905d6bdbd58afd615aea`  
**Source remediation:** `c4b1012f298c77e99b215e0fe8c0ed8f2c634cd6`  
**Regression test:** `9377aa7c310a501ba56c2e915cc28affd0d7a805`  
**Positive-fixture follow-up:** `77906cf1f650dc084ec01ab4a3ea65b6909994f8`  
**R-46 CI:** GitHub Actions [44046589327](https://github.com/nzf210/nafaz_arb/actions/runs/44046589327) SUCCESS for source/test head `77906cf1`.

### Finding and change

- [x] R-46: exact-client-ID startup recovery previously treated a missing `order_registry` as a successful no-op, then marked the Global STOP submit attempt ACKNOWLEDGED.
- [x] Missing registry now raises before ACK. The recovery handler leaves the durable attempt unresolved and retains the global entry fence.
- [x] Positive exact-ID recovery test now supplies a fake registry and verifies lifecycle record persistence.
- [x] New fake-venue regression verifies that a missing registry does not ACK the submit attempt and the entry gate remains halted.

### Revalidated boundary inventory

- [x] Spot BUY and futures short SELL remain the only active executor entry create paths found; both require durable PREPARED/SUBMITTING and shared PostgreSQL session advisory fence in LIVE mode.
- [x] Indodax raw `POST /api/v2/order` remains a wrapper boundary whose exposure classification is inherited from the caller.
- [x] Generic `MutationGateway.execute_create_order` is fenced and rejects missing Global STOP; no production caller found in inspected executor orchestration.
- [x] Spot SELL, futures `reduceOnly=True` BUY, rollback, force-close and cancel remain de-risk/cancel paths; Global STOP does not reject de-risk submits. They are not reclassified as exposure-increasing entries.
- [x] Exact-ID NOT_FOUND and lookup exceptions remain unresolved; no new client ID is allocated from a negative lookup.

### Verification and release gates

- R-45 prior head: run [44044178958](https://github.com/nzf210/nafaz_arb/actions/runs/44044178958) SUCCESS; Python 711 passed / 4 skipped; PostgreSQL integration 4 passed; all five jobs passed.
- R-46 final source/test head `77906cf1`: Python 712 passed / 4 skipped / 111 warnings; disposable PostgreSQL integration 4 passed; all five CI jobs passed. Initial runs failed on two positive recovery fixtures that lacked a registry; those fixtures were corrected and the final run passed.
- Real venue sandbox/paper contract tests, operator-authenticated manual-resolution governance, repeated PostgreSQL multi-process disconnect/fault injection against the actual VPS topology, and futures minimum-notional overhedge policy remain open.

**Overall release state: PARTIAL / NOT LIVE-CERTIFIED.** No merge, deployment, production migration, or cutover was performed.