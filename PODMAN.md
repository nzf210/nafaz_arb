# Panduan Deployment & Menjalankan dengan Podman

Dokumentasi ini menjelaskan langkah demi langkah cara men-deploy dan menjalankan **NAFAZ Spot–Perpetual Scanner** di server/VPS menggunakan [Podman](https://podman.io/).

---

## Daftar Isi
1. [Prasyarat](#1-prasyarat)
2. [Persiapan File di VPS](#2-persiapan-file-di-vps)
3. [Metode A: Menggunakan `podman compose` (Direkomendasikan)](#3-metode-a-menggunakan-podman-compose-direkomendasikan)
4. [Metode B: Menggunakan Perintah `podman` Langsung](#4-metode-b-menggunakan-perintah-podman-langsung)
5. [Verifikasi dan Pengujian](#5-verifikasi-dan-pengujian)
6. [Auto-Start saat VPS Reboot (Systemd)](#6-auto-start-saat-vps-reboot-systemd)
7. [Pemeliharaan & Update Aplikasi](#7-pemeliharaan--update-aplikasi)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Prasyarat

Pastikan VPS Anda telah terpasang:
* **Podman** (versi 3.4+ atau 4.x+):
  ```bash
  # Debian / Ubuntu:
  sudo apt update && sudo apt install -y podman

  # Rocky Linux / AlmaLinux / Fedora / CentOS Stream:
  sudo dnf install -y podman
  ```
* **podman-compose** (opsional, jika ingin memakai workflow compose):
  ```bash
  # Debian / Ubuntu:
  sudo apt install -y podman-compose
  # atau via pip:
  pip3 install podman-compose
  ```
* Verifikasi instalasi:
  ```bash
  podman --version
  ```

---

## 2. Persiapan File di VPS

1. Masuk ke direktori project di VPS:
   ```bash
   cd /path/to/nafaz-scanner
   ```

2. Buat file `config.json` dari template jika belum ada:
   ```bash
   cp config.example.json config.json
   ```

3. Buat folder penyimpanan data histori peluang:
   ```bash
   mkdir -p data data/executor data/scanner
   # Pastikan UID/GID ini sama dengan APP_UID/APP_GID saat build container.
   chown -R ${APP_UID:-1000}:${APP_GID:-1000} data/executor data/scanner
   ```

4. Edit file `config.json` sesuai kebutuhan:
   * Ubah `"mode"`: `"MOCK"` (testing simulasi) atau `"LIVE"` (koneksi data bursa live).
   * Konfigurasi alert Telegram jika ingin notifikasi aktif:
     ```json
     {
       "telegram": {
         "enabled": true,
         "bot_token": "YOUR_TELEGRAM_BOT_TOKEN",
         "chat_id": -100123456789,
         "allowed_chat_ids": [-100123456789],
         "alert_on_new": true
       }
     }
     ```

---

## 3. Metode A: Menggunakan `podman compose` (Direkomendasikan)

Project ini telah dilengkapi file `docker-compose.yml` yang kompatibel dengan Podman.

### Menjalankan Container
```bash
# Build image dan jalankan container di background (-d)
podman compose up -d --build
```
*(atau jika menggunakan tool eksternal `podman-compose`:)*
```bash
podman-compose up -d --build
```

### Memeriksa Status dan Log
```bash
# Cek status kontainer yang sedang berjalan
podman compose ps

# Membaca log realtime
podman compose logs -f
```

### Menghentikan Container
```bash
podman compose down
```

---

## 4. Metode B: Menggunakan Perintah `podman` Langsung

Jika tidak ingin menggunakan compose, Anda dapat mengeksekusi langsung dengan perintah CLI Podman.

### 1. Build Image
```bash
podman build -t nafaz-scanner:latest .
```

### 2. Jalankan Container
```bash
podman run -d \
  --name nafaz-scanner \
  --restart always \
  -p 8080:8080 \
  -v $(pwd)/config.json:/app/config.json:ro,Z \
  -v $(pwd)/data:/app/data:Z \
  -e TZ=Asia/Jakarta \
  nafaz-scanner:latest
```

#### Penjelasan Flag Penting:
* `-d`: Menjalankan container di background (detached mode).
* `--name nafaz-scanner`: Memberikan nama spesifik pada container.
* `--restart always`: Otomatis menyalakan ulang container jika proses crash.
* `-p 8080:8080`: Meneruskan port 8080 VPS ke port 8080 di dalam container.
* `-v $(pwd)/config.json:/app/config.json:ro,Z`: Mount file konfigurasi secara read-only.
* `-v $(pwd)/data:/app/data:Z`: Mount folder data agar histori `opportunities.json` tersimpan permanen di host VPS.
* `:Z` (SELinux flag): **Wajib di Linux bertipe RHEL/Fedora/CentOS/AlmaLinux** agar container memiliki izin akses label SELinux ke volume host.
* `-e TZ=Asia/Jakarta`: Menyamakan zona waktu container dengan waktu lokal.

### Perintah Operasional Harian
```bash
# Melihat log aplikasi
podman logs -f nafaz-scanner

# Stop container
podman stop nafaz-scanner

# Start container kembali
podman start nafaz-scanner

# Restart container (misal setelah ubah config.json)
podman restart nafaz-scanner

# Menghapus container
podman rm -f nafaz-scanner
```

---

## 5. Verifikasi dan Pengujian

Setelah container aktif, uji endpoint HTTP dari VPS:

```bash
# 1. Cek status health
curl http://localhost:8080/health

# 2. Cek status scanner
curl http://localhost:8080/api/v1/scanner/status

# 3. Trigger scan manual
curl -X POST http://localhost:8080/api/v1/scanner/scan

# 4. Cek hasil scan terakhir
curl http://localhost:8080/api/v1/scanner/latest

# 5. Cek metrik histori peluang
curl http://localhost:8080/api/v1/scanner/opportunities/metrics
```

Jika log menampilkan:
```text
read-only scanner (LIVE) listening at :8080
```
Maka scanner dan bot Telegram sudah bekerja normal.

---

## 6. Auto-Start saat VPS Reboot (Systemd)

Agar container otomatis menyala setiap kali server VPS di-reboot atau restart:

### Opsi 1: Menggunakan Systemd Tingkat Sistem (Root)
Jika Anda login sebagai `root` atau menggunakan `sudo`:

```bash
# 1. Generate unit file systemd dari container yang sedang berjalan
podman generate systemd --new --name nafaz-scanner --files

# 2. Pindahkan ke direktori systemd
sudo mv container-nafaz-scanner.service /etc/systemd/system/

# 3. Reload daemon dan aktifkan service
sudo systemctl daemon-reload
sudo systemctl enable --now container-nafaz-scanner.service

# 4. Cek status service
sudo systemctl status container-nafaz-scanner.service
```

### Opsi 2: Menggunakan Systemd Rootless (Non-root user)
Jika menjalankan podman sebagai user biasa tanpa sudo:

```bash
# 1. Pastikan linger aktif agar service berjalan meski user logout
loginctl enable-linger $USER

# 2. Buat folder systemd user
mkdir -p ~/.config/systemd/user/

# 3. Generate file unit
podman generate systemd --new --name nafaz-scanner --files
mv container-nafaz-scanner.service ~/.config/systemd/user/

# 4. Reload dan aktifkan
systemctl --user daemon-reload
systemctl --user enable --now container-nafaz-scanner.service
```

---

## 7. Pemeliharaan & Update Aplikasi

Jika ada perubahan source code Go atau update konfigurasi:

```bash
# 1. Pull perubahan terbaru dari git
git pull

# 2. Re-build dan nyalakan ulang container
podman compose down
podman compose up -d --build

# Atau jika menggunakan podman CLI:
podman stop nafaz-scanner
podman rm nafaz-scanner
podman build -t nafaz-scanner:latest .
podman run -d --name nafaz-scanner --restart always -p 8080:8080 -v $(pwd)/config.json:/app/config.json:ro,Z -v $(pwd)/data:/app/data:Z nafaz-scanner:latest
```

---

## 8. Troubleshooting

| Masalah | Kemungkinan Penyebab | Solusi |
|---|---|---|
| **Permission denied: ./data/opportunities.json** | SELinux membatasi akses mount volume di VPS. | Tambahkan flag `:Z` pada volume mount (`-v $(pwd)/data:/app/data:Z`). Pastikan folder `data` memiliki izin tulis (`chmod 775 data`). |
| **Container langsung exit / stopped** | Format `config.json` tidak valid atau file hilang. | Jalankan `podman logs nafaz-scanner` untuk membaca pesan error Go. Pastikan file `config.json` valid JSON. |
| **Telegram Bot tidak merespon** | Token salah, bot dinonaktifkan di config, atau chat ID belum di-whitelist. | Pastikan `telegram.enabled: true` di `config.json`, masukkan bot token yang valid, dan tambahkan chat ID Anda ke `allowed_chat_ids`. |
| **Port 8080 sudah digunakan** | Ada aplikasi lain di VPS yang memakai port 8080. | Ubah port host di docker-compose atau flag run: `-p 8888:8080` (akses via `http://localhost:8888`). |
| **failed to connect to the docker API at unix:///run/user/1000/podman/podman.sock** | Provider docker-compose membutuhkan Podman API Socket yang belum aktif. | Jalankan `systemctl --user enable --now podman.socket` atau gunakan binary native `podman-compose up -d --build`. |
| **error creating systemd unit `crun-buildah-....scope`: got `failed`** | Rootless Podman di VPS tidak memiliki delegasi cgroup systemd. | Atur cgroup manager ke `cgroupfs` dengan menjalankan perintah:<br>`mkdir -p ~/.config/containers && echo -e '[engine]\ncgroup_manager = "cgroupfs"\nevents_logger = "file"' > ~/.config/containers/containers.conf` |

---

## 9. Integrasi Supabase (opsional)

Scanner dan executor dapat menulis histori operasional ke Supabase. Redis tetap
dipakai untuk pub/sub, locks, harga live, saldo cache, dan pemulihan posisi aktif.
Supabase bukan jalur kritis untuk hedge, rollback, atau force-close.

Sediakan dua environment variable berikut pada host/container:

- `SUPABASE_URL`: URL project Supabase HTTPS.
- `SUPABASE_KEY`: secret `service_role` key (server-only), bukan anon/publishable key.
- `SUPABASE_OUTBOX_PATH`: file SQLite outbox executor; default
  `/app/data/executor/supabase-outbox.sqlite3`. Direktori ini dipetakan ke
  `./data/executor` di host agar antrean tetap ada setelah container restart/recreate.
- `SUPABASE_SCANNER_OUTBOX_PATH`: jurnal JSONL scanner; default
  `/app/data/scanner/supabase-outbox.jsonl`. Path ini berada di volume
  `./data` yang sama tetapi direktori scanner terpisah dari executor.

Contoh memasukkan key tanpa menuliskannya sebagai literal ke command history:

```bash
export SUPABASE_URL="https://<project-ref>.supabase.co"
read -rsp "Supabase service_role key: " SUPABASE_KEY
export SUPABASE_KEY
echo
podman-compose up -d --build
```

Pastikan environment tersebut diteruskan ke kedua service `nafaz-scanner` dan
`nafaz-executor`. Jangan commit key, jangan masukkan ke image, dan jangan expose
ke Vue atau Telegram. File `.env` di repository di-ignore oleh Git, tetapi gunakan
secret manager atau environment systemd yang terlindungi untuk deployment VPS.

Untuk menerapkan migration, periksa project dan migration history terlebih dahulu:

```bash
supabase link --project-ref <project-ref>
supabase migration list --remote
supabase db push --remote
```

Jangan jalankan `db push` sebelum schema dan migration history remote ditinjau.
Migration CI hanya membuktikan kompatibilitas pada PostgreSQL test, bukan bahwa
project Supabase remote Anda sudah menerima migration.

Cakupan runtime saat ini: scanner menulis `scanner_opportunities`; executor
menulis `positions`, `orders`, `order_fills`, `closed_trades`,
`executor_events`, dan `risk_events`. Tabel `position_legs`,
`funding_payments`, `balance_snapshots`, dan `executor_status_snapshots`
belum dihubungkan ke runtime.

Penulisan Supabase bersifat best-effort. Belum ada durable outbox; outage panjang,
queue penuh, atau proses mati dapat menyebabkan histori Supabase tidak lengkap.
Redis dan exchange API tetap menjadi sumber runtime untuk koordinasi dan exposure.
