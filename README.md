# Backbone Client Pull

Proyek Sync Client mandiri untuk menarik data dari **Backbone API** (Kemendikdasmen — Satu Data Pendidikan) ke database milik client, berjalan terjadwal secara mandiri di sisi client (on-premise / VM client), tanpa bergantung pada proyek/server lama.

## Daftar Isi

1. [Ringkasan](#1-ringkasan)
2. [Prasyarat](#2-prasyarat)
3. [Langkah Instalasi](#3-langkah-instalasi)
4. [Konfigurasi `.env`](#4-konfigurasi-env)
5. [Menjalankan sebagai Service](#5-menjalankan-sebagai-service)
6. [Menampilkan di Prefect UI (opsional)](#6-menampilkan-di-prefect-ui-opsional)
7. [Troubleshooting](#7-troubleshooting)
8. [Memastikan Kelengkapan Data](#8-memastikan-kelengkapan-data)

---

## 1. Ringkasan

Proyek ini menarik data dari **Backbone API** dan menyimpannya ke database **SQL Server** atau **PostgreSQL** milik client. Alur singkatnya:

- Mengambil **access-token** otomatis (tukar username/password → JWT), lalu membuat *request* akses ke Backbone API, lalu mengambil metadata tabel yang tersedia (`/metadata`).
- Menarik daftar wilayah akses (kecamatan) client, lalu menarik data tabel `sekolah` per wilayah untuk mendapatkan daftar NPSN.
- Berdasarkan metadata, tabel-tabel lain ditarik per NPSN (`param_type=npsn`) atau per wilayah (`param_type=wilayah`); tabel referensi (`param_type=ref`) bersifat opsional (diatur oleh `PULL_REF`).
- Setiap tabel disimpan dengan skema **incremental**: setiap tabel punya *checkpoint* `last_update` (disimpan di skema `sync`, tabel `pull_checkpoint`) sehingga proses berikutnya hanya menarik data yang berubah sejak penarikan terakhir.
- Struktur tabel (skema, kolom, tipe data, primary key) dibuat/disesuaikan otomatis mengikuti metadata dari Backbone API (auto DDL), termasuk pembuatan database jika belum ada (bisa dimatikan, lihat §8).
- Penarikan dilakukan **paralel** (banyak NPSN/kecamatan sekaligus) dengan *rate limiter* yang menjaga laju request tidak melebihi batas API (default 20/detik) — jauh lebih cepat dari sekuensial, tetap aman. Diatur lewat `BACKBONE_CONCURRENCY` & `BACKBONE_RATE_LIMIT`.
- Dijalankan lewat **Prefect `serve`** — satu proses yang berjalan terus-menerus di sisi client. **Penting:** agar jadwal (`SCHEDULE_CRON`) benar-benar memicu run **otomatis**, proses ini harus tersambung ke **Prefect Server** (self-hosted, lihat §6) atau Prefect Cloud. Tanpa itu (mode ringan/*ephemeral*, default), jadwal terdaftar tapi **tidak pernah otomatis terpicu** — lihat peringatan & alternatif di §5.

Database tujuan dipilih lewat `DB_DIALECT` (`sqlserver` atau `postgres`); logika penarikan data & incremental sama, hanya dialek SQL (tipe kolom, `MERGE`/`ON CONFLICT`, dsb.) yang berbeda di balik layar.

## 2. Prasyarat

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)** — pengelola paket & virtual environment Python yang dipakai proyek ini.

  Instalasi `uv`:

  - macOS / Linux:
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```
  - Windows (PowerShell):
    ```powershell
    powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

- Akses ke database tujuan (SQL Server **atau** PostgreSQL) dengan user yang punya hak baca/tulis pada database target (lihat §8 soal hak `CREATE DATABASE`).
- Kredensial akun Backbone API: **username**, **password**, dan **API key** (didapat dari pengelola Backbone/Kemendikdasmen — bukan bagian dari proyek ini). Access-token (JWT) diambil **otomatis** dari username/password di tiap run, jadi tidak perlu menyiapkan atau menempel JWT manual.

## 3. Langkah Instalasi

```bash
git clone <repo> backbone-client-pull   # atau salin folder proyek
cd backbone-client-pull

uv sync

cp .env.example .env      # Windows: copy .env.example .env
# edit .env: DB_DIALECT, koneksi DB, BACKBONE_API_KEY, BACKBONE_USERNAME, BACKBONE_PASSWORD

uv run python -m backbone_pull.check     # verifikasi koneksi DB & API Backbone
uv run python main.py --run-once         # uji tarik data sekali, lalu keluar
uv run python main.py                    # jalankan terjadwal (Prefect serve, cron)
```

Catatan:
- `uv sync` membuat virtual environment (`.venv/`) dan menginstal seluruh dependensi sesuai `pyproject.toml` (`prefect`, `aiohttp`, `sqlalchemy`, `pymssql`, `psycopg2-binary`, `python-dotenv`, dst.).
- `uv run python -m backbone_pull.check` mencetak ringkasan konfigurasi lalu menguji koneksi database dan koneksi API Backbone (buat *request* akses). Pastikan semuanya `[ OK ]` sebelum lanjut.
- `uv run python main.py --run-once` menjalankan satu siklus penarikan penuh secara langsung (tanpa scheduler) — cocok untuk uji coba awal atau uji manual.
- `uv run python main.py` (tanpa argumen) menjalankan proses jangka panjang yang mendaftarkan jadwal (`SCHEDULE_CRON`) lewat Prefect `serve` dan menunggu di foreground. Untuk produksi, jalankan ini sebagai *service* (lihat §5).

## 4. Konfigurasi `.env`

Salin `.env.example` menjadi `.env` lalu isi sesuai lingkungan client. Variabel yang **wajib diisi per client** ditandai **Wajib**; sisanya punya default yang biasanya cukup dipakai apa adanya.

| Variabel                  | Wajib?  | Default (di `.env.example`)                                         | Keterangan                                                                                                   |
|----------------------------|---------|-----------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| `DB_DIALECT`               | Wajib   | `sqlserver`                                                            | `sqlserver` atau `postgres`. Menentukan dialek SQL & driver yang dipakai.                                       |
| `DB_HOST`                  | Wajib   | *(kosong)*                                                             | Host/alamat server database.                                                                                    |
| `DB_PORT`                  | Wajib*  | `1433`                                                                 | **Default di `.env.example` adalah `1433` (SQL Server)**. Untuk PostgreSQL, ubah menjadi `5432`.                 |
| `DB_USER`                  | Wajib   | *(kosong)*                                                             | User database.                                                                                                   |
| `DB_PASSWORD`              | Wajib   | *(kosong)*                                                             | Password database. **Jangan pernah commit nilai asli** (lihat §7).                                              |
| `DB_NAME`                  | Wajib   | `backbone_client`                                                      | Nama database tujuan; boleh diganti sesuai kebutuhan client.                                                    |
| `DB_AUTO_CREATE_DATABASE`  | Opsional| `true`                                                                 | Jika `true`, aplikasi mencoba membuat database `DB_NAME` otomatis kalau belum ada. Lihat §8.                    |
| `DB_MAINTENANCE_DB`        | Opsional| `postgres`                                                             | **Hanya relevan untuk `DB_DIALECT=postgres`** — database maintenance yang dipakai untuk `CREATE DATABASE`. Lihat §8. |
| `BACKBONE_BASE_URL`        | Opsional| `https://api.data.kemendikdasmen.go.id/svc/satu-data/pendidikan/v3`   | URL dasar Backbone API (endpoint data). Biasanya tidak perlu diubah.                                             |
| `BACKBONE_AUTH_URL`        | Opsional| `https://api.data.kemendikdasmen.go.id/svc/satu-data/auth/v1/access-token` | Endpoint tukar username/password → access-token. Biasanya tidak perlu diubah.                             |
| `BACKBONE_API_KEY`         | Wajib   | *(kosong)*                                                             | API key Backbone milik client (dikirim sebagai header `X-API-Key` di tiap request data).                        |
| `BACKBONE_USERNAME`        | Wajib   | *(kosong)*                                                             | Username akun Backbone milik client. Dipakai untuk mengambil access-token otomatis.                             |
| `BACKBONE_PASSWORD`        | Wajib   | *(kosong)*                                                             | Password akun Backbone milik client. **Jangan pernah commit nilai asli** (lihat §7).                            |
| `BACKBONE_PER_PAGE`        | Opsional| `500`                                                                  | Jumlah baris per halaman saat memanggil API Backbone.                                                            |
| `BACKBONE_RATE_LIMIT`      | Opsional| `20`                                                                   | Maks request/detik ke Backbone API (batas resmi saat ini **20/detik**). Penarikan dijaga tidak melebihi ini. Naikkan bila batas API dinaikkan. |
| `BACKBONE_CONCURRENCY`     | Opsional| `16`                                                                   | Jumlah entity (NPSN/kecamatan) yang ditarik **paralel**. Semakin besar semakin cepat, tetap dibatasi `BACKBONE_RATE_LIMIT`. |
| `SCHEDULE_CRON`            | Opsional| `0 2 * * *`                                                            | Jadwal cron standar (menit jam tgl bulan hari) untuk `main.py` (tanpa `--run-once`). Default: setiap hari jam 02:00. **Hanya benar-benar memicu run otomatis jika Prefect Server aktif** — lihat peringatan di §5. |
| `SCHEDULE_TIMEZONE`        | Opsional| `Asia/Jakarta`                                                         | Timezone untuk `SCHEDULE_CRON`.                                                                                  |
| `DEPLOYMENT_NAME`          | Opsional| `backbone-client-pull`                                                 | Nama deployment yang didaftarkan ke Prefect `serve`.                                                             |
| `PULL_REF`                 | Opsional| `false`                                                                | Jika `true`, tabel referensi (`param_type=ref`) ikut ditarik setiap siklus.                                      |
| `PULL_LOG_RETENTION_DAYS`  | Opsional| `60`                                                                   | Berapa hari riwayat aktivitas (`sync.pull_log`) disimpan sebelum dihapus otomatis. Lihat §8.                     |

\* `DB_PORT` punya default per dialect di kode (`1433` untuk `sqlserver`, `5432` untuk `postgres`) jika variabel dikosongkan sepenuhnya — namun karena `.env.example` sudah mengisi `1433`, **wajib diubah manual menjadi `5432` saat memakai PostgreSQL**.

Nilai boolean (`DB_AUTO_CREATE_DATABASE`, `PULL_REF`) menerima `true/false`, `1/0`, `yes/no`, `y/n`, atau `on/off` (tidak case-sensitive).

## 5. Menjalankan sebagai Service

Untuk produksi, jalankan `uv run python main.py` (mode `serve`, bukan `--run-once`) sebagai *service* yang otomatis restart bila crash dan otomatis jalan saat server reboot.

> ⚠️ **Penting — `SCHEDULE_CRON` TIDAK otomatis berjalan tanpa Prefect Server.**
> Proses `main.py` (mode `serve`) hanya **mem-poll run yang sudah dijadwalkan oleh server** — ia tidak mengevaluasi cron sendiri secara lokal. Dalam mode ringan/*ephemeral* (default, `PREFECT_API_URL` nonaktif), tidak ada layanan penjadwal yang berjalan, sehingga run terjadwal **tidak pernah otomatis terpicu** — proses hanya diam menunggu (akan terlihat warning `Cannot schedule flows on an ephemeral server` di log). Pilih salah satu **sebelum** memasang service produksi di bawah:
>
> - **Opsi A — Prefect Server:** aktifkan dulu (`docker compose up -d` + set `PREFECT_API_URL` di `.env`, lihat §6), baru pasang service **mode `serve`** (tanpa argumen) di §5.3/§5.4. Scheduler sungguhan Prefect memicu run sesuai `SCHEDULE_CRON`, riwayatnya terlihat di Prefect UI.
> - **Opsi B — mode `--loop` (rekomendasi bila tidak butuh Prefect UI):** pasang service yang menjalankan `main.py --loop` (§5.3/§5.4, tinggal ganti argumen) — **tidak perlu Prefect Server sama sekali**. Aplikasi menghitung sendiri jadwal berikutnya dari `SCHEDULE_CRON`/`SCHEDULE_TIMEZONE`; lihat detail di §5.1.
> - **Opsi C — penjadwal OS:** jangan jalankan `main.py` sebagai service jangka panjang sama sekali. Jadwalkan `uv run python main.py --run-once` (sekali jalan lalu keluar) lewat **penjadwal OS** — lihat §5.2.
>
> Memicu run manual kapan saja (semua opsi): `uv run python main.py --run-once`. (Khusus Opsi A yang sedang tersambung Prefect Server, bisa juga lewat `prefect deployment run 'backbone-client-pull/backbone-client-pull'`.)

### 5.1 Mode `--loop`: jadwal internal tanpa Prefect Server (Opsi B)

`uv run python main.py --loop` menjalankan proses jangka panjang yang **menghitung sendiri** jadwal berikutnya dari `SCHEDULE_CRON`/`SCHEDULE_TIMEZONE` di `.env` (pakai library `croniter`) — mendukung ekspresi cron apa pun (bukan cuma jam tetap), dan **tidak memerlukan Prefect Server/Docker sama sekali**. Cocok dipasang sebagai service biasa (NSSM/systemd, §5.3/§5.4) tanpa menambah infrastruktur.

Perilakunya:
- Saat start, **menunggu sampai jadwal berikutnya tiba** dulu (tidak langsung menarik data begitu proses dimulai/di-restart).
- Jadwal yang **terlewat** (mis. komputer/VM mati saat melewati jam terjadwal) otomatis **dilewati** — begitu proses nyala lagi, ia menunggu ke kejadian berikutnya, bukan langsung menyusulkan run yang terlewat.
- Bila satu siklus penarikan **gagal** (mis. error API/DB), proses **tidak berhenti** — kegagalan dicatat ke log lalu loop lanjut menunggu jadwal berikutnya seperti biasa.
- Output berupa `print()` biasa (bukan lewat Prefect) — pastikan `flush`-nya tidak tertahan buffer saat diarahkan ke file log (lihat catatan `AppStdout` di §5.4 untuk NSSM; di Linux/systemd otomatis masuk `journalctl` tanpa perlu pengaturan tambahan).

Contoh log:
```
Mode loop internal aktif — jadwal '0 2 * * *' (Asia/Jakarta). Tidak perlu Prefect Server.
[*] Menunggu... eksekusi berikutnya pada: 2026-08-04 02:00:00 WIB (31245 detik lagi)

[2026-08-04 02:00:00] Eksekusi dimulai...
Eksekusi selesai.
```

### 5.2 Opsi C — penjadwal OS (tanpa service jangka panjang)

Jalankan `uv run python main.py --run-once` langsung lewat penjadwal bawaan OS — tanpa `serve`/`--loop`, tanpa Prefect Server, tanpa NSSM/systemd (§5.3/§5.4 di bawah tidak diperlukan untuk opsi ini).

**Windows (Task Scheduler):**
1. Buka **Task Scheduler** (`taskschd.msc`) → **Create Basic Task**.
2. **Trigger**: Daily, jam sesuai kebutuhan (mis. 02:00).
3. **Action**: Start a program —
   - **Program/script**: `uv` (atau path lengkap ke `uv.exe`, hasil `where uv`)
   - **Add arguments**: `run python main.py --run-once`
   - **Start in**: path folder proyek (folder yang berisi `main.py` dan `.env`)
4. Selesai. Uji jalankan task-nya sekali secara manual dari Task Scheduler untuk memastikan berhasil.

**Linux (cron):**
```bash
crontab -e
```
Tambahkan (sesuaikan path & jadwal):
```
0 2 * * * cd /opt/backbone-client-pull && /usr/bin/env uv run python main.py --run-once >> logs/cron.log 2>&1
```

### 5.3 systemd (Linux) — untuk Opsi A atau Opsi B

Buat file `/etc/systemd/system/backbone-client-pull.service`:

```ini
[Unit]
Description=Backbone Client Pull
After=network-online.target

[Service]
WorkingDirectory=/opt/backbone-client-pull
ExecStart=/usr/bin/env uv run python main.py
Restart=on-failure
User=prefect

[Install]
WantedBy=multi-user.target
```

Sesuaikan `WorkingDirectory` dengan lokasi instalasi proyek (folder yang berisi `main.py`, `.env`, dan `.venv/` hasil `uv sync`), dan `User` dengan user sistem yang menjalankan service (pastikan user tersebut punya akses baca ke folder proyek dan `.env`).

**Pilih `ExecStart` sesuai opsi jadwal (lihat peringatan di atas):**
- **Opsi A** (Prefect Server aktif): `ExecStart=/usr/bin/env uv run python main.py` (tanpa argumen — mode `serve`, seperti contoh di atas).
- **Opsi B** (`--loop`, tanpa Prefect Server): `ExecStart=/usr/bin/env uv run python main.py --loop`.

Aktifkan dan jalankan:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now backbone-client-pull
```

Cek status/log dengan `systemctl status backbone-client-pull` dan `journalctl -u backbone-client-pull -f`.

### 5.4 NSSM (Windows) — untuk Opsi A atau Opsi B

[NSSM](https://nssm.cc/) membungkus proses biasa menjadi Windows Service.

1. Unduh & pasang NSSM, lalu jalankan (di Command Prompt/PowerShell sebagai Administrator) dari folder proyek:
   ```powershell
   nssm install BackboneClientPull
   ```
2. Pada dialog NSSM yang terbuka, isi:
   - **Application path**: `uv` (atau path lengkap ke `uv.exe`, mis. hasil `where uv`)
   - **Arguments**: `run python main.py` (**Opsi A**, Prefect Server aktif) **atau** `run python main.py --loop` (**Opsi B**, tanpa Prefect Server — lihat §5.1)
   - **Startup directory**: path folder proyek (folder yang berisi `main.py` dan `.env`)
3. Klik **Install service**.
4. Jalankan service: `nssm start BackboneClientPull` (atau lewat `services.msc`).

Cek/atur ulang service kapan saja dengan `nssm edit BackboneClientPull`; hapus dengan `nssm remove BackboneClientPull confirm`.

**Mengecek log di Windows:** secara bawaan, NSSM **membuang** output (stdout/stderr) proses yang dibungkusnya — beda dengan systemd di Linux yang otomatis masuk `journalctl`. Aktifkan redirect ke file log (di PowerShell, sebagai Administrator):

```powershell
nssm set BackboneClientPull AppStdout C:\path\ke\project\logs\stdout.log
nssm set BackboneClientPull AppStderr C:\path\ke\project\logs\stderr.log
nssm set BackboneClientPull AppRotateFiles 1
nssm set BackboneClientPull AppRotateOnline 1
nssm set BackboneClientPull AppRotateBytes 10485760
nssm restart BackboneClientPull
```

Lalu pantau log realtime dengan:

```powershell
Get-Content -Wait -Tail 50 C:\path\ke\project\logs\stdout.log
```

Alternatif yang **tidak tergantung OS** untuk cek aktivitas penarikan — dan lebih ringkas — adalah query langsung ke tabel `sync.pull_log` dan `sync.pull_failures` (lihat §8), yang bekerja sama persis di Windows maupun Linux.

## 6. Menampilkan di Prefect UI (opsional)

Secara default, `uv run python main.py` berjalan dalam mode **Prefect `serve` ringan** tanpa server terpisah — cukup untuk penarikan terjadwal, tetapi tanpa dashboard. Jika Anda ingin memantau lewat **Prefect UI** (riwayat *run*, log realtime, status jadwal, dan *Quick run* manual), jalankan **Prefect Server self-hosted** lalu arahkan proses `serve` ke server tersebut. **Logika penarikan tidak berubah** — cukup satu variabel `PREFECT_API_URL`.

### 6.1 Jalankan Prefect Server (Docker)

Proyek ini menyertakan `docker-compose.yml` berisi **Prefect Server + PostgreSQL** (penyimpanan metadata). Dari folder proyek:

```bash
docker compose up -d                    # jalankan server + database di background
docker compose logs -f prefect-server   # (opsional) pantau proses startup
```

- UI tersedia di **http://127.0.0.1:4200**.
- Metadata Prefect tersimpan di volume Docker (`prefect-db`) — tetap ada walau container di-restart.
- Hentikan dengan `docker compose down` (data tetap tersimpan) atau `docker compose down -v` (hapus juga volume/metadata).

> Prasyarat: Docker + Docker Compose terpasang di mesin server. Server ini terpisah dari database **tujuan penarikan** (SQL Server/PostgreSQL milik client) — PostgreSQL di `docker-compose.yml` hanya untuk metadata Prefect.

**Retensi log Prefect Server:** semua *flow run*/*task run*/log yang tampil di Prefect UI tersimpan di database metadata ini (tabel internal Prefect, terpisah dari `sync.pull_log` — lihat §8) — **ini bukan tabel yang sama dengan `sync.pull_log`.** `docker-compose.yml` sudah mengaktifkan pembersihan otomatisnya (`PREFECT_SERVER_SERVICES_DB_VACUUM_ENABLED=events,flow_runs`, retensi ~60 hari via `PREFECT_SERVER_SERVICES_DB_VACUUM_RETENTION_PERIOD`) — konsisten dengan `PULL_LOG_RETENTION_DAYS` di §8. Tanpa ini, Prefect Server **tidak membersihkan flow run/log secara default** dan akan terus bertambah.

### 6.2 Arahkan penarikan ke server

Di `.env`, aktifkan `PREFECT_API_URL` dengan endpoint API server:

```
PREFECT_API_URL=http://127.0.0.1:4200/api
```

(Jika server berada di mesin lain, ganti `127.0.0.1` dengan host/IP server.) Lalu jalankan penarikan seperti biasa:

```bash
uv run python main.py
```

Deployment `backbone-client-pull` beserta seluruh *flow run*, *task run*, log, dan jadwalnya kini muncul di Prefect UI, dan Anda bisa memicu penarikan manual lewat tombol **Quick run**. Proses `main.py` tetap menjadi **eksekutor** dan harus terus berjalan (lihat §5 untuk menjalankannya sebagai service); server hanya menyimpan state & menyediakan UI.

> **Catatan:** bila `PREFECT_API_URL` dikosongkan/di-nonaktifkan, aplikasi kembali ke mode ringan tanpa UI (tidak perlu server). Menjalankan server bersifat **opsional** dan tidak mengubah logika penarikan.
>
> **Alternatif Prefect Cloud:** daripada server self-hosted, Anda bisa memakai [Prefect Cloud](https://app.prefect.cloud) — jalankan `uv run prefect cloud login`, atau isi `PREFECT_API_URL` (URL API workspace Cloud) dan `PREFECT_API_KEY` di `.env`. Metadata *run* akan terkirim ke Cloud — pertimbangkan kebijakan data sebelum memakainya.

### 6.3 Akses dari komputer lain di jaringan (LAN)

Secara default, `docker-compose.yml` sudah membuka port 4200 ke semua interface (`PREFECT_SERVER_API_HOST: 0.0.0.0`) — jadi **secara jaringan**, komputer lain di LAN yang sama sudah bisa mencapai `http://<IP-server>:4200`. Tapi ada satu langkah lagi yang **wajib** supaya UI benar-benar berfungsi saat dibuka dari komputer lain (bukan cuma halamannya termuat, tapi datanya juga tampil):

**Kenapa perlu:** saat browser memuat Prefect UI, ia meminta konfigurasi runtime ke `/ui-settings` untuk tahu alamat API yang harus dipanggil. Tanpa pengaturan tambahan, alamat itu default ke `http://0.0.0.0:4200/api` — alamat yang **tidak valid** dipanggil dari komputer manapun (termasuk dari server itu sendiri, kecuali kebetulan). Akibatnya: halaman UI termuat, tapi gagal menampilkan data run/log (error koneksi API di browser).

**Langkah:**
1. Cek IP atau hostname server (dari server itu sendiri):
   - Windows (PowerShell): `ipconfig` (lihat `IPv4 Address` di adapter jaringan aktif)
   - Linux/macOS: `hostname -I` atau `ip addr show`
2. Di `docker-compose.yml`, isi baris `PREFECT_SERVER_UI_API_URL` (hapus tanda `#` di depannya) dengan IP/hostname tersebut:
   ```yaml
   PREFECT_SERVER_UI_API_URL: http://192.168.1.50:4200/api
   ```
3. Terapkan ulang: `docker compose up -d` (container di-recreate agar env baru terbaca).
4. Dari komputer lain di jaringan yang sama, buka `http://192.168.1.50:4200` di browser.

**Catatan:**
- Ini pengaturan **statis** — setelah diisi, *semua* pengakses (termasuk dari server itu sendiri lewat `127.0.0.1`) akan diarahkan memanggil API di alamat yang sama itu (biasanya tetap berfungsi normal, karena IP tersebut memang bisa dijangkau dari server itu sendiri juga).
- **Firewall**: pastikan port `4200` diizinkan menerima koneksi masuk dari jaringan lokal (Windows Defender Firewall / `ufw` di Linux) — bukan cuma dari `localhost`.
- **Keamanan:** Prefect Server **tidak punya autentikasi bawaan** — siapa pun yang bisa menjangkau port 4200 dapat melihat *dan mengendalikan* semua run. Batasi hanya ke jaringan lokal tepercaya. Jangan expose port ini ke internet; untuk akses dari luar jaringan lokal, gunakan VPN atau reverse-proxy dengan autentikasi, bukan expose langsung.

## 7. Troubleshooting

- **Gagal membuat database otomatis / error hak akses (`CREATE DATABASE`)**
  Saat `DB_AUTO_CREATE_DATABASE=true` (default), aplikasi mencoba membuat database `DB_NAME` jika belum ada — ini butuh user DB dengan hak `CREATE DATABASE` di server. Jika user tidak punya hak tersebut, ada dua opsi:
  1. Minta admin DB memberi hak `CREATE DATABASE` ke user tersebut, **atau**
  2. Set `DB_AUTO_CREATE_DATABASE=false` di `.env` dan buat database secara manual terlebih dahulu (aplikasi akan langsung memakainya tanpa mencoba membuat).

- **PostgreSQL: auto-create database memakai `DB_MAINTENANCE_DB`**
  PostgreSQL tidak mengizinkan `CREATE DATABASE` dijalankan dari koneksi ke database yang sama; karena itu proses auto-create untuk `DB_DIALECT=postgres` membuka koneksi terpisah ke database maintenance (`DB_MAINTENANCE_DB`, default `postgres`) untuk menjalankan `CREATE DATABASE`. Pastikan database maintenance tersebut ada dan user punya akses ke sana. (Untuk `DB_DIALECT=sqlserver`, koneksi maintenance memakai database `master` bawaan dan tidak dikonfigurasi lewat `.env`.)

- **Port default berbeda per dialect**
  `.env.example` mengisi `DB_PORT=1433` (port default SQL Server). Jika `DB_DIALECT=postgres`, ubah `DB_PORT` menjadi `5432` (port default PostgreSQL) — atau sesuaikan dengan port kustom server database Anda.

- **`uv run python -m backbone_pull.check` melaporkan `[GAGAL]` pada API Backbone**
  Biasanya berarti `BACKBONE_USERNAME`/`BACKBONE_PASSWORD` (gagal ambil access-token) dan/atau `BACKBONE_API_KEY` salah. Output `check` menandai langkah **Token akses** dan **API Backbone** secara terpisah; pesan error dari Backbone (field `keterangan`) ditampilkan langsung — periksa kembali nilai di `.env`.

- **`uv run python -m backbone_pull.check` melaporkan `[GAGAL]` pada koneksi database**
  Periksa `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, dan `DB_NAME`; pastikan server database dapat dijangkau dari mesin client (firewall/network) dan driver terkait (`pymssql` untuk SQL Server, `psycopg2-binary` untuk PostgreSQL) sudah terinstal lewat `uv sync`.

## 8. Memastikan Kelengkapan Data

Penarikan ini memverifikasi kelengkapan secara otomatis, sehingga status run bisa dipercaya:

- **Run HIJAU (sukses) = data 100% lengkap.** Untuk tiap entity (NPSN / kecamatan) × tabel, jumlah baris yang diterima dibandingkan dengan `total_rows` yang dilaporkan API. Bila semua cocok dan tanpa error, run sukses.
- **Run MERAH (gagal) = ada yang belum lengkap.** Flow sengaja digagalkan di akhir bila masih ada item yang belum lengkap — jadi terlihat jelas di Prefect UI / *exit code*, bukan "hijau palsu".

### Melihat apa yang gagal
Item yang gagal/tidak lengkap dicatat di tabel **`sync.pull_failures`** pada database target:

```sql
SELECT tbl_name, param_type, entity_id, reason, detail, expected, received, attempts, failed_at
FROM sync.pull_failures;
```

Arti kolom `reason`:
- `error` — gagal memanggil API setelah beberapa kali retry (mis. jaringan/timeout).
- `sp_error` — API mengembalikan pesan error (field `keterangan`).
- `incomplete` — jumlah baris diterima < `total_rows` (bandingkan `expected` vs `received`).

### Pemulihan otomatis
- Di **akhir setiap run**, item di `sync.pull_failures` dicoba ulang sekali — *full pull* tanpa filter incremental agar dijamin lengkap. Yang berhasil dihapus dari tabel.
- Item yang masih gagal **tetap tersimpan** dan **dicoba lagi otomatis pada run terjadwal berikutnya**. Gap akan menutup sendiri saat sumber pulih — Anda cukup memantau apakah `sync.pull_failures` sudah kosong.
- Untuk memaksa coba ulang segera (tanpa menunggu jadwal): jalankan `uv run python main.py --run-once`.

### Riwayat aktivitas penarikan (`sync.pull_log`)
Setiap kali sebuah tabel selesai ditarik (per run), satu baris ringkasan dicatat ke tabel **`sync.pull_log`** — sehingga aktivitas penarikan bisa dicek kapan saja tanpa perlu membuka log Prefect/terminal:

```sql
SELECT tbl_name, param_type, run_started_at, run_finished_at, duration_seconds,
       rows_received, entities_total, entities_failed, status
FROM sync.pull_log
ORDER BY run_started_at DESC;
```

- Satu baris per **tabel per run** (bukan per NPSN/kecamatan/halaman), jadi volumenya kecil (maks puluhan–ratusan baris per run) dan tidak berdampak ke kecepatan penarikan.
- `status` mencerminkan hasil **akhir** run (setelah pemulihan otomatis di bawah), bukan sekadar percobaan pertama: `ok` bila semua entity untuk tabel itu akhirnya lengkap; `incomplete` bila masih ada yang gagal sampai akhir run (lihat detailnya di `sync.pull_failures`). Baris ditulis sesaat setelah tabel itu ditarik, lalu **diperbarui otomatis** di akhir run begitu proses pemulihan (di bawah) selesai — jadi kalau suatu entity sempat gagal lalu berhasil dipulihkan pada run yang sama, `status`-nya akan berubah dari `incomplete` menjadi `ok` tanpa perlu tindakan manual.
- Baris lebih tua dari `PULL_LOG_RETENTION_DAYS` hari (default **60 hari**, ±2 bulan) **dihapus otomatis** di awal tiap run — tidak perlu pembersihan manual.
- Detail per akses endpoint (tiap request HTTP) tetap tersedia di log Prefect/terminal (lihat `journalctl -u backbone-client-pull` bila dijalankan sebagai service, §5) — `sync.pull_log` hanya menyimpan ringkasannya agar hemat & cepat.
