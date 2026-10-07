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

Proyek ini menarik data dari **Backbone API** dan menyimpannya ke database **SQL Server**, **PostgreSQL**, atau **MySQL** milik client. Alur singkatnya:

- Mengambil **access-token** otomatis (tukar username/password → JWT), lalu membuat *request* akses ke Backbone API, lalu mengambil metadata tabel yang tersedia (`/metadata`).
- Menarik daftar wilayah akses (kecamatan) client, lalu menarik data tabel `sekolah` per wilayah untuk mendapatkan daftar NPSN.
- Berdasarkan metadata, tabel-tabel lain ditarik per NPSN (`param_type=npsn`) atau per wilayah (`param_type=wilayah`); tabel referensi (`param_type=ref`) diambil setiap siklus secara default (diatur oleh `PULL_REF`, default `true`), **per tabel lewat API, incremental berdasarkan `last_update`** — dengan opsi mencoba dulu download ZIP sekali jalan (`PULL_REF_USE_BULK_ZIP=true`, lebih hemat request tapi non-default) yang otomatis fallback ke jalur per-tabel bila perlu — lihat §8.
- Setiap tabel disimpan dengan skema **incremental**: setiap tabel punya *checkpoint* `last_update` (disimpan di skema `sync`, tabel `pull_checkpoint`) sehingga proses berikutnya hanya menarik data yang berubah sejak penarikan terakhir.
- Struktur tabel (skema, kolom, tipe data, primary key) dibuat/disesuaikan otomatis mengikuti metadata dari Backbone API (auto DDL), termasuk pembuatan database jika belum ada (bisa dimatikan, lihat §8).
- Penarikan dilakukan **paralel** (banyak NPSN/kecamatan sekaligus) dengan *rate limiter* yang menjaga laju request tidak melebihi batas API (default 20/detik) — jauh lebih cepat dari sekuensial, tetap aman. Diatur lewat `BACKBONE_CONCURRENCY` & `BACKBONE_RATE_LIMIT`.
- Dijalankan lewat **Prefect `serve`** — satu proses yang berjalan terus-menerus di sisi client. **Penting:** agar jadwal (`SCHEDULE_CRON`) benar-benar memicu run **otomatis**, proses ini harus tersambung ke **Prefect Server** (self-hosted, lihat §6) atau Prefect Cloud. Tanpa itu (mode ringan/*ephemeral*, default), jadwal terdaftar tapi **tidak pernah otomatis terpicu** — lihat peringatan & alternatif di §5.

Database tujuan dipilih lewat `DB_DIALECT` (`sqlserver`, `postgres`, atau `mysql`); logika penarikan data & incremental sama, hanya dialek SQL (tipe kolom, `MERGE`/`ON CONFLICT`/`ON DUPLICATE KEY UPDATE`, dsb.) yang berbeda di balik layar.

> **Catatan khusus `DB_DIALECT=mysql`:** MySQL tidak punya konsep *schema-dalam-database* seperti SQL Server/PostgreSQL. Semua `schema_name` dari Backbone (`dbo`, `ref`, `vld`, `datamart`, dst.) digabung ke **satu database** (`DB_NAME`), dengan nama tabel diberi **prefix schema**-nya — mis. `dbo.sekolah` → tabel `dbo_sekolah`, `vld.v_ptk` → `vld_v_ptk`. Butuh **MySQL 8.0+**.

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

  > **Catatan:** Jika instalasi dilakukan via Docker (`docker compose`), `uv` tidak diperlukan di sistem host karena semua berjalan dalam container. `uv` hanya dibutuhkan untuk menjalankan dari source code tanpa Docker.

- Akses ke database tujuan (SQL Server, PostgreSQL, **atau** MySQL 8.0+) dengan user yang punya hak baca/tulis pada database target (lihat §8 soal hak `CREATE DATABASE`).
- Kredensial akun Backbone API: **username**, **password**, dan **API key** (didapat dari pengelola Backbone/Kemendikdasmen — bukan bagian dari proyek ini). Access-token (JWT) diambil **otomatis** dari username/password di tiap run, jadi tidak perlu menyiapkan atau menempel JWT manual.

## 3. Langkah Instalasi

### 3.1 Dapatkan akses repo

Repo proyek ini **privat** — dibagikan ke tiap client lewat **deploy token** GitLab (kredensial read-only khusus untuk `git clone`/`git pull`, bukan akun login GitLab biasa). Pengelola pusat (Kemendikdasmen) akan mengirimkan:

- **Username token**, mis. `client-namakabupaten`
- **Token**, berupa string acak panjang

**Jaga kerahasiaan kredensial ini** — jangan diteruskan ke pihak di luar tim teknis Anda, jangan dikirim lewat kanal tidak terenkripsi, dan jangan commit ke repo lain. Tiap client menerima token yang **berbeda**, jadi bisa dicabut/diganti sendiri-sendiri oleh pengelola pusat tanpa mengganggu client lain — kalau token Anda kedaluwarsa atau perlu diganti, hubungi pengelola pusat untuk token baru.

### 3.2 Clone & instalasi

```bash
git clone https://<username-token>:<token>@dev.kemendikdasmen.go.id/data-pendidikan/interoperabilitas/backbone-client.git
cd backbone-client-pull

uv sync

cp .env.example .env      # Windows: copy .env.example .env
# edit .env: DB_DIALECT, koneksi DB, BACKBONE_API_KEY, BACKBONE_USERNAME, BACKBONE_PASSWORD

uv run python -m backbone_pull.check     # verifikasi koneksi DB & API Backbone
uv run python main.py --run-once         # uji tarik data sekali, lalu keluar
uv run python main.py                    # jalankan terjadwal (Prefect serve, cron)
```

Catatan:
- `uv sync` membuat virtual environment (`.venv/`) dan menginstal seluruh dependensi sesuai `pyproject.toml` (`prefect`, `aiohttp`, `sqlalchemy`, `pymssql`, `psycopg2-binary`, `pymysql`, `python-dotenv`, dst.) — driver untuk ketiga dialect terpasang sekaligus, tinggal pilih lewat `DB_DIALECT`.
- `uv run python -m backbone_pull.check` mencetak ringkasan konfigurasi lalu menguji koneksi database dan koneksi API Backbone (buat *request* akses). Pastikan semuanya `[ OK ]` sebelum lanjut.
- `uv run python main.py --run-once` menjalankan satu siklus penarikan penuh secara langsung (tanpa scheduler) — cocok untuk uji coba awal atau uji manual.
- `uv run python main.py --run-once --tables guru,ats` membatasi siklus itu hanya ke tabel yang disebut (pisah koma) — cocok untuk retest cepat satu/sejumlah tabel yang sempat error, tanpa menunggu full pull semua tabel. `sekolah` (dan proses ambil daftar NPSN-nya) tetap selalu jalan sebagai prasyarat, terlepas dari isi `--tables`. Hanya berlaku bareng `--run-once` — diabaikan (dengan peringatan) di mode `--loop`/`serve`.
- `uv run python main.py` (tanpa argumen) menjalankan proses jangka panjang yang mendaftarkan jadwal (`SCHEDULE_CRON`) lewat Prefect `serve` dan menunggu di foreground. Untuk produksi, jalankan ini sebagai *service* (lihat §5).
- **Update ke versi terbaru**: jalankan `git pull` di dalam folder proyek kapan saja (kredensial deploy token yang sama tetap berlaku selama belum kedaluwarsa/dicabut — tidak perlu clone ulang), lalu `uv sync` ulang kalau ada dependensi baru. Kalau sedang berjalan sebagai *service* (§5), restart service-nya setelah `git pull` agar perubahan terpakai.

## 4. Konfigurasi `.env`

Salin `.env.example` menjadi `.env` lalu isi sesuai lingkungan client. Variabel yang **wajib diisi per client** ditandai **Wajib**; sisanya punya default yang biasanya cukup dipakai apa adanya.

| Variabel                  | Wajib?  | Default (di `.env.example`)                                         | Keterangan                                                                                                   |
|----------------------------|---------|-----------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| `DB_DIALECT`               | Wajib   | `sqlserver`                                                            | `sqlserver`, `postgres`, atau `mysql`. Menentukan dialek SQL & driver yang dipakai.                              |
| `DB_HOST`                  | Wajib   | *(kosong)*                                                             | Host/alamat server database.                                                                                    |
| `DB_PORT`                  | Wajib*  | `1433`                                                                 | **Default di `.env.example` adalah `1433` (SQL Server)**. Untuk PostgreSQL ubah ke `5432`, untuk MySQL ubah ke `3306`. |
| `DB_USER`                  | Wajib   | *(kosong)*                                                             | User database.                                                                                                   |
| `DB_PASSWORD`              | Wajib   | *(kosong)*                                                             | Password database. **Jangan pernah commit nilai asli** (lihat §7).                                              |
| `DB_NAME`                  | Wajib   | `backbone_client`                                                      | Nama database tujuan; boleh diganti sesuai kebutuhan client. Untuk MySQL, ini satu-satunya database — semua tabel (lintas schema Backbone) masuk ke sini dengan nama diberi prefix schema. |
| `DB_AUTO_CREATE_DATABASE`  | Opsional| `true`                                                                 | Jika `true`, aplikasi mencoba membuat database `DB_NAME` otomatis kalau belum ada. Lihat §8.                    |
| `DB_MAINTENANCE_DB`        | Opsional| `postgres`                                                             | **Hanya relevan untuk `DB_DIALECT=postgres`** — database maintenance yang dipakai untuk `CREATE DATABASE`. Diabaikan untuk `sqlserver` (pakai `master`) dan `mysql` (connect tanpa database awal). Lihat §8. |
| `BACKBONE_BASE_URL`        | Opsional| `https://api.data.kemendikdasmen.go.id/svc/satu-data/pendidikan/v3`   | URL dasar Backbone API (endpoint data). Biasanya tidak perlu diubah.                                             |
| `BACKBONE_AUTH_URL`        | Opsional| `https://api.data.kemendikdasmen.go.id/svc/satu-data/auth/v1/access-token` | Endpoint tukar username/password → access-token. Biasanya tidak perlu diubah.                             |
| `BACKBONE_API_KEY`         | Wajib   | *(kosong)*                                                             | API key Backbone milik client (dikirim sebagai header `X-API-Key` di tiap request data).                        |
| `BACKBONE_USERNAME`        | Wajib   | *(kosong)*                                                             | Username akun Backbone milik client. Dipakai untuk mengambil access-token otomatis.                             |
| `BACKBONE_PASSWORD`        | Wajib   | *(kosong)*                                                             | Password akun Backbone milik client. **Jangan pernah commit nilai asli** (lihat §7).                            |
| `BACKBONE_PER_PAGE`        | Opsional| `500`                                                                  | Jumlah baris per halaman saat memanggil API Backbone.                                                            |
| `BACKBONE_RATE_LIMIT`      | Opsional| `20`                                                                   | Maks request/detik ke Backbone API (batas resmi saat ini **20/detik**). Penarikan dijaga tidak melebihi ini. Naikkan bila batas API dinaikkan. |
| `BACKBONE_CONCURRENCY`     | Opsional| `16`                                                                   | Jumlah entity (NPSN/kecamatan) yang ditarik **paralel**. Semakin besar semakin cepat, tetap dibatasi `BACKBONE_RATE_LIMIT`. |
| `SCHEDULE_CRON`            | Opsional| `0 2 * * *`                                                            | Mode `serve` (default): jadwal cron standar (menit jam tgl bulan hari). **Hanya benar-benar memicu run otomatis jika Prefect Server aktif** — lihat peringatan di §5. Mode `--loop` dgn `SCHEDULE_AUTO_FROM_API=true` (default): hanya field **jam:menit** yang dipakai (tgl/bulan/hari diabaikan, digantikan tanggal dari Backbone) — lihat §5.1. |
| `SCHEDULE_TIMEZONE`        | Opsional| `Asia/Jakarta`                                                         | Timezone untuk `SCHEDULE_CRON`.                                                                                  |
| `SCHEDULE_AUTO_FROM_API`   | Opsional| `true`                                                                 | **Khusus mode `--loop`.** Jika `true`, tanggal jadwal diambil otomatis dari `GET /user-info/schedule` Backbone (bukan tebak manual). Jika `false`, atau `SCHEDULE_CRON` bukan jam:menit tunggal, atau API gagal diakses — fallback ke `SCHEDULE_CRON` apa adanya. Lihat §5.1. |
| `SCHEDULE_RETRY_COUNT`     | Opsional| `3`                                                                    | **Khusus mode `--loop`** dgn `SCHEDULE_AUTO_FROM_API=true`. Jumlah percobaan berjeda per hari jadwal (jaga-jaga bila percobaan pertama gagal, mis. gangguan jaringan sesaat). Otomatis dipangkas bila jamnya akan melewati tengah malam. |
| `SCHEDULE_RETRY_INTERVAL_HOURS` | Opsional| `4`                                                               | **Khusus mode `--loop`** dgn `SCHEDULE_AUTO_FROM_API=true`. Jeda jam antar percobaan dalam `SCHEDULE_RETRY_COUNT`. |
| `DEPLOYMENT_NAME`          | Opsional| `backbone-client-pull`                                                 | Nama deployment yang didaftarkan ke Prefect `serve`.                                                             |
| `PULL_REF`                 | Opsional| `true`                                                                 | Jika `true`, tabel referensi (`param_type=ref`) ikut ditarik setiap siklus.                                      |
| `PULL_REF_USE_BULK_ZIP`    | Opsional| `false`                                                                | Jika `true`, coba dulu download ZIP sekali jalan untuk tabel referensi sebelum fallback ke jalur per-tabel via API. Lihat §8. |
| `PULL_LOG_RETENTION_DAYS`  | Opsional| `60`                                                                   | Berapa hari riwayat aktivitas (`sync.pull_log`) disimpan sebelum dihapus otomatis. Lihat §8.                     |

\* `DB_PORT` punya default per dialect di kode (`1433` untuk `sqlserver`, `5432` untuk `postgres`, `3306` untuk `mysql`) jika variabel dikosongkan sepenuhnya — namun karena `.env.example` sudah mengisi `1433`, **wajib diubah manual menjadi `5432`/`3306` saat memakai PostgreSQL/MySQL**.

Nilai boolean (`DB_AUTO_CREATE_DATABASE`, `PULL_REF`, `PULL_REF_USE_BULK_ZIP`) menerima `true/false`, `1/0`, `yes/no`, `y/n`, atau `on/off` (tidak case-sensitive).

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

`uv run python main.py --loop` menjalankan proses jangka panjang yang **menghitung sendiri** jadwal berikutnya (pakai library `croniter`) dan **tidak memerlukan Prefect Server/Docker sama sekali**. Cocok dipasang sebagai service biasa (NSSM/systemd, §5.3/§5.4) tanpa menambah infrastruktur.

**Jadwal otomatis dari Backbone (default, `SCHEDULE_AUTO_FROM_API=true`):** Backbone membatasi hari-dalam-bulan (`tanggal`) kapan sebuah akun boleh membuat request akses baru — diatur oleh Backbone/Kemendikdasmen sendiri per akun (lihat §7 soal `create_request`/jadwal akses), **bukan** sesuatu yang bisa diatur lewat proyek ini. Daripada client harus tahu/hardcode pola tanggalnya secara manual di `SCHEDULE_CRON`, tiap awal iterasi `--loop`:

1. Login (access-token) lalu panggil `GET /user-info/schedule` — dapat daftar tanggal jadwal akun (mis. `[14, 28]`).
2. Gabungkan tanggal itu dengan **jam:menit** dari `SCHEDULE_CRON` (mis. `0 2 * * *` → jam `02:00`) dan `SCHEDULE_RETRY_COUNT`/`SCHEDULE_RETRY_INTERVAL_HOURS`, membentuk cron dinamis — mis. tanggal `[14, 28]`, jam basis `02:00`, retry 3× berjeda 4 jam → `0 2,6,10 14,28 * *` (coba jam 02:00, kalau run sebelumnya di jendela itu belum sukses akan tercoba lagi jam 06:00 dan 10:00, di tanggal 14 **dan** 28 tiap bulan).
3. Cron dinamis itu dipakai untuk menghitung jadwal berikutnya — **bukan** `SCHEDULE_CRON` tanggal/bulan/harinya (field jam:menitnya tetap dipakai).

**Fallback otomatis ke `SCHEDULE_CRON` manual** (dicatat jelas di log, proses tetap jalan) bila: `SCHEDULE_AUTO_FROM_API=false`, atau `SCHEDULE_CRON` bukan format jam:menit tunggal sederhana (mis. `0 2 * * *`, bukan `0 2,6 * * *` atau `*/5 2 * * *`), atau panggilan ke Backbone gagal (jaringan/token), atau `GET /user-info/schedule` kosong.

Perilaku lainnya:
- Saat start, **menunggu sampai jadwal berikutnya tiba** dulu (tidak langsung menarik data begitu proses dimulai/di-restart).
- Jadwal yang **terlewat** (mis. komputer/VM mati saat melewati jam terjadwal) otomatis **dilewati** — begitu proses nyala lagi, ia menunggu ke kejadian berikutnya, bukan langsung menyusulkan run yang terlewat.
- Bila satu siklus penarikan **gagal** (mis. error API/DB), proses **tidak berhenti** — kegagalan dicatat ke log lalu loop lanjut menunggu jadwal berikutnya seperti biasa.
- Output berupa `print()` biasa (bukan lewat Prefect) — pastikan `flush`-nya tidak tertahan buffer saat diarahkan ke file log (lihat catatan `AppStdout` di §5.4 untuk NSSM; di Linux/systemd otomatis masuk `journalctl` tanpa perlu pengaturan tambahan).

Contoh log:
```
Mode loop internal aktif (Asia/Jakarta). Tidak perlu Prefect Server.
[*] Jadwal otomatis dari Backbone: tanggal [14, 28] → cron '0 2,6,10 14,28 * *'
[*] Menunggu... eksekusi berikutnya pada: 2026-08-14 02:00:00 WIB (683473 detik lagi)

[2026-08-14 02:00:00] Eksekusi dimulai...
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
  PostgreSQL tidak mengizinkan `CREATE DATABASE` dijalankan dari koneksi ke database yang sama; karena itu proses auto-create untuk `DB_DIALECT=postgres` membuka koneksi terpisah ke database maintenance (`DB_MAINTENANCE_DB`, default `postgres`) untuk menjalankan `CREATE DATABASE`. Pastikan database maintenance tersebut ada dan user punya akses ke sana. (Untuk `DB_DIALECT=sqlserver`, koneksi maintenance memakai database `master` bawaan; untuk `DB_DIALECT=mysql`, koneksi maintenance connect ke server tanpa memilih database awal — keduanya tidak dikonfigurasi lewat `.env`.)

- **MySQL: satu database, tabel diberi prefix schema**
  MySQL tidak punya konsep schema-dalam-database. Semua `schema_name` Backbone digabung ke satu database (`DB_NAME`) dengan nama tabel diberi prefix schema-nya (mis. `dbo.sekolah` → `dbo_sekolah`, `vld.v_ptk` → `vld_v_ptk`). Tabel kontrol (`sync.pull_checkpoint`, `sync.pull_failures`, `sync.pull_log`, `sync.pull_requests`) mengikuti pola yang sama → `sync_pull_checkpoint`, `sync_pull_failures`, `sync_pull_log`, `sync_pull_requests`. Butuh **MySQL 8.0+**.

- **Port default berbeda per dialect**
  `.env.example` mengisi `DB_PORT=1433` (port default SQL Server). Jika `DB_DIALECT=postgres`, ubah `DB_PORT` menjadi `5432`; jika `DB_DIALECT=mysql`, ubah menjadi `3306` — atau sesuaikan dengan port kustom server database Anda.

- **`uv run python -m backbone_pull.check` melaporkan `[GAGAL]` pada API Backbone**
  Biasanya berarti `BACKBONE_USERNAME`/`BACKBONE_PASSWORD` (gagal ambil access-token) dan/atau `BACKBONE_API_KEY` salah. Output `check` menandai langkah **Token akses** dan **API Backbone** secara terpisah; pesan error dari Backbone (field `keterangan`) ditampilkan langsung — periksa kembali nilai di `.env`.

- **`uv run python -m backbone_pull.check` melaporkan `[GAGAL]` pada koneksi database**
  Periksa `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, dan `DB_NAME`; pastikan server database dapat dijangkau dari mesin client (firewall/network) dan driver terkait (`pymssql` untuk SQL Server, `psycopg2-binary` untuk PostgreSQL, `pymysql` untuk MySQL) sudah terinstal lewat `uv sync`.

## 8. Memastikan Kelengkapan Data

Penarikan ini memverifikasi kelengkapan secara otomatis, sehingga status run bisa dipercaya:

> Contoh query di bawah pakai notasi `sync.pull_failures`/`sync.pull_log`/`sync.pull_requests` (SQL Server/PostgreSQL). **Untuk `DB_DIALECT=mysql`**, ganti dengan nama tabel prefix-schema-nya: `sync_pull_failures`, `sync_pull_log`, `sync_pull_requests` (tanpa titik) — lihat §7.

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
- Kalau setelah itu **masih ada sisa gagal**, `--run-once` dan tiap iterasi `--loop` otomatis mencoba **satu kali lagi** lewat mode retry-only (lihat di bawah) — sebelum menunggu jadwal penuh berikutnya, yang bisa berjarak berhari-hari kalau jadwal akses Backbone jarang (mis. tanggal 14/28 tiap bulan, lihat §5.1). Tidak perlu tindakan manual; ini otomatis.
- Item yang **masih gagal juga setelah itu** tetap tersimpan dan **dicoba lagi otomatis pada run terjadwal berikutnya**. Gap akan menutup sendiri saat sumber pulih — Anda cukup memantau apakah `sync.pull_failures` sudah kosong.
- Untuk memaksa coba ulang segera secara manual — dua pilihan:
  - `uv run python main.py --run-once` — jalankan siklus penuh (semua NPSN/wilayah/tabel, incremental) lalu retry-only otomatis kalau masih ada sisa. Durasinya mirip run terjadwal biasa.
  - `uv run python main.py --retry-failed` — **hanya** retry item yang ada di `sync.pull_failures` saat itu (tanpa pass utama) — jauh lebih cepat, cocok kalau Anda tahu sisa gagalnya sedikit dan ingin membereskannya segera tanpa menunggu/menjalankan siklus penuh. Hasilnya **mengoreksi baris `sync.pull_log` terbaru** untuk tabel itu di tempat (bukan menyisipkan baris baru) — jadi begitu `sync.pull_failures` bersih, `status` di baris yang sama juga otomatis berubah dari `incomplete` menjadi `ok`, tidak perlu bandingkan dua baris berbeda.

### Tabel referensi (`PULL_REF=true`): jalur API per-tabel vs download ZIP
Saat `PULL_REF=true` (default), tabel referensi (`param_type=ref`) ditarik lewat salah satu dari dua jalur, diatur oleh `PULL_REF_USE_BULK_ZIP`:

1. **Jalur default (`PULL_REF_USE_BULK_ZIP=false`) — per tabel lewat API** (`GET /referensi?ref=<nama>`): sama seperti tabel npsn/wilayah lainnya — berpaginasi dan **incremental** berdasarkan `last_update` per tabel (`sync.pull_checkpoint`), jadi setelah pull pertama, siklus berikutnya hanya menarik baris yang berubah. Ini jalur paling sederhana dan tidak punya risiko ambiguitas nilai `NULL` vs string kosong (`''`) — data ditulis langsung dari JSON, bukan lewat perantara CSV.
2. **Jalur opsional (`PULL_REF_USE_BULK_ZIP=true`) — download ZIP sekali jalan** (`GET /referensi/download`): satu request mengambil snapshot *seluruh* tabel referensi sekaligus (dikemas server sebagai ZIP berisi satu CSV per tabel + `manifest.json`), lebih hemat request dibanding jalur per-tabel di atas — tapi **bukan incremental** (selalu snapshot penuh) dan datanya lewat perantara CSV, yang tidak bisa membedakan `NULL` dari string kosong (`''`) pada kolom sumber; kalau kolom `NOT NULL` yang nilainya `''` kena konversi jadi `NULL` saat parse, baris/tabel itu otomatis **fallback** ke jalur per-tabel di atas (lihat `pull_ref_bulk` di `flow.py`) — tidak sampai menjatuhkan seluruh run.

Fallback ke jalur per-tabel juga otomatis terjadi (walau `PULL_REF_USE_BULK_ZIP=true`) bila server Backbone yang diakses belum mendukung `GET /referensi/download`, ZIP belum pernah di-*generate* di sisi server, atau satu tabel tertentu hilang/jumlah barisnya tidak cocok dengan `manifest.json`.

### Riwayat aktivitas penarikan (`sync.pull_log`)
Setiap kali sebuah tabel selesai ditarik (per run), satu baris ringkasan dicatat ke tabel **`sync.pull_log`** — sehingga aktivitas penarikan bisa dicek kapan saja tanpa perlu membuka log Prefect/terminal:

```sql
SELECT tbl_name, param_type, run_started_at, run_finished_at, duration_seconds,
       rows_received, entities_total, entities_failed, status,
       batch_id, request_id
FROM sync.pull_log
ORDER BY run_started_at DESC;
```

- Satu baris per **tabel per run** (bukan per NPSN/kecamatan/halaman), jadi volumenya kecil (maks puluhan–ratusan baris per run) dan tidak berdampak ke kecepatan penarikan.
- `status` mencerminkan hasil **akhir**, bukan sekadar percobaan pertama: `ok` bila semua entity untuk tabel itu akhirnya lengkap; `incomplete` bila masih ada yang gagal (lihat detailnya di `sync.pull_failures`). Baris ditulis sesaat setelah tabel itu ditarik, lalu **dikoreksi otomatis di tempat** (baris yang sama, bukan baris baru) begitu ada pemulihan — baik di akhir run yang sama (pemulihan otomatis di bawah), maupun belakangan lewat `--retry-failed`/auto-chain retry-only (§5) yang bisa terjadi di invocation terpisah — jadi kalau suatu entity sempat gagal lalu berhasil dipulihkan (kapan pun), `status`-nya akan berubah dari `incomplete` menjadi `ok` tanpa perlu tindakan manual maupun membandingkan beberapa baris.
- **`batch_id`** — UUID yang dibuat sekali di awal tiap run dan sama untuk semua baris `pull_log` dari run itu; dipakai untuk mengelompokkan "semua tabel yang ditarik dalam satu run yang sama" tanpa perlu mencocokkan `run_started_at` antar tabel (yang nilainya sedikit berbeda per tabel).
- **`request_id`** — ID sesi akses Backbone (`/user-info/request`) yang dipakai untuk pull itu; berguna untuk menelusuri apakah sekelompok kegagalan berkaitan dengan sesi tertentu (mis. sesi yang kedaluwarsa di tengah run). Detail lengkap sesi itu (termasuk `expired_date`) ada di tabel **`sync.pull_requests`** (lihat di bawah) — join lewat `request_id` bila perlu.
- Untuk instalasi yang sudah berjalan (`sync.pull_log` sudah ada dari sebelum kolom `batch_id`/`request_id` ditambahkan): kolom baru ini **ditambahkan otomatis** (`ALTER TABLE ... ADD`) di awal run berikutnya — tidak perlu migrasi manual, tidak ada downtime.
- Baris lebih tua dari `PULL_LOG_RETENTION_DAYS` hari (default **60 hari**, ±2 bulan) **dihapus otomatis** di awal tiap run — tidak perlu pembersihan manual.
- Detail per akses endpoint (tiap request HTTP) tetap tersedia di log Prefect/terminal (lihat `journalctl -u backbone-client-pull` bila dijalankan sebagai service, §5) — `sync.pull_log` hanya menyimpan ringkasannya agar hemat & cepat.

### Info sesi request Backbone (`sync.pull_requests`)
Satu baris per `request_id` (bukan per run — sesi yang sama dipakai ulang selama masih berlaku akan meng-update baris yang sama, bukan menambah baris baru):

```sql
SELECT request_id, expired_date, raw_info, first_seen_at, last_used_at
FROM sync.pull_requests
ORDER BY last_used_at DESC;
```

- `raw_info` adalah salinan mentah (JSON) seluruh field yang dikembalikan Backbone untuk sesi itu — apa adanya dari API, tidak diasumsikan field tertentu di luar `request_id`/`expired_date`.
- `last_used_at` ter-update tiap kali sesi itu dipakai lagi di run berikutnya (selama belum `expired_date`) — jadi baris ini juga menunjukkan kapan terakhir kali sesi itu masih dipakai.

> **Catatan perbaikan:** Mulai versi ini, checkpoint `last_update` menggunakan `started_at` (waktu mulai penarikan) bukan `datetime.now()` (waktu selesai). Ini memastikan konsistensi dengan `run_started_at` di `sync.pull_log` dan mencegah kehilangan data saat pull terhenti di tengah jalan.
