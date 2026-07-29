# Backbone Client Pull

Proyek Sync Client mandiri untuk menarik data dari **Backbone API** (Kemendikdasmen — Satu Data Pendidikan) ke database milik client, berjalan terjadwal secara mandiri di sisi client (on-premise / VM client), tanpa bergantung pada proyek/server lama.

## Daftar Isi

1. [Ringkasan](#1-ringkasan)
2. [Prasyarat](#2-prasyarat)
3. [Langkah Instalasi](#3-langkah-instalasi)
4. [Konfigurasi `.env`](#4-konfigurasi-env)
5. [Menjalankan sebagai Service](#5-menjalankan-sebagai-service)
6. [Menampilkan di Prefect UI (opsional)](#6-menampilkan-di-prefect-ui-opsional)
7. [⚠️ Keamanan — Rotasi Kredensial](#7-️-keamanan--rotasi-kredensial)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Ringkasan

Proyek ini menarik data dari **Backbone API** dan menyimpannya ke database **SQL Server** atau **PostgreSQL** milik client. Alur singkatnya:

- Mengambil **access-token** otomatis (tukar username/password → JWT), lalu membuat *request* akses ke Backbone API, lalu mengambil metadata tabel yang tersedia (`/metadata`).
- Menarik daftar wilayah akses (kecamatan) client, lalu menarik data tabel `sekolah` per wilayah untuk mendapatkan daftar NPSN.
- Berdasarkan metadata, tabel-tabel lain ditarik per NPSN (`param_type=npsn`) atau per wilayah (`param_type=wilayah`); tabel referensi (`param_type=ref`) bersifat opsional (diatur oleh `PULL_REF`).
- Setiap tabel disimpan dengan skema **incremental**: setiap tabel punya *checkpoint* `last_update` (disimpan di skema `sync`, tabel `pull_checkpoint`) sehingga proses berikutnya hanya menarik data yang berubah sejak penarikan terakhir.
- Struktur tabel (skema, kolom, tipe data, primary key) dibuat/disesuaikan otomatis mengikuti metadata dari Backbone API (auto DDL), termasuk pembuatan database jika belum ada (bisa dimatikan, lihat §8).
- Dijalankan terjadwal menggunakan **Prefect `serve`** (cron), sehingga tidak memerlukan Prefect Server/Cloud terpisah — cukup satu proses yang berjalan terus-menerus di sisi client.

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
| `SCHEDULE_CRON`            | Opsional| `0 2 * * *`                                                            | Jadwal cron standar (menit jam tgl bulan hari) untuk `main.py` (tanpa `--run-once`). Default: setiap hari jam 02:00. |
| `SCHEDULE_TIMEZONE`        | Opsional| `Asia/Jakarta`                                                         | Timezone untuk `SCHEDULE_CRON`.                                                                                  |
| `DEPLOYMENT_NAME`          | Opsional| `backbone-client-pull`                                                 | Nama deployment yang didaftarkan ke Prefect `serve`.                                                             |
| `PULL_REF`                 | Opsional| `false`                                                                | Jika `true`, tabel referensi (`param_type=ref`) ikut ditarik setiap siklus.                                      |

\* `DB_PORT` punya default per dialect di kode (`1433` untuk `sqlserver`, `5432` untuk `postgres`) jika variabel dikosongkan sepenuhnya — namun karena `.env.example` sudah mengisi `1433`, **wajib diubah manual menjadi `5432` saat memakai PostgreSQL**.

Nilai boolean (`DB_AUTO_CREATE_DATABASE`, `PULL_REF`) menerima `true/false`, `1/0`, `yes/no`, `y/n`, atau `on/off` (tidak case-sensitive).

## 5. Menjalankan sebagai Service

Untuk produksi, jalankan `uv run python main.py` (mode `serve`, bukan `--run-once`) sebagai *service* yang otomatis restart bila crash dan otomatis jalan saat server reboot.

### systemd (Linux)

Buat file `/etc/systemd/system/backbone-client-pull.service`:

```ini
[Unit]
Description=Backbone Client Pull (Prefect serve)
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

Aktifkan dan jalankan:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now backbone-client-pull
```

Cek status/log dengan `systemctl status backbone-client-pull` dan `journalctl -u backbone-client-pull -f`.

### NSSM (Windows)

[NSSM](https://nssm.cc/) membungkus proses biasa menjadi Windows Service.

1. Unduh & pasang NSSM, lalu jalankan (di Command Prompt/PowerShell sebagai Administrator) dari folder proyek:
   ```powershell
   nssm install BackboneClientPull
   ```
2. Pada dialog NSSM yang terbuka, isi:
   - **Application path**: `uv` (atau path lengkap ke `uv.exe`, mis. hasil `where uv`)
   - **Arguments**: `run python main.py`
   - **Startup directory**: path folder proyek (folder yang berisi `main.py` dan `.env`)
3. Klik **Install service**.
4. Jalankan service: `nssm start BackboneClientPull` (atau lewat `services.msc`).

Cek/atur ulang service kapan saja dengan `nssm edit BackboneClientPull`; hapus dengan `nssm remove BackboneClientPull confirm`.

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

## 7. ⚠️ Keamanan — Rotasi Kredensial

> **Peringatan penting sebelum membagikan atau memindahkan proyek ini ke pihak lain (client, repo baru, dsb.):**

- Proyek ini diporting dari implementasi lama yang **sudah pernah meng-commit** kredensial asli (Backbone **service JWT**, **API key**, dan **password database**) ke histori git repo lama. Kredensial tersebut **harus dianggap bocor** dan **wajib dirotasi** (diganti nilainya di sisi Backbone/DB) sebelum repo/histori tersebut dibagikan ke pihak manapun.
- Proyek standalone ini (`backbone-client-pull`) tidak berisi nilai kredensial asli di dalam kode maupun histori commit-nya — kredensial hanya diisi lewat file `.env` lokal di masing-masing client.
- **`.env` tidak boleh pernah di-commit ke git** (sudah didaftarkan di `.gitignore`). Selalu gunakan `.env.example` sebagai template dan isi `.env` secara lokal/manual di server client.
- Perlakukan `.env` seperti file rahasia: batasi hak akses baca (mis. `chmod 600 .env` di Linux), jangan kirim lewat chat/email tanpa enkripsi, dan simpan salinan cadangan kredensial di pengelola secret (password manager/vault), bukan di dalam repo.

## 8. Troubleshooting

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
