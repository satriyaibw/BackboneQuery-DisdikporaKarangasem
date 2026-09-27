# Panduan Instalasi — Backbone Client + Webapp Query/Ekspor

> Dokumen instalasi lengkap, high-level. Untuk detail ETL lihat `README.md`,
> untuk kontrak eksekusi per fase lihat `masterplan.md`, untuk contekan
> 1 halaman server lihat `DEPLOY-SERVER.md`.

---

## 1. Gambaran sistem

Sistem ini menarik data pendidikan dari **Backbone API Kemendikdasmen**
ke database lokal secara terjadwal, lalu menyajikannya lewat webapp
agar teknisi bisa query dan ekspor mandiri.

```
[Backbone API: auth → request → metadata → data]
        ↕ (a Hubbard + rate-limit, hanya tiap TANGGAL 4)
[backbone-client --loop] ──upsert──▶ [postgres db-target + volume pgdata-target]
                                          ▲ (role analis: SELECT saja)
[teknisi] ──login wajib──▶ [SQLPad :3000] ──▶ [publik via Cloudflare Tunnel]
```

| Komponen | Image / versi terverifikasi | RAM tipikal | Fungsi |
|---|---|---|---|
| `db-target` | `postgres:16-alpine` | ~250 MB | Database tujuan + volume persisten |
| `backbone-client` | build lokal (`Dockerfile`, Python 3.11) | <150 MB idle | ETL `--loop` (jadwal dinamis dari API) / `--run-once` (uji) |
| `sqlpad` | `sqlpad/sqlpad@sha256:6011…` (v7.5.7) | ~150–300 MB | Webapp: login, SQL editor, ekspor CSV/Excel |
| `quick-tunnel` (dev) | `cloudflare/cloudflared@sha256:072c…` | ~20 MB | URL publik acak `*.trycloudflare.com`, gratis tanpa akun |
| `cloudflared` (prod) | image sama, profile `tunnel` | ~20 MB | Domain tetap via token + CNAME Diskominfo, gratis |

Total puncak < 2 GB — aman untuk server 8 GB. Tanpa Prefect Server
(mematikan 1 Postgres + ~500 MB RAM tanpa mengurangi fungsi ETL).

**Batasan Yazid yang wajib dipahami sebelum instalasi:**

1. Akun Backbone hanya boleh membuat *request* akses pada **tanggal
   tertentu tiap bulan** (akun Karangasem: **tanggal 4**, dibaca otomatis
   dari `GET /user-info/schedule`). Di luar tanggal itu pull selalu
   berhenti di tahap `create-request` — itu normal, bukan error instalasi.
2. Webapp read-only: role Postgres `analis` hanya `SELECT` + batas
   60 detik per query, sehingga query box tidak bisa merusak data.
3. Tanpa login tidak ada data yang terlihat (API mengembalikan
   `Unauthorized`), baik via lokal maupun via tunnel publik.

---

## 2. Prasyarat

* **OS:** Linux (server) / Windows+WSL2 / macOS — yang penting ada
  **Docker 24+ dan plugin Docker Compose v2**.
* **Sumber daya:** 2 vCPU, RAM ≥ 4 GB (8 GB recommended), disk ≥ 30 GB
  (volume data + 1–2 image ~1,2 GB).
* **Kredensial Backbone** (via kanal aman, bukan chat publik):
  `BACKBONE_USERNAME`, `BACKBONE_PASSWORD`, `BACKBONE_API_KEY`.
  Deploy-token GitLab hanya untuk `git clone` repo privat, bukan `.env`.
* **Dev:** tanpa akun tambahan (Quick Tunnel anonim).
  **Prod:** akun Cloudflare Free pribadi (untuk token Named Tunnel) +
  1 × record `CNAME` dari Diskominfo ke `<tunnel-id>.cfargotunnel.com`.
  100% gratis; tidak ada batas pengunjung (kuota "50 user" hanya untuk
  login Zero Trust Access, tim 2–5 orang aman).

---

## 3. File instalasi di repo

| File | Dikomit? | Isi |
|---|---|---|
| `Dockerfile` | ya | Python 3.11-slim + `uv`, user non-root, `.venv`, entrypoint `main.py` |
| `docker-compose.app.yml` | ya | `db-target` + `backbone-client --run-once` (default uji) |
| `docker-compose.loop.yml` | ya | Override server: command `--loop` + `mem_limit` |
| `docker-compose.web.yml` | ya | `sqlpad` + `quick-tunnel` (profile `web`) + `cloudflared` named (profile `tunnel`); image dipin digest |
| `init_analis.sql` | ya | Role `analis` read-only, idempoten, aman sebelum/sesudah pull |
| `.env.example` / `.env.web.example` | ya | Template secret (tanpa nilai asli) |
| `.env` / `.env.web` | **TIDAK** (gitignored, `chmod 600`) | Nilai asli, dibuat lokal di tiap mesin |
| `INSTALL.md` (ini), `masterplan.md`, `DEPLOY-SERVER.md` | ya | Dokumentasi |

---

## 4. Instalasi cepat — dev lokal

```bash
# 0. Clone & secret (password DB dibuat kuat & unik per mesin)
git clone <repo-privat> backbone-client && cd backbone-client
cp .env.example .env && cp .env.web.example .env.web && chmod 600 .env .env.web
# Isi .env: DB_PASSWORD=<acak 20 char>, BACKBONE_USERNAME/PASSWORD/API_KEY.
# Isi .env.web: SQLPAD_ADMIN=<email>, SQLPAD_ADMIN_PASSWORD=<acak>,
#   ANALIS_DB_PASSWORD=<acak, beda dari keduanya>.

# 1. Validasi + database
docker compose -f docker-compose.app.yml config
docker compose -f docker-compose.app.yml up -d db-target   # tunggu healthy

# 2. Role analis (sekali saja; aman diulang)
set -a; source .env.web; set +a
docker compose -f docker-compose.app.yml exec -T db-target psql -U backbone \
  -d backbone_client -v analis_pw="$ANALIS_DB_PASSWORD" -f - < init_analis.sql

# 3. Build & uji ETL
docker compose -f docker-compose.app.yml build backbone-client
docker compose -f docker-compose.app.yml run --rm --entrypoint python backbone-client -m backbone_pull.check
# Ekspektasi di luar tanggal akses: DB OK + Token OK + API 400 (jadwal) = LULUS.

# 4. Webapp + publik dev
docker compose -f docker-compose.app.yml -f docker-compose.web.yml --profile web up -d sqlpad quick-tunnel
docker compose -f docker-compose.app.yml -f docker-compose.web.yml logs -f quick-tunnel
# → buka URL https://xxx.trycloudflare.com → login SQLPad.
```

Koneksi SQLPad (sekali saja, menu Connections → New):
`Driver PostgreSQL · Host db-target · Port 5432 · Database backbone_client ·
Username analis · Password = ANALIS_DB_PASSWORD` → Test → Save.

---

## 5. Verifikasi instalasi (wajib lulus semua)

| # | Uji | Ekspektasi |
|---|---|---|
| 1 | `exec db-target pg_isready` | accepting connections |
| 2 | `check` | DB OK, Token OK (API 400 di luar tanggal 4 = normal) |
| 3 | Login SQLPad salah password | 401 |
| 4 | `SELECT * FROM sync.pull_log LIMIT 10` sebagai analis | finished (boleh 0 baris sebelum pull) |
| 5 | `DROP TABLE sync.pull_log` sebagai analis | error `must be owner` |
| 6 | Download CSV hasil query | file terisi header + baris |
| 7 | Buka URL publik tanpa sesi → `/api/users` | `Unauthorized` |
| 8 | `git status --short` | tanpa `.env`/`.env.web`/dump |

---

## 6. Deploy server produksi (ringkas; detail di `DEPLOY-SERVER.md`)

1. Ulangi §4 langkah 0–3 di server dengan **password baru** (jangan copy
   `.env` dev).
2. Ganti mode ETL ke terjadwal:
   `docker compose -f docker-compose.app.yml -f docker-compose.loop.yml up -d backbone-client`
   Log harus memuat `Jadwal otomatis dari Backbone: tanggal [4]` dan
   `Menunggu...` hingga tanggal akses berikutnya.
3. Ganti Quick Tunnel → Named Tunnel: buat Tunnel di
   `Zero Trust → Networks → Tunnels`, isi `TUNNEL_TOKEN` di `.env.web`,
   `up` service `cloudflared` (profile `tunnel`), minta Diskominfo tambah
   CNAME → verifikasi TLS + login via domain tetap.
4. Opsional recommended: Cloudflare Access (OTP email) sebagai login lapis
   kedua; cron `pg_dump -Fc` mingguan + uji restore sekali.

---

## 7. Operasi rutin

```bash
# Kesehatan harian (ganti tanggal/field sesuai kebutuhan)
docker compose -f docker-compose.app.yml exec db-target psql -U backbone -d backbone_client \
  -c "SELECT tbl_name,status,rows_received FROM sync.pull_log ORDER BY run_started_at DESC LIMIT 20;" \
  -c "SELECT count(*) FROM sync.pull_failures;"
# Sisa gagal → retry cepat (tanpa siklus penuh):
docker compose -f docker-compose.app.yml run --rm backbone-client --retry-failed
# Backup: exec db-target pg_dump -U backbone -d backbone_client -Fc -f /tmp/bb_$(date +%F).dump
#   lalu docker cp keluar + simpan sha256sum (retensi 14 hari).
```

---

## 8. Troubleshooting

| Gejala | Penyebab umum | Aksi |
|---|---|---|
| `Tidak ada jadwal akses untuk hari ini` | Di luar tanggal akses API | Normal; tunggu tanggal 4 / cek `GET /user-info/schedule` |
| SQLPad login 401 padahal yakin benar | Salah ketik / spasi di password | Salin dari password manager; cek tidak ada spasi |
| `User already signed up` saat registrasi | Email sudah terdaftar | Langsung signin, bukan signup |
| URL `trycloudflare.com` mati | Container restart (URL acak) | Ambil URL baru dari log; prod pakai Named Tunnel |
| `sqlpad` unhealthy | DB belum healthy / port bentrok | `up -d db-target` dulu; cek `127.0.0.1:3000` bebas |
| Disk penuh | Volume `pgdata-target` membesar | `df -h`, backup lalu prune; cek `pg_database_size` |
| Query ETL vs analis berebut resource | Query analis berat saat pull | `statement_timeout` 60 dtk sudah aktif; geser analisis di luar jam pull |

---

## 9. Keamanan — aturan non-negosiable

1. Secret hanya di `.env`/`.env.web` (`chmod 600`), tidak pernah di
   Dockerfile/compose/SQL yang dikomit; password DB, analis, dan admin
   webapp ketiganya berbeda.
2. Postgres hanya bind `127.0.0.1`; publikasi internet **hanya via tunnel**,
   tidak pernah expose port DB.
3. Webapp selalu wajib login; peran DB webapp read-only walau semua admin
   boleh membaca semua tabel.
4. Rotasi password bila dicurigai bocor; matikan tunnel darurat via
   `--profile web down quick-tunnel` (dev) / stop `cloudflared` (prod).

---

*Dokumen ini high-level dan stabil. Bila perilaku API berubah (mis. jadwal
tidak lagi tanggal 4), perbarui §1/§8 dan laporkan — jangan ubah logika
`backbone_pull/` tanpa persetujuan.*
