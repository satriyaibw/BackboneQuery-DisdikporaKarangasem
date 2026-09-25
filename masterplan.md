# MASTERPLAN — Backbone Client: ETL Terjadwal + Webapp Query/Ekspor

> Dokumen high-level untuk dieksekusi junior programmer / agent murah.
> Prinsip: ikuti langkah berurutan, jangan lompat fase. Setiap fase punya
> Acceptance Criteria (AC) — fase lanjut hanya bila AC terpenuhi.

---

## 0. Ringkasan eksekutif

**Tujuan akhir:** server kantor (8 GB RAM) menarik otomatis data Backbone
Kemendikdasmen tiap **tanggal 4** ke Postgres lokal, lalu 2–5 teknisi
**wajib login** via webapp untuk query + lihat output + ekspor CSV/Excel.

**Keputusan arsitektur (terkunci, jangan diubah tanpa approval):**

| Aspek | Keputusan | Alasan |
|---|---|---|
| DB target | `postgres:16-alpine` | Native schema (`dbo.*`, `sync.*`) 1:1 seperti SQL Server; adapter matang (`ON CONFLICT`); image ~80 MB. MySQL butuh prefix (`dbo_sekolah`) + rewrite query — ditolak. |
| Mode ETL | `main.py --loop` (jadwal dinamis dari `GET /user-info/schedule`) | Tanpa Prefect Server (hemat ~500 MB RAM). Jadwal tanggal `[4]` diambil otomatis dari API, bukan hardcode cron. |
| Webapp | **SQLPad** (`sqlpad/sqlpad:latest`) | Paling ringan (~150 MB image, ~150 MB RAM) + tercepat deploy untuk use-case "teknisi query + ekspor". Metabase/Superset ditolak (berat, butuh ≥1 GB). pgAdmin hanya fallback. |
| Akses publik dev | **Cloudflare Quick Tunnel** (`*.trycloudflare.com`) | Gratis, tanpa akun/token/domain. URL acak tiap restart. Hanya untuk tahap dev. |
| Akses publik prod | **Cloudflare Named Tunnel** + domain Diskominfo | Gratis (Free plan, tunnel unlimited). Diskominfo cukup tambah 1 × `CNAME`. Token dari akun Cloudflare pribadi. |
| Auth | 1 peran manusia = admin; teknis 2 lapis: role PG `analis` (read-only) + akun SQLPad lokal | Semua data boleh dibaca semua admin, tapi query box tidak boleh bisa `DROP/DELETE`. Tanpa login = tidak bisa apa pun. |

**Batasan kuota Cloudflare (sering disalahpahami):** Tunnel publik =
unlimited pengunjung + unlimited bandwidth di Free plan. Angka **"50 user
gratis" hanya untuk Zero Trust seats** (login Cloudflare Access), bukan
batas pengunjung. Tim 2–5 orang memakai 2–5 dari 50 seat — aman.

---

## 1. Inventarisasi titik awal (state saat dokumen ini ditulis)

Repo: `backbone-client` (root = folder berisi `main.py`).
Branch/commit rujukan: `b2fce72` (cek via `git log --oneline -5`).

| File | Status | Keterangan |
|---|---|---|
| `main.py`, `backbone_pull/`, `pyproject.toml`, `uv.lock`, `.python-version (3.11)` | tracked | Inti ETL, toolchain `uv`. Jangan ubah tanpa alasan. |
| `README.md` (408 baris), `docker-compose.yml` (Prefect Server :4200, opsional) | tracked | `docker-compose.yml` JANGAN dipakai untuk prod (hanya UI observabilitas). |
| `.env.example` | tracked | Template. Nilai asli tidak pernah dikomit. |
| `Dockerfile` | **untracked (baru, dev)** | `python:3.11-slim` + `uv` + user `appuser:1001` + `.venv` + `ENTRYPOINT ["python","main.py"]` default `--run-once`. |
| `docker-compose.app.yml` | **untracked (baru, dev)** | `db-target: postgres:16-alpine` (`127.0.0.1:5433:5432`, volume `pgdata-target`) + `backbone-client` (`env_file: .env`, `depends_on: healthy`, `command: --run-once`). |
| `.env` | gitignored, ada di device dev | Berisi kredensial Karangasem + `DB_PASSWORD` lokal. Jangan `cat`/paste ke chat. |
| `query_data/individu/` | tracked | Query penyajian dialek MsSQL — perlu adaptasi ringan ke Postgres bila dipakai di SQLPad. |
| Kredensial Backbone Karangasem | via kanal aman saja | `BACKBONE_USERNAME`, `BACKBONE_PASSWORD`, `BACKBONE_API_KEY`. Deploy-token GitLab (`backbone-kab-karangasem`) hanya untuk `git clone`, bukan `.env`. |

Fakta operasional terverifikasi: `check` → DB OK + Token OK, tetapi
`POST /user-info/request` 400 di luar tanggal 4 (`GET /user-info/schedule`
= `tanggal: [4]`, `GET /user-info/request` = `[]`). Ini perilaku normal,
bukan bug. Pull penuh hanya sukses tiap tanggal 4.

---

## 2. Target akhir (definisi selesai)

1. Server kantor: `db-target` + `backbone-client --loop` + `sqlpad` + `cloudflared` (named) berjalan sebagai service (`restart: unless-stopped`), RAM total < 2 GB.
2. `backbone-client --loop` log: `Menunggu... eksekusi berikutnya pada: 2026-10-04 ...`.
3. Webapp publik via domain Diskominfo (TLS otomatis), halaman login muncul, tanpa login tidak ada data terlihat.
4. Admin login → `SELECT * FROM sync.pull_log ORDER BY run_started_at DESC LIMIT 20` → grid tampil → Export CSV berhasil.
5. `sync.pull_failures` kosong setelah pull tanggal 4; backup `pg_dump -Fc` terjadwal + pernah diuji restore.
6. Tidak ada secret (`.env*`, password, token Tunnel) yang terkomit ke git.

---

## 3. Fase eksekusi

### FASE A — Kunci & rapikan repo di device dev (estimasi: 0,5 hari)

**Tujuan:** repo siap di-clone server tanpa membawa secret dev.

- A1. Pastikan `.gitignore` mengabaikan: `.env`, `.env.*` (kecuali `*.example`), `*.dump`, `*.sql` hasil dump, `/tmp/`, `pgdata/`.
- A2. Buat file baru berikut (spesifikasi di §4), lalu `git add` + komit HANYA file non-secret:
  - `docker-compose.web.yml` (service `sqlpad` + `quick-tunnel`, `profiles: [web]`)
  - `init_analis.sql` (role read-only)
  - `.env.web.example` (template, tanpa nilai asli)
  - `DEPLOY-SERVER.md` (1 halaman clone-to-run)
  - `masterplan.md` (dokumen ini)
- A3. JANGAN komit: `.env`, nilai password/token asli, file `*.dump`/`*.csv` dev.

**AC-A:** `git status --short` tidak menampilkan `.env`/dump; `git log` memuat komit fase A; `docker compose -f docker-compose.app.yml config` valid.

### FASE B — Webapp dev lokal + Quick Tunnel (estimasi: 0,5–1 hari)

**Tujuan:** buktikan alur login → query → ekspor di device dev sebelum sentuh server.

- B1. `docker compose -f docker-compose.app.yml up -d db-target` → tunggu `healthy`.
- B2. Jalankan `init_analis.sql` sekali (buat role `analis` read-only + `statement_timeout 60s`).
- B3. `docker compose -f docker-compose.app.yml -f docker-compose.web.yml --profile web up -d sqlpad quick-tunnel`.
- B4. Ambil URL acak dari `logs -f quick-tunnel` (`https://xxx.trycloudflare.com`) → buka → login SQLPad admin.
- B5. Uji: koneksi `db-target:5432` sukses; query `SELECT * FROM sync.pull_log LIMIT 10`; export CSV 1 baris; logout → pastikan tanpa login tidak bisa akses.
- B6. Matikan tunnel bila tidak dipakai (`--profile web down quick-tunnel`).

**AC-B:** URL publik dev menampilkan halaman login; query + ekspor CSV berhasil; `db-target` tetap healthy.

### FASE C — Siapkan server kantor via clone (estimasi: 0,5 hari)

**Tujuan:** server = hasil clone bersih + secret prod baru (BUKAN copy `.env` dev).

- C1. Di server: install Docker + compose plugin; `git clone <repo-url-privat> backbone-client && cd backbone-client`.
- C2. `cp .env.example .env` + `cp .env.web.example .env.web` → isi NILAI BARU (password DB prod kuat via `openssl rand`, password SQLPad admin beda dari password DB, kredensial Backbone sama via kanal aman). `chmod 600 .env .env.web`.
- C3. `docker compose -f docker-compose.app.yml up -d db-target` → jalankan `init_analis.sql`.
- C4. `docker compose -f docker-compose.app.yml build backbone-client`.
- C5. Verifikasi `run --rm --entrypoint python backbone-client -m backbone_pull.check`: ekspektasi di luar tanggal 4 = DB OK + Token OK + API 400 (jadwal). Ini LULUS, bukan gagal.

**AC-C:** `check` sesuai ekspektasi; tidak ada file secret di `git status`; `db-target` healthy.

### FASE D — Go-live ETL terjadwal (estimasi: 0,25 hari + tunggu tanggal 4)

- D1. Ubah command service app menjadi `--loop` (via override `command:` di compose server atau variabel env yang disepakati — JANGAN edit `main.py`).
- D2. Jalankan sebagai service: `restart: unless-stopped` → `up -d backbone-client` → cek log memuat `Jadwal otomatis dari Backbone: tanggal [4]` + `Menunggu... 2026-10-04`.
- D3. Tanggal 4: pantau `logs -f backbone-client` + `SELECT * FROM sync.pull_log ORDER BY run_started_at DESC` + `SELECT count(*) FROM sync.pull_failures` (harus 0).
- D4. Jika sisa gagal: `run --rm backbone-client --retry-failed` sekali.

**AC-D:** `pull_log` ada baris `status=ok` per tabel; `pull_failures` kosong.

### FASE E — Publik prod via Named Tunnel + domain Diskominfo (estimasi: 0,5 hari)

**Tujuan:** ganti Quick Tunnel (acak) → domain tetap, tetap 100% gratis.

- E1. Di akun Cloudflare pribadi: `Zero Trust → Networks → Tunnels → Create` → salin **Token**.
- E2. Minta Diskominfo tambah `CNAME <host> → <tunnel-id>.cfargotunnel.com` (satu record, tanpa pindah nameserver).
- E3. Di server: ganti service `quick-tunnel` → `cloudflared` named (`tunnel --no-autoupdate run --token $TUNNEL_TOKEN`), token hanya di `.env.web` (600).
- E4. Uji TLS + login SQLPad via domain tetap. Opsional (recommended): tambah Cloudflare Access (email OTP 2–5 teknisi, memakai ≤5 dari 50 seat gratis).

**AC-E:** domain tetap menampilkan login SQLPad dengan sertifikat valid; tanpa login tidak ada data.

### FASE F — Operasi & backup (estimasi: 0,25 hari, lalu rutin)

- F1. Cron host: `pg_dump -Fc` mingguan (atau sehari setelah tiap tanggal 4) + retensi 14 hari di disk terpisah. Simpan `sha256sum` + `pg_restore --list` sebagai manifest.
- F2. Uji restore SEKALI ke container kosong sebelum dinyatakan selesai.
- F3. Runbook 1 halaman: perintah cek (`ps`, `logs`, query `pull_log`), restart service, rotate password, matikan tunnel darurat.
- F4. (Opsional BigQuery): ekspor Parquet/CSV per tabel penting → `bq load`. Catatan: file `.sql`/`.dump` TIDAK bisa di-load langsung ke BigQuery.

**AC-F:** ada 1 file backup valid + 1 bukti restore + runbook tersimpan.

---

## 4. Spesifikasi file yang harus dibuat (kontrak untuk junior)

> Semua path relatif terhadap root repo. Gunakan image tag persis.
> Secret hanya via `env_file` / variabel env — TIDAK PERNAH via `ENV` di Dockerfile
> atau nilai hardcoded di compose/SQL yang dikomit.

### 4.1 `docker-compose.web.yml` (BARU)

- Service `sqlpad`: `image: sqlpad/sqlpad:latest`, `env_file: [.env.web]`,
  `depends_on: db-target (healthy)`, `ports: ["127.0.0.1:3000:3000"]`,
  `volumes: [sqlpad-data:/etc/sqlpad]`, `mem_limit: 512M`, `restart: unless-stopped`,
  `profiles: [web]`. Koneksi DB di dalam SQLPad menunjuk host `db-target:5432`
  dengan user `analis` (bukan `backbone`).
- Service `quick-tunnel`: `image: cloudflare/cloudflared:latest`,
  `command: ["tunnel","--no-autoupdate","--url","http://sqlpad:3000"]`,
  `depends_on: [sqlpad]`, tanpa secret, `profiles: [web]`, log URL ke stdout.
- Service `cloudflared` (prod, profil `tunnel`, JANGAN aktif bersamaan dengan
  `quick-tunnel`): `command: ["tunnel","--no-autoupdate","run","--token","${TUNNEL_TOKEN}"]`.
- Volumes: `sqlpad-data:`.
- Larangan: jangan duplikasi definisi `db-target` (reuse via `-f` ganda);
  jangan expose `5432/5433` ke `0.0.0.0`; jangan commit token.

### 4.2 `init_analis.sql` (BARU, idempoten)

- `DO $$ ...` atau `CREATE ROLE IF NOT EXISTS analis` (guard manual):
  buat role `analis LOGIN PASSWORD '<diisi-saat-eksekusi-dari-env>'`.
- `GRANT CONNECT ON DATABASE backbone_client; GRANT USAGE ON SCHEMA dbo,ref,vld,datamart,sync; GRANT SELECT ON ALL TABLES IN SCHEMA ...; ALTER DEFAULT PRIVILEGES FOR ROLE backbone IN SCHEMA ... GRANT SELECT ON TABLES TO analis;`
- `ALTER ROLE analis SET statement_timeout='60s';`
- Harus bisa dijalankan 2× tanpa error fatal.

### 4.3 `.env.web.example` (BARU, template saja)

```ini
SQLPAD_ADMIN_EMAIL=admin@lokal
SQLPAD_ADMIN_PASSWORD=GANTI_DENGAN_PASSWORD_KUAT
ANALIS_DB_PASSWORD=GANTI_DENGAN_PASSWORD_KUAT_BERBEDA
# Prod saja (jangan isi di dev):
# TUNNEL_TOKEN=
```

### 4.4 `DEPLOY-SERVER.md` (BARU, ≤1 halaman)

- Clone → `cp` env → isi secret → `up db-target` → `init_analis.sql` →
  `build` → `check` → `up --loop` → `up sqlpad+tunnel` → verifikasi.
- Sertakan perintah copy-paste + ekspektasi output tiap langkah + cara rollback
  (`down`, `down -v` hanya bila diminta eksplisit).

### 4.5 Batasan sumber daya (wajib di compose server)

```yaml
db-target: mem_limit: 1g
backbone-client: mem_limit: 2g
sqlpad: mem_limit: 512m
cloudflared/quick-tunnel: mem_limit: 128m
```

---

## 5. Matriks keputusan (jangan diperdebatkan ulang kecuali ada data baru)

| Keputusan | Alternatif ditolak | Pemicu revisi |
|---|---|---|
| Postgres, bukan MySQL | MySQL butuh prefix + rewrite query, image lebih berat, tanpa keunggulan BigQuery | Kebijakan kantor mewajibkan MySQL (maka estimasi +2 hari rewrite) |
| SQLPad, bukan Metabase | Metabase butuh ≥1 GB RAM, overkill untuk query+ekspor | Butuh dashboard self-service (maka tambah Metabase terpisah) |
| `--loop`, bukan `serve`+Prefect | Prefect Server +1 PG +400 MB RAM untuk nilai observabilitas kecil | Butuh UI run/ retry visual multi-user |
| Quick (dev) → Named (prod) | `nginx + certbot` di server (perlu IP publik + buka port 80/443) | Diskominfo menolak CNAME (maka fallback VPN/Tailscale) |

---

## 6. Risiko & mitigasi

| Risiko | Dampak | Mitigasi |
|---|---|---|
| Pull tanggal 4 gagal (jaringan/API) | Data telat 1 bulan | `--loop` retry `SCHEDULE_RETRY_COUNT=3 × 4 jam` otomatis; pantau `pull_failures`; `--retry-failed` manual |
| Quick Tunnel URL berubah / lambat | Link dev mati | Ekspektasi dev saja; prod pakai Named Tunnel |
| Kredensial bocor via git | Kritis | `chmod 600`, cek `git status` tiap fase, rotasi password bila ragu |
| Query berat analis melumpuhkan DB | ETL ikut lambat | Role `analis` + `statement_timeout` + row limit SQLPad; jadwalkan analisis di luar jam pull |
| Disk penuh (volume pgdata) | DB stop | Monitor `df -h` + `pg_database_size`; backup + prune sebelum tanggal 4 |

---

## 7. Perintah rujukan cepat (copy-paste)

```bash
# Validasi & DB dev
docker compose -f docker-compose.app.yml config
docker compose -f docker-compose.app.yml up -d db-target
docker compose -f docker-compose.app.yml run --rm --entrypoint python backbone-client -m backbone_pull.check

# Web dev (Quick Tunnel)
docker compose -f docker-compose.app.yml -f docker-compose.web.yml --profile web up -d sqlpad quick-tunnel
docker compose -f docker-compose.app.yml -f docker-compose.web.yml logs -f quick-tunnel

# Operasional umum
docker compose -f docker-compose.app.yml exec db-target psql -U backbone -d backbone_client -c "SELECT tbl_name,status,rows_received FROM sync.pull_log ORDER BY run_started_at DESC LIMIT 20;"
docker compose -f docker-compose.app.yml exec db-target psql -U backbone -d backbone_client -c "SELECT count(*) FROM sync.pull_failures;"
```

---

## 8. Definisi selesai (checklist serah terima)

- [ ] Repo terkomit sesuai §4 (tanpa secret), `DEPLOY-SERVER.md` valid diikuti dari clone bersih.
- [ ] Dev: login SQLPad via Quick Tunnel + query + ekspor CSV terbukti (screenshot/log).
- [ ] Server: `--loop` menunggu `2026-10-04`, pull sukses (`pull_log ok`, `failures 0`).
- [ ] Domain prod TLS valid, wajib login, backup + uji restore ada.
- [ ] Runbook + daftar password (via kanal aman, bukan repo) diserahkan ke penanggung jawab.

---

*Catatan untuk pelaksana: bila ada konflik antara dokumen ini dan `README.md`,
ikuti `masterplan.md` untuk urusan deploy/server, ikuti `README.md` untuk
detail ETL. Bila menemukan perbedaan perilaku API (mis. jadwal tidak lagi
tanggal 4), hentikan fase dan laporkan — jangan mengubah logika `backbone_pull/`.*
