# DEPLOY SERVER KANTOR — clone-to-run (1 halaman)

> Prasyarat server: Docker + compose plugin, RAM ≥ 4 GB (8 GB recommended),
> akses internet. Semua secret via file 600, TIDAK PERNAH dikomit.

## 1. Clone & isi secret (prod: password BARU, bukan copy dari dev)

```bash
git clone <repo-privat> backbone-client && cd backbone-client
cp .env.example .env && cp .env.web.example .env.web && chmod 600 .env .env.web
# Isi .env: DB_PASSWORD=<openssl rand -base64 24 | tr -dc A-Za-z0-9 | head -c 20>,
#   BACKBONE_USERNAME/PASSWORD/API_KEY (via kanal aman, sama seperti dev).
# Isi .env.web: SQLPAD_ADMIN_EMAIL/PASSWORD + ANALIS_DB_PASSWORD (ketiganya beda).
```

## 2. DB + role read-only + verifikasi ETL

```bash
docker compose -f docker-compose.app.yml up -d db-target  # tunggu healthy
set -a; source .env.web; set +a  # ambil ANALIS_DB_PASSWORD (jangan echo)
docker compose -f docker-compose.app.yml exec -T db-target psql -U backbone -d backbone_client \
  -v analis_pw="$ANALIS_DB_PASSWORD" -f - < init_analis.sql
docker compose -f docker-compose.app.yml build backbone-client
docker compose -f docker-compose.app.yml run --rm --entrypoint python backbone-client -m backbone_pull.check
# Ekspektasi di luar tanggal 4: DB OK + Token OK + API 400 (jadwal) = LULUS.
```

## 3. ETL terjadwal (`--loop`, auto tanggal 4 dari API)

```bash
# ETL terjadwal via override --loop (jangan edit main.py):
docker compose -f docker-compose.app.yml -f docker-compose.loop.yml up -d backbone-client
docker compose -f docker-compose.app.yml logs -f backbone-client
# Harus memuat: "Jadwal otomatis dari Backbone: tanggal [4]" + "Menunggu... 2026-10-04".
```

## 4. Webapp (dev: Quick Tunnel / prod: Named Tunnel + domain Diskominfo)

```bash
docker compose -f docker-compose.app.yml -f docker-compose.web.yml --profile web up -d sqlpad quick-tunnel
docker compose -f docker-compose.app.yml -f docker-compose.web.yml logs -f quick-tunnel
# → buka URL https://xxx.trycloudflare.com → login SQLPad admin.
# Prod: ganti quick-tunnel → cloudflared (profile `tunnel`, TUNNEL_TOKEN di .env.web),
#   minta Diskominfo: CNAME <host> → <tunnel-id>.cfargotunnel.com.
```

Koneksi SQLPad (sekali saja, via UI: Connections → New):
`Driver PostgreSQL, Host db-target, Port 5432, Database backbone_client,
Username analis, Password = ANALIS_DB_PASSWORD` → Test → Save.
Uji: `SELECT * FROM sync.pull_log ORDER BY run_started_at DESC LIMIT 20` → Run → Download CSV.

## 5. Backup & darurat

```bash
docker compose -f docker-compose.app.yml exec db-target pg_dump -U backbone -d backbone_client -Fc -f /tmp/bb_$(date +%F).dump
docker cp backbone-client-db-target-1:/tmp/bb_$(date +%F).dump ./backups/  # retensi 14 hari
# Darurat: matikan publik → docker compose -f docker-compose.app.yml -f docker-compose.web.yml --profile web down quick-tunnel
# Rollback total (HATI-HATI, hapus data): docker compose -f docker-compose.app.yml down -v
```

## 6. Cek kesehatan rutin

```bash
docker compose -f docker-compose.app.yml exec db-target psql -U backbone -d backbone_client \
  -c "SELECT tbl_name,status,rows_received FROM sync.pull_log ORDER BY run_started_at DESC LIMIT 20;" \
  -c "SELECT count(*) FROM sync.pull_failures;"
```
