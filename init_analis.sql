-- Role read-only untuk webapp (SQLPad) — SEMUA admin manusia boleh baca
-- semua data, tapi query box tidak boleh bisa DROP/DELETE/INSERT.
-- Dijalankan sebagai user `backbone` (owner). Idempoten: aman di-run berkali-kali,
-- termasuk SEBELUM pull tanggal 4 (schema dbo/ref/dst. belum ada).
--
-- Password disubstitusi saat eksekusi dari $ANALIS_DB_PASSWORD (lihat DEPLOY-SERVER.md),
-- JANGAN tulis password asli di file ini.
--
--   docker compose -f docker-compose.app.yml exec -T db-target psql -U backbone \
--     -d backbone_client -v analis_pw="$ANALIS_DB_PASSWORD" -f - < init_analis.sql

-- 1. Buat role bila belum ada (dummy password, langsung dioverride di langkah 2).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'analis') THEN
    CREATE ROLE analis LOGIN PASSWORD 'belum-diisi-jalankan-alter-berikutnya';
  END IF;
END
$$;

-- 2. Set password asli (unconditional, selalu sinkron dengan env).
ALTER ROLE analis WITH LOGIN PASSWORD :'analis_pw';

-- 3. Hak baca: hanya untuk schema yang SUDAH ada (dbo/ref/dst. baru ada setelah pull).
DO $$
DECLARE
  s text;
BEGIN
  EXECUTE 'GRANT CONNECT ON DATABASE backbone_client TO analis';
  FOREACH s IN ARRAY ARRAY['public','dbo','ref','vld','datamart','sync'] LOOP
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = s) THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO analis', s);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO analis', s);
      EXECUTE format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE backbone IN SCHEMA %I GRANT SELECT ON TABLES TO analis', s);
    END IF;
  END LOOP;
END
$$;

-- 4. Batasi query analis agar satu query berat tidak melumpuhkan ETL.
ALTER ROLE analis SET statement_timeout = '60s';
