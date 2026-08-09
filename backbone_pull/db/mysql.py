from datetime import datetime
from typing import List, Optional

from sqlalchemy import create_engine, text, URL

from .base import DatabaseAdapter


class MySQLAdapter(DatabaseAdapter):
    """Semua schema_name Backbone (dbo, ref, vld, datamart, ...) digabung ke
    satu database MySQL (DB_NAME), dengan nama tabel diberi prefix schema
    (mis. dbo.sekolah -> `dbo_sekolah`, vld.v_ptk -> `vld_v_ptk`) — MySQL
    tidak punya konsep schema-dalam-database seperti SQL Server/PostgreSQL."""

    def __init__(self, settings):
        super().__init__(settings)
        self._build_engines()

    def _build_engines(self):
        s = self.settings
        self._engine = create_engine(
            URL.create("mysql+pymysql", username=s.db_user, password=s.db_password,
                       host=s.db_host, port=s.db_port, database=s.db_name),
            pool_size=10, max_overflow=10,
        )
        self._maint = create_engine(
            URL.create("mysql+pymysql", username=s.db_user, password=s.db_password,
                       host=s.db_host, port=s.db_port),
            pool_size=1, max_overflow=0,
        )

    @property
    def engine(self):
        return self._engine

    def _tname(self, schema_name: str, tbl_name: str) -> str:
        return f"`{schema_name}_{tbl_name}`"

    def quote(self, ident: str) -> str:
        return f"`{ident}`"

    def now_expr(self) -> str:
        return "NOW()"

    def is_unbounded(self, col_type: str) -> bool:
        ct = (col_type or "").upper()
        return ct in ("TEXT", "LONGTEXT", "LONGBLOB")

    def build_col_type(self, col: dict) -> str:
        type_name = (col.get("type_name") or "varchar").lower().strip()
        length = col.get("type_length")
        precision = col.get("type_precision")
        scale = col.get("type_scale")
        if type_name in ("nvarchar", "varchar"):
            if length is None:
                return "VARCHAR(500)"
            if int(length) == -1:
                return "TEXT"
            return f"VARCHAR({max(1, int(length))})"
        if type_name in ("char", "nchar"):
            if length is None:
                return "VARCHAR(500)"
            if int(length) == -1:
                return "TEXT"
            return f"CHAR({max(1, int(length))})"
        if type_name in ("decimal", "numeric"):
            p = int(precision) if precision else 18
            s = int(scale) if scale else 0
            return f"DECIMAL({p},{s})"
        if type_name == "float":
            return "DOUBLE"
        if type_name == "real":
            return "FLOAT"
        if type_name in ("int", "integer"):
            return "INT"
        if type_name == "bigint":
            return "BIGINT"
        if type_name in ("smallint", "tinyint", "bit"):
            return "SMALLINT"
        if type_name in ("datetime", "datetime2", "smalldatetime"):
            return "DATETIME"
        if type_name == "date":
            return "DATE"
        if type_name == "time":
            return "TIME"
        if type_name == "uniqueidentifier":
            return "VARCHAR(36)"
        if type_name in ("binary", "varbinary"):
            if length is None or int(length) == -1:
                return "LONGBLOB"
            return f"VARBINARY({int(length)})"
        if type_name in ("text", "ntext"):
            return "TEXT"
        return "TEXT"

    def build_create_table_sql(self, tbl_name, col_defs, pk_columns, schema_name) -> str:
        col_type_map, cols_ddl = {}, []
        for col in col_defs:
            ct = self.build_col_type(col)
            col_type_map[col["name"]] = ct
            nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
            cols_ddl.append(f"    `{col['name']}` {ct} {nullable}")
        valid_pk = [pk for pk in pk_columns if not self.is_unbounded(col_type_map.get(pk, "TEXT"))]
        pk_def = ""
        if valid_pk:
            pk_cols = ", ".join(f"`{pk}`" for pk in valid_pk)
            pk_def = f",\n    PRIMARY KEY ({pk_cols})"
        cols_block = ",\n".join(cols_ddl)
        return (
            f"CREATE TABLE IF NOT EXISTS {self._tname(schema_name, tbl_name)} (\n"
            f"{cols_block}{pk_def}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        )

    def build_add_column_sql(self, tbl_name, col, schema_name) -> str:
        # MySQL (beda dengan MariaDB/Postgres) tidak mendukung ADD COLUMN IF NOT EXISTS;
        # aman karena ensure_table() di base.py sudah memfilter kolom yang sudah ada.
        return (
            f"ALTER TABLE {self._tname(schema_name, tbl_name)} "
            f"ADD COLUMN `{col['name']}` {self.build_col_type(col)} NULL;"
        )

    def build_upsert_sql(self, tbl_name, cols, key_cols, schema_name, col_to_param) -> str:
        non_pk = [c for c in cols if c not in key_cols]
        insert_cols = ", ".join(f"`{c}`" for c in cols)
        vals = ", ".join(f":{col_to_param[c]}" for c in cols)
        if non_pk:
            update_set = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in non_pk)
        else:
            update_set = f"`{key_cols[0]}`=`{key_cols[0]}`"
        return (
            f"INSERT INTO {self._tname(schema_name, tbl_name)} ({insert_cols})\n"
            f"VALUES ({vals})\n"
            f"ON DUPLICATE KEY UPDATE {update_set};"
        )

    def build_insert_sql(self, tbl_name, cols, schema_name, col_to_param) -> str:
        insert_cols = ", ".join(f"`{c}`" for c in cols)
        vals = ", ".join(f":{col_to_param[c]}" for c in cols)
        return f"INSERT INTO {self._tname(schema_name, tbl_name)} ({insert_cols}) VALUES ({vals});"

    def existing_columns(self, conn, schema_name, tbl_name) -> set:
        return {
            row[0].lower()
            for row in conn.execute(text(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t"
            ), {"t": f"{schema_name}_{tbl_name}"}).fetchall()
        }

    def ensure_database(self):
        if not self.settings.db_auto_create_database:
            return
        db = self.settings.db_name
        with self._maint.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db}` CHARACTER SET utf8mb4;"))

    def ensure_schema(self, name: str):
        # Semua schema_name digabung ke satu database (lihat docstring kelas) —
        # tidak ada DDL per-schema yang perlu dijalankan di MySQL.
        pass

    def ensure_checkpoint_table(self):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"CREATE TABLE IF NOT EXISTS {self._tname(self.schema_ctrl, 'pull_checkpoint')} ("
                "  tbl_name VARCHAR(100) NOT NULL PRIMARY KEY,"
                "  last_update DATETIME NOT NULL DEFAULT '1970-01-01',"
                "  total_rows BIGINT NOT NULL DEFAULT 0,"
                "  pulled_at DATETIME NOT NULL DEFAULT NOW()) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
            ))

    def get_last_update(self, tbl_name: str) -> Optional[str]:
        with self._engine.connect() as conn:
            row = conn.execute(text(
                f"SELECT last_update FROM {self._tname(self.schema_ctrl, 'pull_checkpoint')} WHERE tbl_name = :t"
            ), {"t": tbl_name}).fetchone()
        return str(row[0].date()) if row and row[0] else None

    def set_last_update(self, tbl_name: str, ts: datetime):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"INSERT INTO {self._tname(self.schema_ctrl, 'pull_checkpoint')} (tbl_name, last_update) "
                "VALUES (:tbl, :ts) "
                "ON DUPLICATE KEY UPDATE last_update = VALUES(last_update), pulled_at = NOW()"
            ), {"tbl": tbl_name, "ts": ts})

    def add_checkpoint_count(self, tbl_name: str, count: int):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"UPDATE {self._tname(self.schema_ctrl, 'pull_checkpoint')} "
                "SET total_rows = total_rows + :c, pulled_at = NOW() WHERE tbl_name = :t"
            ), {"c": count, "t": tbl_name})

    def fetch_existing_npsn(self, schema_name: str, tbl_name: str) -> List[str]:
        with self._engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT npsn FROM {self._tname(schema_name, tbl_name)} WHERE npsn IS NOT NULL"
            )).fetchall()
        return [r[0] for r in rows if r[0]]

    # ── dead-letter ─────────────────────────────────────────────────────────
    def ensure_failures_table(self):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"CREATE TABLE IF NOT EXISTS {self._tname(self.schema_ctrl, 'pull_failures')} ("
                "  tbl_name VARCHAR(100) NOT NULL,"
                "  param_type VARCHAR(20) NOT NULL,"
                "  entity_id VARCHAR(50) NOT NULL,"
                "  reason VARCHAR(20) NOT NULL,"
                "  detail VARCHAR(500) NULL,"
                "  expected BIGINT NULL,"
                "  received BIGINT NULL,"
                "  attempts INT NOT NULL DEFAULT 1,"
                "  failed_at DATETIME NOT NULL DEFAULT NOW(),"
                "  PRIMARY KEY (tbl_name, param_type, entity_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
            ))

    def record_failure(self, tbl_name, param_type, entity_id, reason, detail, expected, received):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"INSERT INTO {self._tname(self.schema_ctrl, 'pull_failures')} "
                "(tbl_name, param_type, entity_id, reason, detail, expected, received) "
                "VALUES (:tbl, :pt, :ent, :reason, :detail, :expected, :received) "
                "ON DUPLICATE KEY UPDATE "
                "reason = VALUES(reason), detail = VALUES(detail), expected = VALUES(expected), "
                "received = VALUES(received), attempts = attempts + 1, failed_at = NOW()"
            ), {"tbl": tbl_name, "pt": param_type, "ent": entity_id or "",
                "reason": reason, "detail": (detail or "")[:500],
                "expected": expected, "received": received})

    def clear_failure(self, tbl_name, param_type, entity_id):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"DELETE FROM {self._tname(self.schema_ctrl, 'pull_failures')} "
                "WHERE tbl_name = :tbl AND param_type = :pt AND entity_id = :ent"
            ), {"tbl": tbl_name, "pt": param_type, "ent": entity_id or ""})

    def list_failures(self) -> List[dict]:
        with self._engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT tbl_name, param_type, entity_id, reason, detail, expected, received, attempts "
                f"FROM {self._tname(self.schema_ctrl, 'pull_failures')}"
            )).mappings().all()
        return [dict(r) for r in rows]

    def count_failures(self) -> int:
        with self._engine.connect() as conn:
            return conn.execute(text(
                f"SELECT COUNT(*) FROM {self._tname(self.schema_ctrl, 'pull_failures')}"
            )).scalar() or 0

    # ── log aktivitas penarikan ──────────────────────────────────────────────
    def ensure_pull_log_table(self):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"CREATE TABLE IF NOT EXISTS {self._tname(self.schema_ctrl, 'pull_log')} ("
                "  id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                "  tbl_name VARCHAR(100) NOT NULL,"
                "  param_type VARCHAR(20) NOT NULL,"
                # DATETIME(6) (mikrodetik) — bukan DATETIME biasa — karena
                # update_pull_log() mencocokkan run_started_at persis (WHERE =)
                # dengan nilai Python datetime asli; presisi detik saja akan
                # membuat pencocokan gagal diam-diam dan status tak pernah
                # terkoreksi setelah retry.
                "  run_started_at DATETIME(6) NOT NULL,"
                "  run_finished_at DATETIME(6) NOT NULL,"
                "  duration_seconds DOUBLE NOT NULL,"
                "  rows_received BIGINT NOT NULL DEFAULT 0,"
                "  entities_total INT NOT NULL DEFAULT 0,"
                "  entities_failed INT NOT NULL DEFAULT 0,"
                "  status VARCHAR(20) NOT NULL,"
                "  batch_id VARCHAR(36) NULL,"
                "  request_id VARCHAR(100) NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
            ))
            # Migrasi tabel lama (dibuat sebelum kolom batch_id/request_id ada) --
            # MySQL tidak mendukung ADD COLUMN IF NOT EXISTS (beda dengan MariaDB/Postgres).
            existing = self.existing_columns(conn, self.schema_ctrl, "pull_log")
            for col in ({"name": "batch_id", "type_name": "nvarchar", "type_length": 36},
                       {"name": "request_id", "type_name": "nvarchar", "type_length": 100}):
                if col["name"].lower() not in existing:
                    conn.execute(text(self.build_add_column_sql("pull_log", col, self.schema_ctrl)))

    def log_pull_summary(self, tbl_name, param_type, started_at, finished_at,
                         rows_received, entities_total, entities_failed,
                         batch_id=None, request_id=None):
        duration = (finished_at - started_at).total_seconds()
        status = "ok" if entities_failed == 0 else "incomplete"
        with self._engine.begin() as conn:
            conn.execute(text(
                f"INSERT INTO {self._tname(self.schema_ctrl, 'pull_log')} "
                "(tbl_name, param_type, run_started_at, run_finished_at, duration_seconds, "
                " rows_received, entities_total, entities_failed, status, "
                " batch_id, request_id) "
                "VALUES (:tbl, :pt, :started, :finished, :dur, :rows, :etotal, :efail, :status, "
                " :batch, :reqid)"
            ), {"tbl": tbl_name, "pt": param_type, "started": started_at, "finished": finished_at,
                "dur": duration, "rows": rows_received, "etotal": entities_total,
                "efail": entities_failed, "status": status,
                "batch": batch_id, "reqid": request_id})

    # ── info sesi request Backbone (satu baris per request_id) ─────────────
    def ensure_pull_requests_table(self):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"CREATE TABLE IF NOT EXISTS {self._tname(self.schema_ctrl, 'pull_requests')} ("
                "  request_id VARCHAR(100) NOT NULL PRIMARY KEY,"
                "  expired_date DATETIME NULL,"
                "  raw_info TEXT NULL,"
                "  first_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "  last_used_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP) "
                "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
            ))

    def upsert_pull_request(self, request_id, expired_date, raw_info):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"INSERT INTO {self._tname(self.schema_ctrl, 'pull_requests')} "
                "(request_id, expired_date, raw_info) VALUES (:rid, :exp, :info) "
                "ON DUPLICATE KEY UPDATE expired_date=VALUES(expired_date), "
                "raw_info=VALUES(raw_info), last_used_at=CURRENT_TIMESTAMP"
            ), {"rid": request_id, "exp": expired_date, "info": raw_info})

    def purge_old_pull_log(self, retention_days: int) -> int:
        with self._engine.begin() as conn:
            result = conn.execute(text(
                f"DELETE FROM {self._tname(self.schema_ctrl, 'pull_log')} "
                "WHERE run_started_at < DATE_SUB(NOW(), INTERVAL :days DAY)"
            ), {"days": retention_days})
            return result.rowcount or 0

    def update_pull_log(self, tbl_name, param_type, run_started_at, rows_received, entities_failed):
        status = "ok" if entities_failed == 0 else "incomplete"
        with self._engine.begin() as conn:
            conn.execute(text(
                f"UPDATE {self._tname(self.schema_ctrl, 'pull_log')} "
                "SET rows_received = :rows, entities_failed = :efail, status = :status "
                "WHERE tbl_name = :tbl AND param_type = :pt AND run_started_at = :started"
            ), {"rows": rows_received, "efail": entities_failed, "status": status,
                "tbl": tbl_name, "pt": param_type, "started": run_started_at})
