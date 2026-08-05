from datetime import datetime
from typing import List, Optional

from sqlalchemy import create_engine, text, URL

from .base import DatabaseAdapter


class PostgresAdapter(DatabaseAdapter):
    def __init__(self, settings):
        super().__init__(settings)
        self._build_engines()

    def _build_engines(self):
        s = self.settings
        self._engine = create_engine(
            URL.create("postgresql+psycopg2", username=s.db_user, password=s.db_password,
                       host=s.db_host, port=s.db_port, database=s.db_name),
            pool_size=10, max_overflow=10,
        )
        self._maint = create_engine(
            URL.create("postgresql+psycopg2", username=s.db_user, password=s.db_password,
                       host=s.db_host, port=s.db_port, database=s.db_maintenance_db),
            pool_size=1, max_overflow=0,
        )

    @property
    def engine(self):
        return self._engine

    def quote(self, ident: str) -> str:
        return f'"{ident}"'

    def now_expr(self) -> str:
        return "NOW()"

    def is_unbounded(self, col_type: str) -> bool:
        ct = (col_type or "").lower()
        return ct in ("text", "bytea")

    def build_col_type(self, col: dict) -> str:
        type_name = (col.get("type_name") or "varchar").lower().strip()
        length = col.get("type_length")
        precision = col.get("type_precision")
        scale = col.get("type_scale")
        if type_name in ("nvarchar", "varchar"):
            if length is None:
                return "varchar(500)"
            if int(length) == -1:
                return "text"
            return f"varchar({max(1, int(length))})"
        if type_name in ("char", "nchar"):
            if length is None:
                return "varchar(500)"
            if int(length) == -1:
                return "text"
            return f"char({max(1, int(length))})"
        if type_name in ("decimal", "numeric"):
            p = int(precision) if precision else 18
            s = int(scale) if scale else 0
            return f"numeric({p},{s})"
        if type_name == "float":
            return "double precision"
        if type_name == "real":
            return "real"
        if type_name in ("int", "integer"):
            return "integer"
        if type_name == "bigint":
            return "bigint"
        if type_name in ("smallint", "tinyint", "bit"):
            return "smallint"
        if type_name in ("datetime", "datetime2", "smalldatetime"):
            return "timestamp"
        if type_name == "date":
            return "date"
        if type_name == "time":
            return "time"
        if type_name == "uniqueidentifier":
            return "varchar(36)"
        if type_name in ("binary", "varbinary"):
            return "bytea"
        if type_name in ("text", "ntext"):
            return "text"
        return "text"

    def build_create_table_sql(self, tbl_name, col_defs, pk_columns, schema_name) -> str:
        col_type_map, cols_ddl = {}, []
        for col in col_defs:
            ct = self.build_col_type(col)
            col_type_map[col["name"]] = ct
            nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
            cols_ddl.append(f'    "{col["name"]}" {ct} {nullable}')
        valid_pk = [pk for pk in pk_columns if not self.is_unbounded(col_type_map.get(pk, "text"))]
        pk_def = ""
        if valid_pk:
            pk_cols = ", ".join(f'"{pk}"' for pk in valid_pk)
            pk_def = f',\n    CONSTRAINT "PK_{tbl_name}" PRIMARY KEY ({pk_cols})'
        cols_block = ",\n".join(cols_ddl)
        return (
            f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{tbl_name}" (\n'
            f"{cols_block}{pk_def}\n);"
        )

    def build_add_column_sql(self, tbl_name, col, schema_name) -> str:
        return (
            f'ALTER TABLE "{schema_name}"."{tbl_name}" '
            f'ADD COLUMN IF NOT EXISTS "{col["name"]}" {self.build_col_type(col)} NULL;'
        )

    def build_upsert_sql(self, tbl_name, cols, key_cols, schema_name, col_to_param) -> str:
        non_pk = [c for c in cols if c not in key_cols]
        insert_cols = ", ".join(f'"{c}"' for c in cols)
        vals = ", ".join(f":{col_to_param[c]}" for c in cols)
        conflict = ", ".join(f'"{k}"' for k in key_cols)
        if non_pk:
            update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk)
            action = f"DO UPDATE SET {update_set}"
        else:
            action = "DO NOTHING"
        return (
            f'INSERT INTO "{schema_name}"."{tbl_name}" ({insert_cols})\n'
            f"VALUES ({vals})\n"
            f"ON CONFLICT ({conflict}) {action};"
        )

    def build_insert_sql(self, tbl_name, cols, schema_name, col_to_param) -> str:
        insert_cols = ", ".join(f'"{c}"' for c in cols)
        vals = ", ".join(f":{col_to_param[c]}" for c in cols)
        return f'INSERT INTO "{schema_name}"."{tbl_name}" ({insert_cols}) VALUES ({vals});'

    def existing_columns(self, conn, schema_name, tbl_name) -> set:
        return {
            row[0].lower()
            for row in conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = :t"
            ), {"s": schema_name, "t": tbl_name}).fetchall()
        }

    def ensure_database(self):
        if not self.settings.db_auto_create_database:
            return
        db = self.settings.db_name
        with self._maint.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            exists = conn.execute(text(
                "SELECT 1 FROM pg_database WHERE datname = :d"
            ), {"d": db}).fetchone()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{db}"'))

    def ensure_schema(self, name: str):
        with self._engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{name}"'))

    def ensure_checkpoint_table(self):
        self.ensure_schema(self.schema_ctrl)
        with self._engine.begin() as conn:
            conn.execute(text(
                f'CREATE TABLE IF NOT EXISTS "{self.schema_ctrl}"."pull_checkpoint" ('
                '  tbl_name varchar(100) NOT NULL PRIMARY KEY,'
                "  last_update timestamp NOT NULL DEFAULT '1970-01-01',"
                '  total_rows bigint NOT NULL DEFAULT 0,'
                '  pulled_at timestamp NOT NULL DEFAULT NOW());'
            ))

    def get_last_update(self, tbl_name: str) -> Optional[str]:
        with self._engine.connect() as conn:
            row = conn.execute(text(
                f'SELECT last_update FROM "{self.schema_ctrl}"."pull_checkpoint" WHERE tbl_name = :t'
            ), {"t": tbl_name}).fetchone()
        return str(row[0].date()) if row and row[0] else None

    def set_last_update(self, tbl_name: str, ts: datetime):
        with self._engine.begin() as conn:
            conn.execute(text(
                f'INSERT INTO "{self.schema_ctrl}"."pull_checkpoint" (tbl_name, last_update) '
                "VALUES (:tbl, :ts) "
                "ON CONFLICT (tbl_name) DO UPDATE SET last_update = EXCLUDED.last_update, pulled_at = NOW()"
            ), {"tbl": tbl_name, "ts": ts})

    def add_checkpoint_count(self, tbl_name: str, count: int):
        with self._engine.begin() as conn:
            conn.execute(text(
                f'UPDATE "{self.schema_ctrl}"."pull_checkpoint" '
                "SET total_rows = total_rows + :c, pulled_at = NOW() WHERE tbl_name = :t"
            ), {"c": count, "t": tbl_name})

    def fetch_existing_npsn(self, schema_name: str, tbl_name: str) -> List[str]:
        with self._engine.connect() as conn:
            rows = conn.execute(text(
                f'SELECT npsn FROM "{schema_name}"."{tbl_name}" WHERE npsn IS NOT NULL'
            )).fetchall()
        return [r[0] for r in rows if r[0]]

    # ── dead-letter ─────────────────────────────────────────────────────────
    def ensure_failures_table(self):
        self.ensure_schema(self.schema_ctrl)
        with self._engine.begin() as conn:
            conn.execute(text(
                f'CREATE TABLE IF NOT EXISTS "{self.schema_ctrl}"."pull_failures" ('
                '  tbl_name varchar(100) NOT NULL,'
                '  param_type varchar(20) NOT NULL,'
                '  entity_id varchar(50) NOT NULL,'
                '  reason varchar(20) NOT NULL,'
                '  detail varchar(500) NULL,'
                '  expected bigint NULL,'
                '  received bigint NULL,'
                '  attempts int NOT NULL DEFAULT 1,'
                '  failed_at timestamp NOT NULL DEFAULT NOW(),'
                '  CONSTRAINT "PK_pull_failures" PRIMARY KEY (tbl_name, param_type, entity_id));'
            ))

    def record_failure(self, tbl_name, param_type, entity_id, reason, detail, expected, received):
        with self._engine.begin() as conn:
            conn.execute(text(
                f'INSERT INTO "{self.schema_ctrl}"."pull_failures" '
                "(tbl_name, param_type, entity_id, reason, detail, expected, received) "
                "VALUES (:tbl, :pt, :ent, :reason, :detail, :expected, :received) "
                "ON CONFLICT (tbl_name, param_type, entity_id) DO UPDATE SET "
                "reason = EXCLUDED.reason, detail = EXCLUDED.detail, expected = EXCLUDED.expected, "
                'received = EXCLUDED.received, attempts = "pull_failures".attempts + 1, failed_at = NOW()'
            ), {"tbl": tbl_name, "pt": param_type, "ent": entity_id or "",
                "reason": reason, "detail": (detail or "")[:500],
                "expected": expected, "received": received})

    def clear_failure(self, tbl_name, param_type, entity_id):
        with self._engine.begin() as conn:
            conn.execute(text(
                f'DELETE FROM "{self.schema_ctrl}"."pull_failures" '
                "WHERE tbl_name = :tbl AND param_type = :pt AND entity_id = :ent"
            ), {"tbl": tbl_name, "pt": param_type, "ent": entity_id or ""})

    def list_failures(self) -> List[dict]:
        with self._engine.connect() as conn:
            rows = conn.execute(text(
                f'SELECT tbl_name, param_type, entity_id, reason, detail, expected, received, attempts '
                f'FROM "{self.schema_ctrl}"."pull_failures"'
            )).mappings().all()
        return [dict(r) for r in rows]

    def count_failures(self) -> int:
        with self._engine.connect() as conn:
            return conn.execute(text(
                f'SELECT COUNT(*) FROM "{self.schema_ctrl}"."pull_failures"'
            )).scalar() or 0

    # ── log aktivitas penarikan ──────────────────────────────────────────────
    def ensure_pull_log_table(self):
        self.ensure_schema(self.schema_ctrl)
        with self._engine.begin() as conn:
            conn.execute(text(
                f'CREATE TABLE IF NOT EXISTS "{self.schema_ctrl}"."pull_log" ('
                '  id bigserial PRIMARY KEY,'
                '  tbl_name varchar(100) NOT NULL,'
                '  param_type varchar(20) NOT NULL,'
                '  run_started_at timestamp NOT NULL,'
                '  run_finished_at timestamp NOT NULL,'
                '  duration_seconds double precision NOT NULL,'
                '  rows_received bigint NOT NULL DEFAULT 0,'
                '  entities_total int NOT NULL DEFAULT 0,'
                '  entities_failed int NOT NULL DEFAULT 0,'
                '  status varchar(20) NOT NULL);'
            ))

    def log_pull_summary(self, tbl_name, param_type, started_at, finished_at,
                         rows_received, entities_total, entities_failed):
        duration = (finished_at - started_at).total_seconds()
        status = "ok" if entities_failed == 0 else "incomplete"
        with self._engine.begin() as conn:
            conn.execute(text(
                f'INSERT INTO "{self.schema_ctrl}"."pull_log" '
                "(tbl_name, param_type, run_started_at, run_finished_at, duration_seconds, "
                " rows_received, entities_total, entities_failed, status) "
                "VALUES (:tbl, :pt, :started, :finished, :dur, :rows, :etotal, :efail, :status)"
            ), {"tbl": tbl_name, "pt": param_type, "started": started_at, "finished": finished_at,
                "dur": duration, "rows": rows_received, "etotal": entities_total,
                "efail": entities_failed, "status": status})

    def purge_old_pull_log(self, retention_days: int) -> int:
        with self._engine.begin() as conn:
            result = conn.execute(text(
                f'DELETE FROM "{self.schema_ctrl}"."pull_log" '
                "WHERE run_started_at < NOW() - make_interval(days => :days)"
            ), {"days": retention_days})
            return result.rowcount or 0

    def update_pull_log(self, tbl_name, param_type, run_started_at, rows_received, entities_failed):
        status = "ok" if entities_failed == 0 else "incomplete"
        with self._engine.begin() as conn:
            conn.execute(text(
                f'UPDATE "{self.schema_ctrl}"."pull_log" '
                "SET rows_received = :rows, entities_failed = :efail, status = :status "
                "WHERE tbl_name = :tbl AND param_type = :pt AND run_started_at = :started"
            ), {"rows": rows_received, "efail": entities_failed, "status": status,
                "tbl": tbl_name, "pt": param_type, "started": run_started_at})
