from datetime import datetime
from typing import List, Optional

from sqlalchemy import create_engine, text, URL

from .base import DatabaseAdapter


class SqlServerAdapter(DatabaseAdapter):
    def __init__(self, settings):
        super().__init__(settings)
        self._build_engines()

    def _build_engines(self):
        s = self.settings
        self._engine = create_engine(
            URL.create("mssql+pymssql", username=s.db_user, password=s.db_password,
                       host=s.db_host, port=s.db_port, database=s.db_name),
            pool_size=10, max_overflow=10,
        )
        self._master = create_engine(
            URL.create("mssql+pymssql", username=s.db_user, password=s.db_password,
                       host=s.db_host, port=s.db_port, database="master"),
            pool_size=1, max_overflow=0,
        )

    @property
    def engine(self):
        return self._engine

    def quote(self, ident: str) -> str:
        return f"[{ident}]"

    def now_expr(self) -> str:
        return "GETDATE()"

    def is_unbounded(self, col_type: str) -> bool:
        return "(MAX)" in (col_type or "")

    def build_col_type(self, col: dict) -> str:
        type_name = (col.get("type_name") or "nvarchar").lower().strip()
        length = col.get("type_length")
        precision = col.get("type_precision")
        scale = col.get("type_scale")
        if type_name in ("nvarchar", "varchar"):
            if length is None:
                return f"{type_name.upper()}(500)"
            if int(length) == -1:
                return f"{type_name.upper()}(MAX)"
            return f"{type_name.upper()}({max(1, int(length))})"
        if type_name in ("char", "nchar"):
            fallback = "NVARCHAR" if type_name == "nchar" else "VARCHAR"
            if length is None or int(length) == -1:
                return f"{fallback}(500)" if length is None else f"{fallback}(MAX)"
            return f"{type_name.upper()}({max(1, int(length))})"
        if type_name in ("decimal", "numeric"):
            p = int(precision) if precision else 18
            s = int(scale) if scale else 0
            return f"{type_name.upper()}({p},{s})"
        if type_name in ("float", "real"):
            return type_name.upper()
        if type_name in ("int", "bigint", "smallint", "tinyint", "bit"):
            return type_name.upper()
        if type_name in ("datetime", "datetime2", "date", "time", "smalldatetime"):
            return type_name.upper()
        if type_name == "uniqueidentifier":
            return "UNIQUEIDENTIFIER"
        if type_name in ("binary", "varbinary"):
            if length is None or int(length) == -1:
                return f"{type_name.upper()}(MAX)"
            return f"{type_name.upper()}({int(length)})"
        if type_name in ("text", "ntext"):
            return "NVARCHAR(MAX)"
        return "NVARCHAR(MAX)"

    def build_create_table_sql(self, tbl_name, col_defs, pk_columns, schema_name) -> str:
        col_type_map, cols_ddl = {}, []
        for col in col_defs:
            ct = self.build_col_type(col)
            col_type_map[col["name"]] = ct
            nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
            cols_ddl.append(f"    [{col['name']}] {ct} {nullable}")
        valid_pk = [pk for pk in pk_columns if not self.is_unbounded(col_type_map.get(pk, "(MAX)"))]
        pk_def = ""
        if valid_pk:
            pk_cols = ", ".join(f"[{pk}]" for pk in valid_pk)
            pk_def = f",\n    CONSTRAINT [PK_{tbl_name}] PRIMARY KEY ({pk_cols})"
        cols_block = ",\n".join(cols_ddl)
        return (
            f"IF OBJECT_ID('[{schema_name}].[{tbl_name}]', 'U') IS NULL\n"
            f"CREATE TABLE [{schema_name}].[{tbl_name}] (\n{cols_block}{pk_def}\n);"
        )

    def build_add_column_sql(self, tbl_name, col, schema_name) -> str:
        return (
            f"ALTER TABLE [{schema_name}].[{tbl_name}] "
            f"ADD [{col['name']}] {self.build_col_type(col)} NULL;"
        )

    def build_upsert_sql(self, tbl_name, cols, key_cols, schema_name, col_to_param) -> str:
        non_pk = [c for c in cols if c not in key_cols]
        src_cols = ", ".join(f":{col_to_param[c]} AS [{c}]" for c in cols)
        match_cond = " AND ".join(f"t.[{k}] = s.[{k}]" for k in key_cols)
        update_set = ", ".join(f"t.[{c}] = s.[{c}]" for c in non_pk)
        insert_cols = ", ".join(f"[{c}]" for c in cols)
        insert_vals = ", ".join(f"s.[{c}]" for c in cols)
        matched_clause = (
            f"WHEN MATCHED\n        THEN UPDATE SET {update_set}\n"
            if non_pk else ""
        )
        return (
            f"MERGE [{schema_name}].[{tbl_name}] AS t\n"
            f"USING (SELECT {src_cols}) AS s\n    ON {match_cond}\n"
            f"{matched_clause}"
            f"WHEN NOT MATCHED\n        THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
        )

    def build_insert_sql(self, tbl_name, cols, schema_name, col_to_param) -> str:
        insert_cols = ", ".join(f"[{c}]" for c in cols)
        vals = ", ".join(f":{col_to_param[c]}" for c in cols)
        return f"INSERT INTO [{schema_name}].[{tbl_name}] ({insert_cols}) VALUES ({vals});"

    def existing_columns(self, conn, schema_name, tbl_name) -> set:
        return {
            row[0].lower()
            for row in conn.execute(text(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t"
            ), {"s": schema_name, "t": tbl_name}).fetchall()
        }

    def ensure_database(self):
        if not self.settings.db_auto_create_database:
            return
        db = self.settings.db_name
        with self._master.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(
                f"IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'{db}') "
                f"CREATE DATABASE [{db}];"
            ))

    def ensure_schema(self, name: str):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{name}') "
                f"EXEC('CREATE SCHEMA [{name}]');"
            ))

    def ensure_checkpoint_table(self):
        self.ensure_schema(self.schema_ctrl)
        with self._engine.begin() as conn:
            conn.execute(text(
                f"IF OBJECT_ID('[{self.schema_ctrl}].pull_checkpoint', 'U') IS NULL "
                f"CREATE TABLE [{self.schema_ctrl}].pull_checkpoint ("
                "  tbl_name NVARCHAR(100) NOT NULL PRIMARY KEY,"
                "  last_update DATETIME2 NOT NULL DEFAULT '1970-01-01',"
                "  total_rows BIGINT NOT NULL DEFAULT 0,"
                "  pulled_at DATETIME2 NOT NULL DEFAULT GETDATE());"
            ))

    def get_last_update(self, tbl_name: str) -> Optional[str]:
        with self._engine.connect() as conn:
            row = conn.execute(text(
                f"SELECT last_update FROM [{self.schema_ctrl}].pull_checkpoint WHERE tbl_name = :t"
            ), {"t": tbl_name}).fetchone()
        return str(row[0].date()) if row and row[0] else None

    def set_last_update(self, tbl_name: str, ts: datetime):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"MERGE [{self.schema_ctrl}].pull_checkpoint AS t "
                "USING (SELECT :tbl AS tbl_name, :ts AS last_update) AS s "
                "ON t.tbl_name = s.tbl_name "
                "WHEN MATCHED THEN UPDATE SET last_update = s.last_update, pulled_at = GETDATE() "
                "WHEN NOT MATCHED THEN INSERT (tbl_name, last_update) VALUES (s.tbl_name, s.last_update);"
            ), {"tbl": tbl_name, "ts": ts})

    def add_checkpoint_count(self, tbl_name: str, count: int):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"UPDATE [{self.schema_ctrl}].pull_checkpoint "
                "SET total_rows = total_rows + :c, pulled_at = GETDATE() WHERE tbl_name = :t"
            ), {"c": count, "t": tbl_name})

    def fetch_existing_npsn(self, schema_name: str, tbl_name: str) -> List[str]:
        with self._engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT npsn FROM [{schema_name}].[{tbl_name}] WHERE npsn IS NOT NULL"
            )).fetchall()
        return [r[0] for r in rows if r[0]]

    # ── dead-letter ─────────────────────────────────────────────────────────
    def ensure_failures_table(self):
        self.ensure_schema(self.schema_ctrl)
        with self._engine.begin() as conn:
            conn.execute(text(
                f"IF OBJECT_ID('[{self.schema_ctrl}].pull_failures', 'U') IS NULL "
                f"CREATE TABLE [{self.schema_ctrl}].pull_failures ("
                "  tbl_name NVARCHAR(100) NOT NULL,"
                "  param_type NVARCHAR(20) NOT NULL,"
                "  entity_id NVARCHAR(50) NOT NULL,"
                "  reason NVARCHAR(20) NOT NULL,"
                "  detail NVARCHAR(500) NULL,"
                "  expected BIGINT NULL,"
                "  received BIGINT NULL,"
                "  attempts INT NOT NULL DEFAULT 1,"
                "  failed_at DATETIME2 NOT NULL DEFAULT GETDATE(),"
                "  CONSTRAINT PK_pull_failures PRIMARY KEY (tbl_name, param_type, entity_id));"
            ))

    def record_failure(self, tbl_name, param_type, entity_id, reason, detail, expected, received):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"MERGE [{self.schema_ctrl}].pull_failures AS t "
                "USING (SELECT :tbl AS tbl_name, :pt AS param_type, :ent AS entity_id) AS s "
                "ON t.tbl_name = s.tbl_name AND t.param_type = s.param_type AND t.entity_id = s.entity_id "
                "WHEN MATCHED THEN UPDATE SET reason = :reason, detail = :detail, "
                "expected = :expected, received = :received, attempts = t.attempts + 1, failed_at = GETDATE() "
                "WHEN NOT MATCHED THEN INSERT (tbl_name, param_type, entity_id, reason, detail, expected, received) "
                "VALUES (:tbl, :pt, :ent, :reason, :detail, :expected, :received);"
            ), {"tbl": tbl_name, "pt": param_type, "ent": entity_id or "",
                "reason": reason, "detail": (detail or "")[:500],
                "expected": expected, "received": received})

    def clear_failure(self, tbl_name, param_type, entity_id):
        with self._engine.begin() as conn:
            conn.execute(text(
                f"DELETE FROM [{self.schema_ctrl}].pull_failures "
                "WHERE tbl_name = :tbl AND param_type = :pt AND entity_id = :ent"
            ), {"tbl": tbl_name, "pt": param_type, "ent": entity_id or ""})

    def list_failures(self) -> List[dict]:
        with self._engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT tbl_name, param_type, entity_id, reason, detail, expected, received, attempts "
                f"FROM [{self.schema_ctrl}].pull_failures"
            )).mappings().all()
        return [dict(r) for r in rows]

    def count_failures(self) -> int:
        with self._engine.connect() as conn:
            return conn.execute(text(
                f"SELECT COUNT(*) FROM [{self.schema_ctrl}].pull_failures"
            )).scalar() or 0

    # ── log aktivitas penarikan ──────────────────────────────────────────────
    def ensure_pull_log_table(self):
        self.ensure_schema(self.schema_ctrl)
        with self._engine.begin() as conn:
            conn.execute(text(
                f"IF OBJECT_ID('[{self.schema_ctrl}].pull_log', 'U') IS NULL "
                f"CREATE TABLE [{self.schema_ctrl}].pull_log ("
                "  id BIGINT IDENTITY(1,1) PRIMARY KEY,"
                "  tbl_name NVARCHAR(100) NOT NULL,"
                "  param_type NVARCHAR(20) NOT NULL,"
                "  run_started_at DATETIME2 NOT NULL,"
                "  run_finished_at DATETIME2 NOT NULL,"
                "  duration_seconds FLOAT NOT NULL,"
                "  rows_received BIGINT NOT NULL DEFAULT 0,"
                "  entities_total INT NOT NULL DEFAULT 0,"
                "  entities_failed INT NOT NULL DEFAULT 0,"
                "  status NVARCHAR(20) NOT NULL);"
            ))

    def log_pull_summary(self, tbl_name, param_type, started_at, finished_at,
                         rows_received, entities_total, entities_failed):
        duration = (finished_at - started_at).total_seconds()
        status = "ok" if entities_failed == 0 else "incomplete"
        with self._engine.begin() as conn:
            conn.execute(text(
                f"INSERT INTO [{self.schema_ctrl}].pull_log "
                "(tbl_name, param_type, run_started_at, run_finished_at, duration_seconds, "
                " rows_received, entities_total, entities_failed, status) "
                "VALUES (:tbl, :pt, :started, :finished, :dur, :rows, :etotal, :efail, :status)"
            ), {"tbl": tbl_name, "pt": param_type, "started": started_at, "finished": finished_at,
                "dur": duration, "rows": rows_received, "etotal": entities_total,
                "efail": entities_failed, "status": status})

    def purge_old_pull_log(self, retention_days: int) -> int:
        with self._engine.begin() as conn:
            result = conn.execute(text(
                f"DELETE FROM [{self.schema_ctrl}].pull_log "
                "WHERE run_started_at < DATEADD(day, :neg_days, GETDATE())"
            ), {"neg_days": -retention_days})
            return result.rowcount or 0

    def update_pull_log(self, tbl_name, param_type, run_started_at, rows_received, entities_failed):
        status = "ok" if entities_failed == 0 else "incomplete"
        with self._engine.begin() as conn:
            conn.execute(text(
                f"UPDATE [{self.schema_ctrl}].pull_log "
                "SET rows_received = :rows, entities_failed = :efail, status = :status "
                "WHERE tbl_name = :tbl AND param_type = :pt AND run_started_at = :started"
            ), {"rows": rows_received, "efail": entities_failed, "status": status,
                "tbl": tbl_name, "pt": param_type, "started": run_started_at})
