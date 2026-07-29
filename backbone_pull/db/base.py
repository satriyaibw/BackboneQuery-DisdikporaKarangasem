import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)


class DatabaseAdapter(ABC):
    """Antarmuka adapter database + orkestrasi bersama lintas dialect."""

    def __init__(self, settings):
        self.settings = settings
        self.schema_ctrl = "sync"

    # ── dialect primitives (abstrak) ───────────────────────────────────────
    @property
    @abstractmethod
    def engine(self): ...

    @abstractmethod
    def quote(self, ident: str) -> str: ...

    @abstractmethod
    def now_expr(self) -> str: ...

    @abstractmethod
    def is_unbounded(self, col_type: str) -> bool: ...

    @abstractmethod
    def build_col_type(self, col: dict) -> str: ...

    @abstractmethod
    def build_create_table_sql(self, tbl_name, col_defs, pk_columns, schema_name) -> str: ...

    @abstractmethod
    def build_add_column_sql(self, tbl_name, col, schema_name) -> str: ...

    @abstractmethod
    def build_upsert_sql(self, tbl_name, cols, key_cols, schema_name, col_to_param) -> str: ...

    @abstractmethod
    def build_insert_sql(self, tbl_name, cols, schema_name, col_to_param) -> str: ...

    @abstractmethod
    def existing_columns(self, conn, schema_name, tbl_name) -> set:
        """Kembalikan set nama kolom (huruf kecil) yang sudah ada di tabel."""
        ...

    @abstractmethod
    def ensure_database(self): ...

    @abstractmethod
    def ensure_schema(self, name: str): ...

    @abstractmethod
    def ensure_checkpoint_table(self): ...

    @abstractmethod
    def get_last_update(self, tbl_name: str) -> Optional[str]: ...

    @abstractmethod
    def set_last_update(self, tbl_name: str, ts: datetime): ...

    @abstractmethod
    def add_checkpoint_count(self, tbl_name: str, count: int): ...

    @abstractmethod
    def fetch_existing_npsn(self, schema_name: str, tbl_name: str) -> List[str]: ...

    # ── konversi nilai (bersama) ───────────────────────────────────────────
    @staticmethod
    def coerce_value(value, type_name: str):
        if value is None:
            return None
        tn = (type_name or "").lower().strip()
        if tn in ("datetime", "datetime2", "date", "smalldatetime"):
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value)
                except (ValueError, TypeError):
                    logger.debug("coerce_value: gagal konversi %r ke tipe '%s', pakai nilai asli", value, tn)
            return value
        if tn in ("int", "bigint", "smallint", "tinyint", "bit"):
            if isinstance(value, str):
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    logger.debug("coerce_value: gagal konversi %r ke tipe '%s', pakai nilai asli", value, tn)
            return value
        if tn in ("decimal", "numeric", "float", "real", "money", "smallmoney"):
            if isinstance(value, str):
                try:
                    return float(value)
                except (ValueError, TypeError):
                    logger.debug("coerce_value: gagal konversi %r ke tipe '%s', pakai nilai asli", value, tn)
            return value
        return value

    # ── orkestrasi bersama ─────────────────────────────────────────────────
    def ensure_table(self, tbl_name, col_defs, pk_columns, schema_name):
        create_sql = self.build_create_table_sql(tbl_name, col_defs, pk_columns, schema_name)
        with self.engine.begin() as conn:
            conn.execute(text(create_sql))
            existing = self.existing_columns(conn, schema_name, tbl_name)
            for col in col_defs:
                if col["name"].lower() not in existing:
                    conn.execute(text(self.build_add_column_sql(tbl_name, col, schema_name)))

    def upsert_rows(self, tbl_name, rows, meta) -> int:
        if not rows:
            return 0
        schema_name = meta.get("schema_name", "dbo")
        pk_columns = meta.get("pk_columns", [])
        col_defs = meta.get("col_defs", [])
        meta_cols = {c["name"] for c in col_defs}
        type_map = {c["name"]: (c.get("type_name") or "") for c in col_defs}
        col_type_map = {c["name"]: self.build_col_type(c) for c in col_defs}

        cols = [c for c in rows[0].keys() if c in meta_cols]
        if not cols:
            return 0

        # kunci = PK yang bertipe bounded & memang ada di payload
        key_cols = [
            pk for pk in pk_columns
            if pk in cols and not self.is_unbounded(col_type_map.get(pk, ""))
        ]
        params = [f"p{i}" for i in range(len(cols))]
        col_to_param = {c: params[i] for i, c in enumerate(cols)}

        if key_cols:
            sql = self.build_upsert_sql(tbl_name, cols, key_cols, schema_name, col_to_param)
        else:
            sql = self.build_insert_sql(tbl_name, cols, schema_name, col_to_param)

        clean_rows = [
            {col_to_param[c]: self.coerce_value(row.get(c), type_map.get(c, "")) for c in cols}
            for row in rows
        ]
        with self.engine.begin() as conn:
            conn.execute(text(sql), clean_rows)
        return len(rows)
