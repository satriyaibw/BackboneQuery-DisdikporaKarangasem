import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class DatabaseAdapter(ABC):

    def __init__(self, settings):
        self.settings = settings
        self.schema_ctrl = "sync"

    def __getstate__(self):
        return {k: v for k, v in self.__dict__.items() if not isinstance(v, Engine)}

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._build_engines()

    @abstractmethod
    def _build_engines(self):
        """Buat/bangun ulang engine SQLAlchemy dari self.settings. Dipanggil
        dari __init__ dan __setstate__ (setelah unpickle)."""
        ...

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

    # ── dead-letter: item penarikan yang gagal / tidak lengkap ─────────────
    @abstractmethod
    def ensure_failures_table(self): ...

    @abstractmethod
    def record_failure(self, tbl_name: str, param_type: str, entity_id: str,
                       reason: str, detail: str, expected, received): ...

    @abstractmethod
    def clear_failure(self, tbl_name: str, param_type: str, entity_id: str): ...

    @abstractmethod
    def list_failures(self) -> List[dict]: ...

    @abstractmethod
    def count_failures(self) -> int: ...

    # ── log aktivitas penarikan (1 baris per tabel per run) ────────────────
    @abstractmethod
    def ensure_pull_log_table(self): ...

    @abstractmethod
    def log_pull_summary(self, tbl_name: str, param_type: str, started_at: datetime,
                         finished_at: datetime, rows_received: int,
                         entities_total: int, entities_failed: int,
                         batch_id: Optional[str] = None,
                         request_id: Optional[str] = None): ...

    @abstractmethod
    def purge_old_pull_log(self, retention_days: int) -> int:
        """Hapus baris pull_log lebih tua dari retention_days; kembalikan jumlah baris dihapus."""
        ...

    @abstractmethod
    def update_latest_pull_log(self, tbl_name: str, param_type: str,
                               additional_rows_received: int, entities_failed: int):
        """Koreksi baris pull_log TERBARU untuk (tbl_name, param_type) -- dipakai
        retry_failed_only (main.py --retry-failed / auto-chain) yang berjalan di
        invocation TERPISAH dari run asal, jadi tidak punya run_started_at run
        asal untuk update_pull_log() biasa. additional_rows_received ditambahkan
        ke rows_received yang sudah ada (bukan menggantikan); entities_failed
        menggantikan nilai lama (jumlah akhir yang sebenarnya, sudah pasti)."""
        ...

    # ── info sesi request Backbone (satu baris per request_id) ─────────────
    @abstractmethod
    def ensure_pull_requests_table(self): ...

    @abstractmethod
    def upsert_pull_request(self, request_id: str, expired_date: Optional[datetime],
                            raw_info: str): ...

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
