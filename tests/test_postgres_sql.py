from types import SimpleNamespace
from backbone_pull.db.postgres import PostgresAdapter

def _adapter():
    s = SimpleNamespace(db_host="h", db_port=5432, db_user="u",
                        db_password="p", db_name="db",
                        db_auto_create_database=True, db_maintenance_db="postgres")
    return PostgresAdapter(s)

def test_dsn_url_encodes_special_char_password():
    s = SimpleNamespace(db_host="h", db_port=5432, db_user="u",
                        db_password="p@ss:w/rd%x", db_name="db",
                        db_auto_create_database=True, db_maintenance_db="postgres")
    a = PostgresAdapter(s)
    assert a._engine.url.password == "p@ss:w/rd%x"
    assert a._engine.url.database == "db"

def test_col_type_mapping():
    a = _adapter()
    assert a.build_col_type({"type_name": "nvarchar", "type_length": 50}) == "varchar(50)"
    assert a.build_col_type({"type_name": "nvarchar", "type_length": -1}) == "text"
    assert a.build_col_type({"type_name": "nvarchar", "type_length": None}) == "varchar(500)"
    assert a.build_col_type({"type_name": "bit"}) == "smallint"
    assert a.build_col_type({"type_name": "datetime2"}) == "timestamp"
    assert a.build_col_type({"type_name": "uniqueidentifier"}) == "varchar(36)"
    assert a.build_col_type({"type_name": "decimal", "type_precision": 10, "type_scale": 2}) == "numeric(10,2)"
    assert a.build_col_type({"type_name": "varbinary", "type_length": -1}) == "bytea"

def test_quote_and_unbounded():
    a = _adapter()
    assert a.quote("nama") == '"nama"'
    assert a.is_unbounded("text") is True
    assert a.is_unbounded("varchar(50)") is False

def test_upsert_sql_is_on_conflict():
    a = _adapter()
    cols = ["npsn", "nama"]
    col_to_param = {"npsn": "p0", "nama": "p1"}
    sql = a.build_upsert_sql("sekolah", cols, ["npsn"], "dbo", col_to_param)
    assert 'INSERT INTO "dbo"."sekolah"' in sql
    assert "ON CONFLICT (\"npsn\")" in sql
    assert 'SET "nama" = EXCLUDED."nama"' in sql

def test_create_table_sql_if_not_exists():
    a = _adapter()
    col_defs = [{"name": "npsn", "type_name": "nvarchar", "type_length": 20, "nullable": False},
                {"name": "nama", "type_name": "nvarchar", "type_length": -1}]
    sql = a.build_create_table_sql("sekolah", col_defs, ["npsn"], "dbo")
    assert 'CREATE TABLE IF NOT EXISTS "dbo"."sekolah"' in sql
    assert '"npsn" varchar(20) NOT NULL' in sql
    assert 'PRIMARY KEY ("npsn")' in sql
